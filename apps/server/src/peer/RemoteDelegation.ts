import {
  CommandId,
  type EnvironmentId,
  NodeId,
  OrchestratorMcpFailure,
  type OrchestratorMcpDelegateTaskInput,
  type OrchestratorMcpTaskCancelResult,
  type OrchestrationV2RemoteTaskChild,
  type OrchestrationV2Subagent,
  type ProjectId,
  ProviderDriverKind,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";
import * as Scope from "effect/Scope";

import type * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import { ProjectToolkit } from "../mcp/toolkits/project/tools.ts";
import { OrchestratorToolkit } from "../mcp/toolkits/orchestrator/tools.ts";
import { resolveInteractionMode, resolveRuntimeMode } from "../mcp/OrchestratorMcpService.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import { forkParked } from "../serverActivation.ts";
import * as PeerForwarding from "./PeerForwarding.ts";
import * as PeerLinks from "./PeerLinks.ts";

/** How long one follow of a remote task waits on its thread there before checking again. */
const FOLLOW_WAIT_MS = 10 * 60 * 1_000;
/** A linked environment that stops answering is retried, backing off to this. */
const FOLLOW_MAX_BACKOFF = Duration.minutes(5);
/** How far into a long thread there its last reply is looked for. */
const MAX_RESULT_PAGES = 20;

type ThreadScope = McpInvocationContext.McpThreadInvocationScope;

/**
 * `delegate_task` to a linked environment. The task's child is an ordinary
 * thread there, launched through the link; the parent here records the task
 * like a local one, without a child thread, and a follower completes it when
 * the thread there ends, so the parent wakes as it would for a local child.
 */
export class RemoteDelegation extends Context.Service<
  RemoteDelegation,
  {
    readonly delegate: (
      scope: ThreadScope,
      input: OrchestratorMcpDelegateTaskInput & {
        readonly target: NonNullable<OrchestratorMcpDelegateTaskInput["target"]> & {
          readonly environmentId: EnvironmentId;
        };
      },
    ) => Effect.Effect<{ readonly taskId: NodeId }, OrchestratorMcpFailure>;
    /** Interrupts the task's thread there, then completes it here as cancelled. */
    readonly cancel: (
      scope: ThreadScope,
      task: OrchestrationV2Subagent,
      reason: string | undefined,
    ) => Effect.Effect<OrchestratorMcpTaskCancelResult, OrchestratorMcpFailure>;
    /** Follows every open remote task until it ends, including after a restart. */
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  }
>()("t3/peer/RemoteDelegation") {}

const failure = (code: OrchestratorMcpFailure["code"], message: string) =>
  new OrchestratorMcpFailure({ code, message });

const make = Effect.gen(function* () {
  const forwarding = yield* PeerForwarding.PeerForwarding;
  const links = yield* PeerLinks.PeerLinks;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const projects = yield* ProjectService.ProjectService;
  const repositoryIdentities = yield* RepositoryIdentityResolver.RepositoryIdentityResolver;
  const crypto = yield* Crypto.Crypto;
  const hereId = yield* ServerEnvironment.ServerEnvironment.pipe(
    Effect.flatMap((environment) => environment.getEnvironmentId),
  );
  // Followers live as long as this service, so shutting down stops them.
  const followers = yield* Scope.Scope;

  const tools = OrchestratorToolkit.tools;

  /** The peer's project with this thread's repository, or the one the caller named. */
  const resolveRemoteProject = (
    scope: ThreadScope,
    environmentId: EnvironmentId,
    parentProjectId: ProjectId,
    requested: ProjectId | undefined,
  ) =>
    Effect.gen(function* () {
      if (requested !== undefined) return requested;
      const here = yield* projects.getById(parentProjectId).pipe(
        Effect.map(Option.getOrUndefined),
        Effect.orElseSucceed(() => undefined),
      );
      // The project's own identity is a one-minute cache that reads blank when
      // cold, so resolve it here, as the other side's t3_project_list does.
      const key =
        here === undefined
          ? undefined
          : (yield* repositoryIdentities.resolve(here.workspaceRoot))?.canonicalKey;
      if (key === undefined) {
        return yield* failure(
          "target_required",
          "Pass target.projectId: this thread's project has no repository to match there.",
        );
      }
      const listed = yield* forwarding.call(
        scope,
        ProjectToolkit.tools.t3_project_list,
        environmentId,
        {
          limit: 100,
        },
      );
      const matches = listed.projects.filter(
        (project) => project.repositoryIdentity?.canonicalKey === key,
      );
      if (matches.length === 1) return matches[0]!.id;
      return yield* failure(
        "invalid_request",
        matches.length === 0
          ? `No project there has this repository (${key}). Pass target.projectId; t3_project_list with this environmentId lists them.`
          : `Several projects there have this repository: ${matches.map((project) => `${project.title} (${project.id})`).join(", ")}. Pass target.projectId.`,
      );
    });

  const delegate: RemoteDelegation["Service"]["delegate"] = (scope, input) =>
    Effect.gen(function* () {
      const parent = yield* threads
        .getThreadRecords(scope.thread.threadId, ["runs"])
        .pipe(
          Effect.mapError(() => failure("thread_not_found", "The calling thread was not found.")),
        );
      const parentRun = parent.runs
        .filter(ThreadManagement.isActiveRun)
        .toSorted((left, right) => right.ordinal - left.ordinal)[0];
      if (
        parentRun === undefined ||
        parentRun.rootNodeId === null ||
        parentRun.providerInstanceId !== scope.thread.providerInstanceId
      ) {
        return yield* failure(
          "parent_not_active",
          "Delegated tasks require an active run owned by this MCP provider session.",
        );
      }
      const parentNodeId = parentRun.rootNodeId;
      const { target } = input;
      // The provider and model inherit as they do for a task here. Another
      // provider there has no model to inherit, so the caller names one.
      const inherited = parent.thread.modelSelection;
      const instanceId = target.providerInstanceId ?? inherited.instanceId;
      const model =
        target.model ?? (instanceId === inherited.instanceId ? inherited.model : undefined);
      if (model === undefined) {
        return yield* failure(
          "target_required",
          "Pass target.model from orchestrator_capabilities with this environmentId.",
        );
      }
      const options =
        target.options ??
        (instanceId === inherited.instanceId && model === inherited.model
          ? inherited.options
          : undefined);
      // Modes asked for are checked against this thread. Omitted ones are left
      // to the other side, which inherits this thread's modes capped by the
      // link's access, so a narrower link still takes the task.
      const runtimeMode =
        input.runtimeMode === undefined || input.runtimeMode === "inherit"
          ? undefined
          : yield* resolveRuntimeMode(parent.thread.runtimeMode, input.runtimeMode);
      const interactionMode =
        input.interactionMode === undefined || input.interactionMode === "inherit"
          ? undefined
          : yield* resolveInteractionMode(parent.thread.interactionMode, input.interactionMode);
      const projectId = yield* resolveRemoteProject(
        scope,
        target.environmentId,
        parent.thread.projectId,
        target.projectId,
      );
      // One key for the launch there and the record here, so a retry finds both.
      const key = input.clientRequestId ?? (yield* crypto.randomUUIDv4.pipe(Effect.orDie));
      const launched = yield* forwarding.call(
        scope,
        ProjectToolkit.tools.t3_thread_launch,
        target.environmentId,
        {
          projectId,
          title: input.title ?? parent.thread.title,
          modelSelection: { instanceId, model, ...(options === undefined ? {} : { options }) },
          ...(runtimeMode === undefined ? {} : { runtimeMode }),
          ...(interactionMode === undefined ? {} : { interactionMode }),
          message: input.task,
          clientRequestId: `delegate:${scope.thread.threadId}:${key}`,
          // The thread there shows it is this thread's subagent, with a way back.
          delegatedFrom: {
            environmentId: hereId,
            threadId: parent.thread.id,
            title: parent.thread.title,
          },
        },
      );
      const link = (yield* links.list.pipe(Effect.orElseSucceed(() => []))).find(
        (candidate) => candidate.environmentId === target.environmentId,
      );
      const commandId = CommandId.make(
        `command:mcp:remote-delegate:${encodeURIComponent(scope.thread.threadId)}:${encodeURIComponent(key)}`,
      );
      const recorded = yield* threads
        .dispatch({
          type: "delegated_task.remote.request",
          commandId,
          parentThreadId: scope.thread.threadId,
          parentRunId: parentRun.id,
          parentNodeId,
          task: input.task,
          ...(input.title === undefined ? {} : { title: input.title }),
          // The driver runs there; this side only labels the task with it.
          driver: ProviderDriverKind.make("remote"),
          modelSelection: launched.modelSelection,
          remoteChild: {
            environmentId: target.environmentId,
            threadId: launched.threadId,
            label: link?.label ?? target.environmentId,
          },
          completionWake: input.mode === "wait" ? "settled_only" : "always",
        })
        .pipe(
          Effect.mapError((error) =>
            failure("orchestration_error", `Unable to record the delegated task: ${error.message}`),
          ),
        );
      const taskEvent = recorded.storedEvents.find(
        (stored) => stored.event.type === "subagent.updated",
      );
      const taskId =
        taskEvent?.event.type === "subagent.updated"
          ? taskEvent.event.payload.id
          : // A replayed request recorded the task the first time.
            yield* threads.getThreadRecords(scope.thread.threadId, ["subagents"]).pipe(
              Effect.map(
                ({ subagents }) =>
                  subagents.find((task) => task.remoteChild?.threadId === launched.threadId)?.id,
              ),
              Effect.orElseSucceed(() => undefined),
            );
      if (taskId === undefined) {
        return yield* failure("orchestration_error", "The delegated task was not recorded.");
      }
      yield* follow(scope.thread.threadId, taskId).pipe(Effect.forkIn(followers));
      return { taskId };
    });

  /** Completes `task` here with its thread there's final state. */
  const complete = (
    parentThreadId: ThreadId,
    taskId: NodeId,
    status: "completed" | "failed" | "cancelled" | "interrupted",
    result: string,
  ) =>
    threads
      .dispatch({
        type: "delegated_task.remote.complete",
        commandId: CommandId.make(`command:remote-task-complete:${parentThreadId}:${taskId}`),
        parentThreadId,
        taskId,
        status,
        result,
      })
      .pipe(Effect.asVoid);

  /** The last reply on the task's thread there, which is its result. */
  const lastReply = (scope: ThreadScope, remote: OrchestrationV2RemoteTaskChild) =>
    Effect.gen(function* () {
      let reply: string | null = null;
      let afterPosition: number | undefined;
      for (let page = 0; page < MAX_RESULT_PAGES; page += 1) {
        const read = yield* forwarding.call(scope, tools.t3_thread_read, remote.environmentId, {
          threadId: remote.threadId,
          view: "messages",
          limit: 100,
          ...(afterPosition === undefined ? {} : { afterPosition }),
        });
        reply = read.items.findLast((item) => item.type === "assistant_message")?.text ?? reply;
        if (!read.hasMore || read.nextPosition === null) break;
        afterPosition = read.nextPosition;
      }
      return reply ?? "(The thread in the linked environment ended without a reply.)";
    });

  /**
   * Waits on the task's thread there until its run ends, then completes the
   * task here. A revoked or expired link fails the task; anything else (the
   * other machine asleep, a dropped route) is retried, backing off.
   */
  const follow = (parentThreadId: ThreadId, taskId: NodeId): Effect.Effect<void> =>
    followOnce(parentThreadId, taskId).pipe(
      // `false` means it is still running there: follow again straight away.
      Effect.repeat({ while: (done) => !done }),
      Effect.catch((error) =>
        error._tag === "OrchestratorMcpFailure" && error.code === "capability_denied"
          ? // The client's message already says the link was refused and how to renew it.
            complete(parentThreadId, taskId, "failed", error.message).pipe(
              Effect.ignoreCause({ log: true }),
            )
          : Effect.fail(error),
      ),
      Effect.retry(
        Schedule.exponential(Duration.seconds(2)).pipe(
          Schedule.modifyDelay(({ duration }) =>
            Effect.succeed(Duration.min(duration, FOLLOW_MAX_BACKOFF)),
          ),
        ),
      ),
      Effect.ignoreCause({ log: true }),
    );

  /** One wait on the task's thread there; true once the task here is done. */
  const followOnce = (parentThreadId: ThreadId, taskId: NodeId) =>
    Effect.gen(function* () {
      const records = yield* threads.getThreadRecords(parentThreadId, ["subagents"]);
      const task = records.subagents.find((candidate) => candidate.id === taskId);
      if (task?.remoteChild === undefined || task.result !== null) return true;
      const remote = task.remoteChild;
      const scope: ThreadScope = {
        environmentId: remote.environmentId,
        requestNamespace: `remote-task:${parentThreadId}`,
        thread: {
          threadId: parentThreadId,
          providerSessionId: `remote-task:${taskId}`,
          providerInstanceId: records.thread.providerInstanceId,
        },
        client: undefined,
        capabilities: new Set(["orchestration"]),
        issuedAt: 0,
      };
      const waited = yield* forwarding.waitForThread(scope, remote.environmentId, {
        threadId: remote.threadId,
        timeoutMs: FOLLOW_WAIT_MS,
      });
      if (waited.timedOut || !isTerminal(waited.status)) return false;
      const result = yield* lastReply(scope, remote);
      yield* complete(parentThreadId, taskId, terminalStatus(waited.status), result);
      return true;
    });

  const cancel: RemoteDelegation["Service"]["cancel"] = (scope, task, reason) =>
    Effect.gen(function* () {
      const remote = task.remoteChild;
      if (remote === undefined) {
        return yield* failure("task_not_found", `Delegated task ${task.id} is not remote.`);
      }
      if (task.result !== null) {
        return {
          taskId: task.id,
          status:
            task.status === "completed" || task.status === "failed" || task.status === "interrupted"
              ? task.status
              : "cancelled",
        } satisfies OrchestratorMcpTaskCancelResult;
      }
      yield* forwarding.call(scope, tools.t3_thread_interrupt, remote.environmentId, {
        threadId: remote.threadId,
        ...(reason === undefined ? {} : { reason }),
        clientRequestId: `cancel:${task.id}`,
      });
      yield* complete(
        task.threadId,
        task.id,
        "cancelled",
        reason === undefined ? "Cancelled." : `Cancelled: ${reason}`,
      ).pipe(
        Effect.mapError((error) =>
          failure("orchestration_error", `Unable to record the cancellation: ${error.message}`),
        ),
      );
      return {
        taskId: task.id,
        status: "cancel_requested",
      } satisfies OrchestratorMcpTaskCancelResult;
    });

  const start: RemoteDelegation["Service"]["start"] = () =>
    forkParked(
      Effect.gen(function* () {
        const open = yield* projections.getOpenRemoteDelegatedTasks;
        yield* Effect.forEach(
          open,
          ({ parentThreadId, taskId }) => follow(parentThreadId, taskId),
          {
            concurrency: "unbounded",
            discard: true,
          },
        );
      }).pipe(Effect.ignoreCause({ log: true })),
    );

  return RemoteDelegation.of({ delegate, cancel, start });
});

const isTerminal = (status: string) =>
  status === "completed" ||
  status === "failed" ||
  status === "cancelled" ||
  status === "interrupted" ||
  status === "rolled_back";

const terminalStatus = (status: string): "completed" | "failed" | "cancelled" | "interrupted" =>
  status === "completed" || status === "failed" || status === "interrupted" ? status : "cancelled";

export const layer = Layer.effect(RemoteDelegation, make);
