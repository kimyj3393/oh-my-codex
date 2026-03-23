
/**
 * TS Runtime Bridge — thin wrapper over omx-runtime binary.
 */

import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __bridge_dirname = dirname(fileURLToPath(import.meta.url));

export interface RuntimeSnapshot {
  schema_version: number;
  authority: AuthoritySnapshot;
  backlog: BacklogSnapshot;
  replay: ReplaySnapshot;
  readiness: ReadinessSnapshot;
}

export interface AuthoritySnapshot {
  owner: string | null;
  lease_id: string | null;
  leased_until: string | null;
  stale: boolean;
  stale_reason: string | null;
}

export interface BacklogSnapshot {
  pending: number;
  notified: number;
  delivered: number;
  failed: number;
}

export interface ReplaySnapshot {
  cursor: string | null;
  pending_events: number;
  last_replayed_event_id: string | null;
  deferred_leader_notification: boolean;
}

export interface ReadinessSnapshot {
  ready: boolean;
  reasons: string[];
}

export type JsonObject = Record<string, unknown>;
export interface TeamStateCommandContext { stateRoot: string; teamName: string; }

export type RuntimeCommand =
  | { command: 'AcquireAuthority'; owner: string; lease_id: string; leased_until: string }
  | { command: 'RenewAuthority'; owner: string; lease_id: string; leased_until: string }
  | { command: 'QueueDispatch'; request_id: string; target: string; metadata?: Record<string, unknown> }
  | { command: 'MarkNotified'; request_id: string; channel: string }
  | { command: 'MarkDelivered'; request_id: string }
  | { command: 'MarkFailed'; request_id: string; reason: string }
  | { command: 'RequestReplay'; cursor?: string }
  | { command: 'CaptureSnapshot' }
  | { command: 'CreateMailboxMessage'; message_id: string; from_worker: string; to_worker: string; body: string }
  | { command: 'MarkMailboxNotified'; message_id: string }
  | { command: 'MarkMailboxDelivered'; message_id: string };

export type TeamStateCommand =
  | { command: 'AppendTeamEvent'; state_root: string; team_name: string; event_json: string }
  | { command: 'WriteTeamWorkerIdentity'; state_root: string; team_name: string; worker_name: string; identity_json: string }
  | { command: 'ReadTeamWorkerHeartbeat'; state_root: string; team_name: string; worker_name: string }
  | { command: 'UpdateTeamWorkerHeartbeat'; state_root: string; team_name: string; worker_name: string; heartbeat_json: string }
  | { command: 'ReadTeamWorkerStatus'; state_root: string; team_name: string; worker_name: string }
  | { command: 'WriteTeamWorkerStatus'; state_root: string; team_name: string; worker_name: string; status_json: string }
  | { command: 'WriteTeamWorkerInbox'; state_root: string; team_name: string; worker_name: string; prompt: string }
  | { command: 'WriteTeamShutdownRequest'; state_root: string; team_name: string; worker_name: string; request_json: string }
  | { command: 'ReadTeamShutdownAck'; state_root: string; team_name: string; worker_name: string }
  | { command: 'ReadTeamPhase'; state_root: string; team_name: string }
  | { command: 'WriteTeamPhase'; state_root: string; team_name: string; phase_json: string }
  | { command: 'WriteTeamTaskApproval'; state_root: string; team_name: string; task_id: string; approval_json: string }
  | { command: 'ReadTeamTaskApproval'; state_root: string; team_name: string; task_id: string }
  | { command: 'ReadTeamTask'; state_root: string; team_name: string; task_id: string }
  | { command: 'ListTeamTasks'; state_root: string; team_name: string }
  | { command: 'ReadTeamMonitorSnapshot'; state_root: string; team_name: string }
  | { command: 'WriteTeamMonitorSnapshot'; state_root: string; team_name: string; snapshot_json: string }
  | { command: 'ReadTeamSummarySnapshot'; state_root: string; team_name: string }
  | { command: 'WriteTeamSummarySnapshot'; state_root: string; team_name: string; snapshot_json: string }
  | { command: 'AcquireTeamScalingLock'; state_root: string; team_name: string; owner_token: string; lock_stale_ms: number }
  | { command: 'ReleaseTeamScalingLock'; state_root: string; team_name: string; owner_token: string }
  | { command: 'AcquireTeamCreateTaskLock'; state_root: string; team_name: string; owner_token: string; lock_stale_ms: number }
  | { command: 'ReleaseTeamCreateTaskLock'; state_root: string; team_name: string; owner_token: string }
  | { command: 'AcquireTeamTaskClaimLock'; state_root: string; team_name: string; task_id: string; owner_token: string; lock_stale_ms: number }
  | { command: 'ReleaseTeamTaskClaimLock'; state_root: string; team_name: string; task_id: string; owner_token: string }
  | { command: 'AcquireTeamMailboxLock'; state_root: string; team_name: string; worker_name: string; owner_token: string; lock_stale_ms: number }
  | { command: 'ReleaseTeamMailboxLock'; state_root: string; team_name: string; worker_name: string; owner_token: string };

export type RuntimeEvent = { event: string; [key: string]: unknown };
export interface DispatchRecord { request_id: string; target: string; status: 'pending' | 'notified' | 'delivered' | 'failed'; created_at: string; notified_at: string | null; delivered_at: string | null; failed_at: string | null; reason: string | null; metadata: Record<string, unknown> | null; }
export interface MailboxRecord { message_id: string; from_worker: string; to_worker: string; body: string; created_at: string; notified_at: string | null; delivered_at: string | null; }
export interface TeamTaskListEntryPayload { file_id: string; task: unknown; }
interface RuntimeSchema { schema_version?: number; commands?: string[]; events?: string[]; transport?: string; }

const LEGACY_COMMAND_NAMES: Record<RuntimeCommand['command'], string> = {
  AcquireAuthority: 'acquire-authority',
  RenewAuthority: 'renew-authority',
  QueueDispatch: 'queue-dispatch',
  MarkNotified: 'mark-notified',
  MarkDelivered: 'mark-delivered',
  MarkFailed: 'mark-failed',
  RequestReplay: 'request-replay',
  CaptureSnapshot: 'capture-snapshot',
  CreateMailboxMessage: 'create-mailbox-message',
  MarkMailboxNotified: 'mark-mailbox-notified',
  MarkMailboxDelivered: 'mark-mailbox-delivered',
};

const schemaCache = new Map<string, RuntimeSchema>();
const bridgeCache = new Map<string, RuntimeBridge>();

export interface RuntimeBinaryDiscoveryOptions {
  debugPath?: string;
  releasePath?: string;
  fallbackBinary?: string;
  exists?: (path: string) => boolean;
}

export function resolveRuntimeBinaryPath(options: RuntimeBinaryDiscoveryOptions = {}): string {
  const exists = options.exists ?? existsSync;
  const envOverride = process.env.OMX_RUNTIME_BINARY?.trim();
  if (envOverride) return envOverride;
  const workspaceDebug = options.debugPath ?? resolve(__bridge_dirname, '../../target/debug/omx-runtime');
  if (exists(workspaceDebug)) return workspaceDebug;
  const workspaceRelease = options.releasePath ?? resolve(__bridge_dirname, '../../target/release/omx-runtime');
  if (exists(workspaceRelease)) return workspaceRelease;
  return options.fallbackBinary ?? 'omx-runtime';
}

export class RuntimeBridge {
  private binaryPath: string;
  private stateDir: string | undefined;
  private enabled: boolean;

  constructor(options: { stateDir?: string; binaryPath?: string } = {}) {
    this.enabled = process.env.OMX_RUNTIME_BRIDGE !== '0';
    this.stateDir = options.stateDir ? resolve(options.stateDir) : undefined;
    this.binaryPath = options.binaryPath ?? resolveRuntimeBinaryPath();
  }

  isEnabled(): boolean { return this.enabled; }

  execCommand(cmd: RuntimeCommand, options?: { compact?: boolean }): RuntimeEvent {
    return this.execJson<RuntimeEvent>(this.execArgs(cmd, options), [LEGACY_COMMAND_NAMES[cmd.command]]);
  }

  readSnapshot(): RuntimeSnapshot {
    const args = ['snapshot', '--json'];
    if (this.stateDir) args.push(`--state-dir=${this.stateDir}`);
    return JSON.parse(this.run(args)) as RuntimeSnapshot;
  }

  initStateDir(dir: string): void {
    this.run(['init', dir]);
    this.stateDir = resolve(dir);
  }

  readCompatFile<T>(filename: string): T | null {
    if (!this.stateDir) return null;
    const filePath = join(this.stateDir, filename);
    if (!existsSync(filePath)) return null;
    return JSON.parse(readFileSync(filePath, 'utf-8')) as T;
  }

  readAuthority(): AuthoritySnapshot | null { return this.readCompatFile<AuthoritySnapshot>('authority.json'); }
  readReadiness(): ReadinessSnapshot | null { return this.readCompatFile<ReadinessSnapshot>('readiness.json'); }
  readBacklog(): BacklogSnapshot | null { return this.readCompatFile<BacklogSnapshot>('backlog.json'); }
  readDispatchRecords(): DispatchRecord[] { return this.readCompatFile<{ records: DispatchRecord[] }>('dispatch.json')?.records ?? []; }
  readMailboxRecords(): MailboxRecord[] { return this.readCompatFile<{ records: MailboxRecord[] }>('mailbox.json')?.records ?? []; }

  appendTeamEvent(context: TeamStateCommandContext, line: string): void { this.execTeamStateOk({ command: 'AppendTeamEvent', state_root: context.stateRoot, team_name: context.teamName, event_json: line.endsWith('\n') ? line.slice(0, -1) : line }, ['append-team-event']); }
  writeTeamWorkerIdentity(context: TeamStateCommandContext, workerName: string, content: string): void { this.execTeamStateOk({ command: 'WriteTeamWorkerIdentity', state_root: context.stateRoot, team_name: context.teamName, worker_name: workerName, identity_json: content }, ['write-team-worker-identity']); }
  readTeamWorkerHeartbeat(context: TeamStateCommandContext, workerName: string): unknown | null { return this.execTeamStateJson({ command: 'ReadTeamWorkerHeartbeat', state_root: context.stateRoot, team_name: context.teamName, worker_name: workerName }, ['read-team-worker-heartbeat']); }
  updateTeamWorkerHeartbeat(context: TeamStateCommandContext, workerName: string, content: string): void { this.execTeamStateOk({ command: 'UpdateTeamWorkerHeartbeat', state_root: context.stateRoot, team_name: context.teamName, worker_name: workerName, heartbeat_json: content }, ['update-team-worker-heartbeat']); }
  readTeamWorkerStatus(context: TeamStateCommandContext, workerName: string): unknown | null { return this.execTeamStateJson({ command: 'ReadTeamWorkerStatus', state_root: context.stateRoot, team_name: context.teamName, worker_name: workerName }, ['read-team-worker-status']); }
  writeTeamWorkerStatus(context: TeamStateCommandContext, workerName: string, content: string): void { this.execTeamStateOk({ command: 'WriteTeamWorkerStatus', state_root: context.stateRoot, team_name: context.teamName, worker_name: workerName, status_json: content }, ['write-team-worker-status']); }
  writeTeamWorkerInbox(context: TeamStateCommandContext, workerName: string, prompt: string): void { this.execTeamStateOk({ command: 'WriteTeamWorkerInbox', state_root: context.stateRoot, team_name: context.teamName, worker_name: workerName, prompt }, ['write-team-worker-inbox']); }
  writeTeamShutdownRequest(context: TeamStateCommandContext, workerName: string, content: string): void { this.execTeamStateOk({ command: 'WriteTeamShutdownRequest', state_root: context.stateRoot, team_name: context.teamName, worker_name: workerName, request_json: content }, ['write-team-shutdown-request']); }
  readTeamShutdownAck(context: TeamStateCommandContext, workerName: string): unknown | null { return this.execTeamStateJson({ command: 'ReadTeamShutdownAck', state_root: context.stateRoot, team_name: context.teamName, worker_name: workerName }, ['read-team-shutdown-ack']); }
  readTeamPhase(context: TeamStateCommandContext): unknown | null { return this.execTeamStateJson({ command: 'ReadTeamPhase', state_root: context.stateRoot, team_name: context.teamName }, ['read-team-phase']); }
  writeTeamPhase(context: TeamStateCommandContext, content: string): void { this.execTeamStateOk({ command: 'WriteTeamPhase', state_root: context.stateRoot, team_name: context.teamName, phase_json: content }, ['write-team-phase']); }
  writeTeamTaskApproval(context: TeamStateCommandContext, taskId: string, content: string): void { this.execTeamStateOk({ command: 'WriteTeamTaskApproval', state_root: context.stateRoot, team_name: context.teamName, task_id: taskId, approval_json: content }, ['write-team-task-approval']); }
  readTeamTaskApproval(context: TeamStateCommandContext, taskId: string): unknown | null { return this.execTeamStateJson({ command: 'ReadTeamTaskApproval', state_root: context.stateRoot, team_name: context.teamName, task_id: taskId }, ['read-team-task-approval']); }
  readTeamTask(context: TeamStateCommandContext, taskId: string): unknown | null { return this.execTeamStateJson({ command: 'ReadTeamTask', state_root: context.stateRoot, team_name: context.teamName, task_id: taskId }, ['read-team-task']); }
  listTeamTasks(context: TeamStateCommandContext): TeamTaskListEntryPayload[] { const response = this.execTeamStateJson<unknown>({ command: 'ListTeamTasks', state_root: context.stateRoot, team_name: context.teamName }, ['list-team-tasks']); return Array.isArray(response) ? response as TeamTaskListEntryPayload[] : []; }
  readTeamMonitorSnapshot(context: TeamStateCommandContext): unknown | null { return this.execTeamStateJson({ command: 'ReadTeamMonitorSnapshot', state_root: context.stateRoot, team_name: context.teamName }, ['read-team-monitor-snapshot']); }
  writeTeamMonitorSnapshot(context: TeamStateCommandContext, content: string): void { this.execTeamStateOk({ command: 'WriteTeamMonitorSnapshot', state_root: context.stateRoot, team_name: context.teamName, snapshot_json: content }, ['write-team-monitor-snapshot']); }
  readTeamSummarySnapshot(context: TeamStateCommandContext): unknown | null { return this.execTeamStateJson({ command: 'ReadTeamSummarySnapshot', state_root: context.stateRoot, team_name: context.teamName }, ['read-team-summary-snapshot']); }
  writeTeamSummarySnapshot(context: TeamStateCommandContext, content: string): void { this.execTeamStateOk({ command: 'WriteTeamSummarySnapshot', state_root: context.stateRoot, team_name: context.teamName, snapshot_json: content }, ['write-team-summary-snapshot']); }
  acquireScalingLock(context: TeamStateCommandContext, lockStaleMs: number): string { const ownerToken = makeLockOwnerToken(); const response = this.execTeamStateJson<{ owner_token?: string }>({ command: 'AcquireTeamScalingLock', state_root: context.stateRoot, team_name: context.teamName, owner_token: ownerToken, lock_stale_ms: lockStaleMs }, ['acquire-team-scaling-lock']); if (typeof response.owner_token !== 'string' || response.owner_token.trim() === '') throw new Error('omx-runtime returned an empty owner token for scaling lock'); return response.owner_token; }
  releaseScalingLock(context: TeamStateCommandContext, ownerToken: string): void { this.execTeamStateOk({ command: 'ReleaseTeamScalingLock', state_root: context.stateRoot, team_name: context.teamName, owner_token: ownerToken }, ['release-team-scaling-lock']); }
  acquireTeamLock(context: TeamStateCommandContext, lockStaleMs: number): string { const ownerToken = makeLockOwnerToken(); const response = this.execTeamStateJson<{ owner_token?: string }>({ command: 'AcquireTeamCreateTaskLock', state_root: context.stateRoot, team_name: context.teamName, owner_token: ownerToken, lock_stale_ms: lockStaleMs }, ['acquire-team-create-task-lock']); if (typeof response.owner_token !== 'string' || response.owner_token.trim() === '') throw new Error('omx-runtime returned an empty owner token for team lock'); return response.owner_token; }
  releaseTeamLock(context: TeamStateCommandContext, ownerToken: string): void { this.execTeamStateOk({ command: 'ReleaseTeamCreateTaskLock', state_root: context.stateRoot, team_name: context.teamName, owner_token: ownerToken }, ['release-team-create-task-lock']); }
  acquireTaskClaimLock(context: TeamStateCommandContext, taskId: string, lockStaleMs: number): string | null { const ownerToken = makeLockOwnerToken(); const response = this.execTeamStateJson<{ ok?: boolean; owner_token?: string | null }>({ command: 'AcquireTeamTaskClaimLock', state_root: context.stateRoot, team_name: context.teamName, task_id: taskId, owner_token: ownerToken, lock_stale_ms: lockStaleMs }, ['acquire-team-task-claim-lock']); if (response.ok !== true) return null; return typeof response.owner_token === 'string' && response.owner_token.trim() !== '' ? response.owner_token : null; }
  releaseTaskClaimLock(context: TeamStateCommandContext, taskId: string, ownerToken: string): void { this.execTeamStateOk({ command: 'ReleaseTeamTaskClaimLock', state_root: context.stateRoot, team_name: context.teamName, task_id: taskId, owner_token: ownerToken }, ['release-team-task-claim-lock']); }
  acquireMailboxLock(context: TeamStateCommandContext, workerName: string, lockStaleMs: number): string { const ownerToken = makeLockOwnerToken(); const response = this.execTeamStateJson<{ owner_token?: string }>({ command: 'AcquireTeamMailboxLock', state_root: context.stateRoot, team_name: context.teamName, worker_name: workerName, owner_token: ownerToken, lock_stale_ms: lockStaleMs }, ['acquire-team-mailbox-lock']); if (typeof response.owner_token !== 'string' || response.owner_token.trim() === '') throw new Error('omx-runtime returned an empty owner token for mailbox lock'); return response.owner_token; }
  releaseMailboxLock(context: TeamStateCommandContext, workerName: string, ownerToken: string): void { this.execTeamStateOk({ command: 'ReleaseTeamMailboxLock', state_root: context.stateRoot, team_name: context.teamName, worker_name: workerName, owner_token: ownerToken }, ['release-team-mailbox-lock']); }

  private execTeamStateOk(command: TeamStateCommand, requiredCommands: string[]): void { void this.execTeamStateJson<unknown>(command, requiredCommands); }
  private execTeamStateJson<T>(command: TeamStateCommand, requiredCommands: string[]): T { this.validateSchema(requiredCommands); const event = JSON.parse(this.run(this.execArgs(command))) as RuntimeEvent; return this.unwrapTeamStatePayload<T>(command.command, event); }
  private execArgs(command: RuntimeCommand | TeamStateCommand, options?: { compact?: boolean }): string[] { const args = ['exec', JSON.stringify(command)]; if (this.stateDir) args.push(`--state-dir=${this.stateDir}`); if (options?.compact) args.push('--compact'); return args; }
  private execJson<T>(args: string[], requiredCommands: string[] = []): T { this.validateSchema(requiredCommands); return JSON.parse(this.run(args)) as T; }
  private unwrapTeamStatePayload<T>(commandName: TeamStateCommand['command'], event: RuntimeEvent): T { switch (commandName) { case 'AppendTeamEvent': if (event.event === 'TeamEventAppended') return { ok: true } as T; break; case 'WriteTeamWorkerIdentity': if (event.event === 'TeamWorkerIdentityWritten') return { ok: true } as T; break; case 'ReadTeamWorkerHeartbeat': if (event.event === 'TeamWorkerHeartbeatRead') return (event.heartbeat ?? null) as T; break; case 'UpdateTeamWorkerHeartbeat': if (event.event === 'TeamWorkerHeartbeatUpdated') return { ok: true } as T; break; case 'ReadTeamWorkerStatus': if (event.event === 'TeamWorkerStatusRead') return (event.status ?? null) as T; break; case 'WriteTeamWorkerStatus': if (event.event === 'TeamWorkerStatusWritten') return { ok: true } as T; break; case 'WriteTeamWorkerInbox': if (event.event === 'TeamWorkerInboxWritten') return { ok: true } as T; break; case 'WriteTeamShutdownRequest': if (event.event === 'TeamShutdownRequestWritten') return { ok: true } as T; break; case 'ReadTeamShutdownAck': if (event.event === 'TeamShutdownAckRead') return (event.ack ?? null) as T; break; case 'ReadTeamPhase': if (event.event === 'TeamPhaseRead') return (event.phase ?? null) as T; break; case 'WriteTeamPhase': if (event.event === 'TeamPhaseWritten') return { ok: true } as T; break; case 'WriteTeamTaskApproval': if (event.event === 'TeamTaskApprovalWritten') return { ok: true } as T; break; case 'ReadTeamTaskApproval': if (event.event === 'TeamTaskApprovalRead') return (event.approval ?? null) as T; break; case 'ReadTeamTask': if (event.event === 'TeamTaskRead') return (event.task ?? null) as T; break; case 'ListTeamTasks': if (event.event === 'TeamTasksListed') return event.tasks as T; break; case 'ReadTeamMonitorSnapshot': if (event.event === 'TeamMonitorSnapshotRead') return (event.snapshot ?? null) as T; break; case 'WriteTeamMonitorSnapshot': if (event.event === 'TeamMonitorSnapshotWritten') return { ok: true } as T; break; case 'ReadTeamSummarySnapshot': if (event.event === 'TeamSummarySnapshotRead') return (event.snapshot ?? null) as T; break; case 'WriteTeamSummarySnapshot': if (event.event === 'TeamSummarySnapshotWritten') return { ok: true } as T; break; case 'AcquireTeamScalingLock': if (event.event === 'TeamScalingLockAcquired') return { owner_token: event.owner_token } as T; break; case 'ReleaseTeamScalingLock': if (event.event === 'TeamScalingLockReleased') return { ok: true, released: event.released } as T; break; case 'AcquireTeamCreateTaskLock': if (event.event === 'TeamCreateTaskLockAcquired') return { owner_token: event.owner_token } as T; break; case 'ReleaseTeamCreateTaskLock': if (event.event === 'TeamCreateTaskLockReleased') return { ok: true, released: event.released } as T; break; case 'AcquireTeamTaskClaimLock': if (event.event === 'TeamTaskClaimLockAcquireResult') return { ok: event.ok, owner_token: event.owner_token ?? null } as T; break; case 'ReleaseTeamTaskClaimLock': if (event.event === 'TeamTaskClaimLockReleased') return { ok: true, released: event.released } as T; break; case 'AcquireTeamMailboxLock': if (event.event === 'TeamMailboxLockAcquired') return { owner_token: event.owner_token } as T; break; case 'ReleaseTeamMailboxLock': if (event.event === 'TeamMailboxLockReleased') return { ok: true, released: event.released } as T; break; default: break; } throw new Error(`omx-runtime returned unexpected event for ${commandName}: ${event.event}`); }
  private validateSchema(requiredCommands: string[]): void { if (requiredCommands.length === 0) return; const schema = this.loadSchema(); const commands = new Set(schema.commands ?? []); const missing = requiredCommands.filter((command) => !commands.has(command)); if (missing.length > 0) throw new Error(`omx-runtime schema missing commands: ${missing.join(', ')}. Bridge types may be out of sync with the Rust binary.`); }
  private loadSchema(): RuntimeSchema { const cached = schemaCache.get(this.binaryPath); if (cached) return cached; try { const schema = JSON.parse(this.run(['schema', '--json'])) as RuntimeSchema; schemaCache.set(this.binaryPath, schema); return schema; } catch { const fallback: RuntimeSchema = {}; schemaCache.set(this.binaryPath, fallback); return fallback; } }
  private run(args: string[]): string { try { return execFileSync(this.binaryPath, args, { encoding: 'utf-8', timeout: 10_000, maxBuffer: 1024 * 1024 }); } catch (error: unknown) { const execError = error as { stderr?: string; message?: string }; const stderr = execError.stderr?.trim() ?? execError.message ?? 'unknown error'; throw new Error(`omx-runtime ${args[0]} failed: ${stderr}`); } }
}

export function getDefaultBridge(stateDir?: string): RuntimeBridge { const binaryPath = resolveRuntimeBinaryPath(); const key = `${binaryPath}::${stateDir ? resolve(stateDir) : 'default'}`; const cached = bridgeCache.get(key); if (cached) return cached; const bridge = new RuntimeBridge({ stateDir, binaryPath }); bridgeCache.set(key, bridge); return bridge; }
export function clearBridgeCachesForTests(): void { bridgeCache.clear(); schemaCache.clear(); }
export function resetRuntimeBridgeCachesForTests(): void { clearBridgeCachesForTests(); }
export function isBridgeEnabled(): boolean { return process.env.OMX_RUNTIME_BRIDGE !== '0'; }

function makeLockOwnerToken(): string {
  return String(process.pid) + '.' + String(Date.now()) + '.' + Math.random().toString(16).slice(2);
}
