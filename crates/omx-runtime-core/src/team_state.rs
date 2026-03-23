use crate::{RuntimeCommand, RuntimeEvent};
use serde_json::Value;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const SCALING_LOCK_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_LOCK_TIMEOUT_MS: u64 = 5_000;
const LOCK_OWNER_RETRY_MS: u64 = 25;

#[derive(Debug)]
pub enum TeamStateError {
    Io(io::Error),
    Json(serde_json::Error),
    Message(String),
}

impl fmt::Display for TeamStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "io error: {error}"),
            Self::Json(error) => write!(f, "json error: {error}"),
            Self::Message(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for TeamStateError {}

impl From<io::Error> for TeamStateError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for TeamStateError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

pub fn is_team_state_command(command: &RuntimeCommand) -> bool {
    matches!(
        command,
        RuntimeCommand::AppendTeamEvent { .. }
            | RuntimeCommand::WriteTeamWorkerIdentity { .. }
            | RuntimeCommand::ReadTeamWorkerHeartbeat { .. }
            | RuntimeCommand::UpdateTeamWorkerHeartbeat { .. }
            | RuntimeCommand::ReadTeamWorkerStatus { .. }
            | RuntimeCommand::WriteTeamWorkerStatus { .. }
            | RuntimeCommand::WriteTeamWorkerInbox { .. }
            | RuntimeCommand::WriteTeamShutdownRequest { .. }
            | RuntimeCommand::ReadTeamShutdownAck { .. }
            | RuntimeCommand::ReadTeamPhase { .. }
            | RuntimeCommand::WriteTeamPhase { .. }
            | RuntimeCommand::WriteTeamTaskApproval { .. }
            | RuntimeCommand::ReadTeamTaskApproval { .. }
            | RuntimeCommand::ReadTeamTask { .. }
            | RuntimeCommand::ListTeamTasks { .. }
            | RuntimeCommand::ReadTeamMonitorSnapshot { .. }
            | RuntimeCommand::WriteTeamMonitorSnapshot { .. }
            | RuntimeCommand::ReadTeamSummarySnapshot { .. }
            | RuntimeCommand::WriteTeamSummarySnapshot { .. }
            | RuntimeCommand::AcquireTeamScalingLock { .. }
            | RuntimeCommand::ReleaseTeamScalingLock { .. }
            | RuntimeCommand::AcquireTeamCreateTaskLock { .. }
            | RuntimeCommand::ReleaseTeamCreateTaskLock { .. }
            | RuntimeCommand::AcquireTeamTaskClaimLock { .. }
            | RuntimeCommand::ReleaseTeamTaskClaimLock { .. }
            | RuntimeCommand::AcquireTeamMailboxLock { .. }
            | RuntimeCommand::ReleaseTeamMailboxLock { .. }
    )
}

pub fn execute_event(command: RuntimeCommand) -> Result<RuntimeEvent, TeamStateError> {
    match command {
        RuntimeCommand::AppendTeamEvent { .. } => {
            let _ = execute(command)?;
            Ok(RuntimeEvent::TeamEventAppended)
        }
        RuntimeCommand::WriteTeamWorkerIdentity { .. } => {
            let _ = execute(command)?;
            Ok(RuntimeEvent::TeamWorkerIdentityWritten)
        }
        RuntimeCommand::ReadTeamWorkerHeartbeat { .. } => {
            let heartbeat = execute(command)?;
            Ok(RuntimeEvent::TeamWorkerHeartbeatRead {
                heartbeat: if heartbeat.is_null() { None } else { Some(heartbeat) },
            })
        }
        RuntimeCommand::UpdateTeamWorkerHeartbeat { .. } => {
            let _ = execute(command)?;
            Ok(RuntimeEvent::TeamWorkerHeartbeatUpdated)
        }
        RuntimeCommand::ReadTeamWorkerStatus { .. } => {
            let status = execute(command)?;
            Ok(RuntimeEvent::TeamWorkerStatusRead {
                status: if status.is_null() { None } else { Some(status) },
            })
        }
        RuntimeCommand::WriteTeamWorkerStatus { .. } => {
            let _ = execute(command)?;
            Ok(RuntimeEvent::TeamWorkerStatusWritten)
        }
        RuntimeCommand::WriteTeamWorkerInbox { .. } => {
            let _ = execute(command)?;
            Ok(RuntimeEvent::TeamWorkerInboxWritten)
        }
        RuntimeCommand::WriteTeamShutdownRequest { .. } => {
            let _ = execute(command)?;
            Ok(RuntimeEvent::TeamShutdownRequestWritten)
        }
        RuntimeCommand::ReadTeamShutdownAck { .. } => {
            let ack = execute(command)?;
            Ok(RuntimeEvent::TeamShutdownAckRead {
                ack: if ack.is_null() { None } else { Some(ack) },
            })
        }
        RuntimeCommand::ReadTeamPhase { .. } => {
            let phase = execute(command)?;
            Ok(RuntimeEvent::TeamPhaseRead {
                phase: if phase.is_null() { None } else { Some(phase) },
            })
        }
        RuntimeCommand::WriteTeamPhase { .. } => {
            let _ = execute(command)?;
            Ok(RuntimeEvent::TeamPhaseWritten)
        }
        RuntimeCommand::WriteTeamTaskApproval { .. } => {
            let _ = execute(command)?;
            Ok(RuntimeEvent::TeamTaskApprovalWritten)
        }
        RuntimeCommand::ReadTeamTaskApproval { .. } => {
            let approval = execute(command)?;
            Ok(RuntimeEvent::TeamTaskApprovalRead {
                approval: if approval.is_null() { None } else { Some(approval) },
            })
        }
        RuntimeCommand::ReadTeamTask { .. } => {
            let task = execute(command)?;
            Ok(RuntimeEvent::TeamTaskRead {
                task: if task.is_null() { None } else { Some(task) },
            })
        }
        RuntimeCommand::ListTeamTasks { .. } => {
            let tasks = execute(command)?;
            Ok(RuntimeEvent::TeamTasksListed {
                tasks: tasks.as_array().cloned().unwrap_or_default(),
            })
        }
        RuntimeCommand::ReadTeamMonitorSnapshot { .. } => {
            let snapshot = execute(command)?;
            Ok(RuntimeEvent::TeamMonitorSnapshotRead {
                snapshot: if snapshot.is_null() { None } else { Some(snapshot) },
            })
        }
        RuntimeCommand::WriteTeamMonitorSnapshot { .. } => {
            let _ = execute(command)?;
            Ok(RuntimeEvent::TeamMonitorSnapshotWritten)
        }
        RuntimeCommand::ReadTeamSummarySnapshot { .. } => {
            let snapshot = execute(command)?;
            Ok(RuntimeEvent::TeamSummarySnapshotRead {
                snapshot: if snapshot.is_null() { None } else { Some(snapshot) },
            })
        }
        RuntimeCommand::WriteTeamSummarySnapshot { .. } => {
            let _ = execute(command)?;
            Ok(RuntimeEvent::TeamSummarySnapshotWritten)
        }
        RuntimeCommand::AcquireTeamScalingLock { .. } => {
            let value = execute(command)?;
            Ok(RuntimeEvent::TeamScalingLockAcquired {
                owner_token: value
                    .get("owner_token")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            })
        }
        RuntimeCommand::ReleaseTeamScalingLock { .. } => {
            let value = execute(command)?;
            Ok(RuntimeEvent::TeamScalingLockReleased {
                released: value.get("ok").and_then(Value::as_bool).unwrap_or(false),
            })
        }
        RuntimeCommand::AcquireTeamCreateTaskLock { .. } => {
            let value = execute(command)?;
            Ok(RuntimeEvent::TeamCreateTaskLockAcquired {
                owner_token: value
                    .get("owner_token")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            })
        }
        RuntimeCommand::ReleaseTeamCreateTaskLock { .. } => {
            let value = execute(command)?;
            Ok(RuntimeEvent::TeamCreateTaskLockReleased {
                released: value.get("ok").and_then(Value::as_bool).unwrap_or(false),
            })
        }
        RuntimeCommand::AcquireTeamTaskClaimLock { .. } => {
            let value = execute(command)?;
            Ok(RuntimeEvent::TeamTaskClaimLockAcquireResult {
                ok: value.get("ok").and_then(Value::as_bool).unwrap_or(false),
                owner_token: value
                    .get("owner_token")
                    .and_then(Value::as_str)
                    .map(|token| token.to_string()),
            })
        }
        RuntimeCommand::ReleaseTeamTaskClaimLock { .. } => {
            let value = execute(command)?;
            Ok(RuntimeEvent::TeamTaskClaimLockReleased {
                released: value.get("ok").and_then(Value::as_bool).unwrap_or(false),
            })
        }
        RuntimeCommand::AcquireTeamMailboxLock { .. } => {
            let value = execute(command)?;
            Ok(RuntimeEvent::TeamMailboxLockAcquired {
                owner_token: value
                    .get("owner_token")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            })
        }
        RuntimeCommand::ReleaseTeamMailboxLock { .. } => {
            let value = execute(command)?;
            Ok(RuntimeEvent::TeamMailboxLockReleased {
                released: value.get("ok").and_then(Value::as_bool).unwrap_or(false),
            })
        }
        _ => Err(TeamStateError::Message(
            "non team-state command provided to event executor".to_string(),
        )),
    }
}

pub fn execute(command: RuntimeCommand) -> Result<Value, TeamStateError> {
    match command {
        RuntimeCommand::AppendTeamEvent {
            state_root,
            team_name,
            event_json,
        } => {
            validate_json_string(&event_json)?;
            append_line(&team_event_log_path(&state_root, &team_name)?, &event_json)?;
            Ok(ok_response())
        }
        RuntimeCommand::WriteTeamWorkerIdentity {
            state_root,
            team_name,
            worker_name,
            identity_json,
        } => {
            validate_json_string(&identity_json)?;
            write_atomic_string(
                &worker_dir(&state_root, &team_name, &worker_name)?.join("identity.json"),
                &identity_json,
            )?;
            Ok(ok_response())
        }
        RuntimeCommand::ReadTeamWorkerHeartbeat {
            state_root,
            team_name,
            worker_name,
        } => Ok(
            read_json_value(&worker_dir(&state_root, &team_name, &worker_name)?.join("heartbeat.json"))?
                .unwrap_or(Value::Null),
        ),
        RuntimeCommand::UpdateTeamWorkerHeartbeat {
            state_root,
            team_name,
            worker_name,
            heartbeat_json,
        } => {
            validate_json_string(&heartbeat_json)?;
            write_atomic_string(
                &worker_dir(&state_root, &team_name, &worker_name)?.join("heartbeat.json"),
                &heartbeat_json,
            )?;
            Ok(ok_response())
        }
        RuntimeCommand::ReadTeamWorkerStatus {
            state_root,
            team_name,
            worker_name,
        } => Ok(
            read_json_value(&worker_dir(&state_root, &team_name, &worker_name)?.join("status.json"))?
                .unwrap_or(Value::Null),
        ),
        RuntimeCommand::WriteTeamWorkerStatus {
            state_root,
            team_name,
            worker_name,
            status_json,
        } => {
            validate_json_string(&status_json)?;
            write_atomic_string(
                &worker_dir(&state_root, &team_name, &worker_name)?.join("status.json"),
                &status_json,
            )?;
            Ok(ok_response())
        }
        RuntimeCommand::WriteTeamWorkerInbox {
            state_root,
            team_name,
            worker_name,
            prompt,
        } => {
            write_atomic_string(
                &worker_dir(&state_root, &team_name, &worker_name)?.join("inbox.md"),
                &prompt,
            )?;
            Ok(ok_response())
        }
        RuntimeCommand::WriteTeamShutdownRequest {
            state_root,
            team_name,
            worker_name,
            request_json,
        } => {
            validate_json_string(&request_json)?;
            write_atomic_string(
                &worker_dir(&state_root, &team_name, &worker_name)?.join("shutdown-request.json"),
                &request_json,
            )?;
            Ok(ok_response())
        }
        RuntimeCommand::ReadTeamShutdownAck {
            state_root,
            team_name,
            worker_name,
        } => Ok(
            read_json_value(&worker_dir(&state_root, &team_name, &worker_name)?.join("shutdown-ack.json"))?
                .unwrap_or(Value::Null),
        ),
        RuntimeCommand::ReadTeamPhase {
            state_root,
            team_name,
        } => Ok(read_json_value(&team_dir(&state_root, &team_name)?.join("phase.json"))?
            .unwrap_or(Value::Null)),
        RuntimeCommand::WriteTeamPhase {
            state_root,
            team_name,
            phase_json,
        } => {
            validate_json_string(&phase_json)?;
            write_atomic_string(&team_dir(&state_root, &team_name)?.join("phase.json"), &phase_json)?;
            Ok(ok_response())
        }
        RuntimeCommand::WriteTeamTaskApproval {
            state_root,
            team_name,
            task_id,
            approval_json,
        } => {
            validate_json_string(&approval_json)?;
            write_atomic_string(&approval_path(&state_root, &team_name, &task_id)?, &approval_json)?;
            Ok(ok_response())
        }
        RuntimeCommand::ReadTeamTaskApproval {
            state_root,
            team_name,
            task_id,
        } => Ok(read_json_value(&approval_path(&state_root, &team_name, &task_id)?)?
            .unwrap_or(Value::Null)),
        RuntimeCommand::ReadTeamTask {
            state_root,
            team_name,
            task_id,
        } => Ok(
            read_json_value(&task_path(&state_root, &team_name, &task_id)?)?
                .filter(is_team_task_value)
                .unwrap_or(Value::Null),
        ),
        RuntimeCommand::ListTeamTasks {
            state_root,
            team_name,
        } => Ok(Value::Array(list_tasks(&state_root, &team_name)?)),
        RuntimeCommand::ReadTeamMonitorSnapshot {
            state_root,
            team_name,
        } => Ok(
            read_json_value(&team_dir(&state_root, &team_name)?.join("monitor-snapshot.json"))?
                .unwrap_or(Value::Null),
        ),
        RuntimeCommand::WriteTeamMonitorSnapshot {
            state_root,
            team_name,
            snapshot_json,
        } => {
            validate_json_string(&snapshot_json)?;
            write_atomic_string(
                &team_dir(&state_root, &team_name)?.join("monitor-snapshot.json"),
                &snapshot_json,
            )?;
            Ok(ok_response())
        }
        RuntimeCommand::ReadTeamSummarySnapshot {
            state_root,
            team_name,
        } => Ok(
            read_json_value(&team_dir(&state_root, &team_name)?.join("summary-snapshot.json"))?
                .unwrap_or(Value::Null),
        ),
        RuntimeCommand::WriteTeamSummarySnapshot {
            state_root,
            team_name,
            snapshot_json,
        } => {
            validate_json_string(&snapshot_json)?;
            write_atomic_string(
                &team_dir(&state_root, &team_name)?.join("summary-snapshot.json"),
                &snapshot_json,
            )?;
            Ok(ok_response())
        }
        RuntimeCommand::AcquireTeamScalingLock {
            state_root,
            team_name,
            lock_stale_ms,
        } => {
            let owner_token = acquire_lock(
                &team_dir(&state_root, &team_name)?.join(".lock.scaling"),
                LockAcquireOptions {
                    lock_stale_ms,
                    timeout_ms: SCALING_LOCK_TIMEOUT_MS,
                    retry_ms: 50,
                    create_parent_recursive: true,
                    require_root_exists: false,
                },
            )?
            .ok_or_else(|| TeamStateError::Message(format!("Timed out acquiring scaling lock for team {team_name}")))?;
            Ok(owner_token_response(owner_token))
        }
        RuntimeCommand::ReleaseTeamScalingLock {
            state_root,
            team_name,
            owner_token,
        } => {
            release_lock(&team_dir(&state_root, &team_name)?.join(".lock.scaling"), &owner_token);
            Ok(ok_response())
        }
        RuntimeCommand::AcquireTeamCreateTaskLock {
            state_root,
            team_name,
            lock_stale_ms,
        } => {
            let owner_token = acquire_lock(
                &team_dir(&state_root, &team_name)?.join(".lock.create-task"),
                LockAcquireOptions {
                    lock_stale_ms,
                    timeout_ms: DEFAULT_LOCK_TIMEOUT_MS,
                    retry_ms: LOCK_OWNER_RETRY_MS,
                    create_parent_recursive: false,
                    require_root_exists: false,
                },
            )?
            .ok_or_else(|| TeamStateError::Message(format!("Timed out acquiring team task lock for {team_name}")))?;
            Ok(owner_token_response(owner_token))
        }
        RuntimeCommand::ReleaseTeamCreateTaskLock {
            state_root,
            team_name,
            owner_token,
        } => {
            release_lock(&team_dir(&state_root, &team_name)?.join(".lock.create-task"), &owner_token);
            Ok(ok_response())
        }
        RuntimeCommand::AcquireTeamTaskClaimLock {
            state_root,
            team_name,
            task_id,
            lock_stale_ms,
        } => {
            let owner_token = acquire_lock(
                &task_claim_lock_dir(&state_root, &team_name, &task_id)?,
                LockAcquireOptions {
                    lock_stale_ms,
                    timeout_ms: DEFAULT_LOCK_TIMEOUT_MS,
                    retry_ms: LOCK_OWNER_RETRY_MS,
                    create_parent_recursive: false,
                    require_root_exists: false,
                },
            )?;
            Ok(task_claim_lock_response(owner_token))
        }
        RuntimeCommand::ReleaseTeamTaskClaimLock {
            state_root,
            team_name,
            task_id,
            owner_token,
        } => {
            release_lock(&task_claim_lock_dir(&state_root, &team_name, &task_id)?, &owner_token);
            Ok(ok_response())
        }
        RuntimeCommand::AcquireTeamMailboxLock {
            state_root,
            team_name,
            worker_name,
            lock_stale_ms,
        } => {
            let root = team_dir(&state_root, &team_name)?;
            if !root.exists() {
                return Err(TeamStateError::Message(format!("Team {team_name} not found")));
            }
            let owner_token = acquire_lock(
                &mailbox_lock_dir(&state_root, &team_name, &worker_name)?,
                LockAcquireOptions {
                    lock_stale_ms,
                    timeout_ms: DEFAULT_LOCK_TIMEOUT_MS,
                    retry_ms: LOCK_OWNER_RETRY_MS,
                    create_parent_recursive: true,
                    require_root_exists: true,
                },
            )?
            .ok_or_else(|| TeamStateError::Message(format!("Timed out acquiring mailbox lock for {team_name}/{worker_name}")))?;
            Ok(owner_token_response(owner_token))
        }
        RuntimeCommand::ReleaseTeamMailboxLock {
            state_root,
            team_name,
            worker_name,
            owner_token,
        } => {
            release_lock(&mailbox_lock_dir(&state_root, &team_name, &worker_name)?, &owner_token);
            Ok(ok_response())
        }
        _ => Err(TeamStateError::Message(
            "non team-state command provided to team_state executor".to_string(),
        )),
    }
}

fn ok_response() -> Value {
    serde_json::json!({ "ok": true })
}

fn owner_token_response(owner_token: String) -> Value {
    serde_json::json!({ "owner_token": owner_token })
}

fn task_claim_lock_response(owner_token: Option<String>) -> Value {
    match owner_token {
        Some(owner_token) => serde_json::json!({ "ok": true, "owner_token": owner_token }),
        None => serde_json::json!({ "ok": false }),
    }
}

fn validate_json_string(data: &str) -> Result<(), TeamStateError> {
    let _ = serde_json::from_str::<Value>(data)?;
    Ok(())
}

fn read_json_value(path: &Path) -> Result<Option<Value>, TeamStateError> {
    match fs::read_to_string(path) {
        Ok(raw) => match serde_json::from_str::<Value>(&raw) {
            Ok(value) => Ok(Some(value)),
            Err(_) => Ok(None),
        },
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn write_atomic_string(path: &Path, data: &str) -> Result<(), TeamStateError> {
    let parent = path
        .parent()
        .ok_or_else(|| TeamStateError::Message(format!("path has no parent: {}", path.display())))?;
    fs::create_dir_all(parent)?;

    let temp_path = PathBuf::from(format!(
        "{}.tmp.{}.{}.{}",
        path.display(),
        std::process::id(),
        now_millis(),
        now_nanos()
    ));
    fs::write(&temp_path, data)?;
    match fs::rename(&temp_path, path) {
        Ok(()) => Ok(()),
        Err(error) => {
            if error.kind() == io::ErrorKind::NotFound && path.exists() {
                if let Ok(existing) = fs::read_to_string(path) {
                    if existing == data {
                        return Ok(());
                    }
                }
            }
            Err(error.into())
        }
    }
}

fn append_line(path: &Path, line: &str) -> Result<(), TeamStateError> {
    let parent = path
        .parent()
        .ok_or_else(|| TeamStateError::Message(format!("path has no parent: {}", path.display())))?;
    fs::create_dir_all(parent)?;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(line.as_bytes())?;
    file.write_all(b"\n")?;
    Ok(())
}

fn team_dir(state_root: &str, team_name: &str) -> Result<PathBuf, TeamStateError> {
    validate_team_name(team_name)?;
    Ok(PathBuf::from(state_root).join("team").join(team_name))
}

fn worker_dir(state_root: &str, team_name: &str, worker_name: &str) -> Result<PathBuf, TeamStateError> {
    validate_worker_name(worker_name)?;
    Ok(team_dir(state_root, team_name)?.join("workers").join(worker_name))
}

fn team_event_log_path(state_root: &str, team_name: &str) -> Result<PathBuf, TeamStateError> {
    Ok(team_dir(state_root, team_name)?.join("events").join("events.ndjson"))
}

fn approval_path(state_root: &str, team_name: &str, task_id: &str) -> Result<PathBuf, TeamStateError> {
    validate_task_id(task_id)?;
    Ok(team_dir(state_root, team_name)?.join("approvals").join(format!("task-{task_id}.json")))
}

fn task_path(state_root: &str, team_name: &str, task_id: &str) -> Result<PathBuf, TeamStateError> {
    validate_task_id(task_id)?;
    Ok(team_dir(state_root, team_name)?.join("tasks").join(format!("task-{task_id}.json")))
}

fn task_claim_lock_dir(state_root: &str, team_name: &str, task_id: &str) -> Result<PathBuf, TeamStateError> {
    validate_task_id(task_id)?;
    Ok(team_dir(state_root, team_name)?.join("claims").join(format!("task-{task_id}.lock")))
}

fn mailbox_lock_dir(state_root: &str, team_name: &str, worker_name: &str) -> Result<PathBuf, TeamStateError> {
    validate_worker_name(worker_name)?;
    Ok(team_dir(state_root, team_name)?.join("mailbox").join(format!(".lock-{worker_name}")))
}

fn is_team_task_value(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    let Some(status) = object.get("status").and_then(Value::as_str) else {
        return false;
    };
    matches!(status, "pending" | "blocked" | "in_progress" | "completed" | "failed")
        && object.get("id").and_then(Value::as_str).is_some()
        && object.get("subject").and_then(Value::as_str).is_some()
        && object.get("description").and_then(Value::as_str).is_some()
        && object.get("created_at").and_then(Value::as_str).is_some()
}

fn list_tasks(state_root: &str, team_name: &str) -> Result<Vec<Value>, TeamStateError> {
    let tasks_root = team_dir(state_root, team_name)?.join("tasks");
    if !tasks_root.exists() {
        return Ok(Vec::new());
    }

    let mut entries: Vec<(u128, Value)> = Vec::new();
    for entry in fs::read_dir(tasks_root)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        let Some(raw_id) = parse_task_filename(&file_name) else {
            continue;
        };

        let Some(task_value) = read_json_value(&entry.path())? else {
            continue;
        };
        if !is_team_task_value(&task_value) {
            continue;
        }
        if task_value.get("id").and_then(Value::as_str) != Some(raw_id.as_str()) {
            continue;
        }
        let sort_key = raw_id.parse::<u128>().unwrap_or(u128::MAX);
        entries.push((sort_key, task_value));
    }

    entries.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(entries.into_iter().map(|(_, value)| value).collect())
}

fn parse_task_filename(file_name: &str) -> Option<String> {
    if !file_name.starts_with("task-") || !file_name.ends_with(".json") {
        return None;
    }
    let raw = &file_name[5..file_name.len() - 5];
    if raw.is_empty() || raw.len() > 20 || !raw.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    Some(raw.to_string())
}

fn validate_team_name(team_name: &str) -> Result<(), TeamStateError> {
    validate_name(team_name, 30, "team")
}

fn validate_worker_name(worker_name: &str) -> Result<(), TeamStateError> {
    validate_name(worker_name, 64, "worker")
}

fn validate_name(value: &str, max_len: usize, label: &str) -> Result<(), TeamStateError> {
    if value.is_empty() || value.len() > max_len {
        return Err(TeamStateError::Message(format!("Invalid {label} name: {value}")));
    }
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(TeamStateError::Message(format!("Invalid {label} name: {value}")));
    };
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        return Err(TeamStateError::Message(format!("Invalid {label} name: {value}")));
    }
    if !chars.all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-') {
        return Err(TeamStateError::Message(format!("Invalid {label} name: {value}")));
    }
    Ok(())
}

fn validate_task_id(task_id: &str) -> Result<(), TeamStateError> {
    if task_id.is_empty() || task_id.len() > 20 || !task_id.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(TeamStateError::Message(format!("Invalid task ID: {task_id}")));
    }
    Ok(())
}

struct LockAcquireOptions {
    lock_stale_ms: u64,
    timeout_ms: u64,
    retry_ms: u64,
    create_parent_recursive: bool,
    require_root_exists: bool,
}

fn acquire_lock(lock_dir: &Path, options: LockAcquireOptions) -> Result<Option<String>, TeamStateError> {
    if options.require_root_exists {
        let root = lock_dir
            .parent()
            .and_then(Path::parent)
            .ok_or_else(|| TeamStateError::Message(format!("path has no root: {}", lock_dir.display())))?;
        if !root.exists() {
            return Err(TeamStateError::Message(format!(
                "Team {} not found",
                root.file_name().and_then(|name| name.to_str()).unwrap_or("unknown")
            )));
        }
    }
    if options.create_parent_recursive {
        if let Some(parent) = lock_dir.parent() {
            fs::create_dir_all(parent)?;
        }
    }

    let owner_token = format!("{}.{}.{}", std::process::id(), now_millis(), now_nanos());
    let owner_path = lock_dir.join("owner");
    let deadline = Instant::now() + Duration::from_millis(options.timeout_ms);

    loop {
        match fs::create_dir(lock_dir) {
            Ok(()) => {
                if let Err(error) = fs::write(&owner_path, &owner_token) {
                    let _ = fs::remove_dir_all(lock_dir);
                    return Err(error.into());
                }
                return Ok(Some(owner_token));
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                if maybe_recover_stale_lock(lock_dir, options.lock_stale_ms)? {
                    continue;
                }
                if Instant::now() >= deadline {
                    return Ok(None);
                }
                thread::sleep(Duration::from_millis(options.retry_ms));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn maybe_recover_stale_lock(lock_dir: &Path, lock_stale_ms: u64) -> Result<bool, TeamStateError> {
    let metadata = match fs::metadata(lock_dir) {
        Ok(metadata) => metadata,
        Err(_) => return Ok(false),
    };
    let modified = match metadata.modified() {
        Ok(modified) => modified,
        Err(_) => return Ok(false),
    };
    let age_ms = SystemTime::now()
        .duration_since(modified)
        .unwrap_or_default()
        .as_millis();
    if age_ms > u128::from(lock_stale_ms) {
        fs::remove_dir_all(lock_dir).ok();
        return Ok(true);
    }
    Ok(false)
}

fn release_lock(lock_dir: &Path, owner_token: &str) -> bool {
    let owner_path = lock_dir.join("owner");
    let Ok(current_owner) = fs::read_to_string(owner_path) else {
        return false;
    };
    if current_owner.trim() != owner_token {
        return false;
    }
    fs::remove_dir_all(lock_dir).is_ok()
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}
