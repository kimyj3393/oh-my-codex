use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

const DEFAULT_LIMIT: u32 = 10;
const DEFAULT_CONTEXT: u32 = 80;
const MAX_LIMIT: u32 = 100;
const MAX_CONTEXT: u32 = 400;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SessionSearchOptions {
    pub query: String,
    pub limit: Option<u32>,
    pub session: Option<String>,
    pub since: Option<String>,
    pub project: Option<String>,
    pub context: Option<u32>,
    #[serde(default, rename = "caseSensitive")]
    pub case_sensitive: bool,
    pub cwd: Option<String>,
    pub now: Option<i64>,
    #[serde(rename = "codexHomeDir")]
    pub codex_home_dir: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SessionSearchResult {
    pub session_id: String,
    pub timestamp: Option<String>,
    pub cwd: Option<String>,
    pub transcript_path: String,
    pub transcript_path_relative: String,
    pub record_type: String,
    pub line_number: u32,
    pub snippet: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SessionSearchReport {
    pub query: String,
    pub searched_files: u32,
    pub matched_sessions: u32,
    pub results: Vec<SessionSearchResult>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SessionMeta {
    session_id: String,
    timestamp: Option<String>,
    cwd: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SearchableText {
    text: String,
    record_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SearchExecutionOptions {
    query: String,
    limit: u32,
    context: u32,
    case_sensitive: bool,
    session: Option<String>,
    since_cutoff: Option<i64>,
    project_filter: Option<String>,
    cwd: String,
    codex_home_dir: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchError(pub String);

impl std::fmt::Display for SearchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SearchError {}

fn clamp_integer(value: Option<u32>, fallback: u32, max: u32) -> u32 {
    let candidate = value.unwrap_or(fallback);
    candidate.min(max)
}

fn normalize_project_filter(project: Option<&str>, cwd: &str) -> Option<String> {
    let trimmed = project?.trim();
    if trimmed.is_empty() {
        return None;
    }
    match trimmed {
        "current" => Some(cwd.to_string()),
        "all" => None,
        _ => Some(trimmed.to_string()),
    }
}

pub fn parse_since_spec(value: Option<&str>, now: i64) -> Result<Option<i64>, SearchError> {
    let Some(raw) = value else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    if let Some((amount, unit)) = parse_duration_spec(trimmed) {
        let multiplier = match unit {
            's' => 1_000_i64,
            'm' => 60_000_i64,
            'h' => 3_600_000_i64,
            'd' => 86_400_000_i64,
            'w' => 604_800_000_i64,
            _ => unreachable!(),
        };
        return Ok(Some(now - (amount as i64) * multiplier));
    }

    if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
        return Ok(Some(dt.timestamp_millis()));
    }

    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        let naive = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| SearchError(format_invalid_since(raw)))?;
        let utc = DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc);
        return Ok(Some(utc.timestamp_millis()));
    }

    Err(SearchError(format_invalid_since(raw)))
}

fn format_invalid_since(raw: &str) -> String {
    format!("Invalid --since value \"{raw}\". Use formats like 7d, 24h, or 2026-03-10.")
}

fn parse_duration_spec(input: &str) -> Option<(u64, char)> {
    if input.len() < 2 {
        return None;
    }
    let unit = input.chars().last()?.to_ascii_lowercase();
    if !matches!(unit, 's' | 'm' | 'h' | 'd' | 'w') {
        return None;
    }
    let amount = input[..input.len() - unit.len_utf8()].parse::<u64>().ok()?;
    Some((amount, unit))
}

fn list_rollout_files(root: &Path) -> Vec<PathBuf> {
    if !root.exists() {
        return Vec::new();
    }

    let mut files = Vec::new();
    let mut queue = vec![root.to_path_buf()];

    while let Some(dir) = queue.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                queue.push(path);
                continue;
            }
            if file_type.is_file() {
                let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
                    continue;
                };
                if name.starts_with("rollout-") && name.ends_with(".jsonl") {
                    files.push(path);
                }
            }
        }
    }

    files.sort_by(|a, b| b.to_string_lossy().cmp(&a.to_string_lossy()));
    files
}

fn as_trimmed_string(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(ToOwned::to_owned)
}

fn collect_text_fragments(value: &Value, fragments: &mut Vec<String>) {
    match value {
        Value::String(text) => {
            if !text.trim().is_empty() {
                fragments.push(text.clone());
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_text_fragments(item, fragments);
            }
        }
        Value::Object(map) => {
            for (key, child) in map {
                if key == "base_instructions" || key == "developer_instructions" {
                    continue;
                }
                collect_text_fragments(child, fragments);
            }
        }
        _ => {}
    }
}

fn extract_session_meta(parsed: Option<&Value>) -> Option<SessionMeta> {
    let parsed = parsed?;
    if parsed.get("type").and_then(Value::as_str) != Some("session_meta") {
        return None;
    }
    let payload = parsed.get("payload")?.as_object()?;
    let session_id = as_trimmed_string(payload.get("id")?)?;
    Some(SessionMeta {
        session_id,
        timestamp: payload.get("timestamp").and_then(as_trimmed_string),
        cwd: payload.get("cwd").and_then(as_trimmed_string),
    })
}

fn join_fragments(fragments: &[String]) -> String {
    fragments.join(" \n ").trim().to_string()
}

fn extract_searchable_texts(parsed: Option<&Value>, raw_line: &str) -> Vec<SearchableText> {
    let Some(parsed) = parsed else {
        return vec![SearchableText {
            text: raw_line.to_string(),
            record_type: "raw".to_string(),
        }];
    };

    let top_type = parsed
        .get("type")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .unwrap_or("unknown");

    if top_type == "session_meta" {
        let Some(payload) = parsed.get("payload").and_then(Value::as_object) else {
            return Vec::new();
        };
        let summary = ["id", "cwd", "agent_role", "agent_nickname"]
            .iter()
            .filter_map(|field| payload.get(*field).and_then(as_trimmed_string))
            .collect::<Vec<_>>()
            .join(" ")
            .trim()
            .to_string();
        if summary.is_empty() {
            return Vec::new();
        }
        return vec![SearchableText {
            text: summary,
            record_type: "session_meta".to_string(),
        }];
    }

    if top_type == "event_msg" {
        let payload = parsed.get("payload");
        let payload_type = payload
            .and_then(Value::as_object)
            .and_then(|object| object.get("type"))
            .and_then(as_trimmed_string)
            .unwrap_or_else(|| "unknown".to_string());
        let mut fragments = Vec::new();
        if let Some(payload) = payload {
            collect_text_fragments(payload, &mut fragments);
        }
        let text = join_fragments(&fragments);
        if text.is_empty() {
            return Vec::new();
        }
        return vec![SearchableText {
            text,
            record_type: format!("event_msg:{payload_type}"),
        }];
    }

    if top_type == "response_item" {
        let Some(payload) = parsed.get("payload") else {
            return Vec::new();
        };
        let Some(payload_object) = payload.as_object() else {
            return Vec::new();
        };
        let payload_type = payload_object
            .get("type")
            .and_then(as_trimmed_string)
            .unwrap_or_else(|| "unknown".to_string());

        if payload_type == "message" {
            let role = payload_object
                .get("role")
                .and_then(as_trimmed_string)
                .unwrap_or_else(|| "unknown".to_string());
            if role != "assistant" && role != "user" {
                return Vec::new();
            }
            let mut fragments = Vec::new();
            if let Some(content) = payload_object.get("content") {
                collect_text_fragments(content, &mut fragments);
            }
            let text = join_fragments(&fragments);
            if text.is_empty() {
                return Vec::new();
            }
            return vec![SearchableText {
                text,
                record_type: format!("response_item:{payload_type}:{role}"),
            }];
        }

        let mut fragments = Vec::new();
        match payload_type.as_str() {
            "function_call" => {
                if let Some(name) = payload_object.get("name").and_then(as_trimmed_string) {
                    fragments.push(name);
                }
                if let Some(arguments_text) =
                    payload_object.get("arguments").and_then(as_trimmed_string)
                {
                    fragments.push(arguments_text);
                }
            }
            "function_call_output" => {
                if let Some(output) = payload_object.get("output") {
                    collect_text_fragments(output, &mut fragments);
                }
            }
            _ => collect_text_fragments(payload, &mut fragments),
        }

        let text = join_fragments(&fragments);
        if text.is_empty() {
            return Vec::new();
        }
        return vec![SearchableText {
            text,
            record_type: format!("response_item:{payload_type}"),
        }];
    }

    vec![SearchableText {
        text: raw_line.to_string(),
        record_type: top_type.to_string(),
    }]
}

fn collapse_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn build_snippet(text: &str, query: &str, context: u32, case_sensitive: bool) -> Option<String> {
    if text.is_empty() {
        return None;
    }

    let (haystack, needle) = if case_sensitive {
        (text.to_string(), query.to_string())
    } else {
        (text.to_lowercase(), query.to_lowercase())
    };

    let index = haystack.find(&needle)?;
    let start = index.saturating_sub(context as usize);
    let end = (index + query.len() + context as usize).min(text.len());
    let prefix = if start > 0 { "…" } else { "" };
    let suffix = if end < text.len() { "…" } else { "" };
    let slice = text.get(start..end)?;
    Some(format!(
        "{prefix}{}{suffix}",
        collapse_whitespace(slice).trim()
    ))
}

fn matches_filter(value: Option<&str>, filter: Option<&str>, case_sensitive: bool) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    let Some(value) = value else {
        return false;
    };

    let matches = if case_sensitive {
        value.contains(filter)
    } else {
        value.to_lowercase().contains(&filter.to_lowercase())
    };
    if matches {
        return true;
    }

    if looks_like_absolute_path(value) && looks_like_absolute_path(filter) {
        let normalized_value = normalize_comparable_path(value);
        let normalized_filter = normalize_comparable_path(filter);
        if case_sensitive {
            return normalized_value.contains(&normalized_filter);
        }
        return normalized_value
            .to_lowercase()
            .contains(&normalized_filter.to_lowercase());
    }

    false
}

fn looks_like_absolute_path(value: &str) -> bool {
    value.starts_with('/')
        || value
            .as_bytes()
            .get(1)
            .zip(value.as_bytes().get(2))
            .map(|(colon, slash)| *colon == b':' && (*slash == b'/' || *slash == b'\\'))
            .unwrap_or(false)
}

fn normalize_comparable_path(value: &str) -> String {
    fs::canonicalize(value)
        .unwrap_or_else(|_| PathBuf::from(value))
        .to_string_lossy()
        .replace('\\', "/")
}

fn session_timestamp_millis(timestamp: Option<&str>) -> Option<i64> {
    let timestamp = timestamp?;
    DateTime::parse_from_rfc3339(timestamp)
        .map(|value| value.timestamp_millis())
        .ok()
}

fn relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .map(|relative| relative.to_string_lossy().replace('\\', "/"))
        .unwrap_or_else(|_| path.to_string_lossy().replace('\\', "/"))
}

fn fallback_session_id_from_path(file_path: &Path) -> String {
    let file_name = file_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    if let Some(suffix) = file_name.strip_prefix("rollout-") {
        return suffix.trim_end_matches(".jsonl").to_string();
    }
    file_path.to_string_lossy().to_string()
}

fn search_rollout_file(
    file_path: &Path,
    options: &SearchExecutionOptions,
) -> Result<Vec<SessionSearchResult>, SearchError> {
    let file = File::open(file_path)
        .map_err(|error| SearchError(format!("failed to read {}: {error}", file_path.display())))?;
    let reader = BufReader::new(file);
    let mut results = Vec::new();
    let mut meta: Option<SessionMeta> = None;
    let mut line_number = 0_u32;
    let mut skip_file = false;

    for line_result in reader.lines() {
        let line = line_result.map_err(|error| {
            SearchError(format!(
                "failed to read line from {}: {error}",
                file_path.display()
            ))
        })?;
        line_number += 1;
        let parsed_value = serde_json::from_str::<Value>(&line).ok();

        if line_number == 1 {
            meta = extract_session_meta(parsed_value.as_ref()).or_else(|| {
                Some(SessionMeta {
                    session_id: fallback_session_id_from_path(file_path),
                    timestamp: None,
                    cwd: None,
                })
            });

            if let Some(meta_ref) = meta.as_ref() {
                if let Some(since_cutoff) = options.since_cutoff {
                    if let Some(timestamp) = session_timestamp_millis(meta_ref.timestamp.as_deref())
                    {
                        if timestamp < since_cutoff {
                            skip_file = true;
                            break;
                        }
                    }
                }

                if !matches_filter(
                    Some(meta_ref.session_id.as_str()),
                    options.session.as_deref(),
                    options.case_sensitive,
                ) {
                    skip_file = true;
                    break;
                }

                if !matches_filter(
                    meta_ref.cwd.as_deref(),
                    options.project_filter.as_deref(),
                    options.case_sensitive,
                ) {
                    skip_file = true;
                    break;
                }
            }
        }

        for candidate in extract_searchable_texts(parsed_value.as_ref(), &line) {
            let Some(meta_ref) = meta.as_ref() else {
                continue;
            };
            let Some(snippet) = build_snippet(
                &candidate.text,
                &options.query,
                options.context,
                options.case_sensitive,
            ) else {
                continue;
            };

            results.push(SessionSearchResult {
                session_id: meta_ref.session_id.clone(),
                timestamp: meta_ref.timestamp.clone(),
                cwd: meta_ref.cwd.clone(),
                transcript_path: file_path.to_string_lossy().replace('\\', "/"),
                transcript_path_relative: relative_path(
                    Path::new(&options.codex_home_dir),
                    file_path,
                ),
                record_type: candidate.record_type,
                line_number,
                snippet,
            });

            if results.len() >= options.limit as usize {
                return Ok(results);
            }
        }
    }

    if skip_file {
        return Ok(Vec::new());
    }

    Ok(results)
}

pub fn search_session_history(
    options: SessionSearchOptions,
) -> Result<SessionSearchReport, SearchError> {
    let query = options.query.trim().to_string();
    if query.is_empty() {
        return Err(SearchError("Search query must not be empty.".to_string()));
    }

    let cwd = options.cwd.clone().unwrap_or_else(|| {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .to_string_lossy()
            .to_string()
    });
    let codex_home_dir = options.codex_home_dir.clone().unwrap_or_else(|| {
        std::env::var("CODEX_HOME").unwrap_or_else(|_| {
            let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
            format!("{home}/.codex")
        })
    });
    let now = options.now.unwrap_or_else(current_time_millis);
    let limit = clamp_integer(options.limit, DEFAULT_LIMIT, MAX_LIMIT).max(1);
    let context = clamp_integer(options.context, DEFAULT_CONTEXT, MAX_CONTEXT);
    let since_cutoff = parse_since_spec(options.since.as_deref(), now)?;
    let project_filter = normalize_project_filter(options.project.as_deref(), &cwd);
    let rollout_root = Path::new(&codex_home_dir).join("sessions");
    let files = list_rollout_files(&rollout_root);

    let exec_options = SearchExecutionOptions {
        query: query.clone(),
        limit,
        context,
        case_sensitive: options.case_sensitive,
        session: options.session.clone(),
        since_cutoff,
        project_filter,
        cwd,
        codex_home_dir,
    };

    let mut results = Vec::new();
    let mut searched_files = 0_u32;
    let mut matched_sessions = HashSet::new();

    for file_path in files {
        if results.len() >= limit as usize {
            break;
        }

        if let Some(since_cutoff) = since_cutoff {
            if let Ok(metadata) = fs::metadata(&file_path) {
                if let Ok(modified) = metadata.modified() {
                    if let Ok(duration) = modified.duration_since(UNIX_EPOCH) {
                        if (duration.as_millis() as i64) < since_cutoff {
                            continue;
                        }
                    }
                }
            } else {
                continue;
            }
        }

        searched_files += 1;
        let mut file_results = search_rollout_file(&file_path, &exec_options)?;
        let remaining = limit as usize - results.len();
        if file_results.len() > remaining {
            file_results.truncate(remaining);
        }

        for result in file_results {
            matched_sessions.insert(result.session_id.clone());
            results.push(result);
            if results.len() >= limit as usize {
                break;
            }
        }
    }

    Ok(SessionSearchReport {
        query,
        searched_files,
        matched_sessions: matched_sessions.len() as u32,
        results,
    })
}

fn current_time_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{parse_since_spec, search_session_history, SessionSearchOptions};
    use std::fs::{create_dir_all, write};
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("omx-session-search-{name}-{nanos}"))
    }

    fn write_rollout(
        codex_home: &Path,
        iso_date: &str,
        file_name: &str,
        lines: &[&str],
    ) -> PathBuf {
        let year = &iso_date[0..4];
        let month = &iso_date[5..7];
        let day = &iso_date[8..10];
        let dir = codex_home.join("sessions").join(year).join(month).join(day);
        create_dir_all(&dir).unwrap();
        let file_path = dir.join(file_name);
        write(&file_path, format!("{}\n", lines.join("\n"))).unwrap();
        file_path
    }

    #[test]
    fn parses_duration_and_date_since_specs() {
        let now = 1_710_000_000_000_i64;
        assert_eq!(
            parse_since_spec(Some("24h"), now).unwrap(),
            Some(now - 24 * 3_600_000)
        );
        assert_eq!(
            parse_since_spec(Some("2026-03-09"), now).unwrap(),
            Some(
                chrono::NaiveDate::from_ymd_opt(2026, 3, 9)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc()
                    .timestamp_millis()
            )
        );
    }

    #[test]
    fn finds_matches_in_rollout_transcripts() {
        let cwd = temp_dir("smoke");
        let codex_home = cwd.join(".codex-home");
        let file_name = "rollout-2026-03-10T12-00-00-session-a.jsonl";
        write_rollout(
            &codex_home,
            "2026-03-10T12:00:00.000Z",
            file_name,
            &[
                r#"{"type":"session_meta","payload":{"id":"session-a","timestamp":"2026-03-10T12:00:00.000Z","cwd":"/tmp/project-a"}}"#,
                r#"{"type":"event_msg","payload":{"type":"user_message","message":"Please investigate the worker inbox path issue in team mode."}}"#,
            ],
        );

        let report = search_session_history(SessionSearchOptions {
            query: "worker inbox path".to_string(),
            limit: None,
            session: None,
            since: None,
            project: None,
            context: None,
            case_sensitive: false,
            cwd: Some(cwd.to_string_lossy().to_string()),
            now: None,
            codex_home_dir: Some(codex_home.to_string_lossy().to_string()),
        })
        .unwrap();

        assert_eq!(report.results.len(), 1);
        assert_eq!(report.matched_sessions, 1);
        assert_eq!(report.results[0].session_id, "session-a");
        assert_eq!(report.results[0].record_type, "event_msg:user_message");
        assert!(report.results[0]
            .snippet
            .contains("worker inbox path issue"));
        assert_eq!(
            report.results[0].transcript_path_relative,
            format!("sessions/2026/03/10/{file_name}")
        );
    }

    #[test]
    fn supports_project_session_and_since_filters() {
        let cwd = temp_dir("filters");
        let codex_home = cwd.join(".codex-home");
        write_rollout(
            &codex_home,
            "2026-03-10T12:00:00.000Z",
            "rollout-2026-03-10T12-00-00-session-a.jsonl",
            &[
                r#"{"type":"session_meta","payload":{"id":"session-a","timestamp":"2026-03-10T12:00:00.000Z","cwd":"/repo/current"}}"#,
                r#"{"type":"response_item","payload":{"type":"function_call_output","output":"all_workers_idle fired after startup evidence was missing"}}"#,
            ],
        );
        write_rollout(
            &codex_home,
            "2026-03-09T12:00:00.000Z",
            "rollout-2026-03-09T12-00-00-session-b.jsonl",
            &[
                r#"{"type":"session_meta","payload":{"id":"session-b","timestamp":"2026-03-09T12:00:00.000Z","cwd":"/repo/other"}}"#,
                r#"{"type":"event_msg","payload":{"type":"agent_message","message":"all_workers_idle should remain searchable in older sessions too"}}"#,
            ],
        );

        let project_report = search_session_history(SessionSearchOptions {
            query: "all_workers_idle".to_string(),
            limit: Some(1),
            session: Some("session-a".to_string()),
            since: None,
            project: Some("/repo/current".to_string()),
            context: None,
            case_sensitive: false,
            cwd: Some(cwd.to_string_lossy().to_string()),
            now: None,
            codex_home_dir: Some(codex_home.to_string_lossy().to_string()),
        })
        .unwrap();
        assert_eq!(project_report.results.len(), 1);
        assert_eq!(project_report.results[0].session_id, "session-a");

        let since_report = search_session_history(SessionSearchOptions {
            query: "all_workers_idle".to_string(),
            limit: None,
            session: None,
            since: Some("12h".to_string()),
            project: None,
            context: None,
            case_sensitive: false,
            cwd: Some(cwd.to_string_lossy().to_string()),
            now: Some(
                chrono::DateTime::parse_from_rfc3339("2026-03-10T18:00:00.000Z")
                    .unwrap()
                    .timestamp_millis(),
            ),
            codex_home_dir: Some(codex_home.to_string_lossy().to_string()),
        })
        .unwrap();
        assert_eq!(since_report.results.len(), 1);
        assert_eq!(since_report.results[0].session_id, "session-a");
    }
}
