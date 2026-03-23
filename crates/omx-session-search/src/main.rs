use omx_session_search::{search_session_history, SearchError, SessionSearchOptions};

fn usage() -> &'static str {
    "usage: omx-session-search --input <json-options>\n"
}

fn main() {
    if let Err(error) = run() {
        eprintln!("omx-session-search: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), SearchError> {
    let mut args = std::env::args().skip(1);
    let Some(flag) = args.next() else {
        return Err(SearchError(usage().trim_end().to_string()));
    };

    if flag == "--help" || flag == "-h" {
        print!("{}", usage());
        return Ok(());
    }

    if flag != "--input" {
        return Err(SearchError(usage().trim_end().to_string()));
    }

    let Some(input) = args.next() else {
        return Err(SearchError(
            "missing JSON payload after --input".to_string(),
        ));
    };
    if args.next().is_some() {
        return Err(SearchError("unexpected extra arguments".to_string()));
    }

    let options: SessionSearchOptions = serde_json::from_str(&input)
        .map_err(|error| SearchError(format!("invalid JSON options: {error}")))?;
    let report = search_session_history(options)?;
    println!(
        "{}",
        serde_json::to_string(&report)
            .map_err(|error| SearchError(format!("failed to encode search report: {error}")))?
    );
    Ok(())
}
