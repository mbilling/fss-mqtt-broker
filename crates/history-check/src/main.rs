//! `history-check <file.jsonl>...` — verify recorded client-visible histories
//! (issue #231). Exit 0 = every promise held in every file; exit 1 = violations
//! (each printed with its evidence); exit 2 = a file could not be read or parsed.

use std::process::ExitCode;

fn main() -> ExitCode {
    let files: Vec<String> = std::env::args().skip(1).collect();
    if files.is_empty() {
        eprintln!("usage: history-check <history.jsonl>...");
        return ExitCode::from(2);
    }
    let mut violations = 0usize;
    for file in &files {
        let text = match std::fs::read_to_string(file) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("{file}: unreadable: {e}");
                return ExitCode::from(2);
            }
        };
        let events = match history_check::parse(&text) {
            Ok(ev) => ev,
            Err(e) => {
                eprintln!("{file}: {e}");
                return ExitCode::from(2);
            }
        };
        let found = history_check::check(&events);
        for v in &found {
            eprintln!("{file}: {v}");
        }
        if found.is_empty() {
            println!("{file}: OK ({} events, all promises held)", events.len());
        }
        violations += found.len();
    }
    if violations == 0 {
        ExitCode::SUCCESS
    } else {
        eprintln!("{violations} violation(s)");
        ExitCode::from(1)
    }
}
