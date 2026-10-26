use std::env;
use std::io::{self, Write};
use tinydb::{Db, DbOptions, SyncMode};

fn main() {
    if let Err(error) = run() {
        eprintln!("tinydb: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let mut sync_mode = SyncMode::EveryWrite;
    let mut path = None;
    while let Some(arg) = args.next() {
        if arg == "--sync" {
            sync_mode = match args.next().as_deref() {
                Some("always") => SyncMode::EveryWrite,
                Some("manual") => SyncMode::Manual,
                _ => return Err("usage: tinydb [--sync always|manual] [database-file]".into()),
            };
        } else if arg == "--help" || arg == "-h" {
            println!("usage: tinydb [--sync always|manual] [database-file]");
            return Ok(());
        } else if arg.starts_with('-') || path.is_some() {
            return Err("usage: tinydb [--sync always|manual] [database-file]".into());
        } else {
            path = Some(arg);
        }
    }
    let mut db = Db::open_with_options(path.unwrap_or_else(|| "tinydb.data".to_owned()), DbOptions { sync_mode })?;
    println!("tinydb at {} (sync: {}). Type 'help' for commands.", db.path().display(), if sync_mode == SyncMode::EveryWrite { "always" } else { "manual" });
    let stdin = io::stdin();
    let mut line = String::new();
    loop {
        print!("> ");
        io::stdout().flush()?;
        line.clear();
        if stdin.read_line(&mut line)? == 0 { break; }
        let parts = match tokenize(line.trim_end()) {
            Ok(parts) => parts,
            Err(message) => { println!("error: {message}"); continue; }
        };
        if parts.is_empty() { continue; }
        match parts[0].as_str() {
            "set" if parts.len() >= 3 => {
                let value = parts[2..].join(" ");
                db.set(&parts[1], value.as_bytes())?;
                println!("OK");
            }
            "get" if parts.len() == 2 => match db.get(&parts[1]) {
                Some(value) => println!("{}", String::from_utf8_lossy(value)),
                None => println!("(not found)"),
            },
            "delete" if parts.len() == 2 => println!("{}", if db.delete(&parts[1])? { "OK" } else { "(not found)" }),
            "keys" if parts.len() == 1 => {
                let mut keys: Vec<_> = db.iter().map(|(key, _)| String::from_utf8_lossy(key).into_owned()).collect();
                keys.sort();
                for key in keys { println!("{key}"); }
            }
            "stats" if parts.len() == 1 => {
                let stats = db.stats()?;
                println!("keys: {} | key bytes: {} | value bytes: {} | journal bytes: {}", stats.key_count, stats.key_bytes, stats.value_bytes, stats.journal_bytes);
            }
            "compact" if parts.len() == 1 => { db.compact()?; println!("OK"); }
            "flush" if parts.len() == 1 => { db.flush()?; println!("OK"); }
            "help" if parts.len() == 1 => println!("set KEY VALUE | get KEY | delete KEY | keys | stats | compact | flush | help | quit"),
            "quit" | "exit" if parts.len() == 1 => break,
            "set" => println!("usage: set KEY VALUE"),
            "get" => println!("usage: get KEY"),
            "delete" => println!("usage: delete KEY"),
            _ => println!("unknown command or unexpected arguments; type 'help'"),
        }
    }
    if sync_mode == SyncMode::Manual { db.flush()?; }
    Ok(())
}

// Supports single or double quotes and backslash escaping. Unquoted words are
// split on whitespace; `set key several words` joins the value words.
fn tokenize(input: &str) -> Result<Vec<String>, &'static str> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut escaped = false;
    let mut started = false;
    for ch in input.chars() {
        if escaped { current.push(ch); escaped = false; started = true; continue; }
        if ch == '\\' { escaped = true; started = true; continue; }
        if let Some(delimiter) = quote {
            if ch == delimiter { quote = None; } else { current.push(ch); }
            started = true;
        } else if ch == '"' || ch == '\'' {
            quote = Some(ch);
            started = true;
        } else if ch.is_whitespace() {
            if started { words.push(std::mem::take(&mut current)); started = false; }
        } else {
            current.push(ch);
            started = true;
        }
    }
    if escaped { return Err("trailing escape"); }
    if quote.is_some() { return Err("unclosed quote"); }
    if started { words.push(current); }
    Ok(words)
}
