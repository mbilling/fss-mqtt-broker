//! The rule documentation's examples, executed. Every rules snippet in `docs/RULES.md` and
//! `docs/RULES-COOKBOOK.md` loads, and every documented `mqttd --rule-test` and
//! `mqttd --check-rules` command that is shown with its output prints exactly that output
//! when the real binary runs it.
//!
//! The documents' format is the contract. The extraction fails loudly, with the file and
//! line, on a snippet or command it recognises but cannot run, and on output it cannot
//! tell apart from the blocks around it.
//!
//! **Rules snippets** must load with `RuleSet::parse`, the loader `--check-rules`, startup
//! and a reload all use:
//!
//! - a ```` ```toml ```` block (the info string in any case) with a `[rules.<id>]` table,
//!   or a shell heredoc that writes one (`cat > rules.toml <<'EOF'`,
//!   `cat <<'EOF' > rules.toml`, `tee rules.toml <<'EOF'`, or any other heredoc in a block
//!   of commands): a rules file. A header whose id is a `<placeholder>` is a schema sketch
//!   and is not loaded;
//! - a ```` ```toml ```` block without a `[rules.<id>]` header whose top-level keys (those
//!   before any table header) include a rule's (`sql`, `actions`, `enable`,
//!   `description`): one rule's body, loaded as `[rules.doc_example]`, with a placeholder
//!   `sql` when it shows none;
//! - a ```` ```toml ```` block of `{ function = … }` inline tables: one rule's actions.
//!
//! A ```` ```toml ```` block that is none of these (broker configuration) is not a rules
//! snippet. An example of a file that must *not* load belongs in a block of another
//! language.
//!
//! **Commands.** A command this test runs is `mqttd` with `--rule-test` or `--check-rules`
//! among its arguments, after any `NAME=value` environment assignments, with no pipes,
//! redirections, expansions or command lists. Such a command written with them fails,
//! naming its line. Commands are shown in either of two forms:
//!
//! - a fenced block, of any language, with `$ ` prompts. Each `$ ` line starts a command
//!   (a trailing `\` or an open quote continues it onto the next line), and the lines up
//!   to the next `$ ` line are its output. When the block shows output and one of its
//!   commands is one this test runs, every one must be: the others (`cd`, `export`) would
//!   change what it prints;
//! - a block of commands without prompts: a ```` ```sh ```` (`bash`, `shell`, `zsh`)
//!   block, or a block of any language whose first line is a command this test runs.
//!
//! A block whose commands are all ones this test runs, and that shows no output itself,
//! takes its output from the next block when that is an output block (```` ```text ````,
//! ```` ```output ````, ```` ```json ````, ```` ```console ```` without prompts, or
//! unlabelled) with at most one paragraph of prose between and no heading. That block
//! holds the outputs of its commands, in order. A block of another language directly
//! after it, or an output block further on in the same section, fails as ambiguous: a
//! reader would take it for the output.
//!
//! A command's output is what it prints on stdout, or, when it fails, on stderr; the whole
//! terminal view (stderr, then stdout) is accepted too, so a document may show or leave out
//! `--rule-test`'s `(a sample … event)` note. Trailing blank lines are not compared. Its
//! exit status must be the one its output implies: 1 for `rule test FAILED` or
//! `rules INVALID`, 2 for a usage error (`error:`, `mqttd:`), otherwise 0.
//!
//! A command shown without output must still exit 0. That is every `--rule-test`, and, in
//! a block of only such commands, a `--check-rules` that names a file an earlier heredoc
//! writes or the repository holds. A block that runs other commands too (building, `cd`,
//! writing files) has only its `--rule-test` commands run, since the files it checks are
//! the ones its other commands make.
//!
//! Commands run with no `MQTTD_*` from the environment and `TZ=UTC`, from the repository
//! root (a path such as `docs/examples/rules/02-threshold-alert.toml` resolves as written),
//! or, when a command names a file a heredoc earlier in the same document writes, from a
//! scratch directory holding the files the document's heredocs have written by that line:
//! a later heredoc to the same name replaces the file, and `>>` or `tee -a` appends to it.
//!
//! `docs/RULES.md` must exist; `docs/RULES-COOKBOOK.md` is checked when it exists. Each
//! document checked must yield at least one rules snippet and one command shown with its
//! output, so an extraction that stopped recognising the documents' format fails instead
//! of passing on nothing.

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// The documents checked, and whether each must exist.
const DOCS: &[(&str, bool)] = &[("docs/RULES.md", true), ("docs/RULES-COOKBOOK.md", false)];

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the repository root")
}

/// The documents present, with their text. A required one that is missing fails.
fn docs() -> Vec<(&'static str, String)> {
    let mut out = Vec::new();
    for &(rel, required) in DOCS {
        let path = repo_root().join(rel);
        if path.exists() {
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{rel} is not readable: {e}"));
            out.push((rel, text));
        } else {
            assert!(!required, "{rel} does not exist");
        }
    }
    out
}

/// `lines` without its trailing blank lines.
fn trimmed(mut lines: Vec<String>) -> Vec<String> {
    while lines.last().is_some_and(|l| l.trim().is_empty()) {
        lines.pop();
    }
    lines
}

// ---------------------------------------------------------------------------------------
// Markdown: fenced blocks.
// ---------------------------------------------------------------------------------------

/// One fenced code block.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Block {
    /// The info string's first word, lower-cased (`toml`, `sh`, `text`, `console`, or
    /// empty).
    lang: String,
    /// The 1-based line of the opening fence.
    open: usize,
    /// The 1-based line of the closing fence.
    close: usize,
    /// The content, with the fence's own indentation removed (a block inside a list item
    /// is indented).
    lines: Vec<String>,
}

impl Block {
    /// The 1-based line of the block's first content line.
    fn line(&self) -> usize {
        self.open + 1
    }

    /// The content lines, each with its 1-based line.
    fn numbered(&self) -> Vec<(usize, String)> {
        self.lines
            .iter()
            .enumerate()
            .map(|(k, l)| (self.line() + k, l.clone()))
            .collect()
    }

    /// Whether a line starts with a `$ ` prompt.
    fn prompted(&self) -> bool {
        self.lines.iter().any(|l| l.starts_with("$ "))
    }
}

/// Every fenced block of `text`, in order. An unclosed fence fails, naming its line.
fn blocks(doc: &str, text: &str) -> Vec<Block> {
    let lines: Vec<&str> = text.lines().collect();
    let mut out: Vec<Block> = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let trimmed = lines[i].trim_start();
        let indent = lines[i].len() - trimmed.len();
        let fence_char = trimmed.chars().next().filter(|c| *c == '`' || *c == '~');
        let run = fence_char.map_or(0, |c| trimmed.chars().take_while(|x| *x == c).count());
        let (Some(fc), true) = (fence_char, run >= 3) else {
            i += 1;
            continue;
        };
        let lang = trimmed[run..]
            .split_whitespace()
            .next()
            .unwrap_or("")
            .to_lowercase();
        let open = i;
        let mut content = Vec::new();
        i += 1;
        loop {
            assert!(
                i < lines.len(),
                "{doc}:{}: a fenced block that is never closed",
                open + 1
            );
            let t = lines[i].trim_start();
            if t.chars().take_while(|x| *x == fc).count() >= run
                && t.trim_start_matches(fc).trim().is_empty()
            {
                break;
            }
            let l = lines[i];
            let strip = l.len() - l.trim_start().len();
            content.push(l[strip.min(indent)..].to_string());
            i += 1;
        }
        out.push(Block {
            lang,
            open: open + 1,
            close: i + 1,
            lines: content,
        });
        i += 1;
    }
    out
}

// ---------------------------------------------------------------------------------------
// Shell commands.
// ---------------------------------------------------------------------------------------

/// One shell command, split into words.
#[derive(Debug, Default)]
struct Parsed {
    words: Vec<String>,
    /// How many words were complete before the first piece of shell syntax this test does
    /// not run.
    clean: usize,
    /// A quote, or a trailing `\`, is still open: the command goes on to the next line.
    open: bool,
    /// The first piece of shell syntax this test does not run, if any.
    unsupported: Option<String>,
}

impl Parsed {
    fn end_word(&mut self, word: &mut String, in_word: &mut bool) {
        if *in_word {
            self.words.push(std::mem::take(word));
            *in_word = false;
        }
    }

    fn refuse(&mut self, why: String) {
        if self.unsupported.is_none() {
            self.unsupported = Some(why);
            self.clean = self.words.len();
        }
    }
}

/// Split one shell command into words, as `sh` would: `'…'`, `"…"` (with `\"`, `\\`,
/// `\$` and `` \` ``), `$'…'` (with `\n`, `\t`, `\r`, `\\`, `\'`, `\"` and `\xHH`),
/// backslash escapes and line continuations, and `#` comments. Expansions, pipes,
/// redirections and command lists are recorded as `unsupported` (the test runs `mqttd`,
/// not a shell) and split words like a blank.
fn shell_words(cmd: &str) -> Parsed {
    let mut w = Parsed::default();
    let mut word = String::new();
    let mut in_word = false;
    let mut chars = cmd.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            ' ' | '\t' | '\n' => w.end_word(&mut word, &mut in_word),
            '#' if !in_word => {
                for c in chars.by_ref() {
                    if c == '\n' {
                        break;
                    }
                }
            }
            '\'' => {
                in_word = true;
                loop {
                    match chars.next() {
                        Some('\'') => break,
                        Some(ch) => word.push(ch),
                        None => {
                            w.open = true;
                            break;
                        }
                    }
                }
            }
            '"' => {
                in_word = true;
                loop {
                    match chars.next() {
                        Some('"') => break,
                        Some('\\') => match chars.next() {
                            Some(ch @ ('"' | '\\' | '$' | '`')) => word.push(ch),
                            Some('\n') => {}
                            Some(ch) => {
                                word.push('\\');
                                word.push(ch);
                            }
                            None => {
                                w.open = true;
                                break;
                            }
                        },
                        Some(ch @ ('$' | '`')) => {
                            w.refuse(format!("`{ch}` expands inside double quotes"));
                            word.push(ch);
                        }
                        Some(ch) => word.push(ch),
                        None => {
                            w.open = true;
                            break;
                        }
                    }
                }
            }
            '$' if chars.peek() == Some(&'\'') => {
                chars.next();
                in_word = true;
                ansi_c_quoted(&mut chars, &mut word, &mut w);
            }
            '\\' => match chars.next() {
                Some('\n') => {}
                Some(ch) => {
                    in_word = true;
                    word.push(ch);
                }
                None => w.open = true,
            },
            '$' | '`' | '|' | '&' | ';' | '<' | '>' | '(' | ')' => {
                w.end_word(&mut word, &mut in_word);
                w.refuse(format!("`{c}` is shell syntax this test does not run"));
            }
            ch => {
                in_word = true;
                word.push(ch);
            }
        }
    }
    w.end_word(&mut word, &mut in_word);
    if w.unsupported.is_none() {
        w.clean = w.words.len();
    }
    w
}

/// The rest of a `$'…'` word, after its opening quote.
fn ansi_c_quoted(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    word: &mut String,
    w: &mut Parsed,
) {
    loop {
        match chars.next() {
            Some('\'') => return,
            Some('\\') => match chars.next() {
                Some('n') => word.push('\n'),
                Some('t') => word.push('\t'),
                Some('r') => word.push('\r'),
                Some(ch @ ('\\' | '\'' | '"')) => word.push(ch),
                Some('x') => {
                    let mut hex = String::new();
                    while hex.len() < 2 && chars.peek().is_some_and(char::is_ascii_hexdigit) {
                        hex.extend(chars.next());
                    }
                    match u32::from_str_radix(&hex, 16) {
                        Ok(code) => word.extend(char::from_u32(code)),
                        Err(_) => w.refuse("`\\x` without hex digits".to_string()),
                    }
                }
                Some(ch) => w.refuse(format!("`\\{ch}` in $'…'")),
                None => {
                    w.open = true;
                    return;
                }
            },
            Some(ch) => word.push(ch),
            None => {
                w.open = true;
                return;
            }
        }
    }
}

/// One command of a block: its first line and its text, continuation lines joined with
/// `\n`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Logical {
    line: usize,
    text: String,
}

/// A file a shell heredoc writes, or (with no `file`) a heredoc fed to some command.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Heredoc {
    /// The file it writes, and whether it appends to it.
    file: Option<(String, bool)>,
    /// The 1-based line of its first body line.
    line: usize,
    body: String,
}

/// The byte offset of `pat` in a shell command, outside quotes and comments.
fn unquoted_find(cmd: &str, pat: &str) -> Option<usize> {
    let mut quote: Option<char> = None;
    let mut escaped = false;
    for (i, c) in cmd.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match (quote, c) {
            (Some('\''), '\'') | (Some('"'), '"') => quote = None,
            (Some('"') | None, '\\') => escaped = true,
            (None, '\'' | '"') => quote = Some(c),
            (None, '#') if cmd[..i].ends_with(char::is_whitespace) || i == 0 => return None,
            (None, _) if cmd[i..].starts_with(pat) => return Some(i),
            _ => {}
        }
    }
    None
}

/// The delimiter of the heredoc a command starts, and the file it writes (with whether
/// it appends): `cat`'s standard output redirection, or `tee`'s first file argument.
fn heredoc_start(cmd: &str) -> Option<(String, Option<(String, bool)>)> {
    let pos = unquoted_find(cmd, "<<")?;
    let after = &cmd[pos + 2..];
    if after.starts_with('<') {
        return None;
    }
    let after = after.strip_prefix('-').unwrap_or(after).trim_start();
    let (delim, rest) = if let q @ ('\'' | '"') = after.chars().next()? {
        let quoted = &after[1..];
        let end = quoted.find(q)?;
        (&quoted[..end], &quoted[end + 1..])
    } else {
        let end = after
            .find(|c: char| c.is_whitespace() || matches!(c, '>' | ';' | '|' | '&' | ')'))
            .unwrap_or(after.len());
        (after[..end].trim_start_matches('\\'), &after[end..])
    };
    if delim.is_empty() {
        return None;
    }
    let file = heredoc_file(&format!("{} {rest}", &cmd[..pos]));
    Some((delim.to_string(), file))
}

/// The file a `cat` or `tee` command writes, and whether it appends.
fn heredoc_file(cmd: &str) -> Option<(String, bool)> {
    let words: Vec<&str> = cmd.split_whitespace().collect();
    let (&prog, args) = words.split_first()?;
    let unquote = |w: &str| w.trim_matches(['\'', '"']).to_string();
    let mut redirect = None;
    let mut tee_file = None;
    let mut tee_append = false;
    let mut i = 0;
    while i < args.len() {
        let w = args[i];
        let fd = &w[..w.len() - w.trim_start_matches(|c: char| c.is_ascii_digit()).len()];
        if let Some(op) = w[fd.len()..].strip_prefix('>') {
            let (append, attached) = match op.strip_prefix('>') {
                Some(t) => (true, t),
                None => (false, op.strip_prefix('|').unwrap_or(op)),
            };
            let target = if attached.is_empty() {
                i += 1;
                args.get(i).copied().unwrap_or("")
            } else {
                attached
            };
            if matches!(fd, "" | "1") && !target.starts_with('&') {
                redirect = Some((unquote(target), append));
            }
        } else if prog == "tee" && w.starts_with('-') {
            tee_append |= w == "--append" || (!w.starts_with("--") && w.contains('a'));
        } else if prog == "tee" && tee_file.is_none() {
            tee_file = Some(unquote(w));
        }
        i += 1;
    }
    let file = match prog {
        "cat" => redirect,
        "tee" => tee_file.map(|f| (f, tee_append)),
        _ => None,
    };
    file.filter(|(f, _)| !f.is_empty() && f != "/dev/null")
}

/// The commands of a block without prompts (a `$ ` at a command's start is dropped),
/// continuation lines joined, blank lines and comments left out; and the heredocs they
/// start, with their bodies taken out of the commands.
fn shell_block(
    doc: &str,
    lines: &[(usize, String)],
) -> Result<(Vec<Heredoc>, Vec<Logical>), String> {
    let mut heredocs = Vec::new();
    let mut cmds = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let (line_no, first) = &lines[i];
        let start = first.trim_start();
        let start = start.strip_prefix("$ ").unwrap_or(start);
        i += 1;
        if start.trim().is_empty() || start.starts_with('#') {
            continue;
        }
        let mut text = start.to_string();
        while shell_words(&text).open && i < lines.len() {
            text.push('\n');
            text.push_str(&lines[i].1);
            i += 1;
        }
        if let Some((delim, file)) = heredoc_start(&text) {
            let end = (i..lines.len())
                .find(|&j| lines[j].1.trim() == delim)
                .ok_or_else(|| format!("{doc}:{line_no}: heredoc `{delim}` is never closed"))?;
            let body: Vec<&str> = lines[i..end].iter().map(|(_, l)| l.as_str()).collect();
            heredocs.push(Heredoc {
                file,
                line: line_no + 1 + text.matches('\n').count(),
                body: body.join("\n") + "\n",
            });
            i = end + 1;
        }
        cmds.push(Logical {
            line: *line_no,
            text,
        });
    }
    Ok((heredocs, cmds))
}

/// A `$ ` command and the output shown after it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Shown {
    cmd: Logical,
    output: Vec<String>,
}

/// The `$ ` commands of a block, continuation lines joined, each with the lines after it
/// up to the next `$ ` line (trailing blank lines dropped); and the line of the first
/// non-blank line before any `$ `, if there is one.
fn prompted_commands(lines: &[(usize, String)]) -> (Option<usize>, Vec<Shown>) {
    let mut out: Vec<Shown> = Vec::new();
    let mut stray = None;
    let mut i = 0;
    while i < lines.len() {
        let (line_no, l) = &lines[i];
        i += 1;
        let Some(start) = l.strip_prefix("$ ") else {
            match out.last_mut() {
                Some(shown) => shown.output.push(l.clone()),
                None if !l.trim().is_empty() => {
                    stray.get_or_insert(*line_no);
                }
                None => {}
            }
            continue;
        };
        let mut text = start.to_string();
        while shell_words(&text).open && i < lines.len() {
            text.push('\n');
            text.push_str(&lines[i].1);
            i += 1;
        }
        out.push(Shown {
            cmd: Logical {
                line: *line_no,
                text,
            },
            output: Vec::new(),
        });
    }
    for shown in &mut out {
        shown.output = trimmed(std::mem::take(&mut shown.output));
    }
    (stray, out)
}

/// One documented command this test runs.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Cmd {
    line: usize,
    /// The `NAME=value` assignments before `mqttd`.
    env: Vec<(String, String)>,
    /// The arguments after `mqttd`.
    args: Vec<String>,
}

impl Cmd {
    fn tests_a_rule(&self) -> bool {
        self.args.iter().any(|a| a == "--rule-test")
    }

    /// The arguments and environment values, any of which may name a file.
    fn values(&self) -> impl Iterator<Item = &String> {
        self.args.iter().chain(self.env.iter().map(|(_, v)| v))
    }
}

fn assignment(word: &str) -> Option<(String, String)> {
    let (name, value) = word.split_once('=')?;
    let mut chars = name.chars();
    let valid = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    valid.then(|| (name.to_string(), value.to_string()))
}

fn is_mqttd(word: &str) -> bool {
    word == "mqttd" || word.ends_with("/mqttd")
}

fn is_rule_mode(word: &str) -> bool {
    matches!(word, "--rule-test" | "--check-rules")
}

/// Whether a command's text has `mqttd --rule-test` or `mqttd --check-rules` anywhere in
/// it, even inside syntax this test cannot parse (`$(mqttd --rule-test …)`).
fn mentions(text: &str) -> bool {
    let words: Vec<&str> = text.split_whitespace().collect();
    words.windows(2).any(|w| {
        w[0].rsplit(['=', '(', '`', '"', '\''])
            .next()
            .is_some_and(is_mqttd)
            && is_rule_mode(w[1])
    })
}

/// The words of a command up to any syntax this test cannot run: its environment
/// assignments, and the program and its arguments.
fn head(w: &Parsed) -> (Vec<(String, String)>, Option<&str>, &[String]) {
    let clean = &w.words[..w.clean];
    let n_env = clean.iter().take_while(|x| assignment(x).is_some()).count();
    let env = clean[..n_env]
        .iter()
        .filter_map(|x| assignment(x))
        .collect();
    let program = clean.get(n_env).map(String::as_str);
    (env, program, clean.get(n_env + 1..).unwrap_or_default())
}

/// Whether a command (its first line will do) runs `mqttd --rule-test` or
/// `mqttd --check-rules`.
fn runs_a_rule_mode(text: &str) -> bool {
    let w = shell_words(text);
    let (_, program, args) = head(&w);
    program.is_some_and(is_mqttd) && args.iter().any(|a| is_rule_mode(a))
}

/// What a command is to this test: one it runs (`Some`), another program's (`None`), or
/// an `mqttd --rule-test` / `--check-rules` it cannot run as written (`Err`).
fn invocation(line: usize, text: &str) -> Result<Option<Cmd>, String> {
    let w = shell_words(text);
    let cannot =
        |why: &str| format!("this test cannot run this `mqttd` command as written ({why}): {text}");
    let (env, program, args) = head(&w);
    match program {
        Some(p) if is_mqttd(p) && args.iter().any(|a| is_rule_mode(a)) => {
            if let Some(why) = &w.unsupported {
                return Err(cannot(why));
            }
            if w.open {
                return Err(cannot("it never ends: a quote or a `\\` is still open"));
            }
            Ok(Some(Cmd {
                line,
                env,
                args: args.to_vec(),
            }))
        }
        None if mentions(text) => Err(cannot(
            w.unsupported
                .as_deref()
                .unwrap_or("a quote or a `\\` is still open"),
        )),
        Some(_) | None => Ok(None),
    }
}

fn is_shell(lang: &str) -> bool {
    matches!(
        lang,
        "sh" | "bash" | "shell" | "zsh" | "shell-session" | "sh-session"
    )
}

/// Whether a block holds commands without prompts: a shell block, or one whose first line
/// is a command this test runs.
fn command_block(b: &Block) -> bool {
    is_shell(&b.lang)
        || b.lines
            .iter()
            .find(|l| !l.trim().is_empty())
            .is_some_and(|l| runs_a_rule_mode(l))
}

/// Whether a block shows output: a text-like language, no prompts, no commands.
fn is_output_block(b: &Block) -> bool {
    matches!(
        b.lang.as_str(),
        "text" | "" | "output" | "json" | "txt" | "plaintext" | "console"
    ) && !b.prompted()
        && !command_block(b)
}

// ---------------------------------------------------------------------------------------
// Rules snippets.
// ---------------------------------------------------------------------------------------

/// What a TOML snippet is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// A rules file.
    File,
    /// A `[rules.<id>]` schema sketch: shown, not loaded.
    Sketch,
    /// One rule's keys, without its table header.
    RuleBody,
    /// One rule's actions.
    Actions,
}

/// A rules snippet and the file text it stands for.
#[derive(Debug)]
struct Snippet {
    doc: &'static str,
    line: usize,
    kind: Kind,
    toml: String,
}

/// The keys a rule's table takes.
const RULE_KEYS: &[&str] = &["sql", "actions", "enable", "description"];

/// The lines of a TOML text outside multi-line strings, trimmed, without blank lines and
/// comments.
fn toml_code(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut open: Option<&str> = None;
    for line in text.lines() {
        if let Some(delim) = open {
            if line.contains(delim) {
                open = None;
            }
            continue;
        }
        let l = line.trim();
        if l.is_empty() || l.starts_with('#') {
            continue;
        }
        out.push(l);
        open = ["'''", "\"\"\""]
            .into_iter()
            .find(|delim| l.matches(delim).count() % 2 == 1);
    }
    out
}

/// The key of a `key = value` TOML line.
fn toml_key(line: &str) -> Option<&str> {
    let (key, _) = line.split_once('=')?;
    let key = key.trim().trim_matches(['"', '\'']);
    let bare = !key.is_empty()
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'));
    bare.then_some(key)
}

/// What a TOML text is, and the rules file to load for it; `None` for TOML that is not
/// about rules.
fn classify(text: &str) -> Option<(Kind, String)> {
    let code = toml_code(text);
    let rules_headers: Vec<&str> = code
        .iter()
        .filter(|l| l.trim_start_matches('[').trim_start().starts_with("rules."))
        .map(|l| l.split(']').next().unwrap_or(l))
        .collect();
    if !rules_headers.is_empty() {
        let sketch = rules_headers.iter().any(|h| h.contains('<'));
        return Some((
            if sketch { Kind::Sketch } else { Kind::File },
            text.to_string(),
        ));
    }
    let first = code.first()?;
    if first.starts_with('{') && first.contains("function") {
        let actions: Vec<&str> = code.iter().map(|l| l.trim_end_matches(',')).collect();
        return Some((
            Kind::Actions,
            format!(
                "[rules.doc_example]\nsql = 'SELECT * FROM \"t/#\"'\nactions = [\n{}\n]\n",
                actions.join(",\n")
            ),
        ));
    }
    let top: Vec<&str> = code
        .iter()
        .take_while(|l| !l.starts_with('['))
        .filter_map(|l| toml_key(l))
        .collect();
    if !top.iter().any(|k| RULE_KEYS.contains(k)) {
        return None;
    }
    let sql = if top.contains(&"sql") {
        ""
    } else {
        "sql = 'SELECT * FROM \"t/#\"'\n"
    };
    Some((
        Kind::RuleBody,
        format!("[rules.doc_example]\n{sql}{text}\n"),
    ))
}

/// Every rules snippet of a document, and what it could not read.
fn snippets(doc: &'static str, text: &str) -> (Vec<Snippet>, Vec<String>) {
    let mut out = Vec::new();
    let mut errors = Vec::new();
    for b in blocks(doc, text) {
        if b.lang == "toml" {
            if let Some((kind, toml)) = classify(&b.lines.join("\n")) {
                out.push(Snippet {
                    doc,
                    line: b.line(),
                    kind,
                    toml,
                });
            }
        } else if command_block(&b) && !b.prompted() {
            match shell_block(doc, &b.numbered()) {
                Ok((heredocs, _)) => {
                    for h in heredocs {
                        if let Some((kind, toml)) = classify(&h.body) {
                            out.push(Snippet {
                                doc,
                                line: h.line,
                                kind,
                                toml,
                            });
                        }
                    }
                }
                Err(e) => errors.push(e),
            }
        }
    }
    (out, errors)
}

// ---------------------------------------------------------------------------------------
// Transcripts.
// ---------------------------------------------------------------------------------------

/// Commands shown with their output (the outputs of several commands, concatenated).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Transcript {
    doc: &'static str,
    line: usize,
    cmds: Vec<Cmd>,
    expected: Vec<String>,
}

/// What a document's commands are run against, and what they must print.
#[derive(Debug, Default)]
struct Commands {
    transcripts: Vec<Transcript>,
    /// Commands shown without output: each must exit 0 (a `--check-rules` only when the
    /// file it names is known).
    unpaired: Vec<(&'static str, Cmd)>,
    /// The heredocs in the document that write a file, in order.
    files: Vec<Heredoc>,
}

/// Count the paragraphs among lines of prose.
fn paragraphs(lines: &[&str]) -> usize {
    let mut count = 0;
    let mut in_paragraph = false;
    for l in lines {
        let blank = l.trim().is_empty();
        if !blank && !in_paragraph {
            count += 1;
        }
        in_paragraph = !blank;
    }
    count
}

/// The block holding the output of the commands in block `n`, when the document shows
/// it: the next block, if it is an output block and at most one paragraph of prose, and
/// no heading, separates them. A block a reader would take for their output but this
/// test cannot is an error.
fn next_output<'a>(
    doc: &str,
    doc_lines: &[&str],
    bs: &'a [Block],
    n: usize,
) -> Result<Option<&'a Block>, String> {
    let b = &bs[n];
    let Some(next) = bs.get(n + 1) else {
        return Ok(None);
    };
    let between = &doc_lines[b.close..next.open - 1];
    if between.iter().any(|l| l.trim_start().starts_with('#')) {
        return Ok(None);
    }
    let prose = paragraphs(between);
    if is_output_block(next) {
        if prose <= 1 {
            return Ok(Some(next));
        }
        return Err(format!(
            "{doc}:{}: these commands are shown without their output, and an output block \
             follows in the same section (line {}) {prose} paragraphs later: put their \
             output right after them (at most one paragraph between), or, if it is not \
             theirs, give it a language of its own such as `log`",
            b.line(),
            next.line()
        ));
    }
    if prose == 0 && !command_block(next) && !next.prompted() {
        return Err(format!(
            "{doc}:{}: the block right after these commands (line {}) is a `{}` block, not \
             their output: an output block is `text`, `output`, `json`, `console` without \
             prompts, or unlabelled",
            b.line(),
            next.line(),
            next.lang
        ));
    }
    Ok(None)
}

/// A `$ ` block that shows output: each command this test runs is a transcript; every
/// other command is an error.
fn prompted_transcripts(
    doc: &'static str,
    stray: Option<usize>,
    shown: Vec<Shown>,
    found: &mut Commands,
    errors: &mut Vec<String>,
) {
    if let Some(line) = stray {
        errors.push(format!("{doc}:{line}: output before any `$ ` command"));
    }
    for s in shown {
        match invocation(s.cmd.line, &s.cmd.text) {
            Ok(Some(cmd)) => found.transcripts.push(Transcript {
                doc,
                line: cmd.line,
                cmds: vec![cmd],
                expected: s.output,
            }),
            Ok(None) => errors.push(format!(
                "{doc}:{}: a block that shows `$ mqttd --rule-test` or \
                 `$ mqttd --check-rules` with its output runs only those commands, and this \
                 test cannot run `{}`",
                s.cmd.line, s.cmd.text
            )),
            Err(e) => errors.push(format!("{doc}:{}: {e}", s.cmd.line)),
        }
    }
}

/// Every command of a document this test runs. Errors name the line.
fn commands(doc: &'static str, text: &str) -> (Commands, Vec<String>) {
    let doc_lines: Vec<&str> = text.lines().collect();
    let bs = blocks(doc, text);
    let mut found = Commands::default();
    let mut errors = Vec::new();
    for (n, b) in bs.iter().enumerate() {
        let logicals = if b.prompted() {
            let (stray, shown) = prompted_commands(&b.numbered());
            let runs = shown
                .iter()
                .any(|s| !matches!(invocation(s.cmd.line, &s.cmd.text), Ok(None)));
            if shown.iter().any(|s| !s.output.is_empty()) {
                if runs {
                    prompted_transcripts(doc, stray, shown, &mut found, &mut errors);
                }
                continue;
            }
            shown.into_iter().map(|s| s.cmd).collect()
        } else if command_block(b) {
            match shell_block(doc, &b.numbered()) {
                Ok((heredocs, cmds)) => {
                    found
                        .files
                        .extend(heredocs.into_iter().filter(|h| h.file.is_some()));
                    cmds
                }
                Err(e) => {
                    errors.push(e);
                    continue;
                }
            }
        } else {
            continue;
        };
        let mut runs = Vec::new();
        let mut others = false;
        for l in &logicals {
            match invocation(l.line, &l.text) {
                Ok(Some(cmd)) => runs.push(cmd),
                Ok(None) => others = true,
                Err(e) => errors.push(format!("{doc}:{}: {e}", l.line)),
            }
        }
        if runs.is_empty() {
            continue;
        }
        if others {
            // A block that does other things too: its `--rule-test` commands must run.
            found
                .unpaired
                .extend(runs.into_iter().filter(Cmd::tests_a_rule).map(|c| (doc, c)));
            continue;
        }
        match next_output(doc, &doc_lines, &bs, n) {
            Ok(Some(out)) => found.transcripts.push(Transcript {
                doc,
                line: runs[0].line,
                cmds: runs,
                expected: trimmed(out.lines.clone()),
            }),
            Ok(None) => found.unpaired.extend(runs.into_iter().map(|c| (doc, c))),
            Err(e) => errors.push(e),
        }
    }
    (found, errors)
}

// ---------------------------------------------------------------------------------------
// The real binary.
// ---------------------------------------------------------------------------------------

/// Kills the spawned process when the test ends (including on panic).
struct ChildGuard(std::process::Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// What one command printed.
#[derive(Debug)]
struct Ran {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

fn drain(pipe: Option<impl std::io::Read + Send + 'static>) -> std::thread::JoinHandle<String> {
    let mut pipe = pipe.expect("piped");
    std::thread::spawn(move || {
        let mut s = String::new();
        pipe.read_to_string(&mut s).expect("the output is UTF-8");
        s
    })
}

/// Run `mqttd` as `cmd` shows it in `dir`, offline. A regression that starts a broker
/// instead of exiting is killed by the bounded wait.
fn run_mqttd(cmd: &Cmd, dir: &Path) -> Ran {
    let mut command = Command::new(env!("CARGO_BIN_EXE_mqttd"));
    for (k, _) in std::env::vars() {
        if k.starts_with("MQTTD_") {
            command.env_remove(k);
        }
    }
    let child = command
        .env("TZ", "UTC")
        .envs(cmd.env.iter().map(|(k, v)| (k, v)))
        .current_dir(dir)
        .args(&cmd.args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mqttd");
    let mut guard = ChildGuard(child);
    let stdout = drain(guard.0.stdout.take());
    let stderr = drain(guard.0.stderr.take());
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = guard.0.try_wait().expect("try_wait") {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "`mqttd {:?}` did not exit within 30 s",
            cmd.args
        );
        std::thread::sleep(Duration::from_millis(5));
    };
    Ran {
        code: status.code(),
        stdout: stdout.join().expect("stdout reader"),
        stderr: stderr.join().expect("stderr reader"),
    }
}

fn lines_of(s: &str) -> Vec<String> {
    s.lines().map(str::to_string).collect()
}

/// The exit status a command's output implies.
fn implied_status(output: &[String]) -> i32 {
    if output
        .iter()
        .any(|l| l.starts_with("rule test FAILED") || l.starts_with("rules INVALID"))
    {
        1
    } else if output
        .iter()
        .any(|l| l.starts_with("error:") || l.starts_with("mqttd:"))
    {
        2
    } else {
        0
    }
}

/// A file name as a command would name it (`./rules.toml` is `rules.toml`).
fn normal(name: &str) -> String {
    name.strip_prefix("./").unwrap_or(name).to_string()
}

/// The files the document's heredocs have written by `line`, by name.
fn files_at(files: &[Heredoc], line: usize) -> BTreeMap<String, String> {
    let mut state: BTreeMap<String, String> = BTreeMap::new();
    for h in files.iter().filter(|h| h.line < line) {
        if let Some((name, append)) = &h.file {
            let name = normal(name);
            let body = if *append {
                state.remove(&name).unwrap_or_default() + &h.body
            } else {
                h.body.clone()
            };
            state.insert(name, body);
        }
    }
    state
}

/// A scratch directory holding the files a document's heredocs have written by one line.
struct Scratch(PathBuf);

impl Scratch {
    fn new(doc: &str, line: usize, files: &BTreeMap<String, String>) -> Self {
        let name = doc.replace(['/', '.'], "-");
        let dir = std::env::temp_dir().join(format!(
            "mqttd-rules-docs-{}-{name}-{line}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create the scratch directory");
        let scratch = Self(dir);
        for (name, body) in files {
            let path = scratch.0.join(name);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).expect("create a heredoc file's directory");
            }
            std::fs::write(&path, body).expect("write a heredoc file");
        }
        scratch
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Where a command runs: beside the heredoc files it names, as the document has written
/// them by its line, else at the repository root.
fn workdir(
    doc: &str,
    cmd: &Cmd,
    files: &[Heredoc],
    root: &Path,
) -> Result<(PathBuf, Option<Scratch>), String> {
    let state = files_at(files, cmd.line);
    let named: Vec<String> = cmd
        .values()
        .map(|v| normal(v))
        .filter(|v| state.contains_key(v))
        .collect();
    if named.is_empty() {
        return Ok((root.to_path_buf(), None));
    }
    if let Some(outside) = named.iter().find(|v| {
        let p = Path::new(v.as_str());
        p.is_absolute() || p.components().any(|c| c == Component::ParentDir)
    }) {
        return Err(format!(
            "`{outside}` is written by a heredoc outside the directory this test can \
             write to: name it with a relative path"
        ));
    }
    let writable: BTreeMap<String, String> = state
        .into_iter()
        .filter(|(name, _)| {
            let p = Path::new(name);
            !p.is_absolute() && !p.components().any(|c| c == Component::ParentDir)
        })
        .collect();
    let scratch = Scratch::new(doc, cmd.line, &writable);
    Ok((scratch.0.clone(), Some(scratch)))
}

/// Run a transcript; `Err` says how the binary's output differs from the document's.
fn check_transcript(t: &Transcript, files: &[Heredoc], root: &Path) -> Result<(), String> {
    let mut shown = Vec::new();
    let mut terminal = Vec::new();
    let mut statuses = Vec::new();
    for cmd in &t.cmds {
        let (dir, _scratch) = workdir(t.doc, cmd, files, root)?;
        let ran = run_mqttd(cmd, &dir);
        let primary = if ran.code == Some(0) {
            &ran.stdout
        } else {
            &ran.stderr
        };
        let own = lines_of(primary);
        statuses.push((cmd, ran.code, implied_status(&own)));
        shown.extend(own);
        terminal.extend(lines_of(&ran.stderr));
        terminal.extend(lines_of(&ran.stdout));
    }
    let (shown, terminal) = (trimmed(shown), trimmed(terminal));
    if t.expected != shown && t.expected != terminal {
        return Err(format!(
            "the output differs.\n    documented:\n      {}\n    mqttd printed:\n      {}",
            t.expected.join("\n      "),
            terminal.join("\n      ")
        ));
    }
    for (cmd, code, implied) in statuses {
        if code != Some(implied) {
            return Err(format!(
                "`mqttd {}` exited {code:?}, but its output implies {implied}",
                cmd.args.join(" ")
            ));
        }
    }
    Ok(())
}

/// Run a command shown without output; `Err` says how it failed. A `--check-rules` of a
/// file this test does not know (the reader's own) is not run.
fn check_unpaired(doc: &str, cmd: &Cmd, files: &[Heredoc], root: &Path) -> Result<(), String> {
    let state = files_at(files, cmd.line);
    let known = cmd
        .values()
        .any(|v| state.contains_key(&normal(v)) || (!v.starts_with('-') && root.join(v).is_file()));
    if !cmd.tests_a_rule() && !known {
        return Ok(());
    }
    let (dir, _scratch) = workdir(doc, cmd, files, root)?;
    let ran = run_mqttd(cmd, &dir);
    if ran.code == Some(0) {
        return Ok(());
    }
    Err(format!(
        "`mqttd {}` exited {:?}: {}{}",
        cmd.args.join(" "),
        ran.code,
        ran.stderr,
        ran.stdout
    ))
}

// ---------------------------------------------------------------------------------------
// The tests.
// ---------------------------------------------------------------------------------------

/// docs/RULES.md ("The rules file", Actions, Gotchas) and every RULES-COOKBOOK.md recipe:
/// the rules files, rule bodies and action lists the documents show are ones the broker
/// loads. A snippet that drifted from the schema (an unknown key, a SQL statement that no
/// longer parses, an action argument the loader refuses) fails here with its file and
/// line, before a reader copies it into a broker that refuses to boot.
#[test]
fn documented_rules_snippets_load() {
    let mut failures = Vec::new();
    for (doc, text) in docs() {
        let (found, errors) = snippets(doc, &text);
        failures.extend(errors);
        let loaded = found.iter().filter(|s| s.kind != Kind::Sketch).count();
        assert!(
            loaded > 0,
            "{doc}: no rules snippet found; the extraction no longer recognises the document"
        );
        for s in found.iter().filter(|s| s.kind != Kind::Sketch) {
            if let Err(e) = mqtt_rules::RuleSet::parse(&s.toml) {
                failures.push(format!(
                    "{}:{}: {:?} does not load: {e}",
                    s.doc, s.line, s.kind
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "documented rules snippets that do not load:\n{}",
        failures.join("\n")
    );
}

/// docs/RULES.md ("Try it in two minutes", "Testing and debugging rules", Gotchas) and
/// RULES-COOKBOOK.md: every `mqttd --rule-test` and `mqttd --check-rules` shown with its
/// output prints exactly that output, with the exit status the output implies, when the
/// real binary runs it. A change to an output's form, a field, the default message
/// `--rule-test` simulates, or a rules file whose documented digest is stale fails here
/// with the document's line.
#[test]
fn documented_rule_test_and_check_rules_transcripts_match_the_binary() {
    let root = repo_root();
    let mut failures = Vec::new();
    for (doc, text) in docs() {
        let (found, errors) = commands(doc, &text);
        failures.extend(errors);
        assert!(
            !found.transcripts.is_empty(),
            "{doc}: no command shown with its output; the extraction no longer recognises \
             the document"
        );
        for t in &found.transcripts {
            if let Err(e) = check_transcript(t, &found.files, &root) {
                failures.push(format!("{}:{}: {e}", t.doc, t.line));
            }
        }
        for (doc, cmd) in &found.unpaired {
            if let Err(e) = check_unpaired(doc, cmd, &found.files, &root) {
                failures.push(format!("{doc}:{}: {e}", cmd.line));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "documented commands that do not do what the documents show:\n{}",
        failures.join("\n\n")
    );
}

/// A document with one of every form the extraction recognises, and blocks it must not
/// take for snippets.
const SYNTHETIC: &str = r#"Intro.

```console
$ mqttd --rule-test --sql 'SELECT 1 AS a FROM "t/#"' \
    --payload '{"x": 1}'
{"a":1}
$ mqttd --rule-test --sql 'SELECT
  2 AS b FROM "t/#"'
{"b":2}
```

- In a list:

  ```console
  $ mqttd --rule-test --sql 'SELECT 3 AS c FROM "t/#"'
  {"c":3}
  ```

```sh
cat > r.toml <<'EOF'
[rules.r]
sql = 'SELECT * FROM "t/#"'
EOF
mqttd --help | grep -e --rule-test
mqttd --rule-test --sql 'SELECT 4 AS d FROM "t/#"'
```

```sh
mqttd --check-rules r.toml
```

```text
rules OK
```

```sh
mqttd --rule-test --sql 'SELECT 5 AS e FROM "t/#"'
```

It prints:

```json
{"e":5}
```

```bash
cat <<'EOF' >r.toml
[rules.s]
sql = 'SELECT * FROM "s/#"'
EOF
tee -a ./r.toml >/dev/null <<EOF
[rules.t]
sql = 'SELECT * FROM "t/#"'
EOF
```

```
MQTTD_RULES_FILE=r.toml mqttd --check-rules
```

```
rules OK
```

```console
$ mqttd --rule-test --sql 'SELECT 6 AS f FROM "t/#"'
```

```output
{"f":6}
```

```toml
[rules.<id>]
sql = '...'
```

```TOML
description = "the first key is not sql"
sql = '''
SELECT 1 FROM "t"
'''
```

  ```toml
  { function = "console" }
  { function = "republish", args = { topic = "x" } },
  ```

```toml
actions = [{ function = "console" }]
enable = false
```

```toml
[rules]
file = "rules.toml"
```

```sh
tee r2.toml <<'EOF'
[rules.u]
sql = 'SELECT * FROM "u/#"'
EOF
```
"#;

/// The command extraction, on a document written for it: both command forms (a `$ `
/// transcript, one inside a list item, continued commands, a `$ ` command shown without
/// output whose output is the next block), output in a `text`, `json`, `output` or
/// unlabelled block, directly after the commands or after one paragraph, a command with
/// an environment assignment, and a `--rule-test` in a block that runs other commands
/// too. If a form stopped being recognised, the documents' transcript check would pass
/// on less than they show; this fails instead.
#[test]
fn the_extraction_recognises_every_documented_form() {
    /// One command of a transcript: its environment and its arguments.
    type Run = (Vec<(String, String)>, Vec<String>);
    let doc = SYNTHETIC;
    let (found, errors) = commands("synthetic", doc);
    assert_eq!(errors, Vec::<String>::new());
    let shown: Vec<(usize, Vec<Run>, Vec<String>)> = found
        .transcripts
        .iter()
        .map(|t| {
            (
                t.line,
                t.cmds
                    .iter()
                    .map(|c| (c.env.clone(), c.args.clone()))
                    .collect(),
                t.expected.clone(),
            )
        })
        .collect();
    let args = |a: &[&str]| a.iter().map(|s| (*s).to_string()).collect::<Vec<_>>();
    let rule_test = |sql: &str| (vec![], args(&["--rule-test", "--sql", sql]));
    let out = |s: &str| vec![s.to_string()];
    assert_eq!(
        shown,
        vec![
            (
                4,
                vec![(
                    vec![],
                    args(&[
                        "--rule-test",
                        "--sql",
                        "SELECT 1 AS a FROM \"t/#\"",
                        "--payload",
                        "{\"x\": 1}"
                    ])
                )],
                out("{\"a\":1}"),
            ),
            (
                7,
                vec![rule_test("SELECT\n  2 AS b FROM \"t/#\"")],
                out("{\"b\":2}")
            ),
            (
                15,
                vec![rule_test("SELECT 3 AS c FROM \"t/#\"")],
                out("{\"c\":3}")
            ),
            (
                29,
                vec![(vec![], args(&["--check-rules", "r.toml"]))],
                out("rules OK")
            ),
            (
                37,
                vec![rule_test("SELECT 5 AS e FROM \"t/#\"")],
                out("{\"e\":5}")
            ),
            (
                58,
                vec![(
                    vec![("MQTTD_RULES_FILE".to_string(), "r.toml".to_string())],
                    args(&["--check-rules"])
                )],
                out("rules OK")
            ),
            (
                66,
                vec![rule_test("SELECT 6 AS f FROM \"t/#\"")],
                out("{\"f\":6}")
            ),
        ]
    );
    let unpaired: Vec<(usize, Vec<String>)> = found
        .unpaired
        .iter()
        .map(|(_, c)| (c.line, c.args.clone()))
        .collect();
    assert_eq!(
        unpaired,
        vec![(
            25,
            args(&["--rule-test", "--sql", "SELECT 4 AS d FROM \"t/#\""])
        )]
    );
}

/// The rules-file extraction, on the same document: heredoc rules files written with
/// `cat > f`, `cat <<'EOF' >f`, `tee -a` (appending) and `tee`, the files a command sees
/// at its line (a later heredoc to the same name replaces the file, so a transcript is
/// checked against the file the document has written by then), the TOML snippet kinds (a
/// `TOML` fence, a rule body whose first key is not `sql`, one with no `sql`) and a schema
/// sketch, and a broker configuration block that is none of these. If a form stopped
/// being recognised, the documents' snippet check would load less than they show; this
/// fails instead.
#[test]
fn the_extraction_finds_every_documented_rules_file_and_snippet() {
    let doc = SYNTHETIC;
    let (found, errors) = commands("synthetic", doc);
    assert_eq!(errors, Vec::<String>::new());
    let file = |name: &str, append: bool, line: usize, body: &str| Heredoc {
        file: Some((name.to_string(), append)),
        line,
        body: body.to_string(),
    };
    let (r, s, t, u) = (
        "[rules.r]\nsql = 'SELECT * FROM \"t/#\"'\n",
        "[rules.s]\nsql = 'SELECT * FROM \"s/#\"'\n",
        "[rules.t]\nsql = 'SELECT * FROM \"t/#\"'\n",
        "[rules.u]\nsql = 'SELECT * FROM \"u/#\"'\n",
    );
    assert_eq!(
        found.files,
        vec![
            file("r.toml", false, 21, r),
            file("r.toml", false, 48, s),
            file("./r.toml", true, 52, t),
            file("r2.toml", false, 102, u),
        ]
    );
    let state = |line: usize| files_at(&found.files, line).into_iter().collect::<Vec<_>>();
    assert_eq!(state(29), vec![("r.toml".to_string(), r.to_string())]);
    assert_eq!(state(58), vec![("r.toml".to_string(), format!("{s}{t}"))]);
    let (found, errors) = snippets("synthetic", doc);
    assert_eq!(errors, Vec::<String>::new());
    let kinds: Vec<(usize, Kind)> = found.iter().map(|s| (s.line, s.kind)).collect();
    assert_eq!(
        kinds,
        vec![
            (21, Kind::File),
            (48, Kind::File),
            (52, Kind::File),
            (74, Kind::Sketch),
            (79, Kind::RuleBody),
            (86, Kind::Actions),
            (91, Kind::RuleBody),
            (102, Kind::File),
        ]
    );
    for s in found.iter().filter(|s| s.kind != Kind::Sketch) {
        if let Err(e) = mqtt_rules::RuleSet::parse(&s.toml) {
            panic!(
                "synthetic:{}: {:?} does not load: {e}\n{}",
                s.line, s.kind, s.toml
            );
        }
    }
}

/// Commands a reader would take for checked examples but this test cannot run as shown.
const UNRUNNABLE: &str = r#"```console
$ cd /tmp
$ mqttd --rule-test --sql 'SELECT 1 AS a FROM "t/#"'
{"a":1}
```

```sh
mqttd --rule-test --sql 'SELECT 2 AS b FROM "t/#"' | jq .
```

```sh
mqttd --rule-test --sql 'SELECT 3 AS c FROM "t/#"'
```
```yaml
c: 3
```

```sh
mqttd --check-rules r.toml
```

One paragraph.

Another.

```text
rules OK
```

```console
{"stray":1}
$ mqttd --rule-test --sql 'SELECT 4 AS d FROM "t/#"'
{"d":4}
```

```sh
MQTTD_X="$HOME" mqttd --check-rules
```
"#;

/// The extraction refuses, naming the line, every command it would otherwise skip or
/// mis-check: a `$ ` transcript that also runs another command (`cd` changes what
/// `mqttd` prints), an `mqttd --rule-test` piped into another program, a block directly
/// after a command that is neither its output nor more commands, an output block more
/// than one paragraph after a command shown without output, output before a transcript's
/// first `$ ` command, and an expansion in a command's environment. Were any of these
/// silently skipped, a broken example written that way would pass unchecked.
#[test]
fn the_extraction_refuses_what_it_cannot_run() {
    let (_, errors) = commands("unrunnable", UNRUNNABLE);
    assert_eq!(
        errors,
        vec![
            "unrunnable:2: a block that shows `$ mqttd --rule-test` or `$ mqttd --check-rules` \
             with its output runs only those commands, and this test cannot run `cd /tmp`"
                .to_string(),
            "unrunnable:8: this test cannot run this `mqttd` command as written (`|` is shell \
             syntax this test does not run): mqttd --rule-test --sql 'SELECT 2 AS b FROM \
             \"t/#\"' | jq ."
                .to_string(),
            "unrunnable:12: the block right after these commands (line 15) is a `yaml` block, \
             not their output: an output block is `text`, `output`, `json`, `console` without \
             prompts, or unlabelled"
                .to_string(),
            "unrunnable:19: these commands are shown without their output, and an output \
             block follows in the same section (line 27) 2 paragraphs later: put their output \
             right after them (at most one paragraph between), or, if it is not theirs, give \
             it a language of its own such as `log`"
                .to_string(),
            "unrunnable:31: output before any `$ ` command".to_string(),
            "unrunnable:37: this test cannot run this `mqttd` command as written (`$` expands \
             inside double quotes): MQTTD_X=\"$HOME\" mqttd --check-rules"
                .to_string(),
        ]
    );
}
