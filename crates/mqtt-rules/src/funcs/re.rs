//! Regular expressions as EMQX runs them: Erlang's `re` module, which is PCRE2 in byte
//! mode.
//!
//! EMQX's `regex_match`, `regex_replace` and `regex_extract` (`emqx_variform_bif.erl`)
//! hand the pattern to `re:run` / `re:replace` uncompiled and without the `unicode`
//! option, so OTP compiles it per call with no options: 8-bit code units, no UTF, no
//! UCP, newline LF, the C-locale character tables, no JIT. This module compiles with the
//! same PCRE2 defaults (the `pcre2` crate, PCRE2 built from source by `pcre2-sys`) and
//! repeats what OTP's `re.erl` does around `pcre2_match`: its global-match loop
//! (`loopexec/8`, with its retry after an empty match and its CRLF-aware step) and its
//! treatment of match errors — a match, depth or heap limit hit is *no match*, a UTF
//! error in a `(*UTF)` pattern is a failure.
//!
//! The limits are OTP's build defaults (`erts/emulator/pcre/local_config.h`), which are
//! also PCRE2's: 10,000,000 for the match and the depth limit, per start position. The
//! heap limit is lower than OTP's 20,000,000 KiB: [`HEAP_LIMIT_KIB`].

use std::borrow::Cow;
use std::sync::OnceLock;

use pcre2::bytes::{CaptureLocations, Regex as Code, RegexBuilder};

/// The most memory one match may take for its backtracking frames, in KiB (64 MiB).
///
/// OTP builds PCRE2 with a heap limit of 20,000,000 KiB — in effect none — and EMQX's
/// matches yield to the scheduler; here a match runs on the publisher's connection task,
/// and the frames of every concurrent match are live at once. 64 MiB holds about half a
/// million nested backtracking points; a match that needs more is *no match*, as a
/// match that reaches OTP's limits is in EMQX (see `docs/RULES.md`, "Regular
/// expressions"). It is set as a `(*LIMIT_HEAP=…)` start-of-pattern item, which PCRE2
/// applies when lower than the build default, after any the pattern sets itself.
pub(crate) const HEAP_LIMIT_KIB: u32 = 64 * 1024;

/// `pcre2_match` results OTP's `re` turns into `nomatch` (`erl_bif_re.c`, `re_run`).
const NO_MATCH: [i32; 4] = [
    pcre2_sys::PCRE2_ERROR_MATCHLIMIT,
    pcre2_sys::PCRE2_ERROR_DEPTHLIMIT,
    pcre2_sys::PCRE2_ERROR_HEAPLIMIT,
    pcre2_sys::PCRE2_ERROR_RECURSELOOP,
];

/// One match: the start and end of the whole match and of each group, as far as the
/// highest group that took part (`pcre2_match`'s return count, which is how many `re`
/// reports); `None` for a group that did not take part.
pub(crate) type Groups = Vec<Option<(usize, usize)>>;

/// A compiled pattern.
pub(crate) struct Regex {
    /// The pattern as given, for `Debug`.
    shown: String,
    code: Code,
    /// The same pattern with `(*NOTEMPTY_ATSTART)`, for `loopexec`'s retry after an
    /// empty match; compiled the first time one happens.
    retry: OnceLock<Result<Code, String>>,
    /// The pattern's start-of-pattern items (and ours), and the rest of it.
    lead: String,
    rest: String,
    /// Whether the pattern's newline convention includes CRLF (`(*CRLF)`, `(*ANY)`,
    /// `(*ANYCRLF)`), so an empty match before `\r\n` steps over both bytes.
    crlf: bool,
}

impl std::fmt::Debug for Regex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Regex").field(&self.shown).finish()
    }
}

impl Regex {
    /// Compile `pattern` as `re` does with no options. The error is PCRE2's message and
    /// the position in the pattern it names, as `re:compile/1` reports them.
    pub(crate) fn new(pattern: &[u8]) -> Result<Self, String> {
        let text = as_text(pattern);
        let lead = leading_items(text.as_bytes());
        let ours = format!(
            "(*LIMIT_HEAP={})",
            lead.heap.map_or(HEAP_LIMIT_KIB, |h| h.min(HEAP_LIMIT_KIB))
        );
        let (head, rest) = text.split_at(lead.end);
        let lead_text = format!("{head}{ours}");
        let code = build(&format!("{lead_text}{rest}")).map_err(|e| {
            let (message, at) = describe(&e);
            // Positions past our item are shifted back to the pattern as written.
            let at = at.map(|at| {
                if at >= lead.end {
                    at.saturating_sub(ours.len())
                } else {
                    at
                }
            });
            match at {
                Some(at) => format!("invalid regular expression: {message} at position {at}"),
                None => format!("invalid regular expression: {message}"),
            }
        })?;
        Ok(Self {
            shown: String::from_utf8_lossy(pattern).into_owned(),
            code,
            retry: OnceLock::new(),
            lead: lead_text,
            rest: rest.to_string(),
            crlf: lead.crlf,
        })
    }

    /// The first match from the start of `subject` (`re:run/3` without `global`).
    pub(crate) fn first(&self, subject: &[u8]) -> Result<Option<Groups>, String> {
        exec(&self.code, &mut self.code.capture_locations(), subject, 0)
    }

    /// Every match `re:run(Subject, RE, [global])` reports, in its order, each handed
    /// to `each` as it is found — OTP's `loopexec/8`, step for step. After a non-empty
    /// match the search goes on from its end; after an empty one it is retried at the
    /// same offset anchored and with `notempty_atstart`, and then steps on (over `\r\n`
    /// as one when the newline convention includes CRLF). A match equal to the one just
    /// reported is not reported twice.
    pub(crate) fn each<E: From<String>>(
        &self,
        subject: &[u8],
        mut each: impl FnMut(&Groups) -> Result<(), E>,
    ) -> Result<(), E> {
        let mut locs = self.code.capture_locations();
        let mut retry_locs: Option<CaptureLocations> = None;
        let end = subject.len();
        let mut at = 0;
        // The last match reported by the search itself, when nothing came after it.
        let mut previous: Option<Groups> = None;
        while at <= end {
            let Some(m) = exec(&self.code, &mut locs, subject, at)? else {
                break;
            };
            let (start, stop) = m[0].unwrap_or((at, at));
            if previous.as_ref() != Some(&m) {
                each(&m)?;
            }
            if stop > start {
                previous = Some(m);
                at = stop;
                continue;
            }
            // `[{offset, X}, notempty_atstart, anchored]`. The `pcre2` crate cannot pass
            // `anchored`, so the search runs unanchored and counts only a match that
            // starts at X: it tries X first, exactly as the anchored search would, and
            // anything it finds further on is what the anchored search would not have.
            let retry = self.retry()?;
            let locs = retry_locs.get_or_insert_with(|| retry.capture_locations());
            let extra = exec(retry, locs, subject, at)?
                .filter(|r| r[0].is_some_and(|(s, _)| s == at) && *r != m);
            at = match extra.as_ref().and_then(|r| r[0]) {
                Some((s, e)) if e > s => start + (e - s),
                _ if at == start || start == end => self.step(subject, start),
                _ => start,
            };
            match extra {
                Some(r) => {
                    each(&r)?;
                    previous = None;
                }
                None => previous = Some(m),
            }
        }
        Ok(())
    }

    /// `re.erl`'s `forward/5` by one: the next byte, or past `\r\n` when the newline
    /// convention includes CRLF.
    fn step(&self, subject: &[u8], at: usize) -> usize {
        if self.crlf && subject.get(at..at + 2) == Some(b"\r\n") {
            at + 2
        } else {
            at + 1
        }
    }

    fn retry(&self) -> Result<&Code, String> {
        self.retry
            .get_or_init(|| {
                build(&format!("{}(*NOTEMPTY_ATSTART){}", self.lead, self.rest))
                    .map_err(|e| format!("invalid regular expression: {}", describe(&e).0))
            })
            .as_ref()
            .map_err(Clone::clone)
    }
}

/// PCRE2 with `re`'s compile options: none. (The `pcre2` crate's defaults are none too:
/// no UTF, no UCP, no JIT, the default newline and character tables.)
fn build(pattern: &str) -> Result<Code, pcre2::Error> {
    RegexBuilder::new().build(pattern)
}

/// PCRE2's message and position from a compile error.
fn describe(e: &pcre2::Error) -> (String, Option<usize>) {
    let text = e.to_string();
    let message = match e.offset() {
        Some(at) => text
            .split_once(&format!("offset {at}: "))
            .map(|(_, m)| m.to_string()),
        None => text.split_once("pattern: ").map(|(_, m)| m.to_string()),
    };
    (message.unwrap_or(text), e.offset())
}

/// One `pcre2_match` from `at`: the match, `None` for no match — and, as in OTP, for a
/// match, depth or heap limit reached — or the error OTP raises (`badarg`) for anything
/// else, such as an invalid UTF-8 subject under `(*UTF)`.
fn exec(
    code: &Code,
    locs: &mut CaptureLocations,
    subject: &[u8],
    at: usize,
) -> Result<Option<Groups>, String> {
    match code.captures_read_at(locs, subject, at) {
        Ok(None) => Ok(None),
        Ok(Some(_)) => {
            let n = (0..locs.len())
                .rev()
                .find(|&i| locs.get(i).is_some())
                .map_or(0, |i| i + 1);
            Ok(Some((0..n).map(|i| locs.get(i)).collect()))
        }
        Err(e) if NO_MATCH.contains(&e.code()) => Ok(None),
        Err(e) => Err(format!(
            "regular expression match failed: {}",
            e.to_string().trim_start_matches("PCRE2: error matching: ")
        )),
    }
}

/// What the start-of-pattern items (`(*UTF)`, `(*CRLF)`, `(*LIMIT_HEAP=n)` …) of a
/// pattern say, read as `pcre2_compile` reads them.
struct Lead {
    /// Where they end.
    end: usize,
    /// The last `(*LIMIT_HEAP=n)`.
    heap: Option<u32>,
    /// Whether the last newline item includes CRLF.
    crlf: bool,
}

/// What a start-of-pattern item is (PCRE2 10.46's `pso_list` in `pcre2_compile.c`, in
/// its order).
enum Item {
    Flag,
    Heap,
    Limit,
    Newline { crlf: bool },
}

const ITEMS: [(&str, Item); 23] = [
    ("UTF8)", Item::Flag),
    ("UTF)", Item::Flag),
    ("UCP)", Item::Flag),
    ("NOTEMPTY)", Item::Flag),
    ("NOTEMPTY_ATSTART)", Item::Flag),
    ("NO_AUTO_POSSESS)", Item::Flag),
    ("NO_DOTSTAR_ANCHOR)", Item::Flag),
    ("NO_JIT)", Item::Flag),
    ("NO_START_OPT)", Item::Flag),
    ("CASELESS_RESTRICT)", Item::Flag),
    ("TURKISH_CASING)", Item::Flag),
    ("LIMIT_HEAP=", Item::Heap),
    ("LIMIT_MATCH=", Item::Limit),
    ("LIMIT_DEPTH=", Item::Limit),
    ("LIMIT_RECURSION=", Item::Limit),
    ("CR)", Item::Newline { crlf: false }),
    ("LF)", Item::Newline { crlf: false }),
    ("CRLF)", Item::Newline { crlf: true }),
    ("ANY)", Item::Newline { crlf: true }),
    ("NUL)", Item::Newline { crlf: false }),
    ("ANYCRLF)", Item::Newline { crlf: true }),
    ("BSR_ANYCRLF)", Item::Flag),
    ("BSR_UNICODE)", Item::Flag),
];

/// Read the start-of-pattern items. A malformed limit ends them where it starts, so
/// PCRE2 reports it after reading ours.
fn leading_items(p: &[u8]) -> Lead {
    let mut lead = Lead {
        end: 0,
        heap: None,
        crlf: false,
    };
    while p[lead.end..].starts_with(b"(*") {
        let after = lead.end + 2;
        let Some((name, item)) = ITEMS
            .iter()
            .find(|(name, _)| p[after..].starts_with(name.as_bytes()))
        else {
            break;
        };
        let mut next = after + name.len();
        match item {
            Item::Flag => {}
            Item::Newline { crlf } => lead.crlf = *crlf,
            Item::Heap | Item::Limit => {
                let digits = p[next..].iter().take_while(|b| b.is_ascii_digit()).count();
                let value = std::str::from_utf8(&p[next..next + digits])
                    .ok()
                    .and_then(|d| d.parse::<u32>().ok());
                let (Some(value), Some(b')')) = (value, p.get(next + digits)) else {
                    break;
                };
                if matches!(item, Item::Heap) {
                    lead.heap = Some(value);
                }
                next += digits + 1;
            }
        }
        lead.end = next;
    }
    lead
}

/// The pattern as the `&str` the `pcre2` crate takes, meaning the same bytes to PCRE2.
/// A pattern that is not UTF-8 (one taken from a binary payload) has each byte of an
/// invalid sequence written as a `\xHH` escape — outside `\Q…\E`, which is closed
/// around it and reopened; a byte escaped by a backslash keeps that backslash.
fn as_text(pattern: &[u8]) -> Cow<'_, str> {
    if let Ok(s) = std::str::from_utf8(pattern) {
        return Cow::Borrowed(s);
    }
    let mut out = String::with_capacity(pattern.len() + 16);
    let (mut quoted, mut escaped) = (false, false);
    for chunk in pattern.utf8_chunks() {
        for c in chunk.valid().chars() {
            out.push(c);
            if quoted {
                // Inside `\Q…\E` every backslash looks at the next character alone.
                quoted = !(escaped && c == 'E');
                escaped = c == '\\';
            } else if escaped {
                quoted = c == 'Q';
                escaped = false;
            } else {
                escaped = c == '\\';
            }
        }
        for b in chunk.invalid() {
            use std::fmt::Write as _;
            if quoted {
                let _ = write!(out, "\\E\\x{b:02X}\\Q");
            } else if escaped {
                let _ = write!(out, "x{b:02X}");
            } else {
                let _ = write!(out, "\\x{b:02X}");
            }
            escaped = false;
        }
    }
    out.into()
}
