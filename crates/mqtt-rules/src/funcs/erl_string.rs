//! Erlang's `string` module, as EMQX's string functions call it on a UTF-8 binary.
//!
//! Erlang counts and matches **grapheme clusters** (Unicode 16 in OTP 28), not code
//! points: `strlen('a\r\nb')` is 3 because `\r\n` is one cluster, and a flag emoji is one.
//! Its searches are subtler than "match at cluster boundaries": they find a candidate by
//! code point and then segment a cluster *starting at the candidate*, as if the text
//! began there (`unicode_util:gc/1`). So `find('a\r\nb', '\n')` matches — `\n` alone is
//! a cluster when segmentation starts on it — while `find('a\r\nb', '\r')` does not —
//! the cluster that starts on `\r` is `\r\n`. Each function here follows the OTP source
//! (`lib/stdlib/src/string.erl`, maint-28) step by step for a binary argument, so these
//! quirks come out the same; the tests pin each against EMQX 6.3.1.

use unicode_segmentation::UnicodeSegmentation;

/// The grapheme cluster that starts `s`, segmented as if `s` began there
/// (`unicode_util:gc/1`). Empty only for empty `s`.
pub(crate) fn gc(s: &str) -> &str {
    s.graphemes(true).next().unwrap_or("")
}

/// `string:length/1`: the number of grapheme clusters.
pub(crate) fn length(s: &str) -> usize {
    s.graphemes(true).count()
}

/// The byte offset of the `n`th grapheme cluster (`s.len()` when there are fewer).
fn cluster_offset(s: &str, n: usize) -> usize {
    s.grapheme_indices(true).nth(n).map_or(s.len(), |(i, _)| i)
}

/// `string:slice/2,3`: `len` clusters (all, for `None`) from cluster `start`.
pub(crate) fn slice(s: &str, start: usize, len: Option<usize>) -> &str {
    let rest = &s[cluster_offset(s, start)..];
    match len {
        None => rest,
        Some(n) => &rest[..cluster_offset(rest, n)],
    }
}

/// `string:reverse/1` followed by `iolist_to_binary/1`, which is what EMQX's `reverse`
/// returns: the clusters in reverse order, each code point written as ONE BYTE. A code
/// point above 255 cannot be (`badarg`, `None` here); one in 128..=255 comes out as its
/// Latin-1 byte, so `reverse('aé')` is the bytes `E9 61`, not UTF-8.
pub(crate) fn reverse_latin1(s: &str) -> Option<Vec<u8>> {
    let clusters: Vec<&str> = s.graphemes(true).collect();
    let mut out = Vec::with_capacity(s.len());
    for g in clusters.into_iter().rev() {
        for c in g.chars() {
            out.push(u8::try_from(u32::from(c)).ok()?);
        }
    }
    Some(out)
}

/// `unicode_util:whitespace/0`: what `trim/1`, `ltrim/1` and `rtrim/1` remove — Unicode's
/// `Pattern_White_Space`, with `\r\n` as one cluster. Not `char::is_whitespace`: a
/// no-break space stays, a left-to-right mark goes.
pub(crate) const WHITESPACE: &[&str] = &[
    "\r\n", "\t", "\n", "\u{0B}", "\u{0C}", "\r", " ", "\u{85}", "\u{200E}", "\u{200F}",
    "\u{2028}", "\u{2029}",
];

/// `string:trim(S, leading, Seps)`: drop leading clusters that are separators.
pub(crate) fn trim_leading<'a>(s: &'a str, seps: &[&str]) -> &'a str {
    let mut at = 0;
    while at < s.len() {
        let g = gc(&s[at..]);
        if !seps.contains(&g) {
            break;
        }
        at += g.len();
    }
    &s[at..]
}

/// `string:trim(S, trailing, Seps)` on a binary (`trim_t/3`): find the first code point
/// that starts a separator and whose cluster is one; if separators run from there to the
/// end, cut there; otherwise resume the search after the first non-separator.
pub(crate) fn trim_trailing<'a>(s: &'a str, seps: &[&str]) -> &'a str {
    let firsts: Vec<char> = seps.iter().filter_map(|p| p.chars().next()).collect();
    let mut from = 0;
    loop {
        // bin_search_loop: the next separator cluster at or after `from`.
        let cut = loop {
            let Some(off) = s[from..].find(|c| firsts.contains(&c)) else {
                return s;
            };
            let at = from + off;
            let g = gc(&s[at..]);
            if seps.contains(&g) {
                break at;
            }
            from = at + g.len();
        };
        // bin_search_inv: does every cluster from `cut` on separate?
        let mut at = cut;
        loop {
            if at >= s.len() {
                return &s[..cut];
            }
            let g = gc(&s[at..]);
            if !seps.contains(&g) {
                break;
            }
            at += g.len();
        }
        from = at;
    }
}

/// `string:trim(S, both, Seps)`.
pub(crate) fn trim<'a>(s: &'a str, seps: &[&str]) -> &'a str {
    trim_trailing(trim_leading(s, seps), seps)
}

/// Where `needle` (non-empty) matches `s` at byte `at`, and the byte it ends at:
/// `prefix_1/2` — every code point but the last compared as a code point, the last as
/// the whole cluster that starts on it.
fn match_at(s: &str, at: usize, needle: &str) -> Option<usize> {
    let (last_at, last) = needle.char_indices().last()?;
    let head = &needle[..last_at];
    if !s[at..].starts_with(head) {
        return None;
    }
    let tail = at + head.len();
    let g = gc(&s[tail..]);
    (g.len() == last.len_utf8() && g.starts_with(last)).then(|| tail + g.len())
}

/// `bin_search_str/4`: the first match of `needle` (non-empty) at or after byte `from`,
/// as `(start, end)`. A candidate that fails moves the search on by one code point.
fn search(s: &str, from: usize, needle: &str) -> Option<(usize, usize)> {
    let first = needle.chars().next()?;
    let mut from = from;
    loop {
        let at = from + s.get(from..)?.find(first)?;
        if let Some(end) = match_at(s, at, needle) {
            return Some((at, end));
        }
        from = at + first.len_utf8();
    }
}

/// The last match, searching on one code point past each match (so matches may overlap,
/// as `find_r/3` and `split_1/6` with `trailing` do).
fn search_last(s: &str, needle: &str) -> Option<(usize, usize)> {
    let step = needle.chars().next()?.len_utf8();
    let mut last = None;
    let mut from = 0;
    while let Some(m) = search(s, from, needle) {
        last = Some(m);
        from = m.0 + step;
    }
    last
}

/// `string:find(S, Needle, Dir)`, with `None` for `nomatch`.
pub(crate) fn find<'a>(s: &'a str, needle: &str, trailing: bool) -> Option<&'a str> {
    if needle.is_empty() {
        return Some(s);
    }
    let m = if trailing {
        search_last(s, needle)
    } else {
        search(s, 0, needle)
    };
    m.map(|(at, _)| &s[at..])
}

/// Where `string:split/3` and `string:replace/4` act.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Where {
    Leading,
    Trailing,
    All,
}

/// `string:split(S, Needle, Where)`, empty parts included.
pub(crate) fn split<'a>(s: &'a str, needle: &str, at: Where) -> Vec<&'a str> {
    if needle.is_empty() {
        return vec![s];
    }
    match at {
        Where::Leading => match search(s, 0, needle) {
            Some((a, e)) => vec![&s[..a], &s[e..]],
            None => vec![s],
        },
        Where::Trailing => match search_last(s, needle) {
            Some((a, e)) => vec![&s[..a], &s[e..]],
            None => vec![s],
        },
        Where::All => {
            let mut parts = Vec::new();
            let mut from = 0;
            while let Some((a, e)) = search(s, from, needle) {
                parts.push(&s[from..a]);
                from = e;
            }
            parts.push(&s[from..]);
            parts
        }
    }
}

/// `string:lexemes(binary_to_list(S), binary_to_list(Seps))` — EMQX's `tokens`. Both
/// arguments become lists of BYTES, so this works on bytes read as Latin-1 characters:
/// every byte of a separator string is a separator on its own, and the only multi-byte
/// cluster is `\r\n`. `extra` adds whole clusters (`nocrlf` adds `\r`, `\n` and `\r\n`).
pub(crate) fn lexemes_latin1<'a>(s: &'a [u8], seps: &[u8], extra: &[&[u8]]) -> Vec<&'a [u8]> {
    let gcs: Vec<&[u8]> = seps
        .iter()
        .map(std::slice::from_ref)
        .chain(extra.iter().copied())
        .collect();
    if s.is_empty() {
        return Vec::new();
    }
    if gcs.is_empty() {
        return vec![s];
    }
    let firsts: Vec<u8> = gcs.iter().filter_map(|g| g.first().copied()).collect();
    let cluster = |at: usize| -> &[u8] {
        if s[at] == b'\r' && s.get(at + 1) == Some(&b'\n') {
            &s[at..at + 2]
        } else {
            &s[at..=at]
        }
    };
    let is_sep = |at: usize| firsts.contains(&s[at]) && gcs.contains(&cluster(at));
    let mut out = Vec::new();
    let mut at = 0;
    while at < s.len() {
        if is_sep(at) {
            at += cluster(at).len();
            continue;
        }
        // lexeme_pick: a byte that cannot start a separator is taken alone; one that can
        // is taken with its cluster when the cluster is not a separator.
        let start = at;
        while at < s.len() && !is_sep(at) {
            at += if firsts.contains(&s[at]) {
                cluster(at).len()
            } else {
                1
            };
        }
        out.push(&s[start..at]);
    }
    out
}

/// `string:lowercase/1`: each code point's full lowercase mapping, with no context —
/// unlike `str::to_lowercase`, a final `Σ` becomes `σ`, not `ς`.
pub(crate) fn lowercase(s: &str) -> String {
    s.chars().flat_map(char::to_lowercase).collect()
}
