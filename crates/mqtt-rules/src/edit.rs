//! Per-rule edits of a rules file's text (ADR 0084): insert, update or delete one
//! `[rules.<id>]` table and keep every other byte of the file.
//!
//! The file stays the single source of truth, comments and all, so an edit changes only
//! what it was asked to:
//!
//! - an **update** writes into the rule's existing table, and only the values that
//!   differ: an unchanged `sql = '''…'''` keeps its form, a comment between keys stays,
//!   and a key the table lacked (`enable = false`, say) is added after its last key;
//! - an **insert** appends the new table at the end of the file;
//! - a **delete** removes the table's header and keys. The comment lines above a header
//!   belong, in TOML, to the table below them — they are often a file header or a
//!   section banner — so they move onto the next table (or the end of the file) and stay.
//!   That includes the deleted rule's own comment block, which is left for its author to
//!   remove. Blank lines that would end the file do not stay, so a delete undoes an
//!   insert byte for byte, unless the file already ended with blank lines: those go too.
//!
//! Only files written as `[rules.<id>]` tables are edited this way; a rule written with
//! dotted keys, as an inline table or with `[[rules.<id>.actions]]` is refused
//! ([`EditError::LayoutUnsupported`]), and so is a file the editor would not reproduce
//! byte for byte (CRLF line endings, say). Such a file can still be replaced whole.
//!
//! Before an edit is returned, the old and the new text are both parsed and compared:
//! every other rule must be unchanged and the edited one must be exactly what was asked
//! ([`EditError::Failed`] otherwise). Whether the new file is a valid rule set is the
//! caller's question — [`RuleSet::parse`](crate::RuleSet::parse) answers it.

use serde::Deserialize;
use toml_edit::{Array, DocumentMut, InlineTable, Item, Table, Value};

use crate::{valid_rule_id, FileSchema, RuleSchema};

/// One rule as a per-rule edit writes it: every field, as `PUT /admin/v1/rule` takes
/// them (JSON deserializes into it; `null` and numbers beyond 64 bits are refused there).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleEdit {
    /// The rule's SQL.
    pub sql: String,
    /// Its actions, as the rules file writes them.
    pub actions: Vec<toml::Value>,
    /// Its description.
    pub description: String,
    /// Whether it runs.
    pub enable: bool,
}

/// Why an edit was not made. Nothing is ever half-applied: an error means no new text.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EditError {
    /// The id is not a rule id (see [`valid_rule_id`]).
    #[error(
        "`{0}` is not a rule id: a letter or `_` followed by up to 63 letters, digits, `_` or `-`"
    )]
    InvalidId(String),
    /// The file being edited does not parse as a rules file.
    #[error("the rules file does not parse: {0}")]
    FileInvalid(String),
    /// The file is written in a way a per-rule edit does not handle.
    #[error("{0}; replace the whole file instead")]
    LayoutUnsupported(String),
    /// A delete named a rule the file does not define.
    #[error("the rules file has no rule `{0}`")]
    NoSuchRule(String),
    /// A value of the rule cannot be written as asked.
    #[error("{0}")]
    Unrepresentable(String),
    /// The edited text failed the before-and-after comparison; nothing may be written.
    #[error("the edit would not change only rule `{id}`: {why}")]
    Failed {
        /// The rule being edited.
        id: String,
        /// What the comparison found.
        why: String,
    },
}

/// Insert rule `id` into `source`, or update it in place, so that it is `rule`; returns
/// the new text. An update that changes nothing returns `source` unchanged.
pub fn put_rule(source: &str, id: &str, rule: &RuleEdit) -> Result<String, EditError> {
    let (old, mut doc) = open(source, id)?;
    let rules = rules_table(&mut doc)?;
    if let (Some(Item::Table(table)), Some(was)) = (rules.get_mut(id), old.rules.get(id)) {
        update(table, was, rule)?;
    } else {
        let position = next_position(rules);
        let mut table = Table::new();
        table.set_position(Some(position));
        if !rule.description.is_empty() {
            table.insert(
                "description",
                Item::Value(Value::from(rule.description.as_str())),
            );
        }
        if !rule.enable {
            table.insert("enable", Item::Value(Value::from(false)));
        }
        table.insert("sql", Item::Value(sql_value(&rule.sql)));
        table.insert("actions", Item::Value(actions_value(&rule.actions)?));
        rules.insert(id, Item::Table(table));
        // At the very end of the file: after anything that trailed the last table, one
        // blank line below the text before it (none in an empty file).
        let mut prefix = raw(Some(doc.trailing())).to_string();
        if !prefix.is_empty() && !prefix.ends_with('\n') {
            prefix.push('\n');
        }
        if !source.trim().is_empty() && !prefix.ends_with("\n\n") {
            prefix.push('\n');
        }
        doc.set_trailing("");
        if let Some(Item::Table(table)) = rules_table(&mut doc)?.get_mut(id) {
            table.decor_mut().set_prefix(prefix);
        }
    }
    let text = ending_like(source, doc.to_string());
    check(source, &old, &text, id, Some(rule))?;
    Ok(text)
}

/// Delete rule `id` from `source`; returns the new text.
pub fn delete_rule(source: &str, id: &str) -> Result<String, EditError> {
    let (old, mut doc) = open(source, id)?;
    if !old.rules.contains_key(id) {
        return Err(EditError::NoSuchRule(id.to_string()));
    }
    let rules = rules_table(&mut doc)?;
    let Some(Item::Table(removed)) = rules.remove(id) else {
        return Err(EditError::NoSuchRule(id.to_string()));
    };
    let above = raw(removed.decor().prefix()).to_string();
    let at = removed.position().unwrap_or(0);
    // The table that now follows in document order: a sibling, or `[rules]` itself when
    // its header comes later. Its header gets the comments that stood above the removed
    // one, so a file header or a section banner stays where it was.
    let next = rules
        .iter()
        .filter_map(|(k, item)| Some((item.as_table()?.position()?, k.to_string())))
        .filter(|(p, _)| *p > at)
        .min();
    let parent_next = rules
        .position()
        .filter(|p| *p > at && next.as_ref().is_none_or(|(n, _)| p < n));
    if parent_next.is_some() {
        let decor = rules.decor_mut();
        let below = raw(decor.prefix()).to_string();
        decor.set_prefix(above + &below);
    } else if let Some((_, key)) = next {
        if let Some(Item::Table(t)) = rules.get_mut(&key) {
            let below = raw(t.decor().prefix()).to_string();
            t.decor_mut().set_prefix(above + &below);
        }
    } else {
        // Nothing follows: the comments end the file, without the blank lines below them
        // that set the rule apart (an insert put one there).
        let trailing = raw(Some(doc.trailing())).to_string();
        let above = if trailing.trim().is_empty() {
            without_blank_end(&above)
        } else {
            &above
        };
        doc.set_trailing(format!("{above}{trailing}"));
    }
    let text = ending_like(source, doc.to_string());
    check(source, &old, &text, id, None)?;
    Ok(text)
}

/// `text` less the blank lines it ends with.
fn without_blank_end(text: &str) -> &str {
    let mut kept = text;
    while let Some(rest) = kept.strip_suffix('\n') {
        let line = rest.rfind('\n').map_or(0, |i| i + 1);
        if !rest[line..].trim().is_empty() {
            break;
        }
        kept = &rest[..line];
    }
    kept
}

/// The editor's `text`, ending as `source` did: the editor ends the last line with a
/// newline, which a file that had none does not get.
fn ending_like(source: &str, mut text: String) -> String {
    if !source.is_empty() && !source.ends_with('\n') && text.ends_with('\n') {
        text.pop();
    }
    text
}

/// Parse `source` both ways for an edit of rule `id`: as a rules file (for the
/// comparison) and as an editable document whose layout an edit can handle.
fn open(source: &str, id: &str) -> Result<(FileSchema, DocumentMut), EditError> {
    if !valid_rule_id(id) {
        return Err(EditError::InvalidId(id.to_string()));
    }
    let old: FileSchema =
        toml::from_str(source).map_err(|e| EditError::FileInvalid(e.to_string()))?;
    let doc: DocumentMut = source
        .parse()
        .map_err(|e: toml_edit::TomlError| EditError::FileInvalid(e.to_string()))?;
    // An edit promises every other byte unchanged; the editor itself must keep that. It
    // ends the last line with a newline if the file did not, which [`ending_like`] takes
    // back; anything else (CRLF line endings become LF) would rewrite lines nobody edited.
    let printed = doc.to_string();
    if printed != source && printed.strip_suffix('\n') != Some(source) {
        return Err(EditError::LayoutUnsupported(
            "the rules file does not come back byte for byte from the TOML editor \
             (CRLF line endings?)"
                .into(),
        ));
    }
    match doc.get("rules") {
        None => {}
        Some(Item::Table(rules)) if !rules.is_dotted() => {
            for (key, item) in rules {
                let plain = match item {
                    Item::Table(t) => !t.is_dotted() && t.iter().all(|(_, v)| v.is_value()),
                    _ => false,
                };
                if !plain {
                    return Err(EditError::LayoutUnsupported(format!(
                        "rule `{key}` is not written as a [rules.{key}] table of plain keys \
                         (dotted keys, an inline table or a [[rules.{key}.actions]] table)"
                    )));
                }
            }
        }
        Some(_) => {
            return Err(EditError::LayoutUnsupported(
                "the rules are not written as [rules.<id>] tables (dotted keys or an inline \
                 table)"
                    .into(),
            ))
        }
    }
    Ok((old, doc))
}

/// The `rules` table, created (implicit, so it prints no header) when the file has none.
fn rules_table(doc: &mut DocumentMut) -> Result<&mut Table, EditError> {
    doc.entry("rules")
        .or_insert_with(|| {
            let mut t = Table::new();
            t.set_implicit(true);
            Item::Table(t)
        })
        .as_table_mut()
        .ok_or_else(|| EditError::LayoutUnsupported("`rules` is not a table".into()))
}

/// The document position after every table's, for a table appended at the end.
fn next_position(rules: &Table) -> isize {
    rules
        .iter()
        .filter_map(|(_, item)| item.as_table()?.position())
        .chain(rules.position())
        .max()
        .unwrap_or(0)
        + 1
}

/// A decor string's text; an unset one is empty (every parsed decor is set).
fn raw(s: Option<&toml_edit::RawString>) -> &str {
    s.and_then(toml_edit::RawString::as_str).unwrap_or("")
}

/// Write into an existing rule's table the values of `rule` that differ from `was`.
fn update(table: &mut Table, was: &RuleSchema, rule: &RuleEdit) -> Result<(), EditError> {
    if was.sql != rule.sql {
        set(table, "sql", sql_value(&rule.sql));
    }
    if was.actions != rule.actions {
        set(table, "actions", actions_value(&rule.actions)?);
    }
    if was.description != rule.description {
        set(table, "description", Value::from(rule.description.as_str()));
    }
    if was.enable != rule.enable {
        set(table, "enable", Value::from(rule.enable));
    }
    Ok(())
}

/// Set `key` to `value`. An existing value is replaced in place, keeping its key, the
/// comments above it and its trailing comment; a new key goes after the last one.
fn set(table: &mut Table, key: &str, mut value: Value) {
    if let Some(item) = table.get_mut(key) {
        if let Some(old) = item.as_value() {
            *value.decor_mut() = old.decor().clone();
        }
        *item = Item::Value(value);
    } else {
        table.insert(key, Item::Value(value));
    }
}

/// A rule's SQL as the rules files write it: a `'…'` literal on one line, a `'''`
/// block over several (no escapes, so a regex reads as written), and an escaped basic
/// string only for text neither can hold. Every form is read back before it is used.
fn sql_value(sql: &str) -> Value {
    let mut forms = Vec::new();
    if !sql.contains(['\n', '\'']) {
        forms.push(format!("'{sql}'"));
    }
    if !sql.contains("'''") && !sql.ends_with('\'') {
        // A newline right after the opening `'''` is not part of the string.
        forms.push(format!("'''\n{sql}'''"));
    }
    forms
        .into_iter()
        .filter_map(|f| f.parse::<Value>().ok())
        .find(|v| v.as_str() == Some(sql))
        .unwrap_or_else(|| Value::from(sql))
}

/// A rule's actions, one inline table per line, as the rules files write them.
fn actions_value(actions: &[toml::Value]) -> Result<Value, EditError> {
    let mut array = Array::new();
    for a in actions {
        let mut v = edit_value(a)?;
        v.decor_mut().set_prefix("\n  ");
        v.decor_mut().set_suffix("");
        array.push_formatted(v);
    }
    if !array.is_empty() {
        array.set_trailing_comma(true);
        array.set_trailing("\n");
    }
    Ok(Value::Array(array))
}

/// A value from the rules schema as an editable one. Table keys are written in the order
/// the documentation uses (`function`, then `args`; `topic`, `qos`, `retain`,
/// `payload`, …), the rest by name.
fn edit_value(v: &toml::Value) -> Result<Value, EditError> {
    Ok(match v {
        toml::Value::String(s) => Value::from(s.as_str()),
        toml::Value::Integer(n) => Value::from(*n),
        toml::Value::Float(f) => Value::from(*f),
        toml::Value::Boolean(b) => Value::from(*b),
        toml::Value::Datetime(d) => {
            return Err(EditError::Unrepresentable(format!(
                "a date-time ({d}) has no place in a rule's actions"
            )))
        }
        toml::Value::Array(items) => {
            let mut array = Array::new();
            for item in items {
                array.push(edit_value(item)?);
            }
            Value::Array(array)
        }
        toml::Value::Table(map) => {
            const ORDER: &[&str] = &[
                "function",
                "args",
                "topic",
                "qos",
                "retain",
                "payload",
                "user_properties",
                "mqtt_properties",
                "direct_dispatch",
            ];
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_by_key(|k| {
                (
                    ORDER.iter().position(|o| o == k).unwrap_or(ORDER.len()),
                    k.as_str(),
                )
            });
            let mut table = InlineTable::new();
            for (k, v) in keys.into_iter().filter_map(|k| Some((k, map.get(k)?))) {
                table.insert(k, edit_value(v)?);
            }
            Value::InlineTable(table)
        }
    })
}

/// The before-and-after comparison: `text` must define every rule of `old` but `id`
/// unchanged, no new rule but `id`, and `id` as `want` (absent for a delete).
fn check(
    source: &str,
    old: &FileSchema,
    text: &str,
    id: &str,
    want: Option<&RuleEdit>,
) -> Result<(), EditError> {
    let failed = |why: String| EditError::Failed {
        id: id.to_string(),
        why,
    };
    if text == source {
        // Nothing changed, which is right only for an update to what is already there.
        return match (want, old.rules.get(id)) {
            (Some(w), Some(r)) if *r == schema(w) => Ok(()),
            _ => Err(failed("the text did not change".into())),
        };
    }
    let new: FileSchema =
        toml::from_str(text).map_err(|e| failed(format!("the edited text does not parse: {e}")))?;
    for (k, r) in &old.rules {
        if k != id && new.rules.get(k) != Some(r) {
            return Err(failed(format!("rule `{k}` would change")));
        }
    }
    if let Some(k) = new
        .rules
        .keys()
        .find(|k| *k != id && !old.rules.contains_key(*k))
    {
        return Err(failed(format!("rule `{k}` would appear")));
    }
    match (want, new.rules.get(id)) {
        (Some(w), Some(r)) if *r == schema(w) => Ok(()),
        (None, None) => Ok(()),
        (Some(_), _) => Err(failed("it would not read back as requested".into())),
        (None, Some(_)) => Err(failed("it would still be there".into())),
    }
}

fn schema(r: &RuleEdit) -> RuleSchema {
    RuleSchema {
        sql: r.sql.clone(),
        actions: r.actions.clone(),
        enable: r.enable,
        description: r.description.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuleSet;

    /// The demo's rules file: 22 rules in three sections, each section opened by a
    /// banner, the file by a long header, every rule by its own comment block.
    fn demo() -> String {
        std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../demo/rules/rules.toml"
        ))
        .expect("demo/rules/rules.toml is readable from the crate")
    }

    /// Where rule `id`'s header starts and its last key's line ends.
    fn header_to_last_key(text: &str, id: &str) -> std::ops::Range<usize> {
        let start = text
            .find(&format!("\n[rules.{id}]\n"))
            .expect("the rule's header")
            + 1;
        // Every demo rule ends with its `actions = [ … ]` block.
        let end = start + text[start..].find("\n]\n").expect("its actions") + 3;
        start..end
    }

    fn console(sql: &str) -> RuleEdit {
        RuleEdit {
            sql: sql.into(),
            actions: vec![toml::Value::Table(toml::map::Map::from_iter([(
                "function".to_string(),
                toml::Value::from("console"),
            )]))],
            description: String::new(),
            enable: true,
        }
    }

    /// A delete takes out exactly the header and the keys. The comment lines above a
    /// header belong to that table in TOML, so deleting the file's first rule took the
    /// whole file header with it, and the first rule of a section its banner.
    #[test]
    fn deleting_a_rule_removes_exactly_its_header_and_keys() {
        let text = demo();
        for id in ["power_pv_sunspec", "home_p1_decode", "car_obd_decode"] {
            let span = header_to_last_key(&text, id);
            let edited = delete_rule(&text, id).unwrap_or_else(|e| panic!("{id}: {e}"));
            assert_eq!(
                edited,
                format!("{}{}", &text[..span.start], &text[span.end..]),
                "{id}"
            );
            let set = RuleSet::parse(&edited).unwrap().rules;
            assert_eq!(set.len(), 21);
            assert!(set.get(id).is_none());
        }
        assert!(delete_rule(&text, "power_pv_sunspec")
            .unwrap()
            .starts_with("# The rule-engine demo's rules: power plants, homes and cars\n"));
    }

    /// The last rule has no table after it: its comments go to the end of the file.
    #[test]
    fn deleting_the_last_rule_keeps_what_stood_above_it() {
        let text = "# head\n\n[rules.a]\nsql = 'SELECT 1 FROM \"t\"'\n\n# about b\n\
                    [rules.b]\nsql = 'SELECT 2 FROM \"t\"'\n# tail\n";
        assert_eq!(
            delete_rule(text, "b").unwrap(),
            "# head\n\n[rules.a]\nsql = 'SELECT 1 FROM \"t\"'\n\n# about b\n# tail\n"
        );
        assert_eq!(
            delete_rule(text, "a").unwrap(),
            "# head\n\n\n# about b\n[rules.b]\nsql = 'SELECT 2 FROM \"t\"'\n# tail\n"
        );
        // With an explicit [rules] header first, the comments stay below it.
        let text = "[rules]\n\n# about a\n[rules.a]\nsql = 'SELECT 1 FROM \"t\"'\n";
        assert_eq!(delete_rule(text, "a").unwrap(), "[rules]\n\n# about a\n");
        // ...and one that comes after the rule is the next table in document order.
        let text = "# about a\n[rules.a]\nsql = 'SELECT 1 FROM \"t\"'\n\n# the rest\n[rules]\n\
                    [rules.b]\nsql = 'SELECT 2 FROM \"t\"'\n";
        assert_eq!(
            delete_rule(text, "a").unwrap(),
            "# about a\n\n# the rest\n[rules]\n[rules.b]\nsql = 'SELECT 2 FROM \"t\"'\n"
        );
    }

    /// An update writes only what differs, inside the existing table: an unchanged
    /// multi-line SQL keeps its form, and nothing else in the file moves.
    #[test]
    fn an_update_writes_only_the_values_that_differ() {
        let text = demo();
        let set = RuleSet::parse(&text).unwrap().rules;
        let r = set.get("home_grid_feed").unwrap();
        let mut edit = RuleEdit {
            sql: r.sql().to_string(),
            actions: r.action_specs().to_vec(),
            description: r.description().to_string(),
            enable: r.enabled(),
        };
        assert_eq!(put_rule(&text, "home_grid_feed", &edit).unwrap(), text);

        // Disable it: one line, appended to its table.
        edit.enable = false;
        let span = header_to_last_key(&text, "home_grid_feed");
        let off = put_rule(&text, "home_grid_feed", &edit).unwrap();
        assert_eq!(
            off,
            format!("{}enable = false\n{}", &text[..span.end], &text[span.end..])
        );
        // ...and back: the key stays, now true.
        edit.enable = true;
        let on = put_rule(&off, "home_grid_feed", &edit).unwrap();
        assert_eq!(on, off.replacen("enable = false\n", "enable = true\n", 1));

        // A new description changes that one line; the regex-heavy SQL is untouched.
        edit.description = "Pseudonymous \"grid\" feed".into();
        let described = put_rule(&text, "home_grid_feed", &edit).unwrap();
        assert_eq!(
            described,
            text.replacen(
                &format!("description = \"{}\"", r.description()),
                "description = 'Pseudonymous \"grid\" feed'",
                1
            )
        );
    }

    /// Values the request leaves as they were keep their own form, however unlike the
    /// form an edit would write: only the changed line differs.
    #[test]
    fn unchanged_values_keep_their_own_form() {
        let text = "[rules.r]\nsql = \"SELECT 1 AS \\\"x\\\" FROM \\\"t\\\"\"\n\
                    actions = [{function=\"console\"}]\nenable=true\n";
        let r = RuleSet::parse(text).unwrap().rules;
        let r = r.get("r").unwrap();
        let edit = RuleEdit {
            sql: r.sql().to_string(),
            actions: r.action_specs().to_vec(),
            description: "new".into(),
            enable: true,
        };
        assert_eq!(
            put_rule(text, "r", &edit).unwrap(),
            format!("{text}description = \"new\"\n")
        );
    }

    #[test]
    fn an_update_keeps_comments_and_writes_sql_without_escapes() {
        let text = "[rules.r]\n# what it selects\nsql = '''\nSELECT 1 FROM \"t\"\n'''  # keep me\n\
                    actions = []\n# trailing note\n\n[rules.s]\nsql = 'SELECT 2 FROM \"u\"'\n";
        let mut edit =
            console("SELECT regex_extract(payload, '(\\d+)\\.(\\d+)') AS v\nFROM \"t\"\n");
        edit.description = "pairs".into();
        let edited = put_rule(text, "r", &edit).unwrap();
        assert_eq!(
            edited,
            "[rules.r]\n# what it selects\n\
             sql = '''\nSELECT regex_extract(payload, '(\\d+)\\.(\\d+)') AS v\nFROM \"t\"\n'''  \
             # keep me\nactions = [\n  { function = \"console\" },\n]\ndescription = \"pairs\"\n\
             # trailing note\n\n[rules.s]\nsql = 'SELECT 2 FROM \"u\"'\n"
        );
        RuleSet::parse(&edited).unwrap();
    }

    /// A new rule goes at the very end of the file, after a blank line, in the shape the
    /// shipped files use.
    #[test]
    fn an_insert_appends_the_rule_at_the_end() {
        let text = demo();
        let mut edit = console("SELECT * FROM \"plant/#\"");
        edit.description = "Everything a plant says".into();
        edit.enable = false;
        edit.actions
            .push(toml::Value::Table(toml::map::Map::from_iter([
                ("function".to_string(), toml::Value::from("republish")),
                (
                    "args".to_string(),
                    toml::Value::Table(toml::map::Map::from_iter([
                        ("payload".to_string(), toml::Value::from("${.}")),
                        ("qos".to_string(), toml::Value::from(1)),
                        ("topic".to_string(), toml::Value::from("copy/${topic}")),
                    ])),
                ),
            ])));
        let edited = put_rule(&text, "plant_tap", &edit).unwrap();
        assert_eq!(
            edited,
            format!(
                "{text}\n[rules.plant_tap]\ndescription = \"Everything a plant says\"\n\
                 enable = false\nsql = 'SELECT * FROM \"plant/#\"'\nactions = [\n  \
                 {{ function = \"console\" }},\n  {{ function = \"republish\", args = \
                 {{ topic = \"copy/${{topic}}\", qos = 1, payload = \"${{.}}\" }} }},\n]\n"
            )
        );
        let set = RuleSet::parse(&edited).unwrap().rules;
        assert_eq!(set.len(), 23);
        assert!(!set.get("plant_tap").unwrap().enabled());

        // Into an empty file, and after a file's trailing comments.
        let one = console("SELECT 1 FROM \"t\"");
        let rule = "[rules.a]\nsql = 'SELECT 1 FROM \"t\"'\nactions = [\n  { function = \"console\" },\n]\n";
        assert_eq!(put_rule("", "a", &one).unwrap(), rule);
        assert_eq!(
            put_rule("# my rules\n", "a", &one).unwrap(),
            format!("# my rules\n\n{rule}")
        );
    }

    /// A delete undoes an insert byte for byte: the blank line the insert put above the
    /// new rule goes with it, a trailing comment is where it was, and a file that did not
    /// end with a newline still does not.
    #[test]
    fn inserting_then_deleting_a_rule_gives_back_the_same_file() {
        let one = console("SELECT 1 FROM \"t\"");
        let a = "# my rules\n\n[rules.a]\nsql = 'SELECT 1 FROM \"t\"'\nactions = []\n";
        for text in [
            a.to_string(),
            format!("{a}\n# the end\n"),
            format!("{a}# the end"),
            a.trim_end().to_string(),
            "[rules]\n".to_string(),
            "# no rules yet\n".to_string(),
            String::new(),
            demo(),
        ] {
            let inserted = put_rule(&text, "added", &one).unwrap();
            assert!(inserted.contains("\n[rules.added]\n") || text.is_empty());
            assert_eq!(delete_rule(&inserted, "added").unwrap(), text, "{text:?}");
        }
    }

    /// SQL is written in the plainest form that reads back exactly.
    #[test]
    fn sql_is_written_in_the_plainest_exact_form() {
        let form = |sql: &str| sql_value(sql).to_string();
        assert_eq!(form("SELECT * FROM \"t\""), "'SELECT * FROM \"t\"'");
        assert_eq!(
            form("SELECT 'a' FROM \"t\""),
            "'''\nSELECT 'a' FROM \"t\"'''"
        );
        assert_eq!(
            form("SELECT\n  a\nFROM \"t\"\n"),
            "'''\nSELECT\n  a\nFROM \"t\"\n'''"
        );
        for awkward in [
            "SELECT ''' FROM \"t\"",
            "SELECT 'a'",
            "SELECT \u{1} FROM \"t\"",
            "\nleading newline",
            "",
        ] {
            assert_eq!(sql_value(awkward).as_str(), Some(awkward), "{awkward:?}");
        }
    }

    #[test]
    fn files_an_edit_cannot_keep_intact_are_refused() {
        let one = console("SELECT 1 FROM \"t\"");
        let unsupported = |text: &str| {
            assert!(
                matches!(
                    put_rule(text, "a", &one),
                    Err(EditError::LayoutUnsupported(_))
                ),
                "{text}"
            );
        };
        unsupported("[rules]\nx = { sql = 'SELECT 1 FROM \"t\"' }\n");
        unsupported("rules = { x = { sql = 'SELECT 1 FROM \"t\"' } }\n");
        unsupported("rules.x.sql = 'SELECT 1 FROM \"t\"'\n");
        unsupported("[rules]\nx.sql = 'SELECT 1 FROM \"t\"'\n");
        unsupported(
            "[rules.x]\nsql = 'SELECT 1 FROM \"t\"'\n[[rules.x.actions]]\nfunction = \"console\"\n",
        );
        unsupported("[rules.x]\r\nsql = 'SELECT 1 FROM \"t\"'\r\n");

        // A missing final newline is no reason to refuse, and stays missing.
        let edited = put_rule("[rules.x]\nsql = 'SELECT 1 FROM \"t\"'", "a", &one).unwrap();
        assert!(edited.starts_with("[rules.x]\nsql = 'SELECT 1 FROM \"t\"'\n\n[rules.a]\n"));
        assert!(edited.ends_with(']'), "{edited:?}");

        assert!(matches!(
            put_rule("[rules.x]\nsql = 1\n", "a", &one),
            Err(EditError::FileInvalid(_))
        ));
        assert!(matches!(
            put_rule("[rules.x\n", "a", &one),
            Err(EditError::FileInvalid(_))
        ));
        assert_eq!(
            delete_rule("[rules.x]\nsql = 'SELECT 1 FROM \"t\"'\n", "a"),
            Err(EditError::NoSuchRule("a".into()))
        );
        assert_eq!(
            put_rule("", "1a", &one),
            Err(EditError::InvalidId("1a".into()))
        );
        assert_eq!(
            delete_rule("", "a/b"),
            Err(EditError::InvalidId("a/b".into()))
        );
        let mut dated = one.clone();
        dated.actions = vec![toml::Value::Datetime(
            "1979-05-27T07:32:00Z".parse().unwrap(),
        )];
        assert!(matches!(
            put_rule("", "a", &dated),
            Err(EditError::Unrepresentable(_))
        ));
    }

    /// The admin API's body for one rule deserializes straight into a [`RuleEdit`]: every
    /// field required, nothing else allowed, and no JSON `null` (TOML has none).
    #[test]
    fn a_json_rule_body_is_a_rule_edit() {
        let edit: RuleEdit = serde_json::from_str(
            r#"{"sql":"SELECT 1 FROM \"t\"","description":"d","enable":false,
                "actions":[{"function":"republish","args":{"topic":"o","qos":1,"retain":true}}]}"#,
        )
        .unwrap();
        assert_eq!(edit.actions[0]["args"]["qos"], toml::Value::Integer(1));
        let text = put_rule("", "r", &edit).unwrap();
        let set = RuleSet::parse(&text).unwrap().rules;
        assert_eq!(
            serde_json::to_value(set.get("r").unwrap().action_specs()).unwrap(),
            serde_json::json!([{"function":"republish","args":{"topic":"o","qos":1,"retain":true}}])
        );
        for bad in [
            r#"{"sql":"","description":"","enable":true}"#,
            r#"{"sql":"","description":"","enable":true,"actions":[],"extra":1}"#,
            r#"{"sql":"","description":"","enable":true,"actions":[{"function":null}]}"#,
            r#"{"sql":"","description":"","enable":true,"actions":[{"n":18446744073709551615}]}"#,
        ] {
            assert!(serde_json::from_str::<RuleEdit>(bad).is_err(), "{bad}");
        }
    }

    /// The comparison that guards every edit: any other rule changed, a rule added or
    /// lost, or the target not as asked, and nothing is returned.
    #[test]
    fn the_before_and_after_comparison_catches_any_other_change() {
        let source =
            "[rules.a]\nsql = 'SELECT 1 FROM \"t\"'\n[rules.b]\nsql = 'SELECT 2 FROM \"t\"'\n";
        let old: FileSchema = toml::from_str(source).unwrap();
        let want = RuleEdit {
            sql: "SELECT 3 FROM \"t\"".into(),
            actions: Vec::new(),
            description: String::new(),
            enable: true,
        };
        let ok = "[rules.a]\nsql = 'SELECT 3 FROM \"t\"'\n[rules.b]\nsql = 'SELECT 2 FROM \"t\"'\n";
        check(source, &old, ok, "a", Some(&want)).unwrap();
        let why = |text: &str, want: Option<&RuleEdit>| match check(source, &old, text, "a", want) {
            Err(EditError::Failed { why, .. }) => why,
            other => panic!("{other:?}"),
        };
        let other_changed =
            "[rules.a]\nsql = 'SELECT 3 FROM \"t\"'\n[rules.b]\nsql = 'SELECT 9 FROM \"t\"'\n";
        assert_eq!(why(other_changed, Some(&want)), "rule `b` would change");
        let other_lost = "[rules.a]\nsql = 'SELECT 3 FROM \"t\"'\n";
        assert_eq!(why(other_lost, Some(&want)), "rule `b` would change");
        let added = format!("{ok}[rules.c]\nsql = 'SELECT 4 FROM \"t\"'\n");
        assert_eq!(why(&added, Some(&want)), "rule `c` would appear");
        assert_eq!(
            why(&source.replace("1 FROM", "5 FROM"), Some(&want)),
            "it would not read back as requested"
        );
        assert_eq!(why(ok, None), "it would still be there");
        assert_eq!(why(source, Some(&want)), "the text did not change");
        assert!(why("[rules.a\n", Some(&want)).starts_with("the edited text does not parse"));
    }
}
