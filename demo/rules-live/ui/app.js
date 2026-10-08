// The live rules demo's page (demo/rules-live/README.md). server.py serves it, passes
// /api/* on to the broker's admin API, and streams MQTT to it as server-sent events.
//
// Anyone on the broker can publish what this page shows, so every string from MQTT or the
// admin API reaches the DOM as text (textContent, or a text node), never as HTML.
"use strict";

const RULE_ID = /^[A-Za-z_][A-Za-z0-9_-]{0,63}$/;
const DEVICE_ROOTS = ["plant", "home", "vehicle"];
const KEEP_DATA = 500; // messages kept for the filter
const SHOW_DATA = 200; // messages shown
const KEEP_TRACE = 20; // trace records kept per rule, and as many no_result records again
const RATE_WINDOW = 10000; // ms of counts a rate is taken over

const NO_RESULT = "FROM matched, but WHERE was false (or FOREACH produced nothing): nothing was published.";

// New rule's example; its id is demo_rule_<n>, the first one free.
const NEW_RULE = {
  description: "Grid frequency more than 20 mHz off 50 Hz",
  enable: true,
  sql: [
    "SELECT",
    "  topic(2) AS site,",
    "  payload.Hz AS hz,",
    "  round((payload.Hz - 50) * 1000) AS deviation_mhz",
    'FROM "plant/+/poc/grid"',
    "WHERE is_num(payload.Hz) AND (payload.Hz < 49.98 OR payload.Hz > 50.02)",
    "",
  ].join("\n"),
  actions: [
    { function: "republish", args: { topic: "kpi/demo/${site}/frequency", qos: 0, payload: "${.}" } },
  ],
};

const state = {
  list: null, // the last GET /api/rules answer
  stats: new Map(), // rule id -> its latest $SYS record
  history: new Map(), // rule id -> [{t, matched, passed}], the last RATE_WINDOW of its counts
  traces: new Map(), // rule id -> its latest trace records, newest first
  traceItems: new WeakMap(), // trace record -> its item on the page
  summary: null, // the latest $SYS summary
  summaryAt: 0, // when it arrived (ms)
  interval: 2, // seconds between summaries
  mqttUpSince: 0, // when the server's MQTT connection came up, or 0
  selected: null, // the rule in the editor; null for a new one
  editorDigest: null, // the rules file the editor's text came from: its writes' if_match
  editorBase: undefined, // the rule as it was in that file (see followFile); undefined: not known yet
  awaiting: null, // {digest, out}: an Apply waiting to see its digest on $SYS
  fileDigest: null, // the digest the whole-file editor was loaded at
  data: [], // recent device and derived messages, oldest first
  es: null,
  banner: null, // why the banner shows: "predates", "quiet", "admin"
  refresh: 0,
};

const $ = (id) => document.getElementById(id);

function el(tag, text, className) {
  const e = document.createElement(tag);
  if (text !== undefined && text !== null) e.textContent = String(text);
  if (className) e.className = className;
  return e;
}

function short(digest) {
  return typeof digest === "string" ? digest.slice(0, 12) : "-";
}

function clock(ms) {
  return new Date(ms).toLocaleTimeString([], { hour12: false });
}

function ago(when) {
  const ms = typeof when === "string" ? Date.parse(when) : when;
  if (!ms) return "-";
  const s = Math.max(0, Math.round((Date.now() - ms) / 1000));
  if (s < 60) return `${s} s ago`;
  if (s < 3600) return `${Math.floor(s / 60)} min ago`;
  return clock(ms);
}

// A topic or filter as text that may wrap after each "/".
function topicText(parent, text) {
  text.split("/").forEach((level, i) => {
    if (i) parent.append("/", document.createElement("wbr"));
    parent.append(level);
  });
  return parent;
}

function count(n) {
  return typeof n === "number" ? n.toLocaleString() : "-";
}

function mqttMatch(filter, topic) {
  if (topic.startsWith("$") && (filter.startsWith("+") || filter.startsWith("#"))) return false;
  const f = filter.split("/");
  const t = topic.split("/");
  for (let i = 0; i < f.length; i++) {
    if (f[i] === "#") return true;
    if (i >= t.length || (f[i] !== "+" && f[i] !== t[i])) return false;
  }
  return f.length === t.length;
}

// ---- the admin API, through server.py ------------------------------------------------

async function api(method, path, body) {
  const init = { method, headers: {} };
  if (method !== "GET") {
    // server.py refuses a write without these: a cross-site page cannot send them.
    init.headers["Content-Type"] = "application/json";
    init.headers["X-Rules-UI"] = "1";
    if (body !== undefined) init.body = JSON.stringify(body);
  }
  let res;
  try {
    res = await fetch(path, init);
  } catch (e) {
    return { status: 0, body: { error: { code: "network", message: String(e) } } };
  }
  let json;
  try {
    json = await res.json();
  } catch {
    json = { error: { code: "bad-answer", message: `HTTP ${res.status} without a JSON body` } };
  }
  return { status: res.status, body: json };
}

function setBanner(reason, text) {
  const b = $("banner");
  state.banner = text ? reason : null;
  b.textContent = text || "";
  b.hidden = !text;
}

async function loadRules() {
  const r = await api("GET", "/api/rules");
  if (r.status === 404) {
    setBanner("predates", "This broker predates ADR 0084: its admin API has no rules endpoints. " +
      "Rebuild it with demo/rules-live/up.sh, which always builds from this checkout.");
    return;
  }
  if (r.status !== 200) {
    setBanner("admin", `The admin API did not answer the rules list: ${errorText(r)}`);
    return;
  }
  if (state.banner === "admin" || state.banner === "predates") setBanner(null, "");
  state.list = r.body;
  renderHeader();
  renderRules();
  followFile();
}

function scheduleRefresh() {
  clearTimeout(state.refresh);
  state.refresh = setTimeout(loadRules, 300);
}

function errorText(r) {
  const e = r.body && r.body.error;
  return e ? `${e.code}: ${e.message}` : `HTTP ${r.status}`;
}

// A field an error answer carries beside its code, or inside the error object.
function errorField(r, name) {
  const b = r.body || {};
  return b[name] !== undefined ? b[name] : (b.error || {})[name];
}

// ---- header and rules table ------------------------------------------------------------

function renderHeader() {
  const s = state.summary;
  const l = state.list;
  $("s-node").textContent = (s && s.node) || (l && l.node) || "-";
  const running = (s && s.digest) || (l && l.digest);
  $("s-digest").textContent = short(running);
  if (l) {
    const same = l.file_digest === running;
    $("s-file").textContent = `${short(l.file_digest)} ${same ? "(running)" : "(differs from the running rules)"}`;
  }
  if (s) {
    $("s-count").textContent = `${s.enabled} of ${s.rules} enabled`;
    $("s-trace").textContent = s.trace
      ? `on, ${s.trace_rate}/s per rule${s.trace_dropped ? `, ${count(s.trace_dropped)} dropped` : ""}`
      : "off";
  } else if (l && Array.isArray(l.rules)) {
    $("s-count").textContent = `${l.rules.filter((r) => r.enabled).length} of ${l.rules.length} enabled`;
  }
  renderTraceHead();
  const reload = (s && s.reload) || (l && l.reload);
  if (reload) {
    const outcome = reload.applied ? "applied" : `rejected (${reload.error_kind || "error"})`;
    // Repeats count the same attempt failing again; every success would match the last.
    const repeats = !reload.applied && reload.repeats ? `, ${reload.repeats + 1} times` : "";
    $("s-reload").textContent = `${reload.trigger}: ${outcome}${repeats}, ${ago(reload.at)}`;
    $("s-reload").title = reload.error || "";
  }
}

function renderRules() {
  const rules = (state.list && state.list.rules) || [];
  const body = $("rules").tBodies[0];
  // A rebuilt row is a new button: keyboard focus on the old one would fall to <body>.
  const focused = body.contains(document.activeElement) ? document.activeElement.closest("tr").dataset.id : null;
  body.replaceChildren(...rules.map(ruleRow));
  if (focused !== null) {
    const tr = body.querySelector(`tr[data-id="${CSS.escape(focused)}"]`);
    if (tr) tr.querySelector("button").focus();
  }
}

// Which row is in the editor, without rebuilding the table (which would drop the focus).
function markSelected() {
  for (const tr of $("rules").tBodies[0].rows) {
    const on = tr.dataset.id === state.selected;
    tr.classList.toggle("selected", on);
    const pick = tr.querySelector("button");
    if (on) pick.setAttribute("aria-current", "true");
    else pick.removeAttribute("aria-current");
  }
}

function ruleRow(rule) {
  const tr = el("tr");
  tr.dataset.id = rule.id;
  const th = el("th");
  th.scope = "row";
  const pick = el("button", rule.id, "link");
  pick.type = "button";
  pick.addEventListener("click", () => selectRule(rule.id));
  if (rule.id === state.selected) {
    tr.classList.add("selected");
    pick.setAttribute("aria-current", "true");
  }
  th.append(pick);
  if (rule.description) {
    const desc = el("span", rule.description, "desc");
    desc.title = rule.description;
    th.append(desc);
  }
  tr.append(th);
  const from = [].concat(rule.from || [], rule.events || []).join(", ");
  // Where it publishes: the topic templates of its republish actions.
  const to = (rule.actions_spec || []).map((a) => (a && a.function === "republish" && a.args
    ? a.args.topic : a && a.function)).filter((t) => typeof t === "string").join(", ");
  for (const [key, text] of [["on", ""], ["from", from], ["to", to], ["matched"], ["passed"], ["no_result"],
    ["failed"], ["actions_failed"], ["matched_rate"], ["passed_rate"], ["active"], ["error"]]) {
    const td = el("td");
    if (text) topicText(td, text);
    td.dataset.key = key;
    if (!["on", "from", "to", "active", "error"].includes(key)) td.className = "num";
    tr.append(td);
  }
  fillRow(tr, rule, state.stats.get(rule.id));
  return tr;
}

// Live $SYS figures win over the list's, which are as old as the last GET.
function fillRow(tr, rule, live) {
  const src = live || rule;
  const counts = src.counts || {};
  const cell = (key) => tr.querySelector(`td[data-key="${key}"]`);
  cell("on").textContent = src.enabled ? "yes" : "no";
  for (const k of ["matched", "passed", "no_result", "failed", "actions_failed"]) {
    cell(k).textContent = count(counts[k]);
  }
  tr.classList.toggle("failing", (counts.failed || 0) + (counts.actions_failed || 0) > 0);
  const r = rates(rule.id);
  cell("matched_rate").textContent = r ? r.matched.toFixed(2) : "-";
  cell("passed_rate").textContent = r ? r.passed.toFixed(2) : "-";
  cell("active").textContent = src.last_active_at ? ago(src.last_active_at) : "never";
  // $SYS leaves the message out while the trace is off; the list has it for an operator.
  let err = src.last_error;
  if (err && !err.message && rule.last_error && rule.last_error.at === err.at) err = rule.last_error;
  const errCell = cell("error");
  errCell.textContent = err ? `${err.kind}${err.message ? `: ${err.message}` : ""} (${ago(err.at)})` : "";
  errCell.title = err && err.message ? err.message : "";
}

// The broker's own rates are per tick, so they jump with each tick's luck; the table's
// come from the counts over the last RATE_WINDOW.
function sample(id, rec) {
  const t = Date.parse(rec.at);
  const c = rec.counts || {};
  if (!t || typeof c.matched !== "number" || typeof c.passed !== "number") return;
  let h = state.history.get(id) || [];
  const last = h[h.length - 1];
  if (last && t === last.t) return;
  if (last && (t < last.t || c.matched < last.matched)) h = []; // the broker restarted
  h.push({ t, matched: c.matched, passed: c.passed });
  while (h.length > 2 && h[1].t <= t - RATE_WINDOW) h.shift();
  state.history.set(id, h);
}

function rates(id) {
  const h = state.history.get(id);
  if (!h || h.length < 2) return null;
  const a = h[0];
  const b = h[h.length - 1];
  const secs = (b.t - a.t) / 1000;
  return { matched: (b.matched - a.matched) / secs, passed: (b.passed - a.passed) / secs };
}

function updateRow(id) {
  const rule = state.list && state.list.rules.find((r) => r.id === id);
  const tr = rule && $("rules").tBodies[0].querySelector(`tr[data-id="${CSS.escape(id)}"]`);
  if (tr) fillRow(tr, rule, state.stats.get(id));
}

// ---- the rule editor -------------------------------------------------------------------

function fillEditor(rule) {
  $("f-id").value = rule.id;
  $("f-description").value = rule.description || "";
  $("f-enable").checked = Boolean(rule.enable);
  $("f-sql").value = rule.sql || "";
  $("f-actions").value = rule.actions ? JSON.stringify(rule.actions, null, 2) : "";
}

function findRule(id) {
  return state.list && state.list.rules.find((r) => r.id === id);
}

// What a write would change of a rule, to tell whether another write changed it.
function snapshot(rule) {
  return rule ? JSON.stringify([rule.description, rule.enabled, rule.sql, rule.actions_spec]) : "";
}

function selectRule(id) {
  const rule = findRule(id);
  if (!rule) return;
  state.selected = id;
  state.editorDigest = state.list.file_digest;
  state.editorBase = state.list.in_sync ? snapshot(rule) : undefined;
  $("editing").textContent = id;
  $("f-id").readOnly = true;
  $("b-delete").disabled = false;
  fillEditor({ id, description: rule.description, enable: rule.enabled, sql: rule.sql, actions: rule.actions_spec });
  $("r-out").replaceChildren();
  $("r-changed").hidden = true;
  if (rule.redacted) say($("r-out"), "error", "The admin API answered as to a viewer: no SQL or actions.");
  resetTestInput();
  markSelected();
  renderTrace();
  $("h-editor").focus();
}

function newRule() {
  state.selected = null;
  state.editorDigest = state.list ? state.list.file_digest : null;
  state.editorBase = undefined;
  $("editing").textContent = "(new)";
  $("f-id").readOnly = false;
  $("b-delete").disabled = true;
  const ids = new Set(((state.list && state.list.rules) || []).map((r) => r.id));
  let n = 1;
  while (ids.has(`demo_rule_${n}`)) n++;
  fillEditor({ id: `demo_rule_${n}`, ...NEW_RULE });
  $("r-out").replaceChildren();
  $("r-changed").hidden = true;
  resetTestInput();
  markSelected();
  renderTrace();
  $("f-id").focus();
}

// Another rule's message would only answer no_match.
function resetTestInput() {
  $("t-latest").checked = true;
  $("t-topic").value = "";
  $("t-payload").value = "";
}

// After each new rules list. The editor's writes carry the digest of the file its text
// came from, so a write made over another tab's change is refused. When the file changed
// but the open rule did not, the editor moves on to the new file: nothing would be lost.
// The list shows the running rules, which are the file's only while the two are in sync.
function followFile() {
  const l = state.list;
  if (!l || !l.in_sync) return;
  if (state.selected === null) {
    // A new rule: Apply asks before it replaces a rule this list has.
    if (state.editorDigest) state.editorDigest = l.file_digest;
    return;
  }
  const now = snapshot(findRule(state.selected));
  if (l.file_digest === state.editorDigest) {
    state.editorBase = now;
  } else if (state.editorBase === undefined) {
    // Not known what the rule was: a write is refused, and says why.
  } else if (now === state.editorBase) {
    state.editorDigest = l.file_digest;
    $("r-changed").hidden = true;
  } else {
    const note = $("r-changed");
    note.textContent = now
      ? `${state.selected} changed since you opened it, in another tab or client. To see the ` +
        "newer version, choose it in the table (your edit here is lost). Apply is refused once; " +
        "a second Apply replaces that change."
      : `${state.selected} was deleted since you opened it, in another tab or client. Apply is ` +
        "refused once; a second Apply puts it back.";
    note.hidden = false;
  }
}

function ruleFromEditor() {
  const id = $("f-id").value.trim();
  if (!RULE_ID.test(id)) {
    throw new Error("A rule id is a letter or _ followed by up to 63 letters, digits, _ or -.");
  }
  let actions;
  try {
    actions = JSON.parse($("f-actions").value);
  } catch (e) {
    throw new Error(`The actions are not JSON: ${e.message}`);
  }
  if (!Array.isArray(actions)) throw new Error("The actions must be a JSON array, as in the rules file.");
  return {
    id,
    fields: { sql: $("f-sql").value, actions, description: $("f-description").value, enable: $("f-enable").checked },
  };
}

// The topic filters (and $events) after FROM: quoted strings, comma-separated.
function fromFilters(sql) {
  const m = /\bFROM\s+((?:"[^"]*"\s*,\s*)*"[^"]*")/i.exec(sql);
  return m ? Array.from(m[1].matchAll(/"([^"]*)"/g), (x) => x[1]) : [];
}

// Rules do not chain: a rule never runs on a message a rule published. What the other
// rules publish to, as filters: each republish topic up to its first ${…} level. A topic
// that starts with one says nothing, and is left out.
function derivedFilters(exceptId) {
  const found = [];
  for (const r of (state.list && state.list.rules) || []) {
    if (r.id === exceptId) continue;
    for (const a of r.actions_spec || []) {
      const topic = a && a.function === "republish" && a.args && a.args.topic;
      if (typeof topic !== "string") continue;
      const levels = topic.split("/");
      const fixed = levels.findIndex((l) => l.includes("${"));
      if (fixed === 0) continue;
      found.push({ rule: r.id, topic, filter: fixed < 0 ? topic : [...levels.slice(0, fixed), "#"].join("/") });
    }
  }
  return found;
}

// Whether some topic matches both filters.
function filtersOverlap(a, b) {
  const x = a.split("/");
  const y = b.split("/");
  const wild = (l) => l === "+" || l === "#";
  if ((x[0].startsWith("$") && wild(y[0])) || (y[0].startsWith("$") && wild(x[0]))) return false;
  for (let i = 0; ; i++) {
    if (x[i] === "#" || y[i] === "#") return true;
    if (i === x.length || i === y.length) return x.length === y.length;
    if (x[i] !== "+" && y[i] !== "+" && x[i] !== y[i]) return false;
  }
}

// After Check, Test and Apply: the edited rule's FROM covers what other rules publish.
function chainWarning(out, rule) {
  const derived = derivedFilters(rule.id);
  const hits = new Map(); // other rule id -> a topic it publishes to
  for (const f of fromFilters(rule.fields.sql)) {
    if (f.startsWith("$events/")) continue;
    for (const d of derived) {
      if (!hits.has(d.rule) && filtersOverlap(f, d.filter)) hits.set(d.rule, d.topic);
    }
  }
  if (!hits.size) return;
  const named = [...hits].slice(0, 3).map(([id, topic]) => `${id} (${topic})`).join(", ");
  const more = hits.size > 3 ? `, and ${hits.size - 3} more` : "";
  out.append(el("p", `Its FROM matches topics other rules publish to: ${named}${more}. Rules do not ` +
    "chain, so this rule does not run on those messages, only on what clients publish there.", "notice"));
}

async function testInput(rule) {
  if ($("t-custom").checked) {
    return { body: { topic: $("t-topic").value, payload: $("t-payload").value, payload_encoding: "utf8" } };
  }
  const filters = fromFilters(rule.fields.sql);
  const topics = filters.filter((f) => !f.startsWith("$events/"));
  if (topics.length) {
    const q = new URLSearchParams();
    for (const f of topics) q.append("topic_filter", f);
    const r = await api("GET", `/api/latest?${q}`);
    if (r.status !== 200) return { error: errorText(r) };
    const m = r.body;
    // Shown in the custom fields too, so it can be edited and tested again. A binary
    // payload cannot be: a message of your own is sent as text.
    const binary = m.payload_encoding !== "utf8";
    $("t-topic").value = m.topic;
    $("t-payload").value = binary ? "" : m.payload;
    return {
      body: { topic: m.topic, payload: m.payload, payload_encoding: m.payload_encoding },
      note: `the latest ${m.topic} (${count(m.bytes)} bytes, received ${clock(m.at)}` +
        `${binary ? "; binary: tested as received, a message of your own needs a text payload" : ""})`,
    };
  }
  if (filters.length) {
    // An event rule: the last event its trace saw, else a sample of its first event.
    const seen = (state.traces.get(rule.id) || []).find((t) => t.trigger && t.trigger.type === "event");
    const t = seen ? seen.trigger : {};
    const event = t.event || filters[0].slice("$events/".length).replace("/", ".");
    const body = { topic: filters[0], payload: "", event, clientid: t.clientid || "rules-ui-test" };
    if (t.username) body.username = t.username;
    return { body, note: seen ? `the last ${event} event in its trace (${t.clientid})` : `a sample ${event} event` };
  }
  return { error: "No FROM topic in the SQL to take the latest message from: test with a message of your own." };
}

async function checkRule() {
  const out = $("r-out");
  let rule;
  try {
    rule = ruleFromEditor();
  } catch (e) {
    say(out, "error", e.message);
    return;
  }
  say(out, "busy", "Checking…");
  showCheck(out, await api("POST", "/api/check", { rule: { id: rule.id, ...rule.fields } }), $("f-sql"));
  if (state.selected === null && findRule(rule.id)) {
    out.append(el("p", `A rule named ${rule.id} already exists: Apply would replace it.`, "notice"));
  }
  chainWarning(out, rule);
}

async function testRule() {
  const out = $("r-out");
  let rule;
  try {
    rule = ruleFromEditor();
  } catch (e) {
    say(out, "error", e.message);
    return;
  }
  await showTest(out, rule);
  chainWarning(out, rule);
}

async function showTest(out, rule) {
  say(out, "busy", "Testing…");
  const input = await testInput(rule);
  if (input.error) {
    say(out, "error", input.error);
    return;
  }
  const r = await api("POST", "/api/test", { rule: { id: rule.id, ...rule.fields }, ...input.body });
  out.replaceChildren();
  if (r.status !== 200) {
    showError(out, r, $("f-sql"));
    return;
  }
  out.append(el("p", `Tested against ${input.note || `${input.body.topic}`}, without publishing anything:`));
  for (const res of r.body.results || []) {
    const box = el("div", undefined, "result");
    const head = el("p");
    head.append(el("strong", res.rule), " ", badge(res.result));
    if (res.enabled === false) head.append(" (tested as if enabled: it is disabled)");
    box.append(head);
    if (res.reason) box.append(el("p", res.reason));
    if (res.result === "no_result") box.append(el("p", NO_RESULT, "hint"));
    if (res.error) box.append(el("p", res.error, "error"));
    box.append(outputList(res.outputs));
    out.append(box);
  }
}

async function applyRule() {
  const out = $("r-out");
  let rule;
  try {
    rule = ruleFromEditor();
  } catch (e) {
    say(out, "error", e.message);
    return;
  }
  if (state.selected === null && findRule(rule.id) &&
    !confirm(`A rule named ${rule.id} already exists. Replace it?`)) {
    say(out, "hint", `Nothing written. Choose another id, or choose ${rule.id} in the table to edit it.`);
    return;
  }
  const q = new URLSearchParams({ id: rule.id });
  if (state.editorDigest) q.set("if_match", state.editorDigest);
  say(out, "busy", "Applying…");
  const r = await api("PUT", `/api/rule?${q}`, rule.fields);
  showWrite(out, r, $("f-sql"), "The rules list is reloaded now. Your edit is still here: Apply " +
    "again to write it over the newer file, or choose the rule in the table to load its newer version.");
  chainWarning(out, rule);
  wrote(r);
  if (r.status === 200) {
    state.selected = rule.id;
    $("editing").textContent = rule.id;
    $("f-id").readOnly = true;
    $("b-delete").disabled = false;
  }
  await loadRules();
}

async function deleteRule() {
  const id = state.selected;
  if (!id || !confirm(`Delete ${id} from the rules file? Its statistics stop being published ` +
    "(and resume if a rule with this id comes back).")) return;
  const out = $("r-out");
  const q = new URLSearchParams({ id });
  if (state.editorDigest) q.set("if_match", state.editorDigest);
  say(out, "busy", "Deleting…");
  const r = await api("DELETE", `/api/rule?${q}`);
  showWrite(out, r, null, "The rules list is reloaded now: Delete again to delete it from the " +
    "newer file, or choose the rule in the table to see its newer version.");
  wrote(r);
  if (r.status === 200) {
    state.selected = null;
    $("editing").textContent = `(${id} deleted)`;
    $("f-id").readOnly = false;
    $("b-delete").disabled = true;
  }
  await loadRules();
}

// The single-rule editor's next write goes over the file this one wrote, or, after a
// conflict, over the newer one, so a second Apply does what the answer says it does.
function wrote(r) {
  const digest = r.status === 200 ? r.body.digest : r.status === 412 ? errorField(r, "file_digest") : null;
  if (!digest) return;
  state.editorDigest = digest;
  state.editorBase = undefined;
  $("r-changed").hidden = true;
}

// ---- the whole file ----------------------------------------------------------------------

async function loadSource() {
  const out = $("f-out");
  const r = await api("GET", "/api/source");
  if (r.status !== 200) {
    showError(out, r, null);
    return;
  }
  $("f-source").value = r.body.source;
  state.fileDigest = r.body.digest;
  say(out, "ok", `Loaded ${count(r.body.bytes)} bytes, digest ${short(r.body.digest)}: ` +
    (r.body.in_sync ? "this is what runs." : `not what runs (${short(r.body.running_digest)}).`));
}

async function checkFile() {
  const out = $("f-out");
  say(out, "busy", "Checking…");
  showCheck(out, await api("POST", "/api/check", { source: $("f-source").value }), $("f-source"));
}

async function applyFile() {
  const out = $("f-out");
  if (!state.fileDigest) {
    say(out, "error", "Load the file first: Apply replaces it only if it has not changed since.");
    return;
  }
  const q = new URLSearchParams({ if_match: state.fileDigest });
  say(out, "busy", "Applying…");
  const r = await api("PUT", `/api/rules?${q}`, { source: $("f-source").value });
  showWrite(out, r, $("f-source"), "The file changed since you loaded it. Apply again to replace " +
    "it with your text (the newer changes are lost), or copy your text and Load to merge.");
  if (r.status === 200) state.fileDigest = r.body.digest;
  if (r.status === 412 && errorField(r, "file_digest")) state.fileDigest = errorField(r, "file_digest");
  await loadRules();
}

async function resetFile() {
  if (!confirm("Replace the rules file with the shipped demo/rules/rules.toml? Every edit is lost.")) return;
  const out = $("f-out");
  say(out, "busy", "Resetting…");
  const r = await api("PUT", "/api/reset");
  showWrite(out, r, null);
  if (r.status === 200) {
    $("f-source").value = "";
    state.fileDigest = null;
  }
  await loadRules();
}

// ---- answers -------------------------------------------------------------------------------

function say(out, kind, text) {
  out.replaceChildren(el("p", text, kind));
}

function showCheck(out, r, textarea) {
  out.replaceChildren();
  if (r.status !== 200) {
    showError(out, r, textarea);
    return;
  }
  out.append(el("p", `Valid: the file would hold ${r.body.rules} rules, ${r.body.enabled} enabled ` +
    `(digest ${short(r.body.digest)}). Nothing was written.`, "ok"));
  warnings(out, r.body.warnings);
}

// `conflict` is the advice after a 412: what a second try does.
function showWrite(out, r, textarea, conflict) {
  out.replaceChildren();
  if (r.status !== 200) {
    showError(out, r, textarea);
    if (r.status === 412 && conflict) out.append(el("p", conflict));
    return;
  }
  const b = r.body;
  out.append(el("p", b.applied
    ? `Written and running: ${b.rules} rules, ${b.enabled} enabled. ` +
      `On disk ${short(b.digest)}, running ${short(b.running_digest)}.`
    : `Written (${short(b.digest)}), but the running rules are ${short(b.running_digest)}: ` +
      "the reload did not apply it.", b.applied ? "ok" : "error"));
  warnings(out, b.warnings);
  const wait = el("p", `Waiting for $SYS/brokers/+/rules to report ${short(b.running_digest)}…`, "hint");
  out.append(wait);
  state.awaiting = { digest: b.running_digest, out: wait };
  if (state.summary && state.summary.digest === b.running_digest) sawDigest(state.summary);
}

function sawDigest(summary) {
  const { out } = state.awaiting;
  out.textContent = `$SYS/brokers/${summary.node}/rules reports ${short(summary.digest)} at ` +
    `${clock(Date.parse(summary.at) || Date.now())}: the new rules are running (counts keep ` +
    "adding up since the broker started).";
  out.className = "ok";
  state.awaiting = null;
}

function showError(out, r, textarea) {
  out.append(el("p", errorText(r), "error"));
  const d = errorField(r, "details");
  if (d && (d.line || d.rule)) {
    const where = [d.scope === "sql" ? "in the SQL" : "in the file", d.rule && `of ${d.rule}`,
      d.line && `at line ${d.line}`, d.column && `column ${d.column}`].filter(Boolean).join(" ");
    out.append(el("p", `Where: ${where}.`));
    // SQL positions count from the statement, TOML positions from the file.
    const sqlHere = textarea === $("f-sql") && d.scope === "sql";
    const tomlHere = textarea === $("f-source") && d.scope === "toml";
    if (d.line && (sqlHere || tomlHere)) selectAt(textarea, d.line, d.column);
  }
  const running = errorField(r, "running_digest");
  if (errorField(r, "written")) {
    out.append(el("p", `The file was written (${short(errorField(r, "digest"))}), but the reload ` +
      `rejected it: the broker still runs ${short(running)}.`));
  } else if (errorField(r, "file_digest")) {
    out.append(el("p", `On disk now: ${short(errorField(r, "file_digest"))}; running: ${short(running)}.`));
  }
}

function warnings(out, list) {
  if (!Array.isArray(list) || !list.length) return;
  const ul = el("ul", undefined, "warnings");
  for (const w of list) ul.append(el("li", w));
  out.append(el("p", "Warnings:"), ul);
}

function selectAt(textarea, line, column) {
  const lines = textarea.value.split("\n");
  let at = 0;
  for (let i = 0; i < line - 1 && i < lines.length; i++) at += lines[i].length + 1;
  at = Math.min(at + Math.max(0, (column || 1) - 1), textarea.value.length);
  textarea.focus();
  textarea.setSelectionRange(at, Math.min(at + 1, textarea.value.length));
}

function payloadText(p, encoding) {
  return encoding === "base64" ? `(base64) ${p}` : p;
}

function outputList(outputs) {
  const ul = el("ul", undefined, "outputs");
  for (const o of outputs || []) {
    const li = el("li");
    if (o.error !== undefined) {
      li.append(el("span", `action ${o.action_index + 1} failed: ${o.error}`, "error"));
    } else if (o.action === "republish") {
      li.append(el("span", "→ ", "arrow"), el("span", o.topic, "topic"),
        el("span", ` qos ${o.qos}${o.retain ? ", retained" : ""}`, "hint"));
      li.append(el("pre", payloadText(o.payload, o.payload_encoding), "payload"));
      if (o.truncated) li.append(el("span", `first part of ${count(o.payload_bytes)} bytes`, "hint"));
    } else {
      li.append(el("span", `${o.action || "output"}: `, "arrow"),
        el("pre", JSON.stringify(o.output === undefined ? o : o.output), "payload"));
    }
    ul.append(li);
  }
  if (!ul.childElementCount) ul.append(el("li", "no output", "hint"));
  return ul;
}

// ---- trace -----------------------------------------------------------------------------------

function badge(result) {
  const b = el("span", result, `badge ${result}`);
  if (result === "no_result") b.title = NO_RESULT;
  return b;
}

function traceItem(rec) {
  const li = el("li", undefined, `record ${rec.result}`);
  state.traceItems.set(rec, li);
  const t = rec.trigger || {};
  const meta = el("div", undefined, "meta");
  meta.append(el("time", clock(Date.parse(rec.at) || Date.now())), " ", badge(rec.result), " ");
  const about = [t.type === "will" && "a Will", t.qos !== undefined && `qos ${t.qos}`,
    t.retain && "retained", t.clientid && `client ${t.clientid}`, t.username && `user ${t.username}`];
  meta.append(el("span", t.type === "event" ? t.event : t.topic, "topic"),
    el("span", ` ${about.filter(Boolean).join(", ")}`, "hint"));
  li.append(meta);
  if (t.payload !== undefined) {
    li.append(el("pre", payloadText(t.payload, t.payload_encoding), "payload"));
    if (t.truncated) li.append(el("span", `first part of ${count(t.payload_bytes)} bytes`, "hint"));
  }
  if (rec.error) li.append(el("p", rec.error, "error"));
  li.append(outputList(rec.outputs));
  if (rec.outputs_omitted) li.append(el("span", `and ${rec.outputs_omitted} more outputs`, "hint"));
  return li;
}

function renderTrace() {
  const id = state.selected;
  $("trace-rule").textContent = id ? `of ${id}` : "(choose a rule)";
  renderTraceHead();
  $("trace").replaceChildren(...(id ? state.traces.get(id) || [] : []).map(traceItem));
}

// The chosen rule's trace topic, a command to watch it with, and the broker's trace rate.
function renderTraceHead() {
  const id = state.selected;
  const node = (state.summary && state.summary.node) || (state.list && state.list.node) || "+";
  const topic = `$SYS/brokers/${node}/trace/rules/${id || "<id>"}`;
  $("trace-topic").textContent = topic;
  $("trace-sub").hidden = !id;
  $("trace-cmd").textContent = id ? `mosquitto_sub -v -t '${topic.replace(/'/g, "'\\''")}'` : "";
  const s = state.summary;
  if (s) {
    $("trace-rate").textContent = s.trace
      ? `At most ${s.trace_rate} records a second, and up to ${s.trace_rate} no_result records ` +
        "more: they have a budget of their own."
      : "The trace is off on this broker.";
  }
}

async function copyCommand() {
  const button = $("b-copy");
  try {
    await navigator.clipboard.writeText($("trace-cmd").textContent);
    button.textContent = "Copied";
  } catch {
    // No clipboard here: select the command, to copy by hand.
    getSelection().selectAllChildren($("trace-cmd"));
    button.textContent = "Selected";
  }
  setTimeout(() => { button.textContent = "Copy"; }, 2000);
}

// The last KEEP_TRACE no_result records are kept apart from the last KEEP_TRACE others,
// so the passes of a rule whose WHERE seldom passes stay in view. Returns the record
// that made room, if one did.
function keepTrace(list, rec) {
  list.unshift(rec);
  const miss = rec.result === "no_result";
  let n = 0;
  for (let i = 0; i < list.length; i++) {
    if ((list[i].result === "no_result") === miss && ++n > KEEP_TRACE) return list.splice(i, 1)[0];
  }
  return null;
}

// ---- live data -----------------------------------------------------------------------------

function filterMatches(topic) {
  const f = $("d-filter").value.trim();
  if (!f) return true;
  if (f.includes("+") || f.includes("#")) return mqttMatch(f, topic);
  return topic.toLowerCase().includes(f.toLowerCase());
}

function dataItem(m) {
  const root = m.topic.split("/", 1)[0];
  const kind = DEVICE_ROOTS.includes(root) ? "device" : "derived";
  const li = el("li", undefined, `msg ${kind} ${root}`);
  const meta = el("div", undefined, "meta");
  meta.append(el("time", clock(m.at)), " ", el("span", kind, `badge ${kind}`), " ", el("span", m.topic, "topic"));
  if (m.retain) meta.append(" ", el("span", "retained", "badge"));
  li.append(meta, el("pre", payloadText(m.payload, m.encoding), "payload"));
  if (m.truncated) li.append(el("span", `first part of ${count(m.bytes)} bytes`, "hint"));
  return li;
}

function addData(m) {
  state.data.push(m);
  if (state.data.length > KEEP_DATA) state.data.shift();
  if ($("d-pause").checked || !filterMatches(m.topic)) return;
  const list = $("data");
  list.prepend(dataItem(m));
  while (list.childElementCount > SHOW_DATA) list.lastElementChild.remove();
}

function renderData() {
  const shown = state.data.filter((m) => filterMatches(m.topic)).slice(-SHOW_DATA).reverse();
  $("data").replaceChildren(...shown.map(dataItem));
}

// ---- the live feed -------------------------------------------------------------------------

function record(m) {
  if (m.truncated || m.encoding !== "utf8") return null;
  try {
    const v = JSON.parse(m.payload);
    return v && typeof v === "object" ? v : null;
  } catch {
    return null;
  }
}

function onMessage(m) {
  const p = m.topic.split("/");
  if (p[0] !== "$SYS") {
    addData(m);
    return;
  }
  const rec = record(m);
  if (!rec || p[1] !== "brokers") return;
  if (p.length === 4 && p[3] === "rules") onSummary(rec);
  else if (p.length === 5 && p[3] === "rules") onRuleStats(p[4], rec);
  else if (p.length === 6 && p[3] === "trace" && p[4] === "rules") onTrace(p[5], rec);
}

function onSummary(s) {
  state.summary = s;
  state.summaryAt = Date.now();
  if (s.interval_secs) state.interval = s.interval_secs;
  if (state.banner === "quiet") setBanner(null, "");
  if (state.list && s.digest !== state.list.digest) scheduleRefresh();
  if (state.awaiting && s.digest === state.awaiting.digest) sawDigest(s);
  renderHeader();
}

function onRuleStats(id, rec) {
  state.stats.set(id, rec);
  sample(id, rec);
  updateRow(id);
}

function onTrace(id, rec) {
  const list = state.traces.get(id) || [];
  const gone = keepTrace(list, rec);
  state.traces.set(id, list);
  if (id !== state.selected || $("tr-pause").checked) return;
  if (gone && state.traceItems.has(gone)) state.traceItems.get(gone).remove();
  $("trace").prepend(traceItem(rec));
}

function setFeed(mqtt) {
  if (mqtt.state === "connected") {
    if (!state.mqttUpSince) state.mqttUpSince = Date.now();
    $("s-feed").textContent = "live";
  } else {
    state.mqttUpSince = 0;
    $("s-feed").textContent = `MQTT ${mqtt.state}: ${mqtt.detail}`;
  }
}

function openStream() {
  if (state.es) return;
  const es = new EventSource("/api/events");
  state.es = es;
  es.addEventListener("hello", (e) => {
    const h = JSON.parse(e.data);
    state.interval = h.sys_interval_secs || state.interval;
    setFeed(h.mqtt);
  });
  es.addEventListener("mqtt", (e) => setFeed(JSON.parse(e.data)));
  es.addEventListener("msg", (e) => onMessage(JSON.parse(e.data)));
  es.addEventListener("dropped", (e) => {
    $("s-feed").textContent = `live (this page fell behind: ${JSON.parse(e.data).n} messages skipped)`;
  });
  es.addEventListener("error", () => {
    state.mqttUpSince = 0;
    if (es.readyState !== EventSource.CLOSED) {
      $("s-feed").textContent = "reconnecting";
      return;
    }
    // Refused (server.py serves at most 8 open pages) or gone: try again in a while.
    state.es = null;
    $("s-feed").textContent = "refused: too many open pages? Retrying.";
    setTimeout(() => {
      if (!document.hidden) openStream();
    }, 5000);
  });
}

function closeStream() {
  if (state.es) state.es.close();
  state.es = null;
  state.mqttUpSince = 0;
  $("s-feed").textContent = "paused while the tab is hidden";
}

// No summary for three intervals while MQTT is up: the broker publishes no statistics.
function watchdog() {
  if (!state.mqttUpSince || state.banner === "predates") return;
  const since = Math.max(state.mqttUpSince, state.summaryAt);
  const quiet = (Date.now() - since) / 1000;
  if (quiet > 3 * state.interval + 1) {
    setBanner("quiet", `No rule statistics on $SYS/brokers/+/rules for ${Math.round(quiet)} s: ` +
      "this broker predates ADR 0084, or its statistics are off. Rebuild it with demo/rules-live/up.sh.");
  }
  if (state.list && state.summary) renderHeader(); // the "… ago" texts
}

function init() {
  $("new-rule").addEventListener("click", newRule);
  $("b-check").addEventListener("click", checkRule);
  $("b-test").addEventListener("click", testRule);
  $("b-apply").addEventListener("click", applyRule);
  $("b-delete").addEventListener("click", deleteRule);
  $("b-load").addEventListener("click", loadSource);
  $("b-fcheck").addEventListener("click", checkFile);
  $("b-fapply").addEventListener("click", applyFile);
  $("b-reset").addEventListener("click", resetFile);
  $("d-filter").addEventListener("input", renderData);
  $("d-pause").addEventListener("change", renderData);
  $("tr-pause").addEventListener("change", renderTrace);
  const hide = () => $("trace").classList.toggle("hide-no-result", $("tr-hide").checked);
  $("tr-hide").addEventListener("change", hide);
  hide(); // a reload can bring the box back checked
  $("b-copy").addEventListener("click", copyCommand);
  $("t-topic").addEventListener("input", () => { $("t-custom").checked = true; });
  $("t-payload").addEventListener("input", () => { $("t-custom").checked = true; });
  $("rule-form").addEventListener("submit", (e) => e.preventDefault());
  // A hidden tab keeps no stream open: browsers allow only a few connections per host.
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      closeStream();
    } else {
      openStream();
      loadRules();
    }
  });
  $("b-delete").disabled = true;
  renderTrace();
  loadRules();
  if (!document.hidden) openStream();
  setInterval(watchdog, 1000);
}

init();
