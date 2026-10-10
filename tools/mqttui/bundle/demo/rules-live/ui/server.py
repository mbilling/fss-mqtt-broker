"""The live rules demo's UI server: the page, the admin API behind it, and MQTT as SSE.

Runs in compose.yaml's `ui` service, standard library only:

- serves index.html, app.js and style.css;
- passes /api/* on to the broker's admin API over mTLS with the CN=rules-ui operator
  certificate (ADR 0081, ADR 0084), one request per connection;
- streams the rules' statistics and trace from $SYS, and the device and derived messages,
  to each open page as server-sent events, from one MQTT 5 connection (demo/rules/sim/mqtt.py);
  a binary payload (a turbine's half-megabyte Parquet fast log) goes to the page as its
  size, content type and first bytes only;
- keeps the latest device payload per topic, untruncated, for "Test" against the latest
  input (/api/latest).

It holds an operator certificate and asks nobody for a password, so whoever reaches it can
rewrite the rules. It is published on 127.0.0.1 only, and it refuses what a web page on
another site could send it (see `refusal`). DEMO ONLY.

Configuration, from the environment (compose.yaml sets it):
  UI_PORT          the published port; only Host localhost:<it> or 127.0.0.1:<it> is served
  UI_LISTEN        where to listen, default 0.0.0.0:8070
  UI_BROKER        the broker's plaintext MQTT listener, default mqttd:1883
  UI_ADMIN         the admin API, default mqttd:9443; its host is the TLS server name
  UI_PKI           ca.crt, rules-ui.crt and rules-ui.key, default /pki
  UI_RULES         demo/rules: sim/mqtt.py, and the shipped rules.toml, default /opt/rules
  UI_SYS_INTERVAL  the broker's stats interval in seconds until a summary says, default 2
"""

from __future__ import annotations

import base64
import collections
import http.client
import json
import os
import random
import re
import signal
import ssl
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, quote, urlsplit

HERE = Path(__file__).resolve().parent
RULES = Path(os.environ.get("UI_RULES", "/opt/rules"))
sys.path.insert(0, str(RULES))

from sim.mqtt import Client, Message, MqttError  # noqa: E402

UI_PORT = int(os.environ.get("UI_PORT", "8070"))
LISTEN = os.environ.get("UI_LISTEN", "0.0.0.0:8070")
BROKER = os.environ.get("UI_BROKER", "mqttd:1883")
ADMIN = os.environ.get("UI_ADMIN", "mqttd:9443")
PKI = Path(os.environ.get("UI_PKI", "/pki"))
SYS_INTERVAL = float(os.environ.get("UI_SYS_INTERVAL", "2"))

# What the page shows: the rules' statistics and trace (ADR 0084; `#` never matches a $SYS
# topic, so both are named), the six roots the demo's rules publish under, and the devices.
FILTERS = [
    "$SYS/brokers/+/rules/#",
    "$SYS/brokers/+/trace/rules/+",
    "alerts/#", "kpi/#", "normalized/#", "analytics/#", "state/#", "events/#",
    "plant/#", "home/#", "vehicle/#",
]
DEVICE_ROOTS = ("plant/", "home/", "vehicle/")

MAX_STREAMS = 8         # open pages; a browser allows about 6 connections per host anyway
QUEUE = 1024            # events waiting per page; when full, the oldest goes
PING_SECS = 15
SHOW = 4096             # a device or derived payload is cut here for display...
SHOW_SYS = 64 * 1024    # ...a $SYS record, which the page parses as JSON, only past this
BINARY_HEAD = 16        # a binary payload is shown as its size and first bytes, in hex
MAX_BODY = 1 << 20      # the admin API's cap on the rules routes' bodies
MAX_ANSWER = 8 << 20
LATEST_TOPICS = 4096    # /api/latest keeps this many topics,
LATEST_BYTES = 16 << 20  # at most this many bytes in all,
LATEST_PAYLOAD = 1 << 20  # and no payload larger than the admin API would take

RULE_ID = re.compile(r"[A-Za-z_][A-Za-z0-9_-]{0,63}")
DIGEST = re.compile(r"\*|[A-Za-z0-9:_-]{1,128}")
STATIC = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/index.html": ("index.html", "text/html; charset=utf-8"),
    "/app.js": ("app.js", "text/javascript; charset=utf-8"),
    "/style.css": ("style.css", "text/css; charset=utf-8"),
}
# (method, UI path) -> (admin path, query parameters passed on, whether a body goes too)
PROXY = {
    ("GET", "/api/rules"): ("/admin/v1/rules", (), False),
    ("GET", "/api/source"): ("/admin/v1/rules/source", (), False),
    ("POST", "/api/check"): ("/admin/v1/rules/check", (), True),
    ("POST", "/api/test"): ("/admin/v1/rules/test", (), True),
    ("PUT", "/api/rules"): ("/admin/v1/rules", ("if_match",), True),
    ("PUT", "/api/rule"): ("/admin/v1/rule", ("id", "if_match"), True),
    ("DELETE", "/api/rule"): ("/admin/v1/rule", ("id", "if_match"), False),
}
LOCAL = {("GET", "/api/events"), ("GET", "/api/latest"), ("PUT", "/api/reset")}
PATHS = {path for _, path in PROXY} | {path for _, path in LOCAL} | set(STATIC)

HEADERS = {
    "Content-Security-Policy": "default-src 'self'; script-src 'self'; style-src 'self'; "
    "connect-src 'self'; img-src 'self'; frame-ancestors 'none'; base-uri 'none'",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
}


def refusal(method: str, headers, port: int) -> Optional[tuple[int, str, str]]:
    """Why a request must not be served: (status, code, message), or None.

    Loopback is not access control. A page on any site can point a name of its own at
    127.0.0.1 (DNS rebinding), and can send a cross-site POST without reading the answer.
    So the Host must be this server by a loopback name, and a request that changes
    anything must come from this page: its own Origin, a JSON body (a cross-site form
    cannot send one), and a header no cross-site request may carry without a preflight
    this server never answers.
    """
    hosts = headers.get_all("Host") or []
    allowed = {f"localhost:{port}", f"127.0.0.1:{port}"}
    if len(hosts) != 1 or hosts[0] not in allowed:
        return 403, "bad-host", f"Host must be localhost:{port} or 127.0.0.1:{port}"
    if method == "GET":
        return None
    if headers.get("Origin") != f"http://{hosts[0]}":
        return 403, "bad-origin", "a request that changes something must come from this page"
    if headers.get("Content-Type", "").split(";")[0].strip().lower() != "application/json":
        return 415, "bad-content-type", "Content-Type must be application/json"
    if headers.get("X-Rules-UI") != "1":
        return 403, "missing-header", "X-Rules-UI: 1 is required"
    return None


def error(code: str, message: str) -> bytes:
    """The admin API's error envelope, so the page reads both the same way."""
    return json.dumps({"error": {"code": code, "message": message}}).encode()


def topic_matches(filt: str, topic: str) -> bool:
    """MQTT filter matching; a wildcard first level does not match a `$` topic."""
    if topic.startswith("$") and filt[:1] in ("+", "#"):
        return False
    f, t = filt.split("/"), topic.split("/")
    for i, level in enumerate(f):
        if level == "#":
            return True
        if i >= len(t) or (level != "+" and level != t[i]):
            return False
    return len(f) == len(t)


def shown(payload: bytes, cap: int, props: Optional[dict] = None) -> dict:
    """A payload as the page shows it: text when it is UTF-8, cut at `cap`. A binary one
    (a turbine's half-megabyte Parquet fast log, an OBD frame) is not sent at all: the page
    gets its size, its MQTT 5 content type and its first BINARY_HEAD bytes as hex."""
    props = props or {}
    truncated = len(payload) > cap
    binary = props.get("payload_format") == 0  # the publisher says so (MQTT 5)
    if not binary:
        try:
            payload.decode("utf-8")
        except UnicodeDecodeError:
            binary = True
    if binary:
        return {"payload": "", "encoding": "binary", "bytes": len(payload),
                "head": payload[:BINARY_HEAD].hex(), "content_type": props.get("content_type"),
                "truncated": len(payload) > BINARY_HEAD}
    # The payload is UTF-8, so "ignore" drops only a character the cut split.
    text = payload[:cap].decode("utf-8", "ignore")
    return {"payload": text, "encoding": "utf8", "bytes": len(payload), "truncated": truncated}


def sse(event: str, data: dict) -> bytes:
    # json.dumps escapes every newline, so the data is one line, as SSE needs.
    return f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()


def host_port(text: str) -> tuple[str, int]:
    host, _, port = text.rpartition(":")
    return host, int(port)


class Stream:
    """One open page's events: a bounded queue the MQTT reader never blocks on."""

    def __init__(self):
        self.cond = threading.Condition()
        self.queue: collections.deque[bytes] = collections.deque(maxlen=QUEUE)
        self.dropped = 0

    def put(self, event: bytes):
        with self.cond:
            if len(self.queue) == QUEUE:
                self.dropped += 1
            self.queue.append(event)
            self.cond.notify()

    def take(self, timeout: float) -> tuple[list[bytes], int]:
        """What is queued, waiting up to `timeout` for something; and how much was lost."""
        with self.cond:
            if not self.queue:
                self.cond.wait(timeout)
            events = list(self.queue)
            self.queue.clear()
            dropped, self.dropped = self.dropped, 0
        return events, dropped


class Hub:
    """The MQTT side: fans each message out to every open page, and keeps device inputs."""

    def __init__(self):
        self.lock = threading.Lock()
        self.streams: set[Stream] = set()
        self.mqtt = {"state": "connecting", "detail": BROKER}
        self.latest: collections.OrderedDict[str, tuple[bytes, float]] = collections.OrderedDict()
        self.latest_bytes = 0

    def open(self) -> Optional[Stream]:
        with self.lock:
            if len(self.streams) >= MAX_STREAMS:
                return None
            stream = Stream()
            self.streams.add(stream)
            return stream

    def close(self, stream: Stream):
        with self.lock:
            self.streams.discard(stream)

    def hello(self) -> bytes:
        with self.lock:
            mqtt = dict(self.mqtt)
        return sse("hello", {"mqtt": mqtt, "sys_interval_secs": SYS_INTERVAL, "show_bytes": SHOW})

    def send(self, event: bytes):
        with self.lock:
            streams = list(self.streams)
        for stream in streams:
            stream.put(event)

    def status(self, state: str, detail: str):
        with self.lock:
            self.mqtt = {"state": state, "detail": detail}
        self.send(sse("mqtt", {"state": state, "detail": detail}))

    def message(self, m: Message):
        now = time.time()
        if m.topic.startswith(DEVICE_ROOTS):
            self.remember(m.topic, m.payload, now)
        cap = SHOW_SYS if m.topic.startswith("$SYS/") else SHOW
        self.send(sse("msg", {"topic": m.topic, **shown(m.payload, cap, m.props), "retain": m.retain,
                              "at": int(now * 1000)}))

    def remember(self, topic: str, payload: bytes, now: float):
        with self.lock:
            old = self.latest.pop(topic, None)
            if old:
                self.latest_bytes -= len(old[0])
            if len(payload) > LATEST_PAYLOAD:
                return
            self.latest[topic] = (payload, now)
            self.latest_bytes += len(payload)
            while len(self.latest) > LATEST_TOPICS or self.latest_bytes > LATEST_BYTES:
                _, (gone, _) = self.latest.popitem(last=False)
                self.latest_bytes -= len(gone)

    def latest_for(self, filters: list[str]) -> Optional[tuple[str, bytes, float]]:
        """The most recent device message whose topic matches any of `filters`."""
        with self.lock:
            for topic in reversed(self.latest):
                if any(topic_matches(f, topic) for f in filters):
                    payload, at = self.latest[topic]
                    return topic, payload, at
        return None


def read_mqtt(hub: Hub):
    """The one MQTT connection (sim/mqtt.py's Client is not thread-safe: only this thread
    touches it). Reconnects with capped, jittered backoff for as long as the server runs."""
    host, port = host_port(BROKER)
    backoff = 1.0
    while True:
        # MQTT 5, to read a publish's content type and payload format indicator.
        client = Client(host, port, f"rules-ui-{os.getpid()}", keepalive=30, version=5)
        try:
            client.connect()
            client.subscribe([(f, 0) for f in FILTERS])
            hub.status("connected", BROKER)
            backoff = 1.0
            while True:
                m = client.poll(1.0)
                if m:
                    hub.message(m)
        except (MqttError, OSError) as e:
            hub.status("disconnected", str(e))
        finally:
            client.drop()
        time.sleep(backoff * random.uniform(1.0, 1.5))
        backoff = min(backoff * 2, 15.0)


def tls() -> ssl.SSLContext:
    """The admin API client: TLS 1.3, the demo CA, the CN=rules-ui certificate. Built per
    request, so a re-minted PKI is picked up without a restart."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)  # verifies the chain and the host name
    ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    ctx.load_verify_locations(PKI / "ca.crt")
    ctx.load_cert_chain(PKI / "rules-ui.crt", PKI / "rules-ui.key")
    return ctx


def admin(method: str, path: str, body: Optional[bytes]) -> tuple[int, bytes]:
    """One request to the admin API on a connection of its own (the listener answers one
    request per connection, then closes)."""
    host, port = host_port(ADMIN)
    headers = {"Connection": "close"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    try:
        conn = http.client.HTTPSConnection(host, port, context=tls(), timeout=60)
        try:
            conn.request(method, path, body=body, headers=headers)
            resp = conn.getresponse()
            data = resp.read(MAX_ANSWER + 1)
        finally:
            conn.close()
    except (OSError, http.client.HTTPException) as e:  # ssl.SSLError is an OSError
        return 502, error("admin-unreachable", f"the admin API at {ADMIN}: {e}")
    if len(data) > MAX_ANSWER:
        return 502, error("admin-answer-too-large", f"over {MAX_ANSWER} bytes")
    return resp.status, data


class Handler(BaseHTTPRequestHandler):
    timeout = 30  # per socket read or write: a stalled client frees its thread
    hub: Hub  # set in main()

    def version_string(self):
        return "rules-ui"

    def do_GET(self):
        self.route()

    def do_POST(self):
        self.route()

    def do_PUT(self):
        self.route()

    def do_DELETE(self):
        self.route()

    def route(self):
        refused = refusal(self.command, self.headers, UI_PORT)
        if refused:
            status, code, message = refused
            self.answer(status, error(code, message))
            return
        url = urlsplit(self.path)
        # A `+` stays a `+` (topic filters), as in the admin API; spaces come as %20.
        query = parse_qs(url.query.replace("+", "%2B"))
        key = (self.command, url.path)
        if key in PROXY or key in LOCAL:
            body = self.body()
            if body is None:
                pass
            elif key == ("GET", "/api/events"):
                self.events()
            elif key == ("GET", "/api/latest"):
                self.latest(query)
            elif key == ("PUT", "/api/reset"):
                self.reset()
            else:
                self.proxy(key, query, body)
        elif self.command == "GET" and url.path in STATIC:
            self.static(*STATIC[url.path])
        elif self.command == "GET" and url.path == "/favicon.ico":
            self.answer(204, b"")
        elif url.path in PATHS:
            self.answer(405, error("method-not-allowed", f"{self.command} {url.path}"))
        else:
            self.answer(404, error("not-found", url.path))

    def body(self) -> Optional[bytes]:
        """The request's JSON object body (b"" for none), or None once refused."""
        length = self.headers.get("Content-Length")
        if length is None:
            return b""
        if not length.isdigit() or int(length) > MAX_BODY:
            self.answer(413, error("too-large", f"bodies are at most {MAX_BODY} bytes"))
            return None
        raw = self.rfile.read(int(length))
        if raw:
            try:
                if not isinstance(json.loads(raw), dict):
                    raise ValueError("not an object")
            except ValueError as e:
                self.answer(400, error("bad-json", f"the body must be a JSON object: {e}"))
                return None
        return raw

    def proxy(self, key: tuple[str, str], query: dict, body: bytes):
        path, params, with_body = PROXY[key]
        passed = []
        for name in params:
            value = query.get(name, [None])[0]
            if value is None:
                continue
            if not (RULE_ID if name == "id" else DIGEST).fullmatch(value):
                self.answer(400, error("bad-request", f"bad {name}: {value!r}"))
                return
            passed.append(f"{name}={quote(value, safe='')}")
        if passed:
            path += "?" + "&".join(passed)
        self.answer(*admin(self.command, path, (body or b"{}") if with_body else None))

    def reset(self):
        """Put the shipped demo/rules/rules.toml back, whatever is on disk now."""
        try:
            source = (RULES / "rules.toml").read_text(encoding="utf-8")
        except OSError as e:
            self.answer(500, error("no-shipped-rules", str(e)))
            return
        self.answer(*admin("PUT", "/admin/v1/rules?if_match=%2A",
                           json.dumps({"source": source}).encode()))

    def latest(self, query: dict):
        filters = query.get("topic_filter", [])
        if not 1 <= len(filters) <= 8 or not all(0 < len(f) <= 1024 for f in filters):
            self.answer(400, error("bad-request", "give 1 to 8 topic_filter parameters"))
            return
        found = self.hub.latest_for(filters)
        if found is None:
            self.answer(404, error(
                "no-input", f"no device message on {', '.join(filters)} seen yet"))
            return
        topic, payload, at = found
        try:
            text, encoding = payload.decode("utf-8"), "utf8"
        except UnicodeDecodeError:
            text, encoding = base64.b64encode(payload).decode("ascii"), "base64"
        self.answer(200, json.dumps({
            "topic": topic, "payload": text, "payload_encoding": encoding,
            "bytes": len(payload), "at": int(at * 1000)}).encode())

    def events(self):
        stream = self.hub.open()
        if stream is None:
            self.answer(503, error(
                "too-many-streams", f"at most {MAX_STREAMS} open pages; close one"))
            return
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(b"retry: 3000\n\n" + self.hub.hello())
            while True:
                events, dropped = stream.take(PING_SECS)
                if dropped:
                    events.insert(0, sse("dropped", {"n": dropped}))
                self.wfile.write(b"".join(events) if events else b": ping\n\n")
        except OSError:
            pass  # the page went away
        finally:
            self.hub.close(stream)
            self.close_connection = True

    def static(self, name: str, ctype: str):
        data = (HERE / name).read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(data)

    def answer(self, status: int, data: bytes):
        self.send_response(status)
        if data:
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def end_headers(self):
        # On every response, the base class's own errors included.
        for name, value in HEADERS.items():
            self.send_header(name, value)
        super().end_headers()

    def log_request(self, code="-", size="-"):
        # Every request that changes something, and every refusal; not each page load.
        if self.command != "GET" or not str(code).startswith(("2", "3")):
            super().log_request(code, size)


def main() -> int:
    hub = Hub()
    Handler.hub = hub
    threading.Thread(target=read_mqtt, args=(hub,), name="mqtt", daemon=True).start()
    server = ThreadingHTTPServer(host_port(LISTEN), Handler)  # daemon threads
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    print(f"rules UI on http://localhost:{UI_PORT} (MQTT {BROKER}, admin API {ADMIN})",
          flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
