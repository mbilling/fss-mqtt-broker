#!/usr/bin/env python3
"""Apply narrow, checked instrumentation changes to pinned upstream source."""

import sys
from pathlib import Path

p = Path(sys.argv[1])
s = p.read_text()


def replace(a, b):
    global s
    assert s.count(a) == 1, (a, s.count(a))
    s = s.replace(a, b)


replace(
    "    maybe_start_restapi(proplists:get_value(restapi, Opts)),",
    "    qos1_audit:start(),\n    maybe_start_restapi(proplists:get_value(restapi, Opts)),",
)
replace(
    "            case LimitFn() of",
    "            case not qos1_audit:paused() andalso LimitFn() of",
)
replace(
    "                    Parent ! publish_complete_no_exit,",
    "                    case qos1_audit:paused() of true -> qos1_audit:stopped(); false -> ok end,\n                    Parent ! publish_complete_no_exit,",
)
replace(
    "        {publish, #{payload := Payload}} ->",
    "        {publish, #{topic := AuditTopic, payload := Payload}} ->\n            qos1_audit:record(received, AuditTopic, Payload),",
)
replace(
    "    case emqtt:publish(Client, topic_opt(Opts), NewPayload, Flags) of",
    """    AuditTopic = topic_opt(Opts),
    qos1_audit:record(sent, AuditTopic, NewPayload),
    AuditStart = erlang:monotonic_time(microsecond),
    AuditResult = emqtt:publish(Client, AuditTopic, NewPayload, Flags),
    qos1_audit:ack(AuditStart),
    case AuditResult of""",
)
# Restrict success instrumentation to synchronous publish, not async path.
a = s.index("    AuditResult =")
b = s.index("\n\npublish_topic(", a)
part = s[a:b]
needle = "            inc_counter(Prometheus, pub_succ),"
assert part.count(needle) == 1
s = (
    s[:a]
    + part.replace(
        needle,
        "            qos1_audit:record(acked, AuditTopic, NewPayload),\n" + needle,
    )
    + s[b:]
)
replace(
    '    [ {"/metrics", emqtt_bench_http_metrics, []}',
    '    [ {"/audit/[...]", qos1_audit, []}\n    , {"/metrics", emqtt_bench_http_metrics, []}',
)
replace(
    "   histogram_observe(Prometheus, e2e_latency, E2ELatency),",
    "   case qos1_audit:latency(E2ELatency) of true -> histogram_observe(Prometheus, e2e_latency, E2ELatency); false -> ok end,",
)
p.write_text(s)
