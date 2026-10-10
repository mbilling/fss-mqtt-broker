#!/usr/bin/env python3
"""Asks a running EMQX what its jq NIF answers, and writes the answers down.

    docker run -d --name emqx-jq-probe emqx/emqx:6.3.1
    crates/mqtt-wasm-sandbox/jq-module/emqx-oracle.py > crates/mqtt-wasm-sandbox/tests/emqx-jq-oracle.txt
    docker rm -f emqx-jq-probe

Each case is a jq program and an input text, run as
`jq:process_json(Program, Input, 10000)` — the call `emqx_rule_funcs:jq/2` makes, with
the default timeout of `rule_engine.jq_function_default_timeout`. One line per case, fields separated by a space,
every program, input, output and message in base64:

    <program> <input> ok <output>...
    <program> <input> error <tag> <message>
    <program> <input> nodedown
    <program> <input> hang

`nodedown`: the call took the EMQX node down (the NIF runs inside the Erlang VM, so a
crash of jq is a crash of the broker). `hang`: no answer within two minutes, timeout
or not. Either way the script restarts the container and goes on.

An input written `@nest:N` stands for N opening brackets followed by N closing ones,
and `@open:N` for the opening ones alone (too long to pass or to keep otherwise).

The outputs are jq's own text, before EMQX decodes it: the comparison with the Wasm
module is byte for byte. `tests/jq_module.rs` reads the file.
"""
import base64
import re
import subprocess
import sys
import time

CONTAINER = "emqx-jq-probe"


class Symbolic:
    """An input built inside EMQX and named, not spelled out, in the oracle file."""

    def __init__(self, name, erlang):
        self.name = name
        self.erlang = erlang


def nested(n, close=True):
    opening = 'binary:copy(<<"[">>, %d)' % n
    if not close:
        return Symbolic("@open:%d" % n, opening)
    return Symbolic("@nest:%d" % n, 'iolist_to_binary([%s, binary:copy(<<"]">>, %d)])' % (opening, n))


BIG = '{"a":{"b":[1,2,3]},"big":123456789012345678901234567890,"s":"h\\u00e9\\ud83d\\ude00"}'
READINGS = '{"readings":[{"id":"a","temp":61.5},{"id":"b","temp":20},{"id":"a","temp":80.25}]}'

CASES = [
    # --- what a rule usually asks of jq
    (".", '{"a":1}'), (".a", '{"a":1}'), (".a.b|.c", '{"a":{"b":{"c":"x"}}}'),
    (".[]", '[1,"a",null,{"k":[true]}]'), ("empty", "1"), ("map(select(.>1))", "[1,2,3]"),
    (".a.b[]", BIG), (".a", BIG), (".big", BIG), (".big + 0", BIG), (".s", BIG),
    (".readings | map(select(.temp > 50)) | length", READINGS),
    (".readings | group_by(.id) | map({id: .[0].id, n: length, max: (map(.temp) | max)})", READINGS),
    (".readings | map(.temp) | add / length", READINGS),
    (".readings[] | select(.id | test(\"^[a-m]$\")) | .temp", READINGS),
    ("[.readings[] | .temp] | sort | .[length/2|floor]", READINGS),
    (".readings | to_entries | map(\"\\(.key):\\(.value.id)\") | join(\",\")", READINGS),
    ("[paths]", '{"a":[1,{"b":2}]}'), ("[..]", '{"a":[1,{"b":2}]}'), ("tostream", '{"a":[1,{"b":2}]}'),
    ("[leaf_paths]", '{"a":[1,{"b":2}]}'), ("walk(if type == \"number\" then .+1 else . end)", '{"a":[1,{"b":2}]}'),
    ("with_entries(.value |= tostring)", '{"a":1,"b":[2]}'), ("del(.a, .b[0])", '{"a":1,"b":[2,3]}'),
    ("to_entries, keys, keys_unsorted, length, has(\"b\")", '{"b":1,"a":2}'),
    (".a as $x | [$x, .b] | @csv, @tsv, @html, @uri, @sh, @base64, @base32, @json, @text", '{"a":"x y","b":"<&\'\\">"}'),
    ("@base64d, (@base64|@base64d)", '"aGVsbG8="'), ("@base32d", '"NBSWY3DP"'), ("@urid", '"a%20b%C3%A9"'),
    # --- errors: the tag and the text
    (".a", "[1]"), ('error("boom")', "1"), ('1, error("boom")', "1"), ("error(null)", "1"),
    ("error({a:1})", "1"), ("error", '"x"'), ("error", "null"), ("{} | error", "1"),
    ("%%%", "1"), (".nope.x[", "1"), ("", "1"), (" ", "1"), ("toarray", "1"), ("1 | ascii", "1"),
    ("$__prog_args", "1"), ("$nope", "1"), ("foo(1)", "1"), ('include "a"; .', "1"),
    ('import "a" as a; .', "1"), ('import "a" as $a; .', "1"), ("input", "1"), ("[inputs]", "1"),
    ("first(inputs)", "1"), ("input_line_number", "1"), ("input_filename", "1"), ("$__loc__", "1"),
    ("get_search_list", "1"), ("halt", "1"), ("1, halt, 2", "1"), ("halt_error", '"bye"'),
    ('"x" | halt_error(3)', "1"), ("debug", "1"), ('debug("m")', "1"), ("stderr", "1"),
    ("limit(-1; 1,2)", "1"), ("implode", "1"), ("ltrimstr(1)", "1"), ("splits(\"a\")", "1"),
    ("getpath([\"a\",\"b\"])", "-5"), ("@base32d", "-5"), ("tojson", "-5"), ("try error(\"x\") catch .", "1"),
    (".[0]", '{"a":1}'), (".[\"a\"]", "[1]"), ("1 + \"a\"", "1"), ("{} - 1", "1"), ("[] | first", "1"),
    ("\"abc\" | .[0]", "1"), ("tonumber", '"abc"'), ("fromjson", '"{bad"'), ("1 / 0", "1"), ("1 % 0", "1"),
    ("[1,2] | .[\"a\"] = 1", "1"), ("null | implode", "1"), ("\"a\" | test(\"(\")", "1"),
    ("\"a\" | test(\"a\"; \"q\")", "1"), ("{(1):2}", "1"), ("[1] | join(1)", "1"),
    (". as [$a, $b] | $a + $b", "{}"), ("ltrimstr(\"a\")", "-5"), ("env | type", "1"), ("$ENV | type", "1"),
    ("$ENV.PATH | type", "1"), ("env.HOME | type", "1"), ("$ENV.NO_SUCH_VARIABLE_HERE", "1"),
    ("now | type", "1"), ("now > 1700000000", "1"),
    # --- the input text: what jq's parser takes and how it says no
    (".", "abc"), (".", '"abc"'), (".", ""), (".", " "), (".", "1 2 3"), (".", "{bad"), (".", "[1,2"),
    (".", "nan"), (".", "NaN"), (".", "-nan"), (".", "Infinity"), (".", "-Infinity"), (".", "infinite"),
    (".", "[1,2]garbage"), (".", "1\x002"), (".\x00 garbage", "1"), (".", "\xff"), (".", '"\xff"'),
    (".", '"\\ud83d"'), (".", '"a\\u0000b"'), ('"\\u0000"', "1"), (".", '{"b":1,"a":2,"b":3}'),
    (".", "'a'"), (".", "{a:1}"), (".", "[1,]"), (".", "01"), (".", "+1"), (".", ".5"), (".", "1."),
    (".", "0x10"), (".", "true false"), (".", "tru"), (".", "﻿1"), (".", "// c\n1"), (".", "1 # c"),
    (".", '"\\x41"'), (".", '"a\tb"'), (".", '"a\nb"'), (".", "[1 2]"), (".", '{"a" 1}'), (".", '{"a":1,}'),
    (".", '{1:2}'), (".", "nul"), (".", "null\n"), (".", "\t[ 1 , 2 ]\r\n"),
    # --- numbers: literals kept, arithmetic in doubles
    (".", "123456789012345678901234567890"), (".+1", "123456789012345678901234567890"),
    (".+1", "9007199254740993"), (".", "9007199254740993"), (".[0]", "[9007199254740993]"),
    (".", "1e1000"), (".", "-1e1000"), (".", "1e-1000"), (".", "1.0"), (".", "1e2"), (".", "1E2"),
    (".", "100000000000000000000.0"), (".", "0.1"), (".", "-0"), (".", "-0.0"), (".*1", "-0.0"),
    (".", "1.10"), (".", "1.000000000000000000000001"), (".", "0.10000000000000000000000000001e-5"),
    (".", "[1.0, 1.10, 100e-2, 1e0, 0e0, 0.0, 1E+2, 1e+2, 12e3]"), (". == 9007199254740992", "9007199254740993"),
    ("tojson", "100000000000000000000000000001"), ("tostring", "1.10"), ("[., tojson, tostring, @text, @json]", "1e1000"),
    ("[.[] | tojson]", "[1e1000, -1e1000, 1.0, 100000000000000000000000000001, 0.1e1, 1e17, 3.0]"),
    ("[.[] | . + 0]", "[1e1000, -1e1000, 1.0, 100000000000000000000000000001, 0.1e1, 1e17, 3.0]"),
    ("3.0, 3, 1e2, 1.0e2, 0.1+0.2, 1/3, 1e17, 1e17+0, 12345678901234567890+0, 1e-5, 1.5e300*1.5e300, -(1.5e300*1.5e300), nan, infinite, -infinite, [nan]", "null"),
    ("[100000000000000000000000000001, 1.000000000000000000000001, 1e1000, -0, 0.1+0.2, 3.0, 1e17, 1.5e300*1e10]", "1"),
    ("have_decnum, have_literal_numbers", "1"), ("[1e1000] | tojson", "1"), ("1e1000 | tostring", "1"),
    ("100000000000000000000000000000000000000000 | tojson", "1"), ("[.[] | tonumber]", '["1", "1.50", "1e3", "0x1", " 1"]'),
    ("[.[] | tonumber]", '["123456789012345678901234567890", "1e1000", "-0", "nan"]'),
    ("[nan] | sort, (nan < 1), (nan == nan), ([nan, 1] | min)", "1"), ("[1, 1.0, 1e0] | unique", "1"),
    (". as $x | [$x, $x + 0, ($x | floor), ($x | tostring), ($x | tojson)]", "12345678901234567890"),
    ("[limit(3; range(0; 1; 0.3))], [range(5; 0; -2)], (10 / 3), (10 % 3), (-10 % 3), (5.9 % 2.1)", "1"),
    ("[1,2,3] | IN(2), index(2), (.[1:] | length), .[-1:], (.[1:] = [9])", "1"),
    ("abs, toarray?, significand, (. | fabs), -. , length", "-5"), ("@text, @json, tojson, tostring, ascii?", "65"),
    # --- libm: glibc in EMQX, musl in the module
    ("[pow(2; 0.5), pow(10; -2), pow(2; 64), pow(0; 0), exp10, exp2, exp, expm1, log, log2, log10, log1p]", "2"),
    ("[sqrt, cbrt, sin, cos, tan, asin, acos, atan, sinh, cosh, tanh, asinh, atanh]", "0.5"),
    ("[acosh, gamma, lgamma, tgamma, lgamma_r, frexp, logb, significand, trunc, round, ceil, floor, rint, nearbyint]", "5.5"),
    ("[drem(10; 3), ldexp(1; 10), scalb(1; 3), scalbln(1; 3), nextafter(1; 2), nexttoward(1; 2), fma(2; 3; 4), fmin(1; 2), fmax(1; 2), fmod(7; 3), hypot(3; 4), atan2(1; 2), copysign(1; -2), fdim(5; 3)]", "1"),
    ("[j0, j1, y0, y1, jn(2; 1.5), yn(2; 1.5), erf, erfc, modf, ilogb]", "1.5"),
    ("[.[] | sin, cos, exp, log, tan, sqrt]", "[0.1, 0.7, 1.3, 2.9, 10.1, 100.7, 1e5, 1e-5, 3.141592653589793]"),
    ("[.[] | pow(.; 1.5), pow(.; -0.3), exp10, log10, cbrt]", "[0.1, 0.7, 1.3, 2.9, 10.1, 100.7]"),
    ("[.[] | gamma, tgamma, erf, j0, y0, asinh, atan]", "[0.1, 0.7, 1.3, 2.9, 10.1]"),
    # --- strings, unicode, regex (oniguruma)
    (".|length, utf8bytelength, explode", '"h\\u00e9\\ud83d\\ude00"'), (".", '"\\u00e9\\ud83d\\ude00"'),
    ("[1,2] | implode?, ([65, 233, 128512] | implode)", "1"), ('"\\u00e9" | @uri, ascii_downcase, ascii_upcase', "1"),
    ('" a " | trim, ltrim, rtrim', "1"), ('"abc" | trimstr("a"), ltrimstr("a"), rtrimstr("c"), startswith("a"), endswith("b")', "1"),
    ('"abc" | test("B"; "i"), test("B"), [match("b").offset], sub("b"; "X"), gsub("[ac]"; "-")', "1"),
    ('"aXbxc" | [scan("x"; "i")], [splits("x"; "gi")], split("x"; "i"), ascii_downcase, split("X")', "1"),
    ('"test 123 abc 45" | [match("\\\\d+"; "g") | .string], capture("(?<n>\\\\d+) (?<w>[a-z]+)"), [scan("\\\\p{L}+")]', "1"),
    ('"foo bar" | sub("(?<a>\\\\w+) (?<b>\\\\w+)"; "\\(.b) \\(.a)"), gsub("o"; "0"; "g"), [match("o"; "g").offset]', "1"),
    ('"\\u00e9\\u00c9" | test("\\u00e9"; "i"), ascii_downcase, [match("."; "g").length], gsub("\\\\p{Lu}"; "U")', "1"),
    ('"a.b" | test("a.b"; "x"), test("A.B"; "ix"), test("a\\nb"; "s"), [splits(", *"; null)]', "1"),
    ('"x" * 0, "x" * 3, ("ab" | . / "b"), ("a,b" | split(",")), ("abc" | .[1:2]), ("abc" | indices("b"))', "1"),
    ('[.[] | tostring], [.[] | tojson], [.[] | type], [.[] | length]', '[1, "a", null, true, [1], {"a":1}, 1.5]'),
    ('@json "x\\(.)", @base64 "\\(.)", "\\(1;2)"?, "a\\(1+1)b"', "-5"), ('ascii', "65"), ('@sh', '["a b", "c\'d", 1]'),
    ('tojson, (tojson | fromjson), ("[1,{\\"a\\":nan}]" | fromjson)', '{"a":[1,"\\u00e9",null]}'),
    ('ltrimstr("a"), rtrimstr("a"), ascii_downcase?, (tostring | ascii_downcase)', "5"),
    ('[.[] | ascii_downcase] | sort, unique, (map(length) | add), (.[0] | explode | implode)', '["B", "a", "\\u00c9", "b"]'),
    ('getpath(["a","b"]), getpath(["x","y"]), (try getpath(["a","b","c"]) catch .), [paths(type == "number")]', '{"a":{"b":1}}'),
    # --- control flow, reductions, generators
    ("[limit(0; 1,2)], [limit(3; repeat(.))], first(range(10; 0; -1)), [range(3)], until(. > 100; . * 2), [.[]?]", "1"),
    ("reduce range(100) as $i (0; . + $i), ([range(10)] | add), (foreach range(5) as $i (0; . + $i; [$i, .]))", "1"),
    ("if . then 1 end, (if . == 2 then \"a\" elif . == 1 then \"b\" else \"c\" end), (. // 5), (null // 5), (.a? // \"d\")", "1"),
    ("[.[] | select(. != null)] | sort, sort_by(-.), min, max, unique, add, (map(. * 2) | any(. > 5)), all(. > 0)", "[3, null, 1, 2]"),
    ("def f(n): if n == 0 then 0 else 1 + f(n - 1) end; f(1000)", "1"), ("def fac: if . <= 1 then 1 else . * (. - 1 | fac) end; fac", "20"),
    ("[recurse(if . < 5 then . + 1 else empty end)], [1, [2]] | flatten, (.[1:] | length), getpath([1, 0])?", "1"),
    ("label $out | foreach .[] as $x (0; . + $x; if . > 3 then ., break $out else empty end)", "[1, 2, 3, 4]"),
    (". as [$a, $b, {c: $c}] | [$a, $b, $c], (. as [$a] ?// $a | [$a])", '[1, 2, {"c": 3}]'),
    ("{} | .a.b |= 1, (.a += 1), (.[\"x\"] //= 3), ({a: 1} * {a: {b: 2}}), ({a: 1} + {b: 2})", "1"),
    (".. |= ., ([paths] | length), (to_entries | from_entries), (map_values(. + 1)?), (pick(.a)?)", '{"a": 1, "b": 2}'),
    ("[1, 2, 3] | pick(.[0]), del(.[0, 2]), add(.[]), (. - [2]), index(2), (.[2:] + .[:1]), (combinations(2)? // 0), transpose?", "1"),
    ("splits(\"a\")?, ([splits(\"a\")]?), (try error(\"x\") catch .), (try error catch .), ([.[]?]), isvalid(.a)?", '"banana"'),
    ("input_line_number?, ($__loc__ | .line), ([limit(2; .[])]), (first, last, nth(1)), (group_by(. % 2)), (unique_by(. % 2))", "[1, 2, 3]"),
    ("getpath([\"a\"])?, ltrimstr(\"x\"), (tojson | length), ([.[] | numbers]), ([.[] | strings]), any, all, (flatten(1) | length)", '[1, "a", [2, [3]]]'),
    ("env | has(\"PATH\"), ($ENV | has(\"PATH\")), (splits(\"a\")? // \"n\"), (ascii? // \"n\"), (@base32d? // \"n\"), (significand? // \"n\")", "64"),
    # --- time (UTC in the EMQX image)
    ("todate, (. | strftime(\"%A, %B %d, %Y\")), gmtime, (gmtime | mktime), (gmtime | todate), dateadd(\"seconds\"; 10)?", "1700000000"),
    ("strftime(\"%Y-%m-%dT%H:%M:%SZ\"), strftime(\"%j %U %a %b %e %H %I %p %y %C %G %g %u %V %w %%\"), localtime, strflocaltime(\"%H:%M %Z\")", "1700000000"),
    ("fromdate, (strptime(\"%Y-%m-%dT%H:%M:%SZ\")), (strptime(\"%Y-%m-%dT%H:%M:%SZ\") | mktime), fromdateiso8601, date?", '"2015-03-05T23:51:47Z"'),
    ("strptime(\"%H:%M %Y-%m-%d\"), (strptime(\"%H:%M %Y-%m-%d\") | mktime), (strptime(\"%H:%M %Y-%m-%d\") | todate)", '"10:15 2020-01-02"'),
    ("todate, (gmtime | .[5]), strftime(\"%s %S\"), (. | localtime | mktime), dateadd(\"seconds\"; 1)?", "1700000000.5"),
    ("[.[] | todate]", "[0, -1, 1e10, 253402300799, 1.5e9, 4102444800]"), ("strptime(\"%d %B %Y\") | mktime", '"05 March 2015"'),
    ("strptime(\"%a, %d %b %Y %T %z\") | mktime", '"Thu, 05 Mar 2015 23:51:47 +0100"'), ("strftime(\"%Z %z\"), strflocaltime(\"%Z %z\")", "0"),
    ("todate", '"x"'), ("mktime", "[1]"), ("strptime(\"%Y\")", '"abc"'), ("strftime(1)", "1"), ("gmtime", "1e300"),
    # --- depth and size
    ("tojson | length", nested(100)), ("tojson | length", nested(1000)), ("tojson | length", nested(5000)),
    ("tojson | length", nested(10000)), ("tojson | length", nested(10001)),
    (". as $x | 1", nested(10000)), ("[..] | length", nested(2000)), ("flatten | length", nested(3000)),
    ("getpath([range(500) | 0]) | tojson | length", nested(1000)), ("1", nested(300, close=False)),
    ("reduce range(5000) as $i (null; [.]) | tojson | length", "1"), ("reduce range(10001) as $i (null; [.]) | tojson | length", "1"),
    ("reduce range(20000) as $i (null; [.]) | 1", "1"), ("def f(n): if n == 0 then 0 else 1 + f(n - 1) end; f(100000)", "1"),
    ("def f(n): if n == 0 then 0 else f(n - 1) end; f(1000000)", "1"), ("[range(100000)] | length", "1"),
    ("[range(100000)] | map(. * 2) | add", "1"), ("\"x\" * 1000000 | length", "1"), ("last(range(1000000))", "1"),
    ("[range(20000) | tostring] | join(\",\") | length", "1"), ("[range(3000)] | sort_by(-.) | .[0]", "1"),
    ("[range(2000) | {k: (. % 7), v: .}] | group_by(.k) | map(length)", "1"),
    ("reduce range(2000) as $i ({}; .[\"k\\($i)\"] = $i) | length, (keys | .[0])", "1"),
    ("[limit(1000; repeat(\"ab\"))] | join(\"\") | test(\"^(ab)+$\")", "1"), ("\"a\" * 30 | test(\"(a*)*b\")", "1"),
    # --- more time, and doubles as jq prints them
    ("todate, strftime(\"%A, %B %d, %Y\"), gmtime, (gmtime | mktime), (gmtime | todate)", "1700000000"),
    ("todate, (gmtime | .[5]), strftime(\"%s %S\"), (localtime | mktime), gmtime", "1700000000.5"),
    ("env | has(\"PATH\"), ($ENV | has(\"PATH\")), (@base32d? // \"n\"), (significand? // \"n\")", "64"),
    ("[.[] | tojson]", "[0.1, 1.5, 3.14159, 1e-7, 123456.789, 2.5e-10, 1e21, 1.7976931348623157e308, 5e-324]"),
    ("[.[] | . * 1]", "[0.1, 1.5, 3.14159, 1e-7, 123456.789, 2.5e-10, 1e21, 1.7976931348623157e308, 5e-324]"),
    ("[.[] | . / 3]", "[0.1, 1.5, 3.14159, 1e-7, 123456.789, 2.5e-10, 1e21, 1.7976931348623157e308, 5e-324, 1, 2, 10]"),
    ("[.[] | . * 1.1 | tostring]", "[1, 3, 7, 100, 1e15, 1e16, 1e17, 123456789012, 0.000001, 4.35]"),
    ("[.[] | floor, sqrt, (. * 100 | round / 100)]", "[2, 10.5, 99.995, 1e10, 0.125]"),
    # --- a compiled program is reused: what one run leaves behind
    ("first(range(10))", "1"), ("first(range(10))", "2"), ("halt", "1"), ("1, halt, 2", "2"), ("1, halt, 2", "3"),
    ("limit(1; .[])", "[1, 2, 3]"), ("limit(1; .[])", "[4, 5, 6]"), ("error(\"a\")", "1"), ("error(\"a\")", "2"),
    (". as $x | input_line_number? // $x", "7"), (". as $x | input_line_number? // $x", "8"),
]


class NodeDown(Exception):
    pass


def run(expr):
    try:
        out = subprocess.run(
            ["docker", "exec", CONTAINER, "emqx", "eval", expr],
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )
    except subprocess.TimeoutExpired:
        raise NodeDown("hang") from None
    if out.returncode != 0:
        # The node is gone (an RPC `nodedown`, or the container went with it).
        print(f"emqx eval failed ({out.returncode}): {out.stdout}{out.stderr}", file=sys.stderr)
        raise NodeDown("nodedown")
    return out.stdout


def restart():
    subprocess.run(["docker", "restart", "-t", "1", CONTAINER], capture_output=True, check=False)
    for _ in range(120):
        probe = subprocess.run(
            ["docker", "exec", CONTAINER, "emqx", "eval", "ok."], capture_output=True, text=True, check=False
        )
        if probe.returncode == 0 and "ok" in probe.stdout:
            return
        time.sleep(1)
    raise SystemExit("EMQX did not come back")


def ask(cases):
    """The oracle lines for `cases`; one at a time once a batch takes the node down."""
    pairs = ", ".join('{<<"%s">>, %s}' % (encode(p), erlang_input(i)) for p, i in cases)
    try:
        printed = run(ERL % pairs)
    except NodeDown as down:
        restart()
        if len(cases) == 1:
            p, i = cases[0]
            name = i.name if isinstance(i, Symbolic) else encode(i)
            print(f"{down}: {p!r}", file=sys.stderr)
            return ["%s %s %s" % (encode(p), name, down)]
        return [line for case in cases for line in ask([case])]
    got = ["".join(re.findall(r'"([^"]*)"', m)) for m in re.findall(r"<<(.*?)>>", printed, re.S)]
    if len(got) != len(cases):
        raise SystemExit(f"unexpected answer: {printed}")
    return got


def b64(text):
    raw = text if isinstance(text, bytes) else text.encode("utf-8", "surrogateescape")
    return base64.b64encode(raw).decode()


def encode(text):
    # "\xff" in a case means the byte 0xFF, not U+00FF.
    if isinstance(text, str) and any(0x80 <= ord(c) <= 0xFF for c in text) and "\\x" not in text:
        try:
            return b64(text.encode("latin-1")) if "\xff" in text else b64(text)
        except UnicodeEncodeError:
            return b64(text)
    return b64(text)


def erlang_input(text):
    if isinstance(text, Symbolic):
        return '{<<"%s">>, %s}' % (text.name, text.erlang)
    return '{<<"%s">>, base64:decode(<<"%s">>)}' % (encode(text), encode(text))


ERL = (
    "[begin R = (catch jq:process_json(base64:decode(P), Input, 10000)), "
    "iolist_to_binary(lists:join(<<\" \">>, case R of "
    "{ok, L} -> [P, I, <<\"ok\">> | [base64:encode(X) || X <- L]]; "
    "{error, {T, M}} -> [P, I, <<\"error\">>, atom_to_binary(T), base64:encode(iolist_to_binary(M))]; "
    "Other -> [P, I, <<\"crash\">>, base64:encode(iolist_to_binary(io_lib:format(\"~0p\", [Other])))] "
    "end)) end || {P, {I, Input}} <- [%s]]."
)


def main():
    version = run("emqx_release:version().").strip()
    print(f"# jq:process_json/3 (timeout 10000 ms) on EMQX {version} (emqx/jq NIF v0.4.1, jq 1.8.1). Generated by")
    print("# crates/mqtt-wasm-sandbox/jq-module/emqx-oracle.py; do not edit.")
    lines = []
    for at in range(0, len(CASES), 8):
        lines.extend(ask(CASES[at : at + 8]))
    for line in lines:
        print(line)
    print(f"{len(lines)} cases", file=sys.stderr)


if __name__ == "__main__":
    main()
