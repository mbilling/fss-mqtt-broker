#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
out=${1:?output directory}
mkdir -p "$out"
out=$(cd "$out" && pwd)
src="$out/source"
if [ ! -d "$src" ]; then
 git clone -q https://github.com/emqx/emqtt-bench.git "$src"
 git -C "$src" checkout -q cb86b18848da17cae1024e1d06f7e43f8f6691a1
fi
[ "$(git -C "$src" rev-parse HEAD)" = cb86b18848da17cae1024e1d06f7e43f8f6691a1 ]
git -C "$src" show HEAD:src/emqtt_bench.erl > "$out/emqtt_bench.erl"
python3 "$here/patch.py" "$out/emqtt_bench.erl"
mkdir -p "$out/include"
curl -fsSL https://raw.githubusercontent.com/emqx/emqtt/fd7f149378ad4a603297d4e0a4eb037fea76e333/include/emqtt.hrl -o "$out/include/emqtt.hrl"
sed -i 's|-include_lib("emqtt/include/emqtt.hrl").|-include("emqtt.hrl").|' "$out/emqtt_bench.erl"
cp "$here/qos1_audit.erl" "$out/"
git -C "$src" show HEAD:src/http/emqtt_bench_http_metrics.erl > "$out/emqtt_bench_http_metrics.erl"
python3 - "$out/emqtt_bench_http_metrics.erl" <<'PATCH'
from pathlib import Path
import sys
p=Path(sys.argv[1]);s=p.read_text();old='Body = prometheus_text_format:format(),';assert s.count(old)==1
p.write_text(s.replace(old,'Body = [prometheus_text_format:format(), "\\n# EOF\\n"],'))
PATCH
docker run --rm -v "$out:/work" -w /work erlang:27-alpine@sha256:2c5be8c730cafda69344423339377c9b28072540a587efb835a2f5b95c547911 erlc -I include emqtt_bench.erl qos1_audit.erl emqtt_bench_http_metrics.erl
base=emqx/emqtt-bench:0.6.3@sha256:ae7f2d56cd49b14824c835140c808b093c5e3f2defb3a29b34b17560feb456cd
cid=$(docker create "$base")
trap 'docker rm "$cid" >/dev/null 2>&1 || true' EXIT
docker cp "$cid:/emqtt_bench/escript/emqtt_bench" "$out/original.escript"
python3 - "$out" <<'PY'
from pathlib import Path
import sys,zipfile,io
p=Path(sys.argv[1]);raw=(p/'original.escript').read_bytes();prefix=raw[:raw.index(b'PK\x03\x04')]
buf=io.BytesIO()
with zipfile.ZipFile(p/'original.escript') as src,zipfile.ZipFile(buf,'w',zipfile.ZIP_DEFLATED) as dst:
 for name in src.namelist():
  if name not in ('emqtt_bench/ebin/emqtt_bench.beam','emqtt_bench/ebin/emqtt_bench_http_metrics.beam'):dst.writestr(name,src.read(name))
 for name in ['emqtt_bench','qos1_audit','emqtt_bench_http_metrics']:
  dst.writestr(f'emqtt_bench/ebin/{name}.beam',(p/f'{name}.beam').read_bytes())
(p/'emqtt_bench').write_bytes(prefix+buf.getvalue())
PY
printf 'FROM %s\nCOPY --chmod=755 emqtt_bench /emqtt_bench/escript/emqtt_bench\n' "$base" > "$out/Dockerfile"
image=${QOS1_DRIVER_IMAGE:-fss-qos1-audit:local}
docker build -q -t "$image" "$out"
docker save "$image" | gzip > "$out/driver.tar.gz"
sha256sum "$out/driver.tar.gz" "$out/emqtt_bench" > "$out/SHA256SUMS"
