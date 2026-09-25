#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
profile=${QOS1_PROFILE:-$here/confirm.env}
. "$profile"
: "${QOS1_DRIVER_ARCHIVE:?build qos1-driver/build.sh first and export QOS1_DRIVER_ARCHIVE}"
export RUN_DIR="${RUN_DIR:-$HOME/.cache/fss-qos1-proof/$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$RUN_DIR"
cp "$profile" "$RUN_DIR/profile.env"
sha256sum "$QOS1_DRIVER_ARCHIVE" > "$RUN_DIR/driver-archive.sha256"
# Snapshot all harness changes, including files outside git, before provisioning.
tar -czf "$RUN_DIR/harness-source.tar.gz" --exclude=.runs --exclude=.terraform --exclude=__pycache__ --exclude='*.tfstate*' --exclude='*.tfvars*' -C "$here/.." .
set +e
# Sizes come from the profile (QOS1_SIZES) so a 5-node run is a profile, not an
# edit to this wrapper. Default 3: the size the campaign calibrated on.
read -r -a qos1_sizes <<<"${QOS1_SIZES:-3}"
"$here/../run.sh" full "${qos1_sizes[@]}" 2>&1 | tee "$RUN_DIR/run.log"
rc=${PIPESTATUS[0]}
set -e
python3 "$here/report.py" "$RUN_DIR" --output "$RUN_DIR/analysis"
python3 - "$RUN_DIR" <<'PY'
import hashlib,sys
from pathlib import Path
p=Path(sys.argv[1]);files=[f for f in (p/'results').rglob('*') if f.is_file()]
(p/'evidence.sha256').write_text(''.join(f'{hashlib.sha256(f.read_bytes()).hexdigest()}  {f.relative_to(p)}\n' for f in sorted(files)))
PY
exit "$rc"
