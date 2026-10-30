# netprobe provider: Hetzner Cloud, with the hcloud CLI (sourced by run.sh).
#
#   HCLOUD_TOKEN   required
#   TYPE           server type (default ccx53)
#   LOCATION       default fsn1
#   IMAGE          default ubuntu-24.04
#
# Two servers on one private network (10.98.1.0/24), a firewall with SSH and ICMP open,
# and an SSH key, all labelled purpose=mqttd-netprobe and run=<stamp>.

TYPE="${TYPE:-ccx53}"
LOCATION="${LOCATION:-fsn1}"
IMAGE="${IMAGE:-ubuntu-24.04}"

provider_check() {
	[ -n "${HCLOUD_TOKEN:-}" ] || die "set HCLOUD_TOKEN"
	command -v hcloud >/dev/null || die "the hcloud CLI is required"
}

provider_quote() {
	local price
	price=$(hcloud server-type describe "$TYPE" -o json | jq -r --arg l "$LOCATION" \
		'.prices[] | select(.location == $l) | .price_hourly.gross')
	[ -n "$price" ] || die "no price for $TYPE in $LOCATION"
	say "hetzner: 2 x $TYPE in $LOCATION = EUR $(python3 -c "print(f'{2 * $price:.4f}')")/h; ~10 min = EUR $(python3 -c "print(f'{2 * $price * 10 / 60:.2f}')") prorated"
}

# Sets A, B (public addresses for SSH) and APRIV, BPRIV (the private network's).
provider_create() {
	hcloud ssh-key create --name "$NAME" --public-key-from-file "$KEY.pub" --label "$LABEL" --label "run=$STAMP" >/dev/null
	hcloud network create --name "$NAME" --ip-range 10.98.0.0/16 --label "$LABEL" --label "run=$STAMP" >/dev/null
	hcloud network add-subnet "$NAME" --type cloud --network-zone eu-central --ip-range 10.98.1.0/24 >/dev/null
	cat >"$RUN/fw.json" <<'JSON'
[{"direction":"in","protocol":"tcp","port":"22","source_ips":["0.0.0.0/0","::/0"]},
 {"direction":"in","protocol":"icmp","source_ips":["0.0.0.0/0","::/0"]}]
JSON
	hcloud firewall create --name "$NAME" --rules-file "$RUN/fw.json" --label "$LABEL" --label "run=$STAMP" >/dev/null
	say "creating 2 x $TYPE"
	local h
	for h in a b; do
		hcloud server create --name "$NAME-$h" --type "$TYPE" --image "$IMAGE" --location "$LOCATION" \
			--ssh-key "$NAME" --network "$NAME" --firewall "$NAME" \
			--label "$LABEL" --label "run=$STAMP" -o json >"$RUN/server-$h.json" &
	done
	wait
	A=$(jq -r '.server.public_net.ipv4.ip' "$RUN/server-a.json")
	B=$(jq -r '.server.public_net.ipv4.ip' "$RUN/server-b.json")
	APRIV=$(hcloud server describe "$NAME-a" -o json | jq -r '.private_net[0].ip')
	BPRIV=$(hcloud server describe "$NAME-b" -o json | jq -r '.private_net[0].ip')
}

# Deletes this run's resources; prints whatever of them is left (the leak audit).
provider_teardown() {
	local s sel="$LABEL,run=$STAMP"
	for s in $(hcloud server list -l "$sel" -o noheader -o columns=id 2>/dev/null); do
		hcloud server delete "$s" >/dev/null || true
	done
	hcloud firewall delete "$NAME" >/dev/null 2>&1 || true
	hcloud network delete "$NAME" >/dev/null 2>&1 || true
	hcloud ssh-key delete "$NAME" >/dev/null 2>&1 || true
	{ hcloud server list -l "$sel" -o noheader; hcloud firewall list -l "$sel" -o noheader
		hcloud network list -l "$sel" -o noheader; hcloud ssh-key list -l "$sel" -o noheader; } 2>&1 || true
}
