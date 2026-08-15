//! The Rust converter must agree with the Python one, byte for byte (ADR 0056 T10).
//!
//! `scripts/migrate/from-mosquitto.py` is not retired: CI already proves *its* output boots
//! the real broker (ADR 0051 T6). Keeping both is only safe while they agree — **two
//! converters that disagree are worse than one**, because a migrator would get a different
//! policy depending on which they happened to run, and neither would be obviously wrong.
//!
//! So this runs both over the same fixtures and diffs the results. It is not a
//! "does it look plausible" test; the comparison is exact.
//!
//! Skipped, loudly, when `python3` is absent or we are not in a checkout — the Python side
//! is what it compares against, and a test that quietly passed without running it would be
//! claiming agreement it never checked.

use std::path::{Path, PathBuf};
use std::process::Command;

/// The fixture pair: a realistic Mosquitto deployment and its ACL file.
const CONF: &str = "\
persistence true
persistence_location /var/lib/mosquitto/
max_queued_messages 1000
max_packet_size 262144
max_inflight_messages 40
allow_anonymous false
acl_file ACLPATH
password_file /etc/mosquitto/passwd
listener 1883 127.0.0.1
max_connections 5000
listener 8883 0.0.0.0
certfile /etc/certs/server.crt
keyfile /etc/certs/server.key
cafile /etc/certs/ca.crt
crlfile /etc/certs/crl.pem
max_connections 400
listener 9001 0.0.0.0
protocol websockets
listener 8885 0.0.0.0
psk_file /etc/mosq/psk
psk_hint pskid
connection remote-site
address up.example.com:8883
bridge_cafile /certs/bridge-ca.crt
sys_interval 10
autosave_interval 1800
retain_available false
some_future_option 3
";

/// The ACL fixture carries a domain-qualified username on purpose: `CORP\jdoe` used to be
/// emitted as `identities = ["CORP\jdoe"]`, which is not valid TOML, so the broker refused
/// the WHOLE policy over one user. Both converters must escape it, identically (2026-08-14).
const ACL: &str = "\
# a comment, and a blank line follow

topic read public/#
user sensor-1
topic write sensors/sensor-1/#
topic read commands/sensor-1/#
topic denied/topic
user admin
topic readwrite #
topic deny secrets/#
user CORP\\jdoe
topic readwrite sites/CORP\\jdoe/#
topic read odd\"quoted/#
pattern read devices/%u/status
pattern write devices/%c/up
user star*user
topic write out/#
user bob
topic read c/%c/x
nonsense line here
";

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("tools/mqttui sits two levels below the root")
        .to_path_buf()
}

fn python3() -> Option<PathBuf> {
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|d| d.join("python3"))
        .find(|p| p.is_file())
}

/// Both converters, over the same fixtures, must produce identical bytes.
#[test]
#[allow(clippy::too_many_lines)] // one assertion per property the two converters must
                                 // share; splitting them would hide which fixture feeds which.
fn the_rust_converter_agrees_with_the_python_one() {
    let root = repo_root();
    let script = root.join("scripts/migrate/from-mosquitto.py");
    let Some(python) = python3() else {
        // Loud, not silent: this test's whole value is the comparison.
        eprintln!("SKIP: python3 is not on PATH, so the differential comparison did NOT run");
        return;
    };
    if !script.is_file() {
        eprintln!("SKIP: {} is absent; not in a checkout", script.display());
        return;
    }

    let dir = std::env::temp_dir().join(format!("mqttui-diff-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let acl_path = dir.join("aclfile");
    let conf_path = dir.join("mosquitto.conf");
    std::fs::write(&acl_path, ACL).expect("write acl");
    std::fs::write(
        &conf_path,
        CONF.replace("ACLPATH", &acl_path.to_string_lossy()),
    )
    .expect("write conf");

    // ── the Python original ───────────────────────────────────────────────────────
    let py_config = dir.join("py.toml");
    let py_acl = dir.join("py-acl.toml");
    let out = Command::new(python)
        .arg(&script)
        .arg(&conf_path)
        .arg("--out-config")
        .arg(&py_config)
        .arg("--out-acl")
        .arg(&py_acl)
        .output()
        .expect("run the python converter");
    assert!(
        out.status.success(),
        "the python converter failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // ── the Rust port ─────────────────────────────────────────────────────────────
    let (rs_config, rs_acl) = mqttui_migrate(&conf_path);

    let py_config_text = std::fs::read_to_string(&py_config).expect("python config");
    let py_acl_text = std::fs::read_to_string(&py_acl).expect("python acl");

    assert_eq!(
        rs_config, py_config_text,
        "\nthe two converters produced DIFFERENT configs.\n\
         Two converters that disagree are worse than one: a migrator would get a different \
         answer depending on which they ran. Reconcile them — the Python one is proven to \
         boot the real broker (ADR 0051 T6), so it is the reference.\n"
    );
    assert_eq!(
        rs_acl.as_deref().unwrap_or(""),
        py_acl_text,
        "\nthe two converters produced DIFFERENT ACL policies (see above for why that matters)\n"
    );

    // And the comparison must not be vacuous: both sides must have produced something real.
    assert!(
        py_config_text.contains("[limits]") && py_acl_text.contains("[[rules]]"),
        "the fixtures produced no config/rules, so an equality assertion proves nothing"
    );

    // Byte-identical is not enough — both converters once agreed on INVALID TOML
    // (`[listeners]` declared once per listener; tomllib rejects it). The fixture above
    // deliberately has two listeners, so this parse assertion fails on that regression.
    // Found by the 2026-08-11 review panel running the tool, not by this test.
    toml::from_str::<toml::Value>(&rs_config).unwrap_or_else(|e| {
        panic!("the converters agreed on config output that is not valid TOML: {e}")
    });
    toml::from_str::<toml::Value>(py_acl_text.as_str()).unwrap_or_else(|e| {
        panic!("the converters agreed on ACL output that is not valid TOML: {e}")
    });

    // The cafile-without-require_certificate honesty contract: the fixture's TLS listener
    // sets cafile but not require_certificate, so client_ca must be COMMENTED with the
    // TODO — emitting it active silently turns a cert-optional listener into mTLS.
    assert!(
        rs_config.contains("# client_ca ="),
        "client_ca was emitted ACTIVE for a listener that never required certificates"
    );
    assert!(
        rs_config.contains("require_certificate was NOT"),
        "the require_certificate TODO is missing — the drop went silent again"
    );

    // TOML ESCAPING, on both sides. `toml::from_str` above already rejects an unescaped
    // backslash, but assert the exact escaped bytes too: a converter that dropped the
    // hostile user entirely would also parse.
    assert!(
        rs_acl
            .as_deref()
            .unwrap_or("")
            .contains(r#"identities = ["CORP\\jdoe"]"#),
        "the domain-qualified username was not TOML-escaped in the ACL"
    );
    assert!(
        rs_acl
            .as_deref()
            .unwrap_or("")
            .contains(r#""odd\"quoted/#""#),
        "a double quote in a topic filter was not TOML-escaped"
    );
    toml::from_str::<toml::Value>(rs_acl.as_deref().unwrap_or(""))
        .unwrap_or_else(|e| panic!("the Rust port's ACL is not valid TOML: {e}"));

    // A `crl` is only legal beside an ACTIVE `client_ca`: the broker's own words are
    // `invalid configuration: tls.crl requires tls.client_ca`. This fixture sets `cafile`
    // WITHOUT `require_certificate`, so `client_ca` is commented — and both converters used
    // to emit `crl` anyway, producing a config the broker REJECTS. Neither this test (which
    // only parses TOML) nor test-from-mosquitto.sh (whose fixture had no crlfile) could see
    // it; found 2026-08-15 by running --check-config over permuted inputs.
    assert!(
        !rs_config.contains("\ncrl = "),
        "crl was emitted ACTIVE beside a commented client_ca — the broker refuses that pair"
    );
    assert!(
        rs_config.contains("# crl = \"/etc/certs/crl.pem\"")
            && rs_config.contains("tls.crl requires tls.client_ca"),
        "the crlfile was dropped instead of emitted as a commented candidate with the reason"
    );

    // The translated policy must be REFERENCED by the config, or mqttd enforces no
    // authorization at all and the ACL's own header claims it denies by default.
    assert!(
        rs_config.contains("acl_file = \"/etc/mqttd/acl.toml\""),
        "the config does not name the translated ACL, so nothing enforces it"
    );

    // max_inflight_messages is NOT receive_maximum — opposite directions (the fixture sets
    // it, so a re-added mapping fails here).
    assert!(
        !rs_config.contains("receive_maximum = 40"),
        "max_inflight_messages was mapped onto [limits] receive_maximum — opposite directions"
    );
    assert!(
        rs_config.contains("max_inflight_messages 40: NOT carried over"),
        "max_inflight_messages was dropped without the direction-flip TODO"
    );

    // PROVENANCE OR NOTHING (2026-08-15). Every finding of the three review rounds that
    // mattered was a LIVE security-relevant value the tool had not derived from the input, so
    // both converters now emit those through one gate that refuses to write a live line
    // without the input key it came from. This asserts the property on the OUTPUT, which is
    // what an operator (and property_sweep.py's invariant G) reads: every uncommented
    // security-relevant line carries `# from:`.
    for line in rs_config.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with('#') {
            continue;
        }
        let Some((key, _)) = line.split_once(" = ") else {
            continue;
        };
        let security = [
            "plaintext_bind",
            "tls_bind",
            "ws_bind",
            "wss_bind",
            "quic_bind",
            "cert",
            "key",
            "client_ca",
            "crl",
            "acl_file",
            "password_file",
            "allow_anonymous",
            "mtls_identity_source",
        ];
        assert!(
            !security.contains(&key) || line.contains("  # from: "),
            "`{key}` is emitted LIVE with no recorded source key: {line}\n\
             A security-relevant value the converter did not derive from the input must be \
             emitted COMMENTED OUT with a TODO naming the decision — that is the whole \
             fail-open class the provenance gate exists to close."
        );
    }

    // ...and the fixture must actually exercise it, or the loop above proves nothing.
    assert!(
        rs_config.contains("plaintext_bind = \"127.0.0.1:1883\"  # from: listener 1883 127.0.0.1")
            && rs_config.contains("cert = \"/etc/certs/server.crt\"  # from: certfile at listener"),
        "the fixture produced no provenance-carrying binds, so the loop above is vacuous"
    );

    // `protocol websockets` has an exact equivalent in ws_bind. Emitting a WebSocket listener
    // as a raw-MQTT bind breaks every browser client at cutover — and, because a WSS listener
    // then counts as an ordinary TLS listener, it also decides silently whose material wins
    // the single [tls] table (2026-08-15).
    assert!(
        rs_config.contains("ws_bind = \"0.0.0.0:9001\""),
        "a `protocol websockets` listener did not become ws_bind"
    );

    // max_connections is PER LISTENER in Mosquitto and node-wide in mqttd: the SMALLEST must
    // win, because raising a deliberately tight cap is the permissive direction.
    assert!(
        rs_config.contains("max_connections = 400") && rs_config.contains("the SMALLEST (400)"),
        "the per-listener connection caps did not collapse onto the smallest, with a TODO"
    );

    // ...and the config must be one the BROKER accepts, not merely parseable: without a
    // data dir mqttd refuses to start, and this fixture's persistence_location is what
    // provides it. The no-persistence case is covered by test-from-mosquitto.sh, which runs
    // `mqttd --check-config` against the real binary.
    assert!(
        rs_config.contains("data_dir ="),
        "no [node] data_dir was emitted, so the broker would refuse to start on this config"
    );

    // TLS-PSK: ENCRYPTED, and mqttd has no PSK at all, so the listener must NOT fall through
    // to the plaintext key. The provenance loop above cannot catch this — the fabricated bind
    // carried a genuine `# from: listener 8885 0.0.0.0` — because the gate checks where the
    // VALUE came from and the FIELD is what encodes the transport (2026-08-15).
    assert!(
        !rs_config.contains("plaintext_bind = \"0.0.0.0:8885\"")
            && !rs_config.contains("ws_bind = \"0.0.0.0:8885\""),
        "a TLS-PSK listener became a LIVE PLAINTEXT bind — an encrypted transport downgraded \
         to cleartext, in the converter shipped inside the binary"
    );
    assert!(
        rs_config.contains("# tls_bind = \"0.0.0.0:8885\"")
            && rs_config.contains("DOWNGRADE an encrypted transport"),
        "the PSK listener's bind is not an inert candidate on the TLS key, with the reason"
    );

    // A Mosquitto BRIDGE block: every key has an exact equivalent in the mqtt-bridge config
    // this repo ships, and all but `connection` used to be reported as having none.
    assert!(
        rs_config.contains("mqtt-bridge `[[upstreams]] url`")
            && rs_config.contains("mqtt-bridge `[upstreams.tls] ca`"),
        "a bridge block is still reported as having no equivalent, pointing at the wrong document"
    );

    // The ANONYMOUS-scoped ACL block. mosquitto.conf(5): "The first set of topics are applied
    // to anonymous clients, assuming allow_anonymous is true" — emitted with NO identities,
    // mqttd applies them to EVERY authenticated client, which is strictly broader.
    let acl_text = rs_acl.as_deref().unwrap_or("");
    // An UNSCOPED rule is legitimate for a `pattern` line and only for one — mosquitto.conf(5)
    // says a pattern ACL applies to all users, while a leading `topic` block does not. The
    // fixture has exactly two `pattern` lines, so two is the whole budget.
    assert_eq!(
        acl_text
            .matches("# (no identities = applies to every authenticated client)")
            .count(),
        2,
        "an unscoped rule was emitted for something other than the two `pattern` lines — an \
         ANONYMOUS-only grant widened to every authenticated identity"
    );
    assert!(
        acl_text.contains("identities = [\"anonymous\"]")
            && acl_text.contains("applied to anonymous clients"),
        "the pre-`user` topic block was not scoped to mqttd's `anonymous` subject, with the \
         man page's own words"
    );

    // Two constructs mqttd cannot express at all: a LITERAL `*` in a username (identities are
    // globs with no escape) and a literal `%c` in a plain `topic` filter (mqttd substitutes in
    // every rule, Mosquitto only in a `pattern`). Both must be refused, not widened.
    assert!(
        !acl_text.contains("identities = [\"star*user\"]"),
        "a literal `*` in a username became an mqttd identity GLOB"
    );
    assert!(
        !acl_text.contains("\"c/%c/x\""),
        "a literal `topic` filter containing %c became a SUBSTITUTING mqttd rule"
    );
    assert!(
        acl_text.contains("star*user") && acl_text.contains("c/%c/x"),
        "the refused username and filter are not named anywhere — that is a silent drop"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Call the Rust converter the way the binary does.
fn mqttui_migrate(conf: &Path) -> (String, Option<String>) {
    // The binary exposes this as `mqttui migrate mosquitto`; the library entry point is
    // what the test drives so a failure points at the converter, not at argument parsing.
    let exe = env!("CARGO_BIN_EXE_mqttui");
    let dir = conf.parent().expect("temp dir");
    let rs_config = dir.join("rs.toml");
    let rs_acl = dir.join("rs-acl.toml");
    let out = Command::new(exe)
        .args(["migrate", "mosquitto"])
        .arg(conf)
        .arg("--out-config")
        .arg(&rs_config)
        .arg("--out-acl")
        .arg(&rs_acl)
        .output()
        .expect("run mqttui migrate");
    assert!(
        out.status.success(),
        "mqttui migrate failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    (
        std::fs::read_to_string(&rs_config).expect("rust config"),
        std::fs::read_to_string(&rs_acl).ok(),
    )
}
