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
max_connections 5000
max_queued_messages 1000
max_packet_size 262144
max_inflight_messages 40
allow_anonymous false
acl_file ACLPATH
password_file /etc/mosquitto/passwd
listener 1883 127.0.0.1
listener 8883 0.0.0.0
certfile /etc/certs/server.crt
keyfile /etc/certs/server.key
cafile /etc/certs/ca.crt
crlfile /etc/certs/crl.pem
sys_interval 10
autosave_interval 1800
retain_available false
some_future_option 3
";

const ACL: &str = "\
# a comment, and a blank line follow

user sensor-1
topic write sensors/sensor-1/#
topic read commands/sensor-1/#
topic denied/topic
user admin
topic readwrite #
topic deny secrets/#
pattern read devices/%u/status
pattern write devices/%c/up
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
