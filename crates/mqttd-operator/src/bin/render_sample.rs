//! Print the operator's rendered objects for a sample `MqttdCluster`, as a JSON
//! array — the input to `scripts/k8s/render-parity.sh` (ADR 0055 T2).
//!
//! The sample deliberately mirrors the chart's default values so the two renders
//! are comparable; the parity script supplies the matching `helm template` side.

fn main() {
    // `--established` renders the FOUNDER-GUARD-armed form (ADR 0055 T9), matching
    // `helm template --set clusterEstablished=true`. Without it the sample has no
    // status, which is the bootstrap-capable form the chart renders by default.
    let established = std::env::args().any(|a| a == "--established");
    let status = if established {
        serde_json::json!({ "bootstrapped": true })
    } else {
        serde_json::Value::Null
    };
    // `--peer-tls` renders the CLUSTER-BUS-ON form, matching `helm template --set
    // secrets.peerTls.secretName=... --set secrets.gossipKey.secretName=...`. Issue #262:
    // both paths must derive the per-pod MQTTD_PEER_TLS_* / MQTTD_SWIM_KEY_FILE paths from
    // the secret names, and a secret-less parity pass alone would never compare that
    // wiring — which is how the chart came to mount cluster-bus material nothing read.
    let secrets = if std::env::args().any(|a| a == "--peer-tls") {
        serde_json::json!({ "peerTls": "mqttd-peer-tls", "gossipKey": "mqttd-gossip" })
    } else {
        serde_json::Value::Null
    };
    let cr: mqttd_operator::crd::MqttdCluster = serde_json::from_value(serde_json::json!({
        "apiVersion": "mqttd.io/v1alpha1",
        "kind": "MqttdCluster",
        "metadata": { "name": "mqttd", "namespace": "default" },
        "spec": {
            "replicas": 3,
            "image": "ghcr.io/mbilling/fss-mqtt-broker:0.9.0",
            // The chart's default values.config, verbatim.
            "config": include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../deploy/helm/mqttd/parity-config.toml"
            )),
            "secrets": secrets,
        },
        "status": status,
    }))
    .expect("sample CR");
    let rendered = mqttd_operator::render::render(&cr, None);
    let all: Vec<_> = rendered.all().into_iter().cloned().collect();
    println!(
        "{}",
        serde_json::to_string_pretty(&all).expect("serializes")
    );
}
