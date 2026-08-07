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
        },
        "status": status,
    }))
    .expect("sample CR");
    let rendered = mqttd_operator::render::render(&cr);
    let all: Vec<_> = rendered.all().into_iter().cloned().collect();
    println!(
        "{}",
        serde_json::to_string_pretty(&all).expect("serializes")
    );
}
