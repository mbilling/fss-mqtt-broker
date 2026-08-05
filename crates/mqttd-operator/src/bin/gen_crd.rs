//! Print the `MqttdCluster` CRD manifest (JSON — `kubectl apply` accepts it,
//! and JSON avoids a YAML-serializer dependency). Regenerate the committed
//! manifest with:
//!
//! ```sh
//! cargo run -p mqttd-operator --bin gen_crd > deploy/crds/mqttd.io_mqttdclusters.json
//! ```
//!
//! The golden test in `crd.rs` fails until the committed file matches.

use kube::CustomResourceExt;

fn main() {
    println!(
        "{}",
        serde_json::to_string_pretty(&mqttd_operator::crd::MqttdCluster::crd())
            .expect("CRD serializes")
    );
}
