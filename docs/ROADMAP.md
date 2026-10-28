# Roadmap

**Dated 2026-10-25.** What is planned next, and what is not planned by decision. This
was the README's Roadmap section until 2026-10-25. Task-level status, with the evidence
for each finished task, is the generated [delivery dashboard](delivery/STATUS.md); a line
here is an intention, the dashboard is the record.

Tracked on the [delivery dashboard](delivery/STATUS.md).

- **Benchmarks:** the 3- and 10-node figures are measured on the ADR 0090 rig ([RIG2-RESULTS.md](benchmarks/RIG2-RESULTS.md)); next: other brokers on the same rig, larger hosts, TLS posture, per-release re-run ([#545](https://github.com/mbilling/fss-mqtt-broker/issues/545))
- **Scale-out:** optimise durable QoS 1 from 3 to 10 nodes and beyond ([#537](https://github.com/mbilling/fss-mqtt-broker/issues/537)); every bound from a few measured numbers (ADR 0091); durable ownership beyond the voter set (ADR 0073); workload arms incl. burst (ADR 0077); QoS 0 shared-worker capacity ([#482](https://github.com/mbilling/fss-mqtt-broker/issues/482))
- **Auth:** SCRAM · OCSP · PSK suites · server-initiated re-auth
- **Migration:** NanoMQ converter ([#546](https://github.com/mbilling/fss-mqtt-broker/issues/546)); bridge demo ([#547](https://github.com/mbilling/fss-mqtt-broker/issues/547))
- **Security:** OSS-Fuzz ([#553](https://github.com/mbilling/fss-mqtt-broker/issues/553)); funded third-party audit ([#554](https://github.com/mbilling/fss-mqtt-broker/issues/554))
- **Routing:** bloom subscription digests; MQTT 5 Server-Reference redirect
- **Operator:** CRD promotion from `v1alpha1`
- **Not planned, by decision:** dashboard, writing config over the network (beyond the opt-in rules-file write), rule-engine data sinks (Kafka/HTTP/DB), MQTT-SN/CoAP
