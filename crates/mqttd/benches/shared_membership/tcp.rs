//! Real MQTT client sockets around the same synthetic peer-interest fixture.
//! Peer channels remain synthetic: this isolates the hot broker, not a TCP mesh.
use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use mqtt_codec::{
    packet::{Connect, Publish, Subscribe, SubscribeFilter},
    Packet, Properties, ProtocolVersion, QoS, SubscriptionOptions,
};
use mqtt_net::{FrameReader, FrameWriter};
use mqttd::HubCommand;
use tokio::net::{tcp::OwnedReadHalf, tcp::OwnedWriteHalf, TcpListener, TcpStream};
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};

use super::rig::{validate_receipts, Rig, Shape, BURST, GROUP, TOPIC, WORKERS};

const DEADLINE: Duration = Duration::from_secs(10);
type Receipts = Vec<(u64, u64)>;
type ReceiveRequest = (u64, oneshot::Sender<Receipts>);
type PublishRequest = (u64, oneshot::Sender<()>);

struct Client {
    reader: FrameReader<OwnedReadHalf>,
    writer: FrameWriter<OwnedWriteHalf>,
}

impl Client {
    async fn connect(addr: SocketAddr, name: &str) -> Self {
        let stream = TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (reader, writer) = stream.into_split();
        let mut client = Self {
            reader: FrameReader::new(reader, ProtocolVersion::V311),
            writer: FrameWriter::new(writer, ProtocolVersion::V311),
        };
        client
            .writer
            .send(&Packet::Connect(Connect {
                protocol: ProtocolVersion::V311,
                clean_session: true,
                keep_alive: 0,
                client_id: name.into(),
                last_will: None,
                username: None,
                password: None,
                properties: Properties::default(),
            }))
            .await
            .unwrap();
        assert!(matches!(client.recv().await, Packet::ConnAck(a) if a.code == 0));
        client
    }

    async fn recv(&mut self) -> Packet {
        self.reader
            .next_packet()
            .await
            .unwrap()
            .expect("TCP connection closed")
    }

    async fn subscribe(&mut self, worker: usize) {
        self.writer
            .send(&Packet::Subscribe(Subscribe {
                pkid: 1,
                properties: Properties::default(),
                filters: [format!("$share/{GROUP}/{TOPIC}"), fence_topic(worker)]
                    .into_iter()
                    .map(|path| SubscribeFilter {
                        path,
                        qos: QoS::AtMostOnce,
                        options: SubscriptionOptions::default(),
                    })
                    .collect(),
            }))
            .await
            .unwrap();
        assert!(matches!(self.recv().await, Packet::SubAck(a) if a.return_codes == vec![0, 0]));
    }

    async fn publish_burst(&mut self, burst: u64) {
        for seq in 0..BURST {
            let mut payload = [0; 200];
            payload[..8].copy_from_slice(&burst.to_be_bytes());
            payload[8..16].copy_from_slice(&u64::try_from(seq).unwrap().to_be_bytes());
            self.writer
                .queue(&Packet::Publish(Publish {
                    topic: TOPIC.into(),
                    payload: Bytes::copy_from_slice(&payload),
                    qos: QoS::AtMostOnce,
                    retain: false,
                    dup: false,
                    pkid: None,
                    properties: Properties::default(),
                }))
                .unwrap();
        }
        // The reply proves this connection has decoded/handed off the preceding
        // publishes, not that subscribers received them. The orchestrator then
        // uses a Hub barrier before enqueuing per-subscriber FIFO fence messages.
        self.writer.queue(&Packet::PingReq).unwrap();
        self.writer.flush_queued().await.unwrap();
        assert!(matches!(self.recv().await, Packet::PingResp));
    }

    async fn receive_burst(&mut self, worker: usize, burst: u64) -> Receipts {
        let fence = fence_topic(worker);
        let mut receipts = Vec::with_capacity(BURST / WORKERS);
        loop {
            let Packet::Publish(p) = self.recv().await else {
                panic!("unexpected subscriber packet")
            };
            assert_eq!(p.qos, QoS::AtMostOnce);
            assert!(!p.dup && !p.retain);
            if p.topic == fence {
                assert_eq!(p.payload.as_ref(), burst.to_be_bytes());
                assert_eq!(
                    receipts.len(),
                    BURST / WORKERS,
                    "missing/extra member receipts"
                );
                return receipts;
            }
            assert_eq!(p.topic, TOPIC);
            assert_eq!(p.payload.len(), 200);
            receipts.push((
                u64::from_be_bytes(p.payload[..8].try_into().unwrap()),
                u64::from_be_bytes(p.payload[8..16].try_into().unwrap()),
            ));
            assert!(receipts.len() <= BURST / WORKERS, "extra data before fence");
        }
    }
}

fn fence_topic(worker: usize) -> String {
    format!("membership-fence/{worker}")
}

pub struct TcpRig {
    base: Rig,
    publisher: mpsc::UnboundedSender<PublishRequest>,
    subscribers: Vec<mpsc::UnboundedSender<ReceiveRequest>>,
    tasks: Vec<JoinHandle<()>>,
    burst_id: u64,
}

impl Drop for TcpRig {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl TcpRig {
    pub async fn new(endpoints: &Runtime, shape: Shape) -> Self {
        tokio::time::timeout(DEADLINE, Self::setup(endpoints, shape))
            .await
            .expect("TCP membership setup deadline")
    }

    async fn setup(endpoints: &Runtime, shape: Shape) -> Self {
        let base = Rig::peer_only(endpoints, shape).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hub = base.sender();
        let listener_task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        stream.set_nodelay(true).unwrap();
                        connections.spawn(mqttd::conn::handle(stream, hub.clone()));
                    }
                    result = connections.join_next(), if !connections.is_empty() => {
                        result.unwrap().expect("connection task panicked");
                    }
                }
            }
            // Aborting the listener drops the JoinSet and aborts every connection.
        });
        let (publisher, mut requests) = mpsc::unbounded_channel::<PublishRequest>();
        let mut rig = Self {
            base,
            publisher,
            subscribers: Vec::new(),
            tasks: vec![listener_task],
            burst_id: 0,
        };
        let (ready, wait) = oneshot::channel();
        rig.tasks.push(endpoints.spawn(async move {
            let mut client = Client::connect(addr, "tcp-publisher").await;
            ready.send(()).unwrap();
            while let Some((burst, reply)) = requests.recv().await {
                client.publish_burst(burst).await;
                reply.send(()).unwrap();
            }
        }));
        wait.await.unwrap();
        for worker in 0..WORKERS {
            let (tx, mut requests) = mpsc::unbounded_channel::<ReceiveRequest>();
            rig.subscribers.push(tx);
            let (ready, wait) = oneshot::channel();
            rig.tasks.push(endpoints.spawn(async move {
                let mut client = Client::connect(addr, &format!("tcp-worker-{worker}")).await;
                client.subscribe(worker).await;
                ready.send(()).unwrap();
                while let Some((burst, reply)) = requests.recv().await {
                    reply
                        .send(client.receive_burst(worker, burst).await)
                        .unwrap();
                }
            }));
            wait.await.unwrap();
        }
        rig.base.barrier().await;
        rig.base.verify_peers().await;
        rig
    }

    pub async fn burst(&mut self) {
        tokio::time::timeout(DEADLINE, self.burst_inner())
            .await
            .expect("TCP burst did not reach subscriber fences; not a throughput result");
    }

    async fn burst_inner(&mut self) {
        self.burst_id = self.burst_id.checked_add(1).unwrap();
        let mut waits = Vec::new();
        for subscriber in &self.subscribers {
            let (reply, wait) = oneshot::channel();
            subscriber.send((self.burst_id, reply)).unwrap();
            waits.push(wait);
        }
        let (reply, wait) = oneshot::channel();
        self.publisher.send((self.burst_id, reply)).unwrap();
        wait.await.expect("TCP publisher panicked");
        self.base.barrier().await;
        for worker in 0..WORKERS {
            self.base
                .sender()
                .send(HubCommand::Publish {
                    topic: fence_topic(worker),
                    payload: Bytes::copy_from_slice(&self.burst_id.to_be_bytes()),
                    qos: QoS::AtMostOnce,
                    retain: false,
                    message_expiry: None,
                    app: mqtt_core::AppProperties::default(),
                    done: None,
                    publisher: None,
                    v5: false,
                })
                .unwrap();
        }
        // A fence travels behind all data for that subscriber in the SAME Hub
        // outbound FIFO and real socket writer. It detects even a final duplicate
        // or missing message without a sleep/quiet-window guess or read-count cap.
        let mut receipts = Vec::with_capacity(BURST);
        for wait in waits {
            receipts.extend(wait.await.expect("TCP subscriber panicked"));
        }
        validate_receipts(self.burst_id, &receipts);
    }

    pub async fn verify_peers(&self) {
        self.base.verify_peers().await;
    }
}
