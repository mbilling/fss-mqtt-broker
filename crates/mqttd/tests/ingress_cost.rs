//! What one queued client publish really retains (ADR 0082 T1).
//!
//! The ingress credit charges each publish `topic + payload + COMMAND_OVERHEAD` bytes
//! while it waits for the hub. That bound is only as true as the charge, so this
//! measures the real cost. Publishes go down the path a connection uses: framed
//! PUBLISH packets are decoded by the production `FrameReader`, then turned into
//! `HubCommand::Publish` and queued on an unbounded channel, as `conn.rs` does. The
//! test then reads how much the process's RSS grew per queued command, under the
//! allocator the release binary links (mimalloc).
//!
//! This binary holds exactly one test, so nothing else allocates while it measures.
//! Every case keeps its commands queued, and its input, until the end. If a case freed
//! anything, the allocator could hand those pages to the next case, and its growth
//! would read low.

use bytes::Bytes;
use mqtt_codec::packet::Publish;
use mqtt_codec::{Packet, ProtocolVersion, QoS};
use mqttd::hub::HubCommand;
use mqttd::ingress::{IngressCredit, OverloadMode, COMMAND_OVERHEAD};
use std::collections::VecDeque;
use std::fmt::Write as _;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::mpsc::UnboundedReceiver;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Queued bytes per case: large beside RSS's page granularity and allocator slack, small
/// enough that all cases together stay well under a CI runner's memory.
const CASE_BYTES: usize = 32 << 20;

/// This process's RSS: `/proc` on Linux, `ps` elsewhere.
fn rss() -> u64 {
    if let Some(b) = mqttd::memory_watch::resident_bytes() {
        return b;
    }
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .expect("ps");
    let kb: u64 = String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .expect("ps rss");
    kb * 1024
}

/// An in-memory stream that hands out at most `chunk` bytes per read, the way TCP
/// delivers a publisher's bytes in segments.
struct Chunked {
    data: Vec<u8>,
    pos: usize,
    chunk: usize,
}

impl AsyncRead for Chunked {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let n = self
            .chunk
            .min(self.data.len() - self.pos)
            .min(buf.remaining());
        let start = self.pos;
        buf.put_slice(&self.data[start..start + n]);
        self.pos += n;
        Poll::Ready(Ok(()))
    }
}

/// One case: publishes with this topic and payload length, read in `chunk`-byte
/// segments (`0` = one frame per read) and queued: on the channel, or with `lanes` in a
/// `VecDeque`, where the hub holds its backlog once it has sorted it into lanes. Returns
/// the bytes of RSS each queued command added, and the queue and the reader, which the
/// caller keeps alive.
async fn retained_per_command(
    topic_len: usize,
    payload_len: usize,
    chunk: usize,
    lanes: bool,
) -> (usize, Queued, mqtt_net::FrameReader<Chunked>) {
    let n = CASE_BYTES / (topic_len + payload_len + COMMAND_OVERHEAD);
    // A lane's capacity is a power of two, so the worst case per command is one past a
    // doubling, where half the slots are empty.
    let n = if lanes {
        n.next_power_of_two() / 2 + 1
    } else {
        n
    };
    let packet = Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "t".repeat(topic_len),
        pkid: None,
        properties: mqtt_codec::Properties::default(),
        payload: Bytes::from(vec![0u8; payload_len]),
    });
    let mut frame = Vec::new();
    packet.encode(&mut frame, ProtocolVersion::V311).unwrap();
    let payload_at = frame.len() - payload_len;
    let topic_at = payload_at - topic_len;
    // Every byte of topic and payload is pseudo-random: macOS compresses idle pages,
    // and a run of identical bytes would read as almost no RSS.
    let mut seed = 0x9E37_79B9_7F4A_7C15_u64;
    let mut wire = Vec::with_capacity(frame.len() * n);
    for _ in 0..n {
        for (i, b) in frame.iter_mut().enumerate().skip(topic_at) {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let r = seed.to_le_bytes()[0];
            *b = if i < payload_at { b'a' + r % 26 } else { r };
        }
        wire.extend_from_slice(&frame);
    }
    let chunk = if chunk == 0 { frame.len() } else { chunk };
    let mut reader = mqtt_net::FrameReader::new(
        Chunked {
            data: wire,
            pos: 0,
            chunk,
        },
        ProtocolVersion::V311,
    );
    let publisher = mqtt_core::ClientId("publisher".into());
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut lane = VecDeque::new();

    let before = rss();
    for _ in 0..n {
        let Some(Packet::Publish(p)) = reader.next_packet().await.unwrap() else {
            panic!("expected a PUBLISH");
        };
        // As conn.rs: the alias resolution re-owns the topic; the packet is dropped.
        let topic = p.topic.clone();
        let cmd = HubCommand::Publish {
            topic,
            payload: p.payload,
            qos: p.qos,
            retain: p.retain,
            message_expiry: None,
            app: mqtt_core::AppProperties::default(),
            done: None,
            v5: false,
            publisher: Some(publisher.clone()),
            credit: None,
        };
        if lanes {
            lane.push_back(cmd);
        } else {
            tx.send(cmd).unwrap();
        }
    }
    let queued = if lanes {
        Queued::Lane(lane)
    } else {
        Queued::Channel(rx)
    };
    let grown = rss().saturating_sub(before);
    // The reader (and its wire bytes) is kept too: freed, its pages would be reused by
    // the next case.
    (usize::try_from(grown).unwrap() / n, queued, reader)
}

/// Where a case's commands wait: still on the channel, or sorted into a hub lane.
#[allow(dead_code)] // held only to keep the commands alive
enum Queued {
    Channel(UnboundedReceiver<HubCommand>),
    Lane(VecDeque<HubCommand>),
}

#[tokio::test(flavor = "current_thread")]
async fn the_charge_covers_what_a_queued_publish_retains() {
    let credit = IngressCredit::new(1 << 30, 1 << 20, OverloadMode::Pause);
    let slot = std::mem::size_of::<HubCommand>();
    let mut report =
        format!("HubCommand slot = {slot} B, COMMAND_OVERHEAD = {COMMAND_OVERHEAD} B\n");
    let mut alive = Vec::new();
    let mut cases = Vec::new();
    // (topic, payload, read chunk): chunk 0 = one frame per read (a trickling
    // publisher); 65,536 = bulk reads from a publisher with a full socket buffer.
    for (topic, payload, chunk, lanes) in [
        (16, 0, 65_536, false),
        (16, 200, 65_536, false),
        (16, 200, 0, false),
        (64, 200, 65_536, false),
        (16, 1_000, 65_536, false),
        (16, 1_000, 0, false),
        (32, 4_000, 65_536, false),
        (16, 200, 65_536, true),
        (16, 1_000, 65_536, true),
    ] {
        let (held, queued, reader) = retained_per_command(topic, payload, chunk, lanes).await;
        alive.push((queued, reader));
        let charged = credit.cost(topic, payload) as usize;
        cases.push((topic, payload, held, charged));
        writeln!(
            report,
            "topic {topic:>3} payload {payload:>5} chunk {chunk:>6} lanes {lanes:>5}: held {held:>5} B, charged {charged:>5} B, held beyond topic+payload {:>5} B",
            held.saturating_sub(topic + payload)
        )
        .unwrap();
    }
    println!("{report}");
    for (topic, payload, held, charged) in cases {
        // The bound holds: no publish is charged less than it retains, even in a lane
        // at its worst point.
        assert!(
            held <= charged,
            "topic {topic} payload {payload}: retains {held} B but is charged {charged} B\n{report}"
        );
        // And it is not padded into fiction: the pool still holds what it says, within 3x.
        assert!(
            charged <= 3 * held,
            "topic {topic} payload {payload}: charged {charged} B for {held} B retained\n{report}"
        );
    }
}
