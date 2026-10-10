//! Admin API queries and actions answered on the hub loop (ADR 0081 T4, T7).
//!
//! Each query takes one snapshot of hub state and replies with plain, serializable
//! values, so the admin listener never holds a reference into the hub. Queries are
//! read-only. The two actions — kick and purge — go through the same detach and discard
//! paths a disconnect and a session expiry use, so they cannot leave a session half-gone.
//!
//! Cost: listing and ranking visit every session this node holds, on the hub loop, so
//! a page costs O(sessions) — once per operator request, never per message. Every
//! answer is bounded by its `limit` regardless of how many sessions match.

use super::{AuthMethod, Hub, OutState, SubEntry};
use mqtt_codec::{Disconnect, Packet, ProtocolVersion};
use mqtt_core::ClientId;
use serde::Serialize;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use tokio::sync::oneshot;
use tracing::warn;

/// Most rows one answer carries.
pub const MAX_LIMIT: usize = 1000;

/// An admin query or action, with the channel its answer goes to.
#[derive(Debug)]
pub enum AdminRequest {
    /// One page of sessions, ordered by client id.
    Sessions {
        /// Which sessions.
        filter: SessionFilter,
        /// Start after this client id (the previous page's `next_cursor`).
        after: Option<String>,
        /// Page size, capped at [`MAX_LIMIT`].
        limit: usize,
        /// The page.
        reply: oneshot::Sender<SessionPage>,
    },
    /// One session in detail; `None` when this node holds no such session.
    Session {
        /// The client id.
        client: String,
        /// The detail.
        reply: oneshot::Sender<Option<SessionDetail>>,
    },
    /// The sessions whose subscriptions match a topic name.
    Subscribers {
        /// A topic name (no wildcards).
        topic: String,
        /// Most rows, capped at [`MAX_LIMIT`].
        limit: usize,
        /// The matches.
        reply: oneshot::Sender<SubscriberList>,
    },
    /// The sessions with the most messages waiting (in flight plus backlog).
    Backlog {
        /// How many, capped at [`MAX_LIMIT`].
        top: usize,
        /// Largest first.
        reply: oneshot::Sender<Vec<SessionSummary>>,
    },
    /// A handle on the retained store this hub serves from, so the caller can list it
    /// off the loop.
    RetainedStore {
        /// The handle.
        reply: oneshot::Sender<std::sync::Arc<dyn mqtt_storage::RetainedStore>>,
    },
    /// Disconnect a client (MQTT 5: DISCONNECT `0x98` Administrative action). Its session
    /// stays, as for any server-initiated close, and so does its Will semantics.
    Kick {
        /// The client id.
        client: String,
        /// What happened.
        reply: oneshot::Sender<ActionOutcome>,
    },
    /// Disconnect a client if connected, then delete its session: subscriptions,
    /// in-flight state, queued messages and expiry bookkeeping, in memory and in the store.
    Purge {
        /// The client id.
        client: String,
        /// Whether this node is the session's placement owner. Only the owner removes a
        /// stored session it has not materialized (a cold durable session); another node
        /// that holds nothing for the client leaves the store alone, so asking it cannot
        /// delete durable state it does not own.
        owner: bool,
        /// What happened.
        reply: oneshot::Sender<ActionOutcome>,
    },
}

/// What an admin action found and did.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub struct ActionOutcome {
    /// A connection was attached, and was closed.
    pub disconnected: bool,
    /// A session was known to this node (for purge: and was deleted).
    pub session_found: bool,
}

/// Which sessions a [`AdminRequest::Sessions`] page lists. Empty fields match all.
#[derive(Debug, Default, Clone)]
pub struct SessionFilter {
    /// Client ids starting with this.
    pub prefix: Option<String>,
    /// Connected clients authenticated as exactly this principal.
    pub user: Option<String>,
    /// Connected clients whose source address (`ip:port`) starts with this.
    pub source: Option<String>,
}

/// One session, summarized.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SessionSummary {
    /// The client id.
    pub client_id: String,
    /// Whether a connection is attached now.
    pub connected: bool,
    /// The authenticated principal (connected clients).
    pub user: Option<String>,
    /// How it authenticated (connected clients).
    pub auth: Option<&'static str>,
    /// `3.1.1` or `5` (connected clients).
    pub protocol: Option<&'static str>,
    /// The source address this node saw (connected, not relocated).
    pub source: Option<String>,
    /// Seconds since this connection attached.
    pub connected_secs: Option<u64>,
    /// Whether the session survives disconnect.
    pub persistent: bool,
    /// The Session Expiry Interval, when persistent (`4294967295` = never).
    pub expiry_secs: Option<u32>,
    /// When a disconnected session expires (Unix seconds).
    pub expires_at: Option<u64>,
    /// Subscriptions held.
    pub subscriptions: usize,
    /// `QoS` > 0 messages sent and not yet acknowledged.
    pub inflight: usize,
    /// `QoS` > 0 messages waiting for Receive Maximum quota.
    pub backlog: usize,
    /// Bytes in that backlog.
    pub backlog_bytes: usize,
}

/// One page of sessions.
#[derive(Debug, Clone, Serialize)]
pub struct SessionPage {
    /// The sessions, by client id.
    pub sessions: Vec<SessionSummary>,
    /// Pass as `cursor` for the next page; `None` on the last page.
    pub next_cursor: Option<String>,
    /// How many sessions matched the filter in total (all pages).
    pub matched: usize,
}

/// One subscription.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SubscriptionView {
    /// The filter as subscribed, `$share/<group>/` prefix included.
    pub filter: String,
    /// The granted maximum `QoS`.
    pub qos: u8,
    /// The share group, for a shared subscription.
    pub shared_group: Option<String>,
    /// MQTT 5 No Local.
    pub no_local: bool,
    /// MQTT 5 Retain As Published.
    pub retain_as_published: bool,
    /// MQTT 5 Subscription Identifier.
    pub subscription_id: Option<u32>,
}

/// A connection's Will, without its payload.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct WillView {
    /// The Will topic.
    pub topic: String,
    /// Its `QoS`.
    pub qos: u8,
    /// Whether it is published retained.
    pub retain: bool,
    /// Its payload size in bytes (the payload itself is never returned).
    pub payload_bytes: usize,
    /// The Will Delay Interval.
    pub delay_secs: u32,
}

/// One session in detail.
#[derive(Debug, Clone, Serialize)]
pub struct SessionDetail {
    /// The summary fields.
    #[serde(flatten)]
    pub summary: SessionSummary,
    /// The subscriptions, at most [`MAX_LIMIT`] (`subscriptions` has the count).
    pub subscription_list: Vec<SubscriptionView>,
    /// Whether the session holds more than [`MAX_LIMIT`] subscriptions.
    pub subscription_list_truncated: bool,
    /// In-flight messages by acknowledgement state.
    pub inflight_states: BTreeMap<&'static str, usize>,
    /// The client's Receive Maximum.
    pub receive_maximum: Option<u16>,
    /// The attached connection's Will.
    pub will: Option<WillView>,
    /// Seconds until a delayed Will is published, when one is pending.
    pub will_due_in_secs: Option<u64>,
}

/// One matching subscription.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SubscriberView {
    /// The subscriber.
    pub client_id: String,
    /// The filter that matched, as subscribed.
    pub filter: String,
    /// The granted maximum `QoS`.
    pub qos: u8,
    /// The share group, for a shared subscription (one member per group receives).
    pub shared_group: Option<String>,
    /// Whether the subscriber is connected now.
    pub connected: bool,
}

/// The subscriptions matching a topic.
#[derive(Debug, Clone, Serialize)]
pub struct SubscriberList {
    /// The topic asked about.
    pub topic: String,
    /// The matches, by client id then filter.
    pub subscribers: Vec<SubscriberView>,
    /// Whether more matched than `limit`.
    pub truncated: bool,
}

fn qos(q: mqtt_codec::QoS) -> u8 {
    q as u8
}

fn auth_name(m: AuthMethod) -> &'static str {
    match m {
        AuthMethod::Anonymous => "anonymous",
        AuthMethod::Password => "password",
        AuthMethod::Token => "token",
        AuthMethod::Certificate => "certificate",
        AuthMethod::Enhanced => "enhanced",
    }
}

fn protocol_name(p: ProtocolVersion) -> &'static str {
    match p {
        ProtocolVersion::V311 => "3.1.1",
        ProtocolVersion::V5 => "5",
    }
}

/// `$share/<group>/<filter>` → `(Some(group), filter)`; anything else → `(None, filter)`.
fn split_shared(filter: &str) -> (Option<&str>, &str) {
    filter
        .strip_prefix("$share/")
        .and_then(|rest| rest.split_once('/'))
        .map_or((None, filter), |(group, f)| (Some(group), f))
}

fn subscription_view(e: &SubEntry) -> SubscriptionView {
    let (group, _) = split_shared(&e.filter);
    SubscriptionView {
        filter: e.filter.to_string(),
        qos: qos(e.qos),
        shared_group: group.map(String::from),
        no_local: e.flags & SubEntry::NO_LOCAL != 0,
        retain_as_published: e.flags & SubEntry::RETAIN_AS_PUBLISHED != 0,
        subscription_id: e.sub_id.map(std::num::NonZeroU32::get),
    }
}

impl Hub {
    /// Answer one admin request. A dropped reply channel (the caller timed out) is
    /// ignored.
    pub(super) async fn admin(&mut self, request: AdminRequest) {
        match request {
            AdminRequest::Sessions {
                filter,
                after,
                limit,
                reply,
            } => {
                let _ = reply.send(self.admin_sessions(&filter, after.as_deref(), limit));
            }
            AdminRequest::Session { client, reply } => {
                let _ = reply.send(self.admin_session(&ClientId(client.into())));
            }
            AdminRequest::Subscribers {
                topic,
                limit,
                reply,
            } => {
                let _ = reply.send(self.admin_subscribers(topic, limit));
            }
            AdminRequest::Backlog { top, reply } => {
                let _ = reply.send(self.admin_backlog(top));
            }
            AdminRequest::RetainedStore { reply } => {
                let _ = reply.send(self.retained.clone());
            }
            AdminRequest::Kick { client, reply } => {
                let client = ClientId(client.into());
                let session_found = self.admin_known_clients().any(|c| c == &client);
                let disconnected = self.admin_disconnect(&client).await;
                let _ = reply.send(ActionOutcome {
                    disconnected,
                    session_found,
                });
            }
            AdminRequest::Purge {
                client,
                owner,
                reply,
            } => {
                let client = ClientId(client.into());
                let disconnected = self.admin_disconnect(&client).await;
                let session_found = self.admin_known_clients().any(|c| c == &client);
                // The owner's store may hold a session this node has not materialized (a
                // cold durable session), so the owner removes regardless; `discard_session`
                // does memory and store. A non-owner acts only on what it holds.
                if session_found || disconnected || owner {
                    self.discard_session(&client);
                }
                if session_found || disconnected {
                    warn!(client = %client.0, "session purged by an admin action");
                }
                let _ = reply.send(ActionOutcome {
                    disconnected,
                    session_found: session_found || disconnected,
                });
            }
        }
    }

    /// Close `client`'s connection as an administrative action, the way a revocation
    /// eviction does (`evict`): v5 is told why (`0x98`), v3.1.1 just loses the connection,
    /// and the detach is not graceful, so a Will is published as for any server close.
    /// Returns whether a connection was attached.
    async fn admin_disconnect(&mut self, client: &ClientId) -> bool {
        let Some(online) = self.online.get(client) else {
            return false;
        };
        warn!(client = %client.0, "disconnecting a client by admin action");
        online.tx.closing(super::HubClose::Kicked);
        if online.admission.protocol == ProtocolVersion::V5 {
            let _ = online.tx.send(Packet::Disconnect(Disconnect {
                reason: mqtt_codec::reason::ADMINISTRATIVE_ACTION,
                properties: mqtt_codec::Properties::new(),
            }));
        }
        let conn_id = online.conn_id;
        self.detach(client, conn_id, false, None).await;
        true
    }

    /// Every client id this node holds a session for: connected, persistent, or with
    /// materialized subscriptions.
    fn admin_known_clients(&self) -> impl Iterator<Item = &ClientId> {
        let mut seen: HashSet<&ClientId> = HashSet::new();
        self.online
            .keys()
            .chain(self.session_expiry.keys())
            .chain(self.subs.keys())
            .filter(move |c| seen.insert(*c))
    }

    fn admin_summary(&self, client: &ClientId) -> SessionSummary {
        let online = self.online.get(client);
        let inflight = self.inflight.get(client);
        let expiry = self.session_expiry.get(client).copied();
        SessionSummary {
            client_id: client.0.to_string(),
            connected: online.is_some(),
            user: online.map(|o| o.admission.identity.subject.clone()),
            auth: online.map(|o| auth_name(o.admission.method)),
            protocol: online.map(|o| protocol_name(o.admission.protocol)),
            source: online.and_then(|o| o.admission.source.map(|a| a.to_string())),
            connected_secs: online.map(|o| o.attached_at.elapsed().as_secs()),
            persistent: expiry.is_some(),
            expiry_secs: expiry,
            expires_at: self.expiring.get(client).copied(),
            subscriptions: self.subs.get(client).map_or(0, super::ClientSubs::len),
            inflight: inflight.map_or(0, |i| i.pending.len()),
            backlog: inflight.map_or(0, |i| i.backlog.len()),
            backlog_bytes: inflight.map_or(0, |i| i.backlog.bytes()),
        }
    }

    fn admin_matches(&self, client: &ClientId, f: &SessionFilter) -> bool {
        if let Some(p) = &f.prefix {
            if !client.0.starts_with(p.as_str()) {
                return false;
            }
        }
        if f.user.is_none() && f.source.is_none() {
            return true;
        }
        let Some(online) = self.online.get(client) else {
            return false;
        };
        if let Some(u) = &f.user {
            if online.admission.identity.subject != *u {
                return false;
            }
        }
        if let Some(s) = &f.source {
            let Some(addr) = online.admission.source else {
                return false;
            };
            if !addr.to_string().starts_with(s.as_str()) {
                return false;
            }
        }
        true
    }

    fn admin_sessions(&self, f: &SessionFilter, after: Option<&str>, limit: usize) -> SessionPage {
        let limit = limit.clamp(1, MAX_LIMIT);
        // The `limit + 1` smallest matching ids after the cursor, in one pass: a max-heap
        // evicting its largest keeps the page bounded however many sessions match.
        let mut page: BinaryHeap<&ClientId> = BinaryHeap::with_capacity(limit + 2);
        let mut matched = 0usize;
        for client in self.admin_known_clients() {
            if !self.admin_matches(client, f) {
                continue;
            }
            matched += 1;
            if after.is_some_and(|a| &*client.0 <= a) {
                continue;
            }
            page.push(client);
            if page.len() > limit + 1 {
                page.pop();
            }
        }
        let mut ids = page.into_sorted_vec();
        let more = ids.len() > limit;
        ids.truncate(limit);
        SessionPage {
            next_cursor: if more {
                ids.last().map(|c| c.0.to_string())
            } else {
                None
            },
            sessions: ids.into_iter().map(|c| self.admin_summary(c)).collect(),
            matched,
        }
    }

    fn admin_session(&self, client: &ClientId) -> Option<SessionDetail> {
        if !self.admin_known_clients().any(|c| c == client) {
            return None;
        }
        let online = self.online.get(client);
        let inflight = self.inflight.get(client);
        let mut states: BTreeMap<&'static str, usize> = BTreeMap::new();
        if let Some(i) = inflight {
            for p in i.pending.values() {
                let name = match p.state {
                    OutState::AwaitingPubAck => "awaiting_puback",
                    OutState::AwaitingPubRec => "awaiting_pubrec",
                    OutState::AwaitingPubComp => "awaiting_pubcomp",
                    OutState::CompletedQos2 => "completed_qos2_cleanup",
                    OutState::AwaitingIdRecord => "staged",
                };
                *states.entry(name).or_default() += 1;
            }
        }
        let will = online.and_then(|o| o.will.as_ref()).map(|w| WillView {
            topic: w.message.topic.as_str().to_string(),
            qos: qos(w.message.qos),
            retain: w.message.retain,
            payload_bytes: w.message.payload.len(),
            delay_secs: w.delay_secs,
        });
        let will_due_in_secs = self.pending_wills.get(client).map(|(_, due)| {
            due.saturating_duration_since(tokio::time::Instant::now())
                .as_secs()
        });
        Some(SessionDetail {
            summary: self.admin_summary(client),
            subscription_list: self
                .subs
                .get(client)
                .map(|s| s.iter().take(MAX_LIMIT).map(subscription_view).collect())
                .unwrap_or_default(),
            subscription_list_truncated: self.subs.get(client).is_some_and(|s| s.len() > MAX_LIMIT),
            inflight_states: states,
            receive_maximum: inflight.map(|i| i.receive_maximum),
            will,
            will_due_in_secs,
        })
    }

    fn admin_subscribers(&self, topic: String, limit: usize) -> SubscriberList {
        let limit = limit.clamp(1, MAX_LIMIT);
        // Candidates from the routing indexes (ordinary and shared), then the exact
        // entries from each client's own subscription list.
        let mut candidates: HashSet<&ClientId> = HashSet::new();
        self.table.for_each_matching_client(&topic, |c| {
            candidates.insert(c);
        });
        for group in self.shared.matching(&topic) {
            for (member, _) in &group.members {
                if let Some((c, _)) = self.subs.get_key_value(member) {
                    candidates.insert(c);
                }
            }
        }
        // The `limit + 1` smallest (client, filter) matches, in one pass over the
        // candidates: a max-heap of borrowed keys keeps the work on the loop bounded by
        // the page, not by how many subscribers a hot topic has.
        let mut page: BinaryHeap<(&str, &str, u8, &ClientId)> =
            BinaryHeap::with_capacity(limit + 2);
        for client in candidates {
            let Some(subs) = self.subs.get(client) else {
                continue;
            };
            for e in subs.iter() {
                let (_, filter) = split_shared(&e.filter);
                if mqtt_core::topic_matches(filter, &topic) {
                    page.push((&client.0, &e.filter, qos(e.qos), client));
                    if page.len() > limit + 1 {
                        page.pop();
                    }
                }
            }
        }
        let mut rows = page.into_sorted_vec();
        let truncated = rows.len() > limit;
        rows.truncate(limit);
        SubscriberList {
            topic,
            subscribers: rows
                .into_iter()
                .map(|(id, filter, granted, client)| SubscriberView {
                    client_id: id.to_string(),
                    filter: filter.to_string(),
                    qos: granted,
                    shared_group: split_shared(filter).0.map(String::from),
                    connected: self.online.contains_key(client),
                })
                .collect(),
            truncated,
        }
    }

    fn admin_backlog(&self, top: usize) -> Vec<SessionSummary> {
        let top = top.clamp(1, MAX_LIMIT);
        // A min-heap of the `top` largest (waiting, client) pairs seen so far.
        let mut heap: BinaryHeap<Reverse<(usize, &ClientId)>> = BinaryHeap::new();
        for (client, i) in &self.inflight {
            let waiting = i.pending.len() + i.backlog.len();
            if waiting == 0 {
                continue;
            }
            heap.push(Reverse((waiting, client)));
            if heap.len() > top {
                heap.pop();
            }
        }
        let mut ranked: Vec<(usize, &ClientId)> = heap.into_iter().map(|Reverse(x)| x).collect();
        ranked.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));
        ranked
            .into_iter()
            .map(|(_, c)| self.admin_summary(c))
            .collect()
    }
}
