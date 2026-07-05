//! The forwardable MQTT 5 application properties in their **stored/wire** form
//! (ADR 0030 / [ADR 0038](../../../docs/adr/0038-prerelease-compatibility-freeze.md) T3).
//!
//! A serde-able mirror of [`mqtt_core::AppProperties`] (`Vec<u8>` correlation data
//! instead of `Bytes`), shared by the durable retained record codec, the persistent
//! retained store codec, and — re-exported as `WireAppProps` — the peer-bus frames.
//! Carrying the properties everywhere a retained value travels is what makes a
//! retained publish replay **exactly as published** (MQTT-3.3.2-17) from any node's
//! cache and across a restart, not just from the landing node's memory.

use serde::{Deserialize, Serialize};

/// The forwardable application properties of one message (see module docs).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppProps {
    /// `0x01` Payload Format Indicator (0 = bytes, 1 = UTF-8).
    pub payload_format: Option<u8>,
    /// `0x03` Content Type (a MIME-ish description of the payload).
    pub content_type: Option<String>,
    /// `0x08` Response Topic (request/response).
    pub response_topic: Option<String>,
    /// `0x09` Correlation Data (opaque request/response correlation token).
    pub correlation_data: Option<Vec<u8>>,
    /// `0x26` User Properties, in wire order (repeatable).
    pub user_properties: Vec<(String, String)>,
}

impl AppProps {
    /// Whether no forwardable application property is present (the common case).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.payload_format.is_none()
            && self.content_type.is_none()
            && self.response_topic.is_none()
            && self.correlation_data.is_none()
            && self.user_properties.is_empty()
    }

    /// The canonical byte encoding, embedded (length-prefixed) in store record
    /// codecs and folded into retained value digests.
    ///
    /// # Panics
    /// Never in practice: this shape (options, strings, byte vectors) is always
    /// bincode-serializable.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).expect("AppProps serialization is infallible")
    }

    /// Fail-closed decode of [`encode`](Self::encode)'s output: `None` on malformed
    /// bytes, never garbage properties.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize(bytes).ok()
    }

    /// A cheap size estimate for frame-chunking budgets: the variable-length parts
    /// summed, without serializing.
    #[must_use]
    pub fn size_hint(&self) -> usize {
        self.content_type.as_deref().map_or(0, str::len)
            + self.response_topic.as_deref().map_or(0, str::len)
            + self.correlation_data.as_deref().map_or(0, <[u8]>::len)
            + self
                .user_properties
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()
    }
}

impl From<&mqtt_core::AppProperties> for AppProps {
    fn from(a: &mqtt_core::AppProperties) -> Self {
        Self {
            payload_format: a.payload_format,
            content_type: a.content_type.clone(),
            response_topic: a.response_topic.clone(),
            correlation_data: a.correlation_data.as_ref().map(|b| b.to_vec()),
            user_properties: a.user_properties.clone(),
        }
    }
}

impl From<AppProps> for mqtt_core::AppProperties {
    fn from(p: AppProps) -> Self {
        Self {
            payload_format: p.payload_format,
            content_type: p.content_type,
            response_topic: p.response_topic,
            correlation_data: p.correlation_data.map(bytes::Bytes::from),
            user_properties: p.user_properties,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AppProps;

    fn sample() -> AppProps {
        AppProps {
            payload_format: Some(1),
            content_type: Some("application/json".into()),
            response_topic: Some("replies/1".into()),
            correlation_data: Some(vec![1, 2, 3]),
            user_properties: vec![("trace".into(), "abc".into())],
        }
    }

    #[test]
    fn encode_decode_roundtrips_and_fails_closed() {
        let p = sample();
        assert_eq!(AppProps::decode(&p.encode()), Some(p));
        assert_eq!(
            AppProps::decode(&AppProps::default().encode()),
            Some(AppProps::default())
        );
        assert!(
            AppProps::decode(&[0xFF, 0x01]).is_none(),
            "malformed bytes are absent, not garbage"
        );
    }

    #[test]
    fn converts_to_and_from_core_properties_losslessly() {
        let core: mqtt_core::AppProperties = sample().into();
        assert_eq!(AppProps::from(&core), sample());
        assert!(AppProps::default().is_empty());
        assert!(!sample().is_empty());
    }
}
