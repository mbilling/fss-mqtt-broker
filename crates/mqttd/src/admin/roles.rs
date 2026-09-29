//! Who is calling, and what they may do (ADR 0081 §1).
//!
//! The TLS handshake has already required a certificate from a trusted CA; this maps the
//! verified leaf's subject to a role. The role lists come from the live config on every
//! request, so a reload that edits `admin.viewers` / `admin.operators` applies to the next
//! request without restarting the listener.

use mqtt_config::Admin;

/// A caller's role. Ordered: each role may do everything the ones before it may.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    /// Another cluster node (its certificate chains to the cluster CA). May read this
    /// node's own state so any node can answer for the cluster, and nothing else.
    Peer,
    /// Every read.
    Viewer,
    /// Every read, plus the actions.
    Operator,
}

impl Role {
    /// The role's name as it appears in responses and the audit log.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Peer => "peer",
            Role::Viewer => "viewer",
            Role::Operator => "operator",
        }
    }
}

/// The verified caller of one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    /// The certificate's full subject, as `x509-parser` prints it.
    pub subject: String,
    /// The certificate's Common Name, if it has one.
    pub cn: Option<String>,
    /// The granted role, or `None` when the subject is in no list.
    pub role: Option<Role>,
}

/// Map a subject to a role using the live `admin` config. `operators` wins over `viewers`
/// when a subject is in both. `chains_to_cluster_ca` grants [`Role::Peer`] to a subject in
/// neither list.
#[must_use]
pub fn resolve(
    admin: &Admin,
    subject: &str,
    cn: Option<&str>,
    chains_to_cluster_ca: bool,
) -> Option<Role> {
    if admin.operators.iter().any(|e| matches(e, subject, cn)) {
        Some(Role::Operator)
    } else if admin.viewers.iter().any(|e| matches(e, subject, cn)) {
        Some(Role::Viewer)
    } else if chains_to_cluster_ca {
        Some(Role::Peer)
    } else {
        None
    }
}

/// Whether one list entry names this subject. An entry is either the whole subject, or
/// `CN=<name>` matching the Common Name. Whitespace after the commas separating RDNs is
/// ignored on both sides, since printers differ on it.
fn matches(entry: &str, subject: &str, cn: Option<&str>) -> bool {
    let entry = entry.trim();
    if normalize(entry) == normalize(subject) {
        return true;
    }
    match (entry.strip_prefix("CN="), cn) {
        (Some(name), Some(cn)) if !name.contains(',') => name == cn,
        _ => false,
    }
}

fn normalize(dn: &str) -> String {
    dn.split(',').map(str::trim).collect::<Vec<_>>().join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admin(viewers: &[&str], operators: &[&str]) -> Admin {
        Admin {
            viewers: viewers.iter().map(|s| (*s).to_string()).collect(),
            operators: operators.iter().map(|s| (*s).to_string()).collect(),
            ..Admin::default()
        }
    }

    #[test]
    fn subjects_map_to_roles() {
        let a = admin(&["CN=alice", "CN=bob, O=ops"], &["CN=root,O=ops"]);
        assert_eq!(
            resolve(&a, "CN=alice, O=x", Some("alice"), false),
            Some(Role::Viewer)
        );
        assert_eq!(
            resolve(&a, "CN=bob, O=ops", Some("bob"), false),
            Some(Role::Viewer)
        );
        // Full-subject entries match the whole subject, not just the CN.
        assert_eq!(resolve(&a, "CN=bob, O=other", Some("bob"), false), None);
        assert_eq!(
            resolve(&a, "CN=root, O=ops", Some("root"), false),
            Some(Role::Operator)
        );
        assert_eq!(resolve(&a, "CN=mallory", Some("mallory"), false), None);
    }

    #[test]
    fn operator_wins_and_cluster_certificates_are_peers() {
        let a = admin(&["CN=x"], &["CN=x"]);
        assert_eq!(resolve(&a, "CN=x", Some("x"), false), Some(Role::Operator));
        assert_eq!(
            resolve(&a, "CN=node-2", Some("node-2"), true),
            Some(Role::Peer)
        );
        // A listed subject keeps its listed role even when the cluster CA issued it.
        assert_eq!(resolve(&a, "CN=x", Some("x"), true), Some(Role::Operator));
        assert!(Role::Peer < Role::Viewer && Role::Viewer < Role::Operator);
    }
}
