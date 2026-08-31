//! A derived match index over topic filters: walk the TOPIC's levels once
//! instead of testing every registered filter.
//!
//! Extracted from [`crate::subscriptions`] (issue #445, where it replaced the
//! per-publish linear scan for ordinary subscriptions and was measured at +29%
//! routing throughput per core) so that **every** filter population can use the
//! same walk. It was not reused at the time, which left two populations still
//! scanning linearly per publish:
//!
//! - local `$share` groups ([`crate::shared::SharedSubscriptionTable`]);
//! - the hub's record of every PEER's `$share` groups.
//!
//! The second one is measurable from outside: a node that merely KNOWS about
//! peers loses a third of its publish dispatch at 2 peers and half at 4, with no
//! message going anywhere near them, at ~19 ns per publish per remote group
//! (`crates/mqttd/benches/shared_plan.rs`).
//!
//! Matching semantics are exactly [`crate::topic_matches`]'s and are pinned by
//! the equivalence test against a linear reference that has always guarded this
//! walk — now guarding it for every caller rather than one.
//!
//! **Precondition:** filters are [`crate::valid_filter`]-valid (the SUBSCRIBE
//! path enforces this before any table is touched), so `#` only ever terminates
//! a filter. `$share/...` envelopes may be stored verbatim; they contain no
//! wildcard in their first level and behave as literals here.

use crate::FilterKey;
use std::collections::HashMap;

/// One topic level in the match index. `+` and `#` are stored as ordinary keys;
/// their wildcard meaning lives in the walk, not the structure.
#[derive(Debug, Default)]
struct TrieNode {
    children: HashMap<String, TrieNode>,
    /// Set when a registered filter ends at this node: **the same allocation**
    /// `by_filter` is keyed by, not a copy of it. This used to be an owned
    /// `String`, so every distinct filter was stored twice.
    terminal: Option<FilterKey>,
}

impl Drop for TrieNode {
    /// Dismantle the subtree iteratively. Node depth tracks the (client-supplied)
    /// filter length, so the derived recursive drop of nested `HashMap`s would
    /// otherwise overflow the stack and abort the broker when a deep filter's
    /// path is pruned or the table is torn down — the same hazard the insert,
    /// match, and remove walks avoid by staying iterative.
    fn drop(&mut self) {
        let mut stack: Vec<TrieNode> = self.children.drain().map(|(_, n)| n).collect();
        while let Some(mut node) = stack.pop() {
            stack.extend(node.children.drain().map(|(_, n)| n));
            // `node` now has no children; its own drop recurses no further.
        }
    }
}

fn trie_insert(root: &mut TrieNode, filter: &FilterKey) {
    let mut node = root;
    for level in filter.split('/') {
        node = node.children.entry(level.to_string()).or_default();
    }
    // Share the caller's allocation rather than copying the filter a second time.
    node.terminal = Some(FilterKey::clone(filter));
}

/// Remove `filter`'s terminal and prune the trailing chain of nodes it leaves
/// empty. Two iterative passes — depth tracks the filter's level count, which is
/// client-controlled input, so recursion here would let one deep SUBSCRIBE
/// overflow the stack and abort the broker (the insert and match walks are
/// iterative for the same reason).
fn trie_remove(root: &mut TrieNode, filter: &str) {
    let levels: Vec<&str> = filter.split('/').collect();
    // Pass 1 (immutable): confirm the path exists and find the deepest node on it
    // that OUTLIVES the removal — one holding its own terminal or a second child
    // once this filter's leaf is gone. The root always outlives it.
    let mut node = &*root;
    let mut cut = 0usize; // index, on the path, of the deepest surviving node
    for (depth, level) in levels.iter().enumerate() {
        let Some(child) = node.children.get(*level) else {
            return; // filter not registered — nothing to remove
        };
        if depth == 0 || node.terminal.is_some() || node.children.len() > 1 {
            cut = depth;
        }
        node = child;
    }
    // `node` is the leaf (index `levels.len()`); it outlives the removal iff it
    // still has children once we drop this terminal.
    if !node.children.is_empty() {
        cut = levels.len();
    }
    // Pass 2 (mutable): clear the terminal if the leaf survives, else detach the
    // whole dead chain at the surviving node — one child removal drops it all.
    let mut node = &mut *root;
    if cut == levels.len() {
        for level in &levels {
            node = node
                .children
                .get_mut(*level)
                .expect("path verified in pass 1");
        }
        node.terminal = None;
    } else {
        for level in &levels[..cut] {
            node = node
                .children
                .get_mut(*level)
                .expect("path verified in pass 1");
        }
        node.children.remove(levels[cut]);
    }
}

/// A set of topic filters that can be matched against a topic in one walk of
/// the topic's levels — O(topic depth) rather than O(all filters).
///
/// Holds only the filters themselves; what each maps to is the caller's
/// business. A filter enters when its first owner appears and leaves when its
/// last one goes, so the index and the caller's source-of-truth map cannot
/// disagree about which filters exist.
#[derive(Debug, Default)]
pub struct FilterIndex {
    root: TrieNode,
}

impl FilterIndex {
    /// An empty index.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add `filter`. Shares the caller's allocation rather than copying it.
    /// Idempotent: re-inserting an existing filter replaces its terminal.
    pub fn insert(&mut self, filter: &FilterKey) {
        trie_insert(&mut self.root, filter);
    }

    /// Remove `filter` and prune the chain of nodes it leaves empty. A filter
    /// that was never inserted is ignored.
    pub fn remove(&mut self, filter: &str) {
        trie_remove(&mut self.root, filter);
    }

    /// Whether no filter is registered. Callers on a per-publish path should
    /// check this before doing any work of their own; [`for_each_matching`] is
    /// already guarded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.root.children.is_empty() && self.root.terminal.is_none()
    }

    /// Every registered filter matching `topic`, via one walk of the topic's
    /// levels. The callback may fire in any order and each filter at most once
    /// (a filter is one trie path).
    pub fn for_each_matching<'a>(&'a self, topic: &str, mut cb: impl FnMut(&'a FilterKey)) {
        // An empty index is the common case for a population that exists but is
        // unused — a standalone node's peer index, a broker with no shared
        // subscriptions — and the walk below allocates a level vector before it
        // can discover there is nothing to match. Measured: without this guard,
        // indexing the peer population cost a PEERLESS node 48% of its publish
        // dispatch, trading a win at 4+ peers for a regression at 0.
        if self.root.children.is_empty() && self.root.terminal.is_none() {
            return;
        }
        let levels: Vec<&str> = topic.split('/').collect();
        // [MQTT-4.7.2-1]: a filter whose FIRST level is a wildcard never matches a
        // `$`-rooted topic. Deeper levels are unaffected, so the skip applies to
        // the root frame only.
        let skip_root_wildcards = topic.starts_with('$');
        // Iterative: recursion depth would otherwise track topic depth, which is
        // client-controlled input.
        let mut stack: Vec<(&TrieNode, usize, bool)> = vec![(&self.root, 0, skip_root_wildcards)];
        while let Some((node, i, skip_wildcards)) = stack.pop() {
            if !skip_wildcards {
                // A `#` child matches from its parent's level down — INCLUDING the
                // parent level itself: "a/#" matches "a" (its terminal, checked
                // here, not its subtree).
                if let Some(h) = node.children.get("#") {
                    if let Some(f) = &h.terminal {
                        cb(f);
                    }
                }
            }
            if i == levels.len() {
                if let Some(f) = &node.terminal {
                    cb(f);
                }
                continue;
            }
            if !skip_wildcards {
                if let Some(p) = node.children.get("+") {
                    stack.push((p, i + 1, false));
                }
            }
            let level = levels[i];
            // A topic level spelled "+" or "#" was already answered by the wildcard
            // arms above (those keys ARE the wildcard children); looking it up as a
            // literal would visit the same node twice.
            if level != "+" && level != "#" {
                if let Some(c) = node.children.get(level) {
                    stack.push((c, i + 1, false));
                }
            }
        }
    }
}
