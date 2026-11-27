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

use crate::{FastMap, FilterKey};

/// One topic level in the match index. The two wildcard children are fields of their
/// own, the literal levels a map: the match walk asks every node on its path for `#`
/// and `+`, and asking a map meant hashing those two keys at every level of every
/// publish (ADR 0092 T3). Their wildcard meaning still lives in the walk.
#[derive(Debug, Default)]
struct TrieNode {
    /// The literal levels below this one.
    children: FastMap<String, TrieNode>,
    /// The `+` child: one level, any value.
    plus: Option<Box<TrieNode>>,
    /// The `#` child: this level and everything below it.
    hash: Option<Box<TrieNode>>,
    /// Set when a registered filter ends at this node: **the same allocation**
    /// `by_filter` is keyed by, not a copy of it. This used to be an owned
    /// `String`, so every distinct filter was stored twice.
    terminal: Option<FilterKey>,
}

impl TrieNode {
    fn child(&self, level: &str) -> Option<&TrieNode> {
        match level {
            "+" => self.plus.as_deref(),
            "#" => self.hash.as_deref(),
            _ => self.children.get(level),
        }
    }

    fn child_mut(&mut self, level: &str) -> Option<&mut TrieNode> {
        match level {
            "+" => self.plus.as_deref_mut(),
            "#" => self.hash.as_deref_mut(),
            _ => self.children.get_mut(level),
        }
    }

    fn child_or_insert(&mut self, level: &str) -> &mut TrieNode {
        match level {
            "+" => self.plus.get_or_insert_with(Default::default),
            "#" => self.hash.get_or_insert_with(Default::default),
            _ => self.children.entry(level.to_string()).or_default(),
        }
    }

    fn remove_child(&mut self, level: &str) {
        match level {
            "+" => self.plus = None,
            "#" => self.hash = None,
            _ => {
                self.children.remove(level);
            }
        }
    }

    fn child_count(&self) -> usize {
        self.children.len() + usize::from(self.plus.is_some()) + usize::from(self.hash.is_some())
    }

    fn has_children(&self) -> bool {
        self.child_count() > 0
    }

    /// Move every child out, for the iterative drop.
    fn take_children(&mut self, into: &mut Vec<TrieNode>) {
        into.extend(self.children.drain().map(|(_, n)| n));
        if let Some(p) = self.plus.take() {
            into.push(*p);
        }
        if let Some(h) = self.hash.take() {
            into.push(*h);
        }
    }
}

impl Drop for TrieNode {
    /// Dismantle the subtree iteratively. Node depth tracks the (client-supplied)
    /// filter length, so the derived recursive drop of nested nodes would
    /// otherwise overflow the stack and abort the broker when a deep filter's
    /// path is pruned or the table is torn down — the same hazard the insert,
    /// match, and remove walks avoid by staying iterative.
    fn drop(&mut self) {
        let mut stack: Vec<TrieNode> = Vec::new();
        self.take_children(&mut stack);
        while let Some(mut node) = stack.pop() {
            node.take_children(&mut stack);
            // `node` now has no children; its own drop recurses no further.
        }
    }
}

fn trie_insert(root: &mut TrieNode, filter: &FilterKey) {
    let mut node = root;
    for level in filter.split('/') {
        node = node.child_or_insert(level);
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
        let Some(child) = node.child(level) else {
            return; // filter not registered — nothing to remove
        };
        if depth == 0 || node.terminal.is_some() || node.child_count() > 1 {
            cut = depth;
        }
        node = child;
    }
    // `node` is the leaf (index `levels.len()`); it outlives the removal iff it
    // still has children once we drop this terminal.
    if node.has_children() {
        cut = levels.len();
    }
    // Pass 2 (mutable): clear the terminal if the leaf survives, else detach the
    // whole dead chain at the surviving node — one child removal drops it all.
    let mut node = &mut *root;
    if cut == levels.len() {
        for level in &levels {
            node = node.child_mut(level).expect("path verified in pass 1");
        }
        node.terminal = None;
    } else {
        for level in &levels[..cut] {
            node = node.child_mut(level).expect("path verified in pass 1");
        }
        node.remove_child(levels[cut]);
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

impl Clone for FilterIndex {
    /// A node-for-node copy (ADR 0093 Amendment 4: a hub shard publishes copies of its
    /// tables). Iterative, because a node's depth is a client's filter length and a
    /// derived clone would recurse once per level: each node is assembled once its
    /// children are, bottom-up, with each literal map sized for its children up front.
    /// The copy shares each filter's allocation with this one.
    fn clone(&self) -> Self {
        Self {
            root: copy_trie(&self.root),
        }
    }
}

/// Which child of a node a copy is being made for.
enum Slot<'a> {
    Literal(&'a String),
    Plus,
    Hash,
}

/// One node of [`copy_trie`]'s walk: its source, the children copied so far, and the
/// literal children still to copy.
struct Frame<'a> {
    source: &'a TrieNode,
    literals: std::collections::hash_map::Iter<'a, String, TrieNode>,
    children: crate::FastMap<String, TrieNode>,
    plus: Option<Box<TrieNode>>,
    hash: Option<Box<TrieNode>>,
    /// The next wildcard child to copy once the literals are done: 0 `+`, 1 `#`, 2 none.
    wildcard: u8,
    /// The child being copied now, filled in when its frame is popped.
    slot: Option<Slot<'a>>,
}

impl<'a> Frame<'a> {
    fn new(source: &'a TrieNode) -> Self {
        Self {
            source,
            literals: source.children.iter(),
            children: crate::FastMap::with_capacity_and_hasher(
                source.children.len(),
                source.children.hasher().clone(),
            ),
            plus: None,
            hash: None,
            wildcard: 0,
            slot: None,
        }
    }

    /// The next child to copy and the slot it fills, or `None` when every child is done.
    fn next_child(&mut self) -> Option<(&'a TrieNode, Slot<'a>)> {
        if let Some((level, child)) = self.literals.next() {
            return Some((child, Slot::Literal(level)));
        }
        while self.wildcard < 2 {
            self.wildcard += 1;
            let (child, slot) = if self.wildcard == 1 {
                (self.source.plus.as_deref(), Slot::Plus)
            } else {
                (self.source.hash.as_deref(), Slot::Hash)
            };
            if let Some(child) = child {
                return Some((child, slot));
            }
        }
        None
    }
}

/// Copy the subtree under `root` without recursion.
fn copy_trie(root: &TrieNode) -> TrieNode {
    let mut stack: Vec<Frame<'_>> = vec![Frame::new(root)];
    loop {
        let top = stack.last_mut().expect("the root's frame is popped last");
        if let Some((child, slot)) = top.next_child() {
            top.slot = Some(slot);
            stack.push(Frame::new(child));
            continue;
        }
        let done = stack.pop().expect("a frame is on the stack");
        let node = TrieNode {
            children: done.children,
            plus: done.plus,
            hash: done.hash,
            terminal: done.source.terminal.clone(),
        };
        let Some(parent) = stack.last_mut() else {
            return node;
        };
        match parent.slot.take() {
            Some(Slot::Literal(level)) => {
                parent.children.insert(level.clone(), node);
            }
            Some(Slot::Plus) => parent.plus = Some(Box::new(node)),
            Some(Slot::Hash) => parent.hash = Some(Box::new(node)),
            None => unreachable!("a child frame is pushed only with its slot set"),
        }
    }
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
        !self.root.has_children() && self.root.terminal.is_none()
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
        if self.is_empty() {
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
                if let Some(f) = node.hash.as_ref().and_then(|h| h.terminal.as_ref()) {
                    cb(f);
                }
            }
            if i == levels.len() {
                if let Some(f) = &node.terminal {
                    cb(f);
                }
                continue;
            }
            if !skip_wildcards {
                if let Some(p) = node.plus.as_deref() {
                    stack.push((p, i + 1, false));
                }
            }
            // `children` holds literal levels only — the wildcards live in their own
            // fields — so a level spelled "+" or "#" simply finds nothing here.
            if let Some(c) = node.children.get(levels[i]) {
                stack.push((c, i + 1, false));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FilterIndex, TrieNode};
    use crate::FilterKey;

    fn key(s: &str) -> FilterKey {
        FilterKey::from(s)
    }

    fn matches(index: &FilterIndex, topic: &str) -> Vec<String> {
        let mut out = Vec::new();
        index.for_each_matching(topic, |f| out.push(String::from(&**f)));
        out.sort();
        out
    }

    /// The wildcard children are fields: removing `a/+` while `a/#` and `a/b` remain
    /// cuts only the `+` field, and the other two keep matching.
    #[test]
    fn removing_a_wildcard_filter_leaves_its_siblings() {
        let mut index = FilterIndex::new();
        for f in ["a/+", "a/#", "a/b"] {
            index.insert(&key(f));
        }
        assert_eq!(matches(&index, "a/b"), ["a/#", "a/+", "a/b"]);
        index.remove("a/+");
        assert_eq!(matches(&index, "a/b"), ["a/#", "a/b"]);
        assert_eq!(matches(&index, "a/c"), ["a/#"]);
        index.remove("a/#");
        assert_eq!(matches(&index, "a/c"), Vec::<String>::new());
        assert!(!index.is_empty());
        index.remove("a/b");
        assert!(
            index.is_empty(),
            "every filter removed leaves an empty index"
        );
    }

    /// A node whose only child is a wildcard still counts as having children, so it is
    /// not pruned under a filter that goes through it.
    #[test]
    fn a_wildcard_only_node_survives_its_siblings_removal() {
        let mut index = FilterIndex::new();
        index.insert(&key("a/+/c"));
        index.insert(&key("a"));
        index.remove("a");
        assert_eq!(matches(&index, "a/x/c"), ["a/+/c"]);
        assert_eq!(matches(&index, "a"), Vec::<String>::new());
    }

    /// A deep chain of `+` levels is dropped iteratively through the fields, like a
    /// deep chain of literals through the map.
    #[test]
    fn a_deep_wildcard_chain_drops_without_recursion() {
        let mut index = FilterIndex::new();
        let deep = vec!["+"; 50_000].join("/");
        index.insert(&key(&deep));
        assert_eq!(matches(&index, &vec!["x"; 50_000].join("/")).len(), 1);
        drop(index);
    }

    /// A copy matches every topic as the original does, and the two change
    /// independently afterwards.
    #[test]
    fn a_copy_matches_as_the_original_and_changes_on_its_own() {
        let mut index = FilterIndex::new();
        for f in ["a/b", "a/+", "a/#", "+/b/#", "#", "$SYS/x", "a/b/c/d"] {
            index.insert(&key(f));
        }
        let copy = index.clone();
        for topic in ["a/b", "a/c", "a", "x/b/y", "$SYS/x", "a/b/c/d", "z"] {
            assert_eq!(matches(&copy, topic), matches(&index, topic), "{topic}");
        }
        index.remove("a/+");
        assert_eq!(matches(&copy, "a/c"), ["#", "a/#", "a/+"]);
        assert_eq!(matches(&index, "a/c"), ["#", "a/#"]);
    }

    /// A filter as deep as a client may send is copied without recursion.
    #[test]
    fn a_deep_filter_is_copied_without_recursion() {
        let mut index = FilterIndex::new();
        let deep = vec!["l"; 100_000].join("/");
        index.insert(&key(&deep));
        index.insert(&key(&vec!["+"; 50_000].join("/")));
        let copy = index.clone();
        assert_eq!(matches(&copy, &deep).len(), 1);
        drop(index);
        drop(copy);
    }

    /// One node's size is paid per registered topic level; the keyed hasher's state and
    /// the two wildcard fields are in it (ADR 0092 T3). Measured 2026-11-01 under the
    /// test profile; a change that grows it past the pin shows up here.
    #[test]
    fn a_trie_node_stays_small() {
        let actual = std::mem::size_of::<TrieNode>();
        println!("TrieNode = {actual} B");
        assert!(actual <= 128, "TrieNode grew to {actual} B (pinned at 128)");
    }
}
