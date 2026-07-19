//! Pure graph-analytics algorithms over the knowledge graph — centrality, PageRank, communities,
//! and shortest path. Decoupled from storage: everything operates on `(src, dst, weight)` triples,
//! so the engine can feed it a **temporal (as-of) snapshot** of edges and the algorithms stay
//! deterministic and unit-testable without a database.

use std::collections::{HashMap, HashSet, VecDeque};

/// Per-node importance in the graph.
#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeCentrality {
    pub node: String,
    pub in_degree: usize,
    pub out_degree: usize,
    /// PageRank score (sums to ~1 across all nodes).
    pub pagerank: f64,
}

/// The distinct node set of an edge list (union of endpoints), in first-seen order.
fn nodes(edges: &[(String, String, f64)]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for (s, d, _) in edges {
        for n in [s, d] {
            if seen.insert(n.clone()) {
                out.push(n.clone());
            }
        }
    }
    out
}

/// Degree centrality + PageRank for every node, sorted by PageRank (desc), then node.
///
/// PageRank uses damping `0.85` and a fixed iteration count (deterministic — no time/random). Edge
/// weights bias the out-link distribution; dangling nodes (no out-links) redistribute their rank
/// uniformly so the vector stays a proper distribution.
pub fn centrality(edges: &[(String, String, f64)]) -> Vec<NodeCentrality> {
    let nodes = nodes(edges);
    let n = nodes.len();
    if n == 0 {
        return Vec::new();
    }
    let idx: HashMap<&str, usize> = nodes
        .iter()
        .enumerate()
        .map(|(i, s)| (s.as_str(), i))
        .collect();

    let mut in_deg = vec![0usize; n];
    let mut out_deg = vec![0usize; n];
    // Weighted out-links per node, and the total out-weight (for normalization).
    let mut out_links: Vec<Vec<(usize, f64)>> = vec![Vec::new(); n];
    let mut out_weight = vec![0.0f64; n];
    for (s, d, w) in edges {
        let (si, di) = (idx[s.as_str()], idx[d.as_str()]);
        let w = if *w <= 0.0 { 1.0 } else { *w };
        out_deg[si] += 1;
        in_deg[di] += 1;
        out_links[si].push((di, w));
        out_weight[si] += w;
    }

    const DAMPING: f64 = 0.85;
    const ITERS: usize = 40;
    let base = (1.0 - DAMPING) / n as f64;
    let mut pr = vec![1.0 / n as f64; n];
    for _ in 0..ITERS {
        let mut next = vec![base; n];
        // Dangling rank (nodes with no out-links) is spread uniformly.
        let mut dangling = 0.0;
        for i in 0..n {
            if out_links[i].is_empty() {
                dangling += pr[i];
            }
        }
        let dangling_share = DAMPING * dangling / n as f64;
        for v in next.iter_mut() {
            *v += dangling_share;
        }
        for i in 0..n {
            if out_weight[i] <= 0.0 {
                continue;
            }
            let share = DAMPING * pr[i] / out_weight[i];
            for (j, w) in &out_links[i] {
                next[*j] += share * w;
            }
        }
        pr = next;
    }

    let mut result: Vec<NodeCentrality> = nodes
        .iter()
        .enumerate()
        .map(|(i, node)| NodeCentrality {
            node: node.clone(),
            in_degree: in_deg[i],
            out_degree: out_deg[i],
            pagerank: pr[i],
        })
        .collect();
    result.sort_by(|a, b| {
        b.pagerank
            .partial_cmp(&a.pagerank)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.node.cmp(&b.node))
    });
    result
}

/// Shortest **directed** path from `src` to `dst` (BFS, unit edge cost), including both endpoints.
/// None if unreachable or an endpoint is absent.
pub fn shortest_path(edges: &[(String, String, f64)], src: &str, dst: &str) -> Option<Vec<String>> {
    if src == dst {
        return Some(vec![src.to_string()]);
    }
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for (s, d, _) in edges {
        adj.entry(s.as_str()).or_default().push(d.as_str());
    }
    let mut prev: HashMap<&str, &str> = HashMap::new();
    let mut q = VecDeque::from([src]);
    let mut seen: HashSet<&str> = HashSet::from([src]);
    while let Some(cur) = q.pop_front() {
        for &nb in adj.get(cur).map(|v| v.as_slice()).unwrap_or(&[]) {
            if !seen.insert(nb) {
                continue;
            }
            prev.insert(nb, cur);
            if nb == dst {
                // Reconstruct.
                let mut path = vec![dst];
                let mut c = dst;
                while let Some(&p) = prev.get(c) {
                    path.push(p);
                    c = p;
                }
                path.reverse();
                return Some(path.into_iter().map(String::from).collect());
            }
            q.push_back(nb);
        }
    }
    None
}

/// Community detection via connected components on the **undirected** projection (union-find).
/// Returns each community's nodes (sorted), communities ordered by size (desc).
pub fn communities(edges: &[(String, String, f64)]) -> Vec<Vec<String>> {
    let nodes = nodes(edges);
    let idx: HashMap<&str, usize> = nodes
        .iter()
        .enumerate()
        .map(|(i, s)| (s.as_str(), i))
        .collect();
    let mut parent: Vec<usize> = (0..nodes.len()).collect();

    fn find(parent: &mut [usize], mut x: usize) -> usize {
        while parent[x] != x {
            parent[x] = parent[parent[x]]; // path halving
            x = parent[x];
        }
        x
    }
    for (s, d, _) in edges {
        let (a, b) = (
            find(&mut parent, idx[s.as_str()]),
            find(&mut parent, idx[d.as_str()]),
        );
        if a != b {
            parent[a] = b;
        }
    }

    let mut groups: HashMap<usize, Vec<String>> = HashMap::new();
    for (i, node) in nodes.iter().enumerate() {
        let root = find(&mut parent, i);
        groups.entry(root).or_default().push(node.clone());
    }
    let mut out: Vec<Vec<String>> = groups
        .into_values()
        .map(|mut g| {
            g.sort();
            g
        })
        .collect();
    out.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a[0].cmp(&b[0])));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(s: &str, d: &str) -> (String, String, f64) {
        (s.to_string(), d.to_string(), 1.0)
    }

    #[test]
    fn centrality_ranks_hub_highest() {
        // a→hub, b→hub, c→hub, hub→z  ⇒ hub has in_degree 3 and the top PageRank.
        let edges = vec![e("a", "hub"), e("b", "hub"), e("c", "hub"), e("hub", "z")];
        let c = centrality(&edges);
        assert_eq!(c[0].node, "z"); // z is the sink of the highest-rank node → accrues most rank
        let hub = c.iter().find(|n| n.node == "hub").unwrap();
        assert_eq!(hub.in_degree, 3);
        assert_eq!(hub.out_degree, 1);
        // PageRank is a distribution.
        let sum: f64 = c.iter().map(|n| n.pagerank).sum();
        assert!((sum - 1.0).abs() < 1e-6, "pagerank sums to 1, got {sum}");
    }

    #[test]
    fn shortest_path_directed() {
        let edges = vec![e("a", "b"), e("b", "c"), e("c", "d"), e("a", "x")];
        assert_eq!(
            shortest_path(&edges, "a", "d"),
            Some(vec!["a".into(), "b".into(), "c".into(), "d".into()])
        );
        assert_eq!(shortest_path(&edges, "a", "a"), Some(vec!["a".into()]));
        assert_eq!(shortest_path(&edges, "d", "a"), None); // directed: no back-edge
        assert_eq!(shortest_path(&edges, "a", "missing"), None);
    }

    #[test]
    fn communities_are_connected_components() {
        // Two clusters: {a,b,c} and {x,y}.
        let edges = vec![e("a", "b"), e("b", "c"), e("x", "y")];
        let comms = communities(&edges);
        assert_eq!(comms.len(), 2);
        assert_eq!(comms[0], vec!["a", "b", "c"]); // larger first
        assert_eq!(comms[1], vec!["x", "y"]);
    }

    #[test]
    fn empty_graph_is_handled() {
        assert!(centrality(&[]).is_empty());
        assert!(communities(&[]).is_empty());
        assert_eq!(shortest_path(&[], "a", "b"), None);
    }
}
