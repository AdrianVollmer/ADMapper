//! Graph algorithms: shortest path, domain admin paths, BFS.

use std::collections::HashMap;
use tracing::debug;

use super::super::types::{DbError, Result};
use super::CrustDatabase;

impl CrustDatabase {
    /// Find shortest path between two nodes using incremental BFS.
    ///
    /// Uses on-demand neighbor lookups instead of preloading the entire graph,
    /// which dramatically reduces memory usage and improves time-to-first-result
    /// for large graphs. Complexity is O(visited * avg_degree) instead of O(E).
    #[allow(clippy::type_complexity)]
    pub fn shortest_path(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Option<Vec<(String, Option<String>)>>> {
        if from == to {
            return Ok(Some(vec![(from.to_string(), None)]));
        }

        let mut visited = std::collections::HashSet::new();
        let mut parent: std::collections::HashMap<String, (String, String)> =
            std::collections::HashMap::new();
        let mut queue = std::collections::VecDeque::new();

        queue.push_back(from.to_string());
        visited.insert(from.to_string());

        while let Some(current) = queue.pop_front() {
            if current == to {
                let mut path = vec![(to.to_string(), None)];
                let mut node = to.to_string();
                while let Some((prev, rel_type)) = parent.get(&node) {
                    path.push((prev.clone(), Some(rel_type.clone())));
                    node = prev.clone();
                }
                path.reverse();
                return Ok(Some(path));
            }

            // Query neighbors on-demand instead of preloading entire graph
            let edges = self
                .db
                .find_outgoing_relationships_by_objectid(&current)
                .map_err(|e| DbError::Database(e.to_string()))?;

            for (neighbor, rel_type) in edges {
                if !visited.contains(&neighbor) {
                    visited.insert(neighbor.clone());
                    parent.insert(neighbor.clone(), (current.clone(), rel_type));
                    queue.push_back(neighbor);
                }
            }
        }

        Ok(None)
    }

    /// Find paths to Domain Admins.
    ///
    /// Uses reverse BFS from DA groups for O(V+E) instead of per-user BFS.
    pub fn find_paths_to_domain_admins(
        &self,
        exclude_edge_types: &[String],
    ) -> Result<Vec<(String, String, String, usize)>> {
        debug!(exclude = ?exclude_edge_types, "Finding paths to Domain Admins");

        let nodes = self.get_all_nodes()?;
        let relationships = self.get_all_edges()?;

        // Find DA groups (SID ends with -512)
        let da_groups: Vec<&str> = nodes
            .iter()
            .filter(|n| n.id.ends_with("-512"))
            .map(|n| n.id.as_str())
            .collect();

        if da_groups.is_empty() {
            return Ok(Vec::new());
        }

        // Build reverse adjacency list, filtering excluded relationship types
        let exclude_set: std::collections::HashSet<&str> =
            exclude_edge_types.iter().map(|s| s.as_str()).collect();

        let mut reverse_adj: HashMap<&str, Vec<&str>> = HashMap::new();
        for rel in &relationships {
            if !exclude_set.contains(rel.rel_type.as_str()) {
                reverse_adj
                    .entry(rel.target.as_str())
                    .or_default()
                    .push(rel.source.as_str());
            }
        }

        // Single reverse BFS from DA groups
        let distances = reverse_bfs(&da_groups, &reverse_adj);

        // Collect users that can reach DA
        let mut results: Vec<(String, String, String, usize)> = nodes
            .iter()
            .filter(|n| n.label == "User")
            .filter_map(|n| {
                distances
                    .get(n.id.as_str())
                    .map(|&hops| (n.id.clone(), n.label.clone(), n.name.clone(), hops))
            })
            .collect();

        results.sort_by_key(|r| r.3);
        debug!(result_count = results.len(), "Found users with paths to DA");
        Ok(results)
    }

    /// Find membership in a group with matching SID suffix using graph traversal.
    pub fn find_membership_by_sid_suffix(
        &self,
        node_id: &str,
        sid_suffix: &str,
    ) -> Result<Option<String>> {
        let id_escaped = node_id.replace('\'', "''");
        let suffix_escaped = sid_suffix.replace('\'', "''");

        // Use variable-length path to find transitive MemberOf membership
        let query = format!(
            "MATCH p = shortestPath((n {{objectid: '{}'}})-[:MemberOf*1..20]->(g)) \
             WHERE g.objectid ENDS WITH '{}' \
             RETURN g.objectid",
            id_escaped, suffix_escaped
        );

        let result = self.execute(&query)?;

        if let Some(crustdb::ResultValue::Property(crustdb::PropertyValue::String(s))) = result
            .rows
            .first()
            .and_then(|row| row.values.get("g.objectid"))
        {
            return Ok(Some(s.clone()));
        }

        Ok(None)
    }
}

/// BFS backwards from seed nodes through a reverse adjacency list.
///
/// Returns a map from every reachable node to its hop distance from the
/// nearest seed. Runs in O(V + E) -- a single traversal regardless of
/// how many seeds or how many nodes exist.
pub(crate) fn reverse_bfs<'a>(
    seeds: &[&'a str],
    reverse_adj: &HashMap<&'a str, Vec<&'a str>>,
) -> HashMap<&'a str, usize> {
    let mut distances: HashMap<&str, usize> = HashMap::new();
    let mut queue = std::collections::VecDeque::new();

    for &seed in seeds {
        if distances.contains_key(seed) {
            continue;
        }
        distances.insert(seed, 0);
        queue.push_back(seed);
    }

    while let Some(current) = queue.pop_front() {
        let depth = distances[current];
        if let Some(predecessors) = reverse_adj.get(current) {
            for &pred in predecessors {
                if !distances.contains_key(pred) {
                    distances.insert(pred, depth + 1);
                    queue.push_back(pred);
                }
            }
        }
    }

    distances
}
