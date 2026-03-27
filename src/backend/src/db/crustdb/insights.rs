//! Security insights: domain admin analysis, reachability.

use std::collections::HashMap;
use tracing::debug;

use super::super::types::{
    DbEdge, DbNode, ReachabilityInsight, Result, SecurityInsights, DOMAIN_ADMIN_SID_SUFFIX,
    WELL_KNOWN_PRINCIPALS,
};
use super::algorithms::reverse_bfs;
use super::CrustDatabase;

impl CrustDatabase {
    /// Get security insights.
    ///
    /// Uses reverse BFS from DA groups instead of per-user forward BFS.
    /// This reduces complexity from O(Users * V) to O(V) for both real and
    /// effective DA computation.
    pub fn get_security_insights(&self) -> Result<SecurityInsights> {
        debug!("Computing security insights");

        let nodes = self.get_all_nodes()?;
        let relationships = self.get_all_edges()?;

        let total_users = nodes.iter().filter(|n| n.label == "User").count();

        // Find DA groups (SID ends with -512)
        let da_groups: Vec<&str> = nodes
            .iter()
            .filter(|n| n.id.ends_with(DOMAIN_ADMIN_SID_SUFFIX))
            .map(|n| n.id.as_str())
            .collect();

        // Build reverse adjacency lists (target -> sources).
        // Reverse BFS from DA groups walks edges backwards to find all
        // nodes that can reach DA, in a single O(V+E) traversal.
        let mut reverse_memberof: HashMap<&str, Vec<&str>> = HashMap::new();
        let mut reverse_full: HashMap<&str, Vec<&str>> = HashMap::new();
        for rel in &relationships {
            if rel.rel_type == "MemberOf" {
                reverse_memberof
                    .entry(rel.target.as_str())
                    .or_default()
                    .push(rel.source.as_str());
            }
            reverse_full
                .entry(rel.target.as_str())
                .or_default()
                .push(rel.source.as_str());
        }

        // Build a name lookup for users
        let user_names: HashMap<&str, &str> = nodes
            .iter()
            .filter(|n| n.label == "User")
            .map(|n| (n.id.as_str(), n.name.as_str()))
            .collect();

        // Reverse BFS from DA groups through MemberOf-only edges -> real DAs
        let memberof_distances = reverse_bfs(&da_groups, &reverse_memberof);
        let mut real_das: Vec<(String, String)> = memberof_distances
            .iter()
            .filter(|(&id, _)| user_names.contains_key(id))
            .map(|(&id, _)| {
                let name = user_names[id];
                (id.to_string(), name.to_string())
            })
            .collect();
        real_das.sort_unstable_by(|a, b| a.1.cmp(&b.1));

        // Reverse BFS from DA groups through all edges -> effective DAs
        let full_distances = reverse_bfs(&da_groups, &reverse_full);
        let mut effective_das: Vec<(String, String, usize)> = full_distances
            .iter()
            .filter(|(&id, _)| user_names.contains_key(id))
            .map(|(&id, &hops)| {
                let name = user_names[id];
                (id.to_string(), name.to_string(), hops)
            })
            .collect();
        effective_das.sort_unstable_by_key(|r| r.2);

        // Reachability from well-known principals: count direct non-MemberOf
        // neighbors, matching Neo4j/FalkorDB behavior.
        let reachability = Self::compute_reachability(&nodes, &relationships);

        Ok(SecurityInsights::from_counts(
            total_users,
            real_das,
            effective_das,
            reachability,
        ))
    }

    /// Compute reachability from well-known principals.
    ///
    /// Counts distinct non-MemberOf neighbors of each principal, matching
    /// the Neo4j/FalkorDB implementation.
    fn compute_reachability(
        nodes: &[DbNode],
        relationships: &[DbEdge],
    ) -> Vec<ReachabilityInsight> {
        // Build forward adjacency for non-MemberOf edges from each node
        let mut non_memberof_targets: HashMap<&str, std::collections::HashSet<&str>> =
            HashMap::new();
        for rel in relationships {
            if rel.rel_type != "MemberOf" {
                non_memberof_targets
                    .entry(rel.source.as_str())
                    .or_default()
                    .insert(rel.target.as_str());
            }
        }

        WELL_KNOWN_PRINCIPALS
            .iter()
            .map(|(name, sid_pattern)| {
                // Find the principal node matching this SID pattern
                let principal = nodes.iter().find(|n| n.id.ends_with(sid_pattern));

                let (principal_id, reachable_count) = match principal {
                    Some(p) => {
                        let count = non_memberof_targets
                            .get(p.id.as_str())
                            .map(|targets| targets.len())
                            .unwrap_or(0);
                        (Some(p.id.clone()), count)
                    }
                    None => (None, 0),
                };

                ReachabilityInsight {
                    principal_name: name.to_string(),
                    principal_id,
                    reachable_count,
                }
            })
            .collect()
    }
}
