//! Tier management operations.

use crate::api::types::{
    BatchSetTierRequest, BatchSetTierResponse, ComputeEffectiveTiersResponse,
    TierViolationCategory, TierViolationEdge, TierViolationsResponse,
};
use crate::db::{DatabaseBackend, DbEdge};

/// Reverse BFS from `root_id`, following edges of `rel_type` in reverse
/// (target -> source), returning all reached node IDs (excluding root).
pub(crate) fn expand_transitive(
    edges: &[DbEdge],
    root_id: &str,
    rel_type: &str,
) -> std::collections::HashSet<String> {
    use std::collections::{HashMap, HashSet, VecDeque};

    let mut reverse_adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for edge in edges {
        if edge.rel_type.eq_ignore_ascii_case(rel_type) {
            reverse_adj
                .entry(edge.target.as_str())
                .or_default()
                .push(edge.source.as_str());
        }
    }

    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    visited.insert(root_id.to_string());
    queue.push_back(root_id.to_string());

    while let Some(current) = queue.pop_front() {
        if let Some(predecessors) = reverse_adj.get(current.as_str()) {
            for &pred in predecessors {
                if visited.insert(pred.to_string()) {
                    queue.push_back(pred.to_string());
                }
            }
        }
    }

    visited.remove(root_id);
    visited
}

/// Batch-set the tier property on nodes matching the given filters.
pub fn batch_set_tier(
    db: &dyn DatabaseBackend,
    request: BatchSetTierRequest,
) -> Result<BatchSetTierResponse, String> {
    use std::collections::HashSet;

    if !(0..=3).contains(&request.tier) {
        return Err("Tier must be between 0 and 3".into());
    }

    let all_nodes = db.get_all_nodes().map_err(|e| e.to_string())?;
    let regex = request
        .name_regex
        .as_deref()
        .filter(|r| !r.is_empty())
        .map(regex::Regex::new)
        .transpose()
        .map_err(|e| format!("Invalid regex: {e}"))?;

    let mut matching_ids: HashSet<String> = all_nodes
        .iter()
        .filter(|n| {
            if let Some(ref nt) = request.node_type {
                if !n.label.eq_ignore_ascii_case(nt) {
                    return false;
                }
            }
            if let Some(ref re) = regex {
                if !re.is_match(&n.name) {
                    return false;
                }
            }
            true
        })
        .map(|n| n.id.clone())
        .collect();

    let needs_expansion = request.group_id.is_some() || request.ou_id.is_some();
    let edges = if needs_expansion {
        db.get_all_edges().map_err(|e| e.to_string())?
    } else {
        Vec::new()
    };

    if let Some(ref gid) = request.group_id {
        let members = expand_transitive(&edges, gid, "MemberOf");
        if request.node_type.is_some() || regex.is_some() {
            matching_ids = matching_ids.intersection(&members).cloned().collect();
        } else {
            matching_ids = members;
        }
    }

    if let Some(ref oid) = request.ou_id {
        let contained = expand_transitive(&edges, oid, "Contains");
        if request.node_type.is_some() || regex.is_some() || request.group_id.is_some() {
            matching_ids = matching_ids.intersection(&contained).cloned().collect();
        } else {
            matching_ids = contained;
        }
    }

    if let Some(ref ids) = request.node_ids {
        for id in ids {
            matching_ids.insert(id.clone());
        }
    }

    let final_ids: Vec<String> = matching_ids.into_iter().collect();
    let count = final_ids.len();

    for chunk in final_ids.chunks(500) {
        let ids_list: Vec<String> = chunk
            .iter()
            .map(|id| format!("'{}'", id.replace('\'', "\\'")))
            .collect();
        let query = format!(
            "MATCH (n) WHERE n.objectid IN [{}] SET n.tier = {}",
            ids_list.join(", "),
            request.tier
        );
        db.run_custom_query(&query).map_err(|e| e.to_string())?;
    }

    Ok(BatchSetTierResponse { updated: count })
}

/// Compute tier violations: edges crossing tier boundaries.
pub fn tier_violations(db: &dyn DatabaseBackend) -> Result<TierViolationsResponse, String> {
    use std::collections::HashMap;

    let nodes = db.get_all_nodes().map_err(|e| e.to_string())?;
    let edges = db.get_all_edges().map_err(|e| e.to_string())?;
    let total_nodes = nodes.len();
    let total_edges = edges.len();

    // Violations are only meaningful between nodes with explicit tier assignments.
    // Nodes without a tier property are skipped entirely.
    let tier_map: HashMap<&str, Option<i64>> = nodes
        .iter()
        .map(|n| {
            let tier = n.properties.get("tier").and_then(|v| v.as_i64());
            (n.id.as_str(), tier)
        })
        .collect();

    let max_edges = 500;
    let mut violations = Vec::new();

    for (src_label, tgt_label) in [(1i64, 0i64), (2, 1), (3, 2)] {
        let mut count = 0usize;
        let mut sample_edges = Vec::new();

        for edge in &edges {
            let src_tier = match tier_map.get(edge.source.as_str()).copied().flatten() {
                Some(t) => t,
                None => continue,
            };
            let tgt_tier = match tier_map.get(edge.target.as_str()).copied().flatten() {
                Some(t) => t,
                None => continue,
            };
            if src_tier >= src_label && tgt_tier == tgt_label {
                count += 1;
                if sample_edges.len() < max_edges {
                    sample_edges.push(TierViolationEdge {
                        source_id: edge.source.clone(),
                        target_id: edge.target.clone(),
                        rel_type: edge.rel_type.clone(),
                    });
                }
            }
        }

        violations.push(TierViolationCategory {
            source_zone: src_label,
            target_zone: tgt_label,
            count,
            edges: sample_edges,
        });
    }

    Ok(TierViolationsResponse {
        violations,
        total_nodes,
        total_edges,
    })
}

/// Compute effective tiers for all nodes using multi-source reverse BFS.
pub fn compute_effective_tiers(
    db: &dyn DatabaseBackend,
) -> Result<ComputeEffectiveTiersResponse, String> {
    use std::collections::{HashMap, HashSet, VecDeque};

    let nodes = db.get_all_nodes().map_err(|e| e.to_string())?;
    let edges = db.get_all_edges().map_err(|e| e.to_string())?;

    let mut reverse_adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for edge in &edges {
        reverse_adj
            .entry(edge.target.as_str())
            .or_default()
            .push(edge.source.as_str());
    }

    let tier_map: HashMap<&str, i64> = nodes
        .iter()
        .map(|n| {
            let tier = n
                .properties
                .get("tier")
                .and_then(|v| v.as_i64())
                .unwrap_or(3);
            (n.id.as_str(), tier)
        })
        .collect();

    let mut effective_tier: HashMap<&str, i64> =
        nodes.iter().map(|n| (n.id.as_str(), 3i64)).collect();

    for target_tier in [0i64, 1, 2] {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        for node in &nodes {
            if *tier_map.get(node.id.as_str()).unwrap_or(&3) == target_tier
                && visited.insert(node.id.as_str())
            {
                queue.push_back(node.id.as_str());
            }
        }

        while let Some(current) = queue.pop_front() {
            let eff = effective_tier.entry(current).or_insert(3);
            if target_tier < *eff {
                *eff = target_tier;
            }

            if let Some(predecessors) = reverse_adj.get(current) {
                for &pred in predecessors {
                    if visited.insert(pred) {
                        queue.push_back(pred);
                    }
                }
            }
        }
    }

    let node_tiers: Vec<(String, i64)> = effective_tier
        .iter()
        .map(|(id, tier)| (id.to_string(), *tier))
        .collect();

    for chunk in node_tiers.chunks(500) {
        let mut by_tier: HashMap<i64, Vec<String>> = HashMap::new();
        for (id, tier) in chunk {
            by_tier.entry(*tier).or_default().push(id.clone());
        }

        for (tier_val, ids) in &by_tier {
            let ids_list: Vec<String> = ids
                .iter()
                .map(|id| format!("'{}'", id.replace('\'', "\\'")))
                .collect();
            let query = format!(
                "MATCH (n) WHERE n.objectid IN [{}] SET n.effective_tier = {}",
                ids_list.join(", "),
                tier_val
            );
            db.run_custom_query(&query).map_err(|e| e.to_string())?;
        }
    }

    let computed = nodes.len();
    let violations = nodes
        .iter()
        .filter(|n| {
            let assigned = n.properties.get("tier").and_then(|v| v.as_i64());
            match assigned {
                Some(a) => {
                    let effective = *effective_tier.get(n.id.as_str()).unwrap_or(&3);
                    effective < a
                }
                None => false,
            }
        })
        .count();

    Ok(ComputeEffectiveTiersResponse {
        computed,
        violations,
    })
}
