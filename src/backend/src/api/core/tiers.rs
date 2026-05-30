//! Tier management operations.

use crate::api::types::{BatchSetTierRequest, BatchSetTierResponse};
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
        .map(|r| regex::RegexBuilder::new(r).case_insensitive(true).build())
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
