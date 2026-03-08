use super::{Binding, ExecutionResult};
use crate::error::Result;
use crate::storage::SqliteStorage;

pub(super) fn execute_node_scan(
    variable: &str,
    label_groups: &[Vec<String>],
    limit: Option<u64>,
    property_filter: Option<(String, serde_json::Value)>,
    storage: &SqliteStorage,
) -> Result<ExecutionResult> {
    // label_groups structure:
    // - Each inner Vec is OR'd (alternatives)
    // - Outer Vec is AND'd (all groups must match)
    // Example: :Person|Company → [["Person", "Company"]]
    // Example: :Person:Actor|Director → [["Person"], ["Actor", "Director"]]

    // If we have a property filter, use indexed lookup (much faster)
    let nodes = if let Some((prop, value)) = property_filter {
        // Flatten labels for property lookup (handles simple single-label case)
        let flat_labels: Vec<String> = label_groups.iter().flatten().cloned().collect();
        storage.find_nodes_by_property(&prop, &value, &flat_labels, limit)?
    } else if label_groups.is_empty() || label_groups.iter().all(|g| g.is_empty()) {
        // No label filter - scan all nodes
        storage.get_all_nodes_limit(limit)?
    } else if label_groups.len() == 1 && label_groups[0].len() == 1 {
        // Simple single label case - use index
        storage.find_nodes_by_label_limit(&label_groups[0][0], limit)?
    } else if label_groups.len() == 1 && label_groups[0].len() > 1 {
        // Single group with OR alternatives (e.g., :Person|Company)
        // Scan for each label and merge, avoiding duplicates
        let mut seen_ids = std::collections::HashSet::new();
        let mut all_nodes = Vec::new();
        for label in &label_groups[0] {
            for node in storage.find_nodes_by_label(label)? {
                if seen_ids.insert(node.id) {
                    all_nodes.push(node);
                }
            }
        }
        if let Some(lim) = limit {
            all_nodes.truncate(lim as usize);
        }
        all_nodes
    } else {
        // Multiple groups - use first label from first group for initial scan, then filter
        let first_group = &label_groups[0];
        let first_label = first_group.first().map(String::as_str).unwrap_or("");

        let mut nodes = if first_label.is_empty() {
            storage.get_all_nodes_limit(None)?
        } else {
            storage.find_nodes_by_label(first_label)?
        };

        // Filter: for each group, node must have at least one matching label
        nodes.retain(|n: &crate::graph::Node| {
            label_groups.iter().all(|group| {
                // Node must have at least one label from this group
                group.is_empty() || group.iter().any(|label| n.has_label(label))
            })
        });

        if let Some(lim) = limit {
            nodes.truncate(lim as usize);
        }
        nodes
    };

    let bindings = nodes
        .into_iter()
        .map(|node| Binding::new().with_node(variable, node))
        .collect();

    Ok(ExecutionResult::Bindings(bindings))
}
