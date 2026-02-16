//! CrustDB CLI - Interactive Cypher shell

use crustdb::{Database, ResultValue};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::env;
use std::process;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: crustdb <database.db>");
        process::exit(1);
    }

    let db_path = &args[1];

    let db = match Database::open(db_path) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Error opening database: {}", e);
            process::exit(1);
        }
    };

    println!("CrustDB - Interactive Cypher Shell");
    println!("Connected to: {}", db_path);
    println!("Type 'exit' or Ctrl-D to quit.\n");

    let mut rl = DefaultEditor::new().expect("Failed to create editor");

    loop {
        let readline = rl.readline("crustdb> ");

        match readline {
            Ok(line) => {
                let trimmed = line.trim();

                if trimmed.is_empty() {
                    continue;
                }

                if trimmed.eq_ignore_ascii_case("exit") || trimmed.eq_ignore_ascii_case("quit") {
                    break;
                }

                let _ = rl.add_history_entry(trimmed);

                match db.execute(trimmed) {
                    Ok(result) => {
                        if result.columns.is_empty() {
                            // Mutation query with no return
                            let stats = &result.stats;
                            let mut msgs = Vec::new();

                            if stats.nodes_created > 0 {
                                msgs.push(format!("Nodes created: {}", stats.nodes_created));
                            }
                            if stats.nodes_deleted > 0 {
                                msgs.push(format!("Nodes deleted: {}", stats.nodes_deleted));
                            }
                            if stats.relationships_created > 0 {
                                msgs.push(format!(
                                    "Relationships created: {}",
                                    stats.relationships_created
                                ));
                            }
                            if stats.relationships_deleted > 0 {
                                msgs.push(format!(
                                    "Relationships deleted: {}",
                                    stats.relationships_deleted
                                ));
                            }
                            if stats.properties_set > 0 {
                                msgs.push(format!("Properties set: {}", stats.properties_set));
                            }
                            if stats.labels_added > 0 {
                                msgs.push(format!("Labels added: {}", stats.labels_added));
                            }

                            if msgs.is_empty() {
                                println!("Query executed successfully.");
                            } else {
                                println!("{}", msgs.join(", "));
                            }
                        } else {
                            // Query with results
                            print_results(&result.columns, &result.rows);
                        }
                        println!();
                    }
                    Err(e) => {
                        eprintln!("Error: {}\n", e);
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("Goodbye!");
                break;
            }
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }
}

fn print_results(columns: &[String], rows: &[crustdb::Row]) {
    if rows.is_empty() {
        println!("(no results)");
        return;
    }

    // Calculate column widths
    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();

    for row in rows {
        for (i, col) in columns.iter().enumerate() {
            if let Some(val) = row.values.get(col) {
                let formatted = format_value(val);
                widths[i] = widths[i].max(formatted.len());
            }
        }
    }

    // Print header
    let header: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{:width$}", c, width = widths[i]))
        .collect();
    println!("{}", header.join(" | "));

    // Print separator
    let sep: Vec<String> = widths.iter().map(|&w| "-".repeat(w)).collect();
    println!("{}", sep.join("-+-"));

    // Print rows
    for row in rows {
        let cells: Vec<String> = columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let val = row
                    .values
                    .get(col)
                    .map(format_value)
                    .unwrap_or_else(|| "NULL".to_string());
                format!("{:width$}", val, width = widths[i])
            })
            .collect();
        println!("{}", cells.join(" | "));
    }

    println!("\n{} row(s)", rows.len());
}

fn format_value(val: &ResultValue) -> String {
    match val {
        ResultValue::Property(prop) => format_property(prop),
        ResultValue::Node {
            id,
            labels,
            properties,
        } => {
            let labels_str = if labels.is_empty() {
                String::new()
            } else {
                format!(":{}", labels.join(":"))
            };
            let props_str = format_properties(properties);
            format!("(#{}{} {})", id, labels_str, props_str)
        }
        ResultValue::Edge {
            id,
            edge_type,
            properties,
            ..
        } => {
            let props_str = format_properties(properties);
            format!("[#{}:{} {}]", id, edge_type, props_str)
        }
        ResultValue::Path { nodes, edges } => {
            format!("<path: {} nodes, {} edges>", nodes.len(), edges.len())
        }
    }
}

fn format_property(prop: &crustdb::PropertyValue) -> String {
    match prop {
        crustdb::PropertyValue::Null => "NULL".to_string(),
        crustdb::PropertyValue::Bool(b) => b.to_string(),
        crustdb::PropertyValue::Integer(n) => n.to_string(),
        crustdb::PropertyValue::Float(f) => f.to_string(),
        crustdb::PropertyValue::String(s) => format!("\"{}\"", s),
        crustdb::PropertyValue::List(items) => {
            let formatted: Vec<String> = items.iter().map(format_property).collect();
            format!("[{}]", formatted.join(", "))
        }
        crustdb::PropertyValue::Map(map) => format_properties(map),
    }
}

fn format_properties(props: &std::collections::HashMap<String, crustdb::PropertyValue>) -> String {
    if props.is_empty() {
        return "{}".to_string();
    }

    let pairs: Vec<String> = props
        .iter()
        .map(|(k, v)| format!("{}: {}", k, format_property(v)))
        .collect();
    format!("{{{}}}", pairs.join(", "))
}
