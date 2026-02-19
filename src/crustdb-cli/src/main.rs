//! CrustDB CLI - Interactive Cypher shell

use clap::Parser;
use crustdb::{Database, ResultValue};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use std::fs;
use std::process;

#[derive(Parser)]
#[command(name = "crustdb")]
#[command(about = "CrustDB - Interactive Cypher Shell", long_about = None)]
struct Args {
    /// Path to the SQLite database file
    database: String,

    /// Execute query string (multiple queries separated by semicolon)
    #[arg(short = 'q', long = "query", conflicts_with = "file")]
    query: Option<String>,

    /// Execute queries from file (queries separated by semicolon)
    #[arg(short = 'f', long = "file", conflicts_with = "query")]
    file: Option<String>,
}

fn main() {
    let args = Args::parse();

    let db = match Database::open(&args.database) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Error opening database: {}", e);
            process::exit(1);
        }
    };

    if let Some(query_string) = args.query {
        run_batch(&db, &query_string);
    } else if let Some(file_path) = args.file {
        match fs::read_to_string(&file_path) {
            Ok(content) => run_batch(&db, &content),
            Err(e) => {
                eprintln!("Error reading file '{}': {}", file_path, e);
                process::exit(1);
            }
        }
    } else {
        run_interactive(&db, &args.database);
    }
}

/// Run queries in batch mode, outputting JSON-lines.
fn run_batch(db: &Database, queries: &str) {
    for query in queries.split(';') {
        let query = query.trim();
        if query.is_empty() {
            continue;
        }

        match db.execute(query) {
            Ok(result) => {
                // Output each row as a JSON line
                if result.rows.is_empty() {
                    // For mutations with no results, output stats
                    let output = serde_json::json!({
                        "query": query,
                        "stats": {
                            "nodes_created": result.stats.nodes_created,
                            "nodes_deleted": result.stats.nodes_deleted,
                            "relationships_created": result.stats.relationships_created,
                            "relationships_deleted": result.stats.relationships_deleted,
                            "properties_set": result.stats.properties_set,
                            "labels_added": result.stats.labels_added,
                        }
                    });
                    println!("{}", output);
                } else {
                    for row in &result.rows {
                        let row_json = row_to_json(row, &result.columns);
                        println!("{}", row_json);
                    }
                }
            }
            Err(e) => {
                let output = serde_json::json!({
                    "query": query,
                    "error": e.to_string()
                });
                println!("{}", output);
            }
        }
    }
}

/// Convert a row to JSON.
fn row_to_json(row: &crustdb::Row, columns: &[String]) -> serde_json::Value {
    let mut obj = serde_json::Map::new();

    for col in columns {
        if let Some(val) = row.values.get(col) {
            obj.insert(col.clone(), result_value_to_json(val));
        } else {
            obj.insert(col.clone(), serde_json::Value::Null);
        }
    }

    serde_json::Value::Object(obj)
}

/// Convert a ResultValue to JSON.
fn result_value_to_json(val: &ResultValue) -> serde_json::Value {
    match val {
        ResultValue::Property(prop) => property_to_json(prop),
        ResultValue::Node {
            id,
            labels,
            properties,
        } => {
            serde_json::json!({
                "_type": "node",
                "_id": id,
                "_labels": labels,
                "properties": properties_to_json(properties)
            })
        }
        ResultValue::Edge {
            id,
            source,
            target,
            edge_type,
            properties,
        } => {
            serde_json::json!({
                "_type": "edge",
                "_id": id,
                "_source": source,
                "_target": target,
                "_edge_type": edge_type,
                "properties": properties_to_json(properties)
            })
        }
        ResultValue::Path { nodes, edges } => {
            serde_json::json!({
                "_type": "path",
                "nodes": nodes,
                "edges": edges
            })
        }
    }
}

/// Convert PropertyValue to JSON.
fn property_to_json(prop: &crustdb::PropertyValue) -> serde_json::Value {
    match prop {
        crustdb::PropertyValue::Null => serde_json::Value::Null,
        crustdb::PropertyValue::Bool(b) => serde_json::Value::Bool(*b),
        crustdb::PropertyValue::Integer(n) => serde_json::Value::Number((*n).into()),
        crustdb::PropertyValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        crustdb::PropertyValue::String(s) => serde_json::Value::String(s.clone()),
        crustdb::PropertyValue::List(items) => {
            serde_json::Value::Array(items.iter().map(property_to_json).collect())
        }
        crustdb::PropertyValue::Map(map) => properties_to_json(map),
    }
}

/// Convert properties HashMap to JSON object.
fn properties_to_json(
    props: &std::collections::HashMap<String, crustdb::PropertyValue>,
) -> serde_json::Value {
    let obj: serde_json::Map<String, serde_json::Value> = props
        .iter()
        .map(|(k, v)| (k.clone(), property_to_json(v)))
        .collect();
    serde_json::Value::Object(obj)
}

/// Run interactive REPL mode.
fn run_interactive(db: &Database, db_path: &str) {
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
