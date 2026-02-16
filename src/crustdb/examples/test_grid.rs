use crustdb::Database;

fn main() {
    let db = Database::in_memory().expect("Failed to create database");

    // 5x5 grid test (same as benchmark)
    let n = 5;
    let mut parts = Vec::new();

    // Add nodes
    for i in 0..n * n {
        parts.push(format!("(n{}:Node {{id: {}}})", i, i));
    }

    // Add edges
    for row in 0..n {
        for col in 0..n {
            let id = row * n + col;
            if col + 1 < n {
                let right_id = row * n + (col + 1);
                parts.push(format!("(n{})-[:EDGE]->(n{})", id, right_id));
            }
            if row + 1 < n {
                let down_id = (row + 1) * n + col;
                parts.push(format!("(n{})-[:EDGE]->(n{})", id, down_id));
            }
        }
    }

    let query = format!("CREATE {}", parts.join(", "));
    println!("Query: {}", query);

    match db.execute(&query) {
        Ok(result) => println!("CREATE succeeded: {:?}", result),
        Err(e) => println!("CREATE failed: {:?}", e),
    }

    // Check what's in DB
    let nodes = db.execute("MATCH (n:Node) RETURN n.id").unwrap();
    println!("Nodes: {} rows", nodes.rows.len());

    let edges = db
        .execute("MATCH (a)-[e:EDGE]->(b) RETURN a.id, b.id")
        .unwrap();
    println!("Edges: {} rows", edges.rows.len());

    // Try to find path (0 to n*n-1)
    let last_id = n * n - 1;
    println!("Finding path from 0 to {}", last_id);
    let query = format!("MATCH p = SHORTEST 1 (src:Node)-[:EDGE]-+(dst:Node) WHERE src.id = 0 AND dst.id = {} RETURN length(p)", last_id);
    let result = db.execute(&query);
    println!("Shortest path result: {:?}", result);
}
