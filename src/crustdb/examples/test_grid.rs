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

    // Add relationships
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

    let relationships = db
        .execute("MATCH (a)-[e:EDGE]->(b) RETURN a.id, b.id")
        .unwrap();
    println!("Edges: {} rows", relationships.rows.len());

    // Test 1: Simple adjacent path (should find path of length 1)
    println!("\n--- Test 1: Adjacent nodes (0 -> 1) ---");
    let result = db.execute(
        "MATCH p = shortestPath((src:Node {id: 0})-[:EDGE*]->(dst:Node {id: 1})) RETURN length(p), src.id, dst.id",
    );
    println!("Result: {:?}", result);

    // Test 2: Path to node 5 (one down)
    println!("\n--- Test 2: One hop down (0 -> 5) ---");
    let result = db.execute(
        "MATCH p = shortestPath((src:Node {id: 0})-[:EDGE*]->(dst:Node {id: 5})) RETURN length(p), src.id, dst.id",
    );
    println!("Result: {:?}", result);

    // Test 3: Path to node 6 (diagonal, 2 hops)
    println!("\n--- Test 3: Diagonal (0 -> 6, 2 hops) ---");
    let result = db.execute(
        "MATCH p = shortestPath((src:Node {id: 0})-[:EDGE*]->(dst:Node {id: 6})) RETURN length(p), src.id, dst.id",
    );
    println!("Result: {:?}", result);

    // Test 4: Full grid traversal (0 -> 24)
    println!("\n--- Test 4: Full grid (0 -> 24, 8 hops) ---");
    let last_id = n * n - 1;
    let query = format!(
        "MATCH p = shortestPath((src:Node {{id: 0}})-[:EDGE*]->(dst:Node {{id: {}}})) RETURN length(p), src.id, dst.id",
        last_id
    );
    let result = db.execute(&query);
    println!("Result: {:?}", result);

    // Test 5: Variable-length path without shortestPath (finds all paths)
    println!("\n--- Test 5: Variable-length paths (all reachable) ---");
    let result = db.execute(
        "MATCH p = (src:Node {id: 0})-[:EDGE*1..3]->(dst:Node) RETURN length(p), src.id, dst.id",
    );
    match result {
        Ok(r) => println!("Found {} paths", r.rows.len()),
        Err(e) => println!("Error: {:?}", e),
    }
}
