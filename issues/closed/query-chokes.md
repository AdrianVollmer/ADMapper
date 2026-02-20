The "Domain Trusts" query returns super fast, but the browser chokes. I get
"This page is slowing down Firefox".

Here is the CLI logs:


2026-02-20T20:33:37.388023Z  INFO graph_query: admapper::api::handlers: Starting async query query=MATCH p = (a:Domain)-[:TrustedBy]->(b:Domain) RETURN p
2026-02-20T20:33:37.389761Z DEBUG admapper::db::crustdb: Running custom Cypher query query=MATCH p = (a:Domain)-[:TrustedBy]->(b:Domain) RETURN p
2026-02-20T20:33:37.395823Z DEBUG admapper::api::handlers: Query completed query_id=786cf3c7-cc8f-4280-911a-0bb2e8260a23 duration_ms=7 result_count=Some(11) has_graph=true
2026-02-20T20:33:37.399981Z DEBUG admapper::api::handlers: Sending cached final state to late subscriber query_id=786cf3c7-cc8f-4280-911a-0bb2e8260a23

Why would it overwhelm the browser?
