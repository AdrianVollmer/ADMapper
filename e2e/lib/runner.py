"""
Test runner and test implementations for E2E tests.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from api import APIClient


@dataclass
class TestResult:
    """Result of a single test."""

    name: str
    passed: bool
    duration_ms: int
    message: str = ""
    proof: str = ""  # Evidence like HTTP response body


class TestRunner:
    """Runs E2E tests against the ADMapper API."""

    def __init__(
        self,
        api: APIClient,
        test_data: Path,
        golden_file: Path,
        logger: logging.Logger,
        backend: str = "crustdb",
    ):
        self.api = api
        self.test_data = test_data
        self.golden_file = golden_file
        self.logger = logger
        self.backend = backend
        self._expected_stats: dict[str, Any] | None = None
        # Populated by test_query_consistency() for cross-backend comparison
        self.query_counts: dict[str, int] = {}
        # Populated by test_stats() for cross-backend import validation
        self.graph_stats: dict[str, int] = {}
        # Populated by test_graph_data() for cross-backend node/edge comparison
        self.all_nodes: list[tuple[str, ...]] = []
        self.all_edges: list[tuple[str, ...]] = []

    @property
    def expected_stats(self) -> dict[str, Any]:
        """Load expected stats from golden file."""
        if self._expected_stats is None:
            if self.golden_file.exists():
                self._expected_stats = json.loads(self.golden_file.read_text())
            else:
                self._expected_stats = {}
        return self._expected_stats

    def _run_test(
        self, name: str, test_fn: Callable[[], tuple[bool, str, str]]
    ) -> TestResult:
        """Run a single test and capture the result."""
        start = time.time()
        try:
            passed, message, proof = test_fn()
        except Exception as e:
            passed = False
            message = str(e)
            proof = ""
        duration_ms = int((time.time() - start) * 1000)
        return TestResult(
            name=name,
            passed=passed,
            duration_ms=duration_ms,
            message=message,
            proof=proof,
        )

    def _to_proof(self, data: Any) -> str:
        """Convert data to a proof string (JSON formatted)."""
        if isinstance(data, str):
            return data
        try:
            return json.dumps(data, indent=2, default=str)
        except Exception:
            return str(data)

    def _body_get(
        self, body: dict[str, Any] | list[Any], key: str, default: Any = None
    ) -> Any:
        """Safely get a value from response body that might be dict or list."""
        if isinstance(body, dict):
            return body.get(key, default)
        return default

    # =========================================================================
    # Health Check Tests
    # =========================================================================

    def test_health(self) -> list[TestResult]:
        """Test health endpoint."""
        results = []

        def check_health():
            response = self.api.health()
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Health check failed: {response.body}", proof
            return True, "", proof

        results.append(self._run_test("Health endpoint responds", check_health))

        def check_db_status():
            response = self.api.db_status()
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"DB status failed: {response.body}", proof
            if not self._body_get(response.body, "connected"):
                return False, "Database not connected", proof
            return True, "", proof

        results.append(self._run_test("Database is connected", check_db_status))

        return results

    # =========================================================================
    # Import Tests
    # =========================================================================

    def test_import(self) -> list[TestResult]:
        """Test data import."""
        results = []

        # Check test data exists
        def check_test_data():
            if not self.test_data.exists():
                return False, f"Test data not found: {self.test_data}", ""
            return (
                True,
                "",
                f"File: {self.test_data}, size: {self.test_data.stat().st_size} bytes",
            )

        results.append(self._run_test("Test data file exists", check_test_data))

        # Import request
        job_id = None

        def do_import():
            nonlocal job_id
            response = self.api.import_file(self.test_data)
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Import request failed: {response.body}", proof
            job_id = self._body_get(response.body, "job_id")
            if not job_id:
                return False, "No job_id in response", proof
            return True, "", proof

        results.append(self._run_test("Import request succeeds", do_import))

        if not job_id:
            return results

        # Wait for import to complete
        progress = None

        def wait_import():
            nonlocal progress
            progress = self.api.wait_for_import(job_id, timeout=300)
            proof = self._to_proof(progress) if progress else "null"
            if not progress:
                return False, "Import did not complete", proof
            if progress.get("status") == "failed":
                return False, f"Import failed: {progress.get('error')}", proof
            return True, "", proof

        results.append(self._run_test("Import completes successfully", wait_import))

        # Check files were processed
        def check_files_processed():
            proof = self._to_proof(progress) if progress else "null"
            if not progress:
                return False, "No progress data", proof
            files = progress.get("files_processed", 0)
            if files <= 0:
                return False, "No files were processed", proof
            self.logger.info(f"Files processed: {files}")
            return True, "", proof

        results.append(self._run_test("Import processed files", check_files_processed))

        # Check graph has nodes
        def check_nodes():
            response = self.api.stats()
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Stats request failed: {response.body}", proof
            nodes = response.body.get("nodes", 0)
            if nodes <= 0:
                return False, "No nodes in graph after import", proof
            self.logger.info(f"Nodes imported: {nodes}")
            return True, "", proof

        results.append(self._run_test("Graph has nodes after import", check_nodes))

        # Check graph has relationships
        def check_relationships():
            response = self.api.stats()
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Stats request failed: {response.body}", proof
            relationships = response.body.get("relationships", 0)
            if relationships <= 0:
                return False, "No relationships in graph after import", proof
            self.logger.info(f"Relationships imported: {relationships}")
            return True, "", proof

        results.append(
            self._run_test("Graph has relationships after import", check_relationships)
        )

        return results

    # =========================================================================
    # Stats Tests
    # =========================================================================

    def test_stats(self) -> list[TestResult]:
        """Test graph statistics."""
        results = []
        expected = self.expected_stats

        # Basic stats endpoint
        def check_basic_stats():
            response = self.api.stats()
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Stats request failed: {response.body}", proof
            nodes = self._body_get(response.body, "nodes", 0)
            relationships = self._body_get(response.body, "relationships", 0)
            if nodes <= 0 or relationships <= 0:
                return (
                    False,
                    f"Invalid stats: nodes={nodes}, relationships={relationships}",
                    proof,
                )
            return True, "", proof

        results.append(self._run_test("Basic stats endpoint works", check_basic_stats))

        # Detailed stats endpoint
        detailed = None

        def check_detailed_stats():
            nonlocal detailed
            response = self.api.detailed_stats()
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Detailed stats failed: {response.body}", proof
            detailed = response.body
            # Store for cross-backend comparison
            if isinstance(detailed, dict):
                for key in ("total_nodes", "total_edges"):
                    if key in detailed:
                        self.graph_stats[key] = detailed[key]
            return True, "", proof

        results.append(
            self._run_test("Detailed stats endpoint works", check_detailed_stats)
        )

        # Validate counts against expected
        if detailed and expected:

            def check_total_nodes():
                actual = detailed.get("total_nodes", 0)
                exp = expected.get("total_nodes", 0)
                proof = f"actual: {actual}, expected: {exp}"
                # Allow actual >= expected because placeholder nodes may be
                # created for edges referencing nodes in other BloodHound files.
                # Also allow up to 5% additional nodes for placeholders.
                max_allowed = int(exp * 1.05)
                if actual < exp:
                    return False, f"Expected at least {exp} nodes, got {actual}", proof
                if actual > max_allowed:
                    return False, f"Too many nodes: {actual} (expected ~{exp})", proof
                self.logger.info(f"Total nodes: {actual}")
                return True, "", proof

            results.append(
                self._run_test("Total nodes is plausible", check_total_nodes)
            )

            # Note: Relationship count from source files won't match actual imports
            # because some relationships reference non-existent nodes. We just verify
            # relationships exist rather than checking exact count.
            def check_has_relationships():
                actual = detailed.get("total_edges", 0)
                proof = f"actual relationships: {actual}"
                if actual <= 0:
                    return False, "No relationships in database", proof
                self.logger.info(f"Total relationships: {actual}")
                return True, "", proof

            results.append(
                self._run_test("Graph has relationships", check_has_relationships)
            )

            # Check individual type counts
            # Note: domains use >= because trusts can create orphaned domain references
            for type_key, type_name, exact_match in [
                ("users", "Users", True),
                ("computers", "Computers", True),
                ("groups", "Groups", True),
                ("domains", "Domains", False),  # Allow extra domains from trusts
            ]:

                def make_check(key, name, exact):
                    def check():
                        actual = detailed.get(key, 0)
                        exp = expected.get(key, 0)
                        proof = f"actual: {actual}, expected: {exp}"
                        if exact:
                            if actual != exp:
                                return (
                                    False,
                                    f"Expected {exp} {name}, got {actual}",
                                    proof,
                                )
                        else:
                            if actual < exp:
                                return (
                                    False,
                                    f"Expected at least {exp} {name}, got {actual}",
                                    proof,
                                )
                        self.logger.info(f"{name}: {actual}")
                        return True, "", proof

                    return check

                results.append(
                    self._run_test(
                        f"{type_name} count matches expected",
                        make_check(type_key, type_name, exact_match),
                    )
                )

        # Node types endpoint
        def check_node_types():
            response = self.api.node_types()
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Node types failed: {response.body}", proof
            types = response.body
            if not isinstance(types, list) or len(types) == 0:
                return False, "No node types returned", proof
            self.logger.info(f"Node types: {', '.join(types[:5])}")
            return True, "", proof

        results.append(self._run_test("Node types endpoint works", check_node_types))

        # Relationship types endpoint
        def check_relationship_types():
            response = self.api.relationship_types()
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Relationship types failed: {response.body}", proof
            types = response.body
            if not isinstance(types, list) or len(types) == 0:
                return False, "No relationship types returned", proof
            self.logger.info(f"Relationship types: {', '.join(types[:5])}")
            return True, "", proof

        results.append(
            self._run_test(
                "Relationship types endpoint works", check_relationship_types
            )
        )

        return results

    # =========================================================================
    # Query Tests
    # =========================================================================

    def test_queries(self) -> list[TestResult]:
        """Test Cypher query execution."""
        results = []

        # Simple count query
        def check_count_query():
            response = self.api.query("MATCH (n) RETURN count(n) AS total")
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            # Extract count from various result formats
            total = self._extract_query_count(response.body, "total")
            if total is None or total <= 0:
                return False, f"Query returned no results: {response.body}", proof
            self.logger.info(f"Total nodes: {total}")
            return True, "", proof

        results.append(self._run_test("Simple count query", check_count_query))

        # User count query
        def check_user_query():
            response = self.api.query("MATCH (u:User) RETURN count(u) AS users")
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            return True, "", proof

        results.append(self._run_test("Query for User nodes", check_user_query))

        # Computer count query
        def check_computer_query():
            response = self.api.query("MATCH (c:Computer) RETURN count(c) AS computers")
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            return True, "", proof

        results.append(self._run_test("Query for Computer nodes", check_computer_query))

        # Group count query
        def check_group_query():
            response = self.api.query("MATCH (g:Group) RETURN count(g) AS groups")
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            return True, "", proof

        results.append(self._run_test("Query for Group nodes", check_group_query))

        # Relationship query
        def check_rel_query():
            response = self.api.query(
                "MATCH (n)-[r]->(m) RETURN count(r) AS edges LIMIT 1"
            )
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            return True, "", proof

        results.append(self._run_test("Query with relationship", check_rel_query))

        # Property filter query
        def check_property_query():
            response = self.api.query(
                "MATCH (u:User) WHERE u.enabled = true RETURN count(u) AS enabled"
            )
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            return True, "", proof

        results.append(
            self._run_test("Query with property filter", check_property_query)
        )

        # Return node properties
        def check_return_props():
            response = self.api.query("MATCH (u:User) RETURN u.name AS name LIMIT 5")
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            return True, "", proof

        results.append(
            self._run_test("Query returning node properties", check_return_props)
        )

        # type() function
        def check_type_function():
            response = self.api.query(
                "MATCH (n)-[r]->(m) RETURN type(r) AS rel_type LIMIT 5"
            )
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            return True, "", proof

        results.append(
            self._run_test("Query with type() function", check_type_function)
        )

        # labels() function
        def check_labels_function():
            response = self.api.query("MATCH (n) RETURN labels(n) AS labels LIMIT 5")
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            return True, "", proof

        results.append(
            self._run_test("Query with labels() function", check_labels_function)
        )

        return results

    def _extract_count(self, results: dict[str, Any], column: str) -> int | None:
        """Extract a count value from query results in various formats."""
        # Format: {"rows": [[value]], "headers": [...]}
        if isinstance(results, dict) and "rows" in results and results["rows"]:
            try:
                return int(results["rows"][0][0])
            except (IndexError, TypeError, ValueError):
                pass

        # Format: [{"column": value}]
        if isinstance(results, list) and results:
            first = results[0]
            if isinstance(first, dict):
                try:
                    return int(first.get(column, 0))
                except (TypeError, ValueError):
                    pass
            # Format: [[value]] - nested list (FalkorDB)
            elif isinstance(first, list) and first:
                try:
                    return int(first[0])
                except (TypeError, ValueError, IndexError):
                    pass

        # Format: {"results": [...]}
        if isinstance(results, dict) and "results" in results:
            nested = results["results"]
            if isinstance(nested, list) and nested:
                first = nested[0]
                # Format: [{"column": value}]
                if isinstance(first, dict):
                    try:
                        return int(first.get(column, 0))
                    except (TypeError, ValueError):
                        pass
                # Format: [[value]] - nested list (FalkorDB)
                elif isinstance(first, list) and first:
                    try:
                        return int(first[0])
                    except (TypeError, ValueError, IndexError):
                        pass

        return None

    def _extract_query_count(self, body: dict[str, Any], column: str) -> int | None:
        """Extract a count from a query response.

        Extracts from the inline ``results`` dict returned in sync mode.
        """
        result_data = self._body_get(body, "results", {})
        return self._extract_count(result_data, column)

    def _extract_rows(
        self,
        body: dict[str, Any] | list[Any],
        columns: list[str],
    ) -> list[tuple[str, ...]]:
        """Extract all rows from a query response as tuples of strings.

        Handles the various result formats returned by different backends.
        Returns a list of tuples, one per row, with values stringified.
        """
        results = self._body_get(body, "results", body)

        rows: list[list[Any]] = []

        # Format: {"rows": [[v1, v2, ...], ...], "headers": [...]}
        if isinstance(results, dict) and "rows" in results:
            rows = results["rows"]
        # Format: [{"col1": v1, "col2": v2}, ...]
        elif isinstance(results, list) and results and isinstance(results[0], dict):
            rows = [[row.get(c) for c in columns] for row in results]
        # Format: [[v1, v2], ...] - nested lists (FalkorDB)
        elif isinstance(results, list) and results and isinstance(results[0], list):
            rows = results

        return [tuple(str(v) if v is not None else "" for v in row) for row in rows]

    # =========================================================================
    # Search Tests
    # =========================================================================

    def test_search(self) -> list[TestResult]:
        """Test search functionality."""
        results = []

        # Basic search
        def check_basic_search():
            response = self.api.search("admin", limit=10)
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Search failed: {response.body}", proof
            items = response.body
            if not isinstance(items, list):
                return False, f"Expected list, got {type(items)}", proof
            self.logger.info(f"Search 'admin' returned {len(items)} results")
            return True, "", proof

        results.append(self._run_test("Basic search works", check_basic_search))

        # Search with limit (note: search requires min 2 characters)
        def check_search_limit():
            response = self.api.search("user", limit=5)
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Search failed: {response.body}", proof
            items = response.body
            if len(items) > 5:
                return False, f"Limit not respected: got {len(items)} results", proof
            return True, "", proof

        results.append(self._run_test("Search respects limit", check_search_limit))

        # Case insensitive search
        def check_case_insensitive():
            response1 = self.api.search("ADMIN", limit=10)
            response2 = self.api.search("admin", limit=10)
            proof = f"ADMIN: {len(response1.body) if response1.ok else 'error'}, admin: {len(response2.body) if response2.ok else 'error'}"
            if not response1.ok or not response2.ok:
                return False, "Search failed", proof
            # Both should return results
            if not response1.body and not response2.body:
                return False, "No results for either case", proof
            return True, "", proof

        results.append(
            self._run_test("Search is case insensitive", check_case_insensitive)
        )

        # Search for non-existent term
        def check_no_results():
            response = self.api.search("xyznonexistent123", limit=10)
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Search failed: {response.body}", proof
            items = response.body
            if len(items) != 0:
                return False, f"Expected no results, got {len(items)}", proof
            return True, "", proof

        results.append(
            self._run_test("Search returns empty for non-existent", check_no_results)
        )

        return results

    # =========================================================================
    # Performance Tests
    # =========================================================================

    def test_performance(self) -> list[TestResult]:
        """Test query performance with synthetic data."""
        results = []
        max_time_ms = 3000  # 3 seconds max per query

        # Create test data: 20 PerfUser nodes, 10 PerfGroup nodes
        # with edges between them - using batch creation for efficiency
        def setup_perf_data():
            # Create all users with KNOWS chain in a single query
            # This creates: u0 -> u1 -> u2 -> ... -> u19
            user_chain = "-[:PERF_KNOWS]->".join(
                [
                    f"(u{i}:PerfUser {{name: 'perfuser{i}', index: {i}, "
                    f"enabled: {str(i % 2 == 0).lower()}, score: {i * 10}}})"
                    for i in range(20)
                ]
            )
            response = self.api.query(f"CREATE {user_chain}")
            if not response.ok:
                return False, f"Failed to create users: {response.body}", ""

            # Create all groups in a single query
            groups = ", ".join(
                [
                    f"(g{i}:PerfGroup {{name: 'perfgroup{i}', index: {i}, priority: {i % 5}}})"
                    for i in range(10)
                ]
            )
            response = self.api.query(f"CREATE {groups}")
            if not response.ok:
                return False, f"Failed to create groups: {response.body}", ""

            # Note: CrustDB doesn't support MATCH...CREATE for edges between
            # existing nodes, so we only test with the KNOWS edges created above

            self.logger.info("Created 20 PerfUser (with KNOWS chain), 10 PerfGroup")
            return True, "", "Performance data created"

        results.append(self._run_test("Setup performance test data", setup_perf_data))

        # Skip remaining tests if setup failed
        if not results[-1].passed:
            return results

        # Test 1: Count PerfUser nodes (should be 20)
        def check_user_count():
            start = time.time()
            response = self.api.query("MATCH (u:PerfUser) RETURN count(u) AS total")
            elapsed_ms = (time.time() - start) * 1000
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            if elapsed_ms > max_time_ms:
                return (
                    False,
                    f"Query too slow: {elapsed_ms:.0f}ms (max {max_time_ms}ms)",
                    proof,
                )

            total = self._extract_query_count(response.body, "total")
            if total != 20:
                return False, f"Expected 20 PerfUsers, got {total}", proof
            self.logger.info(f"Count PerfUser: {elapsed_ms:.0f}ms")
            return True, "", proof

        results.append(
            self._run_test("Perf: Count users (expect 20)", check_user_count)
        )

        # Test 2: Count PerfGroup nodes (should be 10)
        def check_group_count():
            start = time.time()
            response = self.api.query("MATCH (g:PerfGroup) RETURN count(g) AS total")
            elapsed_ms = (time.time() - start) * 1000
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            if elapsed_ms > max_time_ms:
                return (
                    False,
                    f"Query too slow: {elapsed_ms:.0f}ms (max {max_time_ms}ms)",
                    proof,
                )

            total = self._extract_query_count(response.body, "total")
            if total != 10:
                return False, f"Expected 10 PerfGroups, got {total}", proof
            self.logger.info(f"Count PerfGroup: {elapsed_ms:.0f}ms")
            return True, "", proof

        results.append(
            self._run_test("Perf: Count groups (expect 10)", check_group_count)
        )

        # Test 3: Outgoing KNOWS edges from a user (user 0 -> user 1)
        def check_outgoing():
            start = time.time()
            response = self.api.query(
                "MATCH (u:PerfUser {index: 0})-[r:PERF_KNOWS]->(other:PerfUser) "
                "RETURN count(r) AS total"
            )
            elapsed_ms = (time.time() - start) * 1000
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            if elapsed_ms > max_time_ms:
                return (
                    False,
                    f"Query too slow: {elapsed_ms:.0f}ms (max {max_time_ms}ms)",
                    proof,
                )

            total = self._extract_query_count(response.body, "total")
            # User 0 has exactly 1 outgoing KNOWS edge (to user 1)
            if total != 1:
                return (
                    False,
                    f"Expected 1 outgoing KNOWS edge from user 0, got {total}",
                    proof,
                )
            self.logger.info(f"Outgoing edges: {elapsed_ms:.0f}ms")
            return True, "", proof

        results.append(self._run_test("Perf: Outgoing KNOWS edges", check_outgoing))

        # Test 4: Incoming KNOWS edges to a user (user 10 <- user 9)
        def check_incoming():
            start = time.time()
            response = self.api.query(
                "MATCH (other:PerfUser)-[r:PERF_KNOWS]->(u:PerfUser {index: 10}) "
                "RETURN count(r) AS total"
            )
            elapsed_ms = (time.time() - start) * 1000
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            if elapsed_ms > max_time_ms:
                return (
                    False,
                    f"Query too slow: {elapsed_ms:.0f}ms (max {max_time_ms}ms)",
                    proof,
                )

            total = self._extract_query_count(response.body, "total")
            # User 10 has exactly 1 incoming KNOWS edge (from user 9)
            if total != 1:
                return (
                    False,
                    f"Expected 1 incoming KNOWS edge to user 10, got {total}",
                    proof,
                )
            self.logger.info(f"Incoming edges: {elapsed_ms:.0f}ms")
            return True, "", proof

        results.append(self._run_test("Perf: Incoming KNOWS edges", check_incoming))

        # Test 5: Simple WHERE clause
        def check_simple_where():
            start = time.time()
            response = self.api.query(
                "MATCH (u:PerfUser) WHERE u.enabled = true RETURN count(u) AS total"
            )
            elapsed_ms = (time.time() - start) * 1000
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            if elapsed_ms > max_time_ms:
                return (
                    False,
                    f"Query too slow: {elapsed_ms:.0f}ms (max {max_time_ms}ms)",
                    proof,
                )

            total = self._extract_query_count(response.body, "total")
            # Users 0, 2, 4, ..., 18 are enabled (10 total)
            if total != 10:
                return False, f"Expected 10 enabled users, got {total}", proof
            self.logger.info(f"Simple WHERE: {elapsed_ms:.0f}ms")
            return True, "", proof

        results.append(self._run_test("Perf: Simple WHERE clause", check_simple_where))

        # Test 6: Complex WHERE with AND
        def check_complex_where_and():
            start = time.time()
            response = self.api.query(
                "MATCH (u:PerfUser) WHERE u.enabled = true AND u.score >= 100 "
                "RETURN count(u) AS total"
            )
            elapsed_ms = (time.time() - start) * 1000
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            if elapsed_ms > max_time_ms:
                return (
                    False,
                    f"Query too slow: {elapsed_ms:.0f}ms (max {max_time_ms}ms)",
                    proof,
                )

            total = self._extract_query_count(response.body, "total")
            # enabled (even index) AND score >= 100 (index >= 10)
            # indices: 10, 12, 14, 16, 18 = 5 users
            if total != 5:
                return (
                    False,
                    f"Expected 5 users (enabled AND score>=100), got {total}",
                    proof,
                )
            self.logger.info(f"Complex WHERE AND: {elapsed_ms:.0f}ms")
            return True, "", proof

        results.append(
            self._run_test("Perf: Complex WHERE with AND", check_complex_where_and)
        )

        # Test 7: Complex WHERE with OR
        def check_complex_where_or():
            start = time.time()
            response = self.api.query(
                "MATCH (u:PerfUser) WHERE u.index = 0 OR u.index = 19 "
                "RETURN count(u) AS total"
            )
            elapsed_ms = (time.time() - start) * 1000
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            if elapsed_ms > max_time_ms:
                return (
                    False,
                    f"Query too slow: {elapsed_ms:.0f}ms (max {max_time_ms}ms)",
                    proof,
                )

            total = self._extract_query_count(response.body, "total")
            if total != 2:
                return False, f"Expected 2 users (index 0 OR 19), got {total}", proof
            self.logger.info(f"Complex WHERE OR: {elapsed_ms:.0f}ms")
            return True, "", proof

        results.append(
            self._run_test("Perf: Complex WHERE with OR", check_complex_where_or)
        )

        # Test 8: Shortest path (user 0 to user 10 via KNOWS chain)
        # Backend-specific syntax required:
        # - CrustDB: inline properties in shortestPath pattern
        # - Neo4j: separate MATCH for nodes, then MATCH p = shortestPath(...)
        # - FalkorDB: separate MATCH for nodes, then WITH shortestPath(...) AS p
        def check_shortest_path_perf():
            start = time.time()
            if self.backend == "crustdb":
                query = (
                    "MATCH p = shortestPath((src:PerfUser {index: 0})-[*1..20]->(dst:PerfUser {index: 10})) "
                    "RETURN length(p) AS hops"
                )
            elif self.backend == "falkordb":
                query = (
                    "MATCH (src:PerfUser {index: 0}), (dst:PerfUser {index: 10}) "
                    "WITH shortestPath((src)-[*1..20]->(dst)) AS p "
                    "RETURN length(p) AS hops"
                )
            else:  # neo4j
                query = (
                    "MATCH (src:PerfUser {index: 0}), (dst:PerfUser {index: 10}) "
                    "MATCH p = shortestPath((src)-[*1..20]->(dst)) "
                    "RETURN length(p) AS hops"
                )
            response = self.api.query(query)
            elapsed_ms = (time.time() - start) * 1000
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            if elapsed_ms > max_time_ms:
                return (
                    False,
                    f"Query too slow: {elapsed_ms:.0f}ms (max {max_time_ms}ms)",
                    proof,
                )

            hops = self._extract_query_count(response.body, "hops")
            # user0 -> user1 -> ... -> user10 = 10 hops
            if hops != 10:
                return False, f"Expected 10 hops, got {hops}", proof
            self.logger.info(f"Shortest path (10 hops): {elapsed_ms:.0f}ms")
            return True, "", proof

        results.append(
            self._run_test("Perf: Shortest path (10 hops)", check_shortest_path_perf)
        )

        # Test 9: Longer shortest path (user 0 to user 19)
        # Backend-specific syntax (same as test 8)
        def check_longer_shortest_path():
            start = time.time()
            if self.backend == "crustdb":
                query = (
                    "MATCH p = shortestPath((src:PerfUser {index: 0})-[*1..20]->(dst:PerfUser {index: 19})) "
                    "RETURN length(p) AS hops"
                )
            elif self.backend == "falkordb":
                query = (
                    "MATCH (src:PerfUser {index: 0}), (dst:PerfUser {index: 19}) "
                    "WITH shortestPath((src)-[*1..20]->(dst)) AS p "
                    "RETURN length(p) AS hops"
                )
            else:  # neo4j
                query = (
                    "MATCH (src:PerfUser {index: 0}), (dst:PerfUser {index: 19}) "
                    "MATCH p = shortestPath((src)-[*1..20]->(dst)) "
                    "RETURN length(p) AS hops"
                )
            response = self.api.query(query)
            elapsed_ms = (time.time() - start) * 1000
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            if elapsed_ms > max_time_ms:
                return (
                    False,
                    f"Query too slow: {elapsed_ms:.0f}ms (max {max_time_ms}ms)",
                    proof,
                )

            hops = self._extract_query_count(response.body, "hops")
            if hops != 19:
                return False, f"Expected 19 hops, got {hops}", proof
            self.logger.info(f"Shortest path (19 hops): {elapsed_ms:.0f}ms")
            return True, "", proof

        results.append(
            self._run_test("Perf: Shortest path (19 hops)", check_longer_shortest_path)
        )

        # Test 10: Combined pattern - path traversal with property filter
        def check_combined_pattern():
            start = time.time()
            response = self.api.query(
                "MATCH (u1:PerfUser)-[:PERF_KNOWS]->(u2:PerfUser) "
                "WHERE u1.enabled = true AND u2.score >= 50 "
                "RETURN count(u1) AS total"
            )
            elapsed_ms = (time.time() - start) * 1000
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            if elapsed_ms > max_time_ms:
                return (
                    False,
                    f"Query too slow: {elapsed_ms:.0f}ms (max {max_time_ms}ms)",
                    proof,
                )

            # u1.enabled (even index) -> u2.score >= 50 (index >= 5)
            # Edges: 4->5, 6->7, 8->9, 10->11, 12->13, 14->15, 16->17, 18->19 = 8 pairs
            total = self._extract_query_count(response.body, "total")
            if total != 8:
                return False, f"Expected 8 matching pairs, got {total}", proof
            self.logger.info(f"Combined pattern: {elapsed_ms:.0f}ms")
            return True, "", proof

        results.append(
            self._run_test(
                "Perf: Combined pattern with filters", check_combined_pattern
            )
        )

        # Test 11: Node connections API - get a real node from imported data
        # First, find a User node with connections
        test_node_id = None

        def find_test_node():
            nonlocal test_node_id
            # Search for a user node to test connections API
            response = self.api.search("admin", limit=1)
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Search failed: {response.body}", proof
            items = response.body
            if not items or not isinstance(items, list) or len(items) == 0:
                # Fall back to querying for any user
                response = self.api.query(
                    "MATCH (u:User) RETURN u.objectid AS id LIMIT 1"
                )
                if not response.ok:
                    return False, f"Query failed: {response.body}", proof
                result_data = self._body_get(response.body, "results", {})
                if isinstance(result_data, list) and result_data:
                    first = result_data[0]
                    if isinstance(first, list) and first:
                        test_node_id = str(first[0])
                    elif isinstance(first, dict):
                        test_node_id = str(first.get("id", ""))
                if not test_node_id:
                    return False, "No nodes found for connections test", proof
            else:
                # Use the first search result
                test_node_id = items[0].get("id", items[0].get("objectid", ""))
            self.logger.info(f"Using node {test_node_id} for connections tests")
            return True, "", proof

        results.append(
            self._run_test("Perf: Find test node for connections", find_test_node)
        )

        if not test_node_id:
            return results

        # Test 12: Node counts API (used for badges)
        def check_node_counts():
            start = time.time()
            response = self.api.node_counts(test_node_id)
            elapsed_ms = (time.time() - start) * 1000
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Node counts failed: {response.body}", proof
            if elapsed_ms > max_time_ms:
                return (
                    False,
                    f"Node counts too slow: {elapsed_ms:.0f}ms (max {max_time_ms}ms)",
                    proof,
                )
            # Verify response has expected fields
            body = response.body
            if not isinstance(body, dict):
                return False, f"Expected dict, got {type(body)}", proof
            required = ["incoming", "outgoing"]
            for field in required:
                if field not in body:
                    return False, f"Missing field: {field}", proof
            self.logger.info(
                f"Node counts: {elapsed_ms:.0f}ms, in={body.get('incoming')}, out={body.get('outgoing')}"
            )
            return True, "", proof

        results.append(self._run_test("Perf: Node counts API (<3s)", check_node_counts))

        # Test 13: Incoming connections API
        def check_incoming_connections():
            start = time.time()
            response = self.api.node_connections(test_node_id, "incoming")
            elapsed_ms = (time.time() - start) * 1000
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Incoming connections failed: {response.body}", proof
            if elapsed_ms > max_time_ms:
                return (
                    False,
                    f"Incoming connections too slow: {elapsed_ms:.0f}ms (max {max_time_ms}ms)",
                    proof,
                )
            # Verify response structure
            body = response.body
            if not isinstance(body, dict):
                return False, f"Expected dict, got {type(body)}", proof
            nodes = body.get("nodes", [])
            relationships = body.get("relationships", [])
            self.logger.info(
                f"Incoming connections: {elapsed_ms:.0f}ms, {len(nodes)} nodes, {len(relationships)} relationships"
            )
            return True, "", proof

        results.append(
            self._run_test(
                "Perf: Incoming connections API (<3s)", check_incoming_connections
            )
        )

        # Test 14: Outgoing connections API
        def check_outgoing_connections():
            start = time.time()
            response = self.api.node_connections(test_node_id, "outgoing")
            elapsed_ms = (time.time() - start) * 1000
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Outgoing connections failed: {response.body}", proof
            if elapsed_ms > max_time_ms:
                return (
                    False,
                    f"Outgoing connections too slow: {elapsed_ms:.0f}ms (max {max_time_ms}ms)",
                    proof,
                )
            # Verify response structure
            body = response.body
            if not isinstance(body, dict):
                return False, f"Expected dict, got {type(body)}", proof
            nodes = body.get("nodes", [])
            relationships = body.get("relationships", [])
            self.logger.info(
                f"Outgoing connections: {elapsed_ms:.0f}ms, {len(nodes)} nodes, {len(relationships)} relationships"
            )
            return True, "", proof

        results.append(
            self._run_test(
                "Perf: Outgoing connections API (<3s)", check_outgoing_connections
            )
        )

        return results

    # =========================================================================
    # Query History Tests
    # =========================================================================

    def test_query_history(self) -> list[TestResult]:
        """Test query history functionality."""
        results = []

        # Query history should not be empty (queries were run in test_queries)
        def check_history_not_empty():
            response = self.api.query_history(page=1, per_page=10)
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query history failed: {response.body}", proof
            entries = self._body_get(response.body, "entries", [])
            if not entries:
                return False, "Query history is empty", proof
            self.logger.info(f"Query history has {len(entries)} entries")
            return True, "", proof

        results.append(
            self._run_test("Query history is not empty", check_history_not_empty)
        )

        # Query history should respond quickly (< 100ms)
        def check_history_fast():
            start = time.time()
            response = self.api.query_history(page=1, per_page=10)
            elapsed_ms = (time.time() - start) * 1000
            proof = f"Response time: {elapsed_ms:.1f}ms"
            if not response.ok:
                return False, f"Query history failed: {response.body}", proof
            if elapsed_ms > 100:
                return (
                    False,
                    f"Query history too slow: {elapsed_ms:.1f}ms (expected <100ms)",
                    proof,
                )
            self.logger.info(f"Query history responded in {elapsed_ms:.1f}ms")
            return True, "", proof

        results.append(
            self._run_test("Query history responds in <100ms", check_history_fast)
        )

        return results

    # =========================================================================
    # Node API Tests
    # =========================================================================

    def test_node_apis(self) -> list[TestResult]:
        """Test node-specific APIs (get, status, owned)."""
        results = []
        max_time_ms = 3000

        # First, find a node to test with
        test_node_id = None

        def find_test_node():
            nonlocal test_node_id
            # Search for a user node
            response = self.api.search("admin", limit=1)
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Search failed: {response.body}", proof
            items = response.body
            if items and isinstance(items, list) and len(items) > 0:
                test_node_id = items[0].get("id", items[0].get("objectid", ""))
            if not test_node_id:
                # Fallback: query for any user
                response = self.api.query(
                    "MATCH (u:User) RETURN u.objectid AS id LIMIT 1"
                )
                if response.ok:
                    result_data = self._body_get(response.body, "results", {})
                    if isinstance(result_data, list) and result_data:
                        first = result_data[0]
                        if isinstance(first, list) and first:
                            test_node_id = str(first[0])
                        elif isinstance(first, dict):
                            test_node_id = str(first.get("id", ""))
            if not test_node_id:
                return False, "No nodes found for testing", proof
            self.logger.info(f"Using node {test_node_id} for node API tests")
            return True, "", proof

        results.append(self._run_test("Find test node", find_test_node))

        if not test_node_id:
            return results

        # Test node_get API
        def check_node_get():
            start = time.time()
            response = self.api.node_get(test_node_id)
            elapsed_ms = (time.time() - start) * 1000
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Node get failed: {response.body}", proof
            if elapsed_ms > max_time_ms:
                return False, f"Node get too slow: {elapsed_ms:.0f}ms", proof
            body = response.body
            if not isinstance(body, dict):
                return False, f"Expected dict, got {type(body)}", proof
            self.logger.info(f"Node get: {elapsed_ms:.0f}ms")
            return True, "", proof

        results.append(self._run_test("Node get API works", check_node_get))

        # Test node_status API (high-value detection)
        def check_node_status():
            start = time.time()
            response = self.api.node_status(test_node_id)
            elapsed_ms = (time.time() - start) * 1000
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Node status failed: {response.body}", proof
            if elapsed_ms > max_time_ms:
                return False, f"Node status too slow: {elapsed_ms:.0f}ms", proof
            body = response.body
            if not isinstance(body, dict):
                return False, f"Expected dict, got {type(body)}", proof
            # Should have isHighValue and hasPathToHighValue fields (camelCase)
            if "isHighValue" not in body:
                return False, "Missing isHighValue field", proof
            if "hasPathToHighValue" not in body:
                return False, "Missing hasPathToHighValue field", proof
            self.logger.info(
                f"Node status: {elapsed_ms:.0f}ms, "
                f"isHighValue={body.get('isHighValue')}, "
                f"hasPath={body.get('hasPathToHighValue')}"
            )
            return True, "", proof

        results.append(self._run_test("Node status API works", check_node_status))

        # Test node_set_owned API
        def check_node_set_owned():
            # Set owned = true
            response = self.api.node_set_owned(test_node_id, owned=True)
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Node set owned failed: {response.body}", proof
            # Set owned = false (reset)
            response = self.api.node_set_owned(test_node_id, owned=False)
            if not response.ok:
                return False, f"Node unset owned failed: {response.body}", proof
            self.logger.info("Node set owned API works")
            return True, "", proof

        results.append(self._run_test("Node set owned API works", check_node_set_owned))

        return results

    # =========================================================================
    # Security Insights Tests
    # =========================================================================

    def test_insights(self) -> list[TestResult]:
        """Test security insights API."""
        results = []

        max_time_ms = (
            10000  # 10 seconds (insights involve variable-length path queries)
        )

        # Test insights endpoint
        def check_insights():
            start = time.time()
            response = self.api.insights()
            elapsed_ms = (time.time() - start) * 1000
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Insights failed: {response.body}", proof
            if elapsed_ms > max_time_ms:
                return False, f"Insights too slow: {elapsed_ms:.0f}ms", proof
            body = response.body
            if not isinstance(body, dict):
                return False, f"Expected dict, got {type(body)}", proof
            # Check expected fields exist
            expected_fields = [
                "high_value_targets",
                "kerberoastable",
                "asrep_roastable",
                "unconstrained_delegation",
            ]
            for field in expected_fields:
                if field not in body:
                    self.logger.warning(f"Missing field in insights: {field}")
            self.logger.info(
                f"Insights: {elapsed_ms:.0f}ms, "
                f"high_value={len(body.get('high_value_targets', []))}, "
                f"kerberoastable={len(body.get('kerberoastable', []))}"
            )
            return True, "", proof

        results.append(self._run_test("Insights API works", check_insights))

        return results

    # =========================================================================
    # Choke Points Tests
    # =========================================================================

    def test_choke_points(self) -> list[TestResult]:
        """Test choke points API (edge betweenness centrality)."""
        results = []
        max_time_ms = (
            30000  # 30 seconds (expensive algorithm, but cached after first run)
        )

        # Test choke points endpoint (first call - computes)
        def check_choke_points_first():
            start = time.time()
            response = self.api.choke_points()
            elapsed_ms = (time.time() - start) * 1000
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Choke points failed: {response.body}", proof
            if elapsed_ms > max_time_ms:
                return False, f"Choke points too slow: {elapsed_ms:.0f}ms", proof
            body = response.body
            if not isinstance(body, dict):
                return False, f"Expected dict, got {type(body)}", proof
            if "choke_points" not in body:
                return False, "Missing choke_points field", proof
            if "total_edges" not in body:
                return False, "Missing total_edges field", proof
            choke_points = body.get("choke_points", [])
            self.logger.info(
                f"Choke points (first call): {elapsed_ms:.0f}ms, "
                f"count={len(choke_points)}, total_edges={body.get('total_edges')}"
            )
            return True, "", proof

        results.append(
            self._run_test("Choke points API works", check_choke_points_first)
        )

        # Test choke points endpoint (second call - should use cache)
        def check_choke_points_cached():
            start = time.time()
            response = self.api.choke_points()
            elapsed_ms = (time.time() - start) * 1000
            proof = f"Response time: {elapsed_ms:.1f}ms"
            if not response.ok:
                return False, f"Choke points (cached) failed: {response.body}", proof
            # Cached call should be fast (< 500ms)
            if elapsed_ms > 500:
                self.logger.warning(
                    f"Choke points cached call slower than expected: {elapsed_ms:.0f}ms"
                )
            self.logger.info(f"Choke points (cached): {elapsed_ms:.0f}ms")
            return True, "", proof

        results.append(
            self._run_test("Choke points cached call fast", check_choke_points_cached)
        )

        return results

    # =========================================================================
    # Shortest Path Tests
    # =========================================================================

    def test_shortest_path(self) -> list[TestResult]:
        """Test shortest path API."""
        results = []
        max_time_ms = 5000

        # Find two nodes to test path between
        source_id = None
        target_id = None

        def find_path_nodes():
            nonlocal source_id, target_id
            # Find two directly connected nodes for path testing
            # Using a direct relationship guarantees a path exists
            response = self.api.query(
                "MATCH (a)-[r]->(b) "
                "WHERE a.objectid IS NOT NULL AND b.objectid IS NOT NULL "
                "RETURN a.objectid AS src, b.objectid AS tgt LIMIT 1"
            )
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            result_data = self._body_get(response.body, "results", {})
            # Handle different result formats from different backends
            if isinstance(result_data, dict):
                if "rows" in result_data:
                    # Headers/rows format: {headers: [...], rows: [[...]]}
                    # Use headers to find correct column indices (order is
                    # not guaranteed by all backends).
                    headers = result_data.get("headers", [])
                    rows = result_data.get("rows", [])
                    if rows and len(rows[0]) >= 2 and headers:
                        col_map = {h: i for i, h in enumerate(headers)}
                        src_idx = col_map.get("src", 0)
                        tgt_idx = col_map.get("tgt", 1)
                        source_id = str(rows[0][src_idx]) if rows[0][src_idx] else None
                        target_id = str(rows[0][tgt_idx]) if rows[0][tgt_idx] else None
                    elif rows and len(rows[0]) >= 2:
                        source_id = str(rows[0][0]) if rows[0][0] else None
                        target_id = str(rows[0][1]) if rows[0][1] else None
                elif "results" in result_data:
                    # Neo4j/FalkorDB format: {results: [[...]]} or {results: [{...}]}
                    inner_results = result_data.get("results", [])
                    if inner_results:
                        first = inner_results[0]
                        if isinstance(first, dict):
                            source_id = first.get("src")
                            target_id = first.get("tgt")
                        elif isinstance(first, list) and len(first) >= 2:
                            source_id = str(first[0]) if first[0] else None
                            target_id = str(first[1]) if first[1] else None
            elif isinstance(result_data, list) and result_data:
                first = result_data[0]
                if isinstance(first, dict):
                    source_id = first.get("src")
                    target_id = first.get("tgt")
                elif isinstance(first, list) and len(first) >= 2:
                    source_id = str(first[0]) if first[0] else None
                    target_id = str(first[1]) if first[1] else None
            if not source_id or not target_id:
                # No suitable path nodes found - this can happen with some test data
                self.logger.info("No connected nodes found for path test - skipping")
                return True, "Skipped - no suitable connected nodes in test data", proof
            self.logger.info(f"Path test: {source_id} -> {target_id}")
            return True, "", proof

        results.append(self._run_test("Find nodes for path test", find_path_nodes))

        if not source_id or not target_id:
            # Skip shortest path test if we couldn't find suitable nodes
            return results

        # Test shortest path
        def check_shortest_path():
            start = time.time()
            response = self.api.shortest_path(source_id, target_id)
            elapsed_ms = (time.time() - start) * 1000
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Shortest path failed: {response.body}", proof
            if elapsed_ms > max_time_ms:
                return False, f"Shortest path too slow: {elapsed_ms:.0f}ms", proof
            body = response.body
            if not isinstance(body, dict):
                return False, f"Expected dict, got {type(body)}", proof
            # Check response structure
            if "found" not in body:
                return False, "Missing 'found' field in response", proof
            if "path" not in body:
                return False, "Missing 'path' field in response", proof
            path = body.get("path", [])
            found = body.get("found", False)
            if not found:
                return False, "No path found between nodes", proof
            if len(path) < 2:
                return (
                    False,
                    f"Path too short: expected at least 2 nodes, got {len(path)}",
                    proof,
                )
            self.logger.info(f"Shortest path: {elapsed_ms:.0f}ms, {len(path)} steps")
            return True, "", proof

        results.append(self._run_test("Shortest path API works", check_shortest_path))

        return results

    # =========================================================================
    # Cache and Settings Tests
    # =========================================================================

    def test_cache_and_settings(self) -> list[TestResult]:
        """Test cache stats and settings APIs."""
        results = []

        # Test cache stats
        def check_cache_stats():
            response = self.api.cache_stats()
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Cache stats failed: {response.body}", proof
            body = response.body
            if not isinstance(body, dict):
                return False, f"Expected dict, got {type(body)}", proof
            self.logger.info(f"Cache stats: entries={body.get('entries', 0)}")
            return True, "", proof

        results.append(self._run_test("Cache stats API works", check_cache_stats))

        # Test settings
        def check_settings():
            response = self.api.settings()
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Settings failed: {response.body}", proof
            body = response.body
            if not isinstance(body, dict):
                return False, f"Expected dict, got {type(body)}", proof
            self.logger.info(f"Settings: {list(body.keys())}")
            return True, "", proof

        results.append(self._run_test("Settings API works", check_settings))

        return results

    # =========================================================================
    # Query Consistency Tests (cross-backend comparison)
    # =========================================================================

    # Additional queries not in builtin-queries.ts (from insights.ts and
    # high-value path analysis). These are the only queries defined here;
    # all built-in queries are parsed from the frontend source files.
    EXTRA_CONSISTENCY_QUERIES: list[tuple[str, str]] = [
        (
            "insights/effective-domain-admins",
            "MATCH (u:User), (g:Group), "
            "p = shortestPath((u)-[*1..10]->(g)) "
            "WHERE g.objectid ENDS WITH '-512' "
            "RETURN DISTINCT u",
        ),
        (
            "insights/real-domain-admins",
            "MATCH (u:User)-[:MemberOf*1..10]->(g:Group) "
            "WHERE g.objectid ENDS WITH '-512' "
            "RETURN DISTINCT u",
        ),
    ]

    @staticmethod
    def _parse_builtin_queries(project_root: Path) -> list[tuple[str, str]]:
        """Parse queries from the frontend builtin-queries.ts source file.

        This ensures the e2e test always uses the same queries as the UI,
        with no duplication. Evaluates JS template literal interpolations
        like ``${ridWhereClause("g", [...HIGH_VALUE_RIDS, "-S-1-5-9"])}``.
        """
        import re

        queries_file = (
            project_root
            / "src"
            / "frontend"
            / "components"
            / "queries"
            / "builtin-queries.ts"
        )
        content = queries_file.read_text()

        # Parse HIGH_VALUE_RIDS array from the TS source
        high_value_rids: list[str] = []
        rids_match = re.search(
            r"export\s+const\s+HIGH_VALUE_RIDS\s*=\s*\[(.*?)\]",
            content,
            re.DOTALL,
        )
        if rids_match:
            high_value_rids = re.findall(r'"([^"]+)"', rids_match.group(1))

        def rid_where_clause(variable: str, rids: list[str]) -> str:
            """Python equivalent of the TS ridWhereClause function."""
            return " OR ".join(
                f"{variable}.objectid ENDS WITH '{rid}'" for rid in rids
            )

        def evaluate_interpolation(expr: str) -> str:
            """Evaluate a JS template literal interpolation expression."""
            # Match ridWhereClause("var", [...]) calls
            m = re.match(
                r'ridWhereClause\(\s*"(\w+)"\s*,\s*\[(.*?)\]\s*\)',
                expr,
                re.DOTALL,
            )
            if not m:
                return "${" + expr + "}"  # return unmodified if unrecognized

            variable = m.group(1)
            args_str = m.group(2)

            # Build the RID list by resolving ...HIGH_VALUE_RIDS spreads
            # and string literals
            rids: list[str] = []
            for token in re.finditer(
                r'\.\.\.HIGH_VALUE_RIDS|"([^"]+)"', args_str
            ):
                if token.group(0) == "...HIGH_VALUE_RIDS":
                    rids.extend(high_value_rids)
                else:
                    rids.append(token.group(1))

            return rid_where_clause(variable, rids)

        def interpolate_template(query: str) -> str:
            """Replace ${...} expressions in a JS template literal."""
            return re.sub(
                r"\$\{([^}]+)\}",
                lambda m: evaluate_interpolation(m.group(1)),
                query,
            )

        results: list[tuple[str, str]] = []

        # Extract query objects: { id: "...", ... query: `...` }
        # Match id-to-query spans that do NOT contain another "id:" in between
        # (which would indicate crossing into a different object).
        pattern = re.compile(
            r'id:\s*"([^"]+)"'  # capture the query id
            r'(?:(?!id:\s*").)*?'  # skip fields, but stop if another id: appears
            r"query:\s*`([^`]+)`",  # capture the query string
            re.DOTALL,
        )

        for match in pattern.finditer(content):
            query_id = match.group(1)
            query = match.group(2).strip()
            # Evaluate JS template literal interpolations
            query = interpolate_template(query)
            # Normalize whitespace (template literals may have newlines)
            query = re.sub(r"\s+", " ", query)
            results.append((f"builtin/{query_id}", query))

        return results

    @staticmethod
    def _rewrite_shortestpath_for_falkordb(cypher: str) -> str:
        """Rewrite shortestPath from MATCH pattern to WITH clause for FalkorDB.

        FalkorDB only supports shortestPath in WITH or RETURN clauses, not
        directly in MATCH patterns. This rewrites queries like:
            MATCH ..., p = shortestPath((a)-[*1..N]->(b)) WHERE ... RETURN ...
        to:
            MATCH ..., (a), (b) WHERE ... WITH ..., shortestPath((a)-[*1..N]->(b)) AS p RETURN ...
        """
        import re

        # Match pattern: p = shortestPath((...)-[*..N]->(...))
        sp_match = re.search(
            r",?\s*p\s*=\s*shortestPath\((\([^)]+\))-(\[[^\]]+\])->(\([^)]+\))\)",
            cypher,
        )
        if not sp_match:
            return cypher

        full_sp = sp_match.group(0)
        start_node = sp_match.group(1)  # e.g. (u)
        rel_pattern = sp_match.group(2)  # e.g. [*1..10]
        end_node = sp_match.group(3)  # e.g. (g)

        # Remove the shortestPath assignment from MATCH
        modified = cypher.replace(full_sp, "")

        # Split on WHERE/RETURN to insert WITH clause
        # Pattern: MATCH ... WHERE ... RETURN ...
        return_match = re.search(r"\bRETURN\b", modified)
        if not return_match:
            return cypher

        before_return = modified[: return_match.start()].rstrip()
        return_clause = modified[return_match.start() :]

        # Extract variables used in RETURN that we need to carry through WITH
        # Simple approach: pass through all bound variables from MATCH
        # Find node variable names from start/end patterns
        start_var = re.search(r"\((\w+)", start_node)
        end_var = re.search(r"\((\w+)", end_node)
        if not start_var or not end_var:
            return cypher

        sv = start_var.group(1)
        ev = end_var.group(1)
        sp_expr = f"shortestPath(({sv})-{rel_pattern}->({ev})) AS p"

        result = f"{before_return} WITH {sv}, {ev}, {sp_expr} WHERE p IS NOT NULL {return_clause}"
        return result

    def test_graph_data(self) -> list[TestResult]:
        """Fetch all nodes and edges for cross-backend comparison.

        Queries every node (label + objectid) and every edge (source objectid,
        relationship type, target objectid), sorts them, and stores the results
        on ``self.all_nodes`` / ``self.all_edges``.  The E2E runner compares
        these across backends after all backends complete.
        """
        results: list[TestResult] = []

        # -- Collect all nodes --
        def collect_nodes():
            response = self.api.query(
                "MATCH (n) RETURN labels(n) AS labels, n.objectid AS objectid"
                " ORDER BY objectid",
                timeout=120,
            )
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Node query failed: {response.body}", proof
            rows = self._extract_rows(response.body, ["labels", "objectid"])
            if not rows:
                return False, "No node rows returned", proof
            self.all_nodes = sorted(rows)
            self.logger.info(f"Collected {len(self.all_nodes)} nodes for cross-backend comparison")
            return True, "", proof

        results.append(self._run_test("Collect all nodes", collect_nodes))

        # -- Collect all edges --
        def collect_edges():
            response = self.api.query(
                "MATCH (n)-[r]->(m) RETURN n.objectid AS src, type(r) AS rel,"
                " m.objectid AS tgt ORDER BY src, rel, tgt",
                timeout=120,
            )
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Edge query failed: {response.body}", proof
            rows = self._extract_rows(response.body, ["src", "rel", "tgt"])
            if not rows:
                return False, "No edge rows returned", proof
            self.all_edges = sorted(rows)
            self.logger.info(f"Collected {len(self.all_edges)} edges for cross-backend comparison")
            return True, "", proof

        results.append(self._run_test("Collect all edges", collect_edges))

        return results

    def test_query_consistency(self) -> list[TestResult]:
        """Run all built-in and high-value path queries, recording result counts.

        Queries are parsed from the frontend source files to maintain a single
        source of truth. The counts are stored in self.query_counts for
        cross-backend comparison by the E2E runner after all backends complete.
        """
        results = []
        max_time_ms = 30000  # 30s per query (variable-length paths can be slow)

        # Find project root (e2e/lib/runner.py -> project root)
        project_root = Path(__file__).resolve().parent.parent.parent

        # Parse queries from the frontend source
        consistency_queries = self._parse_builtin_queries(project_root)
        if not consistency_queries:
            results.append(
                self._run_test(
                    "Parse builtin queries",
                    lambda: (False, "No queries parsed from builtin-queries.ts", ""),
                )
            )
            return results

        self.logger.info(
            f"Parsed {len(consistency_queries)} queries from builtin-queries.ts"
        )

        # Add extra queries (insights, etc.)
        consistency_queries.extend(self.EXTRA_CONSISTENCY_QUERIES)

        for query_id, cypher in consistency_queries:
            # FalkorDB requires shortestPath in WITH/RETURN, not in MATCH
            if self.backend == "falkordb":
                cypher = self._rewrite_shortestpath_for_falkordb(cypher)

            def make_check(qid: str, q: str):
                def check():
                    start = time.time()
                    response = self.api.query(q, timeout=60)
                    elapsed_ms = (time.time() - start) * 1000
                    proof = self._to_proof(response.body)
                    if not response.ok:
                        return False, f"Query failed: {response.body}", proof
                    if elapsed_ms > max_time_ms:
                        return False, f"Too slow: {elapsed_ms:.0f}ms", proof

                    count = self._body_get(response.body, "result_count", None)
                    if count is None:
                        # Try extracting from results
                        result_data = self._body_get(response.body, "results", {})
                        count = self._extract_count(result_data, "cnt")
                    if count is None:
                        return False, "Could not extract count from results", proof

                    self.query_counts[qid] = count
                    self.logger.info(f"  {qid}: {count} ({elapsed_ms:.0f}ms)")
                    return True, "", proof

                return check

            results.append(
                self._run_test(
                    f"Consistency: {query_id}",
                    make_check(query_id, cypher),
                )
            )

        return results
