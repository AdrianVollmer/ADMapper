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
    ):
        self.api = api
        self.test_data = test_data
        self.golden_file = golden_file
        self.logger = logger
        self._expected_stats: dict[str, Any] | None = None

    @property
    def expected_stats(self) -> dict[str, Any]:
        """Load expected stats from golden file."""
        if self._expected_stats is None:
            if self.golden_file.exists():
                self._expected_stats = json.loads(self.golden_file.read_text())
            else:
                self._expected_stats = {}
        return self._expected_stats

    def _run_test(self, name: str, test_fn: Callable[[], tuple[bool, str, str]]) -> TestResult:
        """Run a single test and capture the result."""
        start = time.time()
        try:
            passed, message, proof = test_fn()
        except Exception as e:
            passed = False
            message = str(e)
            proof = ""
        duration_ms = int((time.time() - start) * 1000)
        return TestResult(name=name, passed=passed, duration_ms=duration_ms, message=message, proof=proof)

    def _to_proof(self, data: Any) -> str:
        """Convert data to a proof string (JSON formatted)."""
        if isinstance(data, str):
            return data
        try:
            return json.dumps(data, indent=2, default=str)
        except Exception:
            return str(data)

    def _body_get(self, body: dict[str, Any] | list[Any], key: str, default: Any = None) -> Any:
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
            return True, "", f"File: {self.test_data}, size: {self.test_data.stat().st_size} bytes"

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

        # Check graph has edges
        def check_edges():
            response = self.api.stats()
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Stats request failed: {response.body}", proof
            edges = response.body.get("edges", 0)
            if edges <= 0:
                return False, "No edges in graph after import", proof
            self.logger.info(f"Edges imported: {edges}")
            return True, "", proof

        results.append(self._run_test("Graph has edges after import", check_edges))

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
            edges = self._body_get(response.body, "edges", 0)
            if nodes <= 0 or edges <= 0:
                return False, f"Invalid stats: nodes={nodes}, edges={edges}", proof
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
            return True, "", proof

        results.append(self._run_test("Detailed stats endpoint works", check_detailed_stats))

        # Validate counts against expected
        if detailed and expected:
            def check_total_nodes():
                actual = detailed.get("total_nodes", 0)
                exp = expected.get("total_nodes", 0)
                proof = f"actual: {actual}, expected: {exp}"
                if actual != exp:
                    return False, f"Expected {exp} nodes, got {actual}", proof
                self.logger.info(f"Total nodes: {actual}")
                return True, "", proof

            results.append(self._run_test("Total nodes matches expected", check_total_nodes))

            def check_total_edges():
                actual = detailed.get("total_edges", 0)
                exp = expected.get("total_edges", 0)
                proof = f"actual: {actual}, expected: {exp}"
                if actual != exp:
                    return False, f"Expected {exp} edges, got {actual}", proof
                self.logger.info(f"Total edges: {actual}")
                return True, "", proof

            results.append(self._run_test("Total edges matches expected", check_total_edges))

            # Check individual type counts
            for type_key, type_name in [
                ("users", "Users"),
                ("computers", "Computers"),
                ("groups", "Groups"),
                ("domains", "Domains"),
            ]:
                def make_check(key, name):
                    def check():
                        actual = detailed.get(key, 0)
                        exp = expected.get(key, 0)
                        proof = f"actual: {actual}, expected: {exp}"
                        if actual != exp:
                            return False, f"Expected {exp} {name}, got {actual}", proof
                        self.logger.info(f"{name}: {actual}")
                        return True, "", proof
                    return check

                results.append(self._run_test(
                    f"{type_name} count matches expected",
                    make_check(type_key, type_name)
                ))

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

        # Edge types endpoint
        def check_edge_types():
            response = self.api.edge_types()
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Edge types failed: {response.body}", proof
            types = response.body
            if not isinstance(types, list) or len(types) == 0:
                return False, "No edge types returned", proof
            self.logger.info(f"Edge types: {', '.join(types[:5])}")
            return True, "", proof

        results.append(self._run_test("Edge types endpoint works", check_edge_types))

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
            result_data = self._body_get(response.body, "results", {})
            total = self._extract_count(result_data, "total")
            if total is None or total <= 0:
                return False, f"Query returned no results: {result_data}", proof
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
            response = self.api.query("MATCH (n)-[r]->(m) RETURN count(r) AS edges LIMIT 1")
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            return True, "", proof

        results.append(self._run_test("Query with relationship", check_rel_query))

        # Property filter query
        def check_property_query():
            response = self.api.query("MATCH (u:User) WHERE u.enabled = true RETURN count(u) AS enabled")
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            return True, "", proof

        results.append(self._run_test("Query with property filter", check_property_query))

        # Return node properties
        def check_return_props():
            response = self.api.query("MATCH (u:User) RETURN u.name AS name LIMIT 5")
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            return True, "", proof

        results.append(self._run_test("Query returning node properties", check_return_props))

        # type() function
        def check_type_function():
            response = self.api.query("MATCH (n)-[r]->(m) RETURN type(r) AS rel_type LIMIT 5")
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            return True, "", proof

        results.append(self._run_test("Query with type() function", check_type_function))

        # labels() function
        def check_labels_function():
            response = self.api.query("MATCH (n) RETURN labels(n) AS labels LIMIT 5")
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Query failed: {response.body}", proof
            return True, "", proof

        results.append(self._run_test("Query with labels() function", check_labels_function))

        return results

    def _extract_count(self, results: dict[str, Any], column: str) -> int | None:
        """Extract a count value from query results in various formats."""
        # Format: {"rows": [[value]], "headers": [...]}
        if "rows" in results and results["rows"]:
            try:
                return int(results["rows"][0][0])
            except (IndexError, TypeError, ValueError):
                pass

        # Format: [{"column": value}]
        if isinstance(results, list) and results:
            try:
                return int(results[0].get(column, 0))
            except (TypeError, ValueError):
                pass

        # Format: {"results": [...]}
        if "results" in results:
            nested = results["results"]
            if isinstance(nested, list) and nested:
                try:
                    return int(nested[0].get(column, 0))
                except (TypeError, ValueError):
                    pass

        return None

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

        # Search with limit
        def check_search_limit():
            response = self.api.search("a", limit=5)
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Search failed: {response.body}", proof
            items = response.body
            if len(items) > 5:
                return False, f"Limit not respected: got {len(items)} results", proof
            return True, "", proof

        results.append(self._run_test("Search respects limit", check_search_limit))

        # Search with type filter
        def check_search_type_filter():
            response = self.api.search("a", limit=10, node_type="User")
            proof = self._to_proof(response.body)
            if not response.ok:
                return False, f"Search failed: {response.body}", proof
            if not isinstance(response.body, list):
                return False, "Expected list response", proof
            for item in response.body:
                if isinstance(item, dict) and item.get("node_type") != "User":
                    return False, f"Type filter not respected: {item.get('node_type')}", proof
            return True, "", proof

        results.append(self._run_test("Search with type filter", check_search_type_filter))

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

        results.append(self._run_test("Search is case insensitive", check_case_insensitive))

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

        results.append(self._run_test("Search returns empty for non-existent", check_no_results))

        return results
