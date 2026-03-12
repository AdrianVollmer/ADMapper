#!/usr/bin/env python3
"""
E2E Test Runner for ADMapper

Runs integration tests against all supported database backends.

Usage:
    ./e2e/run_tests.py <test_data.zip> [backend]

Arguments:
    test_data.zip - Path to BloodHound data zip file (required)
    backend       - Backend to test: crustdb, neo4j, falkordb, or all (default: all)

Environment variables:
    ADMAPPER_BIN  - Path to admapper binary (default: target/release/admapper)
    DEBUG         - Enable debug output
"""

from __future__ import annotations

import argparse
import atexit
import json
import logging
import os
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime
from dataclasses import dataclass, field
from pathlib import Path
from types import FrameType

# Add lib directory to path
SCRIPT_DIR = Path(__file__).parent.resolve()
sys.path.insert(0, str(SCRIPT_DIR / "lib"))

from api import APIClient, ServerProcess, start_server, stop_server, wait_for_server  # noqa: E402  # type: ignore[import-not-found]
from runner import TestRunner, TestResult  # noqa: E402  # type: ignore[import-not-found]

BACKENDS = ["crustdb", "neo4j", "falkordb"]


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colored output."""

    COLORS = {
        "DEBUG": "\033[0;36m",  # Cyan
        "INFO": "\033[0;32m",  # Green
        "WARNING": "\033[1;33m",  # Yellow
        "ERROR": "\033[0;31m",  # Red
        "CRITICAL": "\033[1;31m",  # Bold Red
        "TEST": "\033[0;34m",  # Blue
        "PASS": "\033[0;32m",  # Green
        "FAIL": "\033[0;31m",  # Red
    }
    RESET = "\033[0m"
    DIM = "\033[2m"

    def format(self, record: logging.LogRecord) -> str:
        # Handle custom log levels
        levelname = record.levelname
        if hasattr(record, "custom_level"):
            levelname = record.custom_level

        color = self.COLORS.get(levelname, "")

        # Dim server output for visual distinction
        if record.msg.startswith("[server]"):
            record.levelname = f"{self.DIM}[SERVER]{self.RESET}"
            record.msg = f"{self.DIM}{record.msg[8:].strip()}{self.RESET}"
        else:
            record.levelname = f"{color}[{levelname}]{self.RESET}"

        return super().format(record)


def setup_logging(debug: bool = False) -> logging.Logger:
    """Set up logging with colored output."""
    logger = logging.getLogger("e2e")
    logger.setLevel(logging.DEBUG if debug else logging.INFO)

    handler = logging.StreamHandler()
    handler.setFormatter(ColoredFormatter("%(levelname)s %(message)s"))
    logger.addHandler(handler)

    return logger


def log_test(logger: logging.Logger, msg: str) -> None:
    """Log a test message with TEST level."""
    record = logger.makeRecord(
        logger.name, logging.INFO, "", 0, msg, (), None
    )
    record.custom_level = "TEST"
    logger.handle(record)


def log_pass(logger: logging.Logger, msg: str) -> None:
    """Log a pass message with PASS level."""
    record = logger.makeRecord(
        logger.name, logging.INFO, "", 0, msg, (), None
    )
    record.custom_level = "PASS"
    logger.handle(record)


def log_fail(logger: logging.Logger, msg: str) -> None:
    """Log a fail message with FAIL level."""
    record = logger.makeRecord(
        logger.name, logging.INFO, "", 0, msg, (), None
    )
    record.custom_level = "FAIL"
    logger.handle(record)


@dataclass
class TestSuite:
    """Collection of test results for a backend."""

    backend: str
    results: list[TestResult] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def passed(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if not r.passed)

    @property
    def total_duration_ms(self) -> int:
        return sum(r.duration_ms for r in self.results)


class E2ETestRunner:
    """Main E2E test runner."""

    def __init__(
        self,
        test_data: Path,
        admapper_bin: Path,
        report_dir: Path,
        debug: bool = False,
    ):
        self.test_data = test_data
        self.admapper_bin = admapper_bin
        self.report_dir = report_dir
        self.debug = debug
        self.logger = setup_logging(debug)
        self.server_process: ServerProcess | None = None
        self.temp_db_dir: Path | None = None
        self.golden_file = Path("/tmp/golden/expected_stats.json")

        # Register cleanup handler
        atexit.register(self.cleanup)
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum: int, frame: FrameType | None) -> None:
        """Handle signals gracefully."""
        self.logger.info("Received signal, cleaning up...")
        self.cleanup()
        sys.exit(1)

    def cleanup(self) -> None:
        """Clean up resources."""
        self.logger.info("Cleaning up...")
        if self.server_process:
            stop_server(self.server_process, self.logger)
            self.server_process = None
        if self.temp_db_dir and self.temp_db_dir.exists():
            shutil.rmtree(self.temp_db_dir, ignore_errors=True)
            self.temp_db_dir = None

    def check_prerequisites(self) -> bool:
        """Check that all prerequisites are met."""
        self.logger.info("Checking prerequisites...")

        # Check binary exists
        if not self.admapper_bin.exists():
            self.logger.error(f"ADMapper binary not found: {self.admapper_bin}")
            self.logger.info("Build it with: ./scripts/build.sh backend")
            return False

        # Check test data exists
        if not self.test_data.exists():
            self.logger.error(f"Test data not found: {self.test_data}")
            return False

        # Generate expected stats from test data
        self.logger.info("Generating expected stats from test data...")
        self.golden_file.parent.mkdir(parents=True, exist_ok=True)

        result = subprocess.run(
            ["python3", str(SCRIPT_DIR / "generate-expected.py"), str(self.test_data)],
            env={**os.environ, "GOLDEN_FILE": str(self.golden_file)},
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            self.logger.error(f"Failed to generate expected stats: {result.stderr}")
            return False

        self.logger.info("Prerequisites OK")
        return True

    def run_backend_tests(self, backend: str) -> TestSuite:
        """Run all tests for a single backend."""
        port = 9191 + random.randint(0, 999)
        suite = TestSuite(backend=backend)

        self.logger.info("=" * 42)
        self.logger.info(f"Testing backend: {backend}")
        self.logger.info("=" * 42)

        # Create temporary database directory
        self.temp_db_dir = Path(tempfile.mkdtemp(prefix=f"e2e-{backend}-"))
        self.logger.info(f"Database directory: {self.temp_db_dir}")

        # Build database URL based on backend
        db_url = self._get_db_url(backend, self.temp_db_dir, port)
        if not db_url:
            self.logger.error(f"Unknown backend: {backend}")
            return suite

        # Start server
        self.server_process = start_server(
            self.admapper_bin, db_url, port, self.logger
        )
        if not self.server_process:
            self.logger.error(f"Failed to start server for {backend}")
            return suite

        # Wait for server to be ready
        api = APIClient(port=port)
        if not wait_for_server(api, timeout=30, logger=self.logger):
            self.logger.error("Server failed to start")
            stop_server(self.server_process, self.logger)
            self.server_process = None
            return suite

        # Run tests
        runner = TestRunner(
            api=api,
            test_data=self.test_data,
            golden_file=self.golden_file,
            logger=self.logger,
            backend=backend,
        )

        # Run each test module
        test_modules = [
            ("Health Check", runner.test_health),
            ("Import Data", runner.test_import),
            ("Graph Stats", runner.test_stats),
            ("Query Execution", runner.test_queries),
            ("Search", runner.test_search),
            ("Query History", runner.test_query_history),
            ("Node APIs", runner.test_node_apis),
            ("Security Insights", runner.test_insights),
            ("Choke Points", runner.test_choke_points),
            ("Shortest Path", runner.test_shortest_path),
            ("Cache and Settings", runner.test_cache_and_settings),
            ("Performance", runner.test_performance),
        ]

        for name, test_func in test_modules:
            log_test(self.logger, f"Running: {name}")
            try:
                results = test_func()
                suite.results.extend(results)
                for result in results:
                    if result.passed:
                        log_pass(self.logger, f"{result.name} ({result.duration_ms}ms)")
                    else:
                        log_fail(self.logger, f"{result.name}: {result.message} ({result.duration_ms}ms)")
            except Exception as e:
                self.logger.error(f"Test module {name} failed with exception: {e}")
                suite.results.append(TestResult(
                    name=name,
                    passed=False,
                    duration_ms=0,
                    message=str(e),
                ))

        # Stop server
        stop_server(self.server_process, self.logger)
        self.server_process = None

        # Generate XML report
        self._generate_xml_report(suite)

        # Cleanup temp directory
        if self.temp_db_dir and self.temp_db_dir.exists():
            shutil.rmtree(self.temp_db_dir, ignore_errors=True)
            self.temp_db_dir = None

        return suite

    def _get_db_url(self, backend: str, db_dir: Path, port: int) -> str | None:
        """Get database URL for a backend."""
        if backend == "crustdb":
            return f"crustdb://{db_dir}/test.db"
        elif backend == "neo4j":
            host = os.environ.get("NEO4J_HOST", "localhost")
            neo_port = os.environ.get("NEO4J_PORT", "7687")
            user = os.environ.get("NEO4J_USER", "neo4j")
            password = os.environ.get("NEO4J_PASSWORD", "neo4j123")
            return f"neo4j://{user}:{password}@{host}:{neo_port}"
        elif backend == "falkordb":
            host = os.environ.get("FALKORDB_HOST", "localhost")
            falkor_port = os.environ.get("FALKORDB_PORT", "6379")
            return f"falkordb://{host}:{falkor_port}"
        return None

    def _generate_xml_report(self, suite: TestSuite) -> None:
        """Generate JUnit XML report for a test suite."""
        output_file = self.report_dir / f"report-{suite.backend}.xml"
        total_time_s = suite.total_duration_ms / 1000.0
        timestamp = datetime.now().isoformat()

        # Build XML structure
        testsuites = ET.Element("testsuites")
        testsuite = ET.SubElement(
            testsuites,
            "testsuite",
            name=f"e2e-{suite.backend}",
            tests=str(suite.total),
            failures=str(suite.failed),
            time=f"{total_time_s:.3f}",
            timestamp=timestamp,
        )

        for result in suite.results:
            duration_s = result.duration_ms / 1000.0
            testcase = ET.SubElement(
                testsuite,
                "testcase",
                name=result.name,
                time=f"{duration_s:.3f}",
                status="passed" if result.passed else "failed",
            )

            # Add proof element (truncate if too long)
            if result.proof:
                proof_text = result.proof
                if len(proof_text) > 2000:
                    proof_text = proof_text[:2000] + "\n... (truncated)"
                proof_elem = ET.SubElement(testcase, "proof")
                proof_elem.text = proof_text

            if not result.passed:
                ET.SubElement(testcase, "failure", message=result.message)

        # Write XML file
        tree = ET.ElementTree(testsuites)
        ET.indent(tree, space="  ")

        # Add XML declaration and stylesheet
        with open(output_file, "w") as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            f.write('<?xml-stylesheet type="text/css" href="report.css"?>\n')
            tree.write(f, encoding="unicode")

        self.logger.info(f"Generated report: {output_file}")

    def _generate_report_css(self) -> None:
        """Generate CSS file for XML report styling."""
        css_content = '''/* E2E Test Report Styles */
testsuites {
  display: block;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  max-width: 900px;
  margin: 2rem auto;
  padding: 1rem;
}

testsuite {
  display: block;
  background: #f8f9fa;
  border-radius: 8px;
  padding: 1.5rem;
  margin-bottom: 1rem;
  box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}

testsuite::before {
  content: attr(name) " - " attr(tests) " tests, " attr(failures) " failures, " attr(time) "s";
  display: block;
  font-size: 1.25rem;
  font-weight: 600;
  margin-bottom: 1rem;
  padding-bottom: 0.75rem;
  border-bottom: 2px solid #dee2e6;
}

testcase {
  display: block;
  padding: 0.75rem 1rem;
  margin: 0.5rem 0;
  border-radius: 4px;
  border-left: 4px solid #28a745;
  background: #d4edda;
}

testcase::before {
  content: "\\2713 " attr(name) " (" attr(time) "s)";
  font-weight: 500;
}

testcase[status="failed"] {
  border-left-color: #dc3545;
  background: #f8d7da;
}

testcase[status="failed"]::before {
  content: "\\2717 " attr(name) " (" attr(time) "s)";
  color: #721c24;
}

failure {
  display: block;
  margin-top: 0.5rem;
  padding: 0.5rem;
  background: rgba(0,0,0,0.05);
  border-radius: 4px;
  font-size: 0.9rem;
  color: #721c24;
}

failure::before {
  content: attr(message);
}

proof {
  display: block;
  margin-top: 0.5rem;
  padding: 0.5rem;
  background: #e9ecef;
  border-radius: 4px;
  font-size: 0.85rem;
  font-family: "SF Mono", Monaco, "Courier New", monospace;
  white-space: pre-wrap;
  word-break: break-all;
  max-height: 200px;
  overflow: auto;
  color: #495057;
}

proof::before {
  content: "Response: ";
  font-weight: 600;
  color: #6c757d;
}
'''
        css_file = self.report_dir / "report.css"
        css_file.write_text(css_content)
        self.logger.info(f"Generated CSS: {css_file}")

    def _get_git_commit(self) -> str:
        """Get the current git commit hash.

        First checks GIT_COMMIT environment variable (set by e2e-test.sh when
        running in a container), then falls back to running git directly.
        """
        # Check environment variable first (container environment)
        env_commit = os.environ.get("GIT_COMMIT")
        if env_commit and env_commit != "unknown":
            return env_commit

        # Fall back to running git directly
        try:
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                capture_output=True,
                text=True,
                cwd=SCRIPT_DIR.parent,
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception:
            pass
        return "unknown"

    def _get_git_commit_short(self) -> str:
        """Get the short git commit hash."""
        commit = self._get_git_commit()
        return commit[:7] if commit != "unknown" else "unknown"

    def _load_previous_reports(self, limit: int = 10) -> list[dict]:
        """Load previous report summaries for comparison."""
        reports_base = self.report_dir.parent
        previous_reports = []

        # Get all report directories sorted by name (timestamp) descending
        report_dirs = sorted(
            [d for d in reports_base.iterdir() if d.is_dir() and d.name != "latest" and d != self.report_dir],
            key=lambda d: d.name,
            reverse=True,
        )[:limit]

        for report_dir in report_dirs:
            summary_file = report_dir / "summary.json"
            if summary_file.exists():
                try:
                    with open(summary_file) as f:
                        summary = json.load(f)
                        previous_reports.append(summary)
                except Exception:
                    pass
            else:
                # Parse XML reports for legacy directories
                summary = self._parse_xml_reports(report_dir)
                if summary:
                    previous_reports.append(summary)

        return previous_reports

    def _parse_xml_reports(self, report_dir: Path) -> dict | None:
        """Parse XML reports from a directory to extract summary info."""
        xml_files = list(report_dir.glob("report-*.xml"))
        if not xml_files:
            return None

        summary: dict = {
            "timestamp": report_dir.name,
            "commit": "unknown",
            "backends": {},
        }

        for xml_file in xml_files:
            backend = xml_file.stem.replace("report-", "")
            try:
                tree = ET.parse(xml_file)
                root = tree.getroot()
                testsuite = root.find("testsuite")
                if testsuite is not None:
                    summary["backends"][backend] = {
                        "total": int(testsuite.get("tests", "0")),
                        "failed": int(testsuite.get("failures", "0")),
                        "passed": int(testsuite.get("tests", "0")) - int(testsuite.get("failures", "0")),
                        "duration_ms": int(float(testsuite.get("time", "0")) * 1000),
                    }
            except Exception:
                pass

        return summary if summary["backends"] else None

    def _generate_summary_report(self, suites: list[TestSuite]) -> None:
        """Generate HTML summary report with comparison to previous runs."""
        commit = self._get_git_commit()
        commit_short = self._get_git_commit_short()
        timestamp = self.report_dir.name
        previous_reports = self._load_previous_reports()

        # Build current summary data with per-test results
        current_summary = {
            "timestamp": timestamp,
            "commit": commit,
            "backends": {},
        }

        for suite in suites:
            tests = {}
            for result in suite.results:
                tests[result.name] = {
                    "passed": result.passed,
                    "duration_ms": result.duration_ms,
                }
            current_summary["backends"][suite.backend] = {
                "total": suite.total,
                "passed": suite.passed,
                "failed": suite.failed,
                "duration_ms": suite.total_duration_ms,
                "tests": tests,
            }

        # Save JSON summary for future comparisons
        summary_file = self.report_dir / "summary.json"
        with open(summary_file, "w") as f:
            json.dump(current_summary, f, indent=2)
        self.logger.info(f"Generated summary JSON: {summary_file}")

        # Get previous report for comparison (most recent)
        prev_report = previous_reports[0] if previous_reports else None

        # Load previous per-test data from XML if not in JSON
        prev_tests = self._load_previous_test_details(prev_report)

        # Generate HTML summary
        html = self._build_summary_html(suites, commit, commit_short, timestamp, prev_report, previous_reports, prev_tests)

        html_file = self.report_dir / "summary.html"
        with open(html_file, "w") as f:
            f.write(html)
        self.logger.info(f"Generated summary report: {html_file}")

    def _load_previous_test_details(self, prev_report: dict | None) -> dict[str, dict[str, dict]]:
        """Load per-test details from previous report (JSON or XML fallback)."""
        prev_tests: dict[str, dict[str, dict]] = {}
        if not prev_report:
            return prev_tests

        # Check if JSON has per-test data
        for backend, data in prev_report.get("backends", {}).items():
            if "tests" in data:
                prev_tests[backend] = data["tests"]

        # If we have data for all backends, return it
        if prev_tests:
            return prev_tests

        # Fallback: parse XML reports from previous run
        prev_timestamp = prev_report.get("timestamp")
        if not prev_timestamp:
            return prev_tests

        prev_dir = self.report_dir.parent / prev_timestamp
        if not prev_dir.exists():
            return prev_tests

        for xml_file in prev_dir.glob("report-*.xml"):
            backend = xml_file.stem.replace("report-", "")
            try:
                tree = ET.parse(xml_file)
                root = tree.getroot()
                tests = {}
                for testcase in root.iter("testcase"):
                    name = testcase.get("name", "")
                    time_s = float(testcase.get("time", "0"))
                    passed = testcase.get("status") == "passed"
                    tests[name] = {
                        "passed": passed,
                        "duration_ms": int(time_s * 1000),
                    }
                prev_tests[backend] = tests
            except Exception:
                pass

        return prev_tests

    def _build_summary_html(
        self,
        suites: list[TestSuite],
        commit: str,
        commit_short: str,
        timestamp: str,
        prev_report: dict | None,
        history: list[dict],
        prev_tests: dict[str, dict[str, dict]],
    ) -> str:
        """Build the HTML content for the summary report."""
        total_passed = sum(s.passed for s in suites)
        total_failed = sum(s.failed for s in suites)
        total_tests = sum(s.total for s in suites)
        total_duration = sum(s.total_duration_ms for s in suites)

        # Build backend rows
        backend_rows = []
        for suite in suites:
            duration_s = suite.total_duration_ms / 1000.0
            status_class = "pass" if suite.failed == 0 else "fail"
            status_icon = "&#x2713;" if suite.failed == 0 else "&#x2717;"

            # Comparison with previous
            comparison = ""
            if prev_report and suite.backend in prev_report.get("backends", {}):
                prev = prev_report["backends"][suite.backend]
                prev_duration = prev.get("duration_ms", 0)
                duration_diff = suite.total_duration_ms - prev_duration

                if duration_diff > 0:
                    comparison = f'<span class="slower">+{duration_diff}ms</span>'
                elif duration_diff < 0:
                    comparison = f'<span class="faster">{duration_diff}ms</span>'

                prev_failed = prev.get("failed", 0)
                if suite.failed != prev_failed:
                    diff = suite.failed - prev_failed
                    if diff > 0:
                        comparison += f' <span class="worse">+{diff} failures</span>'
                    else:
                        comparison += f' <span class="better">{abs(diff)} fewer failures</span>'

            backend_rows.append(f'''
            <tr class="{status_class}">
                <td><span class="status-icon">{status_icon}</span> {suite.backend}</td>
                <td>{suite.passed}</td>
                <td>{suite.failed}</td>
                <td>{suite.total}</td>
                <td>{duration_s:.2f}s {comparison}</td>
            </tr>''')

        # Build detailed test comparison table
        # Collect all unique test names across all backends
        all_test_names: list[str] = []
        seen_names: set[str] = set()
        for suite in suites:
            for result in suite.results:
                if result.name not in seen_names:
                    all_test_names.append(result.name)
                    seen_names.add(result.name)

        # Build per-test data for each backend
        backend_test_data: dict[str, dict[str, TestResult]] = {}
        for suite in suites:
            backend_test_data[suite.backend] = {r.name: r for r in suite.results}

        # Build header row with backend names
        backend_names = [s.backend for s in suites]
        detail_header = "<th>Test</th>" + "".join(f"<th>{b}</th>" for b in backend_names)

        # Build detail rows
        detail_rows = []
        for test_name in all_test_names:
            cells = [f"<td>{test_name}</td>"]
            for backend in backend_names:
                result = backend_test_data.get(backend, {}).get(test_name)
                if result is None:
                    cells.append('<td class="na">—</td>')
                else:
                    status_class = "pass" if result.passed else "fail"
                    status_icon = "✓" if result.passed else "✗"
                    duration_ms = result.duration_ms

                    # Compare with previous
                    delta = ""
                    prev_backend_tests = prev_tests.get(backend, {})
                    if test_name in prev_backend_tests:
                        prev_data = prev_backend_tests[test_name]
                        prev_duration = prev_data.get("duration_ms", 0)
                        diff = duration_ms - prev_duration
                        if diff > 50:  # Only show significant changes (>50ms)
                            delta = f' <span class="slower">+{diff}ms</span>'
                        elif diff < -50:
                            delta = f' <span class="faster">{diff}ms</span>'

                    cells.append(f'<td class="{status_class}"><span class="icon">{status_icon}</span> {duration_ms}ms{delta}</td>')

            detail_rows.append(f"<tr>{''.join(cells)}</tr>")

        details_section = f'''
        <details class="test-details">
            <summary>Detailed Test Results ({len(all_test_names)} tests)</summary>
            <table class="details-table">
                <thead>
                    <tr>{detail_header}</tr>
                </thead>
                <tbody>
                    {"".join(detail_rows)}
                </tbody>
            </table>
        </details>'''

        # Build history table
        history_rows = []
        for report in history[:5]:
            report_commit = report.get("commit", "unknown")[:7]
            report_ts = report.get("timestamp", "unknown")
            backends_info = []
            for backend, data in report.get("backends", {}).items():
                status = "pass" if data.get("failed", 0) == 0 else "fail"
                backends_info.append(f'<span class="{status}">{backend}: {data.get("passed", 0)}/{data.get("total", 0)}</span>')

            history_rows.append(f'''
            <tr>
                <td><code>{report_commit}</code></td>
                <td>{report_ts}</td>
                <td>{" | ".join(backends_info)}</td>
            </tr>''')

        history_section = ""
        if history_rows:
            history_section = f'''
        <details class="history-details">
            <summary>Recent History ({len(history_rows)} runs)</summary>
            <table class="history-table">
                <thead>
                    <tr>
                        <th>Commit</th>
                        <th>Timestamp</th>
                        <th>Results</th>
                    </tr>
                </thead>
                <tbody>
                    {"".join(history_rows)}
                </tbody>
            </table>
        </details>'''

        overall_status = "PASSED" if total_failed == 0 else "FAILED"
        overall_class = "pass" if total_failed == 0 else "fail"

        return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>E2E Test Summary - {timestamp}</title>
    <style>
        :root {{
            --bg: #1a1a2e;
            --card-bg: #16213e;
            --text: #e8e8e8;
            --text-dim: #8a8a9a;
            --pass: #4ade80;
            --fail: #f87171;
            --faster: #4ade80;
            --slower: #fbbf24;
            --border: #2a2a4e;
        }}
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            background: var(--bg);
            color: var(--text);
            line-height: 1.6;
            padding: 2rem;
        }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        h1 {{ margin-bottom: 0.5rem; }}
        h2 {{ margin: 2rem 0 1rem; color: var(--text-dim); font-size: 1rem; text-transform: uppercase; letter-spacing: 0.1em; }}
        .meta {{ color: var(--text-dim); margin-bottom: 2rem; }}
        .meta code {{ background: var(--card-bg); padding: 0.2em 0.5em; border-radius: 4px; font-size: 0.9em; }}
        .overall {{
            display: inline-block;
            padding: 0.5rem 1.5rem;
            border-radius: 8px;
            font-weight: 600;
            font-size: 1.2rem;
            margin-bottom: 2rem;
        }}
        .overall.pass {{ background: rgba(74, 222, 128, 0.2); color: var(--pass); }}
        .overall.fail {{ background: rgba(248, 113, 113, 0.2); color: var(--fail); }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: var(--card-bg);
            border-radius: 8px;
            overflow: hidden;
        }}
        th, td {{ padding: 0.75rem 1rem; text-align: left; }}
        th {{ background: rgba(0,0,0,0.2); font-weight: 500; color: var(--text-dim); }}
        tr {{ border-bottom: 1px solid var(--border); }}
        tr:last-child {{ border-bottom: none; }}
        tr.pass td:first-child {{ color: var(--pass); }}
        tr.fail td:first-child {{ color: var(--fail); }}
        .status-icon {{ margin-right: 0.5rem; }}
        .faster {{ color: var(--faster); font-size: 0.85em; margin-left: 0.5rem; }}
        .slower {{ color: var(--slower); font-size: 0.85em; margin-left: 0.5rem; }}
        .better {{ color: var(--faster); font-size: 0.85em; margin-left: 0.5rem; }}
        .worse {{ color: var(--fail); font-size: 0.85em; margin-left: 0.5rem; }}
        .summary-stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }}
        .stat {{
            background: var(--card-bg);
            padding: 1rem;
            border-radius: 8px;
            text-align: center;
        }}
        .stat-value {{ font-size: 2rem; font-weight: 600; }}
        .stat-label {{ color: var(--text-dim); font-size: 0.85rem; }}
        .stat-value.pass {{ color: var(--pass); }}
        .stat-value.fail {{ color: var(--fail); }}
        details {{
            margin: 1.5rem 0;
            background: var(--card-bg);
            border-radius: 8px;
            overflow: hidden;
        }}
        summary {{
            padding: 1rem;
            cursor: pointer;
            font-weight: 500;
            background: rgba(0,0,0,0.2);
            user-select: none;
        }}
        summary:hover {{ background: rgba(0,0,0,0.3); }}
        details[open] summary {{ border-bottom: 1px solid var(--border); }}
        .details-table {{ border-radius: 0; }}
        .details-table td {{ font-size: 0.9em; padding: 0.5rem 0.75rem; }}
        .details-table td.pass {{ color: var(--pass); }}
        .details-table td.fail {{ color: var(--fail); }}
        .details-table td.na {{ color: var(--text-dim); }}
        .details-table .icon {{ margin-right: 0.25rem; }}
        .history-table {{ margin-top: 0; border-radius: 0; }}
        .history-table .pass {{ color: var(--pass); }}
        .history-table .fail {{ color: var(--fail); }}
    </style>
</head>
<body>
    <div class="container">
        <h1>E2E Test Summary</h1>
        <p class="meta">
            <strong>Timestamp:</strong> {timestamp}<br>
            <strong>Commit:</strong> <code>{commit_short}</code> ({commit})
        </p>

        <div class="overall {overall_class}">{overall_status}</div>

        <div class="summary-stats">
            <div class="stat">
                <div class="stat-value">{total_tests}</div>
                <div class="stat-label">Total Tests</div>
            </div>
            <div class="stat">
                <div class="stat-value pass">{total_passed}</div>
                <div class="stat-label">Passed</div>
            </div>
            <div class="stat">
                <div class="stat-value {"fail" if total_failed > 0 else ""}">{total_failed}</div>
                <div class="stat-label">Failed</div>
            </div>
            <div class="stat">
                <div class="stat-value">{total_duration / 1000:.1f}s</div>
                <div class="stat-label">Duration</div>
            </div>
        </div>

        <h2>Results by Backend</h2>
        <table>
            <thead>
                <tr>
                    <th>Backend</th>
                    <th>Passed</th>
                    <th>Failed</th>
                    <th>Total</th>
                    <th>Duration</th>
                </tr>
            </thead>
            <tbody>
                {"".join(backend_rows)}
            </tbody>
        </table>

        {details_section}

        {history_section}
    </div>
</body>
</html>'''

    def run(self, backends: list[str]) -> int:
        """Run tests for specified backends."""
        self.logger.info("ADMapper E2E Test Suite")
        self.logger.info("=" * 23)

        if not self.check_prerequisites():
            return 1

        # Set up reports directory
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self._generate_report_css()

        suites: list[TestSuite] = []
        overall_failed = False

        for backend in backends:
            suite = self.run_backend_tests(backend)
            suites.append(suite)
            if suite.failed > 0:
                overall_failed = True

        # Generate summary report
        self._generate_summary_report(suites)

        total_passed = sum(s.passed for s in suites)
        total_failed = sum(s.failed for s in suites)

        # Print overall summary
        print()
        print("=" * 34)
        print("Overall Summary")
        print("=" * 34)
        print(f"Commit: {self._get_git_commit_short()}")
        print(f"Total passed: {total_passed}")
        print(f"Total failed: {total_failed}")
        print()
        print(f"Reports generated in: {self.report_dir}")
        for report in sorted(self.report_dir.glob("*")):
            print(f"  - {report.name}")
        print()

        if overall_failed:
            log_fail(self.logger, "Some backends failed!")
            return 1
        else:
            log_pass(self.logger, "All backends passed!")
            return 0


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="E2E Test Runner for ADMapper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Backends:
  all      - Test all backends
  crustdb  - Test CrustDB backend only
  neo4j    - Test Neo4j backend only
  falkordb - Test FalkorDB backend only

Environment variables:
  ADMAPPER_BIN  - Path to admapper binary
  DEBUG         - Enable debug output
  NEO4J_HOST    - Neo4j host (default: localhost)
  NEO4J_PORT    - Neo4j port (default: 7687)
  NEO4J_USER    - Neo4j user (default: neo4j)
  NEO4J_PASSWORD - Neo4j password (default: neo4j123)
  FALKORDB_HOST - FalkorDB host (default: localhost)
  FALKORDB_PORT - FalkorDB port (default: 6379)
        """,
    )
    parser.add_argument(
        "test_data",
        type=Path,
        help="Path to BloodHound data zip file",
    )
    parser.add_argument(
        "backend",
        nargs="?",
        default="all",
        choices=["all"] + BACKENDS,
        help="Backend to test (default: all)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=bool(os.environ.get("DEBUG")),
        help="Enable debug output",
    )

    args = parser.parse_args()

    # Determine backends to test
    if args.backend == "all":
        backends = BACKENDS
    else:
        backends = [args.backend]

    # Find admapper binary
    project_root = SCRIPT_DIR.parent
    admapper_bin = Path(
        os.environ.get(
            "ADMAPPER_BIN",
            project_root / "src" / "backend" / "target" / "release" / "admapper",
        )
    )

    # Set up report directory with timestamp
    reports_base = SCRIPT_DIR / "reports"
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_dir = reports_base / timestamp
    report_dir.mkdir(parents=True, exist_ok=True)

    # Update "latest" symlink
    latest_link = reports_base / "latest"
    if latest_link.is_symlink():
        latest_link.unlink()
    elif latest_link.exists():
        # If it's a regular file/dir, remove it
        if latest_link.is_dir():
            shutil.rmtree(latest_link)
        else:
            latest_link.unlink()
    latest_link.symlink_to(timestamp)

    runner = E2ETestRunner(
        test_data=args.test_data,
        admapper_bin=admapper_bin,
        report_dir=report_dir,
        debug=args.debug,
    )

    return runner.run(backends)


if __name__ == "__main__":
    sys.exit(main())
