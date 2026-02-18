#!/usr/bin/env python3
"""
E2E Test Runner for ADMapper

Runs integration tests against all supported database backends.

Usage:
    ./e2e/run_tests.py <test_data.zip> [backend]

Arguments:
    test_data.zip - Path to BloodHound data zip file (required)
    backend       - Backend to test: cozo, crustdb, neo4j, falkordb, or all (default: all)

Environment variables:
    ADMAPPER_BIN  - Path to admapper binary (default: target/release/admapper)
    DEBUG         - Enable debug output
"""

from __future__ import annotations

import argparse
import atexit
import logging
import os
import random
import shutil
import signal
import subprocess
import sys
import tempfile
from datetime import datetime
from dataclasses import dataclass, field
from pathlib import Path
from types import FrameType

# Add lib directory to path
SCRIPT_DIR = Path(__file__).parent.resolve()
sys.path.insert(0, str(SCRIPT_DIR / "lib"))

from api import APIClient, ServerProcess, start_server, stop_server, wait_for_server  # noqa: E402  # type: ignore[import-not-found]
from runner import TestRunner, TestResult  # noqa: E402  # type: ignore[import-not-found]

# Available backends (cozo disabled - not working)
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
        )

        # Run each test module
        test_modules = [
            ("Health Check", runner.test_health),
            ("Import Data", runner.test_import),
            ("Graph Stats", runner.test_stats),
            ("Query Execution", runner.test_queries),
            ("Search", runner.test_search),
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
        if backend == "cozo":
            return f"cozo://{db_dir}"
        elif backend == "crustdb":
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
        import xml.etree.ElementTree as ET
        from datetime import datetime

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

    def run(self, backends: list[str]) -> int:
        """Run tests for specified backends."""
        self.logger.info("ADMapper E2E Test Suite")
        self.logger.info("=" * 23)

        if not self.check_prerequisites():
            return 1

        # Set up reports directory
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self._generate_report_css()

        total_passed = 0
        total_failed = 0
        overall_failed = False

        for backend in backends:
            suite = self.run_backend_tests(backend)
            total_passed += suite.passed
            total_failed += suite.failed
            if suite.failed > 0:
                overall_failed = True

        # Print overall summary
        print()
        print("=" * 34)
        print("Overall Summary")
        print("=" * 34)
        print(f"Total passed: {total_passed}")
        print(f"Total failed: {total_failed}")
        print()
        print(f"Reports generated in: {self.report_dir}")
        for report in self.report_dir.glob("*.xml"):
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
  cozo     - Test CozoDB backend only
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
