"""
API client for ADMapper E2E tests.

Provides HTTP client for interacting with the ADMapper backend.
"""

from __future__ import annotations

import json
import logging
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from urllib.parse import urlencode


@dataclass
class APIResponse:
    """Response from an API call."""

    status_code: int
    body: dict[str, Any] | list[Any]
    ok: bool

    @classmethod
    def from_error(cls, status_code: int, message: str) -> APIResponse:
        return cls(status_code=status_code, body={"error": message}, ok=False)


class APIClient:
    """HTTP client for ADMapper API."""

    def __init__(self, host: str = "127.0.0.1", port: int = 9191):
        self.base_url = f"http://{host}:{port}"
        self.timeout = 30

    def _request(
        self,
        method: str,
        endpoint: str,
        data: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> APIResponse:
        """Make an HTTP request."""
        url = f"{self.base_url}{endpoint}"
        timeout = timeout or self.timeout

        headers = {}
        body = None

        if data is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(data).encode("utf-8")

        req = Request(url, data=body, headers=headers, method=method)

        try:
            with urlopen(req, timeout=timeout) as response:
                response_body = response.read().decode("utf-8")
                try:
                    parsed = json.loads(response_body) if response_body else {}
                except json.JSONDecodeError:
                    parsed = {"raw": response_body}
                return APIResponse(
                    status_code=response.status,
                    body=parsed,
                    ok=200 <= response.status < 300,
                )
        except HTTPError as e:
            try:
                error_body = e.read().decode("utf-8")
                parsed = json.loads(error_body) if error_body else {}
            except (json.JSONDecodeError, Exception):
                parsed = {"error": str(e)}
            return APIResponse(
                status_code=e.code,
                body=parsed,
                ok=False,
            )
        except URLError as e:
            return APIResponse.from_error(0, str(e.reason))
        except Exception as e:
            return APIResponse.from_error(0, str(e))

    def get(self, endpoint: str, **kwargs: Any) -> APIResponse:
        """Make a GET request."""
        return self._request("GET", endpoint, **kwargs)

    def post(self, endpoint: str, data: dict[str, Any] | None = None, **kwargs: Any) -> APIResponse:
        """Make a POST request."""
        return self._request("POST", endpoint, data=data, **kwargs)

    # API endpoints

    def health(self) -> APIResponse:
        """Check server health."""
        return self.get("/api/health")

    def db_status(self) -> APIResponse:
        """Get database status."""
        return self.get("/api/database/status")

    def stats(self) -> APIResponse:
        """Get graph statistics."""
        return self.get("/api/graph/stats")

    def detailed_stats(self) -> APIResponse:
        """Get detailed graph statistics."""
        return self.get("/api/graph/detailed-stats")

    def node_types(self) -> APIResponse:
        """Get all node types."""
        return self.get("/api/graph/node-types")

    def edge_types(self) -> APIResponse:
        """Get all edge types."""
        return self.get("/api/graph/edge-types")

    def search(self, query: str, limit: int = 10, node_type: str | None = None) -> APIResponse:
        """Search the graph."""
        params = {"q": query, "limit": str(limit)}
        if node_type:
            params["node_type"] = node_type
        return self.get(f"/api/graph/search?{urlencode(params)}")

    def clear(self) -> APIResponse:
        """Clear the graph."""
        return self.post("/api/graph/clear")

    def import_file(self, file_path: Path) -> APIResponse:
        """
        Import a file via multipart form upload.

        Returns response with job_id for tracking progress.
        """
        import mimetypes
        from urllib.request import Request

        boundary = "----WebKitFormBoundary7MA4YWxkTrZu0gW"

        # Build multipart form data
        filename = file_path.name
        content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"

        with open(file_path, "rb") as f:
            file_data = f.read()

        body = (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
            f"Content-Type: {content_type}\r\n\r\n"
        ).encode("utf-8")
        body += file_data
        body += f"\r\n--{boundary}--\r\n".encode("utf-8")

        headers = {
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        }

        url = f"{self.base_url}/api/import"
        req = Request(url, data=body, headers=headers, method="POST")

        try:
            with urlopen(req, timeout=60) as response:
                response_body = response.read().decode("utf-8")
                parsed = json.loads(response_body) if response_body else {}
                return APIResponse(
                    status_code=response.status,
                    body=parsed,
                    ok=200 <= response.status < 300,
                )
        except HTTPError as e:
            return APIResponse(status_code=e.code, body={}, ok=False)
        except Exception as e:
            return APIResponse.from_error(0, str(e))

    def import_progress(self, job_id: str) -> dict[str, Any] | None:
        """
        Get import progress via SSE endpoint.

        Returns the latest progress event or None if unavailable.
        """
        import socket

        url = f"{self.base_url}/api/import/progress/{job_id}"

        try:
            with urlopen(url, timeout=5) as response:
                # Read SSE data - format is "data: {...}\n\n"
                for line in response:
                    line = line.decode("utf-8").strip()
                    if line.startswith("data: "):
                        try:
                            return json.loads(line[6:])
                        except json.JSONDecodeError:
                            pass
        except Exception:
            pass
        return None

    def wait_for_import(
        self, job_id: str, timeout: int = 300, poll_interval: float = 2.0
    ) -> dict[str, Any] | None:
        """
        Wait for an import to complete.

        Returns the final progress or None if timeout/error.
        """
        elapsed = 0.0
        while elapsed < timeout:
            progress = self.import_progress(job_id)
            if progress:
                status = progress.get("status")
                if status == "completed":
                    return progress
                elif status == "failed":
                    return progress
            time.sleep(poll_interval)
            elapsed += poll_interval
        return None

    def query(self, cypher: str, timeout: int = 60) -> APIResponse:
        """
        Execute a Cypher query (async with SSE progress).

        Returns the query results when complete.
        """
        # Start the query
        response = self.post("/api/graph/query", {"query": cypher})
        if not response.ok:
            return response

        if not isinstance(response.body, dict):
            return APIResponse.from_error(500, "Unexpected response format")

        query_id = response.body.get("query_id")
        if not query_id:
            return APIResponse.from_error(500, "No query_id in response")

        # Wait for completion via SSE
        elapsed = 0.0
        poll_interval = 0.5
        while elapsed < timeout:
            progress = self._get_query_progress(query_id)
            if progress:
                status = progress.get("status")
                if status == "completed":
                    return APIResponse(status_code=200, body=progress, ok=True)
                elif status in ("failed", "aborted"):
                    return APIResponse(
                        status_code=500,
                        body=progress,
                        ok=False,
                    )
            time.sleep(poll_interval)
            elapsed += poll_interval

        return APIResponse.from_error(504, f"Query timeout after {timeout}s")

    def _get_query_progress(self, query_id: str) -> dict[str, Any] | None:
        """Get query progress via SSE endpoint."""
        url = f"{self.base_url}/api/query/progress/{query_id}"
        try:
            with urlopen(url, timeout=5) as response:
                for line in response:
                    line = line.decode("utf-8").strip()
                    if line.startswith("data: "):
                        try:
                            return json.loads(line[6:])
                        except json.JSONDecodeError:
                            pass
        except Exception:
            pass
        return None


class ServerProcess:
    """Wrapper for ADMapper server process with log streaming."""

    def __init__(
        self,
        process: subprocess.Popen[str],
        logger: logging.Logger,
    ):
        self.process = process
        self.logger = logger
        self._stop_event = threading.Event()
        self._log_thread: threading.Thread | None = None

    @property
    def pid(self) -> int:
        return self.process.pid

    def start_log_streaming(self) -> None:
        """Start background thread to stream server logs."""
        self._log_thread = threading.Thread(
            target=self._stream_logs,
            daemon=True,
        )
        self._log_thread.start()

    def _stream_logs(self) -> None:
        """Read and log server output."""
        if self.process.stdout is None:
            return

        for line in self.process.stdout:
            if self._stop_event.is_set():
                break
            line = line.rstrip()
            if line:
                self.logger.info(f"[server] {line}")

    def stop(self) -> None:
        """Stop the server and log streaming."""
        self._stop_event.set()
        self.logger.info(f"Stopping server (PID: {self.process.pid})...")
        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        if self._log_thread:
            self._log_thread.join(timeout=1)


def start_server(
    binary: Path,
    db_url: str,
    port: int,
    logger: logging.Logger,
) -> ServerProcess | None:
    """Start the ADMapper server."""
    logger.info(f"Starting ADMapper on port {port}...")
    logger.info(f"Database URL: {db_url}")

    try:
        process = subprocess.Popen(
            [
                str(binary),
                "--headless",
                "--port", str(port),
                "--bind", "0.0.0.0",
                db_url,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        server = ServerProcess(process, logger)
        server.start_log_streaming()
        return server
    except Exception as e:
        logger.error(f"Failed to start server: {e}")
        return None


def stop_server(server: ServerProcess, logger: logging.Logger) -> None:
    """Stop the ADMapper server."""
    if server:
        server.stop()


def wait_for_server(
    api: APIClient,
    timeout: int = 30,
    logger: logging.Logger | None = None,
) -> bool:
    """Wait for the server to be ready."""
    elapsed = 0
    while elapsed < timeout:
        try:
            response = api.health()
            if response.ok:
                if logger:
                    logger.debug(f"Server ready after {elapsed}s")
                return True
        except Exception:
            pass
        time.sleep(1)
        elapsed += 1

    if logger:
        logger.error(f"Server failed to start after {timeout}s")
    return False
