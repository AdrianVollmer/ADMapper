/**
 * Centralized API Client
 *
 * Provides consistent error handling and response parsing for all API calls.
 */

import type { ApiError } from "./types";

/**
 * Custom error class for API errors.
 * Contains the HTTP status code and error message.
 */
export class ApiClientError extends Error {
  constructor(
    public readonly status: number,
    message: string
  ) {
    super(message);
    this.name = "ApiClientError";
  }

  toApiError(): ApiError {
    return {
      status: this.status,
      message: this.message,
    };
  }
}

/**
 * Centralized API client for making HTTP requests.
 * All methods throw ApiClientError on failure.
 * All methods accept an optional AbortSignal for request cancellation.
 */
export class ApiClient {
  /**
   * Make a GET request and parse JSON response.
   * @param url - The URL to fetch
   * @param signal - Optional AbortSignal for cancellation
   * @throws {ApiClientError} If the request fails or response is not OK
   */
  async get<T>(url: string, signal?: AbortSignal): Promise<T> {
    const response = await fetch(url, { signal: signal ?? null });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new ApiClientError(response.status, text || response.statusText || `HTTP ${response.status}`);
    }

    return response.json();
  }

  /**
   * Make a POST request with JSON body.
   * @param url - The URL to post to
   * @param body - The request body (will be JSON stringified)
   * @param signal - Optional AbortSignal for cancellation
   * @throws {ApiClientError} If the request fails or response is not OK
   */
  async post<T>(url: string, body: unknown, signal?: AbortSignal): Promise<T> {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: signal ?? null,
    });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new ApiClientError(response.status, text || response.statusText || `HTTP ${response.status}`);
    }

    return response.json();
  }

  /**
   * Make a PUT request with JSON body.
   * @param url - The URL to put to
   * @param body - The request body (will be JSON stringified)
   * @param signal - Optional AbortSignal for cancellation
   * @throws {ApiClientError} If the request fails or response is not OK
   */
  async put<T>(url: string, body: unknown, signal?: AbortSignal): Promise<T> {
    const response = await fetch(url, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: signal ?? null,
    });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new ApiClientError(response.status, text || response.statusText || `HTTP ${response.status}`);
    }

    return response.json();
  }

  /**
   * Make a POST request with JSON body, expecting no content response.
   * @param url - The URL to post to
   * @param body - Optional request body (will be JSON stringified)
   * @param signal - Optional AbortSignal for cancellation
   * @throws {ApiClientError} If the request fails or response is not OK
   */
  async postNoContent(url: string, body?: unknown, signal?: AbortSignal): Promise<void> {
    const init: RequestInit = {
      method: "POST",
      headers: body ? { "Content-Type": "application/json" } : {},
      signal: signal ?? null,
    };
    if (body !== undefined) {
      init.body = JSON.stringify(body);
    }
    const response = await fetch(url, init);

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new ApiClientError(response.status, text || response.statusText || `HTTP ${response.status}`);
    }
  }

  /**
   * Make a DELETE request.
   * @param url - The URL to delete
   * @param signal - Optional AbortSignal for cancellation
   * @throws {ApiClientError} If the request fails or response is not OK
   */
  async delete(url: string, signal?: AbortSignal): Promise<void> {
    const response = await fetch(url, { method: "DELETE", signal: signal ?? null });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new ApiClientError(response.status, text || response.statusText || `HTTP ${response.status}`);
    }
  }

  /**
   * Upload files using multipart form data.
   * @param url - The URL to upload to
   * @param files - The files to upload
   * @param signal - Optional AbortSignal for cancellation
   * @throws {ApiClientError} If the request fails or response is not OK
   */
  async uploadFiles<T>(url: string, files: FileList | File[], signal?: AbortSignal): Promise<T> {
    const formData = new FormData();
    for (const file of files) {
      formData.append("files", file);
    }

    const response = await fetch(url, {
      method: "POST",
      body: formData,
      signal: signal ?? null,
    });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new ApiClientError(response.status, text || response.statusText || `HTTP ${response.status}`);
    }

    return response.json();
  }
}

/** Singleton API client instance */
export const api = new ApiClient();
