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
 */
export class ApiClient {
  /**
   * Make a GET request and parse JSON response.
   * @throws {ApiClientError} If the request fails or response is not OK
   */
  async get<T>(url: string): Promise<T> {
    const response = await fetch(url);

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new ApiClientError(
        response.status,
        text || response.statusText || `HTTP ${response.status}`
      );
    }

    return response.json();
  }

  /**
   * Make a POST request with JSON body.
   * @throws {ApiClientError} If the request fails or response is not OK
   */
  async post<T>(url: string, body: unknown): Promise<T> {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new ApiClientError(
        response.status,
        text || response.statusText || `HTTP ${response.status}`
      );
    }

    return response.json();
  }

  /**
   * Make a POST request with JSON body, expecting no content response.
   * @throws {ApiClientError} If the request fails or response is not OK
   */
  async postNoContent(url: string, body?: unknown): Promise<void> {
    const response = await fetch(url, {
      method: "POST",
      headers: body ? { "Content-Type": "application/json" } : {},
      body: body ? JSON.stringify(body) : undefined,
    });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new ApiClientError(
        response.status,
        text || response.statusText || `HTTP ${response.status}`
      );
    }
  }

  /**
   * Make a DELETE request.
   * @throws {ApiClientError} If the request fails or response is not OK
   */
  async delete(url: string): Promise<void> {
    const response = await fetch(url, { method: "DELETE" });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new ApiClientError(
        response.status,
        text || response.statusText || `HTTP ${response.status}`
      );
    }
  }

  /**
   * Upload files using multipart form data.
   * @throws {ApiClientError} If the request fails or response is not OK
   */
  async uploadFiles<T>(url: string, files: FileList | File[]): Promise<T> {
    const formData = new FormData();
    for (const file of files) {
      formData.append("files", file);
    }

    const response = await fetch(url, {
      method: "POST",
      body: formData,
    });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new ApiClientError(
        response.status,
        text || response.statusText || `HTTP ${response.status}`
      );
    }

    return response.json();
  }
}

/** Singleton API client instance */
export const api = new ApiClient();
