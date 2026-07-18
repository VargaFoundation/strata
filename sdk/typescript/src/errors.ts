import type { EcphoriaApiError } from "./types.js";

/** Error thrown when the Ecphoria API returns an error response. */
export class EcphoriaError extends Error {
  readonly code: string;
  readonly requestId?: string;
  readonly status: number;

  constructor(message: string, code: string, status: number, requestId?: string) {
    super(message);
    this.name = "EcphoriaError";
    this.code = code;
    this.status = status;
    this.requestId = requestId;
  }

  static fromApiError(err: EcphoriaApiError, status: number): EcphoriaError {
    return new EcphoriaError(err.message, err.code, status, err.request_id);
  }
}
