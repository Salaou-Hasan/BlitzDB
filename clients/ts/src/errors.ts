//! Typed SDK errors mapped from server error strings (see PROTOCOL.md
//! "Errors"). Mapping is by documented prefix; unknown strings surface as
//! `Server` verbatim — never silently reclassified.

export type SdkErrorKind =
  | 'Transport' | 'Timeout' | 'Auth' | 'Retryable'
  | 'NotFound' | 'Invalid' | 'Server' | 'Closed';

export class SdkError extends Error {
  public kind: SdkErrorKind;
  constructor(kind: SdkErrorKind, message: string) {
    super(`${kind.toLowerCase()}: ${message}`);
    this.kind = kind;
    this.name = 'SdkError';
  }
}

/** Map a server error payload to a typed error. */
export function mapServerError(msg: string): SdkError {
  if (msg.startsWith('unauthorized') || msg.startsWith('forbidden')) {
    return new SdkError('Auth', msg);
  }
  if (msg.startsWith('WAL backpressure') || msg.startsWith('group full') || msg.startsWith('rotation in progress')) {
    return new SdkError('Retryable', msg);
  }
  if (msg.includes('not found') || msg === 'not found') {
    return new SdkError('NotFound', msg);
  }
  if (
    msg.includes('requires values') || msg.includes('requires row_id') ||
    msg.includes('too large') || msg.includes('DuplicateKey') ||
    msg.includes('duplicate value') || msg.includes('not supported') ||
    msg.includes('not unique') || msg.includes('validation') ||
    msg.includes('schema error') || msg.includes('type error')
  ) {
    return new SdkError('Invalid', msg);
  }
  return new SdkError('Server', msg);
}
