"""Typed SDK errors mapped from server error strings (see PROTOCOL.md
"Errors"). Mapping is by documented prefix; unknown strings surface as
``Server`` verbatim — never silently reclassified.
"""


class SdkError(Exception):
    """Typed client error with a machine-readable ``kind``."""

    AUTH = "Auth"
    RETRYABLE = "Retryable"
    NOT_FOUND = "NotFound"
    INVALID = "Invalid"
    SERVER = "Server"
    TRANSPORT = "Transport"
    TIMEOUT = "Timeout"
    CLOSED = "Closed"

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(f"{kind.lower()}: {message}")
        self.kind = kind
        self.detail = message


def map_server_error(msg: str) -> SdkError:
    if msg.startswith("unauthorized") or msg.startswith("forbidden"):
        return SdkError(SdkError.AUTH, msg)
    if (msg.startswith("WAL backpressure") or msg.startswith("group full")
            or msg.startswith("rotation in progress")):
        return SdkError(SdkError.RETRYABLE, msg)
    if "not found" in msg or msg == "not found":
        return SdkError(SdkError.NOT_FOUND, msg)
    if (("requires values" in msg) or ("requires row_id" in msg)
            or ("too large" in msg) or ("DuplicateKey" in msg)
            or ("duplicate value" in msg) or ("not supported" in msg)
            or ("not unique" in msg) or ("validation" in msg)
            or ("schema error" in msg) or ("type error" in msg)):
        return SdkError(SdkError.INVALID, msg)
    return SdkError(SdkError.SERVER, msg)
