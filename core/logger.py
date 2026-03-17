import logging
import os
import sys
import uuid

# Module-level request ID, set per invocation by set_request_id()
_request_id = "NO_REQ"


def set_request_id(request_id=None):
    """Set the request ID for the current Lambda invocation.
    Call at the start of each lambda_handler with context.aws_request_id."""
    global _request_id
    _request_id = request_id or uuid.uuid4().hex[:8]
    return _request_id


class _RequestIdFilter(logging.Filter):
    """Injects the current request_id into every log record."""
    def filter(self, record):
        record.request_id = _request_id
        return True


def get_logger(name):
    """
    Configures and returns a logger adapted for AWS Lambda (CloudWatch).
    - Does not write to files (avoids Read-only file system errors).
    - Writes to stdout (Console), which CloudWatch captures automatically.
    - Log level is defined by the LOG_LEVEL environment variable.
    - Each log line includes a request ID to distinguish Lambda invocations
      that share the same CloudWatch log stream (warm starts).
    """
    logger = logging.getLogger(name)

    # Read level from environment. Default: INFO.
    # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    logger.setLevel(log_level)

    # Avoid duplicating handlers if called multiple times (Lambda reuses contexts)
    if logger.hasHandlers():
        return logger

    formatter = logging.Formatter(
        '[%(levelname)s] [%(name)s] [%(request_id)s] %(message)s'
    )

    # Console Handler only
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)

    logger.addHandler(console_handler)
    logger.addFilter(_RequestIdFilter())

    return logger
