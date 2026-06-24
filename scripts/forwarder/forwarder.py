"""
Webhook forwarder shim — runs in the OLD AWS account (416418941636) after the 2026
migration to org account 047719630984. See MIGRATION.md.

Some Notion automations still hardcode the OLD Lambda Function URL
(https://vi6n7zvmytou7djtx7ixmobc4e0ittqz.lambda-url.eu-west-1.on.aws/...). Rather than
rely on every automation being updated by hand, the old Lambda's application code is
replaced with this thin proxy: every HTTP request that lands on the old Function URL is
re-issued verbatim to the NEW account's webhook endpoint (the API Gateway HTTP API), and
the response is returned unchanged. Because we kept the SAME WEBHOOK_PATH_TOKEN, the path
(which carries the token) passes straight through and the new account's path-token gate
accepts it.

EventBridge schedule invokes ({"task": ...}) are ignored — the new account owns scheduling
now (the old schedules are disabled at cutover; if any fires, this no-ops it).

Zero third-party dependencies (stdlib only), so the deployment zip is tiny.

Config: env var FORWARD_BASE_URL = the new webhook base, e.g.
  https://jlhp10k9w9.execute-api.eu-west-1.amazonaws.com
"""
import base64
import os
import urllib.error
import urllib.request

FORWARD_BASE_URL = os.environ.get("FORWARD_BASE_URL", "").rstrip("/")


def lambda_handler(event, context):
    # EventBridge schedule ({"task": ...}) — handled by the new account now. No-op.
    if isinstance(event, dict) and "task" in event:
        return {"statusCode": 200, "body": "forwarder: schedule ignored (new account owns scheduling)"}

    if not FORWARD_BASE_URL:
        return {"statusCode": 500, "body": "forwarder misconfigured: FORWARD_BASE_URL unset"}

    # Function URL / HTTP API payload v2.0 shape (rawPath carries the /<token> segment).
    raw_path = event.get("rawPath") or "/"
    query_string = event.get("rawQueryString") or ""
    method = (event.get("requestContext", {}).get("http", {}) or {}).get("method", "POST")

    url = FORWARD_BASE_URL + raw_path
    if query_string:
        url += "?" + query_string

    # Body may be base64-encoded by the Function URL.
    body = event.get("body")
    data = None
    if body is not None:
        data = base64.b64decode(body) if event.get("isBase64Encoded") else body.encode("utf-8")

    # Forward only safe headers: Content-Type + any X-Nzyme-* (esp. X-Nzyme-Event, which the
    # new account needs to tell created-vs-edit apart). Drop Host/Content-Length/auth so the
    # HTTP client + the new host set them correctly.
    out_headers = {}
    for key, value in (event.get("headers") or {}).items():
        lower = key.lower()
        if lower == "content-type" or lower.startswith("x-nzyme-"):
            out_headers[key] = value
    out_headers.setdefault("Content-Type", "application/json")

    request = urllib.request.Request(url=url, data=data, method=method, headers=out_headers)
    try:
        with urllib.request.urlopen(request, timeout=250) as resp:
            return {"statusCode": resp.status, "body": resp.read().decode("utf-8", "replace")}
    except urllib.error.HTTPError as exc:
        # Propagate the new account's status/body (e.g. a 401 from the path-token gate).
        return {"statusCode": exc.code, "body": exc.read().decode("utf-8", "replace")}
    except Exception as exc:  # network / timeout — let Notion retry.
        return {"statusCode": 502, "body": "forwarder error: " + str(exc)}
