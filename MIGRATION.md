# AWS Account Migration Runbook (2026-06)

One-time record of moving the Nzyme Talent Engine from the original AWS account to the Kibo
Ventures org account. Companion to `import/IMPORT_PLAN.md` (which adopted the original account
into CloudFormation). External data stores (Notion, Supabase, OpenAI, Exa, Logfire) did **not**
move — only the AWS compute/trigger layer.

## Accounts

| | Old | New (production) |
|---|---|---|
| Account ID | `416418941636` | `047719630984` (Kibo Ventures) |
| Credentials | default AWS CLI (IAM user `nzyme-santiago-IAM`) | AWS CLI profile **`nzyme-new`** (SSO `Developer`, short-lived) |
| Region | `eu-west-1` | `eu-west-1` |
| Role now | **forwarder shim only** (pending decommission) | full app |

## What changed vs. the original stack

1. **Webhook ingress is an API Gateway HTTP API, not a Lambda Function URL.**
   Public Lambda Function URLs are blocked org-wide in the Kibo account — a Function URL with
   `AuthType=NONE` + a public-invoke permission still returns **403** at the auth layer (an
   SCP/RCP). The org's sanctioned pattern (used by `nzyme-webhook`) is an HTTP API. HTTP API
   **payload format 2.0** delivers the same event shape as a Function URL (`rawPath`,
   `requestContext.http`, `headers`), so `main_lambda.verify_path_token` + `WebhookRouter`
   needed **no code changes**. Added to `template.yaml` as `WebhookHttpApi` (+ `HttpApi` events
   on the function); deployed with `ManageFunctionUrl=false` so no (unusable) Function URL exists.
   - **New webhook base:** `https://jlhp10k9w9.execute-api.eu-west-1.amazonaws.com`
   - **Full URL:** that base + `/` + `WEBHOOK_PATH_TOKEN` (token unchanged from the old account).
2. **Execution role is the shared org role** `arn:aws:iam::047719630984:role/passable/lambda-execution`
   (the SSO `Developer` permission set cannot `iam:CreateRole`, so the template's greenfield
   role-creation path is unusable here; we pass the shared role ARN instead).
3. **Deploy without SAM CLI** — SAM CLI isn't installed; `scripts/deploy-greenfield.ps1` uses
   `aws cloudformation package` + `deploy` (the transform runs server-side). Code artifact is
   uploaded to the pre-existing `aws-sam-cli-managed-default-*` bucket; the in-stack deploy bucket
   was renamed `nzyme-talent-engine-deploy-047719630984` (the original name is globally taken).

## Steps performed

1. **Deploy new stack** — `scripts/deploy-greenfield.ps1` (`--profile nzyme-new`, `eu-west-1`):
   `ManageFunctionUrl=false`, `ManageEventPermissions=true`, shared role ARN, new bucket name,
   secrets from `params/prod.json`. Creates function + HTTP API + 3 EventBridge rules + invoke
   permissions + deploy bucket.
2. **Smoke test** — challenge echo `200` with the correct token, `401` with a wrong token, against
   the HTTP API; one `observer` manual invoke (clean run: Supabase + Notion + Logfire all reachable).
3. **Schedule cutover** — disabled the 3 OLD rules (default creds), enabled the 3 NEW rules
   (`nzyme-new`). Exactly one scheduler active at a time.
4. **Forwarder** — `scripts/deploy-forwarder.ps1` (default creds → old account): replaced the old
   `nzyme-talent-management` code with `scripts/forwarder/forwarder.py` (handler
   `forwarder.lambda_handler`, env `FORWARD_BASE_URL` merged in, the 21 original vars preserved).
   The old Function URL now proxies every webhook to the new HTTP API (path/token passed through),
   so Notion automations still pointing at the old URL keep working. Verified: old URL +
   correct token → proxied `200` challenge; + wrong token → proxied `401`; `X-Nzyme-Event` passes.

## Remaining manual step (owner: Santiago)

Update each Notion automation's "Send webhook" URL host to the new base
(`https://jlhp10k9w9.execute-api.eu-west-1.amazonaws.com/<token>`). The token (path) is unchanged,
so it's a host swap only. The forwarder covers any automation not yet updated — no rush, no breakage.

## Decommission (deferred, do only after the new account is confirmed healthy)

In the OLD account (default creds): delete the CloudFormation stack (resources are
`DeletionPolicy: Retain`, so this only stops tracking), then delete the now-forwarder Lambda +
its Function URL + the 3 (disabled) rules + the deploy bucket. Leave until every automation is
confirmed moved and a few days of clean new-account operation have passed.

## Rollback

- New deploy bad → delete the `nzyme-talent-engine` stack in the new account (nothing shared is
  touched) and re-enable the old schedules.
- Need the old Lambda doing real work again → `scripts/deploy.ps1` (default creds) restores the
  real code; then reset the handler: `aws lambda update-function-configuration
  --function-name nzyme-talent-management --region eu-west-1 --handler main_lambda.lambda_handler`.
  The env vars were preserved by the forwarder deploy, so no secret re-entry is needed.
- The migration touched **no** Notion/Supabase data.
