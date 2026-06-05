# Resource Import Plan — Adopting Live Prod into CloudFormation

This is the one-time runbook to bring the **existing, manually-created** Nzyme Talent Engine
resources under CloudFormation/SAM management **without recreating or replacing them**.

## What gets imported vs. what stays out-of-band

| Resource | Physical ID | Imported? | Why |
|---|---|---|---|
| Lambda function | `nzyme-talent-management` | ✅ Yes | Importable; never replaced by in-place updates |
| EventBridge rule | `nzyme-factory-schedule` | ✅ Yes | Importable |
| EventBridge rule | `nzyme-harvester-schedule` | ✅ Yes | Importable |
| EventBridge rule | `nzyme-observer-schedule` | ✅ Yes | Importable |
| S3 deploy bucket | `nzyme-talent-engine-deploy` | ✅ Yes | Importable |
| **Lambda Function URL** | `vi6n7zvmytou7djtx7ixmobc4e0ittqz...` | ❌ **No** | `AWS::Lambda::Url` not safely importable; delete+recreate changes the host → breaks every Notion webhook |
| **Lambda permissions** (URL public + 3 EventBridge invoke) | resource policy statements | ❌ **No** | `AWS::Lambda::Permission` is **not importable at all** |
| IAM execution role | `nzyme-talent-management-role-jt70mt88` | ❌ No | Referenced by ARN; left as-is |

The Function URL and permissions remain exactly as they are in the console. Because the
adopted stack deploys with `ManageFunctionUrl=false` and `ManageEventPermissions=false`, no
stack operation can ever touch them. **Constraint #1 (URL host never changes) is guaranteed
by construction.**

## Why two templates

A single SAM-transform IMPORT change-set is impossible: `AWS::Serverless::Function` with
`FunctionUrlConfig`/`Events` macro-expands into `AWS::Lambda::Permission` (not importable) and
`AWS::Lambda::Url` (not safely importable), and CloudFormation rejects an import whose
resource set contains any non-importable type.

- **`import/import-template.yaml`** — plain CloudFormation, only the importable resources, used
  for the import change-set. Logical IDs are identical to `template.yaml`.
- **`template.yaml`** — the SAM template that takes over management afterwards. Because the
  logical IDs match, the imported resources are **updated in place**, never replaced.

---

## Step 0 — Backup (DONE)

Full live config dumped to `infra-backup/live-config-2026-05-29.json` (gitignored; contains
secrets). This is the hand-restore reference.

## Step 1 — Create the IMPORT change-set (non-destructive)

Creating a change-set does **not** modify any resource; it creates a stack in
`REVIEW_IN_PROGRESS` and a change-set you review before executing.

```bash
aws cloudformation create-change-set \
  --region eu-west-1 \
  --stack-name nzyme-talent-engine \
  --change-set-name import-prod-resources \
  --change-set-type IMPORT \
  --resources-to-import file://import/resources-to-import.json \
  --template-body file://import/import-template.yaml \
  --parameters file://params/import.json
```

Notes:
- Use `params/import.json` (the 6 NoEcho secrets only) — the import template has no
  `ManageFunctionUrl`/`ManageEventPermissions` toggles, so `params/prod.json` would be rejected.
- No `--capabilities` needed: the import template contains no IAM resources and no transform.
- The `AWS::Events::Rule` import identifier is **`Arn`** (not `Name`) — already set in
  `resources-to-import.json`.

## Step 2 — Review the change-set

```bash
aws cloudformation describe-change-set \
  --region eu-west-1 \
  --stack-name nzyme-talent-engine \
  --change-set-name import-prod-resources \
  --query 'Changes[].ResourceChange.{Action:Action,Type:ResourceType,LogicalId:LogicalResourceId,PhysicalId:PhysicalResourceId}' \
  --output table
```

Every row must show `Action = Import` (NOT `Add`, `Modify`, or `Remove`). If anything shows
`Add`/`Remove`, **STOP** — do not execute.

## Step 3 — Execute the import (⚠️ REQUIRES EXPLICIT APPROVAL)

```bash
aws cloudformation execute-change-set \
  --region eu-west-1 \
  --stack-name nzyme-talent-engine \
  --change-set-name import-prod-resources

aws cloudformation wait stack-import-complete \
  --region eu-west-1 --stack-name nzyme-talent-engine
```

## Step 4 — Verify, then detect drift

```bash
# Function URL must be byte-for-byte unchanged
aws lambda get-function-url-config --function-name nzyme-talent-management --region eu-west-1

# Crons unchanged
aws events describe-rule --name nzyme-harvester-schedule --region eu-west-1 --query ScheduleExpression

# Drift detection — confirm the template matches reality
aws cloudformation detect-stack-drift --region eu-west-1 --stack-name nzyme-talent-engine
```

## Step 5 — Transition management to the SAM template

From here on, deploy code + infra with:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/deploy-sam.ps1
```

This runs `sam deploy` with `template.yaml` (matching logical IDs → in-place updates only),
keeping `ManageFunctionUrl=false` / `ManageEventPermissions=false`.

## Rollback

The import is reversible: deleting the stack with all resources on `DeletionPolicy: Retain`
leaves every physical resource intact (it only stops CloudFormation tracking them). If a
change-set looks wrong, just `delete-change-set` / `delete-stack` (resources are retained) and
fall back to `scripts/deploy.ps1` (code-only) plus the console. `infra-backup/` holds the full
prior state.
