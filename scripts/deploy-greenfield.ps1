# =============================================================================
# deploy-greenfield.ps1 — Fresh-account deploy of the Nzyme Talent Engine.
#
# Use this to stand the stack up in an account that has NO pre-existing Function
# URL / permissions (the "greenfield" case from template.yaml's header notes).
# It was written for the 2026 migration to org account 047719630984 (see MIGRATION.md),
# but works for any clean account.
#
# Differences vs. scripts/deploy-sam.ps1 (the adopted-prod path):
#   * Does NOT require the SAM CLI. Uses `aws cloudformation package` + `deploy`,
#     which process the AWS::Serverless transform server-side.
#   * Webhook ingress = an API Gateway HTTP API (resource WebhookHttpApi in template.yaml),
#     NOT a Lambda Function URL. Public Function URLs are blocked org-wide in the Kibo
#     account (an SCP/RCP 403s them at the auth layer), so ManageFunctionUrl=false here and
#     the HTTP API — always created by the template — is the real entry point. Its payload
#     format 2.0 matches the Function URL event shape, so no app code changes were needed.
#   * ManageEventPermissions=true -> CloudFormation CREATES the EventBridge invoke permissions.
#   * ExecutionRoleArn reuses the org's shared pass-able Lambda role (see below).
#   * DeployBucketName overridden -> the original name is globally unique and already taken.
#
# !!! ManageEventPermissions MUST stay true on every deploy to this account (the EventBridge
# !!! schedules can't invoke the function without those permissions). And do NOT delete or
# !!! replace the WebhookHttpApi — a new HTTP API gets a new {id}.execute-api host, which
# !!! breaks every Notion automation. Normal in-place `cloudformation deploy` is safe.
# =============================================================================

$ErrorActionPreference = "Stop"

# ---- Target (override via env vars if reused for another account) ----
$region          = if ($env:NZYME_REGION)        { $env:NZYME_REGION }        else { "eu-west-1" }
$stackName       = if ($env:NZYME_STACK)         { $env:NZYME_STACK }         else { "nzyme-talent-engine" }
$profile         = if ($env:NZYME_PROFILE)       { $env:NZYME_PROFILE }       else { "nzyme-new" }
# Existing bucket used purely to upload the code artifact for `cloudformation package`.
$artifactBucket  = if ($env:NZYME_ARTIFACT_BUCKET) { $env:NZYME_ARTIFACT_BUCKET } else { "aws-sam-cli-managed-default-samclisourcebucket-idxj7rxhtd6m" }
# New globally-unique name for the in-stack DeployBucket resource (the old name is taken).
$deployBucket    = if ($env:NZYME_DEPLOY_BUCKET) { $env:NZYME_DEPLOY_BUCKET } else { "nzyme-talent-engine-deploy-047719630984" }
# Execution role: the SSO Developer permission set CANNOT create IAM roles (iam:CreateRole
# denied), so we reuse the org's shared, pass-able Lambda execution role that the other Nzyme
# functions (nzyme-webhook / nzyme-notion-sync / ...) already run on. Leave empty ONLY in an
# account where you actually hold iam:CreateRole (then the template builds a logs-only role).
$execRole        = if ($env:NZYME_EXEC_ROLE_ARN) { $env:NZYME_EXEC_ROLE_ARN } else { "arn:aws:iam::047719630984:role/passable/lambda-execution" }
$packagedFile    = "packaged.yaml"

$awsCommon = @("--profile", $profile, "--region", $region)

# --- 1. Build package/ with manylinux wheels (cached on requirements.txt SHA) ---
$hashFile    = "package/.requirements-hash"
$currentHash = (Get-FileHash requirements.txt -Algorithm SHA256).Hash
$needsInstall = $true

if ((Test-Path package) -and (Test-Path $hashFile)) {
    if ((Get-Content $hashFile) -eq $currentHash) {
        Write-Host "Dependencies unchanged, skipping pip install..."
        $needsInstall = $false
    } else {
        Write-Host "requirements.txt changed, reinstalling dependencies..."
        Remove-Item -Recurse -Force package
    }
}

if ($needsInstall) {
    if (Test-Path package) { Remove-Item -Recurse -Force package }
    pip install -r requirements.txt -t package/ --quiet `
        --platform manylinux2014_x86_64 --only-binary=:all: `
        --implementation cp --python-version 3.11
    if ($LASTEXITCODE -ne 0) { Write-Host "pip install failed"; exit 1 }
    $currentHash | Out-File $hashFile -NoNewline
}

# --- 2. Refresh application code into package/ ---
Remove-Item -Recurse -Force package/core/ -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force package/scripts/ -ErrorAction SilentlyContinue
Remove-Item -Force package/main_lambda.py -ErrorAction SilentlyContinue

Copy-Item main_lambda.py package/
Copy-Item -Recurse core/ package/core/
Copy-Item -Recurse scripts/ package/scripts/

# Drop deploy scripts + the forwarder + caches from the package (not part of the Lambda).
Remove-Item -Force package/scripts/deploy.ps1 -ErrorAction SilentlyContinue
Remove-Item -Force package/scripts/deploy.sh -ErrorAction SilentlyContinue
Remove-Item -Force package/scripts/deploy-sam.ps1 -ErrorAction SilentlyContinue
Remove-Item -Force package/scripts/deploy-greenfield.ps1 -ErrorAction SilentlyContinue
Remove-Item -Force package/scripts/deploy-forwarder.ps1 -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force package/scripts/forwarder/ -ErrorAction SilentlyContinue
Get-ChildItem -Path package -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force

# --- 3. Build secret parameter overrides from gitignored params/prod.json ---
# Take only the 6 NoEcho secrets; the Manage* toggles are set explicitly below (prod.json
# carries them as false for the adopted-prod stack, which is the opposite of greenfield).
$paramFile = "params/prod.json"
if (-not (Test-Path $paramFile)) {
    Write-Host "ERROR: $paramFile not found. Copy params/prod.example.json to $paramFile and fill in secrets."
    exit 1
}
$secretKeys = @("WebhookPathToken","NotionKey","SupabaseKey","OpenAiApiKey","ExaApiKey","LogfireToken")
$secretOverrides = (Get-Content $paramFile -Raw | ConvertFrom-Json) |
    Where-Object { $secretKeys -contains $_.ParameterKey } |
    ForEach-Object { "$($_.ParameterKey)=$($_.ParameterValue)" }

# Greenfield-specific (non-secret) overrides.
$greenfieldOverrides = @(
    "ManageFunctionUrl=false",
    "ManageEventPermissions=true",
    "ExecutionRoleArn=$execRole",
    "DeployBucketName=$deployBucket"
)
$overrides = $greenfieldOverrides + $secretOverrides

# --- 4. Package: zip package/ and upload to the artifact bucket; rewrite CodeUri. ---
Write-Host "Packaging code artifact -> s3://$artifactBucket ..."
aws cloudformation package `
    --template-file template.yaml `
    --s3-bucket $artifactBucket `
    --s3-prefix nzyme-talent-engine `
    --output-template-file $packagedFile `
    @awsCommon
if ($LASTEXITCODE -ne 0) { Write-Host "cloudformation package failed"; exit 1 }

# --- 5. Deploy (code + infra) to the greenfield account. ---
Write-Host "Deploying stack '$stackName' to profile '$profile' ($region)..."
aws cloudformation deploy `
    --template-file $packagedFile `
    --stack-name $stackName `
    --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND `
    --parameter-overrides @overrides `
    --no-fail-on-empty-changeset `
    @awsCommon
if ($LASTEXITCODE -ne 0) { Write-Host "cloudformation deploy failed!"; exit 1 }

# --- 6. Report the public webhook base URL (API Gateway HTTP API). ---
$apiUrl = aws cloudformation describe-stacks --stack-name $stackName @awsCommon `
    --query "Stacks[0].Outputs[?OutputKey=='WebhookHttpApiUrl'].OutputValue | [0]" --output text
Write-Host ""
Write-Host "Deploy complete."
Write-Host "Webhook base URL (API Gateway HTTP API): $apiUrl"
Write-Host "Full webhook URL = <that base> + '/' + WEBHOOK_PATH_TOKEN"
