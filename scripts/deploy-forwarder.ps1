# =============================================================================
# deploy-forwarder.ps1 — Turn the OLD account's Lambda into a webhook forwarder.
#
# Replaces the old `nzyme-talent-management` application code with scripts/forwarder/forwarder.py,
# which proxies every incoming webhook to the NEW account's API Gateway HTTP API. Run AFTER the
# new account is live and the schedules have been cut over (see MIGRATION.md).
#
# Uses DEFAULT AWS credentials (the old-account IAM user nzyme-santiago-IAM, 416418941636) — NOT
# the nzyme-new profile. It asserts the account id before doing anything.
#
# Env vars are MERGED (not wiped): the existing 21 vars are preserved and FORWARD_BASE_URL is
# added, so rollback is just "redeploy real code via deploy.ps1 + reset --handler".
#
# ROLLBACK: powershell -File scripts/deploy.ps1   (restores real code, env still intact)
#           aws lambda update-function-configuration --function-name nzyme-talent-management `
#               --region eu-west-1 --handler main_lambda.lambda_handler
# =============================================================================

$ErrorActionPreference = "Stop"
$region      = "eu-west-1"
$fn          = "nzyme-talent-management"
$oldAccount  = "416418941636"
$forwardBase = if ($env:NZYME_FORWARD_BASE_URL) { $env:NZYME_FORWARD_BASE_URL } else { "https://jlhp10k9w9.execute-api.eu-west-1.amazonaws.com" }

# --- 0. Safety: confirm DEFAULT creds point at the OLD account. ---
$acct = aws sts get-caller-identity --query Account --output text
if ($acct -ne $oldAccount) {
    Write-Host "ERROR: default creds are account '$acct', expected OLD account $oldAccount. Aborting so we never overwrite the new function."
    exit 1
}
Write-Host "Target: $fn in OLD account $acct. Forwarding to: $forwardBase"

# --- 1. Zip just forwarder.py (flat -> handler resolves as forwarder.lambda_handler). ---
$zip = Join-Path $env:TEMP "nzyme-forwarder.zip"
if (Test-Path $zip) { Remove-Item $zip -Force }
Compress-Archive -Path scripts/forwarder/forwarder.py -DestinationPath $zip -Force

# --- 2. Replace the function code. ---
Write-Host "Updating function code..."
aws lambda update-function-code --function-name $fn --region $region --zip-file "fileb://$zip" | Out-Null
if ($LASTEXITCODE -ne 0) { Write-Host "update-function-code failed"; exit 1 }
aws lambda wait function-updated --function-name $fn --region $region

# --- 3. Merge env (GET -> add FORWARD_BASE_URL -> PUT) and point handler at the forwarder. ---
Write-Host "Merging FORWARD_BASE_URL into env + switching handler..."
$envJson = aws lambda get-function-configuration --function-name $fn --region $region --query "Environment.Variables" --output json
$current = $envJson | ConvertFrom-Json
$vars = @{}
if ($current) { $current.PSObject.Properties | ForEach-Object { $vars[$_.Name] = $_.Value } }
$vars["FORWARD_BASE_URL"] = $forwardBase

$envFile = Join-Path $env:TEMP "nzyme-fwd-env.json"
(@{ Variables = $vars } | ConvertTo-Json -Depth 6 -Compress) | Out-File -FilePath $envFile -Encoding ascii -NoNewline
try {
    aws lambda update-function-configuration --function-name $fn --region $region `
        --handler forwarder.lambda_handler `
        --environment "file://$envFile" | Out-Null
    if ($LASTEXITCODE -ne 0) { Write-Host "update-function-configuration failed"; exit 1 }
    aws lambda wait function-updated --function-name $fn --region $region
} finally {
    Remove-Item $envFile -Force -ErrorAction SilentlyContinue  # contains secrets
}

Write-Host ""
Write-Host "Forwarder live in OLD account $acct."
Write-Host "Old Function URL now proxies all webhooks to: $forwardBase"
