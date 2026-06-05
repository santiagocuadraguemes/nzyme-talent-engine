# =============================================================================
# deploy-sam.ps1 — Infrastructure + code deploy for the Nzyme Talent Engine via AWS SAM.
#
# This is the IaC-managed successor to deploy.ps1. It:
#   1. Builds package/ with manylinux2014_x86_64 / cp311 wheels (NO Docker — identical
#      cross-platform dependency strategy as deploy.ps1).
#   2. Runs `sam deploy` against template.yaml, which updates BOTH code and infrastructure
#      on the CloudFormation-managed stack "nzyme-talent-engine".
#   3. Injects the NoEcho secret parameters + safety toggles from the gitignored
#      params/prod.json (so secrets are never committed or re-typed).
#
# PREREQUISITE: the stack must already exist (created via the one-time resource IMPORT —
# see import/IMPORT_PLAN.md). Running this before the import would attempt to CREATE a
# function named nzyme-talent-management, which already exists, and fail safely.
#
# SAFETY: params/prod.json sets ManageFunctionUrl=false and ManageEventPermissions=false,
# so this deploy NEVER touches the live Function URL or its permissions.
# =============================================================================

$ErrorActionPreference = "Stop"
$region    = "eu-west-1"
$stackName = "nzyme-talent-engine"

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

# Drop deploy scripts + caches from the package
Remove-Item -Force package/scripts/deploy.ps1 -ErrorAction SilentlyContinue
Remove-Item -Force package/scripts/deploy.sh -ErrorAction SilentlyContinue
Remove-Item -Force package/scripts/deploy-sam.ps1 -ErrorAction SilentlyContinue
Get-ChildItem -Path package -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force

# --- 3. Build secret parameter overrides from gitignored params/prod.json ---
$paramFile = "params/prod.json"
if (-not (Test-Path $paramFile)) {
    Write-Host "ERROR: $paramFile not found. Copy params/prod.example.json to $paramFile and fill in secrets."
    exit 1
}
$overrides = (Get-Content $paramFile -Raw | ConvertFrom-Json) |
    ForEach-Object { "$($_.ParameterKey)=$($_.ParameterValue)" }

# --- 4. Deploy (code + infra). samconfig.toml supplies region/bucket/capabilities. ---
# --no-confirm-changeset: skip the interactive "Deploy this changeset? [y/N]" prompt so this
#   script runs unattended (samconfig.toml keeps confirm_changeset=true as a gate for anyone
#   running raw `sam deploy` by hand; this flag overrides it for the script path only).
# --no-fail-on-empty-changeset: a re-deploy with no changes exits 0 instead of erroring.
Write-Host "Deploying stack '$stackName' via SAM (no Docker)..."
sam deploy `
    --template-file template.yaml `
    --stack-name $stackName `
    --region $region `
    --parameter-overrides $overrides `
    --no-confirm-changeset `
    --no-fail-on-empty-changeset `
    @args

if ($LASTEXITCODE -ne 0) { Write-Host "sam deploy failed!"; exit 1 }
Write-Host "Deploy complete. Verify CodeSha256 + that the Function URL is unchanged."
