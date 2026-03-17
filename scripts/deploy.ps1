Write-Host "Building lambda.zip..."

# Clean previous zip
if (Test-Path lambda.zip) { Remove-Item -Force lambda.zip }

# --- Dependency caching: only reinstall when requirements.txt changes ---
$hashFile = "package/.requirements-hash"
$currentHash = (Get-FileHash requirements.txt -Algorithm SHA256).Hash
$needsInstall = $true

if ((Test-Path package) -and (Test-Path $hashFile)) {
    $cachedHash = Get-Content $hashFile
    if ($cachedHash -eq $currentHash) {
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

    # Save hash for next run
    $currentHash | Out-File $hashFile -NoNewline
}

# --- Always refresh application code ---
# Remove old app code (but keep dependencies intact)
Remove-Item -Recurse -Force package/core/ -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force package/scripts/ -ErrorAction SilentlyContinue
Remove-Item -Force package/main_lambda.py -ErrorAction SilentlyContinue

# Copy fresh application code
Copy-Item main_lambda.py package/
Copy-Item -Recurse core/ package/core/
Copy-Item -Recurse scripts/ package/scripts/

# Remove deploy scripts from the zip (not needed in Lambda)
Remove-Item -Force package/scripts/deploy.ps1 -ErrorAction SilentlyContinue
Remove-Item -Force package/scripts/deploy.sh -ErrorAction SilentlyContinue

# Remove __pycache__ directories
Get-ChildItem -Path package -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force

# Create zip
Compress-Archive -Path package/* -DestinationPath lambda.zip

# Show result
$size = (Get-Item lambda.zip).Length / 1MB
Write-Host ("lambda.zip created ({0:N1} MB)" -f $size)

# --- Upload to AWS Lambda via S3 (direct upload times out at ~46MB) ---
$functionName = "nzyme-talent-management"
$s3Bucket = "nzyme-talent-engine-deploy"
$s3Key = "lambda.zip"

Write-Host "Uploading to S3: s3://$s3Bucket/$s3Key..."
aws s3 cp lambda.zip "s3://$s3Bucket/$s3Key"
if ($LASTEXITCODE -ne 0) { Write-Host "S3 upload failed!"; exit 1 }

Write-Host "Updating Lambda: $functionName..."
aws lambda update-function-code `
  --function-name $functionName `
  --s3-bucket $s3Bucket `
  --s3-key $s3Key `
  --output text --query 'LastModified'

if ($LASTEXITCODE -ne 0) {
    Write-Host "Lambda update failed!"
    exit 1
}

Write-Host "Deploy complete!"
