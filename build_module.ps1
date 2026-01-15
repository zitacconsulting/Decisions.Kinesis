# PowerShell script to build the Decisions Kinesis Module

Write-Host "Building Decisions Kinesis Module" -ForegroundColor Green

# Build the project
Write-Host "Compiling the project..." -ForegroundColor Yellow
dotnet build build.proj

if ($LASTEXITCODE -ne 0) {
    Write-Host "Build failed!" -ForegroundColor Red
    exit 1
}

# Build the module using CreateDecisionsModule
Write-Host "Creating Decisions module package..." -ForegroundColor Yellow
dotnet msbuild build.proj -t:build_module

if ($LASTEXITCODE -ne 0) {
    Write-Host "Module packaging failed!" -ForegroundColor Red
    exit 1
}

Write-Host "Module built successfully!" -ForegroundColor Green
Write-Host "Output: Decisions.Kinesis.zip" -ForegroundColor Cyan
