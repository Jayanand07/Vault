# Vault: Local Bootstrap & Cluster Runner
param(
    [ValidateSet("single", "cluster")]
    [string]$Mode = "single",
    [int]$Nodes = 3
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Resolve-Path "$ScriptDir\.."

Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "         VAULT DISTRIBUTED OBJECT STORAGE         " -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "Target Mode: $Mode (Nodes: $(if ($Mode -eq 'single') { 1 } else { $Nodes }))" -ForegroundColor Yellow

# 1. Check Docker availability for cluster mode
$dockerAvailable = $false
try {
    $null = docker --version 2>$null
    $null = docker ps 2>$null
    if ($LASTEXITCODE -eq 0) { $dockerAvailable = $true }
} catch {
    $dockerAvailable = $false
}

if ($Mode -eq "cluster") {
    if ($dockerAvailable) {
        Write-Host "[*] Docker daemon detected. Spinning up 3-node cluster + PostgreSQL + Grafana..." -ForegroundColor Green
        Set-Location $ProjectRoot
        docker compose up -d --build
        Write-Host "`n[SUCCESS] Cluster running!" -ForegroundColor Green
        Write-Host "  - Node 1:        http://localhost:8081" -ForegroundColor White
        Write-Host "  - Node 2:        http://localhost:8082" -ForegroundColor White
        Write-Host "  - Node 3:        http://localhost:8083" -ForegroundColor White
        Write-Host "  - Web Console:   http://localhost:8081/console.html" -ForegroundColor Cyan
        Write-Host "  - Grafana:       http://localhost:3000 (use password from .env)" -ForegroundColor White
        Write-Host "  - Prometheus:    http://localhost:9090" -ForegroundColor White
        exit 0
    } else {
        Write-Host "[!] Docker daemon is not running or accessible on this machine." -ForegroundColor Yellow
        Write-Host "    Falling back to single-node standalone process..." -ForegroundColor Yellow
        $Mode = "single"
    }
}

# 2. Single-Node Standalone Mode
Write-Host "[*] Running single-node standalone mode..." -ForegroundColor Cyan

# Locate Maven
$mvnCmd = "$ProjectRoot\apache-maven-3.9.6\bin\mvn.cmd"
if (-not (Test-Path $mvnCmd)) {
    $mvnCmd = "mvn"
}

# Build JAR
Write-Host "[*] Compiling Vault binaries..." -ForegroundColor Yellow
Set-Location $ProjectRoot
& $mvnCmd test-compile -q
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Compilation failed." -ForegroundColor Red
    exit 1
}

$jarPath = "$ProjectRoot\vault-node\target\vault-node-1.0-SNAPSHOT.jar"
if (-not (Test-Path $jarPath)) {
    Write-Host "[*] Packaging vault-node JAR..." -ForegroundColor Yellow
    & $mvnCmd package -DskipTests -q
}

# Ensure data directory exists
$dataDir = "$ProjectRoot\data\node-1"
New-Item -ItemType Directory -Force -Path $dataDir | Out-Null

Write-Host "`n==================================================" -ForegroundColor Green
Write-Host " Vault Node Ready to Launch (DEV PROFILE ONLY)!" -ForegroundColor Green
Write-Host "==================================================" -ForegroundColor Green
Write-Host "  - Address:       http://localhost:8080" -ForegroundColor White
Write-Host "  - Web Console:   http://localhost:8080/console.html" -ForegroundColor Cyan
Write-Host "  - S3 Endpoint:   http://localhost:8080/s3" -ForegroundColor White
Write-Host "  - Profile:       dev (set VAULT_API_KEYS / VAULT_NODE_SECRET / DB_PASSWORD to override)" -ForegroundColor Yellow
Write-Host "  - Data Directory: $dataDir" -ForegroundColor White
Write-Host "`nStarting Java process (Press Ctrl+C to stop)..." -ForegroundColor White

$env:VAULT_NODE_ID = "11111111-1111-1111-1111-111111111111"
$env:VAULT_NODE_ADDRESS = "localhost:8080"
$env:VAULT_DATA_DIR = $dataDir
$env:SPRING_PROFILES_ACTIVE = "dev"

java -jar $jarPath
