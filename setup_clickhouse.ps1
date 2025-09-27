Write-Host "Setting up ClickHouse Data Warehouse Integration" -ForegroundColor Green
Write-Host "===================================================="

# Check if Docker is running
try {
    docker ps | Out-Null
    Write-Host "Docker is running" -ForegroundColor Green
} catch {
    Write-Host "Docker is not running. Please start Docker Desktop first." -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

# Create directories
@("clickhouse-config", "clickhouse-init") | ForEach-Object {
    if (!(Test-Path $_)) {
        New-Item -ItemType Directory -Path $_ | Out-Null
    }
}

Write-Host "Created ClickHouse directories" -ForegroundColor Cyan

# Create ClickHouse config
$clickhouseConfig = @"
<clickhouse>
    <logger>
        <level>information</level>
        <console>true</console>
    </logger>
    <query_log>
        <database>system</database>
        <table>query_log</table>
    </query_log>
</clickhouse>
"@

$clickhouseConfig | Out-File -FilePath "clickhouse-config\config.xml" -Encoding UTF8

Write-Host "Created ClickHouse configuration" -ForegroundColor Green

# Stop existing containers
Write-Host "Stopping existing containers..." -ForegroundColor Yellow
docker-compose down 2>$null

# Start full stack
Write-Host "Starting Kafka + ClickHouse stack..." -ForegroundColor Cyan
docker-compose -f docker-compose-full.yml up -d

Write-Host "Waiting for services to start (60 seconds)..." -ForegroundColor Yellow
Start-Sleep -Seconds 60

# Install Python dependencies
Write-Host "Installing ClickHouse Python dependencies..." -ForegroundColor Cyan
& .\venv\Scripts\Activate.ps1
pip install clickhouse-connect pandas matplotlib seaborn

# Create Kafka topics
Write-Host "Creating Kafka topics..." -ForegroundColor Cyan
docker exec kafka kafka-topics --create --topic transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>$null
docker exec kafka kafka-topics --create --topic fraud-alerts --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>$null

# Test ClickHouse
Write-Host "Testing ClickHouse connection..." -ForegroundColor Cyan
try {
    $response = Invoke-WebRequest -Uri "http://localhost:8123/ping" -UseBasicParsing
    Write-Host "ClickHouse is responding" -ForegroundColor Green
} catch {
    Write-Host "ClickHouse may still be starting up" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "ClickHouse Data Warehouse Setup Complete!" -ForegroundColor Green
Write-Host "==============================================="
Write-Host ""
Write-Host "Available Services:" -ForegroundColor Yellow
Write-Host "  - Kafka UI:        http://localhost:8080"
Write-Host "  - ClickHouse HTTP: http://localhost:8123"  
Write-Host "  - ClickHouse UI:   http://localhost:8081"
Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Yellow
Write-Host "  1. Run: python clickhouse_integration.py"
Write-Host "  2. Run: python kafka_fraud_detection.py stream"
Write-Host "  3. Run: python kafka_fraud_detection.py detect"
Write-Host "  4. Run: python analytics_examples.py"
Write-Host ""
Read-Host "Press Enter to continue"
