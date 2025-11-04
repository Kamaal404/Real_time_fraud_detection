# Complete Banking Analytics System

Write-Host ''
Write-Host 'Complete Banking Fraud Detection & Analytics System'
Write-Host '========================================================='
Write-Host ''
Write-Host 'System Components:'
Write-Host '  - Kafka Message Streaming'
Write-Host '  - Real-time Fraud Detection'
Write-Host '  - ClickHouse Data Warehouse'
Write-Host '  - Customer Segmentation ML'
Write-Host '  - Analytics Dashboard'
Write-Host '  - Automated Reporting'
Write-Host ''

# Check if system is set up
if (-not (Test-Path 'trans.csv')) {
    Write-Host 'trans.csv not found! Please copy your transaction data file to this directory.'
    Write-Host ''
    Write-Host "Expected location: $(Get-Location)\trans.csv"
    Pause
    exit 1
}

if (-not (Test-Path 'venv')) {
    Write-Host 'Virtual environment not found! Please run setup first.'
    Write-Host ''
    Write-Host 'Run this command first: setup_complete_system.ps1'
    Pause
    exit 1
}

Write-Host 'Data file found: trans.csv'
Write-Host 'Virtual environment found'

# Activate virtual environment
Write-Host 'Activating Python environment...'
& 'venv\Scripts\Activate.ps1'

# Check for required Python files
$pythonFiles = @('kafka_fraud_detection.py', 'clickhouse_integration.py', 'customer_segmentation.py', 'complete_fraud_system.py')

foreach ($file in $pythonFiles) {
    if (-not (Test-Path $file)) {
        Write-Host "$file not found! Please make sure all Python files are in this directory."
        Pause
        exit 1
    }
}

Write-Host 'All Python files found'

# Check for required models and train if needed
if (-not (Test-Path 'fraud_model.pkl')) {
    Write-Host 'Fraud model not found. Training now...'
    python model_trainer.py
    if ($LASTEXITCODE -ne 0) {
        Write-Host 'Failed to train fraud model'
        Pause
        exit 1
    }
    Write-Host 'Fraud model trained successfully'
} else {
    Write-Host 'Fraud model found'
}

# Check for docker-compose file
if (-not (Test-Path 'docker-compose-full.yml')) {
    Write-Host 'docker-compose-full.yml not found! This file is required to start the infrastructure.'
    Pause
    exit 1
}

Write-Host 'Docker compose configuration found'

# Start infrastructure
Write-Host ''
Write-Host 'Starting system infrastructure...'
Write-Host '  - Zookeeper'
Write-Host '  - Kafka'
Write-Host '  - ClickHouse Database'
Write-Host '  - Kafka UI'
Write-Host '  - ClickHouse UI'
Write-Host ''

docker-compose -f docker-compose-full.yml up -d

# Wait for services to be ready
Write-Host 'Waiting for services to be ready (45 seconds)...'
Start-Sleep -Seconds 45

# Check if Docker containers are running
Write-Host 'Checking service status...'
docker ps --format 'table {{.Names}}`t{{.Status}}' | Select-String -Pattern 'kafka|clickhouse|zookeeper'

# Create Kafka topics
Write-Host ''
Write-Host 'Creating Kafka topics...'
docker exec kafka kafka-topics --create --topic transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>$null
if ($LASTEXITCODE -eq 0) { Write-Host 'Created transactions topic' } else { Write-Host 'Transactions topic may already exist' }

docker exec kafka kafka-topics --create --topic fraud-alerts --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>$null
if ($LASTEXITCODE -eq 0) { Write-Host 'Created fraud-alerts topic' } else { Write-Host 'Fraud-alerts topic may already exist' }

docker exec kafka kafka-topics --create --topic customer-segments --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>$null
if ($LASTEXITCODE -eq 0) { Write-Host 'Created customer-segments topic' } else { Write-Host 'Customer-segments topic may already exist' }

# Test ClickHouse connection
Write-Host ''
Write-Host 'Testing ClickHouse connection...'
try {
    Invoke-WebRequest -Uri 'http://localhost:8123/ping' -UseBasicParsing -ErrorAction Stop
    Write-Host 'ClickHouse is responding'
} catch {
    Write-Host 'ClickHouse may still be starting up...'
    Write-Host 'Waiting additional 15 seconds...'
    Start-Sleep -Seconds 15
}

# Test Kafka connection
Write-Host 'Testing Kafka connection...'
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 >$null 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host 'Kafka is responding'
} else {
    Write-Host 'Kafka is not responding properly'
    Write-Host ''
    Write-Host 'Troubleshooting:'
    Write-Host '1. Make sure Docker Desktop is running'
    Write-Host '2. Wait a bit longer for services to start'
    Write-Host '3. Check docker logs: docker logs kafka'
    Pause
    exit 1
}

# System ready message
Write-Host ''
Write-Host 'Infrastructure Ready!'
Write-Host '====================='
Write-Host ''
Write-Host 'Available Interfaces:'
Write-Host '  - Kafka UI:           http://localhost:8080'
Write-Host '  - ClickHouse UI:      http://localhost:8081'
Write-Host '  - ClickHouse HTTP:    http://localhost:8123'
Write-Host '  - Analytics Dashboard: analytics_dashboard.html (open in browser)'
Write-Host ''
Write-Host 'Data Flow:'
Write-Host '  CSV Data → Kafka → Fraud Detection → ClickHouse → Analytics'
Write-Host ''

Write-Host 'Starting Complete Analytics System...'
Write-Host ''
Write-Host 'The system will now:'
Write-Host '  1. Stream transactions from your CSV data'
Write-Host '  2. Detect fraud in real-time using ML'
Write-Host '  3. Store all data in ClickHouse warehouse'
Write-Host '  4. Perform customer segmentation analysis'
Write-Host '  5. Generate analytics and insights'
Write-Host ''
Write-Host 'IMPORTANT: Open analytics_dashboard.html in your browser for live dashboard!'
Write-Host ''
Write-Host 'Press Ctrl+C to stop the system'
Write-Host ''
Write-Host 'Starting main system now...'
Write-Host ''

# Start the complete system
python complete_fraud_system.py

# Cleanup when system stops
Write-Host ''
Write-Host 'System stopped. Cleaning up...'
Write-Host ''
Write-Host 'Stopping Docker containers...'
docker-compose -f docker-compose-full.yml down

Write-Host ''
Write-Host 'Cleanup complete'
Write-Host ''
Write-Host 'Thank you for using the Complete Banking Analytics System!'
Write-Host ''
Pause
