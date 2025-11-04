# setup_complete_system.ps1 - PowerShell setup script
Write-Host ""
Write-Host "Setting up Complete Banking Analytics System" -ForegroundColor Green
Write-Host "==============================================="
Write-Host ""

# Check prerequisites
Write-Host "Checking prerequisites..." -ForegroundColor Cyan

try {
    $pythonVersion = python --version 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "SUCCESS: Python found - $pythonVersion" -ForegroundColor Green
    } else {
        throw "Python not found"
    }
} catch {
    Write-Host "ERROR: Python not found in PATH" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please install Python 3.8+ from https://python.org" -ForegroundColor Yellow
    Write-Host "Make sure to check 'Add to PATH' during installation!" -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 1
}

try {
    $dockerVersion = docker --version 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "SUCCESS: Docker found - $dockerVersion" -ForegroundColor Green
    } else {
        throw "Docker not found"
    }
} catch {
    Write-Host "ERROR: Docker not found" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please install Docker Desktop from:" -ForegroundColor Yellow
    Write-Host "https://docker.com/products/docker-desktop" -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 1
}

try {
    docker ps | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "SUCCESS: Docker is running" -ForegroundColor Green
    } else {
        throw "Docker not running"
    }
} catch {
    Write-Host "ERROR: Docker is not running" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please start Docker Desktop and try again." -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host ""
Write-Host "Prerequisites check passed!" -ForegroundColor Green
Write-Host ""

# Check for transaction data
if (!(Test-Path "trans.csv")) {
    Write-Host "WARNING: trans.csv not found in current directory" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Please copy your transaction data file (trans.csv) to:" -ForegroundColor Yellow
    Write-Host (Get-Location).Path -ForegroundColor White
    Write-Host ""
    $choice = Read-Host "Continue without trans.csv? (y/n)"
    if ($choice -ne "y") {
        Write-Host ""
        Write-Host "Please copy your trans.csv file and run setup again." -ForegroundColor Yellow
        Read-Host "Press Enter to exit"
        exit 1
    }
} else {
    Write-Host "SUCCESS: Transaction data file found - trans.csv" -ForegroundColor Green
}

# Create virtual environment
Write-Host ""
Write-Host "Setting up Python environment..." -ForegroundColor Cyan

if (Test-Path "venv") {
    Write-Host "Virtual environment already exists. Removing old one..." -ForegroundColor Yellow
    Remove-Item -Recurse -Force "venv"
}

python -m venv venv
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to create virtual environment" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host "SUCCESS: Virtual environment created" -ForegroundColor Green

# Activate virtual environment
& .\venv\Scripts\Activate.ps1

# Create requirements.txt
Write-Host "Creating requirements file..." -ForegroundColor Cyan
$requirements = @"
kafka-python>=2.0.0
pandas>=2.0.0
numpy>=1.24.0
scikit-learn>=1.3.0
clickhouse-connect>=0.6.0

matplotlib>=3.7.0
seaborn>=0.12.0
plotly>=5.15.0

schedule>=1.2.0
python-dotenv>=1.0.0

flask>=2.3.0
flask-cors>=4.0.0

setuptools>=65.0.0
wheel>=0.40.0
"@

$requirements | Out-File -FilePath "requirements.txt" -Encoding UTF8
Write-Host "SUCCESS: Requirements file created" -ForegroundColor Green

# Install Python packages
Write-Host ""
Write-Host "Installing Python packages..." -ForegroundColor Cyan
Write-Host "This may take a few minutes..." -ForegroundColor Yellow

pip install --upgrade pip
pip install -r requirements.txt

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to install Python packages" -ForegroundColor Red
    Write-Host ""
    Write-Host "Try running this command manually:" -ForegroundColor Yellow
    Write-Host "pip install -r requirements.txt" -ForegroundColor White
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host "SUCCESS: Python packages installed successfully" -ForegroundColor Green

# Create directories
Write-Host ""
Write-Host "Creating directories..." -ForegroundColor Cyan
@("clickhouse-config", "clickhouse-init") | ForEach-Object {
    if (!(Test-Path $_)) {
        New-Item -ItemType Directory -Path $_ | Out-Null
    }
}
Write-Host "SUCCESS: Directories created" -ForegroundColor Green

# Create ClickHouse configuration
Write-Host "Creating ClickHouse configuration..." -ForegroundColor Cyan
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
Write-Host "SUCCESS: ClickHouse configuration created" -ForegroundColor Green

# Create docker-compose file
Write-Host "Creating Docker Compose configuration..." -ForegroundColor Cyan
$dockerCompose = @"
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks:
      - fraud-detection-network

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    hostname: kafka
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
    networks:
      - fraud-detection-network

  clickhouse:
    image: clickhouse/clickhouse-server:23.8
    hostname: clickhouse
    container_name: clickhouse
    ports:
      - "8123:8123"
      - "9000:9000"
    environment:
      CLICKHOUSE_DB: fraud_detection
      CLICKHOUSE_USER: admin
      CLICKHOUSE_PASSWORD: password
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: 1
    volumes:
      - clickhouse_data:/var/lib/clickhouse
      - ./clickhouse-config:/etc/clickhouse-server/config.d
      - ./clickhouse-init:/docker-entrypoint-initdb.d
    networks:
      - fraud-detection-network

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
    networks:
      - fraud-detection-network

  tabix:
    image: spoonest/clickhouse-tabix-web-client
    container_name: clickhouse-ui
    ports:
      - "8081:80"
    depends_on:
      - clickhouse
    networks:
      - fraud-detection-network

volumes:
  clickhouse_data:

networks:
  fraud-detection-network:
    driver: bridge
"@

$dockerCompose | Out-File -FilePath "docker-compose-full.yml" -Encoding UTF8
Write-Host "SUCCESS: Docker Compose configuration created" -ForegroundColor Green

# Download Docker images
Write-Host ""
Write-Host "Downloading Docker images (this may take several minutes)..." -ForegroundColor Cyan
$download = Read-Host "Download images now? (y/n)"
if ($download -eq "y") {
    Write-Host "Downloading images..." -ForegroundColor Yellow
    docker pull confluentinc/cp-zookeeper:7.4.0
    docker pull confluentinc/cp-kafka:7.4.0
    docker pull clickhouse/clickhouse-server:23.8
    docker pull provectuslabs/kafka-ui:latest
    docker pull spoonest/clickhouse-tabix-web-client
    Write-Host "SUCCESS: Docker images downloaded" -ForegroundColor Green
} else {
    Write-Host "SKIPPED: Docker image download" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Setup Complete!" -ForegroundColor Green
Write-Host "==================="
Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Yellow
Write-Host "1. Make sure you have all Python files in this directory"
Write-Host "2. Copy your trans.csv file here if you haven't already"
Write-Host "3. Run: .\run_complete_system.bat"
Write-Host ""
Write-Host "Available after running:" -ForegroundColor Cyan
Write-Host "  - Kafka UI:           http://localhost:8080"
Write-Host "  - ClickHouse UI:      http://localhost:8081"
Write-Host "  - Analytics Dashboard: analytics_dashboard.html"
Write-Host ""
Read-Host "Press Enter to continue"