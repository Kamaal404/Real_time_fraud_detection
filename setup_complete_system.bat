@echo off
title Banking Analytics System Setup

echo.
echo Setting up Complete Banking Analytics System
echo ===============================================
echo.

REM Check prerequisites
echo Checking prerequisites...

python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python not found in PATH
    echo.
    echo Please install Python 3.8+ from https://python.org
    echo Make sure to check "Add to PATH" during installation!
    echo.
    pause
    exit /b 1
)
echo SUCCESS: Python found

docker --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Docker not found
    echo.
    echo Please install Docker Desktop from:
    echo https://docker.com/products/docker-desktop
    echo.
    pause
    exit /b 1
)
echo SUCCESS: Docker found

docker ps >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Docker is not running
    echo.
    echo Please start Docker Desktop and try again.
    echo.
    pause
    exit /b 1
)
echo SUCCESS: Docker is running

echo.
echo Prerequisites check passed!
echo.

REM Check for transaction data
if not exist "trans.csv" (
    echo WARNING: trans.csv not found in current directory
    echo.
    echo Please copy your transaction data file (trans.csv) to:
    echo %CD%
    echo.
    echo The system will work with sample data, but you'll get better
    echo results with your actual banking transaction data.
    echo.
    set /p choice="Continue without trans.csv? (y/n): "
    if /i not "%choice%"=="y" (
        echo.
        echo Please copy your trans.csv file and run setup again.
        pause
        exit /b 1
    )
) else (
    echo SUCCESS: Transaction data file found: trans.csv
)

REM Create virtual environment
echo.
echo Setting up Python environment...

if exist "venv" (
    echo Virtual environment already exists. Removing old one...
    rmdir /s /q venv
)

python -m venv venv
if %errorlevel% neq 0 (
    echo ERROR: Failed to create virtual environment
    pause
    exit /b 1
)

echo SUCCESS: Virtual environment created

REM Activate virtual environment
call venv\Scripts\activate.bat

call venv\Scripts\activate.bat

REM Upgrade pip and install build tools
python -m pip install --upgrade pip
python -m pip install --upgrade --force-reinstall setuptools wheel build

REM Install requirements
pip install -r requirements.txt


if %errorlevel% neq 0 (
    echo ERROR: Failed to install Python packages
    echo.
    echo Try running this command manually:
    echo pip install -r requirements.txt
    echo.
    pause
    exit /b 1
)

echo SUCCESS: Python packages installed successfully

REM Create directories for ClickHouse
echo.
echo Creating directories...
if not exist "clickhouse-config" mkdir clickhouse-config
if not exist "clickhouse-init" mkdir clickhouse-init

echo SUCCESS: Directories created

REM Create ClickHouse configuration
echo Creating ClickHouse configuration...
(
echo ^<clickhouse^>
echo     ^<logger^>
echo         ^<level^>information^</level^>
echo         ^<console^>true^</console^>
echo     ^</logger^>
echo     ^<query_log^>
echo         ^<database^>system^</database^>
echo         ^<table^>query_log^</table^>
echo     ^</query_log^>
echo ^</clickhouse^>
) > clickhouse-config\config.xml

echo SUCCESS: ClickHouse configuration created

REM Create docker-compose file
echo Creating Docker Compose configuration...
(
echo version: '3.8'
echo services:
echo   zookeeper:
echo     image: confluentinc/cp-zookeeper:7.4.0
echo     hostname: zookeeper
echo     container_name: zookeeper
echo     ports:
echo       - "2181:2181"
echo     environment:
echo       ZOOKEEPER_CLIENT_PORT: 2181
echo       ZOOKEEPER_TICK_TIME: 2000
echo     networks:
echo       - fraud-detection-network
echo.
echo   kafka:
echo     image: confluentinc/cp-kafka:7.4.0
echo     hostname: kafka
echo     container_name: kafka
echo     depends_on:
echo       - zookeeper
echo     ports:
echo       - "9092:9092"
echo     environment:
echo       KAFKA_BROKER_ID: 1
echo       KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
echo       KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
echo       KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
echo       KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
echo       KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
echo       KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
echo       KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
echo     networks:
echo       - fraud-detection-network
echo.
echo   clickhouse:
echo     image: clickhouse/clickhouse-server:23.8
echo     hostname: clickhouse
echo     container_name: clickhouse
echo     ports:
echo       - "8123:8123"
echo       - "9000:9000"
echo     environment:
echo       CLICKHOUSE_DB: fraud_detection
echo       CLICKHOUSE_USER: admin
echo       CLICKHOUSE_PASSWORD: password
echo       CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: 1
echo     volumes:
echo       - clickhouse_data:/var/lib/clickhouse
echo       - ./clickhouse-config:/etc/clickhouse-server/config.d
echo       - ./clickhouse-init:/docker-entrypoint-initdb.d
echo     networks:
echo       - fraud-detection-network
echo.
echo   kafka-ui:
echo     image: provectuslabs/kafka-ui:latest
echo     container_name: kafka-ui
echo     depends_on:
echo       - kafka
echo     ports:
echo       - "8080:8080"
echo     environment:
echo       KAFKA_CLUSTERS_0_NAME: local
echo       KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
echo     networks:
echo       - fraud-detection-network
echo.
echo   tabix:
echo     image: spoonest/clickhouse-tabix-web-client
echo     container_name: clickhouse-ui
echo     ports:
echo       - "8081:80"
echo     depends_on:
echo       - clickhouse
echo     networks:
echo       - fraud-detection-network
echo.
echo volumes:
echo   clickhouse_data:
echo.
echo networks:
echo   fraud-detection-network:
echo     driver: bridge
) > docker-compose-full.yml

echo SUCCESS: Docker Compose configuration created

REM Download Docker images
echo.
echo Downloading Docker images (this may take several minutes)...
echo You can skip this step, but the first run will be slower.
echo.
set /p download="Download images now? (y/n): "
if /i "%download%"=="y" (
    echo Downloading images...
    docker pull confluentinc/cp-zookeeper:7.4.0
    docker pull confluentinc/cp-kafka:7.4.0
    docker pull clickhouse/clickhouse-server:23.8
    docker pull provectuslabs/kafka-ui:latest
    docker pull spoonest/clickhouse-tabix-web-client
    echo SUCCESS: Docker images downloaded
) else (
    echo SKIPPED: Docker image download
)

echo.
echo Setup Complete!
echo ==================
echo.
echo SUCCESS: Virtual environment created
echo SUCCESS: Python packages installed  
echo SUCCESS: Docker configuration ready
echo SUCCESS: ClickHouse configuration ready
echo SUCCESS: Project structure created
echo.
echo What has been set up:
echo   - Python virtual environment with all dependencies
echo   - Docker Compose with Kafka, ClickHouse, and UIs
echo   - Configuration files for all services
echo   - Directory structure for the project
echo.
echo Next Steps:
echo ===============
echo.
echo 1. Make sure you have your transaction data:
echo    Copy trans.csv to this directory: %CD%
echo.
echo 2. Make sure you have all the Python files:
echo    - kafka_fraud_detection.py
echo    - clickhouse_integration.py  
echo    - customer_segmentation.py
echo    - complete_fraud_system.py
echo    - model_trainer.py
echo    - analytics_dashboard.html
echo.
echo 3. Run the complete system:
echo    run_complete_system.bat
echo.
echo Once running, you will have access to:
echo   - Kafka UI:           http://localhost:8080
echo   - ClickHouse UI:      http://localhost:8081
echo   - ClickHouse HTTP:    http://localhost:8123
echo   - Analytics Dashboard: analytics_dashboard.html
echo.
echo System Features:
echo   - Real-time transaction streaming
echo   - ML-based fraud detection
echo   - Customer segmentation
echo   - Data warehouse analytics
echo   - Interactive dashboards
echo   - Automated reporting
echo.
echo Ready to start your banking analytics journey!
echo.
pause