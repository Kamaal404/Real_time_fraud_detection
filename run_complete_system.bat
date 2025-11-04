@echo off
title Complete Banking Analytics System

echo.
echo 🏦 Complete Banking Fraud Detection ^& Analytics System
echo =========================================================
echo.
echo 📊 System Components:
echo   ✅ Kafka Message Streaming
echo   ✅ Real-time Fraud Detection  
echo   ✅ ClickHouse Data Warehouse
echo   ✅ Customer Segmentation ML
echo   ✅ Analytics Dashboard
echo   ✅ Automated Reporting
echo.

REM Check if system is set up
if not exist "trans.csv" (
    echo ❌ trans.csv not found! Please copy your transaction data file to this directory.
    echo.
    echo Expected location: %CD%\trans.csv
    echo.
    pause
    exit /b 1
)

if not exist "venv" (
    echo ❌ Virtual environment not found! Please run setup first.
    echo.
    echo Run this command first: setup_complete_system.bat
    echo.
    pause
    exit /b 1
)

echo ✅ Data file found: trans.csv
echo ✅ Virtual environment found

REM Activate virtual environment
echo 🐍 Activating Python environment...
call venv\Scripts\activate.bat

REM Check for required Python files
if not exist "kafka_fraud_detection.py" (
    echo ❌ kafka_fraud_detection.py not found!
    echo Please make sure all Python files are in this directory.
    pause
    exit /b 1
)

if not exist "clickhouse_integration.py" (
    echo ❌ clickhouse_integration.py not found!
    echo Please make sure all Python files are in this directory.
    pause
    exit /b 1
)

if not exist "customer_segmentation.py" (
    echo ❌ customer_segmentation.py not found!
    echo Please make sure all Python files are in this directory.
    pause
    exit /b 1
)

if not exist "complete_fraud_system.py" (
    echo ❌ complete_fraud_system.py not found!
    echo Please make sure all Python files are in this directory.
    pause
    exit /b 1
)

echo ✅ All Python files found

REM Check for required models and train if needed
if not exist "fraud_model.pkl" (
    echo 🤖 Fraud model not found. Training now...
    python model_trainer.py
    if %errorlevel% neq 0 (
        echo ❌ Failed to train fraud model
        pause
        exit /b 1
    )
    echo ✅ Fraud model trained successfully
) else (
    echo ✅ Fraud model found
)

REM Check for docker-compose file
if not exist "docker-compose-full.yml" (
    echo ❌ docker-compose-full.yml not found!
    echo This file is required to start the infrastructure.
    pause
    exit /b 1
)

echo ✅ Docker compose configuration found

REM Start infrastructure
echo.
echo 🚀 Starting system infrastructure...
echo   - Zookeeper
echo   - Kafka
echo   - ClickHouse Database
echo   - Kafka UI
echo   - ClickHouse UI
echo.

docker-compose -f docker-compose-full.yml up -d

REM Wait for services to be ready
echo ⏳ Waiting for services to be ready (45 seconds)...
echo    This may take a while on first startup as Docker downloads images...

timeout /t 45 /nobreak

REM Check if Docker containers are running
echo 🔍 Checking service status...
docker ps --format "table {{.Names}}\t{{.Status}}" | findstr /C:"kafka" /C:"clickhouse" /C:"zookeeper"

REM Create Kafka topics
echo.
echo 📝 Creating Kafka topics...
docker exec kafka kafka-topics --create --topic transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>nul
if %errorlevel% equ 0 (
    echo ✅ Created transactions topic
) else (
    echo ⚠️ Transactions topic may already exist
)

docker exec kafka kafka-topics --create --topic fraud-alerts --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>nul
if %errorlevel% equ 0 (
    echo ✅ Created fraud-alerts topic
) else (
    echo ⚠️ Fraud-alerts topic may already exist
)

docker exec kafka kafka-topics --create --topic customer-segments --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>nul
if %errorlevel% equ 0 (
    echo ✅ Created customer-segments topic
) else (
    echo ⚠️ Customer-segments topic may already exist
)

REM Test ClickHouse connection
echo.
echo 🧪 Testing ClickHouse connection...
curl -s "http://localhost:8123/ping" >nul
if %errorlevel% equ 0 (
    echo ✅ ClickHouse is responding
) else (
    echo ⚠️ ClickHouse may still be starting up...
    echo    Waiting additional 15 seconds...
    timeout /t 15 /nobreak
)

REM Test Kafka connection
echo 🧪 Testing Kafka connection...
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 >nul 2>&1
if %errorlevel% equ 0 (
    echo ✅ Kafka is responding
) else (
    echo ❌ Kafka is not responding properly
    echo.
    echo Troubleshooting:
    echo 1. Make sure Docker Desktop is running
    echo 2. Wait a bit longer for services to start
    echo 3. Check docker logs: docker logs kafka
    echo.
    pause
    exit /b 1
)

echo.
echo 🎉 Infrastructure Ready!
echo ========================
echo.
echo 🌐 Available Interfaces:
echo   - Kafka UI:           http://localhost:8080
echo   - ClickHouse UI:      http://localhost:8081  
echo   - ClickHouse HTTP:    http://localhost:8123
echo   - Analytics Dashboard: analytics_dashboard.html (open in browser)
echo.
echo 📊 Data Flow:
echo   CSV Data → Kafka → Fraud Detection → ClickHouse → Analytics
echo.

echo 🚀 Starting Complete Analytics System...
echo.
echo 📊 The system will now:
echo   1. Stream transactions from your CSV data
echo   2. Detect fraud in real-time using ML
echo   3. Store all data in ClickHouse warehouse
echo   4. Perform customer segmentation analysis
echo   5. Generate analytics and insights
echo.
echo 📈 IMPORTANT: Open analytics_dashboard.html in your browser for live dashboard!
echo.
echo ⏹️ Press Ctrl+C to stop the system
echo.
echo 🎬 Starting main system now...
echo.

REM Start the complete system
python complete_fraud_system.py

REM Cleanup when system stops
echo.
echo 🛑 System stopped. Cleaning up...
echo.
echo Stopping Docker containers...
docker-compose -f docker-compose-full.yml down

echo.
echo ✅ Cleanup complete
echo.
echo Thank you for using the Complete Banking Analytics System!
echo.
pause