@echo off
echo 🏗️ Setting up ClickHouse Data Warehouse Integration
echo ===================================================

REM Stop any existing containers
echo 🛑 Stopping existing containers...
docker-compose down >nul 2>&1

REM Start the full stack with ClickHouse
echo 🚀 Starting Kafka + ClickHouse stack...
docker-compose -f docker-compose-full.yml up -d

echo ⏳ Waiting for services to start (60 seconds)...
timeout /t 60 >nul

REM Check if services are running
echo 🔍 Checking service status...
docker ps

REM Install Python dependencies
echo 📦 Installing ClickHouse Python dependencies...
call venv\Scripts\activate.bat
pip install clickhouse-connect pandas

REM Create Kafka topics
echo 📝 Creating Kafka topics...
docker exec kafka kafka-topics --create --topic transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>nul
docker exec kafka kafka-topics --create --topic fraud-alerts --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>nul

REM Wait for ClickHouse to be ready
echo ⏳ Waiting for ClickHouse to be ready...
timeout /t 30 >nul

REM Test ClickHouse connection
echo 🧪 Testing ClickHouse connection...
curl -s "http://localhost:8123/ping" >nul
if %errorlevel% equ 0 (
    echo ✅ ClickHouse is responding
) else (
    echo ⚠️ ClickHouse may still be starting up
)

echo.
echo 🎉 ClickHouse Data Warehouse Setup Complete!
echo ===============================================
echo.
echo 🌐 Available Services:
echo   - Kafka UI:        http://localhost:8080
echo   - ClickHouse HTTP: http://localhost:8123
echo   - ClickHouse UI:   http://localhost:8081
echo.
echo 📊 Next Steps:
echo   1. Run: python clickhouse_integration.py
echo   2. Run: python kafka_fraud_detection.py stream
echo   3. Run: python kafka_fraud_detection.py detect
echo   4. Access ClickHouse UI for analytics
echo.
pause

# ==================================================
# run_full_stack.bat - Run complete fraud detection with data warehouse
# ==================================================

@echo off
title Kafka + ClickHouse Fraud Detection System

echo 🏢 Starting Complete Fraud Detection Data Warehouse System
echo ==========================================================

REM Check prerequisites
if not exist "trans.csv" (
    echo ❌ trans.csv not found! Please copy your transaction data file.
    pause
    exit /b 1
)

if not exist "fraud_model.pkl" (
    echo ❌ Fraud model not found! Training model...
    call venv\Scripts\activate.bat
    python model_trainer.py
)

REM Activate virtual environment
call venv\Scripts\activate.bat

REM Start the full stack
echo 🚀 Starting services...
docker-compose -f docker-compose-full.yml up -d

echo ⏳ Waiting for all services to be ready...
timeout /t 45 >nul

REM Start ClickHouse data warehouse streaming
echo 🏗️ Starting ClickHouse data warehouse integration...
start "ClickHouse Streamer" cmd /k "call venv\Scripts\activate.bat && python clickhouse_integration.py"

timeout /t 5 >nul

REM Start transaction streaming
echo 📊 Starting transaction streaming...
start "Transaction Streamer" cmd /k "call venv\Scripts\activate.bat && python kafka_fraud_detection.py stream"

timeout /t 5 >nul

REM Start fraud detection
echo 🛡️ Starting fraud detection...
start "Fraud Detector" cmd /k "call venv\Scripts\activate.bat && python kafka_fraud_detection.py detect"

echo.
echo 🎉 Complete System is Running!
echo ==============================
echo.
echo 📊 Data Flow:
echo   Transactions → Kafka → Fraud Detection → ClickHouse
echo                                        ↓
echo                              Fraud Alerts → ClickHouse
echo.
echo 🌐 Available Interfaces:
echo   - Kafka UI:         http://localhost:8080
echo   - ClickHouse UI:    http://localhost:8081  
echo   - ClickHouse HTTP:  http://localhost:8123
echo   - Dashboard:        fraud_dashboard.html
echo.
echo 📈 Analytics Available:
echo   - Real-time fraud metrics
echo   - Transaction pattern analysis  
echo   - Account risk scoring
echo   - Historical fraud trends
echo.
echo Press any key to stop the system...
pause >nul

echo 🛑 Stopping system...
taskkill /f /im python.exe >nul 2>&1
docker-compose -f docker-compose-full.yml down >nul 2>&1
echo ✅ System stopped