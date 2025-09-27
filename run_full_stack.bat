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