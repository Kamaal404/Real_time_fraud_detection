CREATE DATABASE IF NOT EXISTS fraud_detection;

USE fraud_detection;

-- ==================================================
-- Raw Transactions Table
-- ==================================================
CREATE TABLE IF NOT EXISTS transactions
(
    trans_id UInt32,
    account_id UInt32,
    date UInt32,
    type Nullable(String),
    operation Nullable(String),
    amount Float64,
    balance Float64,
    k_symbol Nullable(String),
    bank Nullable(String),
    account Nullable(UInt32),
    timestamp DateTime DEFAULT now(),
    processed_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (account_id, timestamp)
PARTITION BY toYYYYMM(timestamp)
TTL timestamp + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- ==================================================
-- Fraud Alerts Table
-- ==================================================
CREATE TABLE IF NOT EXISTS fraud_alerts
(
    alert_id String,
    transaction_id UInt32,
    account_id UInt32,
    amount Float64,
    anomaly_score Float64,
    confidence Float64,
    alert_timestamp DateTime,
    detected_at DateTime DEFAULT now(),
    alert_type String DEFAULT 'anomaly',
    status String DEFAULT 'new',
    investigated_by Nullable(String),
    investigation_notes Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (account_id, detected_at)
PARTITION BY toYYYYMM(detected_at)
SETTINGS index_granularity = 8192;

-- ==================================================
-- Account Statistics Table (for ML features)
-- ==================================================
CREATE TABLE IF NOT EXISTS account_stats
(
    account_id UInt32,
    date Date,
    transaction_count UInt32,
    total_amount Float64,
    avg_amount Float64,
    max_amount Float64,
    min_amount Float64,
    std_amount Float64,
    balance_avg Float64,
    balance_std Float64,
    fraud_score Float64 DEFAULT 0.0,
    last_updated DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(last_updated)
ORDER BY (account_id, date)
PARTITION BY toYYYYMM(date)
SETTINGS index_granularity = 8192;

-- ==================================================
-- Daily Fraud Summary Table
-- ==================================================
CREATE TABLE IF NOT EXISTS daily_fraud_summary
(
    date Date,
    total_transactions UInt32,
    total_fraud_alerts UInt32,
    fraud_rate Float64,
    total_amount_processed Float64,
    total_suspicious_amount Float64,
    top_risk_accounts Array(UInt32),
    created_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY date
SETTINGS index_granularity = 8192;

-- ==================================================
-- Kafka Integration Tables
-- ==================================================

-- Kafka consumer for transactions
CREATE TABLE IF NOT EXISTS transactions_kafka
(
    trans_id UInt32,
    account_id UInt32,
    date UInt32,
    type Nullable(String),
    operation Nullable(String),
    amount Float64,
    balance Float64,
    k_symbol Nullable(String),
    bank Nullable(String),
    account Nullable(UInt32),
    timestamp DateTime,
    processed Bool
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'clickhouse_transactions',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

-- Materialized view to move data from Kafka to main table
CREATE MATERIALIZED VIEW IF NOT EXISTS transactions_mv TO transactions AS
SELECT
    trans_id,
    account_id,
    date,
    type,
    operation,
    amount,
    balance,
    k_symbol,
    bank,
    account,
    timestamp,
    now() as processed_at
FROM transactions_kafka;

-- Kafka consumer for fraud alerts
CREATE TABLE IF NOT EXISTS fraud_alerts_kafka
(
    alert_id String,
    transaction_id UInt32,
    account_id UInt32,
    amount Float64,
    anomaly_score Float64,
    confidence Float64,
    detected_at String,
    transaction String  -- JSON string of original transaction
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'fraud-alerts',
    kafka_group_name = 'clickhouse_fraud_alerts',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

-- Materialized view for fraud alerts
CREATE MATERIALIZED VIEW IF NOT EXISTS fraud_alerts_mv TO fraud_alerts AS
SELECT
    alert_id,
    transaction_id,
    account_id,
    amount,
    anomaly_score,
    confidence,
    parseDateTimeBestEffort(detected_at) as alert_timestamp,
    now() as detected_at,
    'ml_anomaly' as alert_type,
    'new' as status,
    NULL as investigated_by,
    NULL as investigation_notes
FROM fraud_alerts_kafka;

-- ==================================================
-- Analytical Views and Functions
-- ==================================================

-- View: Account Risk Scores
CREATE VIEW IF NOT EXISTS account_risk_view AS
SELECT 
    account_id,
    COUNT(*) as total_transactions,
    AVG(amount) as avg_transaction_amount,
    SUM(amount) as total_amount,
    MAX(amount) as max_transaction,
    COUNT(*) / (SELECT COUNT(DISTINCT account_id) FROM transactions) * 100 as transaction_frequency_percentile,
    (SELECT COUNT(*) FROM fraud_alerts fa WHERE fa.account_id = t.account_id) as fraud_alerts_count,
    CASE 
        WHEN (SELECT COUNT(*) FROM fraud_alerts fa WHERE fa.account_id = t.account_id) > 0 THEN 'HIGH'
        WHEN MAX(amount) > (SELECT quantile(0.95)(amount) FROM transactions) THEN 'MEDIUM'
        ELSE 'LOW'
    END as risk_level
FROM transactions t
GROUP BY account_id;

-- View: Daily Transaction Metrics
CREATE VIEW IF NOT EXISTS daily_metrics_view AS
SELECT 
    toDate(timestamp) as date,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    quantile(0.5)(amount) as median_amount,
    quantile(0.95)(amount) as p95_amount,
    COUNT(DISTINCT account_id) as unique_accounts,
    SUM(case when amount < 0 then 1 else 0 end) as withdrawals,
    SUM(case when amount > 0 then 1 else 0 end) as deposits
FROM transactions
GROUP BY date
ORDER BY date;

-- View: Real-time Fraud Dashboard
CREATE VIEW IF NOT EXISTS fraud_dashboard_view AS
SELECT 
    toStartOfHour(detected_at) as hour,
    COUNT(*) as alerts_count,
    AVG(anomaly_score) as avg_anomaly_score,
    SUM(amount) as total_suspicious_amount,
    COUNT(DISTINCT account_id) as accounts_affected,
    MAX(confidence) as max_confidence,
    MIN(confidence) as min_confidence
FROM fraud_alerts
WHERE detected_at >= now() - INTERVAL 24 HOUR
GROUP BY hour
ORDER BY hour DESC;

-- ==================================================
-- Useful Analytical Queries
-- ==================================================

-- Top 10 accounts by fraud alerts
-- SELECT account_id, COUNT(*) as fraud_count, SUM(amount) as total_suspicious_amount
-- FROM fraud_alerts 
-- GROUP BY account_id 
-- ORDER BY fraud_count DESC 
-- LIMIT 10;

-- Fraud detection performance over time
-- SELECT 
--     toDate(detected_at) as date,
--     COUNT(*) as alerts,
--     (SELECT COUNT(*) FROM transactions WHERE toDate(timestamp) = date) as total_transactions,
--     COUNT(*) / (SELECT COUNT(*) FROM transactions WHERE toDate(timestamp) = date) * 100 as fraud_rate
-- FROM fraud_alerts
-- GROUP BY date
-- ORDER BY date DESC;

-- Real-time monitoring query
-- SELECT 
--     COUNT(*) as current_alerts,
--     AVG(anomaly_score) as avg_score,
--     COUNT(DISTINCT account_id) as affected_accounts
-- FROM fraud_alerts 
-- WHERE detected_at >= now() - INTERVAL 1 HOUR;