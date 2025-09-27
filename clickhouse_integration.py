import pandas as pd
import numpy as np
import json
import time
import logging
from datetime import datetime, timedelta
import clickhouse_connect
from kafka import KafkaConsumer, KafkaProducer
import threading
import warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ClickHouseDataWarehouse:
    """
    ClickHouse Data Warehouse integration for fraud detection system
    """
    
    def __init__(self, 
                 host='localhost', 
                 port=8123, 
                 username='admin', 
                 password='password',
                 database='fraud_detection'):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.client = None
        self.connect()
        
    def connect(self):
        """Connect to ClickHouse"""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database
            )
            logger.info("Connected to ClickHouse successfully")
            
            # Test connection
            result = self.client.command('SELECT version()')
            logger.info(f"ClickHouse version: {result}")
            
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def create_tables_if_not_exist(self):
        """Create tables if they don't exist"""
        try:
            # Read SQL file and execute
            with open('clickhouse-init/01-create-database.sql', 'r') as f:
                sql_commands = f.read()
            
            # Split and execute each command
            commands = sql_commands.split(';')
            for command in commands:
                command = command.strip()
                if command and not command.startswith('--'):
                    self.client.command(command)
            
            logger.info("ClickHouse tables created successfully")
            
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
    
    def insert_transaction_batch(self, transactions_df):
        """Insert batch of transactions into ClickHouse"""
        try:
            # Prepare data for insertion
            data = []
            for _, row in transactions_df.iterrows():
                data.append([
                    int(row['trans_id']),
                    int(row['account_id']),
                    int(row['date']),
                    row['type'] if pd.notna(row['type']) else None,
                    row['operation'] if pd.notna(row['operation']) else None,
                    float(row['amount']),
                    float(row['balance']),
                    row['k_symbol'] if pd.notna(row['k_symbol']) else None,
                    row['bank'] if pd.notna(row['bank']) else None,
                    int(row['account']) if pd.notna(row['account']) else None,
                    datetime.now(),
                    datetime.now()
                ])
            
            # Insert data
            self.client.insert('transactions', data, 
                             column_names=['trans_id', 'account_id', 'date', 'type', 'operation',
                                         'amount', 'balance', 'k_symbol', 'bank', 'account',
                                         'timestamp', 'processed_at'])
            
            logger.info(f"Inserted {len(data)} transactions into ClickHouse")
            
        except Exception as e:
            logger.error(f"Failed to insert transactions: {e}")
    
    def insert_fraud_alert(self, alert_data):
        """Insert fraud alert into ClickHouse"""
        try:
            data = [[
                alert_data['alert_id'],
                alert_data['transaction_id'],
                alert_data['account_id'],
                alert_data['amount'],
                alert_data['anomaly_score'],
                alert_data['confidence'],
                datetime.fromisoformat(alert_data['detected_at'].replace('Z', '+00:00')),
                datetime.now(),
                'ml_anomaly',
                'new',
                None,
                None
            ]]
            
            self.client.insert('fraud_alerts', data,
                             column_names=['alert_id', 'transaction_id', 'account_id', 'amount',
                                         'anomaly_score', 'confidence', 'alert_timestamp', 'detected_at',
                                         'alert_type', 'status', 'investigated_by', 'investigation_notes'])
            
            logger.info(f"Inserted fraud alert {alert_data['alert_id']} into ClickHouse")
            
        except Exception as e:
            logger.error(f"Failed to insert fraud alert: {e}")
    
    def get_account_statistics(self, account_id, days=30):
        """Get account statistics for fraud detection"""
        try:
            query = """
            SELECT 
                account_id,
                COUNT(*) as transaction_count,
                AVG(amount) as avg_amount,
                stddevSamp(amount) as std_amount,
                MIN(amount) as min_amount,
                MAX(amount) as max_amount,
                AVG(balance) as avg_balance,
                stddevSamp(balance) as std_balance
            FROM transactions 
            WHERE account_id = {account_id} 
            AND timestamp >= now() - INTERVAL {days} DAY
            GROUP BY account_id
            """.format(account_id=account_id, days=days)
            
            result = self.client.query(query)
            return result.result_rows[0] if result.result_rows else None
            
        except Exception as e:
            logger.error(f"Failed to get account statistics: {e}")
            return None
    
    def get_fraud_dashboard_data(self):
        """Get data for fraud detection dashboard"""
        try:
            # Recent fraud alerts
            recent_alerts_query = """
            SELECT 
                COUNT(*) as total_alerts,
                AVG(anomaly_score) as avg_score,
                COUNT(DISTINCT account_id) as affected_accounts,
                SUM(amount) as total_suspicious_amount
            FROM fraud_alerts 
            WHERE detected_at >= now() - INTERVAL 24 HOUR
            """
            
            # Daily transaction volume
            daily_volume_query = """
            SELECT 
                toDate(timestamp) as date,
                COUNT(*) as transaction_count,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount
            FROM transactions
            WHERE timestamp >= now() - INTERVAL 7 DAY
            GROUP BY date
            ORDER BY date DESC
            """
            
            # Top risk accounts
            risk_accounts_query = """
            SELECT 
                account_id,
                COUNT(*) as fraud_count,
                SUM(amount) as total_suspicious_amount,
                MAX(detected_at) as last_alert
            FROM fraud_alerts
            WHERE detected_at >= now() - INTERVAL 7 DAY
            GROUP BY account_id
            ORDER BY fraud_count DESC
            LIMIT 10
            """
            
            dashboard_data = {
                'recent_alerts': self.client.query(recent_alerts_query).result_rows,
                'daily_volume': self.client.query(daily_volume_query).result_rows,
                'risk_accounts': self.client.query(risk_accounts_query).result_rows
            }
            
            return dashboard_data
            
        except Exception as e:
            logger.error(f"Failed to get dashboard data: {e}")
            return None
    
    def update_account_stats_daily(self):
        """Update daily account statistics (run as scheduled job)"""
        try:
            query = """
            INSERT INTO account_stats 
            SELECT 
                account_id,
                toDate(timestamp) as date,
                COUNT(*) as transaction_count,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount,
                MAX(amount) as max_amount,
                MIN(amount) as min_amount,
                stddevSamp(amount) as std_amount,
                AVG(balance) as balance_avg,
                stddevSamp(balance) as balance_std,
                0.0 as fraud_score,
                now() as last_updated
            FROM transactions
            WHERE toDate(timestamp) = yesterday()
            GROUP BY account_id, date
            """
            
            self.client.command(query)
            logger.info("Updated daily account statistics")
            
        except Exception as e:
            logger.error(f"Failed to update account stats: {e}")

class KafkaClickHouseStreamer:
    """
    Stream data from Kafka to ClickHouse
    """
    
    def __init__(self, 
                 kafka_servers=['localhost:9092'],
                 clickhouse_config=None):
        self.kafka_servers = kafka_servers
        self.clickhouse = ClickHouseDataWarehouse(**(clickhouse_config or {}))
        self.running = False
        
    def stream_transactions_to_clickhouse(self, topic='transactions', batch_size=100):
        """Stream transactions from Kafka to ClickHouse"""
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.kafka_servers,
            group_id='clickhouse_transactions_streamer',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        logger.info(f"Starting transaction streaming from Kafka to ClickHouse...")
        
        batch = []
        self.running = True
        
        try:
            for message in consumer:
                if not self.running:
                    break
                    
                transaction = message.value
                batch.append(transaction)
                
                # Insert batch when it reaches batch_size
                if len(batch) >= batch_size:
                    df = pd.DataFrame(batch)
                    self.clickhouse.insert_transaction_batch(df)
                    batch = []
                    
        except KeyboardInterrupt:
            logger.info("Transaction streaming interrupted by user")
        except Exception as e:
            logger.error(f"Error in transaction streaming: {e}")
        finally:
            # Insert remaining batch
            if batch:
                df = pd.DataFrame(batch)
                self.clickhouse.insert_transaction_batch(df)
            
            consumer.close()
            logger.info("Transaction streaming stopped")
    
    def stream_fraud_alerts_to_clickhouse(self, topic='fraud-alerts'):
        """Stream fraud alerts from Kafka to ClickHouse"""
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.kafka_servers,
            group_id='clickhouse_fraud_alerts_streamer',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        logger.info(f"Starting fraud alerts streaming from Kafka to ClickHouse...")
        
        self.running = True
        
        try:
            for message in consumer:
                if not self.running:
                    break
                    
                alert = message.value
                self.clickhouse.insert_fraud_alert(alert)
                
        except KeyboardInterrupt:
            logger.info("Fraud alerts streaming interrupted by user")
        except Exception as e:
            logger.error(f"Error in fraud alerts streaming: {e}")
        finally:
            consumer.close()
            logger.info("Fraud alerts streaming stopped")
    
    def start_streaming_threads(self):
        """Start both streaming processes in separate threads"""
        # Start transaction streaming thread
        transaction_thread = threading.Thread(
            target=self.stream_transactions_to_clickhouse,
            daemon=True
        )
        transaction_thread.start()
        
        # Start fraud alerts streaming thread
        fraud_thread = threading.Thread(
            target=self.stream_fraud_alerts_to_clickhouse,
            daemon=True
        )
        fraud_thread.start()
        
        logger.info("Both streaming threads started")
        
        return transaction_thread, fraud_thread
    
    def stop_streaming(self):
        """Stop streaming"""
        self.running = False
        logger.info("Stopping Kafka to ClickHouse streaming...")

class ClickHouseAnalytics:
    """
    Analytics and reporting functions for ClickHouse
    """
    
    def __init__(self, clickhouse_client):
        self.client = clickhouse_client
    
    def fraud_detection_report(self, days=7):
        """Generate fraud detection performance report"""
        query = f"""
        SELECT 
            toDate(detected_at) as date,
            COUNT(*) as total_alerts,
            COUNT(DISTINCT account_id) as unique_accounts,
            AVG(anomaly_score) as avg_anomaly_score,
            SUM(amount) as total_suspicious_amount,
            quantile(0.5)(confidence) as median_confidence,
            quantile(0.95)(confidence) as p95_confidence
        FROM fraud_alerts
        WHERE detected_at >= now() - INTERVAL {days} DAY
        GROUP BY date
        ORDER BY date DESC
        """
        
        result = self.client.query(query)
        return pd.DataFrame(result.result_rows, 
                          columns=['date', 'total_alerts', 'unique_accounts', 
                                 'avg_anomaly_score', 'total_suspicious_amount',
                                 'median_confidence', 'p95_confidence'])
    
    def account_risk_analysis(self, top_n=50):
        """Analyze top risky accounts"""
        query = f"""
        SELECT 
            account_id,
            COUNT(*) as fraud_alerts,
            SUM(amount) as total_suspicious_amount,
            AVG(anomaly_score) as avg_anomaly_score,
            MAX(detected_at) as last_alert_date,
            (SELECT COUNT(*) FROM transactions t WHERE t.account_id = fa.account_id) as total_transactions
        FROM fraud_alerts fa
        GROUP BY account_id
        ORDER BY fraud_alerts DESC, total_suspicious_amount DESC
        LIMIT {top_n}
        """
        
        result = self.client.query(query)
        return pd.DataFrame(result.result_rows,
                          columns=['account_id', 'fraud_alerts', 'total_suspicious_amount',
                                 'avg_anomaly_score', 'last_alert_date', 'total_transactions'])
    
    def transaction_patterns_analysis(self):
        """Analyze transaction patterns for fraud detection"""
        query = """
        SELECT 
            toHour(timestamp) as hour_of_day,
            COUNT(*) as transaction_count,
            AVG(amount) as avg_amount,
            (SELECT COUNT(*) FROM fraud_alerts fa 
             JOIN transactions t ON fa.transaction_id = t.trans_id 
             WHERE toHour(t.timestamp) = hour_of_day) as fraud_count
        FROM transactions
        GROUP BY hour_of_day
        ORDER BY hour_of_day
        """
        
        result = self.client.query(query)
        return pd.DataFrame(result.result_rows,
                          columns=['hour_of_day', 'transaction_count', 'avg_amount', 'fraud_count'])

# Example usage and main execution
def main():
    """Main execution function"""
    logger.info("Starting ClickHouse-Kafka Fraud Detection Data Warehouse")
    
    # Initialize ClickHouse connection
    clickhouse = ClickHouseDataWarehouse()
    
    # Create tables if they don't exist
    clickhouse.create_tables_if_not_exist()
    
    # Initialize Kafka-ClickHouse streamer
    streamer = KafkaClickHouseStreamer()
    
    try:
        # Start streaming threads
        transaction_thread, fraud_thread = streamer.start_streaming_threads()
        
        logger.info("Data warehouse streaming is running...")
        logger.info("ClickHouse UI available at: http://localhost:8081")
        logger.info("Press Ctrl+C to stop")
        
        # Keep main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Stopping data warehouse streaming...")
        streamer.stop_streaming()
    
    logger.info("Data warehouse streaming stopped")

if __name__ == "__main__":
    main()
