
import pandas as pd
import numpy as np
import json
import time
from datetime import datetime, timedelta
import threading
import pickle
import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TransactionDataStreamer:
    """
    Simulates real-time transaction streaming from historical data
    """
    
    def __init__(self, data_file='trans.csv', kafka_servers=['localhost:9092']):
        self.data_file = data_file
        self.kafka_servers = kafka_servers
        self.producer = None
        self.transactions_df = None
        self.streaming = False
        
    def load_data(self):
        """Load historical transaction data"""
        logger.info("Loading transaction data...")
        self.transactions_df = pd.read_csv(self.data_file, sep=";", low_memory=False)
        
        # Sort by date to simulate chronological order
        self.transactions_df = self.transactions_df.sort_values('date').reset_index(drop=True)
        logger.info(f"Loaded {len(self.transactions_df)} transactions")
        
    def setup_kafka_producer(self):
        """Initialize Kafka producer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: str(x).encode('utf-8') if x else None
            )
            logger.info("Kafka producer initialized successfully")
        except NoBrokersAvailable:
            logger.error("No Kafka brokers available. Make sure Kafka is running on localhost:9092")
            raise
            
    def create_transaction_message(self, row):
        """Convert pandas row to JSON message"""
        message = {
            'trans_id': int(row['trans_id']),
            'account_id': int(row['account_id']),
            'date': int(row['date']),
            'type': row['type'] if pd.notna(row['type']) else None,
            'operation': row['operation'] if pd.notna(row['operation']) else None,
            'amount': float(row['amount']),
            'balance': float(row['balance']),
            'k_symbol': row['k_symbol'] if pd.notna(row['k_symbol']) else None,
            'bank': row['bank'] if pd.notna(row['bank']) else None,
            'account': int(row['account']) if pd.notna(row['account']) else None,
            'timestamp': datetime.now().isoformat(),
            'processed': False
        }
        return message
        
    def stream_transactions(self, topic='transactions', delay_ms=100, batch_size=1):
        """
        Stream transactions to Kafka topic
        
        Args:
            topic: Kafka topic name
            delay_ms: Delay between messages in milliseconds
            batch_size: Number of transactions to send per batch
        """
        if self.transactions_df is None:
            self.load_data()
            
        if self.producer is None:
            self.setup_kafka_producer()
            
        self.streaming = True
        logger.info(f"Starting transaction streaming to topic '{topic}'...")
        logger.info(f"Delay: {delay_ms}ms, Batch size: {batch_size}")
        
        sent_count = 0
        batch_count = 0
        
        try:
            for index in range(0, len(self.transactions_df), batch_size):
                if not self.streaming:
                    break
                    
                # Get batch of transactions
                batch = self.transactions_df.iloc[index:index + batch_size]
                
                for _, row in batch.iterrows():
                    if not self.streaming:
                        break
                        
                    # Create message
                    message = self.create_transaction_message(row)
                    
                    # Send to Kafka
                    future = self.producer.send(
                        topic, 
                        key=str(message['account_id']),  # Partition by account_id
                        value=message
                    )
                    
                    sent_count += 1
                    
                    # Log progress
                    if sent_count % 1000 == 0:
                        logger.info(f"Sent {sent_count} transactions...")
                
                batch_count += 1
                
                # Wait before next batch
                time.sleep(delay_ms / 1000.0)
                
        except KeyboardInterrupt:
            logger.info("Streaming interrupted by user")
        except Exception as e:
            logger.error(f"Error during streaming: {e}")
        finally:
            self.streaming = False
            if self.producer:
                self.producer.flush()
                self.producer.close()
            logger.info(f"Streaming completed. Sent {sent_count} transactions in {batch_count} batches")
    
    def stop_streaming(self):
        """Stop the streaming process"""
        self.streaming = False
        logger.info("Stopping transaction stream...")

class RealTimeFraudDetector:
    """
    Real-time fraud detection consumer
    """
    
    def __init__(self, kafka_servers=['localhost:9092'], model_file='fraud_model.pkl'):
        self.kafka_servers = kafka_servers
        self.model_file = model_file
        self.consumer = None
        self.model = None
        self.scaler = None
        self.account_stats = {}
        self.processing_stats = {
            'processed': 0,
            'fraud_detected': 0,
            'start_time': None
        }
        
    def load_trained_model(self):
        """Load pre-trained fraud detection model"""
        try:
            with open(self.model_file, 'rb') as f:
                model_data = pickle.load(f)
                self.model = model_data['model']
                self.scaler = model_data['scaler']
                self.account_stats = model_data.get('account_stats', {})
            logger.info("Loaded pre-trained fraud detection model")
        except FileNotFoundError:
            logger.warning(f"Model file {self.model_file} not found. Training basic model...")
            self.train_basic_model()
            
    def train_basic_model(self):
        """Train a basic model if none exists"""
        from sklearn.ensemble import IsolationForest
        from sklearn.preprocessing import StandardScaler
        
        # Load historical data for training
        trans_df = pd.read_csv('trans.csv', sep=";", low_memory=False)
        
        # Basic feature engineering
        features_df = self.engineer_features(trans_df.sample(10000))  # Sample for speed
        
        # Train model
        self.scaler = StandardScaler()
        scaled_features = self.scaler.fit_transform(features_df)
        
        self.model = IsolationForest(contamination=0.05, random_state=42)
        self.model.fit(scaled_features)
        
        # Save model
        model_data = {
            'model': self.model,
            'scaler': self.scaler,
            'account_stats': self.account_stats
        }
        with open(self.model_file, 'wb') as f:
            pickle.dump(model_data, f)
            
        logger.info("Basic fraud detection model trained and saved")
        
    def engineer_features(self, df):
        features_df = pd.DataFrame()
        features_df['amount_abs'] = abs(df['amount'])
        features_df['is_withdrawal'] = (df['amount'] < 0).astype(int)
        features_df['amount_log'] = np.log1p(features_df['amount_abs'])
        features_df['balance_abs'] = abs(df['balance'])

        # Account-level statistics
        account_stats = df.groupby('account_id')['amount'].agg(['mean', 'std']).reset_index()
        account_stats.columns = ['account_id', 'account_mean', 'account_std']
        df_with_stats = df.merge(account_stats, on='account_id', how='left')
        features_df['amount_z_score'] = abs(df_with_stats['amount'] - df_with_stats['account_mean']) / (df_with_stats['account_std'] + 1)

        # Daily transaction count
        daily_counts = df.groupby(['account_id', 'date']).size().reset_index()
        daily_counts.columns = ['account_id', 'date', 'daily_count']
        df_with_daily = df.merge(daily_counts, on=['account_id', 'date'], how='left')
        features_df['daily_trans_count'] = df_with_daily['daily_count']

        # Time-based features
        df['date_str'] = df['date'].astype(str)
        features_df['day_of_month'] = df['date_str'].str[4:6].astype(int)
        features_df['is_weekend'] = (features_df['day_of_month'] % 7).isin([0, 6]).astype(int)

        return features_df.fillna(0)

        
    def detect_fraud(self, transaction):
        """Detect fraud in a single transaction"""
        # Convert to DataFrame for feature engineering
        df = pd.DataFrame([transaction])
        
        # Engineer features
        features = self.engineer_features(df)
        
        # Scale features
        scaled_features = self.scaler.transform(features)
        
        # Predict
        prediction = self.model.predict(scaled_features)[0]
        anomaly_score = self.model.decision_function(scaled_features)[0]
        
        is_fraud = prediction == -1
        
        return {
            'is_fraud': is_fraud,
            'anomaly_score': float(anomaly_score),
            'confidence': abs(anomaly_score)
        }
        
    def setup_kafka_consumer(self, topic='transactions', group_id='fraud_detector'):
        """Initialize Kafka consumer"""
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.kafka_servers,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        logger.info(f"Kafka consumer initialized for topic '{topic}'")
        
    def process_stream(self, output_topic='fraud_alerts'):
        """Process incoming transaction stream"""
        if self.model is None:
            self.load_trained_model()
            
        if self.consumer is None:
            self.setup_kafka_consumer()
            
        # Setup producer for fraud alerts
        alert_producer = KafkaProducer(
            bootstrap_servers=self.kafka_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        self.processing_stats['start_time'] = time.time()
        logger.info("Starting real-time fraud detection...")
        
        try:
            for message in self.consumer:
                transaction = message.value
                
                # Detect fraud
                fraud_result = self.detect_fraud(transaction)
                
                # Update stats
                self.processing_stats['processed'] += 1
                
                if fraud_result['is_fraud']:
                    self.processing_stats['fraud_detected'] += 1
                    
                    # Create fraud alert
                    alert = {
                        'alert_id': f"alert_{int(time.time())}_{transaction['trans_id']}",
                        'transaction_id': transaction['trans_id'],
                        'account_id': transaction['account_id'],
                        'amount': transaction['amount'],
                        'anomaly_score': fraud_result['anomaly_score'],
                        'confidence': fraud_result['confidence'],
                        'detected_at': datetime.now().isoformat(),
                        'transaction': transaction
                    }
                    
                    # Send alert to output topic
                    alert_producer.send(output_topic, value=alert)
                    
                    logger.warning(f"FRAUD DETECTED! Transaction {transaction['trans_id']}, "
                                 f"Account {transaction['account_id']}, "
                                 f"Amount ${transaction['amount']:,.2f}, "
                                 f"Score: {fraud_result['anomaly_score']:.3f}")
                
                # Log progress
                if self.processing_stats['processed'] % 1000 == 0:
                    self.log_stats()
                    
        except KeyboardInterrupt:
            logger.info("Fraud detection interrupted by user")
        except Exception as e:
            logger.error(f"Error in fraud detection: {e}")
        finally:
            if alert_producer:
                alert_producer.close()
            if self.consumer:
                self.consumer.close()
            self.log_final_stats()
            
    def log_stats(self):
        """Log processing statistics"""
        elapsed = time.time() - self.processing_stats['start_time']
        rate = self.processing_stats['processed'] / elapsed if elapsed > 0 else 0
        fraud_rate = (self.processing_stats['fraud_detected'] / 
                     self.processing_stats['processed'] * 100) if self.processing_stats['processed'] > 0 else 0
        
        logger.info(f"Processed: {self.processing_stats['processed']}, "
                   f"Fraud detected: {self.processing_stats['fraud_detected']}, "
                   f"Fraud rate: {fraud_rate:.2f}%, "
                   f"Processing rate: {rate:.1f} trans/sec")
    
    def log_final_stats(self):
        """Log final statistics"""
        logger.info("=== FINAL STATISTICS ===")
        self.log_stats()

# Example usage functions
def start_streaming():
    """Start the transaction streaming"""
    streamer = TransactionDataStreamer()
    streamer.stream_transactions(delay_ms=50, batch_size=5)  # Fast streaming for demo

def start_fraud_detection():
    """Start the fraud detection consumer"""
    detector = RealTimeFraudDetector()
    detector.process_stream()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "stream":
            start_streaming()
        elif sys.argv[1] == "detect":
            start_fraud_detection()
        else:
            print("Usage: python kafka_fraud_detection.py [stream|detect]")
    else:
        print("Usage: python kafka_fraud_detection.py [stream|detect]")
        print("  stream: Start transaction streaming")
        print("  detect: Start fraud detection")