import clickhouse_connect
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta

class FraudAnalytics:
    def __init__(self):
        self.client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,
            username='admin', 
            password='password',
            database='fraud_detection'
        )
    
    def fraud_trends_report(self):
        """Generate fraud trends over time"""
        query = """
        SELECT 
            toDate(detected_at) as date,
            COUNT(*) as fraud_alerts,
            SUM(amount) as suspicious_amount,
            AVG(anomaly_score) as avg_score,
            COUNT(DISTINCT account_id) as affected_accounts
        FROM fraud_alerts
        WHERE detected_at >= now() - INTERVAL 30 DAY
        GROUP BY date
        ORDER BY date
        """
        
        result = self.client.query(query)
        df = pd.DataFrame(result.result_rows, 
                         columns=['date', 'fraud_alerts', 'suspicious_amount', 
                                'avg_score', 'affected_accounts'])
        
        # Create visualization
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle('Fraud Detection Trends (Last 30 Days)', fontsize=16)
        
        # Daily fraud alerts
        axes[0,0].plot(df['date'], df['fraud_alerts'], marker='o')
        axes[0,0].set_title('Daily Fraud Alerts')
        axes[0,0].set_ylabel('Number of Alerts')
        
        # Suspicious amount over time
        axes[0,1].plot(df['date'], df['suspicious_amount'], marker='o', color='red')
        axes[0,1].set_title('Daily Suspicious Amount')
        axes[0,1].set_ylabel('Amount ($)')
        
        # Average anomaly score
        axes[1,0].plot(df['date'], df['avg_score'], marker='o', color='orange')
        axes[1,0].set_title('Average Anomaly Score')
        axes[1,0].set_ylabel('Score')
        
        # Affected accounts
        axes[1,1].bar(df['date'], df['affected_accounts'], color='purple')
        axes[1,1].set_title('Daily Affected Accounts')
        axes[1,1].set_ylabel('Number of Accounts')
        
        plt.tight_layout()
        plt.savefig('fraud_trends_report.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        return df
    
    def top_risk_accounts_analysis(self):
        """Analyze top risk accounts"""
        query = """
        SELECT 
            fa.account_id,
            COUNT(*) as fraud_count,
            SUM(fa.amount) as total_suspicious,
            AVG(fa.anomaly_score) as avg_anomaly_score,
            MAX(fa.detected_at) as last_fraud_date,
            (SELECT COUNT(*) FROM transactions t WHERE t.account_id = fa.account_id) as total_transactions,
            (SELECT AVG(amount) FROM transactions t WHERE t.account_id = fa.account_id) as avg_transaction_amount
        FROM fraud_alerts fa
        GROUP BY account_id
        HAVING fraud_count >= 2
        ORDER BY fraud_count DESC, total_suspicious DESC
        LIMIT 20
        """
        
        result = self.client.query(query)
        df = pd.DataFrame(result.result_rows,
                         columns=['account_id', 'fraud_count', 'total_suspicious',
                                'avg_anomaly_score', 'last_fraud_date', 
                                'total_transactions', 'avg_transaction_amount'])
        
        # Create risk score
        df['fraud_rate'] = df['fraud_count'] / df['total_transactions'] * 100
        df['risk_score'] = (df['fraud_count'] * 0.4 + 
                           df['fraud_rate'] * 0.3 + 
                           df['avg_anomaly_score'].abs() * 0.3)
        
        print("🚨 TOP RISK ACCOUNTS ANALYSIS")
        print("=" * 50)
        print(df[['account_id', 'fraud_count', 'total_suspicious', 'fraud_rate', 'risk_score']].to_string(index=False))
        
        return df
    
    def transaction_patterns_by_hour(self):
        """Analyze transaction patterns by hour of day"""
        query = """
        SELECT 
            toHour(timestamp) as hour,
            COUNT(*) as transaction_count,
            AVG(amount) as avg_amount,
            SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) as withdrawals,
            SUM(CASE WHEN amount > 0 THEN 1 ELSE 0 END) as deposits
        FROM transactions
        WHERE timestamp >= now() - INTERVAL 7 DAY
        GROUP BY hour
        ORDER BY hour
        """
        
        result = self.client.query(query)
        df = pd.DataFrame(result.result_rows,
                         columns=['hour', 'transaction_count', 'avg_amount', 
                                'withdrawals', 'deposits'])
        
        # Fraud patterns by hour
        fraud_query = """
        SELECT 
            toHour(t.timestamp) as hour,
            COUNT(*) as fraud_count
        FROM fraud_alerts fa
        JOIN transactions t ON fa.transaction_id = t.trans_id
        WHERE fa.detected_at >= now() - INTERVAL 7 DAY
        GROUP BY hour
        ORDER BY hour
        """
        
        fraud_result = self.client.query(fraud_query)
        fraud_df = pd.DataFrame(fraud_result.result_rows, columns=['hour', 'fraud_count'])
        
        # Merge dataframes
        df = df.merge(fraud_df, on='hour', how='left')
        df['fraud_count'] = df['fraud_count'].fillna(0)
        df['fraud_rate'] = df['fraud_count'] / df['transaction_count'] * 100
        
        # Create visualization
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle('Transaction Patterns by Hour of Day (Last 7 Days)', fontsize=16)
        
        # Transaction volume by hour
        axes[0,0].bar(df['hour'], df['transaction_count'])
        axes[0,0].set_title('Transaction Volume by Hour')
        axes[0,0].set_xlabel('Hour of Day')
        axes[0,0].set_ylabel('Transaction Count')
        
        # Average amount by hour
        axes[0,1].plot(df['hour'], df['avg_amount'], marker='o', color='green')
        axes[0,1].set_title('Average Transaction Amount by Hour')
        axes[0,1].set_xlabel('Hour of Day')
        axes[0,1].set_ylabel('Average Amount ($)')
        
        # Fraud rate by hour
        axes[1,0].bar(df['hour'], df['fraud_rate'], color='red')
        axes[1,0].set_title('Fraud Rate by Hour')
        axes[1,0].set_xlabel('Hour of Day')
        axes[1,0].set_ylabel('Fraud Rate (%)')
        
        # Withdrawals vs Deposits
        axes[1,1].bar(df['hour'], df['withdrawals'], alpha=0.7, label='Withdrawals')
        axes[1,1].bar(df['hour'], df['deposits'], alpha=0.7, label='Deposits')
        axes[1,1].set_title('Withdrawals vs Deposits by Hour')
        axes[1,1].set_xlabel('Hour of Day')
        axes[1,1].set_ylabel('Count')
        axes[1,1].legend()
        
        plt.tight_layout()
        plt.savefig('transaction_patterns_by_hour.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        return df
    
    def real_time_dashboard_data(self):
        """Get real-time dashboard data"""
        queries = {
            'current_stats': """
                SELECT 
                    COUNT(*) as total_alerts_today,
                    SUM(amount) as total_suspicious_amount_today,
                    COUNT(DISTINCT account_id) as affected_accounts_today,
                    AVG(anomaly_score) as avg_anomaly_score_today
                FROM fraud_alerts 
                WHERE toDate(detected_at) = today()
            """,
            
            'hourly_trends': """
                SELECT 
                    toHour(detected_at) as hour,
                    COUNT(*) as alerts_count
                FROM fraud_alerts
                WHERE detected_at >= today()
                GROUP BY hour
                ORDER BY hour
            """,
            
            'top_amounts': """
                SELECT 
                    account_id,
                    amount,
                    anomaly_score,
                    detected_at
                FROM fraud_alerts
                WHERE toDate(detected_at) = today()
                ORDER BY amount DESC
                LIMIT 10
            """
        }
        
        dashboard_data = {}
        for key, query in queries.items():
            result = self.client.query(query)
            dashboard_data[key] = result.result_rows
        
        return dashboard_data

# Usage example
def run_analytics_examples():
    """Run analytics examples"""
    print("🔍 Starting ClickHouse Fraud Analytics")
    print("=" * 40)
    
    analytics = FraudAnalytics()
    
    try:
        # Generate fraud trends report
        print("📈 Generating fraud trends report...")
        fraud_trends = analytics.fraud_trends_report()
        
        # Analyze top risk accounts
        print("\n🚨 Analyzing top risk accounts...")
        risk_accounts = analytics.top_risk_accounts_analysis()
        
        # Transaction patterns analysis
        print("\n⏰ Analyzing transaction patterns by hour...")
        hourly_patterns = analytics.transaction_patterns_by_hour()
        
        # Real-time dashboard
        print("\n📊 Getting real-time dashboard data...")
        dashboard_data = analytics.real_time_dashboard_data()
        
        print("\n✅ Analytics completed! Check generated PNG files.")
        
    except Exception as e:
        print(f"❌ Error running analytics: {e}")

if __name__ == "__main__":
    run_analytics_examples()