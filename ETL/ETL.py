import pymongo
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import yaml
import random
from apscheduler.schedulers.blocking import BlockingScheduler

class ETLPipeline:
    def __init__(self):
        self.load_config()
        self.setup_connections()
        
    def load_config(self):
        try:
            with open(r"C:\Users\rosep\OneDrive\Documentos\fintech-etl-dwh-streaming\ETL\config.yaml") as f:
                self.config = yaml.safe_load(f)

            required_sections = ['mongo', 'postgresql', 'warehouse']
            for section in required_sections:
                if section not in self.config:
                    raise ValueError(f"Missing section: {section}")

        except Exception as e:
            print(f"Config error: {str(e)}")
            raise

    def setup_connections(self):
        try:
            # MongoDB connection
            mongo_conf = self.config['mongo']
            self.mongo_client = pymongo.MongoClient(
                host=mongo_conf['host'],
                port=mongo_conf['port'],
                serverSelectionTimeoutMS=5000
            )
            self.mongo_db = self.mongo_client[mongo_conf['database']]
            self.mongo_collection = self.mongo_db[mongo_conf['collection']]
            self.mongo_client.server_info()  # test connection

            # PostgreSQL source connection (customers)
            pg_conf = self.config['postgresql']
            self.pg_source_conn = psycopg2.connect(
                dbname=pg_conf['database'],
                user=pg_conf['user'],
                password=pg_conf['password'],
                host=pg_conf['host'],
                port=pg_conf['port']
            )

            # Data warehouse connection (SQLAlchemy engine)
            dw_conf = self.config['warehouse']
            self.dw_engine = create_engine(
                f"postgresql+psycopg2://{dw_conf['user']}:{dw_conf['password']}@{dw_conf['host']}:{dw_conf['port']}/{dw_conf['database']}"
            )

        except Exception as e:
            print(f"Connection error: {str(e)}")
            self.close_connections()
            raise

    def extract_mongo_transactions(self):
        """Extract data from MongoDB and inject synthetic 'transaction_type'."""
        try:
            raw_data = list(self.mongo_collection.find())
            transactions = []
            for doc in raw_data:
                transactions.append({
                    "transaction_id": doc.get("transaction_id", str(doc.get("_id"))),
                    "customer_id": str(doc.get("customer_id", "")),
                    "amount": doc.get("amount"),
                    "transaction_type": random.choice(["credit", "debit"]),  # Synthetic
                    "timestamp": doc.get("timestamp"),
                    "currency": doc.get("currency"),
                    "account_type": doc.get("account_type"),
                    "branch": doc.get("branch"),
                    "description": doc.get("description")
                })
            return pd.DataFrame(transactions)
        except Exception as e:
            print(f"MongoDB extract error: {str(e)}")
            raise

    def extract_postgres_customers(self):
        try:
            query = "SELECT customer_id, full_name, email, phone, created_at FROM customers"
            return pd.read_sql(query, self.pg_source_conn)
        except Exception as e:
            print(f"PostgreSQL extract error: {str(e)}")
            raise

    def transform_data(self, transactions, customers):
        try:
            transactions = transactions.copy()
            transactions["transaction_id"] = transactions["transaction_id"].astype(str)
            transactions["customer_id"] = transactions["customer_id"].astype(str)

            # Clean transactions
            transactions = transactions.dropna(subset=[
                "transaction_id", "customer_id", "amount", 
                "transaction_type", "timestamp"
            ])
            transactions["timestamp"] = pd.to_datetime(transactions["timestamp"], errors="coerce")
            transactions = transactions.dropna(subset=["timestamp"])
            transactions["amount"] = pd.to_numeric(transactions["amount"], errors="coerce")
            transactions = transactions[~transactions["amount"].isna()]

            # Clean customers
            customers = customers.copy()
            customers.columns = [c.lower() for c in customers.columns]
            customers["customer_id"] = customers["customer_id"].astype(str)
            customers = customers.drop_duplicates(subset=["customer_id"])

            for col in ["full_name", "email"]:
                customers[col] = customers[col].fillna("Unknown").str.strip()
            customers["phone"] = customers["phone"].fillna("0000000000").astype(str)

            # Create date dimension
            transactions["date"] = transactions["timestamp"].dt.date
            dim_date = transactions[["date"]].drop_duplicates().reset_index(drop=True)
            dim_date["year"] = pd.to_datetime(dim_date["date"]).dt.year
            dim_date["month"] = pd.to_datetime(dim_date["date"]).dt.month
            dim_date["day"] = pd.to_datetime(dim_date["date"]).dt.day
            dim_date["date_id"] = dim_date.index + 1

            # Map date_id to transactions
            transactions = transactions.merge(dim_date[["date", "date_id"]], on="date", how="left")

            fact_transactions = transactions[[
                "transaction_id", "customer_id", "date_id", 
                "amount", "transaction_type"
            ]]

            # Print sample merged data for debugging (not loaded to warehouse)
            merged_preview = fact_transactions.merge(customers, on="customer_id", how="left")
            print("\nSample merged transaction + customer records:")
            print(merged_preview.head(10))

            return fact_transactions, customers, dim_date

        except Exception as e:
            print(f"Transformation error: {str(e)}")
            raise

    def load_to_warehouse(self, fact_df, customer_df, date_df):
        try:
            with self.dw_engine.begin() as conn:
                # Load dimension tables with replace (overwrite)
                customer_df.to_sql("dim_customer", conn, if_exists="replace", index=False)
                date_df.to_sql("dim_date", conn, if_exists="replace", index=False)
                # Load fact table with append (or replace if you prefer)
                fact_df.to_sql("fact_transactions", conn, if_exists="append", index=False)
        except Exception as e:
            print(f"Loading error: {str(e)}")
            raise

    def run_etl(self):
        start_time = datetime.now()
        print(f"\nStarting ETL at {start_time}")
        try:
            transactions = self.extract_mongo_transactions()
            customers = self.extract_postgres_customers()
            fact_df, customer_df, date_df = self.transform_data(transactions, customers)
            self.load_to_warehouse(fact_df, customer_df, date_df)
            end_time = datetime.now()
            print(f"ETL completed in {(end_time - start_time).total_seconds():.2f} seconds")
        except Exception as e:
            print(f"ETL failed: {str(e)}")
        finally:
            self.close_connections()

    def close_connections(self):
        if hasattr(self, 'mongo_client'):
            self.mongo_client.close()
        if hasattr(self, 'pg_source_conn'):
            self.pg_source_conn.close()
        if hasattr(self, 'dw_engine'):
            self.dw_engine.dispose()

def run_scheduled_etl(minutes=60):
    scheduler = BlockingScheduler()
    etl = ETLPipeline()

    @scheduler.scheduled_job('interval', minutes=minutes)
    def scheduled_job():
        print("\n" + "="*50)
        print(f"Running scheduled ETL at {datetime.now()}")
        etl.run_etl()

    print(f"Starting ETL scheduler. Runs every {minutes} minutes.")
    scheduler.start()

if __name__ == "__main__":
    ETLPipeline().run_etl()
    # Or schedule it by uncommenting below
    # run_scheduled_etl(minutes=60)
