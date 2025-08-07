
# ETL Pipeline for Financial Data Warehouse

## Overview

This ETL pipeline extracts financial transaction data from MongoDB and customer data from PostgreSQL, transforms it into a star schema-friendly format, and loads it into a PostgreSQL-based data warehouse. The pipeline includes synthetic transaction type injection, data cleaning, date dimension creation, and loading into dimension and fact tables.

It can run once or be scheduled to run at fixed intervals using APScheduler.

---

## How It Works

1. **Configuration**  
   The pipeline loads database connection details and other parameters from a YAML configuration file (`config.yaml`). It requires connection info for MongoDB, PostgreSQL source database, and the PostgreSQL data warehouse.

2. **Extraction**  
   - Transactions are extracted from MongoDB, with a synthetic `transaction_type` field randomly assigned ("credit" or "debit").  
   - Customer data is extracted from PostgreSQL source.

3. **Transformation**  
   - Cleans and validates transaction and customer data.  
   - Converts timestamps and amounts into appropriate formats.  
   - Creates a date dimension table from transaction timestamps.  
   - Prepares fact transactions and dimension tables for loading.

4. **Loading**  
   - Loads dimension tables (`dim_customer`, `dim_date`) replacing existing data.  
   - Appends new transaction records to the fact table (`fact_transactions`) in the data warehouse.

5. **Scheduling** (optional)  
   The pipeline can be scheduled to run repeatedly at defined intervals using the `run_scheduled_etl` function.

---

## Requirements

- Python 3.7+
- PostgreSQL server for source and warehouse databases
- MongoDB server for transactions data
- Python packages:  
  - `pymongo`  
  - `psycopg2`  
  - `pandas`  
  - `sqlalchemy`  
  - `PyYAML`  
  - `APScheduler`

Install required packages using:

```bash
pip install pymongo psycopg2 pandas sqlalchemy PyYAML APScheduler
```

---

## Configuration

Create a `config.yaml` file with the following structure:

```yaml
mongo:
  host: localhost
  port: 27017
  database: your_mongo_db
  collection: your_transaction_collection

postgresql:
  host: localhost
  port: 5432
  database: your_source_db
  user: your_pg_user
  password: your_pg_password

warehouse:
  host: localhost
  port: 5432
  database: your_warehouse_db
  user: your_dw_user
  password: your_dw_password
```

Update connection details as per your environment.

---

## Running the Pipeline

Run the ETL pipeline once with:

```bash
python your_etl_script.py
```

To schedule the ETL to run every N minutes (e.g., every 60 minutes), uncomment and use:

```python
run_scheduled_etl(minutes=60)
```

---

## Logging and Errors

- Errors during config loading, extraction, transformation, or loading will be printed to the console.
- Connections are gracefully closed after each run.
- You can enhance logging by integrating Python’s `logging` module.

---

## Extending the Pipeline

- Add additional transformations or data quality checks.
- Include more dimension and fact tables.
- Implement incremental loads instead of full replacements.
- Enhance synthetic data injection logic.

---

## Contact

For issues or improvements, please contact the maintainer.

---
