
# Financial Data Warehouse - Star Schema Implementation

## Overview

This project implements a star schema data warehouse for financial data using PostgreSQL. It integrates core financial dimensions and fact tables designed for analytics and reporting in the financial industry.

The schema supports transaction tracking, account balances, customer profiling, branch details, and transaction classification. It also includes security roles, data quality checks, and sample analytical queries.

---

## Features

- **Star Schema Design** with fact and dimension tables
- **Dimension Tables**: Date, Customer, Account, Branch, Transaction Type
- **Fact Tables**: Transactions and Account Balances snapshots
- **Data Quality Checks** and validation support
- **Views and Materialized Views** for reporting convenience and performance
- **Role-Based Security** for controlled access (Analyst, Manager, Admin)
- Sample data insertion for transaction types and date dimension
- Comprehensive documentation comments within the schema

---

## Database Setup

1. **Create Database and Connect**

```sql
CREATE DATABASE financial_dw;
\c financial_dw;
```

2. **Run the SQL Script**

Execute the provided SQL script `financial_dw_star_schema.sql` to create all tables, indexes, roles, views, and initial data.

---

## Usage

### Sample Analytical Queries

- **Daily Transaction Volume by Type**

```sql
SELECT 
    d.full_date,
    tt.type_name,
    COUNT(*) as transaction_count,
    SUM(ft.amount) as total_amount
FROM fact_transactions ft
JOIN dim_date d ON ft.transaction_date_id = d.date_id
JOIN dim_transaction_type tt ON ft.transaction_type_id = tt.transaction_type_id
GROUP BY d.full_date, tt.type_name
ORDER BY d.full_date, total_amount DESC;
```

- **Customer Portfolio Analysis**

```sql
SELECT 
    c.customer_id,
    c.last_name || ', ' || c.first_name as customer_name,
    c.segment,
    COUNT(DISTINCT a.account_id) as account_count,
    SUM(fab.balance) as total_balance
FROM dim_customer c
JOIN dim_account a ON c.customer_id = a.customer_id
JOIN fact_account_balances fab ON a.account_id = fab.account_id
WHERE fab.date_id = (SELECT date_id FROM dim_date WHERE full_date = CURRENT_DATE)
GROUP BY c.customer_id, c.last_name, c.first_name, c.segment
ORDER BY total_balance DESC
LIMIT 100;
```

---

## Security and Access Control

- **Roles Created:**

  - `finance_analyst`: Read-only access
  - `finance_manager`: Read and write access
  - `finance_admin`: Full privileges

- Permissions are granted to ensure secure access based on job responsibilities.

---

## Data Quality Checks

A table `data_quality_checks` tracks validation rules such as:

- Missing transaction dates
- Negative balances
- Orphaned account references

These checks can be automated to maintain data integrity.

---

## Notes

- The `dim_customer.ssn_encrypted` column stores encrypted social security numbers; access is masked via views.
- Date dimension populated for 5 years from the current date.
- The star schema is optimized for query performance using indexes and materialized views.
- Modify the schema versioning table `schema_version` to track schema changes.

---

## Requirements

- PostgreSQL 12 or higher recommended
- Sufficient permissions to create databases, tables, roles, and views

---

## How to Run

1. Ensure PostgreSQL server is installed and running.
2. Run the `financial_dw_star_schema.sql` script using psql or any PostgreSQL client.

```bash
psql -U your_username -f financial_dw_star_schema.sql
```

3. Connect to the `financial_dw` database and begin querying.

---

## License

This project is open for modification and extension under your organization’s policies.

---

## Contact

For questions or contributions, contact the maintainer.

---
