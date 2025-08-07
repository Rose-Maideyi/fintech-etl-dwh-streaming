
-- Financial Data Warehouse Star Schema Implementation
-- PostgreSQL Script
-- Save this as financial_dw_star_schema.sql

-- 1. Database Setup
CREATE DATABASE financial_dw;

\c financial_dw;

-- 2. Create Dimension Tables

-- Date Dimension
CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE NOT NULL,
    day INT NOT NULL,
    month INT NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    day_of_week INT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE(full_date)
);

COMMENT ON TABLE dim_date IS 'Master date dimension for all fact tables';
COMMENT ON COLUMN dim_date.is_holiday IS 'Flag for banking holidays';

-- Customer Dimension
CREATE TABLE dim_customer (
    customer_id SERIAL PRIMARY KEY,
    customer_code VARCHAR(20) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    birth_date DATE,
    ssn_encrypted VARCHAR(256),
    email VARCHAR(100),
    phone VARCHAR(20),
    customer_since DATE,
    segment VARCHAR(50),
    risk_level VARCHAR(20),
    UNIQUE(customer_code)
);

COMMENT ON TABLE dim_customer IS 'Customer master data with PII';
COMMENT ON COLUMN dim_customer.ssn_encrypted IS 'Encrypted social security number';

-- Account Dimension
CREATE TABLE dim_account (
    account_id SERIAL PRIMARY KEY,
    account_number VARCHAR(20) NOT NULL,
    customer_id INT REFERENCES dim_customer(customer_id),
    account_type VARCHAR(50) NOT NULL,
    open_date DATE NOT NULL,
    close_date DATE,
    status VARCHAR(20),
    branch_id INT,
    UNIQUE(account_number)
);

COMMENT ON TABLE dim_account IS 'Financial accounts with relationship to customers';

-- Branch Dimension
CREATE TABLE dim_branch (
    branch_id SERIAL PRIMARY KEY,
    branch_code VARCHAR(10) NOT NULL,
    branch_name VARCHAR(100) NOT NULL,
    address VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    region VARCHAR(50),
    manager_id INT,
    UNIQUE(branch_code)
);

COMMENT ON TABLE dim_branch IS 'Bank branch locations and hierarchy';

-- Transaction Type Dimension
CREATE TABLE dim_transaction_type (
    transaction_type_id SERIAL PRIMARY KEY,
    type_code VARCHAR(10) NOT NULL,
    type_name VARCHAR(50) NOT NULL,
    category VARCHAR(50),
    is_fraud_risk BOOLEAN DEFAULT FALSE,
    UNIQUE(type_code)
);

COMMENT ON TABLE dim_transaction_type IS 'Transaction classification system';

-- 3. Create Fact Tables

-- Transaction Fact Table
CREATE TABLE fact_transactions (
    transaction_id BIGSERIAL PRIMARY KEY,
    transaction_date_id INT REFERENCES dim_date(date_id),
    account_id INT REFERENCES dim_account(account_id),
    branch_id INT REFERENCES dim_branch(branch_id),
    transaction_type_id INT REFERENCES dim_transaction_type(transaction_type_id),
    amount DECIMAL(15,2) NOT NULL,
    balance_after DECIMAL(15,2),
    teller_id INT,
    reference_number VARCHAR(50),
    is_suspicious BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMP,
    CONSTRAINT positive_amount CHECK (amount != 0)
);

COMMENT ON TABLE fact_transactions IS 'Core transaction facts with financial events';
COMMENT ON COLUMN fact_transactions.is_suspicious IS 'Flag for potentially fraudulent transactions';

-- Account Balance Snapshot Fact Table
CREATE TABLE fact_account_balances (
    snapshot_id BIGSERIAL PRIMARY KEY,
    account_id INT REFERENCES dim_account(account_id),
    date_id INT REFERENCES dim_date(date_id),
    balance DECIMAL(15,2) NOT NULL,
    available_balance DECIMAL(15,2),
    credit_limit DECIMAL(15,2),
    status VARCHAR(20),
    UNIQUE(account_id, date_id)
);

COMMENT ON TABLE fact_account_balances IS 'Daily snapshot of account balances';

-- 4. Create Indexes for Performance
CREATE INDEX idx_fact_transactions_date ON fact_transactions(transaction_date_id);
CREATE INDEX idx_fact_transactions_account ON fact_transactions(account_id);
CREATE INDEX idx_fact_transactions_type ON fact_transactions(transaction_type_id);
CREATE INDEX idx_fact_transactions_amount ON fact_transactions(amount);
CREATE INDEX idx_fact_balances_account ON fact_account_balances(account_id);
CREATE INDEX idx_fact_balances_date ON fact_account_balances(date_id);
CREATE INDEX idx_customer_segment ON dim_customer(segment);
CREATE INDEX idx_account_type ON dim_account(account_type);
CREATE INDEX idx_account_status ON dim_account(status);

-- 5. Sample Data Population

-- Insert Transaction Types
INSERT INTO dim_transaction_type (type_code, type_name, category, is_fraud_risk)
VALUES 
    ('DEP', 'Deposit', 'Credit', FALSE),
    ('WDL', 'Withdrawal', 'Debit', FALSE),
    ('TRF', 'Transfer', 'Debit', TRUE),
    ('PMT', 'Payment', 'Debit', FALSE),
    ('FEE', 'Fee', 'Debit', FALSE),
    ('INT', 'Interest', 'Credit', FALSE);

-- Populate Date Dimension (5 years of dates)
INSERT INTO dim_date (full_date, day, month, quarter, year, day_of_week, is_weekend)
SELECT 
    date,
    EXTRACT(DAY FROM date)::int,
    EXTRACT(MONTH FROM date)::int,
    EXTRACT(QUARTER FROM date)::int,
    EXTRACT(YEAR FROM date)::int,
    EXTRACT(DOW FROM date)::int,
    EXTRACT(DOW FROM date) IN (0,6)
FROM generate_series(
    CURRENT_DATE - INTERVAL '5 years',
    CURRENT_DATE,
    INTERVAL '1 day'
) AS date;

-- 6. Create Views for Security and Convenience

-- Customer View with Masked PII
CREATE VIEW vw_customer_masked AS
SELECT 
    customer_id,
    first_name,
    last_name,
    '***-**-' || RIGHT(ssn_encrypted, 4) as ssn_masked,
    email,
    customer_since,
    segment,
    risk_level
FROM dim_customer;

-- Transaction Summary View
CREATE VIEW vw_transaction_summary AS
SELECT 
    d.full_date,
    a.account_number,
    c.last_name || ', ' || c.first_name as customer_name,
    tt.type_name,
    ft.amount,
    ft.balance_after,
    b.branch_name
FROM 
    fact_transactions ft
JOIN dim_date d ON ft.transaction_date_id = d.date_id
JOIN dim_account a ON ft.account_id = a.account_id
JOIN dim_customer c ON a.customer_id = c.customer_id
JOIN dim_transaction_type tt ON ft.transaction_type_id = tt.transaction_type_id
JOIN dim_branch b ON ft.branch_id = b.branch_id;

-- 7. Create Materialized Views for Reporting

-- Monthly Transaction Summary
CREATE MATERIALIZED VIEW mv_monthly_transaction_summary AS
SELECT 
    d.year,
    d.month,
    tt.type_name,
    COUNT(*) as transaction_count,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_amount
FROM 
    fact_transactions ft
JOIN dim_date d ON ft.transaction_date_id = d.date_id
JOIN dim_transaction_type tt ON ft.transaction_type_id = tt.transaction_type_id
GROUP BY d.year, d.month, tt.type_name;

-- 8. Security and Access Control

-- Create Roles
CREATE ROLE finance_analyst;
CREATE ROLE finance_manager;
CREATE ROLE finance_admin;

-- Grant Permissions
GRANT CONNECT ON DATABASE financial_dw TO finance_analyst, finance_manager, finance_admin;

-- Analyst Permissions (Read-only)
GRANT USAGE ON SCHEMA public TO finance_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO finance_analyst;
GRANT SELECT ON vw_customer_masked TO finance_analyst;

-- Manager Permissions (Read + Write)
GRANT USAGE ON SCHEMA public TO finance_manager;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO finance_manager;
GRANT SELECT ON vw_customer_masked TO finance_manager;

-- Admin Permissions (Full access)
GRANT ALL PRIVILEGES ON DATABASE financial_dw TO finance_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO finance_admin;

-- 9. Data Quality Checks

-- Create Data Validation Table
CREATE TABLE data_quality_checks (
    check_id SERIAL PRIMARY KEY,
    check_name VARCHAR(100) NOT NULL,
    check_query TEXT NOT NULL,
    severity VARCHAR(20) NOT NULL,
    last_run TIMESTAMP,
    last_result BOOLEAN
);

-- Insert Sample Data Quality Checks
INSERT INTO data_quality_checks (check_name, check_query, severity)
VALUES
    ('Missing Transaction Dates', 
     'SELECT COUNT(*) FROM fact_transactions WHERE transaction_date_id IS NULL', 
     'CRITICAL'),
     
    ('Negative Balances', 
     'SELECT COUNT(*) FROM fact_account_balances WHERE balance < 0', 
     'HIGH'),
     
    ('Orphaned Account References', 
     'SELECT COUNT(*) FROM fact_transactions ft LEFT JOIN dim_account a ON ft.account_id = a.account_id WHERE a.account_id IS NULL', 
     'HIGH');

-- 10. Sample Analytical Queries

-- Daily Transaction Volume by Type
-- This query shows transaction patterns over time
/* 
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
*/

-- Customer Portfolio Analysis
-- This query helps identify high-value customers
/* 
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
*/

-- 11. Documentation Comments

-- Database Documentation
COMMENT ON DATABASE financial_dw IS 'Financial Data Warehouse - Star Schema Implementation';

-- Schema Version Tracking
CREATE TABLE schema_version (
    version_id SERIAL PRIMARY KEY,
    version_number VARCHAR(20) NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    description TEXT
);

INSERT INTO schema_version (version_number, description)
VALUES ('1.0.0', 'Initial star schema implementation');
