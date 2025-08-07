-- Financial Data Warehouse Analytical Queries
-- Comprehensive SQL File
-- Save as: financial_dw_analytical_queries.sql

-- 1. DAILY TRANSACTION VOLUME AND VALUE
SELECT 
    d.full_date,
    COUNT(*) AS transaction_count,
    SUM(ft.amount) AS total_amount,
    AVG(ft.amount) AS avg_amount
FROM 
    fact_transactions ft
JOIN 
    dim_date d ON ft.transaction_date_id = d.date_id
WHERE 
    d.full_date BETWEEN '2023-01-01' AND '2023-12-31'
GROUP BY 
    d.full_date
ORDER BY 
    d.full_date;

-- 2. TOP CUSTOMERS BY TRANSACTION ACTIVITY
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.segment,
    COUNT(DISTINCT ft.transaction_id) AS transaction_count,
    SUM(ft.amount) AS total_amount,
    SUM(CASE WHEN tt.is_fraud_risk THEN ft.amount ELSE 0 END) AS risky_amount
FROM 
    fact_transactions ft
JOIN 
    dim_account a ON ft.account_id = a.account_id
JOIN 
    dim_customer c ON a.customer_id = c.customer_id
JOIN 
    dim_transaction_type tt ON ft.transaction_type_id = tt.transaction_type_id
GROUP BY 
    c.customer_id, c.first_name, c.last_name, c.segment
ORDER BY 
    total_amount DESC
LIMIT 50;

-- 3. ACCOUNT BALANCE DISTRIBUTION BY TYPE
SELECT 
    a.account_type,
    COUNT(DISTINCT a.account_id) AS account_count,
    AVG(fab.balance) AS avg_balance,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fab.balance) AS median_balance,
    MIN(fab.balance) AS min_balance,
    MAX(fab.balance) AS max_balance
FROM 
    fact_account_balances fab
JOIN 
    dim_account a ON fab.account_id = a.account_id
WHERE 
    fab.date_id = (SELECT date_id FROM dim_date WHERE full_date = CURRENT_DATE)
    AND a.close_date IS NULL
GROUP BY 
    a.account_type
ORDER BY 
    avg_balance DESC;

-- 4. BRANCH PERFORMANCE COMPARISON
SELECT 
    b.branch_name,
    b.region,
    COUNT(DISTINCT ft.transaction_id) AS transaction_count,
    SUM(ft.amount) AS total_amount,
    COUNT(DISTINCT a.customer_id) AS customer_count,
    SUM(fab.balance) AS total_deposits
FROM 
    fact_transactions ft
JOIN 
    dim_branch b ON ft.branch_id = b.branch_id
JOIN 
    dim_account a ON ft.account_id = a.account_id
JOIN 
    fact_account_balances fab ON a.account_id = fab.account_id
WHERE 
    fab.date_id = (SELECT date_id FROM dim_date WHERE full_date = CURRENT_DATE)
GROUP BY 
    b.branch_name, b.region
ORDER BY 
    total_amount DESC;

-- 5. POTENTIAL SUSPICIOUS ACTIVITY
SELECT 
    ft.transaction_id,
    d.full_date,
    a.account_number,
    c.first_name || ' ' || c.last_name AS customer_name,
    tt.type_name,
    ft.amount,
    ft.balance_after,
    b.branch_name
FROM 
    fact_transactions ft
JOIN 
    dim_date d ON ft.transaction_date_id = d.date_id
JOIN 
    dim_account a ON ft.account_id = a.account_id
JOIN 
    dim_customer c ON a.customer_id = c.customer_id
JOIN 
    dim_transaction_type tt ON ft.transaction_type_id = tt.transaction_type_id
JOIN 
    dim_branch b ON ft.branch_id = b.branch_id
WHERE 
    (tt.is_fraud_risk = TRUE OR ft.amount > 10000)
    AND d.full_date BETWEEN CURRENT_DATE - INTERVAL '30 days' AND CURRENT_DATE
ORDER BY 
    ft.amount DESC;

-- 6. CUSTOMER VALUE SEGMENTATION
WITH customer_stats AS (
    SELECT 
        c.customer_id,
        c.segment,
        COUNT(DISTINCT ft.transaction_id) AS transaction_count,
        SUM(ft.amount) AS total_amount,
        SUM(fab.balance) AS current_balance
    FROM 
        dim_customer c
    JOIN 
        dim_account a ON c.customer_id = a.customer_id
    LEFT JOIN 
        fact_transactions ft ON a.account_id = ft.account_id
    JOIN 
        fact_account_balances fab ON a.account_id = fab.account_id
    WHERE 
        fab.date_id = (SELECT date_id FROM dim_date WHERE full_date = CURRENT_DATE)
    GROUP BY 
        c.customer_id, c.segment
)
SELECT 
    segment,
    COUNT(customer_id) AS customer_count,
    AVG(transaction_count) AS avg_transactions,
    AVG(total_amount) AS avg_amount,
    AVG(current_balance) AS avg_balance,
    SUM(current_balance) AS total_balance
FROM 
    customer_stats
GROUP BY 
    segment
ORDER BY 
    total_balance DESC;

-- 7. MONTHLY TRANSACTION TRENDS BY TYPE
SELECT 
    d.year,
    d.month,
    tt.type_name,
    COUNT(*) AS transaction_count,
    SUM(ft.amount) AS total_amount,
    SUM(SUM(ft.amount)) OVER (PARTITION BY tt.type_name ORDER BY d.year, d.month) AS running_total
FROM 
    fact_transactions ft
JOIN 
    dim_date d ON ft.transaction_date_id = d.date_id
JOIN 
    dim_transaction_type tt ON ft.transaction_type_id = tt.transaction_type_id
WHERE 
    d.full_date BETWEEN '2022-01-01' AND '2023-12-31'
GROUP BY 
    d.year, d.month, tt.type_name
ORDER BY 
    d.year, d.month, total_amount DESC;

-- 8. CUSTOMER LIFETIME VALUE
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.customer_since,
    COUNT(DISTINCT ft.transaction_id) AS lifetime_transactions,
    SUM(ft.amount) AS lifetime_value,
    SUM(fab.balance) AS current_balance,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, c.customer_since)) AS years_as_customer,
    SUM(ft.amount) / NULLIF(EXTRACT(YEAR FROM AGE(CURRENT_DATE, c.customer_since)), 0) AS annual_value
FROM 
    dim_customer c
JOIN 
    dim_account a ON c.customer_id = a.customer_id
LEFT JOIN 
    fact_transactions ft ON a.account_id = ft.account_id
JOIN 
    fact_account_balances fab ON a.account_id = fab.account_id
WHERE 
    fab.date_id = (SELECT date_id FROM dim_date WHERE full_date = CURRENT_DATE)
GROUP BY 
    c.customer_id, c.first_name, c.last_name, c.customer_since
ORDER BY 
    lifetime_value DESC
LIMIT 100;

-- 9. DORMANT ACCOUNT ANALYSIS
SELECT 
    a.account_number,
    a.account_type,
    c.first_name || ' ' || c.last_name AS customer_name,
    MAX(d.full_date) AS last_transaction_date,
    CURRENT_DATE - MAX(d.full_date) AS days_inactive,
    fab.balance AS current_balance
FROM 
    dim_account a
JOIN 
    dim_customer c ON a.customer_id = c.customer_id
LEFT JOIN 
    fact_transactions ft ON a.account_id = ft.account_id
LEFT JOIN 
    dim_date d ON ft.transaction_date_id = d.date_id
JOIN 
    fact_account_balances fab ON a.account_id = fab.account_id
WHERE 
    fab.date_id = (SELECT date_id FROM dim_date WHERE full_date = CURRENT_DATE)
    AND a.close_date IS NULL
GROUP BY 
    a.account_number, a.account_type, c.first_name, c.last_name, fab.balance
HAVING 
    MAX(d.full_date) < CURRENT_DATE - INTERVAL '180 days' OR MAX(d.full_date) IS NULL
ORDER BY 
    fab.balance DESC;

-- 10. ACCOUNT TYPE PERFORMANCE BY MONTH
SELECT 
    d.year,
    d.month,
    a.account_type,
    COUNT(DISTINCT a.account_id) AS active_accounts,
    COUNT(DISTINCT ft.transaction_id) AS transaction_count,
    SUM(ft.amount) AS total_amount,
    SUM(fab.balance) AS total_balance
FROM 
    dim_date d
JOIN 
    fact_account_balances fab ON d.date_id = fab.date_id
JOIN 
    dim_account a ON fab.account_id = a.account_id
LEFT JOIN 
    fact_transactions ft ON a.account_id = ft.account_id AND ft.transaction_date_id = d.date_id
WHERE 
    d.full_date BETWEEN '2023-01-01' AND '2023-12-31'
    AND a.close_date IS NULL
GROUP BY 
    d.year, d.month, a.account_type
ORDER BY 
    d.year, d.month, total_amount DESC;

-- 11. REGIONAL TRANSACTION PATTERNS
SELECT 
    b.region,
    d.quarter,
    d.year,
    tt.category,
    COUNT(*) AS transaction_count,
    SUM(ft.amount) AS total_amount,
    SUM(ft.amount) / COUNT(DISTINCT d.date_id) AS avg_daily_amount
FROM 
    fact_transactions ft
JOIN 
    dim_branch b ON ft.branch_id = b.branch_id
JOIN 
    dim_date d ON ft.transaction_date_id = d.date_id
JOIN 
    dim_transaction_type tt ON ft.transaction_type_id = tt.transaction_type_id
WHERE 
    d.full_date BETWEEN '2022-01-01' AND '2023-12-31'
GROUP BY 
    b.region, d.quarter, d.year, tt.category
ORDER BY 
    b.region, d.year, d.quarter, total_amount DESC;

-- 12. CROSS-SELL OPPORTUNITIES
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.segment,
    STRING_AGG(DISTINCT a.account_type, ', ') AS current_products,
    COUNT(DISTINCT a.account_id) AS account_count,
    SUM(fab.balance) AS total_balance
FROM 
    dim_customer c
JOIN 
    dim_account a ON c.customer_id = a.customer_id
JOIN 
    fact_account_balances fab ON a.account_id = fab.account_id
WHERE 
    fab.date_id = (SELECT date_id FROM dim_date WHERE full_date = CURRENT_DATE)
    AND a.close_date IS NULL
GROUP BY 
    c.customer_id, c.first_name, c.last_name, c.segment
HAVING 
    COUNT(DISTINCT a.account_type) = 1
    AND SUM(fab.balance) > 10000
ORDER BY 
    total_balance DESC;