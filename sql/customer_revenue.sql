-- Customer Revenue Aggregation Query
-- This query calculates the total transaction amount and count per customer

-- Create or replace the customer_revenue table with aggregated data
CREATE TABLE IF NOT EXISTS customer_revenue AS
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    COALESCE(revenue_data.total_amount, 0) AS total_amount,
    COALESCE(revenue_data.transaction_count, 0) AS transaction_count,
    COALESCE(revenue_data.avg_transaction_amount, 0) AS avg_transaction_amount,
    COALESCE(revenue_data.first_transaction_date, c.registration_date) AS first_transaction_date,
    COALESCE(revenue_data.last_transaction_date, c.registration_date) AS last_transaction_date
FROM customers c
LEFT JOIN (
    SELECT 
        t.customer_id,
        SUM(t.amount) AS total_amount,
        COUNT(*) AS transaction_count,
        ROUND(AVG(t.amount), 2) AS avg_transaction_amount,
        MIN(t.transaction_date) AS first_transaction_date,
        MAX(t.transaction_date) AS last_transaction_date
    FROM transactions t
    GROUP BY t.customer_id
) revenue_data ON c.customer_id = revenue_data.customer_id;

-- Alternative query to get top customers by revenue
SELECT 
    cr.customer_id,
    cr.first_name || ' ' || cr.last_name AS customer_name,
    cr.email,
    cr.total_amount,
    cr.transaction_count,
    cr.avg_transaction_amount,
    CASE 
        WHEN cr.total_amount >= 200 THEN 'High Value'
        WHEN cr.total_amount >= 100 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_segment
FROM customer_revenue cr
WHERE cr.total_amount > 0
ORDER BY cr.total_amount DESC;

-- Query to get revenue by product category
SELECT 
    p.category,
    COUNT(DISTINCT t.customer_id) AS unique_customers,
    SUM(t.amount) AS category_revenue,
    COUNT(*) AS total_transactions,
    ROUND(AVG(t.amount), 2) AS avg_transaction_amount
FROM transactions t
JOIN products p ON t.product_id = p.product_id
GROUP BY p.category
ORDER BY category_revenue DESC;

-- Query to get monthly revenue trends
SELECT 
    strftime('%Y-%m', t.transaction_date) AS month,
    COUNT(DISTINCT t.customer_id) AS unique_customers,
    SUM(t.amount) AS monthly_revenue,
    COUNT(*) AS transaction_count,
    ROUND(AVG(t.amount), 2) AS avg_transaction_amount
FROM transactions t
GROUP BY strftime('%Y-%m', t.transaction_date)
ORDER BY month;

-- Query to identify customers with no transactions
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.registration_date
FROM customers c
LEFT JOIN transactions t ON c.customer_id = t.customer_id
WHERE t.customer_id IS NULL
ORDER BY c.registration_date DESC;
