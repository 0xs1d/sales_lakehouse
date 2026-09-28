-- ============================================================
-- Enterprise Sales Lakehouse
-- Business Analytics SQL Queries
-- ============================================================

-- 1. Overall Sales KPIs
SELECT
    ROUND(SUM(net_amount), 2) AS total_revenue,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(quantity) AS total_units_sold,
    ROUND(
        SUM(net_amount) / COUNT(DISTINCT order_id),
        2
    ) AS average_order_value
FROM gold_fact_sales;


-- 2. Monthly Revenue Trend
SELECT
    YEAR(order_date) AS sales_year,
    MONTH(order_date) AS sales_month,
    ROUND(SUM(net_amount), 2) AS monthly_revenue,
    COUNT(DISTINCT order_id) AS monthly_orders,
    SUM(quantity) AS monthly_units_sold
FROM gold_fact_sales
GROUP BY
    YEAR(order_date),
    MONTH(order_date)
ORDER BY
    sales_year,
    sales_month;


-- 3. Top Products by Revenue
SELECT
    p.product_id,
    p.product_name,
    p.category,
    SUM(f.quantity) AS units_sold,
    ROUND(SUM(f.net_amount), 2) AS revenue,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM gold_fact_sales f
JOIN gold_dim_product p
    ON f.product_id = p.product_id
GROUP BY
    p.product_id,
    p.product_name,
    p.category
ORDER BY
    revenue DESC
LIMIT 10;


-- 4. Regional Sales Performance
SELECT
    s.region,
    ROUND(SUM(f.net_amount), 2) AS revenue,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS units_sold
FROM gold_fact_sales f
JOIN gold_dim_store s
    ON f.store_id = s.store_id
GROUP BY
    s.region
ORDER BY
    revenue DESC;


-- 5. Store Performance
SELECT
    s.store_id,
    s.store_name,
    s.city,
    s.region,
    ROUND(SUM(f.net_amount), 2) AS revenue,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS units_sold
FROM gold_fact_sales f
JOIN gold_dim_store s
    ON f.store_id = s.store_id
GROUP BY
    s.store_id,
    s.store_name,
    s.city,
    s.region
ORDER BY
    revenue DESC;


-- 6. Customer Revenue Ranking
SELECT
    c.customer_id,
    c.customer_name,
    c.customer_segment,
    c.region,
    ROUND(SUM(f.net_amount), 2) AS customer_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS units_purchased
FROM gold_fact_sales f
JOIN gold_dim_customer c
    ON f.customer_id = c.customer_id
GROUP BY
    c.customer_id,
    c.customer_name,
    c.customer_segment,
    c.region
ORDER BY
    customer_revenue DESC
LIMIT 20;


-- 7. Revenue by Customer Segment
SELECT
    c.customer_segment,
    ROUND(SUM(f.net_amount), 2) AS revenue,
    COUNT(DISTINCT f.customer_id) AS customers,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM gold_fact_sales f
JOIN gold_dim_customer c
    ON f.customer_id = c.customer_id
GROUP BY
    c.customer_segment
ORDER BY
    revenue DESC;


-- 8. Revenue by Product Category
SELECT
    p.category,
    ROUND(SUM(f.net_amount), 2) AS revenue,
    SUM(f.quantity) AS units_sold,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM gold_fact_sales f
JOIN gold_dim_product p
    ON f.product_id = p.product_id
GROUP BY
    p.category
ORDER BY
    revenue DESC;


-- 9. Average Selling Price by Product
SELECT
    p.product_id,
    p.product_name,
    p.category,
    ROUND(AVG(f.unit_price), 2) AS average_selling_price,
    SUM(f.quantity) AS units_sold,
    ROUND(SUM(f.net_amount), 2) AS revenue
FROM gold_fact_sales f
JOIN gold_dim_product p
    ON f.product_id = p.product_id
GROUP BY
    p.product_id,
    p.product_name,
    p.category
ORDER BY
    revenue DESC;


-- 10. Daily Sales Performance
SELECT
    order_date,
    ROUND(SUM(net_amount), 2) AS daily_revenue,
    COUNT(DISTINCT order_id) AS daily_orders,
    SUM(quantity) AS daily_units_sold
FROM gold_fact_sales
GROUP BY
    order_date
ORDER BY
    order_date;


-- 11. Discount Impact Analysis
SELECT
    discount_pct,
    COUNT(*) AS sales_lines,
    SUM(quantity) AS units_sold,
    ROUND(SUM(gross_amount), 2) AS gross_revenue,
    ROUND(SUM(net_amount), 2) AS net_revenue,
    ROUND(
        SUM(gross_amount) - SUM(net_amount),
        2
    ) AS discount_value
FROM gold_fact_sales
GROUP BY
    discount_pct
ORDER BY
    discount_pct;


-- 12. Top Customers by Average Order Value
WITH customer_orders AS (
    SELECT
        customer_id,
        order_id,
        SUM(net_amount) AS order_value
    FROM gold_fact_sales
    GROUP BY
        customer_id,
        order_id
)
SELECT
    customer_id,
    COUNT(order_id) AS total_orders,
    ROUND(SUM(order_value), 2) AS total_revenue,
    ROUND(AVG(order_value), 2) AS average_order_value
FROM customer_orders
GROUP BY
    customer_id
ORDER BY
    average_order_value DESC
LIMIT 20;


-- 13. Region and Category Matrix
SELECT
    s.region,
    p.category,
    ROUND(SUM(f.net_amount), 2) AS revenue,
    SUM(f.quantity) AS units_sold,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM gold_fact_sales f
JOIN gold_dim_store s
    ON f.store_id = s.store_id
JOIN gold_dim_product p
    ON f.product_id = p.product_id
GROUP BY
    s.region,
    p.category
ORDER BY
    s.region,
    revenue DESC;


-- 14. Data Quality Validation Query
SELECT
    COUNT(*) AS total_fact_records,
    SUM(
        CASE
            WHEN order_id IS NULL
              OR customer_id IS NULL
              OR product_id IS NULL
              OR store_id IS NULL
            THEN 1
            ELSE 0
        END
    ) AS null_key_records,
    SUM(
        CASE
            WHEN quantity <= 0
              OR unit_price < 0
              OR net_amount < 0
            THEN 1
            ELSE 0
        END
    ) AS invalid_business_records
FROM gold_fact_sales;


-- 15. Pipeline Audit Summary
SELECT
    pipeline_stage,
    layer,
    status,
    execution_timestamp
FROM audit_pipeline_runs
ORDER BY
    execution_timestamp,
    pipeline_stage;


-- 16. Monitoring Health Summary
SELECT
    monitoring_check,
    actual_value,
    expected_value,
    status
FROM audit_monitoring_health
ORDER BY
    monitoring_check;


-- ============================================================
-- End of Business Analytics Queries
-- ============================================================
