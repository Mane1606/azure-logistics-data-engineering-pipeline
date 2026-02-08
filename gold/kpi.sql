-----------------deliverycompletionrate---------------------
SELECT
    COUNT(*) AS total_shipments,
    SUM(freight_cost_usd) AS revenue,
    COUNT(DISTINCT customer_id) AS customers,
    ROUND(
        100.0 * SUM(CASE WHEN shipment_status='DELIVERED' THEN 1 END) / COUNT(*),
        2
    ) AS deliverycompletionrate
FROM logistics_catalog.gold.base;



------------------------Revenue by country ----------------
SELECT customer_country, SUM(freight_cost_usd) AS revenue
FROM logistics_catalog.gold.base
GROUP BY customer_country;


------------------------shipment by category----------------
SELECT category, COUNT(*) shipments
FROM logistics_catalog.gold.base
GROUP BY category;

