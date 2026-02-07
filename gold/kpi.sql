SELECT
    COUNT(*) AS total_shipments,
    SUM(freight_cost_usd) AS revenue,
    COUNT(DISTINCT customer_id) AS customers,
    ROUND(
        100.0 * SUM(CASE WHEN shipment_status='DELIVERED' THEN 1 END) / COUNT(*),
        2
    ) AS deliverycompletionrate
FROM logistics_catalog.gold.base;
