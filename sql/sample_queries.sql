-- -- Sample Analytics Queries on Gold Layer
-- -- These queries demonstrate business insights from the Gold layer

-- -- ============================================================================
-- -- REVENUE ANALYTICS
-- -- ============================================================================

-- -- Total revenue by product category and brand
-- SELECT
--     product_category,
--     product_brand,
--     COUNT(*) as purchase_count,
--     SUM(purchase_amount) as total_revenue,
--     AVG(purchase_amount) as avg_order_value,
--     COUNT(DISTINCT user_id) as unique_customers
-- FROM analytics.facts_purchases
-- WHERE purchase_date >= CURRENT_DATE - INTERVAL 30 DAY
-- GROUP BY product_category, product_brand
-- ORDER BY total_revenue DESC;


-- -- Daily revenue trend
-- SELECT
--     event_date,
--     SUM(purchase_amount) as daily_revenue,
--     COUNT(*) as purchase_count,
--     COUNT(DISTINCT user_id) as unique_purchasers,
--     SUM(purchase_amount) / COUNT(*) as avg_order_value
-- FROM analytics.facts_purchases
-- WHERE purchase_date >= CURRENT_DATE - INTERVAL 90 DAY
-- GROUP BY event_date
-- ORDER BY event_date DESC;


-- -- Top 10 revenue-generating products
-- SELECT
--     product_id,
--     product_category,
--     product_brand,
--     COUNT(*) as purchase_count,
--     SUM(purchase_amount) as total_revenue,
--     AVG(purchase_amount) as avg_order_value
-- FROM analytics.facts_purchases
-- WHERE purchase_date >= CURRENT_DATE - INTERVAL 30 DAY
-- GROUP BY product_id, product_category, product_brand
-- ORDER BY total_revenue DESC
-- LIMIT 10;


-- -- ============================================================================
-- -- CONVERSION & FUNNEL ANALYSIS
-- -- ============================================================================

-- -- Conversion funnel by signup channel
-- SELECT
--     user_signup_channel,
--     COUNT(DISTINCT user_id) as unique_users,
--     COUNT(DISTINCT CASE WHEN event_type = 'product_view' THEN user_id END) as viewed_products,
--     COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END) as added_to_cart,
--     COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) as made_purchase,
--     ROUND(
--         COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END) * 100.0 /
--         COUNT(DISTINCT CASE WHEN event_type = 'product_view' THEN user_id END), 2
--     ) as view_to_cart_rate,
--     ROUND(
--         COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) * 100.0 /
--         COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END), 2
--     ) as cart_to_purchase_rate
-- FROM analytics.facts_events
-- WHERE event_date >= CURRENT_DATE - INTERVAL 30 DAY
-- GROUP BY user_signup_channel
-- ORDER BY unique_users DESC;


-- -- Session conversion analysis
-- SELECT
--     event_date,
--     COUNT(*) as total_sessions,
--     SUM(CASE WHEN purchase_count > 0 THEN 1 ELSE 0 END) as sessions_with_purchase,
--     ROUND(
--         SUM(CASE WHEN purchase_count > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
--     ) as session_conversion_rate,
--     AVG(conversion_rate) as avg_session_conversion_rate,
--     AVG(session_duration_minutes) as avg_session_duration_min,
--     AVG(event_count) as avg_events_per_session
-- FROM analytics.facts_user_sessions
-- WHERE event_date >= CURRENT_DATE - INTERVAL 30 DAY
-- GROUP BY event_date
-- ORDER BY event_date DESC;


-- -- ============================================================================
-- -- USER ANALYTICS
-- -- ============================================================================

-- -- User purchase behavior segmentation
-- SELECT
--     CASE
--         WHEN total_purchases >= 10 THEN 'High Value (10+)'
--         WHEN total_purchases >= 5 THEN 'Medium Value (5-9)'
--         WHEN total_purchases >= 2 THEN 'Low Value (2-4)'
--         ELSE 'One-Time Buyer'
--     END as customer_segment,
--     COUNT(*) as customer_count,
--     ROUND(AVG(total_spent), 2) as avg_customer_spend,
--     MIN(total_spent) as min_spend,
--     MAX(total_spent) as max_spend,
--     ROUND(AVG(avg_order_value), 2) as avg_order_value
-- FROM analytics.v_user_purchase_history
-- GROUP BY customer_segment
-- ORDER BY
--     CASE
--         WHEN customer_segment = 'High Value (10+)' THEN 1
--         WHEN customer_segment = 'Medium Value (5-9)' THEN 2
--         WHEN customer_segment = 'Low Value (2-4)' THEN 3
--         ELSE 4
--     END;


-- -- Customer lifetime value (CLV) by signup channel
-- SELECT
--     user_signup_channel,
--     COUNT(*) as customer_count,
--     ROUND(SUM(total_spent), 2) as total_channel_revenue,
--     ROUND(AVG(total_spent), 2) as avg_customer_clv,
--     ROUND(AVG(total_purchases), 2) as avg_purchases_per_customer,
--     ROUND(AVG(avg_order_value), 2) as avg_order_value
-- FROM analytics.v_user_purchase_history
-- GROUP BY user_signup_channel
-- ORDER BY total_channel_revenue DESC;


-- -- Top 20 customers by spend
-- SELECT
--     user_id,
--     user_country,
--     user_signup_channel,
--     total_purchases,
--     ROUND(total_spent, 2) as total_spent,
--     ROUND(avg_order_value, 2) as avg_order_value,
--     last_purchase_date,
--     categories_purchased
-- FROM analytics.v_user_purchase_history
-- ORDER BY total_spent DESC
-- LIMIT 20;


-- -- ============================================================================
-- -- PRODUCT ANALYTICS
-- -- ============================================================================

-- -- Product performance dashboard
-- SELECT
--     product_category,
--     product_brand,
--     product_id,
--     unique_viewers,
--     total_views,
--     total_add_to_cart,
--     total_purchases,
--     ROUND(total_purchases * 100.0 / total_views, 2) as view_to_purchase_rate,
--     ROUND(total_add_to_cart * 100.0 / total_views, 2) as view_to_cart_rate,
--     ROUND(total_revenue, 2) as total_revenue
-- FROM analytics.v_product_performance
-- WHERE total_views > 10
-- ORDER BY total_revenue DESC
-- LIMIT 20;


-- -- Category performance comparison
-- SELECT
--     product_category,
--     COUNT(DISTINCT product_id) as product_count,
--     SUM(unique_viewers) as total_viewers,
--     SUM(total_views) as total_views,
--     SUM(total_purchases) as total_purchases,
--     ROUND(SUM(total_revenue), 2) as total_revenue,
--     ROUND(SUM(total_purchases) * 100.0 / SUM(total_views), 2) as purchase_rate
-- FROM analytics.v_product_performance
-- GROUP BY product_category
-- ORDER BY total_revenue DESC;


-- -- ============================================================================
-- -- MARKETING CHANNEL ANALYTICS
-- -- ============================================================================

-- -- Channel performance by day
-- SELECT
--     event_date,
--     user_signup_channel,
--     total_sessions,
--     unique_users,
--     total_events,
--     ROUND(total_revenue, 2) as revenue,
--     purchase_count,
--     ROUND(total_revenue / NULLIF(purchase_count, 0), 2) as avg_order_value,
--     ROUND(purchase_count * 100.0 / total_sessions, 2) as session_conversion_rate
-- FROM analytics.v_channel_performance
-- WHERE event_date >= CURRENT_DATE - INTERVAL 30 DAY
-- ORDER BY event_date DESC, total_revenue DESC;


-- -- Channel ROI and efficiency
-- SELECT
--     user_signup_channel,
--     COUNT(*) as total_events,
--     COUNT(DISTINCT user_id) as unique_users,
--     COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) as purchasers,
--     ROUND(
--         COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) * 100.0 /
--         COUNT(DISTINCT user_id), 2
--     ) as user_conversion_rate,
--     SUM(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END) as total_revenue,
--     ROUND(
--         SUM(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END) /
--         COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END),
--         2
--     ) as revenue_per_buyer
-- FROM analytics.facts_events
-- WHERE event_date >= CURRENT_DATE - INTERVAL 30 DAY
-- GROUP BY user_signup_channel
-- ORDER BY total_revenue DESC;


-- -- ============================================================================
-- -- DEVICE & LOCATION ANALYTICS
-- -- ============================================================================

-- -- Device performance
-- SELECT
--     device_type,
--     COUNT(*) as event_count,
--     COUNT(DISTINCT user_id) as unique_users,
--     COUNT(DISTINCT session_id) as session_count,
--     COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) as purchasers,
--     ROUND(
--         COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) * 100.0 /
--         COUNT(DISTINCT user_id), 2
--     ) as user_conversion_rate,
--     SUM(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END) as total_revenue
-- FROM analytics.facts_events
-- GROUP BY device_type
-- ORDER BY total_revenue DESC;


-- -- Geographic performance
-- SELECT
--     country,
--     COUNT(*) as total_events,
--     COUNT(DISTINCT user_id) as unique_users,
--     COUNT(DISTINCT session_id) as session_count,
--     SUM(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END) as total_revenue,
--     COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN order_id END) as purchase_count,
--     ROUND(
--         SUM(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END) /
--         COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END),
--         2
--     ) as revenue_per_customer
-- FROM analytics.facts_events
-- WHERE country IS NOT NULL
-- GROUP BY country
-- ORDER BY total_revenue DESC;


-- -- ============================================================================
-- -- SESSION ANALYTICS
-- -- ============================================================================

-- -- Session quality metrics
-- SELECT
--     event_date,
--     COUNT(*) as session_count,
--     AVG(event_count) as avg_events_per_session,
--     AVG(session_duration_minutes) as avg_session_duration_min,
--     PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY session_duration_minutes) as median_duration_min,
--     AVG(CASE WHEN purchase_count > 0 THEN session_duration_minutes END) as avg_duration_purchase_sessions,
--     SUM(CASE WHEN purchase_count > 0 THEN 1 ELSE 0 END) as purchase_sessions,
--     ROUND(
--         SUM(CASE WHEN purchase_count > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
--     ) as session_conversion_rate
-- FROM analytics.facts_user_sessions
-- WHERE event_date >= CURRENT_DATE - INTERVAL 30 DAY
-- GROUP BY event_date
-- ORDER BY event_date DESC;


-- -- Session abandonment analysis
-- SELECT
--     event_date,
--     SUM(CASE WHEN purchase_count = 0 AND add_to_cart_count > 0 THEN 1 ELSE 0 END) as abandoned_carts,
--     SUM(CASE WHEN purchase_count > 0 THEN 1 ELSE 0 END) as completed_purchases,
--     ROUND(
--         SUM(CASE WHEN purchase_count = 0 AND add_to_cart_count > 0 THEN 1 ELSE 0 END) * 100.0 /
--         (SUM(CASE WHEN purchase_count = 0 AND add_to_cart_count > 0 THEN 1 ELSE 0 END) +
--          SUM(CASE WHEN purchase_count > 0 THEN 1 ELSE 0 END)), 2
--     ) as cart_abandonment_rate
-- FROM analytics.facts_user_sessions
-- WHERE event_date >= CURRENT_DATE - INTERVAL 30 DAY
-- GROUP BY event_date
-- ORDER BY event_date DESC;
