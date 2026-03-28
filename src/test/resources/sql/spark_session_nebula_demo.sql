INSERT OVERWRITE TABLE ads.demo_ads_shop_trade_metric
SELECT
    o.dt AS biz_date,
    o.shop_id,
    s.shop_name,
    COUNT(DISTINCT o.order_id) AS order_cnt,
    COUNT(DISTINCT CASE
        WHEN o.order_status IN ('PAID', 'FINISHED') AND p.pay_time IS NOT NULL THEN o.order_id
        ELSE NULL
    END) AS paid_order_cnt,
    SUM(COALESCE(p.pay_amount, CAST(0 AS DECIMAL(18,2)))) AS paid_gmv,
    COUNT(DISTINCT o.user_id) AS buyer_cnt,
    ROUND(
        COUNT(DISTINCT CASE
            WHEN o.order_status IN ('PAID', 'FINISHED') AND p.pay_time IS NOT NULL THEN o.order_id
            ELSE NULL
        END) / COUNT(DISTINCT o.order_id),
        4
    ) AS pay_rate,
    CASE
        WHEN SUM(COALESCE(p.pay_amount, CAST(0 AS DECIMAL(18,2)))) >= 100000 THEN 'S'
        WHEN SUM(COALESCE(p.pay_amount, CAST(0 AS DECIMAL(18,2)))) >= 10000 THEN 'A'
        WHEN SUM(COALESCE(p.pay_amount, CAST(0 AS DECIMAL(18,2)))) >= 1000 THEN 'B'
        ELSE 'C'
    END AS gmv_band
FROM dwd.demo_fact_order_detail o
LEFT JOIN dwd.demo_fact_payment_detail p
    ON o.order_id = p.order_id
LEFT JOIN dim.demo_dim_shop_snapshot s
    ON o.shop_id = s.shop_id
   AND s.dt = '${biz_date}'
WHERE o.dt = '${biz_date}'
GROUP BY
    o.dt,
    o.shop_id,
    s.shop_name
