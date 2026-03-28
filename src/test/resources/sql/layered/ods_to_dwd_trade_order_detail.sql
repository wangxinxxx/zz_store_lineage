WITH order_event_ranked AS (
    SELECT
        e.dt,
        e.order_id,
        e.shop_id,
        e.user_id,
        e.order_status,
        e.order_amount,
        e.discount_amount,
        e.event_time,
        ROW_NUMBER() OVER (
            PARTITION BY e.order_id
            ORDER BY e.event_time DESC
        ) AS rn
    FROM ods.ods_order_event_di e
    WHERE e.dt = '${biz_date}'
),
pay_event_agg AS (
    SELECT
        p.order_id,
        MAX(p.pay_time) AS pay_time,
        SUM(CASE WHEN p.pay_status = 'SUCCESS' THEN p.pay_amount ELSE CAST(0 AS DECIMAL(18,2)) END) AS pay_amount,
        COUNT(DISTINCT CASE WHEN p.pay_status = 'SUCCESS' THEN p.pay_id ELSE NULL END) AS pay_success_cnt
    FROM ods.ods_pay_event_di p
    WHERE p.dt = '${biz_date}'
    GROUP BY p.order_id
),
shop_snapshot AS (
    SELECT
        s.shop_id,
        s.shop_name,
        s.region_name,
        s.shop_type
    FROM dim.dim_shop_region_snapshot s
    WHERE s.dt = '${biz_date}'
)
INSERT OVERWRITE TABLE dwd.dwd_trade_order_detail_di
SELECT
    o.dt AS biz_date,
    o.order_id,
    o.shop_id,
    s.shop_name,
    s.region_name,
    o.user_id,
    o.order_status,
    COALESCE(o.order_amount, CAST(0 AS DECIMAL(18,2))) AS order_amount,
    COALESCE(o.discount_amount, CAST(0 AS DECIMAL(18,2))) AS discount_amount,
    COALESCE(p.pay_amount, CAST(0 AS DECIMAL(18,2))) AS pay_amount,
    COALESCE(p.pay_success_cnt, 0) AS pay_success_cnt,
    CASE
        WHEN p.pay_time IS NOT NULL THEN 1
        ELSE 0
    END AS is_paid,
    CASE
        WHEN o.order_status IN ('SIGNED', 'FINISHED') THEN 1
        ELSE 0
    END AS is_signed,
    CASE
        WHEN COALESCE(p.pay_amount, CAST(0 AS DECIMAL(18,2))) >= 500 THEN 'high'
        WHEN COALESCE(p.pay_amount, CAST(0 AS DECIMAL(18,2))) >= 100 THEN 'mid'
        ELSE 'low'
    END AS amount_band
FROM order_event_ranked o
LEFT JOIN pay_event_agg p
    ON o.order_id = p.order_id
LEFT JOIN shop_snapshot s
    ON o.shop_id = s.shop_id
WHERE o.rn = 1
