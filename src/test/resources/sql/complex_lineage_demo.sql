WITH base_order AS (
    SELECT
        o.dt AS biz_date,
        o.shop_id,
        o.order_id,
        o.user_id,
        COALESCE(p.pay_amount, 0D) AS pay_amount,
        CASE
            WHEN o.order_status IN ('PAID', 'FINISHED') AND p.pay_time IS NOT NULL THEN 1
            ELSE 0
        END AS is_paid
    FROM dwd.fact_order_detail o
    LEFT JOIN dwd.fact_payment_detail p
        ON o.order_id = p.order_id
    WHERE o.dt = '${biz_date}'
),
latest_paid_order AS (
    SELECT
        biz_date,
        shop_id,
        order_id,
        user_id,
        pay_amount,
        is_paid
    FROM base_order
),
shop_agg AS (
    SELECT
        biz_date,
        shop_id,
        COUNT(DISTINCT order_id) AS order_cnt,
        COUNT(DISTINCT CASE WHEN is_paid = 1 THEN order_id END) AS paid_order_cnt,
        SUM(pay_amount) AS paid_gmv,
        COUNT(DISTINCT user_id) AS buyer_cnt
    FROM latest_paid_order
    GROUP BY biz_date, shop_id
)
INSERT OVERWRITE TABLE ads.ads_shop_trade_metric_di
PARTITION (dt = '${biz_date}')
SELECT
    a.biz_date,
    a.shop_id,
    s.shop_name,
    a.order_cnt,
    a.paid_order_cnt,
    a.paid_gmv,
    a.buyer_cnt,
    ROUND(a.paid_order_cnt / a.order_cnt, 4) AS pay_rate,
    CASE
        WHEN a.paid_gmv >= 100000 THEN 'S'
        WHEN a.paid_gmv >= 10000 THEN 'A'
        WHEN a.paid_gmv >= 1000 THEN 'B'
        ELSE 'C'
    END AS gmv_band
FROM shop_agg a
LEFT JOIN dim.dim_shop_snapshot s
    ON a.shop_id = s.shop_id
   AND s.dt = '${biz_date}';
