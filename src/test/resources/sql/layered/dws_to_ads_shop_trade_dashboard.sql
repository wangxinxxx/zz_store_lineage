INSERT OVERWRITE TABLE ads.ads_shop_trade_dashboard_di
SELECT
    s.biz_date,
    s.shop_id,
    s.shop_name,
    s.region_name,
    m.operation_owner,
    m.city_tier,
    s.order_cnt,
    s.paid_order_cnt,
    s.paid_gmv,
    s.refund_amount,
    s.paid_gmv - s.refund_amount AS net_paid_gmv,
    COUNT(DISTINCT u.user_id) AS active_buyer_cnt,
    SUM(CASE WHEN u.vip_user_flag = 1 THEN 1 ELSE 0 END) AS vip_buyer_cnt,
    ROUND(s.paid_order_cnt / s.order_cnt, 4) AS pay_rate,
    ROUND(s.refund_amount / s.paid_gmv, 4) AS refund_rate,
    CASE
        WHEN s.paid_gmv >= 10000 THEN 'S'
        WHEN s.paid_gmv >= 3000 THEN 'A'
        WHEN s.paid_gmv >= 1000 THEN 'B'
        ELSE 'C'
    END AS gmv_band
FROM dws.dws_shop_trade_day_1d s
LEFT JOIN dws.dws_user_trade_day_1d u
    ON s.shop_id = u.last_shop_id
   AND s.biz_date = u.biz_date
LEFT JOIN dim.dim_shop_region_snapshot m
    ON s.shop_id = m.shop_id
   AND m.dt = '${biz_date}'
WHERE s.biz_date = '${biz_date}'
GROUP BY
    s.biz_date,
    s.shop_id,
    s.shop_name,
    s.region_name,
    m.operation_owner,
    m.city_tier,
    s.order_cnt,
    s.paid_order_cnt,
    s.paid_gmv,
    s.refund_amount
