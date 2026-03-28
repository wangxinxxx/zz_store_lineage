INSERT OVERWRITE TABLE dws.dws_user_trade_day_1d
SELECT
    o.biz_date,
    o.user_id,
    MAX(o.shop_id) AS last_shop_id,
    COUNT(DISTINCT CASE WHEN o.is_paid = 1 THEN o.order_id ELSE NULL END) AS paid_order_cnt,
    SUM(CASE WHEN o.is_paid = 1 THEN o.pay_amount ELSE CAST(0 AS DECIMAL(18,2)) END) AS paid_gmv,
    MAX(CASE WHEN o.amount_band = 'high' THEN 1 ELSE 0 END) AS has_high_value_order,
    MAX(CASE WHEN u.member_level IN ('VIP', 'SVIP') THEN 1 ELSE 0 END) AS vip_user_flag
FROM dwd.dwd_trade_order_detail_di o
LEFT JOIN dim.dim_user_tag_snapshot u
    ON o.user_id = u.user_id
   AND u.dt = '${biz_date}'
WHERE o.biz_date = '${biz_date}'
GROUP BY
    o.biz_date,
    o.user_id
