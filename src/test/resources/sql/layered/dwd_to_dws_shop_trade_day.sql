INSERT OVERWRITE TABLE dws.dws_shop_trade_day_1d
SELECT
    o.biz_date,
    o.shop_id,
    MAX(o.shop_name) AS shop_name,
    MAX(o.region_name) AS region_name,
    COUNT(DISTINCT o.order_id) AS order_cnt,
    COUNT(DISTINCT CASE WHEN o.is_paid = 1 THEN o.order_id ELSE NULL END) AS paid_order_cnt,
    SUM(CASE WHEN o.is_paid = 1 THEN o.pay_amount ELSE CAST(0 AS DECIMAL(18,2)) END) AS paid_gmv,
    COUNT(DISTINCT o.user_id) AS buyer_cnt,
    COUNT(DISTINCT CASE WHEN r.valid_refund_flag = 1 THEN r.refund_id ELSE NULL END) AS refund_cnt,
    SUM(CASE WHEN r.valid_refund_flag = 1 THEN r.refund_amount ELSE CAST(0 AS DECIMAL(18,2)) END) AS refund_amount,
    ROUND(
        SUM(CASE WHEN o.is_paid = 1 THEN o.pay_amount ELSE CAST(0 AS DECIMAL(18,2)) END)
            / COUNT(DISTINCT CASE WHEN o.is_paid = 1 THEN o.order_id ELSE NULL END),
        2
    ) AS avg_paid_order_amount
FROM dwd.dwd_trade_order_detail_di o
LEFT JOIN dwd.dwd_trade_refund_detail_di r
    ON o.order_id = r.order_id
   AND r.biz_date = '${biz_date}'
WHERE o.biz_date = '${biz_date}'
GROUP BY
    o.biz_date,
    o.shop_id
