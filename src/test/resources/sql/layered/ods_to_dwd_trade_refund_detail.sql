WITH refund_event_ranked AS (
    SELECT
        r.dt,
        r.refund_id,
        r.order_id,
        r.refund_status,
        r.refund_amount,
        r.apply_time,
        ROW_NUMBER() OVER (
            PARTITION BY r.refund_id
            ORDER BY r.apply_time DESC
        ) AS rn
    FROM ods.ods_refund_event_di r
    WHERE r.dt = '${biz_date}'
)
INSERT OVERWRITE TABLE dwd.dwd_trade_refund_detail_di
SELECT
    r.dt AS biz_date,
    r.refund_id,
    r.order_id,
    o.shop_id,
    o.user_id,
    COALESCE(r.refund_amount, CAST(0 AS DECIMAL(18,2))) AS refund_amount,
    CASE
        WHEN r.refund_status IN ('SUCCESS', 'CLOSED') THEN 1
        ELSE 0
    END AS refund_finish_flag,
    CASE
        WHEN o.is_paid = 1 AND COALESCE(r.refund_amount, CAST(0 AS DECIMAL(18,2))) > 0 THEN 1
        ELSE 0
    END AS valid_refund_flag
FROM refund_event_ranked r
LEFT JOIN dwd.dwd_trade_order_detail_di o
    ON r.order_id = o.order_id
   AND o.biz_date = '${biz_date}'
WHERE r.rn = 1
