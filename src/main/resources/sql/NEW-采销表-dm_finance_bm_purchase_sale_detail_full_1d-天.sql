-------门店回收订单临时表
drop table if EXISTS hdp_zhuanzhuan_tmp_global.tmp_offline_offline_recycle_caixiao_${dateSuffix};
CREATE table if not EXISTS hdp_zhuanzhuan_tmp_global.tmp_offline_offline_recycle_caixiao_${dateSuffix} as
select (total_real_price - coalesce(e2.price,e.price,0)) as md_pur_discount_price,qc_code,order_id from
    (
        --门店回收单
        select
            cast(nvl(get_json_object(remark,'$.quoteId'),'-99') as BIGINT) as quote_id
             ,order_id
             ,seller_id
             ,create_time
             ,pay_time
             ,total_real_price
        from hdp_zhuanzhuan_rawdb_global.raw_trade_t_recycle_order_full_1d        --交易回收主表
        where dt='${outFileSuffix}' and order_source in (46,2706003)
    ) AS orders
        left join
    (
        SELECT  id,
                cast(if(get_json_object(extend,'$.couponPrice') is null,
                        nvl(get_json_object(extend,'$.price'),0)-nvl(get_json_object(extend,'$.baseMinus'),0),
                        nvl(get_json_object(get_json_object(extend,'$.couponPrice'),'$.esprice'),0)+nvl(get_json_object(get_json_object(extend,'$.couponPrice'),'$.couprice'),0)-nvl(get_json_object(get_json_object(extend,'$.couponPrice'),'$.couponContentPrice'),0)) as int) as price
                ,get_json_object(extend,'$.qcCode') as qc_code
        FROM hdp_zhuanzhuan_rawdb_global.raw_t_recycle_extend_full_1d  as b2   --c2b聚合回收扩展信息
        WHERE b2.dt=${today} --这表分区时间晚一天
    ) e on (orders.quote_id = e.id)
        left Join
    (
        --从新逻辑中获取
        select quote_id,sum(price) as price from hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_c2brecycle_t_price_item_full_1d where dt = '${outFileSuffix}' and price_type = 3 group by quote_id
    )e2 on (orders.quote_id = e2.quote_id);




INSERT OVERWRITE TABLE hdp_ubu_zhuanzhuan_dm_c2b.dm_finance_bm_purchase_sale_detail_full_1d partition(dt="${outFileSuffix}")
select t1.skyway_product_id,t1.sku_id,t1.qc_code,t1.admin_quality_no,t1.principal_code,t1.principal_name,t1.pur_source_first_id,t1.pur_source_first_name,t1.pur_source_second_id,t1.pur_source_second_name,t1.pur_source_third_id,t1.pur_source_third_name,t1.pur_order_id,t1.pur_seller_id,t1.pur_buyer_id,t1.pur_order_source,t1.qc_cate_id,t1.qc_brand_id,t1.qc_model_id,if(t1.qc_cate_name = '耳机','耳机/耳麦',t1.qc_cate_name) as qc_cate_name,t1.qc_brand_name,t1.qc_model_name,t1.purchase_time,t1.pur_deal_price,t1.pur_buyer_service_fee,t1.pur_discount_price,t1.pur_raise_price,t1.pur_buyer_act_pay_amt,t1.pur_seller_act_receipt_amt,t3.first_pur_order_id,t3.first_pur_seller_act_receipt_amt,t1.order_source,t1.skyway_state,nvl(t2.skyway_mart_id,0) skyway_mart_id,t2.skyway_mart_name,t2.sale_mart_id,t2.sale_mart_name,if(t2.sale_order_id is null or t2.sale_order_id = 0,null,t2.sale_order_id) sale_order_id,t2.sale_seller_id,t2.sale_buyer_id,t2.sale_info_id,t2.sale_business_line_id,t2.sale_pay_time,nvl(t2.sale_deal_amt,0) sale_deal_amt,nvl(t2.sale_seller_promotion_fee,0) sale_seller_promotion_fee,nvl(t2.sale_seller_brokerage,0) sale_seller_brokerage,nvl(t2.sale_seller_act_receipt_amt,0) sale_seller_act_receipt_amt,nvl(t2.sale_buyer_act_pay_amt,0) sale_buyer_act_pay_amt,nvl(t2.sale_buyer_discount,0) sale_buyer_discount,nvl(t2.sale_sum_service_amt,0) sale_sum_service_amt,t2.is_after_sale_refund,t2.is_pure_pay_success,t2.oms_delivery_time,first_pur_source_third_id,first_pur_source_third_name from
    (
        --采购侧
        select skyway_product_id,sku_id,qc_code,admin_quality_no,principal_code,principal_name,pur_source_first_id,pur_source_first_name,pur_source_second_id,pur_source_second_name,pur_source_third_id,pur_source_third_name,pur_order_id,pur_seller_id,pur_buyer_id,pur_order_source,qc_cate_id,qc_brand_id,qc_model_id,qc_cate_name,qc_brand_name,qc_model_name,purchase_time,pur_deal_price,pur_buyer_service_fee,pur_discount_price,pur_raise_price,pur_buyer_act_pay_amt,pur_seller_act_receipt_amt,order_source,skyway_state from hdp_ubu_zhuanzhuan_dw_c2b.dw_finance_bm_purchase_full_1d where dt = '${outFileSuffix}'
    )t1
        left join
    (
        --销售侧
        select sale_mart_id,sale_mart_name,sale_order_id,sale_seller_id,sale_buyer_id,sale_info_id,sale_business_line_id,sale_pay_time,sale_deal_amt,sale_seller_promotion_fee,sale_seller_brokerage,sale_seller_act_receipt_amt,sale_buyer_act_pay_amt,sale_buyer_discount,sale_sum_service_amt,is_after_sale_refund,is_pure_pay_success,oms_delivery_time,product_id,skyway_mart_id,skyway_mart_name from hdp_ubu_zhuanzhuan_dw_c2b.dw_finance_bm_sale_full_1d where dt = '${outFileSuffix}'
    )t2 on t1.skyway_product_id = t2.product_id
        left join
    (
        select pur_order_id,first_pur_seller_act_receipt_amt,first_pur_order_id,first_pur_source_third_id,first_pur_source_third_name from hdp_ubu_zhuanzhuan_tmp_c2b.tmp_dm_finance_recycle_sale_detail_full_1d_wkx05_${dateSuffix}
    )t3 on t1.pur_order_id = t3.pur_order_id

where t1.pur_seller_id not in
      (509609508868756608,512093937671396864,509947108661966592,495097909269027968/*找靓机测试uid*/,67028177603724288,337807166515757824,50405229526551,287396970430041088,207417883783940096,51168623003236480,51168623003236480,464247619105879296/*转转测试uid*/,142000831087352448,495415012857806720,37725230213390,36376792914454,454150192116545280,113550304284507648,48966529438227,289011233892591104,35661692364951296,357322817248878592,41972219954965,40114893715987,261072460223435648,43124104739093,35301718697487,451251486132358272/*保卖配置uid*/
          ) or t1.pur_seller_id is null
;



INSERT OVERWRITE TABLE hdp_ubu_zhuanzhuan_dm_c2b.dm_finance_bm_purchase_sale_detail_full_1d partition(dt="${outFileSuffix}")
select skyway_product_id,sku_id,pur_qc_code,admin_quality_no,principal_code,principal_name,pur_source_first_id,pur_source_first_name,pur_source_second_id,pur_source_second_name,pur_source_third_id,pur_source_third_name,pur_order_id,pur_seller_id,pur_buyer_id,pur_order_source,qc_cate_id,qc_brand_id,qc_model_id,qc_cate_name,qc_brand_name,qc_model_name,purchase_time,pur_deal_price,pur_buyer_service_fee,nvl(md_pur_discount_price,pur_discount_price) pur_discount_price,pur_raise_price,pur_buyer_act_pay_amt,pur_seller_act_receipt_amt,first_pur_order_id,first_pur_seller_act_receipt_amt,order_source,skyway_state,skyway_mart_id,skyway_mart_name,sale_mart_id,sale_mart_name,sale_order_id,sale_seller_id,sale_buyer_id,sale_info_id,sale_business_line_id,sale_pay_time,sale_deal_amt,sale_seller_promotion_fee,sale_seller_brokerage,sale_seller_act_receipt_amt,sale_buyer_act_pay_amt,sale_buyer_discount,sale_sum_service_price,is_after_sale_refund,is_pure_pay_success,oms_delivery_time,first_pur_source_third_id,first_pur_source_third_name from
    (
        select skyway_product_id,sku_id,pur_qc_code,admin_quality_no,principal_code,principal_name,pur_source_first_id,pur_source_first_name,pur_source_second_id,pur_source_second_name,pur_source_third_id,pur_source_third_name,pur_order_id,pur_seller_id,pur_buyer_id,pur_order_source,qc_cate_id,qc_brand_id,qc_model_id,qc_cate_name,qc_brand_name,qc_model_name,purchase_time,pur_deal_price,pur_buyer_service_fee,pur_discount_price,pur_raise_price,pur_buyer_act_pay_amt,pur_seller_act_receipt_amt,first_pur_order_id,first_pur_seller_act_receipt_amt,order_source,skyway_state,skyway_mart_id,skyway_mart_name,sale_mart_id,sale_mart_name,sale_order_id,sale_seller_id,sale_buyer_id,sale_info_id,sale_business_line_id,sale_pay_time,sale_deal_amt,sale_seller_promotion_fee,sale_seller_brokerage,sale_seller_act_receipt_amt,sale_buyer_act_pay_amt,sale_buyer_discount,sale_sum_service_price,is_after_sale_refund,is_pure_pay_success,oms_delivery_time,first_pur_source_third_id,first_pur_source_third_name from hdp_ubu_zhuanzhuan_dm_c2b.dm_finance_bm_purchase_sale_detail_full_1d where dt = '${outFileSuffix}' and pur_source_third_id = 7  and pur_order_source in (46,2706003)
    )t1
        left join
    (
        --total_real_price/100 - init_price/100  as  pur_discount_price ----回收加价金额
        select md_pur_discount_price,qc_code,order_id from hdp_zhuanzhuan_tmp_global.tmp_offline_offline_recycle_caixiao_${dateSuffix}
    )t2 on t1.pur_order_id = t2.order_id

union


select skyway_product_id,sku_id,pur_qc_code,admin_quality_no,principal_code,principal_name,pur_source_first_id,pur_source_first_name,pur_source_second_id,pur_source_second_name,pur_source_third_id,pur_source_third_name,pur_order_id,pur_seller_id,pur_buyer_id,pur_order_source,qc_cate_id,qc_brand_id,qc_model_id,qc_cate_name,qc_brand_name,qc_model_name,purchase_time,pur_deal_price,pur_buyer_service_fee,pur_discount_price,pur_raise_price,pur_buyer_act_pay_amt,pur_seller_act_receipt_amt,first_pur_order_id,first_pur_seller_act_receipt_amt,order_source,skyway_state,skyway_mart_id,skyway_mart_name,sale_mart_id,sale_mart_name,sale_order_id,sale_seller_id,sale_buyer_id,sale_info_id,sale_business_line_id,sale_pay_time,sale_deal_amt,sale_seller_promotion_fee,sale_seller_brokerage,sale_seller_act_receipt_amt,sale_buyer_act_pay_amt,sale_buyer_discount,sale_sum_service_price,is_after_sale_refund,is_pure_pay_success,oms_delivery_time,first_pur_source_third_id,first_pur_source_third_name from hdp_ubu_zhuanzhuan_dm_c2b.dm_finance_bm_purchase_sale_detail_full_1d where dt = '${outFileSuffix}' and pur_source_third_id = 7  and pur_order_source not in (46,2706003)

union


select skyway_product_id,sku_id,pur_qc_code,admin_quality_no,principal_code,principal_name,pur_source_first_id,pur_source_first_name,pur_source_second_id,pur_source_second_name,pur_source_third_id,pur_source_third_name,pur_order_id,pur_seller_id,pur_buyer_id,pur_order_source,qc_cate_id,qc_brand_id,qc_model_id,qc_cate_name,qc_brand_name,qc_model_name,purchase_time,pur_deal_price,pur_buyer_service_fee,pur_discount_price,pur_raise_price,pur_buyer_act_pay_amt,pur_seller_act_receipt_amt,first_pur_order_id,first_pur_seller_act_receipt_amt,order_source,skyway_state,skyway_mart_id,skyway_mart_name,sale_mart_id,sale_mart_name,sale_order_id,sale_seller_id,sale_buyer_id,sale_info_id,sale_business_line_id,sale_pay_time,sale_deal_amt,sale_seller_promotion_fee,sale_seller_brokerage,sale_seller_act_receipt_amt,sale_buyer_act_pay_amt,sale_buyer_discount,sale_sum_service_price,is_after_sale_refund,is_pure_pay_success,oms_delivery_time,first_pur_source_third_id,first_pur_source_third_name from hdp_ubu_zhuanzhuan_dm_c2b.dm_finance_bm_purchase_sale_detail_full_1d where dt = '${outFileSuffix}' and pur_source_third_id != 7;




drop table if EXISTS hdp_zhuanzhuan_tmp_global.tmp_offline_offline_recycle_caixiao_${sevenDaysBeforeSuffix};
