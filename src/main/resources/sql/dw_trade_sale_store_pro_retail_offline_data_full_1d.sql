


SET spark.sql.adaptive.enabled=false;


--drop table if exists hdp_ubu_zhuanzhuan_tmp_c2b.tmp_pro_store_sale_order_info;
--create table hdp_ubu_zhuanzhuan_tmp_c2b.tmp_pro_store_sale_order_info as
insert overwrite table hdp_ubu_zhuanzhuan_dw_c2b.dw_trade_sale_store_pro_retail_offline_data_full_1d partition(dt='${outFileSuffix}')
select
/*+ COALESCE(1) */
    t3.province
     ,t3.region
     ,t3.first_opening_time
     ,t3.store_type
     ,t3.store_type_name
     ,t1.order_id
     ,t2.order_id as child_order_id
     ,t1.order_type
     ,case when t1.order_type is null then ''
           when t1.order_type = 1  then '门店订单'
           when t1.order_type = 3  then '同售订单'
           when t1.order_type = 2  then '线下销售'
           when t1.order_type = 4  then '严选订单'
           when t1.order_type = 5  then '小时达订单'
           else '' end as order_type_desc
     ,t1.state
     ,case when t1.state = 10 then "已下单"
           when t1.state = 20 then "已完成"
           when t1.state = 30 then "已关闭"
           else "" end as state_desc
     ,t1.create_time
     ,t1.info_id
     ,t1.qc_code
     ,t1.cate_id
     ,t1.cate_name
     ,t1.buyer_id
     ,t1.seller_id
     ,t1.city_id
     ,t1.city_name
     ,t1.store_id
     ,t1.store_name
     ,t1.staff_id
     ,t8.uid as staff_uid
     ,t8.email as staff_email
     ,t1.staff_name
     ,t1.deal_price
     ,t1.pay_type
     ,case when t1.pay_type = 0 then "微信支付"
           when t1.pay_type = 6  then "京东支付"
           when t1.pay_type = 10 then "花呗支付"
           when t1.pay_type = 103 or  t1.pay_type = 106 then "支付宝支付"
           else "其它" end as pay_type_desc
     ,if(t9.order_id is not null or t10.product_id is  not null,1,0) as is_old_change
     ,t2.refund_time
     ,t1.info_title
     ,t1.points_money
     ,t1.update_time
     ,t1.finish_time
     ,t1.info_num
     ,t1.pack_amount
     ,t1.pay_time
     ,t1.market_channel
     ,case when t2.is_afs = 1 then t2.create_time
    end as afs_create_time
     ,case when t2.is_afs = 1 then t2.finish_time
    end as afs_finish_time
     ,case when t2.is_afs = 1 then t2.responsibility_type
    end as afs_responsibility_type
     ,case when t2.is_afs = 1 then t2.compensation_amount
    end as compensation_amount
     ,nvl(t2.is_afs,0) as is_afs

     ,t4.storage_time
     ,case when t4.purchase_channels = 8 then t7.recheck_time
           else t4.outbound_time
    end as outbound_time
     ,t4.storage_age
     ,t4.capacity_desc
     ,t4.colour_name
     ,t4.product_channel
     ,t4.appearance_condition_id
     ,t4.appearance_condition_name
     ,t4.functional_condition_id
     ,t4.functional_condition_name
     ,t4.condition_desc
     ,t4.imei
     ,t4.sku_id
     ,t4.brand_id
     ,t4.brand_name
     ,t4.model_id
     ,t4.model_name
     ,t4.purchase_channels
     ,t4.cost_price
     ,t4.business_id as skyway_product_id
     ,null as pack_id
     ,null as total_amt
     ,nvl(t11.bind_id,0) as is_add_union
from
    hdp_zhuanzhuan_rawdb_global.raw_t_sales_order_1d t1


        left join
    (
        select
            case when a.parent_order_id = 0 then cast(random() * 1000000 as bigint)
                 else a.parent_order_id
                end as parent_order_id
             ,a.order_id
             ,b.state
             ,b.pay_money
             ,case when b.order_id is not null and b.state in (3,4) and b.pay_money > 0 then 1
                   else 0 end is_afs
             ,from_unixtime(floor(b.update_time/1000),'yyyy-MM-dd HH:mm:ss') as refund_time
             ,c.create_time
             ,c.finish_time
             ,c.responsibility_type
             ,c.compensation_amount
        from

            -- hdp_zhuanzhuan_dw_global.dw_mysql_order_1d a
            hdp_zhuanzhuan_dim_global.dim_trade_order_sale_all_full_1d a

                left join
            hdp_zhuanzhuan_rawdb_global.raw_mysql_db58_zzkfass_t_ass_pay_record_full_1d b
            on a.order_id = b.order_id
                and b.dt = '${outFileSuffix}'

                left join
            hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_offline_sales_t_after_sales_product_full_1d c
            on a.parent_order_id = c.order_id
                and c.dt = '${outFileSuffix}'

        where a.dt = '${outFileSuffix}'
          and from_unixtime(floor(a.create_time/1000),'yyyy-MM-dd') >= '2025-10-22'


    )t2
    on t1.order_id = t2.order_id

        left join
    hdp_ubu_zhuanzhuan_dim_c2b.dim_offline_store_detail_full_1d_0p t3
    on t1.store_id = t3.store_id

        left join
    (

        select
            a.id
             ,a.storage_time
             ,a.outbound_time
             ,a.storage_age
             ,a.info_id
             ,a.business_id
             ,get_json_object(b.qc_extra_json,"$.capacityDesc") as capacity_desc
             ,get_json_object(b.qc_extra_json,'$.colourName') as colour_name
             ,get_json_object(b.qc_extra_json,'$.channelName') as product_channel
             ,get_json_object(b.qc_extra_json,'$.appearanceConditionId') as appearance_condition_id
             ,get_json_object(b.qc_extra_json,'$.appearanceConditionName') as appearance_condition_name
             ,get_json_object(b.qc_extra_json,'$.functionalConditionId') as functional_condition_id
             ,get_json_object(b.qc_extra_json,'$.functionalConditionName') as functional_condition_name
             ,b.condition_desc
             ,c.imei
             ,c.sku_id
             ,c.brand_id
             ,c.brand_name
             ,c.model_id
             ,c.model_name
             ,c.purchase_channels
             ,c.cost_price
             ,c.qc_code

        from
            hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_offline_sales_t_store_product_full_1d a

                left join
            hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_offline_sales_t_product_base_full_1d c
            on a.id = c.store_product_id
                and c.dt = '${outFileSuffix}'

                left join
            hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_offline_sales_t_store_product_base_full_1d b
            on c.qc_code = b.qc_code
                and b.dt = '${outFileSuffix}'

        where a.dt = '${outFileSuffix}'

    ) t4
    on t1.info_id = t4.info_id


        left join
    (
        select
            cate_id,
            1 as rn
        from
            hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_offline_sales_t_fittings_spu_full_1d
        where dt = '${outFileSuffix}'

        union all

        select
            explode(array(1,2)) as cate_id,
            sum(1) over(partition by cate_id,id order by spu_id) as rn
        from
            hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_offline_sales_t_fittings_spu_full_1d
        where dt = '${outFileSuffix}'
          -- having rn > 2
    )t5
    on t1.cate_id = t5.cate_id


        left join
    (
        select
            store_id,
            if(max(case when field_key = 'storeScale' then field_value end)=1,1,0) as is_pro_store
        from
            hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_storefootstone_t_store_extra_attr_full_1d
        where dt='${outFileSuffix}'
        group by
            store_id
    ) t6
    on t1.store_id=t6.store_id

        left join
    (

        select
            distinct
            biz_order_id
                   ,from_unixtime(recheck_time /1000 ,'yyyy-MM-dd HH:mm:ss') as recheck_time
        from
            hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_zzwms_t_shipping_order_detail_full_1d
        where dt='${outFileSuffix}'
          and recheck_time <>0
          and recheck_time is not null
    ) t7
    on t1.order_id = t7.biz_order_id

        left join
    hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_storefootstone_t_employee_full_1d t8
    on t1.staff_id = t8.id
        and t8.dt = '${outFileSuffix}'


        left join
    (
        select
            distinct
            a.order_id
        from
            hdp_zhuanzhuan_rawdb_global.raw_mysql_dbwww58com_trade_t_order_full_1h a

                left join
            hdp_zhuanzhuan_rawdb_global.raw_mysql_dbwww58com_trade_t_order_price_adjust_full_1h b
            on a.order_id = b.order_id
                and b.dt= '${outFileSuffix}'
                and b.adjust_type = 5
                and b.sub_type = 0

                join
            hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_offline_sales_t_activity_coupon_relation_full_1d c
            on a.parent_order_id = c.sales_order_id
                and b.parent_relate_id = c.red_plan_id
                and c.dt = '${outFileSuffix}'
                and c.red_status = 2
                and c.scene_type = 0

        where a.dt= '${outFileSuffix}'
          and a.pack_amount > 0
          and a.order_structure_type in(0,1)


    ) t9
    on t1.order_id = t9.order_id

        left join
    (
        select
            product_id,
            max_by(event_detail,create_time) as event_detail,
            max_by(event_detail_des,create_time) as event_detail_des,
            max_by(create_by,create_time) as create_by
        from
            hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_offline_sales_t_sales_product_log_full_1h
        where dt = '${outFileSuffix}'
          and event_detail = 2
        group by
            product_id

    ) t10
    on t4.id = t10.product_id
        and nvl(split(split(t10.event_detail_des,'：')[1],'/')[0],'') = '以旧换优'

        left join
    (

        select
            distinct
            bind_id
        from
            hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_trade_t_extension_hub_inc_1d
        where ext_key="wxUnionid"
          and deleted = 0
          and dt != ''

    ) t11
    on t1.order_id = t11.bind_id

where t1.dt='12121'
  and t5.cate_id is null
  and t6.is_pro_store = 1
  and t3.store_type_name not like "%测试%"
  and to_date(t1.create_time) >= '2025-10-22'
  and order_type in (1,2)


