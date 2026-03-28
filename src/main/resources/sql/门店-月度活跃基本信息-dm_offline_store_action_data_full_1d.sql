




add jar viewfs://58-cluster/home/hdp_58dp/udf/C2bRecyclePriceItemGetUDF2.jar;
create temporary function getRecyclePriceItem as 'ColaUDF.C2bRecyclePriceItemGet2';

insert overwrite table hdp_ubu_zhuanzhuan_dw_c2b.dw_recycle_order_amt_data_full_1d  partition(dt='${outFileSuffix}')
select

    tep.order_id,
    tep.order_source,
    tep.perform_type,
    from_unixtime(floor(tep.create_time/1000),'yyyy-MM-dd HH:mm:ss') create_time,
    tep.evaluate_price,
    if(tep.evaluate_price = 0 or tep.view_price = 0,0,round(tep.evaluate_price/tep.view_price,2)) as discount_rate,
    tep.prop_coupon_price,
    tep.prop_coupon_rate,
    tep.value_coupon_price,
    tep.customer_subsidy_price,
    tep.staff_add_price,
    tep.engineer_add_price,
    tep.insurance_subsidy,
    tep.small_amount_subsidy,
    tep.total_price,
    tep.hscm_subsidy,
    tep.apply_markup_amount,

    c.basic_amount_voucher,
    C.basic_amount_title,
    C.basic_amount_equity_config_id,
    C.rate_amount_title,
    C.rate_amount_equity_config_id,
    C.amount_title,
    C.amount_equity_config_id,
    tep.member_points_deduct

from
    (
        select
            1 as type,
            t1.order_id,
            order_source,
            perform_type,
            create_time,
            nvl(t2.evaluate_price,0) as evaluate_price,
            nvl(t6.view_price,0) as view_price,
            nvl(t3.prop_coupon_price,0) prop_coupon_price,
            nvl(t3.prop_coupon_rate,0) prop_coupon_rate,
            nvl(t3.value_coupon_price,0) as value_coupon_price,
            0 as customer_subsidy_price,
            nvl(t4.staff_add_price,0) staff_add_price,
            0 as engineer_add_price,
            nvl(t4.insurance_subsidy,0) as insurance_subsidy,
            nvl(t4.small_amount_subsidy,0) as small_amount_subsidy,
            nvl(t5.total_price,0) total_price,
            nvl(t4.hscm_subsidy,0) as hscm_subsidy,
            nvl(t7.apply_markup_amount,0) apply_markup_amount,
            nvl(t2.member_points_deduct,0) as member_points_deduct
        from
            (
                select
                    order_id,
                    order_source,
                    '线下门店' as perform_type,
                    create_time
                from
                    hdp_zhuanzhuan_rawdb_global.raw_trade_t_recycle_order_full_1d
                where dt = '${outFileSuffix}'
                  and order_source in (46,2706003)
                  and state >=38
                  and state <=80
                  and pay_state=20
            )t1

                left join
            (
                --估价器价格
                select
                    order_id,
                    max(case when price_type = 3 then price end) as evaluate_price,
                    max(case when price_type = 10 then getRecyclePriceItem(price_message,60,'price') end)
                                                                 as member_points_deduct  --`会员积分抵扣金额`

                from
                    hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_trade_extend_t_recycle_price_full_1d
                where dt = '${outFileSuffix}'
                  and price_type in (
                                     3, --估价器价格
                                     10  --会员积分抵扣
                    )
                group by
                    order_id

            )t2
            on t1.order_id = t2.order_id

                left join
            (
                --比例券金额   比例券rate  金额券金额
                select
                    order_id,
                    getRecyclePriceItem(price_message,32,'price') as prop_coupon_price,
                    getRecyclePriceItem(price_message,32,'ratio') as prop_coupon_rate,
                    getRecyclePriceItem(price_message,33,'price') as value_coupon_price
                from
                    hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_trade_extend_t_recycle_price_full_1d
                where dt = '${outFileSuffix}'
                  and price_type=1
            )t3
            on t1.order_id = t3.order_id

                left join
            (
                --店员加价 保价补贴 小额补差
                select
                    order_id,
                    getRecyclePriceItem(price_message,40,'price') as staff_add_price,
                    getRecyclePriceItem(price_message,43,'price') as insurance_subsidy,
                    getRecyclePriceItem(price_message,41,'price') as small_amount_subsidy,
                    getRecyclePriceItem(price_message,45,'price') as hscm_subsidy
                from
                    hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_trade_extend_t_recycle_price_full_1d
                where dt = '${outFileSuffix}'
                  and price_type=0
            )t4
            on t1.order_id = t4.order_id

                left join
            (
                --价格合计
                select
                    price as total_price,
                    order_id
                from
                    hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_trade_extend_t_recycle_price_full_1d
                where dt = '${outFileSuffix}'
                  and price_type=4
            )t5
            on t1.order_id = t5.order_id

                left join
            (
                select
                    get_json_object(price_message, '$[0].showPrice') as view_price,order_id
                from
                    hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_trade_extend_t_recycle_price_full_1d
                where dt = '${outFileSuffix}' and price_type=3
            )t6
            on t1.order_id = t6.order_id

                left join
            (
                SELECT
                    order_id,
                    cast(nvl(GET_JSON_OBJECT(extend_attr,'$.applyMarkupAmount'),0) as bigint) as apply_markup_amount
                from
                    hdp_zhuanzhuan_rawdb_global.raw_t_offline_apply_price_order_full_1d
                where dt='${outFileSuffix}'
                  and apply_type = 2
                  and state=1
            )t7
            on t1.order_id = t7.order_id


        union all

        ---------------------------邮寄 & 上门
        select
            2 as type,
            t1.order_id,
            order_source,
            perform_type,
            create_time,
            nvl(t2.evaluate_price,0) as evaluate_price,
            nvl(t6.view_price,0) as view_price,
            nvl(t3.prop_coupon_price,0) prop_coupon_price,
            nvl(t3.prop_coupon_rate,0) prop_coupon_rate,
            nvl(t3.value_coupon_price,0) as value_coupon_price,
            if(order_source in (40,41,55,56,69,61,2701019,2701020),0,nvl(t4.customer_subsidy_price,0)) customer_subsidy_price,
            0 as staff_add_price,
            if(order_source in (40,41,55,56,69,61,2701019,2701020),nvl(t4.engineer_add_price,0),0) as engineer_add_price,
            nvl(t4.insurance_subsidy,0) as insurance_subsidy,
            nvl(t4.small_amount_subsidy,0) as small_amount_subsidy,
            nvl(t5.total_price,0) total_price,
            nvl(t4.hscm_subsidy,0) as hscm_subsidy,
            0 as apply_markup_amount,
            nvl(t2.member_points_deduct,0) as member_points_deduct

        from
            (
                select
                    order_id,
                    order_source,
                    if(order_source in (40,41,55,56,69,61,2701019,2701020),'上门','邮寄') as perform_type,
                    create_time
                from
                    hdp_zhuanzhuan_rawdb_global.raw_trade_t_recycle_order_full_1d
                where dt = '${outFileSuffix}'
                  and order_source in (21,22,34,35,51,52,53,54,2701018,2705009,40,41,55,56,69,61,2701019,2701020,2701028,2701031,2701029,2701030,66)
                  and state=80
                  and pay_state=20
            )t1

                left join
            (
                --估价器价格
                select
                    order_id,
                    max(case when price_type = 3 then price end) as evaluate_price,
                    max(case when price_type = 10 then getRecyclePriceItem(price_message,1001,'price') end)
                                                                 as member_points_deduct  --`会员积分抵扣金额`
                from
                    hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_trade_extend_t_recycle_price_full_1d
                where dt = '${outFileSuffix}'
                  and price_type in (
                                     3,  --估价器价格
                                     10  --会员积分抵扣金额
                    )

                group by
                    order_id

            )t2
            on t1.order_id = t2.order_id

                left join
            (
                --比例券金额   比例券rate  金额券金额
                select
                    order_id,
                    getRecyclePriceItem(price_message,10,'price') as prop_coupon_price,
                    getRecyclePriceItem(price_message,10,'ratio') as prop_coupon_rate,
                    getRecyclePriceItem(price_message,11,'price') as value_coupon_price
                from
                    hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_trade_extend_t_recycle_price_full_1d
                where dt = '${outFileSuffix}' and price_type=1
            )t3
            on t1.order_id = t3.order_id

                left join
            (
                --工程师加价 客服补贴 保价补贴 小额补差
                select
                    order_id,
                    (nvl(getRecyclePriceItem(price_message,50,'price'),0) + nvl(getRecyclePriceItem(price_message,51,'price'),0) + nvl(getRecyclePriceItem(price_message,52,'price'),0) + nvl(getRecyclePriceItem(price_message,56,'price'),0)) as customer_subsidy_price,
                    nvl(getRecyclePriceItem(price_message,40,'price'),0) as engineer_add_price,
                    getRecyclePriceItem(price_message,54,'price') as insurance_subsidy,
                    getRecyclePriceItem(price_message,55,'price') as small_amount_subsidy,
                    getRecyclePriceItem(price_message,81,'price') as hscm_subsidy
                from
                    hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_trade_extend_t_recycle_price_full_1d
                where dt = '${outFileSuffix}' and price_type=0
            )t4
            on t1.order_id = t4.order_id

                left join
            (
                --价格合计
                select
                    price as total_price,
                    order_id
                from
                    hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_trade_extend_t_recycle_price_full_1d
                where dt = '${outFileSuffix}'
                  and price_type=4
            )t5
            on t1.order_id = t5.order_id

                left join
            (
                select
                    get_json_object(price_message, '$[0].showPrice') as view_price,order_id
                from
                    hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_trade_extend_t_recycle_price_full_1d
                where dt = '${outFileSuffix}'
                  and price_type=3
            )t6
            on t1.order_id = t6.order_id
    )tep

        left join
    (
        select
            nvl(order_id,business_order_id) as order_id,
            case when price_json.type = 12 then '1'
                 when price_json.type = 34 then '2'
                end as type,
            price_json.price as basic_amount_voucher,
            basic_amount_title,
            basic_amount_equity_config_id,
            rate_amount_title,
            rate_amount_equity_config_id,
            amount_title,
            amount_equity_config_id
        from
            (
                select
                    order_id,
                    explode_outer(
                            filter(
                                    from_json( price_message, 'array<struct<price:int, type:int >>' ),   --解析想要的字段
                                    x -> x.type in (12,34)  --从数组中过滤出需要的数据
                            )
                    ) as price_json
                from
                    hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_trade_extend_t_recycle_price_full_1d a
                where a.dt='${outFileSuffix}'
                  and a.price_type=1
            ) a

                full join
            (

                select
                    t1.business_order_id,
                    max(case when t2.equity_type = 14 then t2.title end) as basic_amount_title,
                    max(case when t2.equity_type = 14 then t2.equity_config_id end) as basic_amount_equity_config_id,
                    max(case when t2.equity_type = 11 then t2.title end) as rate_amount_title,
                    max(case when t2.equity_type = 11 then t2.equity_config_id end) as rate_amount_equity_config_id,
                    max(case when t2.equity_type = 10 then t2.title end) as amount_title,
                    max(case when t2.equity_type = 10 then t2.equity_config_id end) as amount_equity_config_id
                from
                    hdp_zhuanzhuan_rawdb_global.raw_t_business_coupon_relation_full_1d t1

                        left join

                    (

                        select
                            coupon_id,
                            equity_type,
                            title,
                            equity_config_id
                        from
                            hdp_zhuanzhuan_rawdb_global.raw_aladdin_t_user_coupon_full_1d  -- 获取券配置ID，30天以前
                        where dt='${outFileSuffix}'

                        union

                        select
                            coupon_id,
                            equity_type,
                            title,
                            equity_config_id
                        from
                            hdp_zhuanzhuan_rawdb_global.raw_mysql_tdb_aladdin_t_user_coupon_full_full_1d  -- 获取券配置ID，30天以前
                        where dt='${outFileSuffix}'

                    ) t2

                    on t1.coupon_id = t2.coupon_id
                where t1.dt='${outFileSuffix}'
                  and equity_type in  (
                                       10, -- 取叠加金额
                                       11, -- 取比例券
                                       14 -- 取基础金额券
                    )
                group by
                    t1.business_order_id
            ) b -- 获取券配置ID
            on  a.order_id=b.business_order_id -- 用户券ID匹配

    ) c
    on tep.order_id = c.order_id
--and tep.type = c.type
where create_time >= 1672502400000 and total_price != 0















