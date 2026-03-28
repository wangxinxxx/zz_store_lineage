

drop table if exists hdp_ubu_zhuanzhuan_tmp_c2b.tmp_recycle_gold_order_data_v3;
create table hdp_ubu_zhuanzhuan_tmp_c2b.tmp_recycle_gold_order_data_v3 as

select
    store_id,
    store_name,
    area,
    province,
    city_name,
    group_name,
    store_type_name,
    deal_month,
    gold_rec_gmv,
    gold_rec_weight,
    gold_rec_cnt,
    gold_store_profit,
    gold_jms_share,
    gold_zz_profit,
    gold_yb_share,
    gold_jms_undertake_commission_cost,
    gold_zz_undertake_commission_cost,
    gold_zz_profit*0.06 as gold_tax,
    gold_sf_insure_cost,
    gold_consume_cost,
    0 as gold_machine_cost,  --黄金机器摊销
    gold_shelf_cost,
    gold_jms_undertake_non_fixed_cost,
    gold_zz_undertake_non_fixed_cost,
    gold_zz_undertake_fixed_cost,
    round(gold_store_profit-gold_jms_undertake_non_fixed_cost-gold_zz_undertake_non_fixed_cost-gold_zz_undertake_fixed_cost-gold_yb_share,2) as gold_store_net_profit,
-- round(gold_zz_profit-gold_jms_share-gold_zz_undertake_fixed_cost-gold_zz_undertake_non_fixed_cost+gold_jms_machine_cost,2) as gold_zz_net_profit,
    round(gold_store_profit-gold_yb_share-gold_jms_share-gold_zz_undertake_fixed_cost-gold_zz_undertake_non_fixed_cost+gold_jms_machine_cost,2) as gold_zz_net_profit,
    round(gold_jms_share-gold_jms_undertake_non_fixed_cost-gold_jms_machine_cost,2) as gold_jms_net_profit,
    product_gold_real_value,
    gold_store_profit / product_gold_real_value as gold_service_fee_rate,
    gold_zz_machine_cost,
    gold_jms_machine_cost
from
    (
        select
            t2.store_id,
            t2.store_name,
            t2.area,
            t2.province,
            t2.city_name,
            t2.group_name,
            t2.store_type_name,
            t2.deal_month,
            t2.gold_rec_gmv,
            t2.gold_rec_weight,
            t2.gold_rec_cnt,
            t2.gold_store_profit,
            t2.gold_jms_share,
            t2.gold_store_profit-nvl(t3.gold_consume_cost,0)-nvl(t3.gold_sf_insure_cost,0)-t2.gold_yb_share as gold_zz_profit,
            t2.gold_yb_share,
            t2.gold_jms_undertake_commission_cost,
            t2.gold_zz_undertake_commission_cost,
            case when t2.store_type_name='加盟' then gold_jms_undertake_commission_cost
                 else 0
                end as gold_jms_undertake_non_fixed_cost,
            case when t2.store_type_name!='加盟' then round(gold_zz_undertake_commission_cost+(gold_store_profit-nvl(t3.gold_consume_cost,0)-nvl(t3.gold_sf_insure_cost,0)-gold_yb_share)*0.06,2)
                 else round((gold_store_profit-nvl(t3.gold_consume_cost,0)-nvl(t3.gold_sf_insure_cost,0)-gold_yb_share)*0.06,2)
                end as gold_zz_undertake_non_fixed_cost,
            nvl(t3.gold_zz_undertake_fixed_cost,0) as gold_zz_undertake_fixed_cost,
            t2.product_gold_real_value,
            nvl(t3.gold_zz_machine_cost,0) as gold_zz_machine_cost,
            nvl(t3.gold_sf_insure_cost,0) as gold_sf_insure_cost,  --黄金快递保险
            nvl(t3.gold_consume_cost,0) as gold_consume_cost,  --黄金耗材
            nvl(t3.gold_shelf_cost,0) as gold_shelf_cost,  --黄金展柜摊销
            nvl(t3.gold_jms_machine_cost,0) as gold_jms_machine_cost
        from
            (

                select
                    store_id,
                    store_name,
                    area,
                    province,
                    city_name,
                    group_name,
                    store_type_name,
                    deal_month,
                    round(sum(product_gmv),2) as gold_rec_gmv,
                    round(sum(product_weight),2) as gold_rec_weight,
                    count(distinct recycle_order_id) as gold_rec_cnt,
                    round(sum(gold_store_profit),2) as gold_store_profit,
                    case when store_type_name='加盟' then round(sum(gold_jms_share),2)
                         else 0
                        end as gold_jms_share,
                    round(sum(product_others_income),2) as gold_yb_share,
                    round(sum(gold_jms_undertake_commission_cost),2) as gold_jms_undertake_commission_cost,
                    round(sum(gold_zz_undertake_commission_cost),2) as gold_zz_undertake_commission_cost,
                    sum(product_gold_real_value) as product_gold_real_value
                from
                    (
                        select
                            distinct
                            a.store_id
                                   ,a.store_name
                                   ,a.area
                                   ,a.province
                                   ,a.city_name
                                   ,a.group_name
                                   ,a.store_type_name
                                   ,substr(a.pay_time,1,7) as deal_month
                                   ,a.recycle_order_id
                                   ,a.product_id
                                   ,a.product_weight
                                   ,a.product_gmv/100 as product_gmv
                                   ,a.product_gold_real_value/100 as product_gold_real_value
                                   ,a.product_others_income/100 as product_others_income
                                   ,a.product_service_fee/100 as gold_store_profit  --黄金门店毛利额
                                   ,case when store_type_name='加盟' then product_service_fee/100*0.85*0.6
                                         else 0
                            end as gold_jms_share  --黄金商分润
                                   ,case when store_type_name='加盟' then product_service_fee/100*0.18
                                         else 0
                            end as gold_jms_undertake_commission_cost  --黄金商承担员工提成
                                   ,case when store_type_name!='加盟' then product_service_fee/100*0.18
                                         else 0
                            end as gold_zz_undertake_commission_cost  --黄金转转承担员工提成
                        from
                            hdp_ubu_zhuanzhuan_dw_c2b.dw_trade_store_rec_reservation_recycle_order_detail_full_1d a

                                left join
                            hdp_ubu_zhuanzhuan_tmp_c2b.tmp_gold_refund_orders b
                            on a.recycle_order_id=b.order_id  --状态为已完成的售后退款黄金订单

                        where a.dt = '${outFileSuffix}'
                          and a.order_source_desc='黄金回收'
                          and a.store_name not like '%测试%'
                          and to_date(pay_time) is not null
                          and a.order_state_id not in (10,90)  --剔除待确定和已关闭订单
                          and b.order_id is null  --剔除状态为已完成的售后退款单

                    ) t1
                group by
                    store_id
                       ,store_name
                       ,area
                       ,province
                       ,city_name
                       ,group_name
                       ,store_type_name
                       ,deal_month

            ) t2

                left join
            (

                select
                    a.month,
                    a.store_id,
                    a.store_name,
                    a.store_type_name,
                    a.gold_zz_machine_cost*b.biz_gold_rate as gold_zz_machine_cost,
                    a.gold_shelf_cost*b.biz_gold_rate as gold_shelf_cost,
                    a.gold_consume_cost*b.biz_gold_rate as gold_consume_cost,
                    a.gold_sf_insure_cost*b.biz_gold_rate as gold_sf_insure_cost,
                    a.gold_jms_machine_cost*b.biz_gold_rate as gold_jms_machine_cost,
                    (nvl(a.gold_zz_machine_cost,0)+nvl(a.gold_consume_cost,0)+nvl(a.gold_sf_insure_cost,0)+nvl(a.gold_shelf_cost,0))*b.biz_gold_rate as gold_zz_undertake_fixed_cost

                from
                    hdp_ubu_zhuanzhuan_tmp_c2b.tmp_store_gold_cost a

                        left join
                    (
                        select
                            month,
                            store_id,
                            sum(gold_open_flag) / dayofmonth(last_day(max(stat_date))) as biz_gold_rate,
                            sum(luxury_open_flag) / dayofmonth(last_day(max(stat_date))) as biz_luxury_rate
                        from
                            hdp_ubu_zhuanzhuan_dm_c2b.dm_offline_store_action_data_full_1d
                        where dt='${outFileSuffix}'
                        group by
                            month,
                            store_id

                    ) b
                    on a.month = b.month
                        and a.store_id = b.store_id

            ) t3
            on t2.deal_month = t3.month
                and t2.store_id = t3.store_id

    ) t4




