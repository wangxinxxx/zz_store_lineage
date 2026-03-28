drop table if exists hdp_ubu_zhuanzhuan_tmp_c2b.tmp_recycle_fit_order_data_v2;
create table hdp_ubu_zhuanzhuan_tmp_c2b.tmp_recycle_fit_order_data_v2 as


with new_fit_info as
         (
             select
                 deal_month,
                 store_id,
                 store_name,
                 sum(fit_encourage_com) as fit_encourage_com
             from
                 (
                     select
                         deal_month,
                         store_id,
                         store_name,
                         parent_order_id,
                         child_cnt,
                         price,
                         case when deal_month<'2025-08' then 0
                              when price>=200 then 20
                              when price>=100 then 10
                              when price>=50 then 5 else 0 -- 保守版和均衡版和激进版
                             end as fit_encourage_com
                     from
                         (
                             select
                                 scene_type_name,
                                 a.store_name,
                                 a.store_id,
                                 staff_id,
                                 buyer_id,
                                 parent_order_id,
                                 deal_month,
                                 count(distinct order_id) as child_cnt,
                                 sum(price) as price
                             from
                                 (
                                     select
                                         distinct
                                         a.order_id,
                                         a.parent_order_id,
                                         scene_type,
                                         case when scene_type=1 then '回收'
                                              when scene_type=2 then '零售'
                                             end as scene_type_name,
                                         staff_id,
                                         staff_name,
                                         outbound_time,
                                         buyer_id,
                                         store_name,
                                         store_id,
                                         a.deal_price/100 as price,
                                         substr(outbound_time,1,10) as fit_outbound_time,
                                         substr(outbound_time,1,7) as deal_month
                                     from
                                         hdp_ubu_zhuanzhuan_dw_c2b.dw_trade_fitting_offline_data_full_1d  a

                                             left join
                                         hdp_ubu_zhuanzhuan_dw_c2b.dw_market_coupon_t_activity_coupon_relation_full_1d b
                                         on a.parent_order_id=b.sales_order_id
                                             and b.dt='${outFileSuffix}' -- 注意这里要用parent的ID号关联
                                     where a.dt='${outFileSuffix}'
                                       and a.state = '已完成'
                                       and a.store_name not like '%测试%'
                                       and a.store_name not like '%直播%'
                                       and a.order_type in (1,2)
                                       and a.is_refund=0
                                       and scene_type in (1,2)
                                 ) a
                             group by
                                 scene_type_name,
                                 a.store_name,
                                 a.store_id,
                                 staff_id,
                                 buyer_id,
                                 parent_order_id,
                                 deal_month
                         ) a
                 ) a
             group by
                 deal_month,
                 store_id,
                 store_name
         )


select
    t2.deal_month,
    t2.store_id,
    t2.store_name,
    t2.area,
    t2.province,
    t2.city_name,
    t2.group_name,
    t2.store_type_name,
    t2.fit_sale_gmv,
    t2.fit_sale_cnt,
    t2.fit_cur_cost_price,
    t2.fit_store_profit,
    t2.fit_jms_share,
    t2.fit_zz_profit,
    t2.employee_com,  --配件员工基础提成

    nvl(t3.fit_encourage_com,0) as fit_encourage_com,  --配件激励

    case when store_type_name='加盟' then t2.employee_com
         else 0 end as fit_jms_undertake_commission_cost,  --配件商承担可变成本
    case when store_type_name='加盟' then t2.employee_com
         else 0 end as fit_jms_undertake_non_fixed_cost,  --配件商承担可变成本

    case when store_type_name='加盟' then t3.fit_encourage_com
         else t2.employee_com + nvl(t3.fit_encourage_com,0)
        end as fit_zz_undertake_commission_cost,  --配件转转承担可变成本
    case when store_type_name='加盟' then t3.fit_encourage_com
         else t2.employee_com + nvl(t3.fit_encourage_com,0)
        end as fit_zz_undertake_non_fixed_cost,  --配件转转承担可变成本

    case when store_type_name='加盟' then t2.fit_store_profit - t2.employee_com  - nvl(t3.fit_encourage_com,0)
         else t2.fit_store_profit  - t2.employee_com - nvl(t3.fit_encourage_com,0)
        end as fit_store_net_profit,  --配件门店净利润

    case when store_type_name='加盟' then t2.fit_zz_profit - nvl(t3.fit_encourage_com,0)
         else t2.fit_zz_profit - t2.employee_com - nvl(t3.fit_encourage_com,0)
        end as fit_zz_net_profit,  --配件转转净利润

    round(t2.fit_jms_share-t2.fit_jms_undertake_non_fixed_cost,2) as fit_jms_net_profit  --配件商净利润

from
    (
        select
            deal_month,
            store_id,
            store_name,
            area,
            province,
            city_name,
            group_name,
            store_type_name,
            round(sum(deal_price),2) as fit_sale_gmv,
            count(distinct order_id) as fit_sale_cnt,
            round(sum(cur_cost_price),2) as fit_cur_cost_price,
            round(sum(single_gross_profit),2) as fit_store_profit,  --配件门店毛利额
            round(sum(jms_single_share_profit),2) as fit_jms_share,  --配件商分润
            round(sum(zz_single_share_profit),2) as fit_zz_profit,  --配件转转毛利额
            round(sum(single_employee_com),2) as employee_com,   --配件员工基础提成
            case when store_type_name='加盟' then round(sum(single_employee_com),2)
                 else 0
                end as fit_jms_undertake_non_fixed_cost,  --配件商承担可变成本
            case when store_type_name='加盟' then round(sum(single_employee_com),2)
                end as fit_zz_undertake_non_fixed_cost  --配件转转承担可变成本
        from
            (
                select
                    distinct
                    to_date(a.outbound_time) as deal_date,
                    date_format(a.outbound_time,'yyyy-MM') as deal_month,
                    b.area,
                    b.province,
                    b.city_name,
                    b.group_name,
                    a.store_type_name,
                    a.store_id,
                    b.store_name,
                    a.staff_id,
                    a.staff_name,
                    a.order_id,
                    a.spu_name,
                    a.info_id as qc_code,
                    a.deal_price/100 as deal_price, --成交价
                    a.cur_cost_price/100 as cur_cost_price,  --成本价
                    (a.deal_price/100 - a.cur_cost_price/100) as single_gross_profit,  --单笔毛利额
                    a.deal_price/100*0.18 as  single_employee_com,  --单笔员工提成
                    case when a.store_type_name='加盟' then (a.deal_price/100 - a.cur_cost_price/100*1.1)/2
                         else 0 end as jms_single_share_profit,  --`单笔商分润`
                    case when a.store_type_name='加盟' then (a.deal_price/100 - a.cur_cost_price/100) - (a.deal_price/100-a.cur_cost_price/100*1.1)/2
                         else (a.deal_price/100 - a.cur_cost_price/100) end as zz_single_share_profit  --单笔转转毛利额
                from
                    hdp_ubu_zhuanzhuan_dw_c2b.dw_trade_fitting_offline_data_full_1d a

                        left join
                    hdp_ubu_zhuanzhuan_tmp_c2b.tmp_c2b_store_organization_full_1d b
                    on b.dt='${outFileSuffix}'
                        and a.store_id=b.store_id

                where a.dt='${outFileSuffix}'
                  and a.state = '已完成'
                  and a.store_name not like '%测试%'
                  and a.store_name not like '%直播%'
                  and to_date(a.outbound_time) >='2023-04-01'
                  and order_type_desc in ('门店订单','线下销售')


            ) t1
        group by
            deal_month,
            store_id,
            store_name,
            area,
            province,
            city_name,
            group_name,
            store_type_name
    ) t2

        left join
    new_fit_info t3
    on t2.deal_month = t3.deal_month
        and t2.store_id = t3.store_id

