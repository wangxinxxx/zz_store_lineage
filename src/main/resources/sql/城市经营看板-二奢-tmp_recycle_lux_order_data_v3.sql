
-- 25年8月1日开始，门店抽佣，门店毛利，店员提成，加盟商分润，都要发生变化


drop table if exists hdp_ubu_zhuanzhuan_tmp_c2b.tmp_recycle_lux_order_data_v3;
create table hdp_ubu_zhuanzhuan_tmp_c2b.tmp_recycle_lux_order_data_v3 as

select
    t3.store_id,
    t3.area,
    t3.province,
    t3.city_name,
    t3.store_name,
    t3.store_type_name,
    t3.deal_month as pay_month,
    t3.lux_rec_cnt,
    t3.lux_rec_gmv,
    t3.lux_coupon_gmv,
    t3.lux_store_income_with_coupon,
    t3.lux_store_income,
    t3.lux_zz_income,
    t3.lux_franchisee_income,
    t3.lux_baozhuang_cost,
    t3.lux_employee_com,
    case when t3.store_type_name='加盟' then t3.lux_employee_com
         else 0
        end as lux_jms_employee_com,
    case when t3.store_type_name='加盟' then 0
         else t3.lux_employee_com
        end as lux_zz_employee_com,
    case when t3.store_type_name='加盟' then t3.lux_employee_com
         else 0
        end as lux_jms_undertake_non_fixed_cost,
    case when t3.store_type_name='加盟' then t3.lux_baozhuang_cost
         else t3.lux_baozhuang_cost + t3.lux_employee_com
        end as lux_zz_undertake_non_fixed_cost,
    200*nvl(t3.biz_luxury_rate,1) as lux_zz_undertake_fixed_cost,
    t3.lux_baozhuang_cost+t3.lux_employee_com as lux_store_undertake_fixed_cost,
    200*nvl(t3.biz_luxury_rate,1) as store_undertake_fixed_cost,
    t3.lux_store_income-200*nvl(t3.biz_luxury_rate,1)-(t3.lux_baozhuang_cost+t3.lux_employee_com) as lux_store_net_profit,
    case when t3.store_type_name='加盟' then t3.lux_zz_income-200*nvl(t3.biz_luxury_rate,1)-t3.lux_baozhuang_cost
         else t3.lux_zz_income-200*nvl(t3.biz_luxury_rate,1)-(t3.lux_baozhuang_cost+t3.lux_employee_com)
        end as lux_zz_net_profit,
    case when t3.store_type_name='加盟' then t3.lux_franchisee_income - t3.lux_employee_com
         else t3.lux_franchisee_income
        end as lux_jms_net_profit
from
    (
        select
            t2.store_id,
            t2.area,
            t2.province,
            t2.city_name,
            t2.store_name,
            t2.store_type_name,
            substr(deal_day,1,7) as deal_month,
            count(distinct rec_order_id) as lux_rec_cnt,
            sum(lux_rec_gmv_by1) as lux_rec_gmv,
            sum(lux_coupon_gmv_by1) as lux_coupon_gmv,
            sum(lux_store_income_with_coupon_by1) as lux_store_income_with_coupon,
            sum(lux_store_income_by1) as lux_store_income,
            case when store_type_name='加盟' then sum(lux_zz_income_by1)+200
                 else sum(lux_store_income_by1)
                end as lux_zz_income, -- 转转多200块管理费
            case when store_type_name='加盟' then sum(lux_franchisee_income_by1)-200
                 else 0
                end as lux_franchisee_income, -- 在每一笔分润的基础上合起来全月再扣掉200元管理费
            sum(lux_baozhuang_cost_by1) as lux_baozhuang_cost,
            sum(lux_employee_com_by1) as lux_employee_com,
            max(t4.biz_luxury_rate) as biz_luxury_rate
        from
            (
                select
                    rec_order_id
                     ,order_source
                     ,store_id
                     ,area
                     ,province
                     ,city_name
                     ,store_name
                     ,store_type_name
                     ,deal_day
                     ,lux_rec_gmv_by1
                     ,coupon_price
                     ,allowance_price
                     ,lux_employee_com_base
                     ,lux_store_income_with_coupon_by1
                     ,lux_coupon_gmv_by1
                     ,lux_franchisee_income_base
                     ,lux_baozhuang_cost_by1
                     ,case when store_type_name="加盟" and deal_day<'2025-08-01' then lux_franchisee_income_base*0.6
                           when store_type_name="加盟" and deal_day>='2025-08-01' then lux_franchisee_income_base*0.65
                           else 0
                    end as lux_franchisee_income_by1
                     ,case when reservation_id is not null and deal_day<'2025-08-01' then least(lux_employee_com_base*0.012,500)
                           when reservation_id is null and deal_day<'2025-08-01' then least(lux_employee_com_base*0.024,1000)
                           when reservation_id is not null and deal_day>='2025-08-01' then least(lux_employee_com_base*0.012,1000)
                           when reservation_id is null and deal_day>='2025-08-01' then least(lux_employee_com_base*0.024,1500)
                    end as lux_employee_com_by1
                     ,lux_store_income_with_coupon_by1-lux_coupon_gmv_by1 as lux_store_income_by1
                     ,case when store_type_name="加盟" and deal_day<'2025-08-01' then lux_store_income_with_coupon_by1-lux_coupon_gmv_by1 - lux_franchisee_income_base*0.6
                           when store_type_name="加盟" and deal_day>='2025-08-01' then lux_store_income_with_coupon_by1-lux_coupon_gmv_by1 - lux_franchisee_income_base*0.65
                           else lux_store_income_with_coupon_by1-lux_coupon_gmv_by1
                    end as lux_zz_income_by1
                from
                    (
                        select
                            distinct
                            recycle_order_id as rec_order_id
                                   ,reservation_id
                                   ,order_source
                                   ,store_id
                                   ,area
                                   ,province
                                   ,city_name
                                   ,store_name
                                   ,store_type_name
                                   ,to_date(pay_time) deal_day
                                   ,total_real_price/100 as lux_rec_gmv_by1
                                   ,amt_ticket_amt/100 as coupon_price
                                   ,emp_add_price/100 as allowance_price
                                   ,total_real_price/100-amt_ticket_amt/100-emp_add_price/100 as lux_employee_com_base
                                   ,case when reservation_id is not null and to_date(a.pay_time)<'2025-08-01' then least(total_real_price/100*0.04,2000)
                                         when reservation_id is null and to_date(a.pay_time)<'2025-08-01' then least(total_real_price/100*0.08,4000)
                                         when reservation_id is not null and to_date(a.pay_time)>='2025-08-01' then least(total_real_price/100*0.06,2000)
                                         when reservation_id is null and to_date(a.pay_time)>='2025-08-01' then least(total_real_price/100*0.12,4000)
                            end as lux_store_income_with_coupon_by1
                                   ,case when reservation_id is not null then emp_add_price/100
                                         when reservation_id is null and to_date(a.pay_time)<'2025-08-01' then emp_add_price/100
                                         when reservation_id is null and to_date(a.pay_time)>='2025-08-01' then (emp_add_price+amt_ticket_amt)/100
                            end as lux_coupon_gmv_by1
                                   ,case when reservation_id is not null then least(total_real_price/100*0.04,2000)-emp_add_price/100
                                         when reservation_id is null then least(total_real_price/100*0.08,4000)-emp_add_price/100
                            end as lux_franchisee_income_base
                                   ,31.5 as lux_baozhuang_cost_by1
                        from
                            hdp_ubu_zhuanzhuan_dw_c2b.dw_trade_store_rec_reservation_recycle_order_detail_full_1d a
                        where dt = '${outFileSuffix}'
                          and order_source_desc='二奢回收'
                          and store_name not like '%测试%'
                          and pay_time is not null
                          and pay_time !=''
                    ) t1
            ) t2

                left join
            (
                select
                    stat_date,
                    store_id,
                    month,
                    sum(gold_open_flag) over(partition by month,store_id) / dayofmonth(last_day(max(stat_date) over(partition by month,store_id)))  as biz_gold_rate,
                    sum(luxury_open_flag) over(partition by month,store_id)  / dayofmonth(last_day(max(stat_date) over(partition by month,store_id))) as biz_luxury_rate
                from
                    hdp_ubu_zhuanzhuan_dm_c2b.dm_offline_store_action_data_full_1d
                where dt='${outFileSuffix}'
            ) t4
            on t2.store_id = t4.store_id
                and t2.deal_day = t4.stat_date

        group by
            t2.store_id,
            t2.area,
            t2.province,
            t2.city_name,
            t2.store_name,
            t2.store_type_name,
            substr(t2.deal_day,1,7)
    ) t3
