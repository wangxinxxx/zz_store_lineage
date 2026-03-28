

--drop table if exists hdp_ubu_zhuanzhuan_tmp_c2b.test_ads_bi_offline_store_operating_data_center_v3_full_1d;
--create table hdp_ubu_zhuanzhuan_tmp_c2b.test_ads_bi_offline_store_operating_data_center_v3_full_1d as

with total_left  as (
    -- 当月所有在营的门店的信息和天数信息放在一起，作为左表，其他所有数据都要和这个左表关联，保证维度一致
    select
    distinct
    month
   ,store_id
   ,store_name
   ,store_type_name
   ,store_open_date
   ,nvl(store_close_date,'未撤店') as store_close_date
   ,area
   ,province
   ,city_name
   ,group_name
   ,franchisee_id
   ,franchisee_name
   ,days_in_month
   ,this_month_finish
   ,is_this_month_open
   ,need_qianfan_date
   ,need_data_from_qinfan
   ,convert_num
   ,sum(gold_open_flag) over(partition by store_id,month) / day(last_day(stat_date)) as biz_gold_rate
   ,sum(luxury_open_flag) over(partition by store_id,month) / day(last_day(stat_date)) as biz_luxury_rate
   ,max(gold_open_flag) over(partition by store_id,month) as gold_open_flag
   ,max(luxury_open_flag) over(partition by store_id,month) as luxury_open_flag
   ,max_by(luxury_store_level,stat_date) over(partition by store_id,month) as luxury_store_level
   ,line_level
   ,store_area
   ,store_position_type
   ,gold_open_date
   ,luxury_open_date
   ,is_pro_store
from
    hdp_ubu_zhuanzhuan_dm_c2b.dm_offline_store_action_data_full_1d
where dt='${outFileSuffix}'
    )



    ,sm_rec as (
select
    date_format(purchase_time,'yyyy-MM') as stat_month
        ,sum(t1.sm_rec_order_subsidy) as sm_rec_order_subsidy  --`营销补贴`
        ,sum(t1.pur_seller_act_receipt_amt) as sm_rec_order_gmv  --`回收GMV`
        ,sum(t1.sm_rec_order_subsidy) / sum(t1.pur_seller_act_receipt_amt) as sm_rec_order_subsidy_rate
from
    (
    select
    to_date(a.purchase_time ) as purchase_time
        ,a.pur_seller_act_receipt_amt/100 as pur_seller_act_receipt_amt
        ,nvl(d.prop_coupon_price/100,0) as prop_coupon_price
        ,nvl(d.value_coupon_price/100,0) as value_coupon_price
        ,nvl(d.basic_amount_voucher/100,0) as basic_amount_voucher
        ,nvl(d.prop_coupon_price/100,0) + nvl(d.value_coupon_price/100,0) + nvl(d.basic_amount_voucher/100,0) as sm_rec_order_subsidy
    from
    hdp_ubu_zhuanzhuan_dm_c2b.dm_finance_bm_purchase_sale_detail_full_1d a

    left join
    hdp_ubu_zhuanzhuan_dw_c2b.dw_recycle_order_amt_data_full_1d d      --门店、上门、邮寄回收订单价格信息表
    on a.pur_order_id = d.rec_order_id
    and d.dt = '${outFileSuffix}'

    where a.dt='${outFileSuffix}'
    and a.pur_source_third_id in (6,9,54,71)  --C2B回收，上门
    and to_date(a.purchase_time )between '2023-01-01' and '${outFileSuffix}'
    ) t1
group by
    1

    )

        ,rpc as (
select
    substr(statistics_date,1,7) as stat_month,
    round(sum(store_gmv_n)/sum(czc_mn_gmv),4) as rpc1
from
    hdp_ubu_zhuanzhuan_tmp_c2b.retail_price_table2 -- 直接用目前RPC看板的结果表
where dt='${outFileSuffix}'
-- and substr(statistics_date,1,7)>='2023-01' -- 这个之后要改成2301开始
  and substr(statistics_date,1,7) between '2023-01' and '2025-08'
  and store_kdj>0
  and czc_kdj>0
group by
    substr(statistics_date,1,7)

union all
select
    substr(statistics_date,1,7) as stat_month,
    sum(store_gmv_n)/sum(czc_mn_gmv) as rpc1
from
    hdp_zhuanzhuan_tmp_global.tmp_new_retail_price_table_2_day
where dt='${outFileSuffix}'
  and substr(statistics_date,1,7) >='2025-09'
  and store_kdj>0
  and czc_kdj>0
group by
    substr(statistics_date,1,7)
    )

        , t_income as (
-- 按日统计门店回收、零售净利润和分润
select
    substr(stat_date,1,7) as month
        ,stat_date
        ,store_id
        ,rec_net_gmv
        ,rec_net_cnt
        ,sale_cnt
        ,online_rec_net_gmv
        ,offline_rec_net_gmv
        ,sale_net_gmv
        ,sale_net_cnt
        ,ts_net_gmv
        ,ts_net_cnt
        ,sale_cnt_old_change
    -- ,sale_old_change_cost
        ,coupon_total_amt
        ,base_amt_ticket
        ,rate_ticket_amt
        ,amt_ticket_amt
        ,emp_add_price
        ,apply_markup_amount
        ,rec_basic_income
        ,add_price_amt
        ,online_phone_net_gmv
        ,offline_phone_net_gmv
        ,online_unphone_net_gmv
        ,offline_unphone_net_gmv
        ,unself_deal_order_net_gmv
        ,online_phone_net_cnt
        ,offline_phone_net_cnt
        ,online_unphone_net_cnt
        ,offline_unphone_net_cnt
        ,unself_deal_order_net_cnt
        ,over7_punish_amt
        ,over30_punish_amt
        ,rec_wl_cost
        ,sale_wl_cost
        ,sale_pay_cost
        ,sale_aftersale_cost
        ,sale_wrap_cost
        ,sale_peifu_cost
        ,ts_wl_cost
        ,ts_commission_cost
        ,store_eval_price
        ,judg_difference_amt
        ,rec_net_gmv-unself_deal_order_net_gmv as zy_rec_net_gmv  --剔除竞拍的净回收gmv
        ,rec_order_gmv - unself_deal_order_net_gmv as zy_rec_order_gmv  --剔除竞拍的回收gmv
from
    --hdp_ubu_zhuanzhuan_tmp_c2b.test_dm_offline_store_income_data_full_1d
    hdp_ubu_zhuanzhuan_dm_c2b.dm_offline_store_income_data_full_1d
where dt='${outFileSuffix}'
    )

    , t_gmv_n_profit_result as (
-- 1. 门店视角业绩和毛利结果汇总
select
    month
        ,store_id
        ,rec_net_gmv
        ,rec_net_cnt
    -- ,online_rec_net_gmv
    -- ,offline_rec_net_gmv
        ,zonghe_online_rec_net_gmv
        ,zonghe_offline_rec_net_gmv
        ,sale_net_gmv
        ,sale_net_cnt
        ,sale_cnt
        ,sale_cnt_old_change
        ,sale_cnt_old_change / sale_cnt as sale_cnt_old_change_rate

        ,rpc1
        ,(1.5/100 - add_price_rate)*(zy_rec_net_gmv) as add_price_encourage_income
        ,rec_basic_income+judge_encourage_income+coupon_encourage_income+(1.5/100 - add_price_rate)*(zy_rec_net_gmv) as rec_total_income
        ,offline_sale_income
        ,ts_sale_income
        ,nvl(offline_sale_income,0) + nvl(ts_sale_income,0) - nvl(ts_commission_cost,0) as sale_without_punish_income
        ,nvl(offline_sale_income,0) + nvl(ts_sale_income,0) - nvl(ts_commission_cost,0) + nvl(over30_punish_amt,0) + nvl(over7_punish_amt,0) as sale_total_income
        ,rec_basic_income+judge_encourage_income+coupon_encourage_income+(1.5/100 - add_price_rate)*(zy_rec_net_gmv) + nvl(offline_sale_income,0) + nvl(ts_sale_income,0) - nvl(ts_commission_cost,0) + nvl(over30_punish_amt,0) + nvl(over7_punish_amt,0) as total_income
        ,rec_net_gmv
        ,rec_net_cnt

        ,online_phone_net_gmv
        ,offline_phone_net_gmv
        ,online_unphone_net_gmv
        ,offline_unphone_net_gmv
        ,unself_deal_order_net_gmv
        ,online_phone_net_cnt
        ,offline_phone_net_cnt
        ,online_unphone_net_cnt
        ,offline_unphone_net_cnt
        ,unself_deal_order_net_cnt
        ,sale_net_gmv
        ,sale_net_cnt
        ,ts_net_gmv
        ,ts_net_cnt
        ,sale_cnt_old_change
        ,store_net_gmv
        ,store_net_cnt
        ,judg_difference_amt
        ,store_eval_price
        ,judg_difference_rate
        ,judge_encourage_income
        ,sale_cnt
        ,coupon_total_amt


        ,coupon_total_rate
        ,base_amt_ticket
        ,rate_ticket_amt
        ,amt_ticket_amt
        ,emp_add_price
        ,apply_markup_amount
        ,over30_punish_amt
        ,over7_punish_amt
        ,sm_rec_order_gmv
        ,sm_rec_order_subsidy
        ,sm_rec_order_subsidy_rate
        ,rec_basic_income
        ,coupon_encourage_income
        ,add_price_amt
        ,add_price_rate
        ,rpc1
        ,offline_sale_income
        ,ts_sale_income
        ,rec_wl_cost
        ,sale_wl_cost
        ,sale_pay_cost
        ,sale_aftersale_cost
        ,sale_wrap_cost
        ,sale_peifu_cost
        ,ts_wl_cost
        ,ts_commission_cost
        ,zy_rec_order_gmv

from
    (
    select
    month
        ,t1.store_id
        ,sum(rec_net_gmv) as rec_net_gmv
        ,sum(rec_net_cnt) as rec_net_cnt
        ,sum(sale_cnt) as sale_cnt
        ,sum(online_rec_net_gmv) as zonghe_online_rec_net_gmv
        ,sum(offline_rec_net_gmv) as zonghe_offline_rec_net_gmv
        ,sum(online_phone_net_gmv) as online_phone_net_gmv
        ,sum(offline_phone_net_gmv) as offline_phone_net_gmv
        ,sum(online_unphone_net_gmv) as online_unphone_net_gmv
        ,sum(offline_unphone_net_gmv) as offline_unphone_net_gmv
        ,sum(unself_deal_order_net_gmv) as unself_deal_order_net_gmv
        ,sum(online_phone_net_cnt) as online_phone_net_cnt
        ,sum(offline_phone_net_cnt) as offline_phone_net_cnt
        ,sum(online_unphone_net_cnt) as online_unphone_net_cnt
        ,sum(offline_unphone_net_cnt) as offline_unphone_net_cnt
        ,sum(unself_deal_order_net_cnt) as unself_deal_order_net_cnt
        ,sum(sale_net_gmv) as sale_net_gmv
        ,sum(sale_net_cnt) as sale_net_cnt
        ,sum(ts_net_gmv) as ts_net_gmv
        ,sum(ts_net_cnt) as ts_net_cnt
        ,sum(sale_cnt_old_change) as sale_cnt_old_change
    -- ,sum(sale_old_change_cost) as sale_old_change_cost
        ,sum(rec_net_gmv+sale_net_gmv) as store_net_gmv
        ,sum(rec_net_cnt+sale_net_cnt) as store_net_cnt
        ,sum(judg_difference_amt) as judg_difference_amt
        ,sum(store_eval_price) as store_eval_price
        ,nvl(sum(judg_difference_amt) / sum(store_eval_price),0) as judg_difference_rate
        ,sum(zy_rec_net_gmv) as zy_rec_net_gmv
        ,sum(coupon_total_amt) as coupon_total_amt
    -- ,nvl(sum(coupon_total_amt) / sum(zy_rec_net_gmv),0) as coupon_total_rate
        ,nvl(sum(coupon_total_amt) / sum(zy_rec_order_gmv),0) as coupon_total_rate
        ,sum(base_amt_ticket) as base_amt_ticket
        ,sum(rate_ticket_amt) as rate_ticket_amt
        ,sum(amt_ticket_amt) as amt_ticket_amt
        ,sum(emp_add_price) as emp_add_price
        ,sum(apply_markup_amount) as apply_markup_amount

        ,sum(over30_punish_amt) as over30_punish_amt
        ,sum(over7_punish_amt) as over7_punish_amt

        ,max(t3.sm_rec_order_gmv) as sm_rec_order_gmv
        ,max(t3.sm_rec_order_subsidy) as sm_rec_order_subsidy
        ,max(t3.sm_rec_order_subsidy_rate) as sm_rec_order_subsidy_rate
        ,sum(rec_basic_income) as rec_basic_income
        ,(nvl(sum(judg_difference_amt)  / sum(store_eval_price),0) + 1.8/100) * sum(store_eval_price) * 30/100 as judge_encourage_income
        ,(max(t3.sm_rec_order_subsidy_rate) - nvl(sum(coupon_total_amt) / sum(zy_rec_net_gmv),0) ) * sum(zy_rec_net_gmv) as coupon_encourage_income
        ,sum(add_price_amt) as add_price_amt
        ,nvl(sum(add_price_amt)/sum(zy_rec_order_gmv),0) as add_price_rate
        ,max(t2.rpc1) as rpc1
        ,sum(sale_net_gmv) * (6.5/100+max(t2.rpc1)-1) as offline_sale_income
        ,sum(ts_net_gmv)*6.5/100 as ts_sale_income

        ,sum(rec_wl_cost) as rec_wl_cost
        ,sum(sale_wl_cost) as sale_wl_cost
        ,sum(sale_pay_cost) as sale_pay_cost
        ,sum(sale_aftersale_cost) as sale_aftersale_cost
        ,sum(sale_wrap_cost) as sale_wrap_cost
        ,sum(sale_peifu_cost) as sale_peifu_cost
        ,sum(ts_wl_cost) as ts_wl_cost
        ,sum(ts_commission_cost) as ts_commission_cost
        ,sum(zy_rec_order_gmv) as zy_rec_order_gmv

    from
    t_income t1

    left join
    rpc t2
    on t1.month = t2.stat_month

    left join
    sm_rec t3
    on t1.month = t3.stat_month

    group by
    month,
    c
    ) t1
    )


        , jms_share_result as (
-- 2.分润
select
    month,
    store_id,
    recycle_total_share_amt,
    recycle_online_share_amt,
    recycle_offline_share_amt,
    sale_total_share_amt,
    phone_k_incentive_amt,
    renovation_subsidy_amt,
    insure_amt,
    festivities_amt,
    team_building_amt,
    recruit_amt,
    site_selection_amt,
    design_amt,
    support_employee_amt,
    store_quality_error_deduct_amt,
    em_quality_error_deduct_amt,
    business_recycle_tc_amt,
    business_sale_tc_amt,
    golden_share_amt,
    incity_transfer_fee,  --同城调拨费
    gaode_annual_fee,  --高德年费
    sale_motivate_fee,  --零售阶梯激励
    meituan_fee,  --美团商户费用
    luxury_share_fee,  --二奢分润金额
    other_amt,
    other_jms_share_amt_v2 as other_jms_share_amt,
    -1*(insure_amt+festivities_amt+team_building_amt+recruit_amt+support_employee_amt+incity_transfer_fee+gaode_annual_fee+meituan_fee)
    +business_recycle_tc_amt+business_sale_tc_amt as zz_withhold_jms_cost

from
    hdp_ubu_zhuanzhuan_dm_c2b.dm_offline_store_share_data_full_1d
where dt='${outFileSuffix}'
    )


    ,employee_com_result as (
-- 3.员工成本
select
    month,store_id,
    zz_rec_commission_cost,
    zz_sale_commission_cost,
    zz_sale_up_commission_cost,
    zz_rec_commission_cost+zz_sale_commission_cost+zz_sale_up_commission_cost+zz_encourage_commission_cost as zz_commission_cost,
    zz_bcsalary_cost,
    jms_rec_commission_cost,
    jms_sale_commission_cost,
    jms_rec_commission_cost+jms_sale_commission_cost as jms_commission_cost,
    jms_bcsalary_cost,
    zz_encourage_commission_cost,
    person_cnt
from
    (
    select
    a.month,a.store_id,
    nvl(sum(if(store_type_name='自营' and personnel_ownership='自营',rec_com,0))+sum(if(store_type_name='加盟' and personnel_ownership='自营' and cover_side='转',rec_com,0)),0) as zz_rec_commission_cost,
    nvl(sum(if(store_type_name='自营' and personnel_ownership='自营',sale_com,0))+sum(if(store_type_name='加盟' and personnel_ownership='自营' and cover_side='转',sale_com,0)),0) as zz_sale_commission_cost,
    nvl(sum(if(personnel_ownership='自营',sale_up_com,0)),0) as zz_sale_up_commission_cost,
    nvl(sum(if(store_type_name='加盟' and personnel_ownership='加盟',rec_com,0)),0) as jms_rec_commission_cost ,
    nvl(sum(if(store_type_name='加盟' and personnel_ownership='加盟',sale_com,0)),0) as jms_sale_commission_cost,
    nvl(sum(zz_bcsalary_cost),0) as zz_bcsalary_cost,
    nvl(sum(jms_bcsalary_cost),0) as jms_bcsalary_cost,
    nvl(sum(hs_multi_com),0)+nvl(sum(ls_change_com),0) as zz_encourage_commission_cost,
    max(person_cnt) as person_cnt
    from
    (
    select
    month,
    stat_date,
    store_id,
    store_type_name,
    employee_uid,
    role_type,
    personnel_ownership,
    cover_side,
    zz_bcsalary_cost,
    jms_bcsalary_cost,
    rec_order_cnt,
    rec_com - rec_refund_com as rec_com,
    rec_yuanjia,
    rec_jiajia,
    sale_order_cnt,
    sale_com - sale_refund_com as sale_com,
    sale_up_com - sale_up_refund_com as sale_up_com,
    sale_yuanjia,
    sale_jiajia,
    hs_multi_com,
    ls_change_com,
    person_cnt
    from
    hdp_ubu_zhuanzhuan_dm_c2b.dm_offline_store_emp_cost_full_1d
    where  dt='${outFileSuffix}'
    ) a
    group by
    a.month,
    a.store_id
    ) a
    )


        , fixed_cost_result as (
-- 4.固定成本汇总，直接在左表基础上折算到天，不包含员工基本工资绩效
select
    distinct
    a.month,
    a.store_id,
    -- 房租
    nvl(rent*convert_num/days_in_month,0) as rent,
    -- 装修摊销，两边都要承担
    nvl(zx_cost*convert_num/days_in_month,0) as zz_zxcost,
    nvl(zx_jmcost*convert_num/days_in_month,0) as jms_zxcost,
    -- 设备摊销,两边都要承担
    nvl(equip_cost*convert_num/days_in_month,0) as zz_eqcost,
    nvl(equip_jmcost*convert_num/days_in_month,0) as jms_eqcost,
    -- 水电费
    nvl(500*convert_num/days_in_month,0) as pwcost,
    -- 其他费用摊销
    nvl(713*convert_num/days_in_month,0) as othcost,
    -- 行政四费
    nvl(550*convert_num/days_in_month,0) as xingzheng_cost


from
    total_left a
    left join
    hdp_ubu_zhuanzhuan_defaultdb.tmp_store_chengben_240708 b
on a.month=b.month
    and a.store_id = b.store_id

    )



    , t_all_join as (
select
    distinct
    -- 基本信息
    a.month
        ,a.store_id
        ,a.store_name
        ,a.area
        ,a.province
        ,a.city_name
        ,a.group_name
        ,a.store_type_name
        ,a.franchisee_id
        ,a.franchisee_name
        ,a.store_open_date
        ,a.store_close_date
        ,a.is_this_month_open
        ,a.days_in_month
        ,need_qianfan_date
        ,need_data_from_qinfan
        ,convert_num
        ,a.gold_open_flag
        ,a.luxury_open_flag
        ,a.luxury_store_level
        ,a.line_level
        ,a.store_area
        ,a.store_position_type
        ,a.gold_open_date
        ,a.luxury_open_date


        ,a.biz_gold_rate
        ,a.biz_luxury_rate
        ,a.is_pro_store

    -- 业绩
        ,nvl(rec_net_gmv,0) as rec_net_gmv
        ,nvl(rec_net_cnt,0) as rec_net_cnt
        ,nvl(zonghe_online_rec_net_gmv,0) as zonghe_online_rec_net_gmv
        ,nvl(zonghe_offline_rec_net_gmv,0) as zonghe_offline_rec_net_gmv

        ,nvl(online_phone_net_gmv,0) as online_phone_net_gmv
        ,nvl(offline_phone_net_gmv,0) as offline_phone_net_gmv
        ,nvl(online_unphone_net_gmv,0) as online_unphone_net_gmv
        ,nvl(offline_unphone_net_gmv,0) as offline_unphone_net_gmv
        ,nvl(unself_deal_order_net_gmv,0) as unself_deal_order_net_gmv
        ,nvl(online_phone_net_cnt,0) as online_phone_net_cnt
        ,nvl(offline_phone_net_cnt,0) as offline_phone_net_cnt
        ,nvl(online_unphone_net_cnt,0) as online_unphone_net_cnt
        ,nvl(offline_unphone_net_cnt,0) as offline_unphone_net_cnt
        ,nvl(unself_deal_order_net_cnt,0) as unself_deal_order_net_cnt
        ,nvl(sale_net_gmv,0) as sale_net_gmv
        ,nvl(sale_net_cnt,0) as sale_net_cnt
        ,nvl(ts_net_gmv,0) as ts_net_gmv
        ,nvl(ts_net_cnt,0) as ts_net_cnt

        ,nvl(sale_cnt_old_change,0) as sale_cnt_old_change
        ,nvl(sale_cnt_old_change_rate,0) as sale_cnt_old_change_rate
        ,round(sale_net_cnt*nvl(sale_cnt_old_change_rate,0),0)*91 as sale_old_change_cost

        ,nvl(rec_net_gmv+sale_net_gmv,0) as store_net_gmv
        ,nvl(rec_net_cnt+sale_net_cnt,0) as store_net_cnt

        ,nvl(judg_difference_amt,0) as judg_difference_amt
        ,nvl(store_eval_price,0) as store_eval_price
        ,nvl(judg_difference_rate,0) as judg_difference_rate
        ,nvl(coupon_total_amt,0) as coupon_total_amt
        ,nvl(coupon_total_rate,0) as coupon_total_rate
        ,nvl(base_amt_ticket,0) as base_amt_ticket
        ,nvl(rate_ticket_amt,0) as rate_ticket_amt
        ,nvl(amt_ticket_amt,0) as amt_ticket_amt
        ,nvl(emp_add_price,0) as emp_add_price
        ,nvl(apply_markup_amount,0) as apply_markup_amount
        ,nvl(sm_rec_order_subsidy,0) as sm_rec_order_subsidy
        ,nvl(sm_rec_order_gmv,0) as sm_rec_order_gmv
        ,nvl(sm_rec_order_subsidy_rate,0) as sm_rec_order_subsidy_rate
        ,nvl(rec_basic_income,0) as rec_basic_income
        ,nvl(judge_encourage_income,0) as judge_encourage_income
        ,nvl(coupon_encourage_income,0) as coupon_encourage_income
        ,nvl(add_price_amt,0) as add_price_amt
        ,nvl(add_price_rate,0) as add_price_rate
        ,nvl(add_price_encourage_income,0) as add_price_encourage_income
        ,nvl(rec_total_income,0) as rec_total_income
        ,nvl(rpc1,0) as rpc1
        ,nvl(offline_sale_income,0) as offline_sale_income
        ,nvl(ts_sale_income,0) as ts_sale_income
        ,nvl(sale_without_punish_income,0) as sale_without_punish_income
        ,nvl(over30_punish_amt,0) as over30_punish_amt
        ,nvl(over7_punish_amt,0) as over7_punish_amt
        ,nvl(sale_total_income,0) as sale_total_income
        ,nvl(total_income,0) as total_income


    -- 分润和转转毛利额
        ,nvl(recycle_total_share_amt,0) as recycle_total_share_amt
        ,nvl(recycle_online_share_amt,0) as recycle_online_share_amt
        ,nvl(recycle_offline_share_amt,0) as recycle_offline_share_amt
        ,nvl(sale_total_share_amt,0) as sale_total_share_amt
        ,nvl(phone_k_incentive_amt,0) as phone_k_incentive_amt
        ,nvl(renovation_subsidy_amt,0) as renovation_subsidy_amt
        ,nvl(insure_amt,0) as insure_amt
        ,nvl(festivities_amt,0) as festivities_amt
        ,nvl(team_building_amt,0) as team_building_amt
        ,nvl(recruit_amt,0) as recruit_amt
        ,nvl(site_selection_amt,0) as site_selection_amt
        ,nvl(design_amt,0) as design_amt
        ,nvl(support_employee_amt,0) as support_employee_amt
        ,nvl(store_quality_error_deduct_amt,0) as store_quality_error_deduct_amt
        ,nvl(em_quality_error_deduct_amt,0) as em_quality_error_deduct_amt
        ,nvl(business_recycle_tc_amt,0) as business_recycle_tc_amt
        ,nvl(business_sale_tc_amt,0) as business_sale_tc_amt
        ,nvl(other_amt,0) as other_amt
        ,nvl(incity_transfer_fee,0) as incity_transfer_fee  --同城调拨费
        ,nvl(gaode_annual_fee,0) as gaode_annual_fee  --高德年费
        ,nvl(sale_motivate_fee,0) as sale_motivate_fee  --零售阶梯激励
        ,nvl(meituan_fee,0) as meituan_fee  --美团商户费用
        ,nvl(luxury_share_fee,0) as luxury_share_fee  --二奢分润金额
        ,nvl(other_jms_share_amt,0) as other_jms_share_amt
        ,nvl(recycle_total_share_amt+sale_total_share_amt+other_jms_share_amt,0) as total_share_profit
        ,nvl((recycle_total_share_amt+sale_total_share_amt+other_jms_share_amt)/(rec_net_gmv+sale_net_gmv),0) as total_share_profit_rate
        ,nvl(total_income,0) - nvl(recycle_total_share_amt+sale_total_share_amt+other_jms_share_amt,0) as zz_profit
        ,nvl(rec_total_income,0) - nvl(recycle_total_share_amt,0) as zz_rec_profit
        ,nvl(sale_total_income,0) - nvl(sale_total_share_amt,0) as zz_sale_profit
        ,nvl(zz_withhold_jms_cost,0) as zz_withhold_jms_cost
        ,nvl(phone_k_incentive_amt,0) + nvl(sale_motivate_fee,0) as jms_withhold_zz_cost
        ,nvl(zz_rec_commission_cost,0) as zz_rec_commission_cost
        ,nvl(zz_sale_commission_cost,0) as zz_sale_commission_cost
        ,nvl(zz_sale_up_commission_cost,0) as zz_sale_up_commission_cost
        ,nvl(zz_commission_cost,0) as zz_commission_cost
        ,nvl(jms_rec_commission_cost,0) as jms_rec_commission_cost
        ,nvl(jms_sale_commission_cost,0) as jms_sale_commission_cost
        ,nvl(jms_commission_cost,0) as jms_commission_cost
        ,nvl(zz_encourage_commission_cost,0) as zz_encourage_commission_cost
        ,nvl(b.rec_wl_cost,0) as rec_wl_cost
        ,nvl(b.sale_wl_cost,0) as sale_wl_cost
        ,nvl(b.sale_pay_cost,0) as sale_pay_cost
        ,nvl(b.sale_aftersale_cost,0) as sale_aftersale_cost
        ,nvl(b.sale_wrap_cost,0) as sale_wrap_cost
        ,nvl(b.sale_peifu_cost,0) as sale_peifu_cost
        ,nvl(b.ts_wl_cost,0) as ts_wl_cost
        ,nvl(b.ts_commission_cost,0) as ts_commission_cost
        ,nvl(b.zy_rec_order_gmv,0) as zy_rec_order_gmv
        ,nvl(zz_commission_cost,0)
    + nvl(rec_wl_cost,0)
    + nvl(sale_wl_cost,0)
    + nvl(sale_pay_cost,0)
    + nvl(sale_aftersale_cost,0)
    + nvl(sale_wrap_cost,0)
    + nvl(sale_peifu_cost,0)
    + nvl(ts_wl_cost,0)
    -- + nvl(ts_commission_cost,0)
    + nvl(zz_withhold_jms_cost,0) as zz_undertake_non_fixed_cost
        ,nvl(jms_commission_cost,0) + nvl(phone_k_incentive_amt,0) + nvl(sale_motivate_fee,0) as jms_undertake_non_fixed_cost
        ,nvl(rent,0) as rent
        ,nvl(zz_zxcost,0) as zz_zxcost
        ,nvl(jms_zxcost,0) as jms_zxcost
        ,nvl(zz_eqcost,0) as zz_eqcost
        ,nvl(jms_eqcost,0) as jms_eqcost
        ,nvl(pwcost,0) as pwcost
        ,nvl(zz_bcsalary_cost,0) as zz_bcsalary_cost
        ,nvl(jms_bcsalary_cost,0) as jms_bcsalary_cost
        ,nvl(xingzheng_cost,0) as xingzheng_cost
        ,nvl(f.person_cnt,0) as person_cnt
        ,nvl(f.person_cnt,0) * 500 as people_service_cost

from
    total_left a

    left join
    t_gmv_n_profit_result b
on a.month=b.month
    and a.store_id=b.store_id -- 门店视角业绩和毛利额

    left join
    jms_share_result c
    on a.month=c.month
    and a.store_id=c.store_id -- 加盟商分润

    left join
    fixed_cost_result d
    on a.month=d.month
    and a.store_id=d.store_id -- 固定成本

    left join
    employee_com_result f
    on a.month=f.month
    and a.store_id=f.store_id -- 员工提成
    )



insert overwrite table hdp_ubu_zhuanzhuan_ads_c2b.ads_bi_offline_store_operating_data_center_v3_full_1d partition(dt='${outFileSuffix}')

select
    t2.month
     ,t2.store_id
     ,t2.store_name
     ,t2.is_pro_store
     ,t2.area
     ,t2.province
     ,t2.city_name
     ,t2.group_name
     ,t2.store_type_name
     ,t2.franchisee_id
     ,t2.franchisee_name
     ,t2.store_open_date
     ,t2.store_close_date
     ,t2.is_this_month_open
     ,t2.days_in_month
     ,t2.need_qianfan_date
     ,t2.need_data_from_qinfan
     ,t2.convert_num
     ,t2.gold_open_flag
     ,t2.luxury_open_flag
     ,nvl(t2.luxury_store_level,'') as luxury_store_level

     ,t2.line_level
     ,t2.store_area
     ,t2.store_position_type
     ,t2.gold_open_date
     ,t2.luxury_open_date

     ,t2.rec_net_gmv
     ,t2.rec_net_cnt
     ,t2.zonghe_online_rec_net_gmv
     ,t2.zonghe_offline_rec_net_gmv
     ,t2.online_phone_net_gmv
     ,t2.offline_phone_net_gmv
     ,t2.online_unphone_net_gmv
     ,t2.offline_unphone_net_gmv
     ,t2.unself_deal_order_net_gmv
     ,t2.online_phone_net_cnt
     ,t2.offline_phone_net_cnt
     ,t2.online_unphone_net_cnt
     ,t2.offline_unphone_net_cnt
     ,t2.unself_deal_order_net_cnt


     ,t2.sale_net_gmv
     ,t2.sale_net_cnt
     ,t2.ts_net_gmv
     ,t2.ts_net_cnt
     ,t2.sale_cnt_old_change
     ,t2.sale_cnt_old_change_rate
     ,t2.sale_old_change_cost
     ,t2.store_net_gmv
     ,t2.store_net_cnt
     ,t2.judg_difference_amt
     ,t2.store_eval_price
     ,t2.judg_difference_rate
     ,t2.coupon_total_amt
     ,t2.coupon_total_rate
     ,t2.base_amt_ticket
     ,t2.rate_ticket_amt
     ,t2.amt_ticket_amt
     ,t2.emp_add_price
     ,t2.apply_markup_amount
     ,t2.sm_rec_order_subsidy
     ,t2.sm_rec_order_gmv
     ,t2.sm_rec_order_subsidy_rate
     ,t2.rec_basic_income
     ,t2.judge_encourage_income
     ,t2.coupon_encourage_income
     ,t2.add_price_amt
     ,t2.add_price_rate
     ,t2.add_price_encourage_income
     ,t2.rec_total_income
     ,t2.rpc1
     ,t2.offline_sale_income
     ,t2.ts_sale_income
     ,t2.sale_without_punish_income
     ,t2.over30_punish_amt
     ,t2.over7_punish_amt
     ,t2.sale_total_income
     ,t2.total_income
     ,t2.recycle_total_share_amt
     ,t2.recycle_online_share_amt
     ,t2.recycle_offline_share_amt
     ,t2.sale_total_share_amt
     ,t2.phone_k_incentive_amt
     ,t2.renovation_subsidy_amt
     ,t2.insure_amt
     ,t2.festivities_amt
     ,t2.team_building_amt
     ,t2.recruit_amt
     ,t2.site_selection_amt
     ,t2.design_amt
     ,t2.support_employee_amt
     ,t2.store_quality_error_deduct_amt
     ,t2.em_quality_error_deduct_amt
     ,t2.business_recycle_tc_amt
     ,t2.business_sale_tc_amt
     ,t2.other_amt
     ,t2.incity_transfer_fee
     ,t2.gaode_annual_fee
     ,t2.sale_motivate_fee
     ,t2.meituan_fee
     ,t2.other_jms_share_amt
     ,t2.total_share_profit
     ,t2.total_share_profit_rate
     ,t2.zz_profit
     ,t2.zz_rec_profit
     ,t2.zz_sale_profit
     ,t2.zz_withhold_jms_cost
     ,t2.jms_withhold_zz_cost
     ,t2.zz_rec_commission_cost
     ,t2.zz_sale_commission_cost
     ,t2.zz_sale_up_commission_cost
     ,t2.zz_commission_cost
     ,t2.jms_rec_commission_cost
     ,t2.jms_sale_commission_cost
     ,t2.jms_commission_cost
     ,t2.zz_encourage_commission_cost
     ,t2.rec_wl_cost
     ,t2.sale_wl_cost
     ,t2.sale_pay_cost
     ,t2.sale_aftersale_cost
     ,t2.sale_wrap_cost
     ,t2.sale_peifu_cost
     ,t2.ts_wl_cost
     ,t2.ts_commission_cost
     ,t2.zy_rec_order_gmv


     ,t2.zz_undertake_non_fixed_cost
     ,t2.jms_undertake_non_fixed_cost
     ,t2.rent
     ,t2.zz_zxcost
     ,t2.jms_zxcost
     ,t2.zz_eqcost
     ,t2.jms_eqcost
     ,t2.pwcost
     ,t2.zz_bcsalary_cost
     ,t2.jms_bcsalary_cost
     ,t2.xingzheng_cost
     ,t2.person_cnt
     ,t2.people_service_cost
     ,t2.zz_undertake_fixed_cost
     ,t2.jms_undertake_fixed_cost
     ,t2.store_net_profit
     ,t2.zz_net_profit
     ,t2.jms_net_profit



     ,nvl(gold.gold_rec_gmv,0) as gold_rec_gmv
     ,nvl(gold.gold_rec_weight,0) as gold_rec_weight
     ,nvl(gold.gold_rec_cnt,0) as gold_rec_cnt
     ,nvl(gold.gold_store_profit,0) as gold_store_profit
     ,nvl(gold.gold_jms_share,0) as gold_jms_share
     ,nvl(gold.gold_zz_profit,0) as gold_zz_profit
     ,nvl(gold.gold_yb_share,0) as gold_yb_share
     ,nvl(gold.gold_jms_undertake_commission_cost,0) as gold_jms_undertake_commission_cost
     ,nvl(gold.gold_zz_undertake_commission_cost,0) as gold_zz_undertake_commission_cost
     ,nvl(gold.gold_tax,0) as gold_tax
     ,nvl(gold.gold_sf_insure_cost,0) as gold_sf_insure_cost
     ,nvl(gold.gold_consume_cost,0) as gold_consume_cost
     ,nvl(gold.gold_machine_cost,0) as gold_machine_cost
     ,nvl(gold.gold_shelf_cost,0) as gold_shelf_cost
     ,nvl(gold.gold_jms_undertake_non_fixed_cost,0) as gold_jms_undertake_non_fixed_cost
     ,nvl(gold.gold_zz_undertake_non_fixed_cost,0) as gold_zz_undertake_non_fixed_cost
     ,nvl(gold.gold_zz_undertake_fixed_cost,0) as gold_zz_undertake_fixed_cost
     ,nvl(gold.gold_store_net_profit,0) as gold_store_net_profit
     ,nvl(gold.gold_zz_net_profit,0) as gold_zz_net_profit
     ,nvl(gold.gold_jms_net_profit,0) as gold_jms_net_profit
     ,nvl(gold.gold_service_fee_rate,0) as gold_service_fee_rate
     ,nvl(gold.product_gold_real_value,0) as gold_real_value
     ,nvl(gold.gold_zz_machine_cost,0) as gold_zz_machine_cost
     ,nvl(gold.gold_jms_machine_cost,0) as gold_jms_machine_cost


     ,nvl(lux.lux_rec_cnt,0) as lux_rec_cnt
     ,nvl(lux.lux_rec_gmv,0) as lux_rec_gmv
     ,nvl(lux.lux_store_income_with_coupon,0) as lux_store_income_with_coupon
     ,nvl(lux.lux_coupon_gmv,0) as lux_coupon_gmv
     ,nvl(lux.lux_store_income,0) as lux_store_income
     ,nvl(lux.lux_zz_income,0) as lux_zz_income
     ,nvl(lux.lux_franchisee_income,0) as lux_franchisee_income
     ,nvl(lux.lux_baozhuang_cost,0) as lux_baozhuang_cost
     ,nvl(lux.lux_employee_com,0) as lux_employee_com
     ,nvl(lux.lux_jms_undertake_non_fixed_cost,0) as lux_jms_undertake_non_fixed_cost
     ,nvl(lux.lux_zz_undertake_non_fixed_cost,0) as lux_zz_undertake_non_fixed_cost
     ,nvl(lux.lux_zz_undertake_fixed_cost,0) as lux_zz_undertake_fixed_cost
     ,nvl(lux.lux_store_undertake_fixed_cost,0) as lux_store_undertake_non_fixed_cost
     ,nvl(lux.store_undertake_fixed_cost,0) as lux_store_undertake_fixed_cost
     ,nvl(lux.lux_store_net_profit,0) as lux_store_net_profit
     ,nvl(lux.lux_zz_net_profit,0) as lux_zz_net_profit
     ,nvl(lux.lux_jms_net_profit,0) as lux_jms_net_profit


     ,nvl(fit.fit_sale_gmv,0) as fit_sale_gmv
     ,nvl(fit.fit_sale_cnt,0) as fit_sale_cnt
     ,nvl(fit.fit_cur_cost_price,0) as fit_cur_cost_price
     ,nvl(fit.fit_store_profit,0) as fit_store_profit
     ,nvl(fit.fit_jms_share,0) as fit_jms_share
     ,nvl(fit.fit_zz_profit,0) as fit_zz_profit
     ,nvl(fit.fit_jms_undertake_commission_cost,0) as fit_jms_undertake_commission_cost
     ,nvl(fit.fit_jms_undertake_non_fixed_cost,0) as fit_jms_undertake_non_fixed_cost
     ,nvl(fit.fit_zz_undertake_non_fixed_cost,0) as fit_zz_undertake_non_fixed_cost
     ,nvl(fit.fit_zz_undertake_commission_cost,0) as fit_zz_undertake_commission_cost
     ,nvl(fit.fit_store_net_profit,0) as fit_store_net_profit
     ,nvl(fit.fit_zz_net_profit,0) as fit_zz_net_profit
     ,nvl(fit.fit_jms_net_profit,0) as fit_jms_net_profit
     ,nvl(fit.fit_encourage_com,0) as fit_encourage_com

from
    (
        select
            *,
            nvl(total_income-zz_undertake_fixed_cost-jms_undertake_fixed_cost-zz_undertake_non_fixed_cost-jms_undertake_non_fixed_cost,0) as store_net_profit,
            nvl(zz_profit-zz_undertake_fixed_cost-zz_undertake_non_fixed_cost,0) as zz_net_profit,
            nvl(total_share_profit-jms_undertake_fixed_cost-jms_undertake_non_fixed_cost,0) as jms_net_profit
        from
            (
                select
                    *,

                    case when store_type_name ='自营' then rent + pwcost + zz_zxcost + zz_eqcost + zz_bcsalary_cost + xingzheng_cost + people_service_cost
                         when store_type_name ='加盟' then zz_zxcost + zz_eqcost + zz_bcsalary_cost + xingzheng_cost + people_service_cost
                        end as zz_undertake_fixed_cost,
                    case when  store_type_name ='自营' then 0
                         when  store_type_name ='加盟' then rent + pwcost + jms_zxcost + jms_eqcost + jms_bcsalary_cost
                        end as jms_undertake_fixed_cost
                -- jms_commission_cost + jms_withhold_zz_cost  as jms_undertake_non_fixed_cost
                from
                    t_all_join
            ) t1
    ) t2

        left join
    hdp_ubu_zhuanzhuan_tmp_c2b.tmp_recycle_gold_order_data_v3 as gold
    on t2.month = gold.deal_month
        and t2.store_id = gold.store_id

        left join
    hdp_ubu_zhuanzhuan_tmp_c2b.tmp_recycle_lux_order_data_v3 as lux
    on t2.month = lux.pay_month
        and t2.store_id = lux.store_id

        left join
    hdp_ubu_zhuanzhuan_tmp_c2b.tmp_recycle_fit_order_data_v2 as fit
    on t2.month = fit.deal_month
        and t2.store_id = fit.store_id


