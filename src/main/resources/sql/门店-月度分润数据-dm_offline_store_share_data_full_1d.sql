set start_date=2023-01-01; -- 起始日期

with total_left  as (
    select
    month,
    stat_date,
    store_id,
    store_name,
    store_type_name,
    store_open_date,
    area,
    province,
    city_name,
    group_name,
    franchisee_id,
    franchisee_name,
    days_in_month,
    this_month_finish,
    is_this_month_open,
    need_qianfan_date,
    need_data_from_qinfan,
    convert_num
from
    hdp_ubu_zhuanzhuan_dm_c2b.dm_offline_store_action_data_full_1d
where
    dt='${outFileSuffix}'
    ), t_recycle as(
-- 回收业绩和回收退回
select
    distinct
    'pay' as order_type,
    seller_id,order_id,
    deliver_employee_id as employee_uid,
    nvl(total_real_price/100,0)  AS total_real_price,
    0 as apply_coupon_amt,
    0 as coupon_content_amt,
    0 as apply_markup_amount,
    nvl(total_real_price/100,0)-nvl(apply_coupon_amt/100,0)-nvl(coupon_content_amt/100,0)-nvl(apply_markup_amount/100,0) as rec_yuanjia,
    to_date(pay_time) AS stat_date,
    store_id,
    recycle_source
from hdp_ubu_zhuanzhuan_dm_c2b.dm_recycle_offline_order_detail_full_1d
where
    dt = '${outFileSuffix}'
  and store_name not like '%测试%'
  and to_date(pay_time) between '${start_date}' and '${outFileSuffix}'
union all
select
    distinct
    'refund' as order_type,
    seller_id,order_id,
    deliver_employee_id as employee_uid,
    nvl(total_real_price/100,0)  AS total_real_price,
    nvl(apply_coupon_amt/100,0) as apply_coupon_amt,
    nvl(coupon_content_amt/100,0) as coupon_content_amt,
    nvl(apply_markup_amount/100,0) as apply_markup_amount,
    nvl(total_real_price/100,0)-nvl(apply_coupon_amt/100,0)-nvl(coupon_content_amt/100,0)-nvl(apply_markup_amount/100,0) as rec_yuanjia,
    to_date(refund_create_time) AS stat_date,
    store_id,
    recycle_source
from hdp_ubu_zhuanzhuan_dm_c2b.dm_recycle_offline_order_detail_full_1d
where
    dt = '${outFileSuffix}'
  and refund_type in (1,2)
  and store_name not like '%测试%'
  and to_date(refund_create_time) between '${start_date}' and '${outFileSuffix}'

    )
    , t_ls as
    (
-- 零售业绩和退回
select
    'pay' as order_type,
    store_id,
    deal_price,
    up_price/100 as up_price,
    (deal_price-up_price)/100 as real_price,
    case when cate_name  = '手机' and brand_name  = '苹果' then '苹果'
    when cate_name  = '手机' and brand_name  != '苹果' then '安卓'
    else '非手机'
    end as cate_type,
    to_date(outbound_time) as stat_date,
    child_order_id as order_id,
    staff_id as employee_uid
from
    (

    select
    store_id
        ,deal_price
        ,up_price
        ,cate_name
        ,brand_name
        ,outbound_time
        ,child_order_id
        ,staff_id
    from
    hdp_zhuanzhuan_dw_global.dw_trade_retail_offline_data_full_1d
    where dt='${outFileSuffix}'
    and store_name not like '%测试%'
    and to_date(outbound_time) between '${start_date}' and '${outFileSuffix}'
    and order_state IN (20, 50, 60,70)
    and order_type in (1,2)

    union all

    select
    store_id
        ,deal_price
        ,0 as up_price
        ,cate_name
        ,brand_name
        ,outbound_time
        ,child_order_id
        ,staff_id
    from
    hdp_ubu_zhuanzhuan_dw_c2b.dw_trade_sale_store_pro_retail_offline_data_full_1d
    where order_type in (1,2) -- 门店订单
    and purchase_channels in (1,3) -- 天路货源，首次销售+售后货源
    and state=20 -- 订单状态已完成
    and dt='${outFileSuffix}'
    and date(outbound_time) between '2021-01-01' and '${outFileSuffix}'

    ) t1

union all

select
    'refund' as order_type,
    store_id,
    deal_price,
    up_price/100 as up_price,
    (deal_price-up_price)/100 as real_price,
    case when cate_name  = '手机' and brand_name  = '苹果' then '苹果'
    when cate_name  = '手机' and brand_name  <> '苹果' then '安卓'
    else '非手机'
    end as cate_type,
    to_date(back_time) as stat_date,
    child_order_id as order_id,
    staff_id as employee_uid
from
    (

    select
    store_id
        ,deal_price
        ,up_price
        ,cate_name
        ,brand_name
        ,back_time
        ,child_order_id
        ,staff_id

    from
    hdp_zhuanzhuan_dw_global.dw_trade_retail_offline_data_full_1d
    where dt='${outFileSuffix}'
    and store_name not like '%测试%'
    and to_date(back_time) between '${start_date}' and '${outFileSuffix}'
    and order_state IN (20, 50, 60,70)
    and order_type in (1,2)


    union all

    select
    store_id
        ,deal_price
        ,0 as up_price
        ,cate_name
        ,brand_name
        ,afs_finish_time as back_time
        ,child_order_id
        ,staff_id
    from
    hdp_ubu_zhuanzhuan_dw_c2b.dw_trade_sale_store_pro_retail_offline_data_full_1d
    where order_type in (1,2) -- 门店订单
    and purchase_channels in (1,3) -- 天路货源，首次销售+售后货源
    and state=20 -- 订单状态已完成
    and dt='${outFileSuffix}'
    and date(afs_finish_time) between '2021-01-01' and '${outFileSuffix}'

    ) t2
    )

        , t_recycle_net as(
-- 剔除退回之后的回收净业绩
select
    stat_date,
    store_id,
    nvl(rec_order_gmv,0)-nvl(refund_rec_order_gmv,0) as rec_net_gmv,
    nvl(rec_online_order_gmv,0)-nvl(refund_rec_online_order_gmv,0) as online_rec_net_gmv,
    nvl(rec_offline_order_gmv,0)-nvl(refund_rec_offline_order_gmv,0) as offline_rec_net_gmv,
    nvl(rec_order_cnt,0)-nvl(refund_rec_order_cnt,0) as rec_net_cnt,
    nvl(rec_order_cnt,0) as rec_order_cnt
from(
    select
    stat_date,
    store_id,
    sum(if(order_type='pay',total_real_price,0)) as rec_order_gmv,
    sum(if(order_type='pay' and recycle_source='线上导流', total_real_price,0)) as rec_online_order_gmv,
    sum(if(order_type='pay' and recycle_source='线下回收', total_real_price,0)) as rec_offline_order_gmv,
    count(distinct if(order_type='pay',order_id,null)) as rec_order_cnt,
    count(distinct if(order_type='pay' and recycle_source='线上导流', order_id,null)) as rec_online_order_cnt,
    count(distinct if(order_type='pay' and recycle_source='线下回收', order_id,null)) as rec_offline_order_cnt,
    sum(if(order_type='refund',total_real_price,0)) as refund_rec_order_gmv,
    sum(if(order_type='refund' and recycle_source='线上导流', total_real_price,0)) as refund_rec_online_order_gmv,
    sum(if(order_type='refund' and recycle_source='线下回收', total_real_price,0)) as refund_rec_offline_order_gmv,
    count(distinct if(order_type='refund',order_id,null)) as refund_rec_order_cnt,
    count(distinct if(order_type='refund' and recycle_source='线上导流', order_id,null)) as refund_rec_online_order_cnt,
    count(distinct if(order_type='refund' and recycle_source='线下回收', order_id,null)) as refund_rec_offline_order_cnt
    from
    t_recycle
    group by
    stat_date,
    store_id) t1

    ), t_sale_net as(
-- 零售业绩，只看门店订单，不看直播店
select
    stat_date,
    store_id,
    nvl(sale_gmv,0)-nvl(refund_gmv,0) as sale_net_gmv,
    nvl(sale_cnt,0)-nvl(refund_cnt,0) as sale_net_cnt,
    nvl(sale_cnt,0) as sale_cnt
from(
    select
    stat_date,
    store_id,
    sum(if(order_type = 'pay',1,0)) as sale_cnt,
    sum(if(order_type = 'pay', deal_price/100,0.0)) sale_gmv,
    sum(if(order_type = 'refund',1,0)) as refund_cnt,
    sum(if(order_type = 'refund', deal_price/100,0.0)) as refund_gmv
    from
    t_ls
    group by
    stat_date,
    store_id
    ) t1

    ), t_share as (
-- 回收零售分润数据汇总
select
    substr(stat_date,1,7) as month,store_id,
    sum(recycle_total_share_amt) as recycle_total_share_amt,
    sum(recycle_online_share_amt) as recycle_online_share_amt,
    sum(recycle_offline_share_amt) as recycle_offline_share_amt,
    sum(refund_total_share_amt) as refund_total_share_amt,
    sum(refund_online_share_amt) as refund_online_share_amt,
    sum(refund_offline_share_amt) as refund_offline_share_amt,
    sum(sale_jms_share_amt) as sale_jms_share_amt,
    sum(refund_jms_share_amt) as refund_jms_share_amt
from(
    select
    stat_date,store_id,
    sum(rec_online_jms_share + rec_offline_jms_share) as recycle_total_share_amt,
    sum(rec_online_jms_share) as recycle_online_share_amt,
    sum(rec_offline_jms_share) as recycle_offline_share_amt,
    sum(refund_online_jms_share + refund_offline_jms_share) as refund_total_share_amt,
    sum(refund_online_jms_share) as refund_online_share_amt,
    sum(refund_offline_jms_share) as refund_offline_share_amt,
    0 as sale_jms_share_amt,
    0 as refund_jms_share_amt
    from(
    select
    distinct
    t1.stat_date,
    t1.store_id,
    t1.order_id,
    if(t1.order_type = 'pay' and t1.recycle_source='线上导流', nvl(franchisee_profit/100,0), 0) as rec_online_jms_share,
    if(t1.order_type = 'pay' and t1.recycle_source='线下回收', nvl(franchisee_profit/100,0), 0) as rec_offline_jms_share,
    if(t1.order_type = 'refund' and t1.recycle_source='线上导流', nvl(franchisee_profit/100,0), 0) as refund_online_jms_share,
    if(t1.order_type = 'refund' and t1.recycle_source='线下回收', nvl(franchisee_profit/100,0), 0) as refund_offline_jms_share
    from
    t_recycle t1
    join
    --hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_store_bds_t_account_statement_recycle_full_1d t2
    hdp_ubu_zhuanzhuan_dw_c2b.dw_trade_t_account_statement_recycle_detail_full_1d t2
    on
    t1.order_id=t2.recycle_order_id
    and t2.dt='${outFileSuffix}'
    ) t1
    group by
    stat_date,
    store_id
    union all
    -- 零售分润
    select
    stat_date,store_id,
    0 as recycle_total_share_amt,
    0 as recycle_online_share_amt,
    0 as recycle_offline_share_amt,
    0 as refund_total_share_amt,
    0 as refund_online_share_amt,
    0 as refund_offline_share_amt,
    sum(sale_jms_share_amt) as sale_jms_share_amt,
    sum(refund_jms_share_amt) as refund_jms_share_amt
    from(
    select
    distinct
    t1.stat_date,
    t1.order_id,
    t1.store_id,
    if(order_type = 'pay',nvl(fr_profit/100,0),0) as sale_jms_share_amt,
    if(order_type = 'refund',nvl(fr_profit/100,0),0) as refund_jms_share_amt
    from
    t_ls t1
    join
    --hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_store_bds_t_account_statement_sales_full_1d t2
    hdp_ubu_zhuanzhuan_dw_c2b.dw_trade_t_account_statement_sales_detail_full_1d t2
    on
    t1.order_id=t2.sales_order_id
    and t2.dt='${outFileSuffix}'
    ) t1
    group by
    stat_date,
    store_id
    ) t1
group by
    substr(stat_date,1,7),
    store_id
    ), t_jms_share1 as (
-- 从加盟店分润专项表取的，如果一个月已经过完了，就用这里的分润数据
select
    settlement_month as month,
    store_id,
    sum(recycle_total_share_amt)/100 as recycle_total_share_amt,
    sum(recycle_online_share_amt)/100 as recycle_online_share_amt,
    sum(recycle_offline_share_amt)/100 as recycle_offline_share_amt,
    sum(sale_total_share_amt)/100+sum(franchise_em_add_share_amt)/100 as sale_total_share_amt -- 得加上给员工的零售加价提成
from
    hdp_ubu_zhuanzhuan_dw_c2b.dw_trade_store_franchisee_profit_share_full_1d
where
    dt='${outFileSuffix}'
group by
    settlement_month,
    store_id
    ),t_other_jms_share as (
-- 其他分润项，需要从加盟商分润专项表里面取
select
    month,
    store_id,
    phone_k_incentive_amt,
    renovation_subsidy_amt,
    insure_amt,festivities_amt,
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
    incity_transfer_fee,
    gaode_annual_fee,
    sale_motivate_fee,
    meituan_fee,
    luxury_share_fee,
    other_amt,

    -- 除了商承担的自营店员提成之外的所有项目相加  v3新增 减去 business_recycle_tc_amt  business_sale_tc_amt
    phone_k_incentive_amt+renovation_subsidy_amt+insure_amt+festivities_amt+team_building_amt+recruit_amt+site_selection_amt
    +design_amt+support_employee_amt+store_quality_error_deduct_amt+em_quality_error_deduct_amt-business_recycle_tc_amt
    -business_sale_tc_amt+golden_share_amt+other_amt+incity_transfer_fee+gaode_annual_fee+sale_motivate_fee+meituan_fee+luxury_share_fee as other_jms_share_amt,

    phone_k_incentive_amt+renovation_subsidy_amt+insure_amt+festivities_amt+team_building_amt+recruit_amt
    +site_selection_amt+design_amt+support_employee_amt+store_quality_error_deduct_amt+em_quality_error_deduct_amt
    -business_recycle_tc_amt-business_sale_tc_amt+other_amt+incity_transfer_fee+gaode_annual_fee
    +sale_motivate_fee+meituan_fee as other_jms_share_amt_v2  --其他分润金额 v2 口徑  去掉二奢分润金额和黄金回收分润金额


from(
    select
    settlement_month as month,store_id,
    sum(phone_k_incentive_amt/100) as phone_k_incentive_amt,
    sum(renovation_subsidy_amt/100) as renovation_subsidy_amt,
    sum(insure_amt/100) as insure_amt,
    sum(festivities_amt/100) as festivities_amt,
    sum(team_building_amt/100) as team_building_amt,
    sum(recruit_amt/100) as recruit_amt,
    sum(site_selection_amt/100) as site_selection_amt,
    sum(design_amt/100) as design_amt,
    sum(support_employee_amt/100) as support_employee_amt,
    sum(store_quality_error_deduct_amt/100) as store_quality_error_deduct_amt,
    sum(em_quality_error_deduct_amt/100) as em_quality_error_deduct_amt,
    sum(business_recycle_tc_amt/100) as business_recycle_tc_amt,
    sum(business_sale_tc_amt/100) as business_sale_tc_amt,
    sum(golden_share_amt/100) as golden_share_amt,  --新增黄金分润
    sum(incity_transfer_fee/100) as incity_transfer_fee,  --同城调拨费
    sum(gaode_annual_fee/100) as gaode_annual_fee,  --高德年费
    sum(sale_motivate_fee/100) as sale_motivate_fee,  --零售阶梯激励
    sum(meituan_fee/100) as meituan_fee,  --美团商户费用
    sum(luxury_share_fee/100) as luxury_share_fee,  --二奢分润费用
    sum(other_amt/100) as other_amt
    from
    hdp_ubu_zhuanzhuan_dw_c2b.dw_trade_store_franchisee_profit_share_full_1d
    where
    dt='${outFileSuffix}'
    group by
    settlement_month,store_id
    ) t1
    ),t_jms_share_result as (
-- 加盟商分润数据汇总
select
    distinct
    -- 如果这个月已经过完，就用加盟店分润宽表的数据，没过完就用订单聚合的数据
    t1.month,t1.store_id,
    -- 城市维度
    nvl(case when need_data_from_qinfan=1 then t3.recycle_total_share_amt else t2.recycle_total_share_amt - t2.refund_total_share_amt end,0) as recycle_total_share_amt_city,
    nvl(case when need_data_from_qinfan=1 then t3.recycle_online_share_amt else t2.recycle_online_share_amt - t2.refund_online_share_amt end,0) as recycle_online_share_amt_city,
    nvl(case when need_data_from_qinfan=1 then t3.recycle_offline_share_amt else t2.recycle_offline_share_amt - t2.refund_offline_share_amt end,0) as recycle_offline_share_amt_city,
    nvl(case when need_data_from_qinfan=1 then t3.sale_total_share_amt else t2.sale_jms_share_amt - t2.refund_jms_share_amt end,0) as sale_total_share_amt_city,
    --  总部维度
    nvl(case when need_data_from_qinfan=1 then t3.recycle_total_share_amt else t2.recycle_total_share_amt end,0) as recycle_total_share_amt,
    nvl(case when need_data_from_qinfan=1 then t3.recycle_online_share_amt else t2.recycle_online_share_amt end,0) as recycle_online_share_amt,
    nvl(case when need_data_from_qinfan=1 then t3.recycle_offline_share_amt else t2.recycle_offline_share_amt end,0) as recycle_offline_share_amt,
    nvl(case when need_data_from_qinfan=1 then t3.sale_total_share_amt else t2.sale_jms_share_amt - t2.refund_jms_share_amt  end,0) as sale_total_share_amt,
    -- 其他分润项直接从加盟店分润宽表取，只有一个月过完再结算的时候才会上传
    nvl(t4.phone_k_incentive_amt,0) as phone_k_incentive_amt,
    nvl(t4.renovation_subsidy_amt,0) as renovation_subsidy_amt,
    nvl(t4.insure_amt,0) as insure_amt,
    nvl(t4.festivities_amt,0) as festivities_amt,
    nvl(t4.team_building_amt,0) as team_building_amt,
    nvl(t4.recruit_amt,0) as recruit_amt,
    nvl(t4.site_selection_amt,0) as site_selection_amt,
    nvl(t4.design_amt,0) as design_amt,
    nvl(t4.support_employee_amt,0) as support_employee_amt,
    nvl(t4.store_quality_error_deduct_amt,0) as store_quality_error_deduct_amt,
    nvl(t4.em_quality_error_deduct_amt,0) as em_quality_error_deduct_amt,
    nvl(t4.business_recycle_tc_amt,0) as business_recycle_tc_amt,
    nvl(t4.business_sale_tc_amt,0) as business_sale_tc_amt,
    nvl(t4.golden_share_amt,0) as golden_share_amt,
    nvl(t4.incity_transfer_fee,0) as incity_transfer_fee,
    nvl(t4.gaode_annual_fee,0) as gaode_annual_fee,
    nvl(t4.sale_motivate_fee,0) as sale_motivate_fee,
    nvl(t4.meituan_fee,0) as meituan_fee,
    nvl(t4.luxury_share_fee,0) as luxury_share_fee,
    nvl(t4.other_amt,0) as other_amt,
    nvl(t4.other_jms_share_amt,0) as other_jms_share_amt,
    nvl(t4.other_jms_share_amt_v2,0) as other_jms_share_amt_v2  --其他分润金额 v2 口径
from total_left t1
    left join t_share t2 on t1.month=t2.month and t1.store_id=t2.store_id
    left join t_jms_share1 t3 on t1.month=t3.month and t1.store_id=t3.store_id -- 这个是按月的
    left join t_other_jms_share t4 on t1.month=t4.month and t1.store_id=t4.store_id -- 这个是按月的
    )


insert overwrite table hdp_ubu_zhuanzhuan_dm_c2b.dm_offline_store_share_data_full_1d partition(dt='${outFileSuffix}')
select
    month,
    store_id,
    recycle_total_share_amt_city,
    recycle_online_share_amt_city,
    recycle_offline_share_amt_city,
    sale_total_share_amt_city,
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
    other_amt,
    other_jms_share_amt,
    golden_share_amt,
    incity_transfer_fee,
    gaode_annual_fee,
    sale_motivate_fee,
    meituan_fee,
    luxury_share_fee,
    other_jms_share_amt_v2
from
    t_jms_share_result