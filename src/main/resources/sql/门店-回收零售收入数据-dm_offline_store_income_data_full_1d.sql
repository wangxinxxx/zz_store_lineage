



set start_date=2023-01-01; -- 起始日期

-- drop table if exists hdp_ubu_zhuanzhuan_tmp_c2b.test_dm_offline_store_income_data_full_1d;
-- create table hdp_ubu_zhuanzhuan_tmp_c2b.test_dm_offline_store_income_data_full_1d as

with total_left  as (
    select
        distinct
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
where dt='${outFileSuffix}'
    ),



    judge as (
select
    -- date_format(judge_duty_date,'yyyy-MM') as stat_month
    stat_date
        ,store_id
        ,sum(store_eval_price) as store_eval_price
        ,sum(judg_difference_amt) as judg_difference_amt
from
    (
    select
    distinct
    order_no
        ,station_id as store_id
        ,to_date(judge_duty_finish_time) as stat_date
        ,store_eval_price / 100 as store_eval_price
        ,judg_difference_amt / 100 as judg_difference_amt
    from
    hdp_ubu_zhuanzhuan_dm_c2b.dm_recycle_home_offline_difference_order_full_1d
    where dt = '${outFileSuffix}'
    and to_date(judge_duty_finish_time) between '2023-01-01' and '${outFileSuffix}'
    and business_type = '门店'
    and station_name not like '%测试%'
    ) t1

group by
    -- date_format(judge_duty_date,'yyyy-MM')
    stat_date
        ,store_id
    ),




    t_recycle as(
-- 回收业绩和回收退回
select
    distinct
    'pay' as order_type
        ,seller_id
        ,order_id
        ,deliver_employee_id as employee_uid
        ,nvl(total_real_price/100,0)  as total_real_price
        ,0 as apply_coupon_amt
        ,0 as coupon_content_amt
        ,apply_markup_amount / 100 as apply_markup_amount  -- 调整
        ,nvl(total_real_price/100,0)-nvl(apply_coupon_amt/100,0)-nvl(coupon_content_amt/100,0)-nvl(apply_markup_amount/100,0) as rec_yuanjia
        ,to_date(pay_time) AS stat_date
        ,store_id
        ,recycle_source
        ,actual_source
        ,case when cate_name = '手机' then '手机' else '非手机' end as cate_name
        ,rate_ticket_amt / 100 as rate_ticket_amt
        ,amt_ticket_amt / 100 as amt_ticket_amt
        ,base_amt_ticket / 100 as base_amt_ticket
        ,emp_add_price / 100 as emp_add_price
        ,reservation_store_name
        ,case when deal_type = '竞拍' then '竞拍'
    else '自营'
    end as deal_type
from
    hdp_ubu_zhuanzhuan_dm_c2b.dm_recycle_offline_order_detail_full_1d
where dt = '${outFileSuffix}'
  and store_name not like '%测试%'
  and to_date(pay_time) between '2023-01-01' and '${outFileSuffix}'

union all

select
    distinct
    'refund' as order_type
        ,seller_id,order_id
        ,deliver_employee_id as employee_uid
        ,nvl(total_real_price/100,0)  AS total_real_price
        ,nvl(apply_coupon_amt/100,0) as apply_coupon_amt
        ,nvl(coupon_content_amt/100,0) as coupon_content_amt
        ,nvl(apply_markup_amount/100,0) as apply_markup_amount
        ,nvl(total_real_price/100,0)-nvl(apply_coupon_amt/100,0)-nvl(coupon_content_amt/100,0)-nvl(apply_markup_amount/100,0) as rec_yuanjia
        ,to_date(refund_create_time) AS stat_date
        ,store_id
        ,recycle_source
        ,actual_source
        ,case when cate_name = '手机' then '手机'
    else '非手机'
    end as cate_name
        ,rate_ticket_amt
        ,amt_ticket_amt
        ,base_amt_ticket
        ,emp_add_price
        ,reservation_store_name
        ,case when deal_type = '竞拍' then '竞拍'
    else '自营'
    end as deal_type
from
    hdp_ubu_zhuanzhuan_dm_c2b.dm_recycle_offline_order_detail_full_1d
where dt = '${outFileSuffix}'
  and refund_type in (1,2)
  and store_name not like '%测试%'
  and to_date(refund_create_time) between '2023-01-01' and '${outFileSuffix}'
    ),

    sale_rpc as ( -- 零售rpc，用每个月的大盘值套给每个店
select
    price_month,
    rpc1,
    round(rpc1-1,4) as rpc_use, -- 减掉1之后的部分才是要用的，还要四舍五入一下
    round(if(rpc1<1,1,rpc1)-1,4) as rpc_use_v2 -- 减掉1之后的部分才是要用的，还要四舍五入一下
from
    (
    select
    substr(statistics_date,1,7) as price_month,
    sum(czc_mn_gmv) as czc_mn_gmv,  --czc_模拟gmv
    sum(store_gmv_n) as store_gmv_n,  --门店销量
    sum(store_gmv_n)/sum(czc_mn_gmv) as rpc1
    from
    hdp_ubu_zhuanzhuan_tmp_c2b.retail_price_table2 -- 直接用目前RPC看板的结果表
    where dt='${outFileSuffix}'
    and substr(statistics_date,1,7) between '2023-01' and '2025-08' -- 这个之后要改成2301开始
    and store_kdj>0  --门店客单价
    and czc_kdj>0  --czc客单价
    group by
    substr(statistics_date,1,7)

    union all

    select
    substr(statistics_date,1,7) as price_month,
    sum(czc_mn_gmv) as czc_mn_gmv,
    sum(store_gmv_n) as store_gmv_n,
    sum(store_gmv_n)/sum(czc_mn_gmv) as rpc1
    from
    hdp_zhuanzhuan_tmp_global.tmp_new_retail_price_table_2_day
    where dt='${outFileSuffix}'
    and substr(statistics_date,1,7) >='2025-09'
    and store_kdj>0
    and czc_kdj>0
    group by
    substr(statistics_date,1,7)
    ) a
    ),

    t_ls as (
-- 零售业绩和退回
select
    'pay' as order_type,
    order_type as type,
    store_id,
    deal_price,
    up_price/100 as up_price,
    actual_cost_price/100 AS actual_cost_price,-- 实际成本价
    (deal_price/100-actual_cost_price/100) AS real_diff_price, -- 真实差价毛利额
    case when (deal_price/100-actual_cost_price/100)>deal_price/100*0.09 then deal_price/100*0.09 -- 按单笔9%上限处理毛利额
    else (deal_price/100-actual_cost_price/100)
    end as diff_price, -- 处理后差价毛利额

    case when (deal_price/100-actual_cost_price/100)>deal_price/100*(0.0565+rpc_use) then deal_price/100*(0.0565+rpc_use)
    else (deal_price/100-actual_cost_price/100)
    end as diff_price_v2, -- 处理后差价毛利额 v2 口径

    case when (deal_price/100-actual_cost_price/100)>deal_price/100*(0.0565+rpc_use_v2) then deal_price/100*(0.0565+rpc_use_v2)
    else (deal_price/100-actual_cost_price/100)
    end as diff_price_v3, -- 处理后差价毛利额 v3 口径

    -- (deal_price-up_price)/100 as real_price,
    case when cate_name  = '手机' and brand_name  = '苹果' then '苹果'
    when cate_name  = '手机' and brand_name  != '苹果' then '安卓'
    else '非手机' end as cate_type,
    to_date(outbound_time) as stat_date,
    child_order_id as order_id,
    staff_id as employee_uid,
    non_coupon_recycle_price,
    rpc1
from
    (

    select
    order_type
        ,store_id
        ,deal_price
        ,up_price
        ,actual_cost_price
        ,outbound_time
        ,child_order_id
        ,staff_id
        ,cate_name
        ,brand_name
    from
    hdp_zhuanzhuan_dw_global.dw_trade_retail_offline_data_full_1d
    where dt='${outFileSuffix}'
    and store_name not like '%测试%'
    and to_date(outbound_time) between '2023-01-01' and '${outFileSuffix}'
    and order_state IN (20, 50, 60,70)
    and order_type in (1,2,3)


    union all


    select
    order_type
        ,store_id
        ,deal_price
        ,0 as up_price
        ,0 as actual_cost_price
        ,outbound_time
        ,child_order_id
        ,staff_id
        ,cate_name
        ,brand_name
    from
    hdp_ubu_zhuanzhuan_dw_c2b.dw_trade_sale_store_pro_retail_offline_data_full_1d
    where order_type in (1,2) -- 门店订单
    and purchase_channels in (1,3) -- 天路货源，首次销售+售后货源
    and state=20 -- 订单状态已完成
    and dt='${outFileSuffix}'
    and date(outbound_time) between '2021-01-01' and '${outFileSuffix}'

    ) a


    left join
    sale_rpc
on substr(a.outbound_time,1,7)=sale_rpc.price_month -- 取每个月的大盘rpc


    left join
    ( -- 以旧换优
    select
    a.sales_order_id
    ,sum(round(total_real_price/100,2)) as total_real_price  --`以旧换优回收单C1实得`,
    ,sum(round(non_coupon_recycle_price/100,2)) as non_coupon_recycle_price  --`以旧换优回收核算绩效金额`
    from
    hdp_ubu_zhuanzhuan_dw_c2b.dw_market_coupon_t_activity_coupon_relation_full_1d a

    left join
    hdp_ubu_zhuanzhuan_dm_c2b.dm_recycle_offline_order_detail_full_1d b
    on a.origin_order_id=b.order_id
    and b.dt='${outFileSuffix}'  -- 取对应回收单的信息

    where a.dt='${outFileSuffix}'
    and red_status=2 -- 红包已使用
    and scene_type in (0) -- 0 以旧换优
    group by
    sales_order_id
    ) coupon0
    on coupon0.sales_order_id=a.child_order_id


/*
  where dt='${outFileSuffix}'
  and store_name not like '%测试%'
  and to_date(outbound_time) between '2023-01-01' and '${outFileSuffix}'
  and order_state IN (20, 50, 60,70)
  and order_type in (1,2,3)
*/

union all

select
    'refund' as order_type,
    order_type as type,
    store_id,
    deal_price,
    up_price/100 as up_price,
    0 as actual_cost_price,
    0 as real_diff_price,
    0 as diff_price,
    0 as diff_price_v2,
    0 as diff_price_v3,
    -- (deal_price-up_price)/100 as real_price,
    case when cate_name  = '手机' and brand_name  = '苹果' then '苹果'
    when cate_name  = '手机' and brand_name  <> '苹果' then '安卓'
    else '非手机' end as cate_type,
    to_date(back_time) as stat_date,
    child_order_id as order_id,
    staff_id as employee_uid,
    0 as non_coupon_recycle_price,
    0 as rpc1
from
    (

    select
    order_type
        ,store_id
        ,deal_price
        ,up_price
        ,back_time
        ,cate_name
        ,brand_name
        ,child_order_id
        ,staff_id

    from
    hdp_zhuanzhuan_dw_global.dw_trade_retail_offline_data_full_1d
    where dt='${outFileSuffix}'
    and store_name not like '%测试%'
    and to_date(back_time) between '2023-01-01' and '${outFileSuffix}'
    and order_state in (20, 50, 60,70)
    and order_type in (1,2,3)


    union all


    select
    order_type
        ,store_id
        ,deal_price
        ,0 as up_price
        ,afs_finish_time as back_time
        ,cate_name
        ,brand_name
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

    ),

    t_recycle_net as(
-- 剔除退回之后的回收净业绩
select
    stat_date
        ,store_id
        ,nvl(rec_order_gmv,0) as rec_order_gmv
        ,nvl(rec_order_gmv,0)-nvl(refund_rec_order_gmv,0) as rec_net_gmv
        ,nvl(rec_online_order_gmv,0)-nvl(refund_rec_online_order_gmv,0) as online_rec_net_gmv
        ,nvl(rec_offline_order_gmv,0)-nvl(refund_rec_offline_order_gmv,0) as offline_rec_net_gmv
        ,nvl(rec_order_cnt,0)-nvl(refund_rec_order_cnt,0) as rec_net_cnt
        ,nvl(rec_order_cnt,0) as rec_order_cnt
        ,nvl(online_phone_cnt,0) as online_phone_cnt
        ,nvl(offline_phone_cnt,0) as offline_phone_cnt
        ,nvl(online_unphone_cnt,0) as online_unphone_cnt
        ,nvl(offline_unphone_cnt,0) as offline_unphone_cnt
        ,nvl(unself_deal_order_cnt,0) as unself_deal_order_cnt
        ,nvl(online_phone_gmv,0) as online_phone_gmv
        ,nvl(offline_phone_gmv,0) as offline_phone_gmv
        ,nvl(online_unphone_gmv,0) as online_unphone_gmv
        ,nvl(offline_unphone_gmv,0) as offline_unphone_gmv
        ,nvl(unself_deal_order_gmv,0) as unself_deal_order_gmv
        ,nvl(base_amt_ticket,0) as base_amt_ticket
        ,nvl(rate_ticket_amt,0) as rate_ticket_amt
        ,nvl(amt_ticket_amt,0) as amt_ticket_amt
        ,nvl(emp_add_price,0) as emp_add_price
        ,nvl(apply_markup_amount,0) as apply_markup_amount
        ,nvl(self_deal_order_gmv,0) as self_deal_order_gmv

        ,nvl(refund_online_phone_cnt,0) as refund_online_phone_cnt
        ,nvl(refund_offline_phone_cnt,0) as refund_offline_phone_cnt
        ,nvl(refund_online_unphone_cnt,0) as refund_online_unphone_cnt
        ,nvl(refund_offline_unphone_cnt,0) as refund_offline_unphone_cnt
        ,nvl(refund_unself_deal_order_cnt,0) as refund_unself_deal_order_cnt
        ,nvl(refund_online_phone_gmv,0) as refund_online_phone_gmv
        ,nvl(refund_offline_phone_gmv,0) as refund_offline_phone_gmv
        ,nvl(refund_online_unphone_gmv,0) as refund_online_unphone_gmv
        ,nvl(refund_offline_unphone_gmv,0) as refund_offline_unphone_gmv
        ,nvl(refund_unself_deal_order_gmv,0) as refund_unself_deal_order_gmv


from
    (
    select
    stat_date
        ,store_id
        ,sum(if(order_type='pay',total_real_price,0)) as rec_order_gmv
        ,sum(if(order_type='pay' and recycle_source='线上导流', total_real_price,0)) as rec_online_order_gmv
        ,sum(if(order_type='pay' and recycle_source='线下回收', total_real_price,0)) as rec_offline_order_gmv
        ,count(distinct if(order_type='pay',order_id,null)) as rec_order_cnt
        ,count(distinct if(order_type='pay' and recycle_source='线上导流', order_id,null)) as rec_online_order_cnt
        ,count(distinct if(order_type='pay' and recycle_source='线下回收', order_id,null)) as rec_offline_order_cnt
        ,sum(if(order_type='refund',total_real_price,0)) as refund_rec_order_gmv
        ,sum(if(order_type='refund' and recycle_source='线上导流', total_real_price,0)) as refund_rec_online_order_gmv
        ,sum(if(order_type='refund' and recycle_source='线下回收', total_real_price,0)) as refund_rec_offline_order_gmv
        ,count(distinct if(order_type='refund',order_id,null)) as refund_rec_order_cnt
        ,count(distinct if(order_type='refund' and recycle_source='线上导流', order_id,null)) as refund_rec_online_order_cnt
        ,count(distinct if(order_type='refund' and recycle_source='线下回收', order_id,null)) as refund_rec_offline_order_cnt



        ,count(distinct if(order_type='pay' and reservation_store_name not like '%测试%' and actual_source = '真实线上' and cate_name = '手机' and deal_type = '自营', order_id, null)) as online_phone_cnt
        ,count(distinct if(order_type='pay' and reservation_store_name not like '%测试%' and actual_source = '真实线下' and cate_name = '手机' and deal_type = '自营', order_id, null)) as offline_phone_cnt
        ,count(distinct if(order_type='pay' and reservation_store_name not like '%测试%' and actual_source = '真实线上' and cate_name = '非手机' and deal_type = '自营', order_id, null)) as online_unphone_cnt
        ,count(distinct if(order_type='pay' and reservation_store_name not like '%测试%' and actual_source = '真实线下' and cate_name = '非手机' and deal_type = '自营', order_id, null)) as offline_unphone_cnt
        ,count(distinct if(order_type='pay' and reservation_store_name not like '%测试%' and deal_type = '竞拍', order_id, null)) as unself_deal_order_cnt
        ,sum(if(order_type='pay' and reservation_store_name not like '%测试%' and actual_source = '真实线上' and cate_name = '手机' and deal_type = '自营', total_real_price, 0)) as online_phone_gmv
        ,sum(if(order_type='pay' and reservation_store_name not like '%测试%' and actual_source = '真实线下' and cate_name = '手机' and deal_type = '自营', total_real_price, 0)) as offline_phone_gmv
        ,sum(if(order_type='pay' and reservation_store_name not like '%测试%' and actual_source = '真实线上' and cate_name = '非手机' and deal_type = '自营', total_real_price, 0)) as online_unphone_gmv
        ,sum(if(order_type='pay' and reservation_store_name not like '%测试%' and actual_source = '真实线下' and cate_name = '非手机' and deal_type = '自营', total_real_price, 0)) as offline_unphone_gmv
        ,sum(if(order_type='pay' and reservation_store_name not like '%测试%' and deal_type = '竞拍', total_real_price, 0)) as unself_deal_order_gmv

        ,sum(if(order_type='pay' and reservation_store_name not like '%测试%' and deal_type != '竞拍',base_amt_ticket,0)) as base_amt_ticket
        ,sum(if(order_type='pay' and reservation_store_name not like '%测试%' and deal_type != '竞拍',rate_ticket_amt,0)) as rate_ticket_amt
        ,sum(if(order_type='pay' and reservation_store_name not like '%测试%' and deal_type != '竞拍',amt_ticket_amt,0)) as amt_ticket_amt
        ,sum(if(order_type='pay' and reservation_store_name not like '%测试%' and deal_type != '竞拍',emp_add_price,0)) as emp_add_price
        ,sum(if(order_type='pay' and reservation_store_name not like '%测试%' and deal_type != '竞拍',apply_markup_amount,0)) as apply_markup_amount
        ,sum(if(order_type='pay' and reservation_store_name not like '%测试%' and deal_type != '竞拍' and deal_type = '自营', total_real_price, 0)) as self_deal_order_gmv

        ,count(distinct if(order_type='refund' and reservation_store_name not like '%测试%' and actual_source = '真实线上' and cate_name = '手机' and deal_type = '自营', order_id, null)) as refund_online_phone_cnt
        ,count(distinct if(order_type='refund' and reservation_store_name not like '%测试%' and actual_source = '真实线下' and cate_name = '手机' and deal_type = '自营', order_id, null)) as refund_offline_phone_cnt
        ,count(distinct if(order_type='refund' and reservation_store_name not like '%测试%' and actual_source = '真实线上' and cate_name = '非手机' and deal_type = '自营', order_id, null)) as refund_online_unphone_cnt
        ,count(distinct if(order_type='refund' and reservation_store_name not like '%测试%' and actual_source = '真实线下' and cate_name = '非手机' and deal_type = '自营', order_id, null)) as refund_offline_unphone_cnt
        ,count(distinct if(order_type='refund' and reservation_store_name not like '%测试%' and deal_type = '竞拍', order_id, null)) as refund_unself_deal_order_cnt
        ,sum(if(order_type='refund' and reservation_store_name not like '%测试%' and actual_source = '真实线上' and cate_name = '手机' and deal_type = '自营', total_real_price, 0)) as refund_online_phone_gmv
        ,sum(if(order_type='refund' and reservation_store_name not like '%测试%' and actual_source = '真实线下' and cate_name = '手机' and deal_type = '自营', total_real_price, 0)) as refund_offline_phone_gmv
        ,sum(if(order_type='refund' and reservation_store_name not like '%测试%' and actual_source = '真实线上' and cate_name = '非手机' and deal_type = '自营', total_real_price, 0)) as refund_online_unphone_gmv
        ,sum(if(order_type='refund' and reservation_store_name not like '%测试%' and actual_source = '真实线下' and cate_name = '非手机' and deal_type = '自营', total_real_price, 0)) as refund_offline_unphone_gmv
        ,sum(if(order_type='refund' and reservation_store_name not like '%测试%' and deal_type = '竞拍', total_real_price, 0)) as refund_unself_deal_order_gmv


    from
    t_recycle
    group by
    stat_date,
    store_id
    ) t1
    ),

    ts_gmv as (
select
    to_date(outbound_time) as stat_date
        ,store_id
        ,count(qc_code) as ts_sale_cnt
        ,sum(deal_price/100) as ts_sale_gmv
from
    hdp_zhuanzhuan_dw_global.dw_trade_retail_offline_data_full_1d
where  dt = '${outFileSuffix}'
  and to_date(outbound_time) between '2023-01-01' and '${outFileSuffix}'
  and store_name not like '%测试%'
  and order_type = 3
  and cate_name in ("游戏卡带","平板电脑","手机","电纸书","游戏机","智能手表","耳机/耳麦","笔记本","单电/微单机身","单反机身","手写笔","单电/微单机身","智能手环")

group by
    to_date(outbound_time)
        ,store_id
    ),

    ts_refund as (
select
    to_date(happen_time) as stat_date
        ,a.store_id
        ,count(parent_order_id) as ts_refund_cnt
        ,sum(compensation_amt/100) as ts_refund_gmv
from
    hdp_ubu_zhuanzhuan_dw_c2b.dw_service_afs_offline_data_full_1d a
where a.dt = '${outFileSuffix}'
  and to_date(a.happen_time) between '2023-01-01' and '${outFileSuffix}'
  and order_type = '同售订单'
  and cate_name in ("游戏卡带","平板电脑","手机","电纸书","游戏机","智能手表","耳机/耳麦","笔记本","单电/微单机身","单反机身","手写笔","单电/微单机身","智能手环")

group by
    to_date(happen_time)
        ,a.store_id
    ),


    t_sale_net as(
-- 零售业绩，只看门店订单，不看直播店
select
    stat_date,
    store_id,
    nvl(sale_gmv,0)-nvl(refund_gmv,0) as sale_net_gmv,
    nvl(sale_cnt,0)-nvl(refund_cnt,0) as sale_net_cnt,
    nvl(sale_cnt,0) as sale_cnt,
    ls_gross_up,
    ls_gross_down,
    ls_gross_up_v2,
    ls_gross_up_v3,
    sale_cnt_old_change,
    rpc1
from
    (
    select
    stat_date,
    store_id,
    sum(if(type in (1,2) and order_type = 'pay',1,0)) as sale_cnt,
    count(distinct if(type in (1,2) and order_type = 'pay' and non_coupon_recycle_price > 0,order_id,null)) as sale_cnt_old_change,
    sum(if(type in (1,2) and order_type = 'pay', deal_price/100,0.0)) sale_gmv,
    sum(if(type in (1,2) and order_type = 'refund',1,0)) as refund_cnt,
    sum(if(type in (1,2) and order_type = 'refund', deal_price/100,0.0)) as refund_gmv,
    sum(if(type in (1,2) and order_type = 'pay',diff_price,0)) as ls_gross_up, -- 零售毛利率分子
    sum(if(type in (1,2) and order_type = 'pay',diff_price_v2,0)) as ls_gross_up_v2, -- 零售毛利率分子 v2口径
    sum(if(type in (1,2) and order_type = 'pay',diff_price_v3,0)) as ls_gross_up_v3, -- 零售毛利率分子 v3口径
    sum(if(type in (1,2) and order_type = 'pay',deal_price/100,0)) as ls_gross_down, -- 零售毛利率分母
    max(rpc1) as rpc1
    from
    t_ls
    group by
    stat_date,
    store_id
    ) t1


    ),

    t_gross as(
-- 回收销售毛利率
select
    stat_date,
    store_id,
    -- 毛利率
    round(SUM(sale_buyer_act_pay_amt/100-pur_seller_act_receipt_amt/100),2) as gross_up , -- 毛利率分子是差价c2实付-c1实得
    round(SUM(sale_buyer_act_pay_amt/100),2) as gross_down, -- 毛利率分母是c2实付
    -- 溢价率
    round(SUM(sale_buyer_act_pay_amt/100-pur_seller_act_receipt_amt/100),2)as premium_up, -- 溢价率分子是收入c2实付
    round(SUM(pur_seller_act_receipt_amt/100),2) as premium_down -- 溢价率分母是成本c1实得
from
    (
    select
    to_date(sale_pay_time) as stat_date,
    store_id,
    sale_buyer_act_pay_amt, -- C2实付
    pur_seller_act_receipt_amt -- C1实得
    from
    hdp_ubu_zhuanzhuan_dm_c2b.dm_recycle_offline_order_detail_full_1d
    where
    dt = '${outFileSuffix}'
    and store_name not like '%测试%'
    and to_date(sale_pay_time) between '2023-01-01' and '${outFileSuffix}'
    ) t
group by
    stat_date,
    store_id

    ),

    t_share as (

-- 回收分润
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
from
    (
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
    hdp_ubu_zhuanzhuan_dw_c2b.dw_trade_t_account_statement_recycle_detail_full_1d t2

    on t1.order_id=t2.recycle_order_id
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
from
    (
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

    on t1.order_id=t2.sales_order_id
    and t2.dt='${outFileSuffix}'
    ) t1
group by
    stat_date,
    store_id
    ),

    t_result as (
-- 汇总数据
select
    t1.stat_date
        ,t1.store_id
        ,nvl(sum(t2.rec_net_gmv),0) as rec_net_gmv
        ,nvl(sum(t2.rec_order_gmv),0) as rec_order_gmv
        ,nvl(sum(t2.online_rec_net_gmv),0) as online_rec_net_gmv
        ,nvl(sum(t2.offline_rec_net_gmv),0) as offline_rec_net_gmv
        ,nvl(sum(t2.rec_net_cnt),0) as rec_net_cnt
        ,nvl(sum(t2.rec_order_cnt),0) as rec_order_cnt
        ,nvl(sum(t2.sale_net_gmv),0) as sale_net_gmv
        ,nvl(sum(t2.sale_net_cnt),0) as sale_net_cnt
        ,nvl(max(t6.ts_sale_gmv),0) - nvl(max(t7.ts_refund_gmv),0) as ts_net_gmv
        ,nvl(max(t6.ts_sale_cnt),0) - nvl(max(t7.ts_refund_cnt),0) as ts_net_cnt
    -- ,nvl(sum(t2.sale_net_gmv),0) as ts_net_gmv
    -- ,nvl(sum(t2.sale_net_cnt),0) as ts_net_cnt
        ,nvl(sum(t2.sale_cnt),0) as sale_cnt
        ,nvl(sum(t2.sale_cnt_old_change),0) as sale_cnt_old_change
        ,round(nvl(sum(t2.sale_net_cnt),0) * nvl(sum(t2.sale_cnt_old_change),0) / nvl(sum(t2.sale_cnt),1),0) * 91 as sale_old_change_cost
        ,nvl(sum(t2.gross_up),0) as gross_up
        ,nvl(sum(t2.gross_down),0) as gross_down
        ,nvl(sum(t2.ls_gross_up),0) as ls_gross_up
        ,nvl(sum(t2.ls_gross_up_v2),0) as ls_gross_up_v2
        ,nvl(sum(t2.ls_gross_up_v3),0) as ls_gross_up_v3
        ,nvl(sum(t2.ls_gross_down),0) as ls_gross_down
        ,nvl(sum(t2.premium_up),0) as premium_up
        ,nvl(sum(t2.premium_down),0) as premium_down
        ,nvl(sum(t2.recycle_total_share_amt),0) as recycle_total_share_amt
        ,nvl(sum(t2.recycle_online_share_amt),0) as recycle_online_share_amt
        ,nvl(sum(t2.recycle_offline_share_amt),0) as recycle_offline_share_amt
        ,nvl(sum(t2.refund_total_share_amt),0) as refund_total_share_amt
        ,nvl(sum(t2.refund_online_share_amt),0) as refund_online_share_amt
        ,nvl(sum(t2.refund_offline_share_amt),0) as refund_offline_share_amt
        ,nvl(sum(t2.sale_jms_share_amt),0) as sale_jms_share_amt
        ,nvl(sum(t2.refund_jms_share_amt),0) as refund_jms_share_amt

        ,nvl(sum(online_phone_cnt),0) as online_phone_cnt
        ,nvl(sum(offline_phone_cnt),0) as offline_phone_cnt
        ,nvl(sum(online_unphone_cnt),0) as online_unphone_cnt
        ,nvl(sum(offline_unphone_cnt),0) as offline_unphone_cnt
        ,nvl(sum(unself_deal_order_cnt),0) as unself_deal_order_cnt
        ,nvl(sum(online_phone_gmv),0) as online_phone_gmv
        ,nvl(sum(offline_phone_gmv),0) as offline_phone_gmv
        ,nvl(sum(online_unphone_gmv),0) as online_unphone_gmv
        ,nvl(sum(offline_unphone_gmv),0) as offline_unphone_gmv
        ,nvl(sum(unself_deal_order_gmv),0) as unself_deal_order_gmv
        ,nvl(sum(base_amt_ticket),0) as base_amt_ticket
        ,nvl(sum(rate_ticket_amt),0) as rate_ticket_amt
        ,nvl(sum(amt_ticket_amt),0) as amt_ticket_amt
        ,nvl(sum(emp_add_price),0) as emp_add_price
        ,nvl(sum(apply_markup_amount),0) as apply_markup_amount
        ,nvl(sum(self_deal_order_gmv),0) as self_deal_order_gmv
        ,nvl(sum(refund_online_phone_cnt),0) as refund_online_phone_cnt
        ,nvl(sum(refund_offline_phone_cnt),0) as refund_offline_phone_cnt
        ,nvl(sum(refund_online_unphone_cnt),0) as refund_online_unphone_cnt
        ,nvl(sum(refund_offline_unphone_cnt),0) as refund_offline_unphone_cnt
        ,nvl(sum(refund_unself_deal_order_cnt),0) as refund_unself_deal_order_cnt
        ,nvl(sum(refund_online_phone_gmv),0) as refund_online_phone_gmv
        ,nvl(sum(refund_offline_phone_gmv),0) as refund_offline_phone_gmv
        ,nvl(sum(refund_online_unphone_gmv),0) as refund_online_unphone_gmv
        ,nvl(sum(refund_offline_unphone_gmv),0) as refund_offline_unphone_gmv
        ,nvl(sum(refund_unself_deal_order_gmv),0) as refund_unself_deal_order_gmv

        ,nvl(max(t2.rpc1),0) as rpc1

        ,nvl(max(t3.over7_punish_amt),0) as over7_punish_amt
        ,nvl(max(t4.over30_punish_amt),0) as over30_punish_amt

        ,nvl(max(store_eval_price),0) as store_eval_price
        ,nvl(max(judg_difference_amt),0) as judg_difference_amt



from
    total_left t1

    left join
    (
    select
    stat_date
        ,store_id
        ,rec_net_gmv
        ,rec_order_gmv
        ,online_rec_net_gmv
        ,offline_rec_net_gmv
        ,rec_net_cnt
        ,rec_order_cnt
        ,0 as sale_net_gmv
        ,0 as sale_net_cnt
        ,0 as sale_cnt
        ,0 as sale_cnt_old_change
        ,0 as gross_up
        ,0 as gross_down
        ,0 as ls_gross_up
        ,0 as ls_gross_up_v2
        ,0 as ls_gross_up_v3
        ,0 as ls_gross_down
        ,0 as premium_up
        ,0 as premium_down
        ,0 as recycle_total_share_amt
        ,0 as recycle_online_share_amt
        ,0 as recycle_offline_share_amt
        ,0 as refund_total_share_amt
        ,0 as refund_online_share_amt
        ,0 as refund_offline_share_amt
        ,0 as sale_jms_share_amt
        ,0 as refund_jms_share_amt

        ,online_phone_cnt
        ,offline_phone_cnt
        ,online_unphone_cnt
        ,offline_unphone_cnt
        ,unself_deal_order_cnt
        ,online_phone_gmv
        ,offline_phone_gmv
        ,online_unphone_gmv
        ,offline_unphone_gmv
        ,unself_deal_order_gmv
        ,base_amt_ticket
        ,rate_ticket_amt
        ,amt_ticket_amt
        ,emp_add_price
        ,apply_markup_amount
        ,self_deal_order_gmv
        ,refund_online_phone_cnt
        ,refund_offline_phone_cnt
        ,refund_online_unphone_cnt
        ,refund_offline_unphone_cnt
        ,refund_unself_deal_order_cnt
        ,refund_online_phone_gmv
        ,refund_offline_phone_gmv
        ,refund_online_unphone_gmv
        ,refund_offline_unphone_gmv
        ,refund_unself_deal_order_gmv
        ,0 as rpc1

    from
    t_recycle_net


    union all

    select
    stat_date
        ,store_id
        ,0 as rec_net_gmv
        ,0 as rec_order_gmv
        ,0 as online_rec_net_gmv
        ,0 as offline_rec_net_gmv
        ,0 as rec_net_cnt
        ,0 as rec_order_cnt
        ,sale_net_gmv
        ,sale_net_cnt
        ,sale_cnt
        ,sale_cnt_old_change
        ,0 as gross_up
        ,0 as gross_down
        ,ls_gross_up
        ,ls_gross_up_v2
        ,ls_gross_up_v3
        ,ls_gross_down
        ,0 as premium_up
        ,0 as premium_down
        ,0 as recycle_total_share_amt
        ,0 as recycle_online_share_amt
        ,0 as recycle_offline_share_amt
        ,0 as refund_total_share_amt
        ,0 as refund_online_share_amt
        ,0 as refund_offline_share_amt
        ,0 as sale_jms_share_amt
        ,0 as refund_jms_share_amt

        ,0 as online_phone_cnt
        ,0 as offline_phone_cnt
        ,0 as online_unphone_cnt
        ,0 as offline_unphone_cnt
        ,0 as unself_deal_order_cnt
        ,0 as online_phone_gmv
        ,0 as offline_phone_gmv
        ,0 as online_unphone_gmv
        ,0 as offline_unphone_gmv
        ,0 as unself_deal_order_gmv
        ,0 as base_amt_ticket
        ,0 as rate_ticket_amt
        ,0 as amt_ticket_amt
        ,0 as emp_add_price
        ,0 as apply_markup_amount
        ,0 as self_deal_order_gmv
        ,0 as refund_online_phone_cnt
        ,0 as refund_offline_phone_cnt
        ,0 as refund_online_unphone_cnt
        ,0 as refund_offline_unphone_cnt
        ,0 as refund_unself_deal_order_cnt
        ,0 as refund_online_phone_gmv
        ,0 as refund_offline_phone_gmv
        ,0 as refund_online_unphone_gmv
        ,0 as refund_offline_unphone_gmv
        ,0 as refund_unself_deal_order_gmv
        ,rpc1

    from
    t_sale_net

    union all

    select
    stat_date
        ,store_id
        ,0 as rec_net_gmv
        ,0 as rec_order_gmv
        ,0 as online_rec_net_gmv
        ,0 as offline_rec_net_gmv
        ,0 as rec_net_cnt
        ,0 as rec_order_cnt
        ,0 as sale_net_gmv
        ,0 as sale_net_cnt
        ,0 as sale_cnt
        ,0 as sale_cnt_old_change
        ,gross_up
        ,gross_down
        ,0 as ls_gross_up
        ,0 as ls_gross_up_v2
        ,0 as ls_gross_up_v3
        ,0 as ls_gross_down
        ,premium_up
        ,premium_down
        ,0 as recycle_total_share_amt
        ,0 as recycle_online_share_amt
        ,0 as recycle_offline_share_amt
        ,0 as refund_total_share_amt
        ,0 as refund_online_share_amt
        ,0 as refund_offline_share_amt
        ,0 as sale_jms_share_amt
        ,0 as refund_jms_share_amt

        ,0 as online_phone_cnt
        ,0 as offline_phone_cnt
        ,0 as online_unphone_cnt
        ,0 as offline_unphone_cnt
        ,0 as unself_deal_order_cnt
        ,0 as online_phone_gmv
        ,0 as offline_phone_gmv
        ,0 as online_unphone_gmv
        ,0 as offline_unphone_gmv
        ,0 as unself_deal_order_gmv
        ,0 as base_amt_ticket
        ,0 as rate_ticket_amt
        ,0 as amt_ticket_amt
        ,0 as emp_add_price
        ,0 as apply_markup_amount
        ,0 as self_deal_order_gmv
        ,0 as refund_online_phone_cnt
        ,0 as refund_offline_phone_cnt
        ,0 as refund_online_unphone_cnt
        ,0 as refund_offline_unphone_cnt
        ,0 as refund_unself_deal_order_cnt
        ,0 as refund_online_phone_gmv
        ,0 as refund_offline_phone_gmv
        ,0 as refund_online_unphone_gmv
        ,0 as refund_offline_unphone_gmv
        ,0 as refund_unself_deal_order_gmv
        ,0 as rpc1

    from
    t_gross

    union all

    select
    stat_date
        ,store_id
        ,0 as rec_net_gmv
        ,0 as rec_order_gmv
        ,0 as online_rec_net_gmv
        ,0 as offline_rec_net_gmv
        ,0 as rec_net_cnt
        ,0 as rec_order_cnt
        ,0 as sale_net_gmv
        ,0 as sale_net_cnt
        ,0 as sale_cnt
        ,0 as sale_cnt_old_change
        ,0 as gross_up
        ,0 as gross_down
        ,0 as ls_gross_up
        ,0 as ls_gross_up_v2
        ,0 as ls_gross_up_v3
        ,0 as ls_gross_down
        ,0 as premium_up
        ,0 as premium_down
        ,recycle_total_share_amt
        ,recycle_online_share_amt
        ,recycle_offline_share_amt
        ,refund_total_share_amt
        ,refund_online_share_amt
        ,refund_offline_share_amt
        ,sale_jms_share_amt
        ,refund_jms_share_amt

        ,0 as online_phone_cnt
        ,0 as offline_phone_cnt
        ,0 as online_unphone_cnt
        ,0 as offline_unphone_cnt
        ,0 as unself_deal_order_cnt
        ,0 as online_phone_gmv
        ,0 as offline_phone_gmv
        ,0 as online_unphone_gmv
        ,0 as offline_unphone_gmv
        ,0 as unself_deal_order_gmv
        ,0 as base_amt_ticket
        ,0 as rate_ticket_amt
        ,0 as amt_ticket_amt
        ,0 as emp_add_price
        ,0 as apply_markup_amount
        ,0 as self_deal_order_gmv
        ,0 as refund_online_phone_cnt
        ,0 as refund_offline_phone_cnt
        ,0 as refund_online_unphone_cnt
        ,0 as refund_offline_unphone_cnt
        ,0 as refund_unself_deal_order_cnt
        ,0 as refund_online_phone_gmv
        ,0 as refund_offline_phone_gmv
        ,0 as refund_online_unphone_gmv
        ,0 as refund_offline_unphone_gmv
        ,0 as refund_unself_deal_order_gmv
        ,0 as rpc1
    from
    t_share
    ) t2
on t1.stat_date = t2.stat_date
    and t1.store_id = t2.store_id

    left join
    hdp_ubu_zhuanzhuan_tmp_c2b.tmp_over7_punish_amt t3
    on t1.stat_date = t3.stat_date
    and t1.store_id = t3.store_id

    left join
    hdp_ubu_zhuanzhuan_tmp_c2b.tmp_over30_punish_amt t4
    on t1.stat_date = t4.stat_date
    and t1.store_id = t4.store_id

    left join
    judge t5
    on t1.stat_date = t5.stat_date
    and t1.store_id = t5.store_id

    left join
    ts_gmv t6
    on t1.stat_date = t6.stat_date
    and t1.store_id = t6.store_id

    left join
    ts_refund t7
    on t1.stat_date = t7.stat_date
    and t1.store_id = t7.store_id

group by
    t1.stat_date,
    t1.store_id
    )



insert overwrite table hdp_ubu_zhuanzhuan_dm_c2b.dm_offline_store_income_data_full_1d partition(dt='${outFileSuffix}')

-- create table hdp_ubu_zhuanzhuan_tmp_c2b.test_dm_offline_store_income_data_full_1d as


select
    stat_date
     ,store_id
     ,nvl(rec_net_gmv,0) as rec_net_gmv
     ,nvl(rec_net_cnt,0) as rec_net_cnt
     ,nvl(rec_order_cnt,0) as rec_order_cnt
     ,nvl(online_rec_net_gmv,0) as online_rec_net_gmv
     ,nvl(offline_rec_net_gmv,0) as offline_rec_net_gmv
     ,nvl(sale_net_gmv,0) as sale_net_gmv
     ,nvl(sale_net_cnt,0) as sale_net_cnt
     ,nvl(sale_cnt,0) as sale_cnt
     ,nvl(gross_up,0) as gross_up
     ,nvl(gross_down,0) as gross_down
     ,nvl(premium_up,0) as premium_up
     ,nvl(premium_down,0) as premium_down
     ,nvl(recycle_total_share_amt,0) as recycle_total_share_amt
     ,nvl(recycle_online_share_amt,0) as recycle_online_share_amt
     ,nvl(recycle_offline_share_amt,0) as recycle_offline_share_amt
     ,nvl(refund_total_share_amt,0) as refund_total_share_amt
     ,nvl(refund_online_share_amt,0) as refund_online_share_amt
     ,nvl(refund_offline_share_amt,0) as refund_offline_share_amt
     ,nvl(sale_jms_share_amt,0) as sale_jms_share_amt
     ,nvl(refund_jms_share_amt,0) as refund_jms_share_amt
     ,nvl(ls_gross_up,0) as ls_gross_up
     ,nvl(ls_gross_down,0) as ls_gross_down
     ,nvl(ls_gross_up_v2,0) as ls_gross_up_v2
     ,nvl(ls_gross_up_v3,0) as ls_gross_up_v3
     ,nvl(sale_cnt_old_change,0) as sale_cnt_old_change
--  ,nvl(sale_old_change_cost,0) as sale_old_change_cost

     ,nvl(base_amt_ticket,0) as base_amt_ticket
     ,nvl(rate_ticket_amt,0) as rate_ticket_amt
     ,nvl(amt_ticket_amt,0) as amt_ticket_amt
     ,nvl(emp_add_price,0) as emp_add_price
     ,nvl(apply_markup_amount,0) as apply_markup_amount
     ,nvl(ts_net_gmv,0) as ts_net_gmv
     ,nvl(ts_net_cnt,0) as ts_net_cnt
     ,nvl(rec_net_gmv,0) + nvl(sale_net_gmv,0) as store_net_gmv
     ,nvl(rec_net_cnt,0) + nvl(sale_net_cnt,0) as store_net_cnt
     ,nvl(base_amt_ticket,0) + nvl(rate_ticket_amt,0) + nvl(amt_ticket_amt,0) as coupon_total_amt
     ,nvl(over7_punish_amt,0) as over7_punish_amt
     ,nvl(over30_punish_amt,0) as over30_punish_amt

     ,nvl(store_eval_price,0) as store_eval_price
     ,nvl(judg_difference_amt,0) as judg_difference_amt

     ,nvl(online_phone_gmv,0) as online_phone_gmv
     ,nvl(refund_online_phone_gmv,0) as refund_online_phone_gmv
     ,nvl(offline_phone_gmv,0) as offline_phone_gmv
     ,nvl(refund_offline_phone_gmv,0) as refund_offline_phone_gmv
     ,nvl(online_unphone_gmv,0) as online_unphone_gmv
     ,nvl(refund_online_unphone_gmv,0) as refund_online_unphone_gmv
     ,nvl(offline_unphone_gmv,0) as offline_unphone_gmv
     ,nvl(refund_offline_unphone_gmv,0) as refund_offline_unphone_gmv
     ,nvl(unself_deal_order_gmv,0) as unself_deal_order_gmv
     ,nvl(refund_unself_deal_order_gmv,0) as refund_unself_deal_order_gmv


     ,nvl(online_phone_gmv,0) - nvl(refund_online_phone_gmv,0) as online_phone_net_gmv
     ,nvl(offline_phone_gmv,0) - nvl(refund_offline_phone_gmv,0) as offline_phone_net_gmv
     ,nvl(online_unphone_gmv,0) - nvl(refund_online_unphone_gmv,0) as online_unphone_net_gmv
     ,nvl(offline_unphone_gmv,0) - nvl(refund_offline_unphone_gmv,0) as offline_unphone_net_gmv
     ,nvl(unself_deal_order_gmv,0) - nvl(refund_unself_deal_order_gmv,0) as unself_deal_order_net_gmv

     ,nvl(emp_add_price,0) + nvl(apply_markup_amount,0) as add_price_amt
     ,nvl(rec_net_cnt,0) * 5 as rec_wl_cost
     ,nvl(sale_net_cnt,0) * 5 as sale_wl_cost
     ,nvl(sale_net_cnt,0) * 8.1 as sale_pay_cost
     ,nvl(sale_net_cnt,0) * 2.3 as sale_aftersale_cost
     ,nvl(sale_net_cnt,0) * 2 as sale_wrap_cost
     ,nvl(sale_net_cnt,0) * 0.8 as sale_peifu_cost
     ,nvl(ts_net_cnt,0) * 5 as ts_wl_cost
     ,nvl(ts_net_gmv,0) * 5/100 as ts_commission_cost


     ,nvl(online_phone_cnt,0) as online_phone_cnt
     ,nvl(refund_online_phone_cnt,0) as refund_online_phone_cnt
     ,nvl(offline_phone_cnt,0) as offline_phone_cnt
     ,nvl(refund_offline_phone_cnt,0) as refund_offline_phone_cnt
     ,nvl(online_unphone_cnt,0) as online_unphone_cnt
     ,nvl(refund_online_unphone_cnt,0) as refund_online_unphone_cnt
     ,nvl(offline_unphone_cnt,0) as offline_unphone_cnt
     ,nvl(refund_offline_unphone_cnt,0) as refund_offline_unphone_cnt
     ,nvl(unself_deal_order_cnt,0) as unself_deal_order_cnt
     ,nvl(refund_unself_deal_order_cnt,0) as refund_unself_deal_order_cnt

     ,nvl(online_phone_cnt,0)-nvl(refund_online_phone_cnt,0) as online_phone_net_cnt
     ,nvl(offline_phone_cnt,0)-nvl(refund_offline_phone_cnt,0) as offline_phone_net_cnt
     ,nvl(online_unphone_cnt,0)-nvl(refund_online_unphone_cnt,0) as online_unphone_net_cnt
     ,nvl(offline_unphone_cnt,0)-nvl(refund_offline_unphone_cnt,0) as offline_unphone_net_cnt
     ,nvl(unself_deal_order_cnt,0)-nvl(refund_unself_deal_order_cnt,0) as unself_deal_order_net_cnt

     ,(nvl(online_phone_gmv,0)-nvl(refund_online_phone_gmv,0))*7/100
    +(nvl(offline_phone_gmv,0)-nvl(refund_offline_phone_gmv,0))*10/100
    +(nvl(online_unphone_gmv,0)-nvl(refund_online_unphone_gmv,0))*11/100
    +(nvl(offline_unphone_gmv,0)-nvl(refund_offline_unphone_gmv,0))*14/100
    +(nvl(unself_deal_order_gmv,0) + nvl(refund_unself_deal_order_gmv,0))*11/100 as rec_basic_income
     ,rec_order_gmv
from
    t_result






