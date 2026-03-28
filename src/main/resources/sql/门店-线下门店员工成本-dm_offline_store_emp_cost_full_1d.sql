-- set start_date=2023-01-01; -- 起始日期

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
    ),


    sale_rpc as ( -- 零售rpc，用每个月的大盘值套给每个店
select
    price_month,
    rpc1,
    round(if(rpc1<1,1,rpc1)-1,4) as rpc_use -- 减掉1之后的部分才是要用的，还要四舍五入一下
from
    (
    select
    substr(statistics_date,1,7) as price_month,
    sum(czc_mn_gmv) as czc_mn_gmv,
    sum(store_gmv_n) as store_gmv_n,
    sum(store_gmv_n)/sum(czc_mn_gmv) as rpc1
    from
    hdp_ubu_zhuanzhuan_tmp_c2b.retail_price_table2 -- 直接用目前RPC看板的结果表
    where dt='${outFileSuffix}'
    and substr(statistics_date,1,7) >='2023-01'
    -- between '${start_date}' and '${end_date}'-- 这个之后要改成2301开始
    and store_kdj>0
    and czc_kdj>0
    group by substr(statistics_date,1,7)
    ) a
    ),



    raw_rec as (
select
    distinct
    seller_id,
    order_id,
    cate_name,
    deliver_employee_id as employee_uid,
    nvl(total_real_price/100,0) as total_real_price,
    nvl(apply_coupon_amt/100,0) as apply_coupon_amt,
    nvl(coupon_content_amt/100,0) as coupon_content_amt,
    nvl(apply_markup_amount/100,0) as apply_markup_amount,
    round(non_coupon_recycle_price/100,2) as rec_yuanjia,
    -- nvl(total_real_price/100,0)-nvl(apply_coupon_amt/100,0)-nvl(coupon_content_amt/100,0)-nvl(apply_markup_amount/100,0) as rec_yuanjia, -- 这里需要改成新的绩效核算金额的口径
    to_date(pay_time) as stat_date, --回收成交日期,
    store_id,
    recycle_source
from
    hdp_ubu_zhuanzhuan_dm_c2b.dm_recycle_offline_order_detail_full_1d a
where dt = '${outFileSuffix}'
  and store_name not like '%测试%'
  and store_type_name not like '测试'
-- and to_date(pay_time) between '${start_date}' and '${end_date}'
-- and to_date(pay_time)>='2023-01-01' -- 实际上需要23年开始的数据
    ),


-- 回收退回
    raw_rec_refund as (
select
    distinct
    t0.order_id,
    cate_name,
    to_date(t0.refund_create_time) AS stat_date,
    deliver_employee_id as employee_uid,
    t0.total_real_price / 100 as total_real_price,
    store_id,
    (nvl(base_coupon_amt,0)+nvl(coupon_content_amt,0)+nvl(apply_coupon_amt,0))/100 as other_amt,
    recycle_source,
    round(non_coupon_recycle_price/100,2) as rec_yuanjia
-- ,nvl(total_real_price/100,0)-nvl(apply_coupon_amt/100,0)-nvl(coupon_content_amt/100,0)-nvl(apply_markup_amount/100,0) as rec_yuanjia
from
    hdp_ubu_zhuanzhuan_dm_c2b.dm_recycle_offline_order_detail_full_1d as t0
where t0.dt = '${outFileSuffix}'
  and t0.store_name not like '%测试%'
  and refund_type in (1,2)
-- and to_date(t0.refund_create_time) between '${start_date}' and '${end_date}'
-- and t0.refund_create_time >='2023-01-01'
    ),


-- 零售业绩，只看门店订单，不看直播店
    raw_ls as (
select
    a.store_id,
    deal_price,up_price/100 as up_price,
    (deal_price-up_price)/100 as real_price,
    actual_cost_price/100 as actual_cost_price, -- 实际成本价
    (deal_price/100-actual_cost_price/100) as real_diff_price, -- 真实差价毛利额
    if((deal_price/100-actual_cost_price/100)>deal_price/100*(0.0565+rpc_use),deal_price/100*(0.0565+rpc_use),(deal_price/100-actual_cost_price/100)) as diff_price,
    case when a.cate_name  = '手机' and a.brand_name  = '苹果' then '苹果'
    when a.cate_name  = '手机' and a.brand_name  != '苹果' then '安卓'
    else '非手机'
    end as cate_type,
    substr(a.outbound_time,1,10) as stat_date,
    child_order_id as order_id,
    staff_id as employee_uid,
    case when a.purchase_channels=5 then 1 else 0 end as is_resource_machine,   --`是否资源机`,
    nvl(old_change_price,0) as old_change_price

from
    (

    select
    store_id
        ,deal_price
        ,up_price
        ,actual_cost_price
        ,cate_name
        ,brand_name
        ,outbound_time
        ,child_order_id
        ,staff_id
        ,purchase_channels
    from
    hdp_zhuanzhuan_dw_global.dw_trade_retail_offline_data_full_1d
    where dt='${outFileSuffix}'
    and store_name not like '%测试%'
    and substr(outbound_time,1,10)>='2023-01-01'
    and order_state in (20,50,60,70)
    and order_type in (1,2) -- order_type这两个的是门店订单
    and store_name not like '%直播%' -- 验数用的条件


    union all

    select
    store_id
        ,deal_price
        ,0 as up_price
        ,0 as actual_cost_price
        ,cate_name
        ,brand_name
        ,outbound_time
        ,child_order_id
        ,staff_id
        ,purchase_channels
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
    a.sales_order_id,
    b.order_id,
    a.origin_order_id,
    round(non_coupon_recycle_price/100,2) as old_change_price  --以旧换优回收核算绩效金额
    from
    hdp_ubu_zhuanzhuan_dw_c2b.dw_market_coupon_t_activity_coupon_relation_full_1d a

    left join
    hdp_ubu_zhuanzhuan_dm_c2b.dm_recycle_offline_order_detail_full_1d b
    on a.origin_order_id=b.order_id
    and b.dt='${outFileSuffix}'  -- 取对应回收单的信息
    where a.dt='${outFileSuffix}'
    and red_status=2 -- 红包已使用
    and scene_type in (0) -- 0 以旧换优
    ) coupon0

    on coupon0.sales_order_id=a.child_order_id

    ),



-- 零售退回
    raw_ls_refund as (
select
    store_id,
    deal_price,
    up_price/100 as up_price,
    (deal_price-up_price)/100 as real_price,
    case when cate_name  = '手机' and brand_name  = '苹果' then '苹果'
    when cate_name  = '手机' and brand_name  != '苹果' then '安卓'
    else '非手机'
    end as cate_type,
    substr(a.back_time,1,10) as stat_date,
    child_order_id as order_id,
    staff_id as employee_uid,
    nvl(old_change_price,0) as old_change_price
from
    (
    select
    store_id
        ,deal_price
        ,up_price
        ,cate_name
        ,brand_name
        ,back_time
        ,order_id
        ,child_order_id
        ,staff_id

    from
    hdp_zhuanzhuan_dw_global.dw_trade_retail_offline_data_full_1d
    where dt='${outFileSuffix}'
    and store_name not like '%测试%'
    and order_type in (1,2) -- order_type这两个的是门店订单
    and order_state in (20,50,60,70)

    union all

    select
    store_id
        ,deal_price
        ,0 as up_price
        ,cate_name
        ,brand_name
        ,afs_finish_time as back_time
        ,order_id
        ,child_order_id
        ,staff_id
    from
    hdp_ubu_zhuanzhuan_dw_c2b.dw_trade_sale_store_pro_retail_offline_data_full_1d
    where order_type in (1,2) -- 门店订单
    and purchase_channels in (1,3) -- 天路货源，首次销售+售后货源
    and state=20 -- 订单状态已完成
    and dt='${outFileSuffix}'
    and date(afs_finish_time) between '2021-01-01' and '${outFileSuffix}'

    ) a
    left join
    (
    -- 以旧换优
    select
    a.sales_order_id,b.order_id,round(total_real_price/100,2) as`以旧换优回收单C1实得`,a.origin_order_id,
    round(non_coupon_recycle_price/100,2) as old_change_price
    from
    hdp_ubu_zhuanzhuan_dw_c2b.dw_market_coupon_t_activity_coupon_relation_full_1d a

    left join
    hdp_ubu_zhuanzhuan_dm_c2b.dm_recycle_offline_order_detail_full_1d b on a.origin_order_id=b.order_id and b.dt='${outFileSuffix}'  -- 取对应回收单的信息
    where a.dt='${outFileSuffix}'
    and red_status=2 -- 红包已使用
    and scene_type in (0) -- 0 以旧换优
    ) coupon0
on coupon0.sales_order_id=a.order_id

    ),


-- 员工基本信息，精确到天，每天在哪个店，归属和角色，加盟店的自营店员提成承担方
    employee_info as (
select
    a.stat_date, -- 日期
    a.month, -- 月份
    a.store_id, -- 员工归属门店
    a.store_name,
    c.city, -- 门店所属城市
    a.store_type_name,
    b.uid as employee_uid, -- 员工uid，用于关联订单
    employee_name,
    case
    when employee_role in ('offline_store:r_store_manager','offline_store:r_operation_group_leader') then '店长'
    else '店员'
    end as role_type, -- 是店员还是店长
    case
    when personnel_ownership=1 then '自营'
    when personnel_ownership=2 then '加盟'
    when personnel_ownership=3 then '发条爱客'
    when personnel_ownership=4 then '非企业微信用户'
    else '未知'
    end as personnel_ownership, -- 身份归属
    case
    when a.store_type_name='自营' then '转'
    -- 首月两种情况
    when (YEAR(a.stat_date)-year(store_open_date))*12+month(a.stat_date)-month(store_open_date)=0 and fm_commission_commitment=1 then '商'
    when (YEAR(a.stat_date)-year(store_open_date))*12+month(a.stat_date)-month(store_open_date)=0 and fm_commission_commitment=2 then '转'
    -- 次月开始
    when (YEAR(a.stat_date)-year(store_open_date))*12+month(a.stat_date)-month(store_open_date)>0 and after_fm_commission_commitment=-1 then '转'
    when (YEAR(a.stat_date)-year(store_open_date))*12+month(a.stat_date)-month(store_open_date)>0 and after_fm_commission_commitment=0 then '商'
    when (YEAR(a.stat_date)-year(store_open_date))*12+month(a.stat_date)-month(store_open_date)<=after_fm_commission_commitment then '转'
    when (YEAR(a.stat_date)-year(store_open_date))*12+month(a.stat_date)-month(store_open_date)>after_fm_commission_commitment then '商'
    else '未知'
    end as cover_side -- 本月加盟店里的自营人员的提成是谁承担，基本工资不用看这个，基本工资+绩效是只看身份
from
    total_left a

    left join
    hdp_ubu_zhuanzhuan_dim_c2b.dim_offline_store_employee_full_1d b
on a.store_id=b.store_id
    and a.stat_date=b.dt
    -- and b.dt between '${start_date}' and '${end_date}'
    and b.uid is not null

    left join
    hdp_ubu_zhuanzhuan_dim_c2b.dim_offline_store_detail_full_1d_0p c
    on c.store_id=b.store_id

where 1=1
-- and b.dt between '${start_date}' and '${end_date}'
-- b.dt>='2023-01-01'
  and b.is_enabled=1 and b.state=1 -- 当天在职的
  and employee_role in (
    'offline_store:r_store_clerk',
    'offline_store:r_operation_group_leader',
    'offline_store:r_store_manager',
    'offline_store:r_reserve_store_manager') -- 只有这四种角色需要把成本算在门店上
  and c.store_name is not null
  and b.uid is not null
    ),



-- 1.基本工资+绩效
    employee_bcsalary_result as (

select
    month,
    store_id,
    -- 转转承担：不管什么店的自营人基本工资绩效
    nvl(sum(case when personnel_ownership='自营' then cost  else 0 end),0) as zz_bcsalary_cost,
    -- 加盟商承担：加盟店加盟人基本工资绩效
    nvl(sum(case when store_type_name='加盟' and personnel_ownership='加盟' then cost  else 0 end),0) as jms_bcsalary_cost,
    sum(in_store_days_pre_month) as person_cnt
from
    (
    select
    distinct
    a.month,
    a.store_id,
    a.store_type_name,
    b.employee_uid,
    b.personnel_ownership,
    in_store_days,
    (in_store_days/days_in_month) as in_store_days_pre_month,
    nvl(cost/days_in_month*in_store_days,0) as cost -- 这个月这个人在这个店总的基本工资+提成
    from
    total_left a

    left join
    ( -- 所有在职店员和归属信息
    select
    month,
    store_id,
    employee_uid,
    city,
    store_type_name,
    role_type,
    personnel_ownership,
    count(distinct stat_date) as in_store_days -- 这个人这个月在这个店待了多少天
    from
    employee_info a
    group by
    month,
    store_id,
    employee_uid,
    city,
    store_type_name,
    role_type,
    personnel_ownership
    ) b
    on a.month=b.month and a.store_id=b.store_id

    left join
    hdp_ubu_zhuanzhuan_defaultdb.tmp_store_employee_cost c
    on b.month=c.month
    and b.city=c.city
    and b.role_type=c.employee_type
    and b.store_type_name=c.store_type
    ) a
group by
    month,
    store_id
    ),


-- 2.提成
    raw_employee_com as ( -- 每天每人
-- 回收
select
    stat_date,
    employee_uid,
    sum(com1)-sum(com4)+sum(com2)-sum(com3) as tmp_com1, -- 回收和零售原价提成
    sum(up_com2)-sum(up_com3) as tmp_com2, -- 零售加价提成
    -- 回收多品类激励
    sum(com5) as com5,sum(com6) as com6,
    -- 零售以旧换优激励
    sum(com7) as com7,sum(com8) as com8,
    -- 下面三个指标排查用的，把几种不同提成拆开
    sum(com1)-sum(com4) as rec_com,
    sum(com2)-sum(com3) as sale_com,
    sum(up_com2)-sum(up_com3) as sale_up_com
from
    (
    select
    a.stat_date,
    a.employee_uid,
    order_id,
    case when stat_date<'2025-08-01' then
    round(IF(recycle_source like "%线下%",IF(rec_yuanjia<=40,1,IF(rec_yuanjia<=400,10,IF(rec_yuanjia<=8000,rec_yuanjia*0.025,200))),IF(rec_yuanjia<=40,1,IF(rec_yuanjia<=400,10,IF(rec_yuanjia<=8000,rec_yuanjia*0.015,120)))),3)
    -- 25年8月1号之后改上限和下限
    when rec_yuanjia<=40 then 1
    when recycle_source like "%线上%" then if(0.015*rec_yuanjia>60,60,if(0.015*rec_yuanjia<5,5,0.015*rec_yuanjia))
    when recycle_source like "%线下%" then if(0.025*rec_yuanjia>140,140,if(0.025*rec_yuanjia<10,10,0.025*rec_yuanjia))
    end as com1,
    0 as com2,
    0 as up_com2,
    0 as com3,
    0 as up_com3,
    0 as com4,
    -- 25年8月1日开始有回收多品类激励
    case when cate_name='耳机/耳麦' and stat_date>='2025-08-01' and rec_yuanjia>=40 then 5
    when cate_name='游戏卡带' and stat_date>='2025-08-01' and rec_yuanjia>=40 then 5
    when cate_name='手写笔' and stat_date>='2025-08-01' and rec_yuanjia>=40 then 5
    when cate_name='笔记本' and stat_date>='2025-08-01' and rec_yuanjia>=100 then 10
    when cate_name='智能手表' and stat_date>='2025-08-01' and rec_yuanjia>=100 then 5
    when cate_name='游戏机' and stat_date>='2025-08-01' and rec_yuanjia>=100 then 10
    when cate_name='单反机身' and stat_date>='2025-08-01' and rec_yuanjia>=100 then 10
    when cate_name='单电/微单机身' and stat_date>='2025-08-01' and rec_yuanjia>=100 then 10
    when cate_name='相机镜头' and stat_date>='2025-08-01' and rec_yuanjia>=100 then 10
    else 0
    end as com5, -- 回收多品类激励
    0 as com6, -- 回收多品类激励退货
    -- 25年8月1日开始有零售以旧换优激励
    0 as com7,
    0 as com8
    from
    raw_rec a

    union

    -- 零售未剔除退回
    select
    a.stat_date,
    a.employee_uid,
    order_id,
    0,
    case when stat_date<='2024-06-30' then round(if(real_price*0.015>110,110,real_price*0.015),4)
    -- 24年7月1日开始员工提成分安卓苹果非手机
    when cate_type='安卓' and stat_date<'2025-08-01' then round(if(real_price*0.020>150,150,real_price*0.020),4)
    when cate_type='苹果' and stat_date<'2025-08-01' then round(if(real_price*0.013>85,85,real_price*0.013),4)
    when cate_type='非手机' and stat_date<'2025-08-01' then round(if(real_price*0.015>110,110,real_price*0.015),4)
    -- 25念8月1日开始员工提成分客单价
    when cate_type='安卓' and real_price>=5000 then 50
    when cate_type='安卓' and real_price>=4000 then 50
    when cate_type='安卓' and real_price>=3000 then 50
    when cate_type='安卓' and real_price>=2000 then 60
    when cate_type='安卓' and real_price>=1000 then 60
    when cate_type='安卓' and real_price>=500 then 40
    when cate_type='安卓' and real_price>=0 then 20

    when cate_type='苹果' and real_price>=5000 then 30
    when cate_type='苹果' and real_price>=4000 then 40
    when cate_type='苹果' and real_price>=3000 then 40
    when cate_type='苹果' and real_price>=2000 then 60
    when cate_type='苹果' and real_price>=1000 then 60
    when cate_type='苹果' and real_price>=500 then 40
    when cate_type='苹果' and real_price>=0 then 20

    when cate_type='非手机' and real_price>=5000 then 50
    when cate_type='非手机' and real_price>=4000 then 50
    when cate_type='非手机' and real_price>=3000 then 50
    when cate_type='非手机' and real_price>=2000 then 50
    when cate_type='非手机' and real_price>=1000 then 50
    when cate_type='非手机' and real_price>=500 then 30
    when cate_type='非手机' and real_price>=0 then 30
    end as com2,
    up_price*0.5 as up_com2,
    0,
    0,
    0,
    0,
    0,
    case when old_change_price >=1000 and stat_date>='2025-08-01' then 10 else 0 end as com7, -- 以旧换优激励
    0 -- 以旧换优退回
    from
    raw_ls a
    where a.is_resource_machine=0

    union
    -- 零售退回
    select
    a.stat_date,
    a.employee_uid,
    order_id,
    0,
    0,
    0,
    case when stat_date<='2024-06-30' then round(if(real_price*0.015>110,110,real_price*0.015),4)
    -- 24年7月1日开始员工提成分安卓苹果非手机
    when cate_type='安卓' and stat_date<'2025-08-01' then round(if(real_price*0.020>150,150,real_price*0.020),4)
    when cate_type='苹果' and stat_date<'2025-08-01' then round(if(real_price*0.013>85,85,real_price*0.013),4)
    when cate_type='非手机' and stat_date<'2025-08-01' then round(if(real_price*0.015>110,110,real_price*0.015),4)
    -- 25念8月1日开始员工提成分客单价
    when cate_type='安卓' and real_price>=5000 then 50
    when cate_type='安卓' and real_price>=4000 then 50
    when cate_type='安卓' and real_price>=3000 then 50
    when cate_type='安卓' and real_price>=2000 then 60
    when cate_type='安卓' and real_price>=1000 then 60
    when cate_type='安卓' and real_price>=500 then 40
    when cate_type='安卓' and real_price>=0 then 20

    when cate_type='苹果' and real_price>=5000 then 30
    when cate_type='苹果' and real_price>=4000 then 40
    when cate_type='苹果' and real_price>=3000 then 40
    when cate_type='苹果' and real_price>=2000 then 60
    when cate_type='苹果' and real_price>=1000 then 60
    when cate_type='苹果' and real_price>=500 then 40
    when cate_type='苹果' and real_price>=0 then 20

    when cate_type='非手机' and real_price>=5000 then 50
    when cate_type='非手机' and real_price>=4000 then 50
    when cate_type='非手机' and real_price>=3000 then 50
    when cate_type='非手机' and real_price>=2000 then 50
    when cate_type='非手机' and real_price>=1000 then 50
    when cate_type='非手机' and real_price>=500 then 30
    when cate_type='非手机' and real_price>=0 then 30
    end as com3,
    up_price*0.5 as up_com3,
    0,
    0,
    0,
    0,
    case when old_change_price >=1000 and stat_date>='2025-08-01' then 10 else 0 end as com8 -- 以旧换优零售单退货后扣除激励
    from
    raw_ls_refund a

    union
    -- 回收退回
    select
    a.stat_date,
    a.employee_uid,
    order_id,
    0 as com1,
    0 as com2,
    0 as up_com2,
    0 as com3,
    0 as up_com3,
    case when stat_date<'2025-08-01' then
    round(IF(recycle_source like "%线下%",IF(rec_yuanjia<=40,1,IF(rec_yuanjia<=400,10,IF(rec_yuanjia<=8000,rec_yuanjia*0.025,200))),IF(rec_yuanjia<=40,1,IF(rec_yuanjia<=400,10,IF(rec_yuanjia<=8000,rec_yuanjia*0.015,120)))),3)
    -- 25年8月1号之后改上限和下限
    when rec_yuanjia<=40 then 1
    when recycle_source like "%线上%" then if(0.015*rec_yuanjia>60,60,if(0.015*rec_yuanjia<5,5,0.015*rec_yuanjia))
    when recycle_source like "%线下%" then if(0.025*rec_yuanjia>140,140,if(0.025*rec_yuanjia<10,10,0.025*rec_yuanjia))
    end as com4,
    0,
    case when cate_name='耳机/耳麦' and stat_date>='2025-08-01' and rec_yuanjia>=40 then 5
    when cate_name='游戏卡带' and stat_date>='2025-08-01' and rec_yuanjia>=40 then 5
    when cate_name='手写笔' and stat_date>='2025-08-01' and rec_yuanjia>=40 then 5
    when cate_name='笔记本' and stat_date>='2025-08-01' and rec_yuanjia>=100 then 10
    when cate_name='智能手表' and stat_date>='2025-08-01' and rec_yuanjia>=100 then 5
    when cate_name='游戏机' and stat_date>='2025-08-01' and rec_yuanjia>=100 then 10
    when cate_name='单反机身' and stat_date>='2025-08-01' and rec_yuanjia>=100 then 10
    when cate_name='单电/微单机身' and stat_date>='2025-08-01' and rec_yuanjia>=100 then 10
    when cate_name='相机镜头' and stat_date>='2025-08-01' and rec_yuanjia>=100 then 10
    else 0
    end as com6, -- 回收多品类激励退回
    0,
    0
    from
    raw_rec_refund a
    --
    ) a
group by
    stat_date,
    employee_uid
    ),

-- 员工提成
    employee_com_result as (

select
    distinct
    a.month,
    a.stat_date,
    a.store_id,
    a.store_type_name,
    a.role_type,
    a.employee_uid,
    a.personnel_ownership,
    a.cover_side,
    nvl(c.zz_bcsalary_cost,0) / count(*) over(partition by a.month,a.store_id) as zz_bcsalary_cost,
    nvl(c.jms_bcsalary_cost,0) / count(*) over(partition by a.month,a.store_id)  as jms_bcsalary_cost,
    nvl(b.rec_com,0) as rec_com,
    nvl(b.sale_com,0) as sale_com,
    nvl(b.sale_up_com,0) as sale_up_com,
    nvl(b.com5,0)-nvl(b.com6,0) as hs_multi_com,
    nvl(b.com7,0)-nvl(b.com8,0) as ls_change_com,
    nvl(c.person_cnt,0) as person_cnt
from
    employee_info a

    left join
    raw_employee_com b
on a.stat_date=b.stat_date
    and a.employee_uid=b.employee_uid

    left join
    employee_bcsalary_result c
    on a.month=c.month
    and a.store_id=c.store_id

where a.employee_uid is not null
    --and b.employee_uid is not null
    )



insert overwrite table hdp_ubu_zhuanzhuan_dm_c2b.dm_offline_store_emp_cost_full_1d partition(dt='${outFileSuffix}')
-- insert overwrite table hdp_ubu_zhuanzhuan_dm_c2b.dm_offline_store_emp_cost_full_1d partition(dt='test')
select
    month,
    stat_date,
    store_id,
    store_type_name,
    employee_uid,
    role_type,
    personnel_ownership,
    cover_side,
    nvl(zz_bcsalary_cost,0) as zz_bcsalary_cost,
    nvl(jms_bcsalary_cost,0) as jms_bcsalary_cost,
    0 as rec_order_cnt,
    nvl(rec_com,0) as rec_com,
    0 as rec_refund_com,
    0 as rec_yuanjia,
    0 as rec_jiajia,
    0 as sale_order_cnt,
    nvl(sale_com,0) as sale_com,
    0 as sale_refund_com,
    nvl(sale_up_com,0) as sale_up_com,
    0 as sale_up_refund_com,
    0 as sale_yuanjia,
    0 as sale_jiajia,
    nvl(hs_multi_com,0) as hs_multi_com,
    nvl(ls_change_com,0) as ls_change_com,
    person_cnt
from
    employee_com_result



