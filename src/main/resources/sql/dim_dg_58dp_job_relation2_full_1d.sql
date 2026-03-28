set start_date=2023-01-01; -- 起始日期

-- 当月所有在营的门店的信息和天数信息放在一起，作为左表，其他所有数据都要和这个左表关联，保证维度一致
insert overwrite table hdp_ubu_zhuanzhuan_dm_c2b.dm_offline_store_action_data_full_1d partition(dt='${outFileSuffix}')
select
    distinct
    month
        ,stat_date
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
        ,nvl(franchisee_name,'无') as franchisee_name
        ,days_in_month
        ,this_month_finish
        ,is_this_month_open
        ,need_qianfan_date
        ,need_data_from_qinfan
        ,max(convert_num) over(partition by month,store_id) as convert_num
        ,gold_open_flag
        ,luxury_open_flag
        ,luxury_store_level
        ,line_level
        ,store_area
        ,store_position_type
        ,gold_open_date
        ,luxury_open_date
        ,is_pro_store
from
    (

    select
    month
        ,t3.stat_date
        ,t3.is_pro_store
        ,day(last_day(t3.stat_date)) as days_in_month
        ,date_add(last_day(t3.stat_date),8) as need_qianfan_date
        ,t3.store_id
        ,t3.store_name
        ,t3.area
        ,t3.province
        ,t3.city_name
        ,t3.group_name
        ,t3.store_type_name
        ,t3.open_time as store_open_date
        ,t3.close_time as store_close_date
        ,t3.franchisee_id
        ,t3.franchisee_name
        ,t3.gold_open_flag
        ,t3.luxury_open_flag
        ,t3.luxury_store_level
        ,t3.line_level
        ,t3.store_area
        ,t3.store_position_type
        ,t3.gold_open_date
        ,t3.luxury_open_date
        ,case when month<substr('${outFileSuffix}',1,7) then 1
    else 0
    end as this_month_finish
        ,case when date_add(last_day(t3.stat_date),8) <=substr('${outFileSuffix}',1,10) then 1
    else 0
    end as need_data_from_qinfan  -- 如果已经过了这个日期则使用结算单的数据否则使用订单表的数据
        ,case when substr(t3.open_time,1,7)<substr(t3.stat_date,1,7) then 0
    else 1
    end as is_this_month_open
        ,case when month=substr('${outFileSuffix}',1,7) and substr(t3.open_time,1,7)=substr(t3.stat_date,1,7) then datediff('${outFileSuffix}',t3.stat_date)+1
    when month=substr('${outFileSuffix}',1,7) and substr(t3.open_time,1,7)<substr(t3.stat_date,1,7) then day('${outFileSuffix}')
    when month<substr('${outFileSuffix}',1,7) and substr(t3.open_time,1,7)=substr(t3.stat_date,1,7) then datediff(last_day(t3.stat_date),t3.open_time)+1
    when month<substr('${outFileSuffix}',1,7) and substr(t3.open_time,1,7)<substr(t3.stat_date,1,7) then day(last_day(t3.stat_date))
    end as convert_num
    from
    (
    select
    distinct
    substr(t2.day,1,7) as month
        ,t2.day as stat_date
        ,t1.area
        ,t1.province
        ,t1.city_name
        ,t1.group_name
        ,t1.store_id
        ,t1.store_name
        ,t1.store_type_name
        ,t1.franchisee_id
        ,t1.franchisee_name
    -- ,t1.open_time
        ,to_date(least(open_time,first_recycle_time,first_sale_time)) as open_time
        ,t1.close_time
        ,case when to_date(close_time) is null and to_date(least(open_time,first_recycle_time,first_sale_time)) <= t2.day then 1
    when to_date(least(open_time,first_recycle_time,first_sale_time)) <= t2.day and  to_date(close_time) > t2.day then 1
    else 0 end as open_flag
    -- ,case when to_date(close_time) is null and to_date(open_time) <= t2.day then 1
    --       when to_date(open_time) <= t2.day and  to_date(close_time) > t2.day then 1
    --       else 0 end as open_flag
        ,case when to_date(gold_end_date) is null and to_date(gold_start_date) <= t2.day then 1
    when to_date(gold_start_date) <= t2.day and  to_date(gold_end_date) >= t2.day then 1
    else 0 end as gold_open_flag
        ,case when to_date(luxury_end_date) is null and to_date(luxury_start_date) <= t2.day then 1
    when to_date(luxury_start_date) <= t2.day and  to_date(luxury_end_date) >= t2.day then 1
    else 0 end as luxury_open_flag
        ,luxury_store_level
        ,line_level
        ,store_area
        ,store_position_type
        ,to_date(gold_start_date) as gold_open_date
        ,to_date(luxury_start_date) as luxury_open_date
        ,is_pro_store
    from
    -- hdp_ubu_zhuanzhuan_tmp_c2b.tmp_new_dim_offline_store_detail_full_1d t1
    hdp_ubu_zhuanzhuan_dim_c2b.dim_offline_store_info_full_1d t1

    cross join
    hdp_ubu_zhuanzhuan_dim_c2b.dim_trade_store_date_0p t2

    where t2.day between '2023-01-01' and '${outFileSuffix}'
    and t1.dt = '${outFileSuffix}'
    and store_name not like '%测试%'
    and store_name not like '%直播%'
    and t1.store_name not in (
    '1111',
    '1111133595357918592门店',
    '1111134197701896704',
    '1111166324766703616门店',
    '北京地区上门回收专用门店'
    )
    ) t3


    left join
    hdp_ubu_zhuanzhuan_dw_c2b.dw_offline_store_detail_full_1d t4
    on t3.store_id=t4.store_id
    and t4.dt='${outFileSuffix}'-- 取这些门店最新的信息

    where open_flag = 1
    and substr(t4.first_opening_time,1,10) <='${outFileSuffix}'
    ) t5
where convert_num is not null and convert_num>0





