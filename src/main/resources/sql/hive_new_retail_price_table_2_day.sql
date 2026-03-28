WITH ydn_details AS (
    select   DISTINCT
        a.qc_code
                    ,nvl(get_json_object(a.report_result,'$.baseInfo.surfaceGoldLevelName'),'') as spec_quality_name   --新版本的成色
                    ,nvl(regexp_extract(get_json_object(a.report_result,'$.baseInfo.deviceInfos'), '\\{"itemId":"6652","itemName":"购买渠道",(.*?)"children":\\[\\{"itemId":"[0-9]+","itemName":"(.*?)"(.*?)\\}', 2),'') as `purchase_channel`  --购买渠道
                    ,nvl(regexp_extract(get_json_object(a.report_result,'$.baseInfo.deviceInfos'), '\\{"itemId":"7191","itemName":"网络制式",(.*?)"children":\\[\\{"itemId":"[0-9]+","itemName":"(.*?)"(.*?)\\}', 2),'') as `spec_net`         --网络制式
                    ,nvl(regexp_extract(get_json_object(a.report_result,'$.baseInfo.deviceInfos'), '\\{"itemId":"10491","itemName":"存储容量",(.*?)"children":\\[\\{"itemId":"[0-9]+","itemName":"(.*?)"(.*?)\\}', 2),'') as spec_ram_2        --运存容量
                    ,nvl(regexp_extract(get_json_object(a.report_result,'$.baseInfo.deviceInfos'), '\\{"itemId":"7000","itemName":"运行内存",(.*?)"children":\\[\\{"itemId":"[0-9]+","itemName":"(.*?)"(.*?)\\}', 2),'') as spec_ram_1        --运行内存
                    ,regexp_extract(get_json_object(a.report_result,'$.baseInfo.deviceInfos'), '\\{"itemId":"5587","itemName":"颜色",(.*?)"children":\\[\\{"itemId":"[0-9]+","itemName":"(.*?)"(.*?)\\}', 2) as `颜色`
    from hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_athena_qc_report_result_full_1d a
    where a.dt='${outFileSuffix}'
)
   ,czc as
    (
        select
            `支付日期`,`品类`,`品牌`,`机型`,`容量`
             ,`外观成色`,`功能成色`,`网络制式`,`细分等级`
             ,concat_ws('-',`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`) as sku
             ,count(order_id) as `czc销量`
             ,sum(`实际支付金额`) as `czc售出gmv`
        from
            (
                select
                    *
                from
                    (
                        select
                        --线上C卖场销售
                            a.order_id
                             ,'C卖场' as `卖场类型`
                             ,DATE(a.pay_time) as `支付日期`
                   ,case WHEN b.cate_first_id in(101,119) then b.cate_first_name
							   when b.cate_second_id=1100000665 then b.cate_second_name
							   else b.cate_third_name end as `品类`
				   ,b.brand_name as `品牌`
				   ,b.model_id as `机型id`
				   ,lower(b.model_name) as `机型`
				   ,a.total_amt/100  as `实际支付金额`
				   ,c.subdivision_grade as `细分等级`
				   ,b.spec_appearance_quality as `外观成色`
				   ,b.spec_function_quality as `功能成色`
				   ,e.purchase_channel as `购买渠道`
				   ,e.spec_net  as `网络制式`
				   ,case when spec_ram_1<>'' and spec_ram_2<>'' then concat_ws('+',e.spec_ram_1,e.spec_ram_2) when spec_ram_1='' then spec_ram_2 when spec_ram_2='' then spec_ram_1 end  as `容量`
				   ,b.spec_color as `颜色`
					,ROW_NUMBER() over(partition by b.qc_code order by c.create_time desc) as num
                from hdp_ubu_zhuanzhuan_dw_b2c.dw_trade_order_ord_all_subject_dtl_full_1d a   --B2C卖场订单表
                    left join hdp_zhuanzhuan_dw_global.dw_info_prod_detail_full_1d b on a.info_id =b.info_id and b.dt='${outFileSuffix}'  --B2C卖场商品表
                    left join
                    (
                    SELECT qc_code
                    ,create_time
                    ,subdivision_grade
                    ,surface_gold_level_name
                    from hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_bmskyway_t_skyway_product_full_1d --天路商品表取细分等级和成色
                    where dt='${outFileSuffix}'
                    ) c on b.qc_code=c.qc_code
                    left join
                    hdp_ubu_zhuanzhuan_ads_c2b.t_bi_czc_uid_full_1d d on a.seller_id=d.uid and d.dt='${outFileSuffix}'
                    LEFT JOIN ydn_details e on b.qc_code=e.qc_code
                where a.dt='${outFileSuffix}'
                  and if(a.is_refund =1 and a.is_after_sale_refund =0,1,0)=0 --排除售前退款
                  and a.is_exchange_order_flag =0 --非换货
                  and b.spec_machine_source in ('二手优品')
                  and b.sale_mark not in ('找靓机卖场')
                  and d.uid is not null
                  and e.purchase_channel = '国行'
                  and a.pay_time>c.create_time
            ) a
        where num=1
    ) s
    group by
		`支付日期`,`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`
	,concat_ws('-',`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`)
)
,b2c as
(
select
    `支付日期`,`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`
        ,concat_ws('-',`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`) as sku
        ,count(order_id) as `b2c销量`
        ,sum(`实际支付金额`) as `b2c售出gmv`
from
    (
    select
    *
    from
    (
    select
    --线上C卖场销售
    a.order_id
        ,'C卖场' as `卖场类型`
        ,DATE(a.pay_time) as `支付日期`
        ,case WHEN b.cate_first_id in(101,119) then b.cate_first_name
    when b.cate_second_id=1100000665 then b.cate_second_name
    else b.cate_third_name end as `品类`
        ,b.brand_name as `品牌`
        ,b.model_id as `机型id`
        ,lower(b.model_name) as `机型`
        ,a.total_amt/100  as `实际支付金额`
        ,c.subdivision_grade as `细分等级`
        ,b.spec_appearance_quality as `外观成色`
        ,b.spec_function_quality as `功能成色`
        ,e.purchase_channel as `购买渠道`
        ,e.spec_net  as `网络制式`
        ,case when spec_ram_1<>'' and spec_ram_2<>'' then concat_ws('+',e.spec_ram_1,e.spec_ram_2) when spec_ram_1='' then spec_ram_2 when spec_ram_2='' then spec_ram_1 end  as `容量`
        ,b.spec_color as `颜色`
        ,ROW_NUMBER() over(partition by b.qc_code order by c.create_time desc) as num
    from hdp_ubu_zhuanzhuan_dw_b2c.dw_trade_order_ord_all_subject_dtl_full_1d a   --B2C卖场订单表
    left join hdp_zhuanzhuan_dw_global.dw_info_prod_detail_full_1d b on a.info_id =b.info_id and b.dt='${outFileSuffix}'  --B2C卖场商品表
    left join
    (
    SELECT qc_code
        ,create_time
        ,subdivision_grade
        ,surface_gold_level_name
    from hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_bmskyway_t_skyway_product_full_1d --天路商品表取细分等级和成色
    where dt='${outFileSuffix}'
    ) c on b.qc_code=c.qc_code
    LEFT JOIN ydn_details e on b.qc_code=e.qc_code
    where a.dt='${outFileSuffix}'
    and if(a.is_refund =1 and a.is_after_sale_refund =0,1,0)=0 --排除售前退款
    and a.is_exchange_order_flag =0 --非换货
    and b.spec_machine_source in ('二手优品')
    and e.purchase_channel = '国行'
    and a.pay_time>c.create_time
    ) a
    where num=1
    ) s
group by
    `支付日期`,`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`
        ,concat_ws('-',`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`)
    ),
    store as --含同售，统计质检码
    (
select
    `支付日期`,`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`
        ,concat_ws('-',`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`) as sku
        ,count(case when order_type in (1,2) then qc_code else null end ) as `门店销量`
        ,sum(case when order_type in (1,2) then `成交价` else null end ) as `门店gmv`
        ,count(case when order_type in (3) then qc_code else null end ) as `同售销量`
        ,sum(case when order_type in (3) then `成交价` else null end ) as `同售gmv`
from
    (
    select distinct
    b.qc_code,
    b.cate_name AS `品类`,
    b.brand_name AS `品牌`,
    lower(b.model_name) AS `机型`,
    case when e.spec_ram_1<>'' and e.spec_ram_2<>'' then concat_ws('+',e.spec_ram_1,e.spec_ram_2) when spec_ram_1='' then spec_ram_2 when spec_ram_2='' then spec_ram_1 end as `容量`,
    b.valuer_grade_desc AS `等级`,
    b.appearance_grade_name as `外观成色`
        ,b.functional_grade_name as `功能成色`
        ,b.actual_cost_price/100 as `成本价`,
    substr(b.outbound_time,1,10) AS `支付日期`,
    b.deal_price/100 AS `成交价`,
    b.order_type
        ,e.spec_net as `网络制式`,c.subdivision_grade as `细分等级`
    from hdp_zhuanzhuan_dw_global.dw_trade_retail_offline_data_full_1d b
    LEFT JOIN ydn_details e on b.qc_code=e.qc_code
    left join hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_bmskyway_t_skyway_product_full_1d c on b.skyway_product_id = c.id and c.dt = '${outFileSuffix}' --天路商品表
    where  b.dt = '${outFileSuffix}'
    and substr(b.outbound_time,1,10)  BETWEEN '2023-01-01' and '${outFileSuffix}'
    and b.store_name not like '%测试%'
    and b.order_type in (1,2,3)
    ) b
group by `支付日期`,`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`
        ,concat_ws('-',`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`  )
    )
        ,store2 as --库存
    (
select
    dt,`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`
        ,concat_ws('-',`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`) as sku
        ,count(qc_code) as `在店库量`
        ,count(case when `战区` in ( '郑州战区','成都战区','长沙战区','合肥战区','济南战区' ) then qc_code else null end ) as `测试在店库量`
from
    (
    select DISTINCT
    b.dt,m.province as `战区`
        ,b.qc_code
        ,b.cate_name as `品类`
        ,b.brand_name as `品牌`
        ,LOWER(b.model_name) as `机型`
        ,case when e.spec_ram_1<>'' and e.spec_ram_2<>'' then concat_ws('+',e.spec_ram_1,e.spec_ram_2) when spec_ram_1='' then spec_ram_2 when spec_ram_2='' then spec_ram_1 end as `容量`
        ,b.appearance_grade_name as `外观成色`
        ,b.functional_grade_name as `功能成色`
        ,e.spec_net as `网络制式`,c.subdivision_grade as `细分等级`
    from hdp_zhuanzhuan_dw_global.dw_trade_retail_offline_data_full_1d b
    LEFT JOIN ydn_details e on b.qc_code=e.qc_code
    left join hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_bmskyway_t_skyway_product_full_1d c on b.skyway_product_id = c.id and c.dt = '${outFileSuffix}' --天路商品表
    left join hdp_ubu_zhuanzhuan_tmp_c2b.tmp_c2b_store_organization_full_1d m on b.store_id = m.store_id and m.dt = '${outFileSuffix}'
    where  b.dt  BETWEEN '2023-01-01' and '${outFileSuffix}'
    and b.store_name not like '%测试%'
    and b.storage_state in (2,3)  --商品库存状态：待入库,已入库，待出库
    and b.recall_state!=2 --召回状态：处理中
    ) a
group by
    dt,`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`
        ,concat_ws('-',`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`  )
    ),
    store3 as --纯门店销量，统计订单数
    (
select
    `支付日期`,`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`
        ,concat_ws('-',`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`) as sku
        ,count(case when order_type in (1,2) then `订单号` else null end) as `store销量`
        ,sum(case when order_type in (1,2) then `实际支付金额` else 0 end) as `store售出gmv`
        ,count(case when order_type in (1,2) and `战区` in ( '郑州战区','成都战区','长沙战区','合肥战区','济南战区' ) then `订单号` else null end ) as `store测试销量`
        ,sum(case when order_type in (1,2) and `战区` in ( '郑州战区','成都战区','长沙战区','合肥战区','济南战区' ) then `实际支付金额` else null end ) as `store测试售出gmv`
        ,count(case when order_type in (3) then `订单号` else null end) as `同售销量`
        ,sum(case when order_type in (3) then `实际支付金额` else 0 end) as `同售gmv`
        ,count(case when order_type in (5) then `订单号` else null end) as `小时达销量`
        ,sum(case when order_type in (5) then `实际支付金额` else 0 end) as `小时达gmv`
        ,sum(case  when order_type in (1,2)  then (`实际支付金额`+`以旧换优券成本`) else 0 end) as `以旧换优抵扣后gmv`
from
    (
    select
    a.order_id   as `订单号`,a.store_id,m.province as `战区`
        ,order_type
        ,'门店' as `卖场类型`
        ,date(a.outbound_time)  as `支付日期`
        ,b.qc_cate_name as `品类`
        ,b.qc_brand_name as `品牌`
        ,lower(b.qc_model_name) as `机型`
        ,a.deal_price/100  as `实际支付金额`
        ,(case(f.money)
    when 50 then 20
    when 120 then 30
    when 200 then 80
    when 300 then 120
    when 400 then 220
    else 0 end)
    as `以旧换优券成本`
        ,c.subdivision_grade as `细分等级`
        ,a.appearance_grade_name as `外观成色`
        ,a.functional_grade_name as `功能成色`
        ,e.purchase_channel as `购买渠道`
        ,e.spec_net as `网络制式`
        ,case when e.spec_ram_1<>'' and e.spec_ram_2<>'' then concat_ws('+',e.spec_ram_1,e.spec_ram_2) when spec_ram_1='' then spec_ram_2 when spec_ram_2='' then spec_ram_1 end as `容量`
    --,e.`保修时间`
    --,a.warranty as `保修情况`
    -- ,datediff(to_date(e.`保修时间`),CURRENT_DATE()) as `保修时长`
        ,e.`颜色`
        ,'二手优品' as `机器来源`
        ,count(a.order_id) over (partition by a.order_id) as order_ct
    from hdp_zhuanzhuan_dw_global.dw_trade_retail_offline_data_full_1d a --门店宽表
    join  hdp_ubu_zhuanzhuan_dm_c2b.dm_finance_bm_purchase_sale_detail_full_1d b on a.skyway_product_id = b.skyway_product_id and b.dt = '${outFileSuffix}' --采销表
    left join hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_bmskyway_t_skyway_product_full_1d c on a.skyway_product_id = c.id and c.dt = '${outFileSuffix}' --天路商品表
    LEFT JOIN ydn_details e on b.pur_qc_code=e.qc_code
    left join hdp_ubu_zhuanzhuan_tmp_c2b.tmp_c2b_store_organization_full_1d m on a.store_id = m.store_id and m.dt = '${outFileSuffix}'
    left join (SELECT distinct
    sales_order_id
        ,red_envelope_id
        ,money
    FROM hdp_ubu_zhuanzhuan_dw_c2b.dw_market_coupon_t_activity_coupon_relation_full_1d
    WHERE dt="${outFileSuffix}"
    and red_status in (2,3) --已使用，不含退货
    and scene_type=0
    )
    f on a.order_id=f.sales_order_id and a.is_old_change=1 --以旧换优
    where a.dt='${outFileSuffix}'
    and a.purchase_channels = 1 --天路货源,首次销售
    AND a.store_name not like '%测试%'
    AND a.order_type in (1,2,3,5) -- 20门店店内零售已完成，40门店同售严选完成
    --AND a.store_id not IN (40334455667788,652738901)
    and e.purchase_channel = '国行'
    ) a
where a.order_ct=1
group by
    `支付日期`,`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`
        ,concat_ws('-',`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`  )
    ),
    s as
    (
select distinct
    `支付日期` as `统计日期`,`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`,sku
from b2c
union
select distinct
    `支付日期` as `统计日期`,`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`,sku
from czc
union
select distinct
    `支付日期` as `统计日期`,`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`,sku
from store
union
select distinct
    dt as `统计日期`,`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`,sku
from store2
union
select distinct
    `支付日期` as `统计日期`,`品类`,`品牌`,`机型`,`容量`,`外观成色`,`功能成色`,`网络制式`,`细分等级`,sku
from store3
    )

insert overwrite table hdp_zhuanzhuan_tmp_global.tmp_new_retail_price_table_2_day partition(dt='${outFileSuffix}')

select
    `统计日期`,`品类`,`品牌`,a.`机型`,`容量`,`外观成色`,`功能成色`
     ,`b2c销量`
     ,`b2c售出gmv`
     ,`czc销量`
     ,`czc售出gmv`
     ,`门店销量`
     ,`门店gmv`
     ,`同售销量`
     ,`同售gmv`
     ,case when  s.`机型` is not null then '是' else '否' end as `热销机型`
     ,`在店库量`
     ,`store销量`
     ,`store售出gmv`
     ,`网络制式`
     ,`细分等级`
     ,`机型标签`
     ,`store测试销量`
     ,`store测试售出gmv`
     ,`测试在店库量`
     ,`czc售出gmv`/`czc销量`  as `czc客单价`
     ,`store售出gmv`/`store销量` as `门店客单价`
     ,(`czc售出gmv`/`czc销量`)*`store销量` as `czc_模拟gmv`
     ,`以旧换优抵扣后销量`
     ,`以旧换优抵扣后gmv`
     ,`小时达销量`
     ,`小时达gmv`
     ,(`czc售出gmv`/`czc销量`)*`小时达销量` as `czc_模拟gmv_小时达`
from
    (
        select
            s.`统计日期`,s.`品类`,s.`品牌`,s.`机型`,s.`容量`,s.`外观成色`,s.`功能成色`,s.`网络制式`,s.`细分等级`
             ,b2c.`b2c销量`
             ,b2c.`b2c售出gmv`
             ,czc.`czc销量`
             ,czc.`czc售出gmv`
             ,store.`门店销量`
             ,store.`门店gmv`
             ,store.`同售销量`
             ,store.`同售gmv`
             ,store2.`在店库量`
             ,store3.`store销量`
             ,store3.`store售出gmv`
             ,m.`机型标签`
             ,store3.`store测试销量`
             ,store3.`store测试售出gmv`
             ,store2.`测试在店库量`
             ,store3.`store销量` as `以旧换优抵扣后销量`
             ,store3.`以旧换优抵扣后gmv`
             ,store3.`小时达销量`
             ,store3.`小时达gmv`
        from s
                 left join b2c on s.`统计日期`=b2c.`支付日期` and s.sku = b2c.sku
                 left join czc on s.`统计日期`=czc.`支付日期` and s.sku = czc.sku
                 left join store on s.`统计日期`=store.`支付日期` and s.sku = store.sku
                 left join store2 on s.`统计日期`=store2.dt and s.sku = store2.sku
                 left join store3 on s.`统计日期`=store3.`支付日期` and s.sku = store3.sku
                 left join (SELECT distinct lower(model_name) as `机型`,group as `机型标签` FROM hdp_zhuanzhuan_dm_algo.dm_human_price_model_phone_inner_quotation_c_full_1d WHERE dt = '${outFileSuffix}') m
                           on s.`机型` = m.`机型`
    ) a
        left join  ( select *,lower(model_name) as `机型` from hdp_ubu_zhuanzhuan_dw.dw_hot_model_top50_1m) s
                   on a.`机型`=s.`机型` and last_day(a.`统计日期`)=last_day(s.operative_month)
where `统计日期` BETWEEN '2023-09-01' and '${outFileSuffix}'