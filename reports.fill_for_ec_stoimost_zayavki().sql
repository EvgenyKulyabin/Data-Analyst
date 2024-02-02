-- DROP PROCEDURE reports.fill_for_ec_stoimost_zayavki();

CREATE OR REPLACE PROCEDURE reports.fill_for_ec_stoimost_zayavki()
 LANGUAGE plpgsql
AS $procedure$
begin
----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
drop table if exists temp_z; 
create temp table temp_z 
as
select distinct mz.request_number
from dm_transportations.v_requests_in_route_lists as mz
where mz.route_list_date >= '2020-01-01 00:00:00' and mz.final_status in ('Отгружен', 'В работе', 'В пути', 'Перевозчик назначен', 'Доставлено', 'Выполнено', 'Согласовано')
and route_list_number is not null 
--group by left(mz.request_number,3)
;
create index ci_z on temp_z(request_number);
----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
--drop table if exists temp_c;
--create temp table temp_c 
--as
--select *
--from xpl_new.cargos as c
----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
drop table if exists temp_t;
create temp table temp_t 
as
select route_list_date/*::date*/, 
d.request_date, 
d.plan_loading_date, 
d.plan_unloading_date, 
d.fact_loading_date, 
case when d.fact_unloading_date < d.request_date then null else d.fact_unloading_date end fact_unloading_date, 
Max(d.status_date) over (partition by d.request_number) as status_date, 
Max(d.route_list_date::date) over (partition by d.request_number) as ml_last_date,
d.route_list_number,
d.request_number,
d.payer as client, 
d.main as client_group,
d.sender_organization, 
d.sender_group,
d.recipient_organization, 
d.recipient_group,
d.delivery_type,
d.status_upp,
d.final_status,
d.is_consolidation,
d.route_type,
d.trailer_type,
CASE
    WHEN case when max_dimension > 15 and rl_weight < 20 then 20 else rl_weight end = NULL::numeric OR d.delivery_type <> 'Автотранспорт' THEN 'Нет данных'
    WHEN d.route_type = 'STANDARD' AND case when max_dimension > 15 and rl_weight < 20 then 20 else rl_weight end <= 1.5 THEN '1,5 т'
    WHEN d.route_type = 'STANDARD' AND case when max_dimension > 15 and rl_weight < 20 then 20 else rl_weight end <= 3.5 THEN '3,5 т'
    WHEN d.route_type = 'STANDARD' AND case when max_dimension > 15 and rl_weight < 20 then 20 else rl_weight end <= 5::numeric THEN '5 т'
    WHEN d.route_type = 'STANDARD' AND case when max_dimension > 15 and rl_weight < 20 then 20 else rl_weight end <= 10::numeric THEN '10 т'
    WHEN d.route_type = 'STANDARD' AND case when max_dimension > 15 and rl_weight < 20 then 20 else rl_weight end <= 24::numeric THEN '20 т'
    WHEN d.route_type = 'CIRCULAR' AND (case when max_dimension > 15 and rl_weight < 20 then 20 else rl_weight end / 2::numeric) <= 1.5 THEN '1,5 т'
    WHEN d.route_type = 'CIRCULAR' AND (case when max_dimension > 15 and rl_weight < 20 then 20 else rl_weight end / 2::numeric) <= 3.5 THEN '3,5 т'
    WHEN d.route_type = 'CIRCULAR' AND (case when max_dimension > 15 and rl_weight < 20 then 20 else rl_weight end / 2::numeric) <= 5::numeric THEN '5 т'
    WHEN d.route_type = 'CIRCULAR' AND (case when max_dimension > 15 and rl_weight < 20 then 20 else rl_weight end / 2::numeric) <= 10::numeric THEN '10 т'
    WHEN d.route_type = 'CIRCULAR' AND (case when max_dimension > 15 and rl_weight < 20 then 20 else rl_weight end / 2::numeric) <= 24::numeric THEN '20 т'
    WHEN d.route_type = 'COMPLEX' AND (case when max_dimension > 15 and rl_weight < 20 then 20 else rl_weight end / 3::numeric) <= 1.5 THEN '1,5 т'
    WHEN d.route_type = 'COMPLEX' AND (case when max_dimension > 15 and rl_weight < 20 then 20 else rl_weight end / 3::numeric) <= 3.5 THEN '3,5 т'
    WHEN d.route_type = 'COMPLEX' AND (case when max_dimension > 15 and rl_weight < 20 then 20 else rl_weight end / 3::numeric) <= 5::numeric THEN '5 т'
    WHEN d.route_type = 'COMPLEX' AND (case when max_dimension > 15 and rl_weight < 20 then 20 else rl_weight end / 3::numeric) <= 10::numeric THEN '10 т'
    WHEN d.route_type = 'COMPLEX' AND (case when max_dimension > 15 and rl_weight < 20 then 20 else rl_weight end / 3::numeric) <= 24::numeric THEN '20 т'
    ELSE 'Прочие'
END AS trailer_type_new,
case 
  when coalesce(case when d.route_type = 'CIRCULAR' or rl_weight_max > rl_weight then rl_weight_max else rl_weight end, 0) <= 1.5 then '1.5 т.'
  when coalesce(case when d.route_type = 'CIRCULAR' or rl_weight_max > rl_weight then rl_weight_max else rl_weight end, 0) > 1.5 and coalesce(case when d.route_type = 'CIRCULAR' or rl_weight_max > rl_weight then rl_weight_max else rl_weight end, 0) <= 3.5 then '3 т.'
  when coalesce(case when d.route_type = 'CIRCULAR' or rl_weight_max > rl_weight then rl_weight_max else rl_weight end, 0) > 3.5 and coalesce(case when d.route_type = 'CIRCULAR' or rl_weight_max > rl_weight then rl_weight_max else rl_weight end, 0) <= 5.5 then '5 т.'
  when coalesce(case when d.route_type = 'CIRCULAR' or rl_weight_max > rl_weight then rl_weight_max else rl_weight end, 0) > 5.5 and coalesce(case when d.route_type = 'CIRCULAR' or rl_weight_max > rl_weight then rl_weight_max else rl_weight end, 0) <= 10.5 then '10 т.'
  when coalesce(case when d.route_type = 'CIRCULAR' or rl_weight_max > rl_weight then rl_weight_max else rl_weight end, 0) > 10.5 and coalesce(case when d.route_type = 'CIRCULAR' or rl_weight_max > rl_weight then rl_weight_max else rl_weight end, 0) <= 20.5 then '20 т.'
  when coalesce(case when d.route_type = 'CIRCULAR' or rl_weight_max > rl_weight then rl_weight_max else rl_weight end, 0) > 20.5 then '20+ т.'
 end weigth_category,
 d.max_dimension as cargo_length,
 case 
  when coalesce(case when weight_by_dimension > weight then weight_by_dimension else weight end, 0) <= 1.5 then '1.5 т.'
  when coalesce(case when weight_by_dimension > weight then weight_by_dimension else weight end, 0) > 1.5 and coalesce(case when weight_by_dimension > weight then weight_by_dimension else weight end, 0) <= 3.5 then '3 т.'
  when coalesce(case when weight_by_dimension > weight then weight_by_dimension else weight end, 0) > 3.5 and coalesce(case when weight_by_dimension > weight then weight_by_dimension else weight end, 0) <= 5.5 then '5 т.'
  when coalesce(case when weight_by_dimension > weight then weight_by_dimension else weight end, 0) > 5.5 and coalesce(case when weight_by_dimension > weight then weight_by_dimension else weight end, 0) <= 10.5 then '10 т.'
  when coalesce(case when weight_by_dimension > weight then weight_by_dimension else weight end, 0) > 10.5 and coalesce(case when weight_by_dimension > weight then weight_by_dimension else weight end, 0) <= 20.5 then '20 т.'
  when coalesce(case when weight_by_dimension > weight then weight_by_dimension else weight end, 0) > 20.5 then '20+ т.'
 end as request_weigth_category,
d.sender_region, 
d.sender_country, 
d.sender_city,
d.sender_address,
d.recipient_country, 
d.recipient_region,
d.recipient_city,
d.recipient_address, 
d.carrier,
d.carrier_inn, 
d.red_carrier_count,
d.document_create_place,
d.manager_xpl as manager,
case when coalesce(d.is_consolidation,0) = 0 then 'Прямой рейс' else 'Через склад' end as transportation_type,
case when d.is_consolidation = 0 then 'Нет' else consolidation_warehouse end as warehouse,
coalesce(d.customer_price,0) as price
, coalesce(carrier_price,0) as "cost", 
coalesce(d.r_customer_price,0) as request_price,
sum(d.customer_price) over (partition by d.route_list_number) as price_ml
, sum(d.carrier_price) over (partition by d.route_list_number) as cost_ml,
sum(d.customer_price) over (partition by d.request_number) as price_request
, sum(d.carrier_price) over (partition by d.request_number) as cost_request,
coalesce(d.start_price_amount,0) as NMC,
d.class1+d.class2+d.class3+d.class4+d.class5+d.class6+d.class7+d.class8+d.class9 as danger,
d.fast as fast,
d.side+d.top as "top/side",
coalesce(d.customer_price,0) - coalesce(carrier_price,0) as profit,
d.distance as distance, 
d.weight as weight,
d.volume as volume,
d.max_dimension,
d.vehicle_number,
d.rl_loading_address ,
--sum(coalesce(d.r_customer_price,0)) over (partition by request_number)::numeric(10,2) as request_price_all , 
--row_number() over (partition by request_number order by route_list_date desc nulls last) as route_num,
case when row_number() over (partition by d.request_number order by d.plan_loading_date desc nulls last, d.route_list_date desc nulls last) = 1 then 
		case when d.document_create_place = 'xPlanet' /*or coalesce(d.r_customer_price,0) = 0*/ then --в первой итерации "or" не был закомментирован
				  --sum(coalesce(d.customer_price,0)) over (partition by request_number)::numeric(10,2)
				  --sum(coalesce(d.customer_price,0)) over (partition by route_list_number)::numeric(10,2)
				  --d.customer_price::numeric(10,2)
				  coalesce(d.r_customer_price::numeric(10,2), 0)
			 else 
				  sum(coalesce(d.r_customer_price,0)) over (partition by request_number)::numeric(10,2)
		end 
	 else 0
end as price_all, 
	 --, row_number() over (partition by request_number order by d.plan_loading_date desc nulls last, d.route_list_date desc nulls last) as r_num
--, 1 as sovpadenie
--, case when row_number() over (partition by request_number order by route_list_date desc nulls last) = 1 then lt_stoimost_perevozki else 0 end as price_all_
--, case when row_number() over (partition by request_number order by route_list_date desc nulls last) = 1 then r_customer_price else 0 end as price_all_r
--, d.r_customer_price

--, row_number() over (partition by d.request_number order by d.plan_loading_date desc nulls last, d.route_list_date desc nulls last) as r_num
--, d.r_customer_price
--, d.customer_price::numeric(10,2)
finish_type 
from dm_transportations.v_requests_in_route_lists as d
left join upp_work.zayavka_na_transport as znt on znt.nomer = d.request_number and abs(extract(epoch from znt."data"-d.request_date)) < 20
where d.request_number in (select request_number from temp_z) 
and (znt.pometka_udaleniya is false or znt.pometka_udaleniya is null)
--and d.request_number='WMS030238'
;

--select * from temp_t where route_list_number in ('XPC120375',
--'XPC119982',
--'XPC120274',
--'XPC120467',
--'XPC121610',
--'XPC121669',
--'XPL100772',
--'XPL100918',
--'XPL101070',
--'XPL101278',
--'XPX107039',
--'XPX107040',
--'XPX107341',
--'XPX107409'
--)
--select sum(price_all) from temp_t where route_list_number = 'СЛ0602215'
--select 3012099.18/1.2



truncate reports.t_for_ec_stoimost_zayavki;
insert into reports.t_for_ec_stoimost_zayavki
select * from temp_t;
----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------

--select *
--from upp_work.zayavka_na_transport
--where nomer='WMS030238'

end 
$procedure$
;
