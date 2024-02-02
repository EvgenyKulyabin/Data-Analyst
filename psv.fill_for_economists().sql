-- DROP PROCEDURE psv.fill_for_economists();

CREATE OR REPLACE PROCEDURE psv.fill_for_economists()
 LANGUAGE plpgsql
AS $procedure$
declare 
	month_fix date := date_trunc('month', now() - '1 month'::interval);
	year_fix date := date_trunc('year', month_fix);
begin

if (select count(*) from psv.t_for_economists fe where date_d >= month_fix)=0 then
	
	drop table if exists temp_d;
	create temp table temp_d
	as
	with cte as (
					select d.request_number, d.request_date::date,
						string_agg(d.route_list_number, ', ') over (partition by d.request_number) as ml,
						count(d.route_list_number) over (partition by d.request_number) as count_ml,
						first_value(d.payer) over (partition by d.request_number order by route_list_date DESC) as client,
						first_value(d.main) over (partition by d.request_number order by route_list_date DESC) as client_group,
						first_value(d.delivery_type) over (partition by d.request_number order by route_list_date) as delivery_type1,
						case when count(d.route_list_number) over (partition by d.request_number) > 1 then first_value(d.delivery_type) over (partition by d.request_number order by route_list_date DESC) else null end as delivery_type2,
						first_value(d.route_type) over (partition by d.request_number order by route_list_date) as route_type1,
						first_value(d.route_type) over (partition by d.request_number order by route_list_date DESC) as route_type2,
						first_value(d.trailer_type) over (partition by d.request_number order by route_list_date) as trailer_type1,
						first_value(d.document_create_place) over (partition by d.request_number) as document_create_place,
						first_value(case when d.is_consolidation = 0 then 'Прямой рейс' else 'Через склад' end) over (partition by d.request_number) as transportation_type,
						sum(d.customer_price) over (partition by d.request_number) as price,
						sum(d.carrier_price) over (partition by d.request_number) as cost,
						sum(d.class1+d.class2+d.class3+d.class4+d.class5+d.class6+d.class7+d.class8+d.class9) over (partition by d.request_number) as danger,
						sum(d.fast) over (partition by d.request_number) as fast,
						sum(d.side+d.top) over (partition by d.request_number) as "top/side",
						sum(d.customer_price) over (partition by d.request_number)- sum(d.carrier_price) over (partition by d.request_number) as profit,
						sum(d.distance) over (partition by d.request_number) as distance, 
						first_value(case when d.weight > 1000 then d.weight / 1000 else d.weight end) over (partition by d.request_number) as weight,
						first_value(d.status_upp) over (partition by d.request_number order by route_list_date DESC) as status,
						case when d.final_status in ('Доставлен', 'Отгружен', 'В работе','В пути', 'Водитель назначен', 
						'Доставлено', 'Выполнено','Выполнена') then true else false end as status_fin
						, dd.date_d
						, date_trunc('month', date_d) as date_d_month
						, d.final_status
						, row_number() over (partition by request_number order by dd.date_d desc) as rn
					from salair_upp.mz_requests_in_route_lists d
					left join xpl_new.cargos c on c.id = d.guid
					left join xpl_new.orders o on o.id = c.order_id 
					join lateral (select case when 
													case when d.fact_unloading_date < d.request_date then null else d.fact_unloading_date end > '2011-01-01' then fact_unloading_date
											  when d.plan_unloading_date > '2011-01-01' then plan_unloading_date
											  else case when d.fact_loading_date > '2011-01-01' then fact_loading_date + '5 day'::interval
													    else d.plan_loading_date + '5 day'::interval
												   end
										 end::date as date_d) as dd on true
					where d.request_date > year_fix)
	select request_number
	, request_date
	, ml
	, count_ml
	, client
	, client_group
	, delivery_type1
	, delivery_type2
	, route_type1
	, route_type2
	, trailer_type1
	, document_create_place
	, transportation_type
	, price
	, "cost"
	, danger
	, fast
	, "top/side"
	, profit
	, distance
	, weight
	, status
	, status_fin
	, date_d
	, date_d_month
	, final_status
	from cte
	where rn=1 --and date_d_month<date_trunc('month', now()) 
	;
	--select * from temp_d
	
	
	drop table if exists temp_m;
	create temp table temp_m
	as
	select request_number
	, request_date
	, ml
	, count_ml
	, client
	, client_group
	, delivery_type1
	, delivery_type2
	, route_type1
	, route_type2
	, trailer_type1
	, document_create_place
	, transportation_type
	, price
	, "cost"
	, danger
	, fast
	, "top/side"
	, profit
	, distance
	, weight
	, status
	, status_fin
	, date_d
	, date_d_month
	from temp_d
	where date_d between year_fix and date_trunc('month', now()) - '1 second'::interval
		  and final_status in ('Доставлен', 'Доставлено', 'Выполнено','Выполнена')
	;
	--select * from temp_m where date_d>='2023-02-01'
	
	drop table if exists temp_final; --инкремент
	create temp table temp_final
	as
	select d.request_number
	, d.request_date
	, d.ml
	, d.count_ml
	, d.client
	, d.client_group
	, d.delivery_type1
	, d.delivery_type2
	, d.route_type1
	, d.route_type2
	, d.trailer_type1
	, d.document_create_place
	, d.transportation_type
	, d.price
	, d."cost"
	, d.danger
	, d.fast
	, d."top/side"
	, d.profit
	, d.distance
	, d.weight
	, d.status
	, d.status_fin
	, d.date_d
	, d.date_d_month
	from temp_m as d
	join (
			select request_number, ceil(price) -----вычисляем инкремент-----
			--,
			--*
			from temp_m --where date_d >= '2023-02-01'
				except -----вычисляем инкремент-----
			select request_number, ceil(price)
	--		, 
	--		*
			from psv.t_for_economists fe 
			where date_d >= year_fix
			) as inc using(request_number)
	--where date_d>='2023-02-01' and inc.request_number is null
	;
	--21_823  19_973
	--ALS054235 8375.00
	update temp_final -- всем записям ставим фиксируемый месяц, в том числе с предыдущих периодов, которые были изменены к моменту фиксации месяца, и которые попали в инкремент
	set date_d_month = month_fix;
	--select * from temp_final
			
	insert into psv.t_for_economists 
	select *
	from temp_final	;
	
	RAISE NOTICE '%', '';
	RAISE NOTICE '%', 'Предыдущий месяц зафиксирован!';
	RAISE NOTICE '%', '';
	RAISE NOTICE '%', 'Данные записались!';

	insert into psv.t_for_economists_load_log select now(), 'Данные записались!';
	
	--select * from psv.for_economists
else
	RAISE NOTICE '%', '';
	RAISE NOTICE '%', 'Предыдущий месяц уже был зафиксирован!';
	RAISE NOTICE '%', '';
	RAISE NOTICE '%', 'Данные не перезаписались!';

	insert into psv.t_for_economists_load_log select now(), 'Предыдущий месяц уже был зафиксирован! Данные не записались!';
end if;
end
$procedure$
;
