-- DROP PROCEDURE reports.fill_rukovodstvo_plan_fact();

CREATE OR REPLACE PROCEDURE reports.fill_rukovodstvo_plan_fact()
 LANGUAGE plpgsql
AS $procedure$
begin


drop table if exists temp_z; 
create temp table temp_z 
as
select distinct mz.request_number
from dm_transportations.v_requests_in_route_lists as mz
where mz.route_list_date >= '2020-01-01 00:00:00' --and mz.final_status in ('Доставлен', 'Отгружен', 'В работе', 'В пути', 'Водитель назначен', 'Доставлено', 'Выполнено','Выполнена')
--group by left(mz.request_number,3)
;
create index ci_z on temp_z(request_number);
--select * from temp_z where request_number = 'ZZS320582'
----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
drop table if exists temp_final;
create temp table temp_final
as
with c1 as (select *
			, case when row_number() over (partition by request_number order by route_list_date desc nulls last) = 1 then 
--											case when document_create_place = 'xPlanet' or coalesce(r_customer_price,0) = 0 then 
--													  sum(coalesce(customer_price,0)) over (partition by request_number)::numeric(10,2)
--												 else 
													  sum(coalesce(r_customer_price,0)) over (partition by request_number)::numeric(10,2)
--											end 
									   else 0
								  end as price_all
			from reports.t_requests_in_route_lists as d)
   , cte as (
                        select d.request_number, d.request_date::date,
                                string_agg(d.route_list_number, ', ') over (partition by d.request_number) as ml,
                                count(d.route_list_number) over (partition by d.request_number) as count_ml,
                                first_value(d.payer) over (partition by d.request_number order by coalesce(route_list_date, '2001-01-01') DESC) as client,
                                first_value(d.main) over (partition by d.request_number order by coalesce(route_list_date, '2001-01-01') DESC) as client_group,
                                first_value(d.delivery_type) over (partition by d.request_number order by route_list_date) as delivery_type1,
                                case when count(d.route_list_number) over (partition by d.request_number) > 1 then first_value(d.delivery_type) over (partition by d.request_number order by route_list_date DESC) else null end as delivery_type2,
                                first_value(d.route_type) over (partition by d.request_number order by route_list_date) as route_type1,
                                first_value(d.route_type) over (partition by d.request_number order by route_list_date DESC) as route_type2,
                                first_value(d.trailer_type) over (partition by d.request_number order by route_list_date) as trailer_type1,
                                first_value(d.document_create_place) over (partition by d.request_number) as document_create_place,
                                --first_value(case when d.is_consolidation = 0 then 'Прямой рейс' else 'Через склад' end) over (partition by d.request_number) as transportation_type,
                                max(coalesce(d.is_consolidation, 0)) over (partition by d.request_number) as transportation_type,
                                sum(d.customer_price) over (partition by d.request_number) as price,
                                sum(d.carrier_price) over (partition by d.request_number) as cost,
                                sum(d.class1+d.class2+d.class3+d.class4+d.class5+d.class6+d.class7+d.class8+d.class9) over (partition by d.request_number) as danger,
                                sum(d.fast) over (partition by d.request_number) as fast,
                                sum(d.side+d.top) over (partition by d.request_number) as "top/side",
                                sum(d.price_all) over (partition by d.request_number)- sum(d.carrier_price) over (partition by d.request_number) as profit,
                                sum(d.distance) over (partition by d.request_number) as distance, 
                                first_value(case when d.weight > 1000 then d.weight / 1000 else d.weight end) over (partition by d.request_number) as weight,
                                first_value(d.status_upp) over (partition by d.request_number order by route_list_date DESC) as status,
                                case when d.final_status in ('Доставлен', 'Отгружен', 'В работе','В пути', 'Водитель назначен', 
                                'Доставлено', 'Выполнено','Выполнена') then true else false end as status_fin
                                , case when d.final_status in ('Доставлен','Доставлено', 'Выполнено','Выполнена') then true else false end as oper_uchet
                                , dd.date_d
                                , row_number() over (partition by request_number order by dd.date_d desc) as rn
                                , d.update_date 
                                
--                                , d.r_customer_price
--                                , d.customer_price
--                                , d.route_list_date
                                
                                , price_all
                                
                        from c1 as d
                        join lateral (select case when 
                        case when d.fact_unloading_date < d.request_date then null else d.fact_unloading_date end > '2011-01-01' then fact_unloading_date
                        when d.plan_unloading_date > '2011-01-01' then plan_unloading_date
                        else case when d.fact_loading_date > '2011-01-01' then fact_loading_date + '5 day'::interval
                        else d.plan_loading_date + '5 day'::interval
                        end
                        end::date as date_d) as dd on true
                        where d.request_number in (select request_number from temp_z) )
select request_number,
request_date,
ml,
count_ml,
client,
client_group,
delivery_type1,
delivery_type2,
route_type1,
route_type2,
trailer_type1,
document_create_place,
case when transportation_type = 0 then 'Прямой рейс' else 'Через склад' end as transportation_type,
--transportation_type2,
price,
cost,
danger,
fast,
"top/side",
profit,
distance, 
weight,
status,
status_fin,
oper_uchet,
date_d,
update_date,
price_all
from cte
where rn=1
;

--select * from temp_final where request_number in ('EDS306315', 'LLS449904')


--drop table if exists reports.t_rukovodstvo_plan_fact;  --650453
--create table reports.t_rukovodstvo_plan_fact
--as
--select * from temp_final;

truncate reports.t_rukovodstvo_plan_fact;
insert into reports.t_rukovodstvo_plan_fact
select * from temp_final;


end
$procedure$
;
