-- DROP PROCEDURE psv.fill_rating_of_carrier();

CREATE OR REPLACE PROCEDURE psv.fill_rating_of_carrier()
 LANGUAGE plpgsql
AS $procedure$
declare 
	vh int := 1;
begin

--стоимость МЛ в разрезах 185, 1y, 2y

drop table if exists temp_carrier;
create temp table temp_carrier
as
WITH moderation_status AS (SELECT DISTINCT b2.inn,
				              first_value(b2.moderation_status) OVER (PARTITION BY b2.inn ORDER BY (COALESCE(b2.created_date, b2.modified_date)) DESC) AS last_status
				           FROM xpl_new.branches b2
				           )
SELECT b.inn,
    'xPlanet'::text AS create_place,
    max(b.name::text) AS carrier_name,
    min(COALESCE(b.created_date, /*b.modified_date*/'2019-07-15 09:00:00'::timestamptz)) AS created_date,
    ms.last_status
--select *
FROM xpl_new.branches b
LEFT JOIN xpl_new.users u using(org_id) --ON u.org_id = b.org_id
LEFT JOIN moderation_status ms using(inn) --ON ms.inn::text = b.inn::text
WHERE u.type::text = 'CARRIER'::text
GROUP BY b.inn, 'xPlanet'::text, ms.last_status
;
--select * from temp_carrier where inn = '362301301499'

--ALS048449 ALS048681 ALS048771


drop table if exists temp_le;
create temp table temp_le
as
select distinct le.order_id
from xpl_new.lifecycle_events as le --11516534
where le.originator='driver-service'
;
create index ci_le on temp_le(order_id);



drop table if exists temp_ces;
create temp table temp_ces
as
select ces.id --175184
from xpl_new.cargo_endpoints ces
where ces.region_id in ('1', '3', '14', '19', '24', '25', '27', '28', '38', '49', '65', '75', '79') /*от Красноярска и восточнее*/ 
;
create index ci_ces on temp_ces(id);


--====================================================================================================================================--
--====================================================================================================================================--
drop table if exists temp_rt;
create temp table temp_rt
as
select distinct rt.negotiation_id
from xpl_new.route_types as rt
where rt.route_type='COMPLEX' --126672
;
create index ci_rt on temp_rt(negotiation_id);



drop table if exists temp_reason;
create temp table temp_reason
as
select id
--select *
from xpl.application_cancel_reasons cr --where choosable is true
where canceler_type='CARRIER' or "name" in (
'Отмена по инициативе перевозчика', 'По требованию перевозчика', 'Не можем подтвердить стоимость перевозки согласно заявки', 'Бэк офис: Не могу выполнить перевозку (срыв сроков загрузки/доставки) +',
'Бек-офис: Не можем подтвердить выполнение заявки (водитель и т/с не назначены)', 'Не выбирать-По требованию перевозчика', 'Нажал не туда (ошибочно выиграл редукцион / тендер)'

, 'Бэк офис: Не могу выполнить перевозку (срыв сроков загрузки/доставки) +'
, 'По требованию перевозчика - Не можем подтвердить стоимость перевозки согласно заявки'
, 'Не выбирать-По требованию перевозчика - Нажал не туда (ошибочно выиграл редукцион / тендер)'
, 'Не выбирать -По требованию перевозчика - Не можем подтвердить сроки загрузки согласно заявки (срыв сроков загрузки)'
, 'Не выбирать -Не можем подтвердить выполнение заявки (водитель и т/с не назначены)'
, 'По требованию перевозчика - Не можем подтвердить стоимость перевозки согласно заявки')
--order by id
;
create index ci_r on temp_reason(id);


drop table if exists temp_owner_of_transport;
create temp table temp_owner_of_transport
as
with rn as (SELECT tra."number", tra.price, tra.created_at, tra.carrier_id 
			, case when v.right_to_use  = 'OWN' then 1 else 0 end as owner_sign --91582
			, case when tra.status = 'CANCELED'::text then 1 else 0 end as cancel_sign
			, case when cr.id is null then 0 else 1 end as carrier_canceled_sign
			, tra.negotiation_id 
			, tra.first_driver_id
			, row_number() over (partition by tra."number" order by coalesce(updated_at, created_at) desc) as rn
			--select *
			FROM xpl_new.transportations as tra
			left join xpl.vehicles v ON v.id::text = tra.truck_id and v.right_to_use  = 'OWN'
			left join temp_reason cr on cr.id::text = tra.cancellation_reason_id::text
			--WHERE "number"='ALS048449'
			)
select "number"
, case when rn=1 then price end as final_price
, case when rn=1 then owner_sign end as owner_sign
, case when rn=1 then cancel_sign end as cancel_sign
, carrier_canceled_sign
, case when rn=1 then created_at end as created_at
, carrier_id
, case when rn=1 then bw.inn end as inn_w
, case when rn=1 then rt.negotiation_id end as route_type_complex
, bw.inn
, negotiation_id 
, first_driver_id
, rn
from rn
left join xpl_new.branches bw on bw.org_id = rn.carrier_id
left join temp_rt rt using(negotiation_id)
--where rn=1
--AND transportations.created_at >= /*(now()::date - 365)::timestamptz*/ perem_date
;
create index ci_owner_of_transport on temp_owner_of_transport("number", carrier_id);
--select * from temp_owner_of_transport where carrier_id='6428'   "number" = 'ABS300005';



drop table if exists temp_neg;
create temp table temp_neg
as
with ng as (select n."number"
			, b.inn as inn
			, nb.carrier_id
			--, n.id
			from xpl_new.negotiations as n
			left join xpl_new.negotiation_bids nb on nb.negotiation_id = n.id 
			left join xpl_new.branches b on b.org_id = nb.carrier_id 
			where n."number" is not null --and /*n.number =*/ n."number" in ('LLS424579', 'ALS048449', 'XPX117069' )
			group by n."number", b.inn, nb.carrier_id)
			
select n."number" as n_number
, tra."number" as own_number
, coalesce(n.inn, tra.inn) as inn
, coalesce(n.carrier_id, tra.carrier_id) as carrier_id
, tra.final_price
, tra.owner_sign
, tra.cancel_sign
, tra.carrier_canceled_sign
, tra.created_at
, case when tra.rn=1 then tra.carrier_id end as winner_id
, tra.inn_w
, tra.negotiation_id 
, tra.route_type_complex
--, tra.rn
from ng as n
left join temp_owner_of_transport as tra on (tra."number" = n."number" and n.carrier_id is not null and tra.carrier_id=n.carrier_id)
										 or (tra."number" = n."number" and n.carrier_id is null)
where tra.first_driver_id <> '07d586e9-b445-42eb-9336-e0d6b46ef074'
;
--select * from temp_neg
--====================================================================================================================================--
--====================================================================================================================================--


--20 секунд +/-
drop table if exists temp_bills;
create temp table temp_bills
as
select neg.inn
, o.created_at::date --, o.cancel_reason_id, cr.id
, count(c.number) as count_of_requests
, count(case when neg.carrier_id = neg.winner_id then c.number else null end) as win

, count(case when neg.carrier_id = neg.winner_id and (o.status in ('CANCELED','EXPIRED') or neg.cancel_sign=1) then c.number else null end) as cancel 
, count(case when /*neg.carrier_id = neg.winner_id and (o.status in ('CANCELED','EXPIRED') or neg.cancel_sign=1) and*/ neg.carrier_canceled_sign=1 then c.number else null end) as canceled_by_the_carrier 

, count(case when neg.route_type_complex is not null and neg.carrier_id = neg.winner_id and (o.status not in ('CANCELED','EXPIRED') and neg.cancel_sign=0) then neg.route_type_complex else null end) as complicated_routes
, max(case when le.order_id is not null then 1 else 0 end) as mobile_application
, count(case when ces.id is not null and neg.carrier_id = neg.winner_id and (o.status not in ('CANCELED','EXPIRED') and neg.cancel_sign=0) then ces.id else null end) as from_the_far_est
, count(case when neg.owner_sign = 1 and (o.status not in ('CANCELED','EXPIRED') and neg.cancel_sign=0) and neg.carrier_id = neg.winner_id then neg.own_number else null end) as owner_of_transport
, sum(case when neg.own_number is not null and (o.status not in ('CANCELED','EXPIRED') and neg.cancel_sign=0) and neg.carrier_id = neg.winner_id then neg.final_price else 0 end) as price_of_transport
, string_agg(neg.n_number, ', ') as number_ml 
, neg.n_number as ml
, neg.cancel_sign
, o.status
, neg.carrier_id, neg.winner_id
--select *
from xpl_new.cargos c  --where id<>order_id --344046
left join /*select * from*/ xpl_new.orders o on o.id = c.order_id --279
left join temp_le as le on le.order_id=o.id
left join temp_ces ces ON ces.id = c.origin_id --697

join /*select * from*/ xpl_new.negotiation_orders nor on nor.order_id = o.id --2.8
left join temp_neg as neg on neg.negotiation_id=nor.negotiation_id

group by neg.inn, o.created_at::date
, neg.n_number
, neg.cancel_sign
, o.status
, neg.carrier_id, neg.winner_id
;
/*order by o.created_at::date desc*/    --78363
create index ci_bills on temp_bills(inn);
--select * from temp_bills where number_ml like '%LPS320998%' inn = '681301538112' and to_char(created_at, 'YYYY-MM')='2023-01' group by inn --ZZS310434, ZZS310434, ZLS302776 /*2022-02*/
--select * from temp_bills where ml='ALS048449' --inn = '6150073464' and to_char(created_at, 'YYYY-MM')='2023-01' order by 2 
	
--ZPS302766
--168567



if vh = 1 then
	drop table if exists temp_owner_of_transport, temp_rt, temp_ces, temp_le, temp_reason;
end if;

	

drop table if exists temp_car;
create temp table temp_car
as			   
select car.inn
, car.carrier_name
, car.last_status
, b.status as status_ml
, car.create_place
, car.created_date::date as working_start
, b.created_at as date_bills
, to_char(b.created_at, 'YYYY-MM') as yyyy_mm
, to_char(date_trunc('month',b.created_at)::date-180, 'YYYY-MM') as yyyy_mm_180
, to_char(date_trunc('month',b.created_at)::date-365, 'YYYY-MM') as yyyy_mm_1y
, to_char(date_trunc('month',b.created_at)::date-365*2, 'YYYY-MM') as yyyy_mm_2y
, coalesce(b.count_of_requests, 0) as count_of_requests
, coalesce(b.win, 0) as win
, coalesce(b.cancel, 0) as cancel
, case when coalesce(b.win, 0)=0 then 0 else coalesce(b.win, 0) - coalesce(b.cancel/*ed_by_the_carrier*/, 0) end as went
, coalesce(b.canceled_by_the_carrier, 0) as canceled_by_the_carrier
, coalesce(b.complicated_routes, 0) as complicated_routes
, coalesce(b.mobile_application, 0) as mobile_application
, coalesce(b.from_the_far_est, 0) as from_the_far_est
, coalesce(b.owner_of_transport, 0) as owner_of_transport
, coalesce(b.price_of_transport, 0) as price_of_transport
, min(case when win>0 and b.canceled_by_the_carrier=0 then b.created_at else null end) over (partition by car.carrier_name) as first_winning_date
, max(case when win>0 and b.canceled_by_the_carrier=0 then b.created_at else null end) over (partition by car.carrier_name) as last_winning_date
, b.ml
from temp_carrier car
join temp_bills b using(inn) --on b.inn = car.inn
--order by "Кол-во заявок" nulls last
--select * from temp_car where ml = 'ABS300005' order by yyyy_mm desc nulls last where ml is null inn = '681301538112'  and price_of_transport > 0 --carrier_name='ООО "Веста"' /* inn = '681301538112'*/ and yyyy_mm='2023-01' and status_ml not in ('CANCELED')  --carrier_name = 'Серов Михаил Игоревич'
;
create index ci_car on temp_car(inn, yyyy_mm);
--167204

if vh = 1 then
	drop table if exists temp_bills;
end if;


drop table if exists temp_cym;
create temp table temp_cym
as
select c.carrier_name
, c.inn
, c.working_start
, c.first_winning_date
, c.last_winning_date
, date_part('year', age(c.last_winning_date, c.first_winning_date))*12+date_part('month', age(c.last_winning_date, c.first_winning_date)) as month_of_cooperation_work
, date_part('year', age(current_date , c.last_winning_date))*12+date_part('month', age(current_date , c.last_winning_date)) as month_of_cooperation_idle
, c.yyyy_mm
, c.yyyy_mm_180
, c.yyyy_mm_1y
, c.yyyy_mm_2y
, sum(c.count_of_requests) as count_of_requests
, sum(c.win) as win
, sum(c.cancel) as cancel
, sum(c.went) as went
, sum(c.canceled_by_the_carrier) as canceled_by_the_carrier
, sum(c.complicated_routes) as complicated_routes
, max(c.mobile_application) as mobile_application
, sum(c.from_the_far_est) as from_the_far_est
, sum(c.owner_of_transport) as owner_of_transport
, sum(case when c.went>0 then c.price_of_transport else 0 end) as price_of_transport
--select *
from temp_car as c
--where c.carrier_name = 'Серов Михаил Игоревич'
group by c.carrier_name
, c.inn
, c.working_start
, c.first_winning_date
, c.last_winning_date
, c.yyyy_mm
, c.yyyy_mm_180
, c.yyyy_mm_1y
, c.yyyy_mm_2y
--order by 2
;
--select * from temp_cym where inn = '3811126641' and yyyy_mm='2021-07'



--select date_trunc('month',current_date - 180), current_date;
drop table if exists temp_final;
create temp table temp_final
as
select c.carrier_name
, c.inn
, c.working_start
, c.first_winning_date
, c.last_winning_date
--, month_of_cooperation_idle
--, month_of_cooperation_work
, case when month_of_cooperation_work-month_of_cooperation_idle<0 then 0 else month_of_cooperation_work-month_of_cooperation_idle end as month_of_cooperation
, (c.yyyy_mm || '-01')::date as yyyy_mm
--, c.yyyy_mm_180
, c.count_of_requests
, c.win
, c.cancel
, c.went
, c.canceled_by_the_carrier
, c.complicated_routes
, c.mobile_application
, c.from_the_far_est
, c.owner_of_transport
, c.price_of_transport

, coalesce(c2.count_of_requests, 0) as count_of_requests_185
, coalesce(c2.win, 0) as win_185
, coalesce(c2.cancel, 0) as cancel_185
, coalesce(c2.went, 0) as went_185
, coalesce(c2.canceled_by_the_carrier, 0) as canceled_by_the_carrier_185
, coalesce(c2.complicated_routes, 0) as complicated_routes_185
, coalesce(c2.from_the_far_est, 0) as from_the_far_est_185
, coalesce(c2.owner_of_transport, 0) as owner_of_transport_185
, coalesce(c2.price_of_transport, 0) as price_of_transport_185

, coalesce(c3.count_of_requests, 0) as count_of_requests_1y
, coalesce(c3.win, 0) as win_1y
, coalesce(c3.cancel, 0) as cancel_1y
, coalesce(c3.went, 0) as went_1y
, coalesce(c3.canceled_by_the_carrier, 0) as canceled_by_the_carrier_1y
, coalesce(c3.complicated_routes, 0) as complicated_routes_1y
, coalesce(c3.from_the_far_est, 0) as from_the_far_est_1y
, coalesce(c3.owner_of_transport, 0) as owner_of_transport_1y
, coalesce(c3.price_of_transport, 0) as price_of_transport_1y

, coalesce(c4.count_of_requests, 0) as count_of_requests_2y
, coalesce(c4.win, 0) as win_2y
, coalesce(c4.cancel, 0) as cancel_2y
, coalesce(c4.went, 0) as went_2y
, coalesce(c4.canceled_by_the_carrier, 0) as canceled_by_the_carrier_2y
, coalesce(c4.complicated_routes, 0) as complicated_routes_2y
, coalesce(c4.from_the_far_est, 0) as from_the_far_est_2y
, coalesce(c4.owner_of_transport, 0) as owner_of_transport_2y
, coalesce(c4.price_of_transport, 0) as price_of_transport_2y
--, 1 as test
from temp_cym as c
left join lateral (select sum(c2.count_of_requests) as count_of_requests, sum(win) as win
				  , sum(c2.cancel) as cancel, sum(c2.went) as went, sum(canceled_by_the_carrier) as canceled_by_the_carrier
			      , sum(c2.complicated_routes) as complicated_routes, sum(c2.from_the_far_est) as from_the_far_est
				  , sum(c2.owner_of_transport) as owner_of_transport, sum(c2.price_of_transport) as price_of_transport
			      from temp_car as c2
			      where c2.inn=c.inn and c2.yyyy_mm >= c.yyyy_mm_180 and c2.yyyy_mm<c.yyyy_mm
			      ) as c2 on true
left join lateral (select sum(c2.count_of_requests) as count_of_requests, sum(win) as win
				  , sum(c2.cancel) as cancel, sum(c2.went) as went, sum(canceled_by_the_carrier) as canceled_by_the_carrier
			      , sum(c2.complicated_routes) as complicated_routes, sum(c2.from_the_far_est) as from_the_far_est
			      , sum(c2.owner_of_transport) as owner_of_transport, sum(c2.price_of_transport) as price_of_transport
				  from temp_car as c2
			      where c2.inn=c.inn and c2.yyyy_mm >= c.yyyy_mm_1y and c2.yyyy_mm<c.yyyy_mm
			      group by c2.inn) as c3 on true
left join lateral (select sum(c2.count_of_requests) as count_of_requests, sum(win) as win
				  , sum(c2.cancel) as cancel, sum(c2.went) as went, sum(canceled_by_the_carrier) as canceled_by_the_carrier
			      , sum(c2.complicated_routes) as complicated_routes, sum(c2.from_the_far_est) as from_the_far_est
			      , sum(c2.owner_of_transport) as owner_of_transport, sum(c2.price_of_transport) as price_of_transport
				  from temp_car as c2
			      where c2.inn=c.inn and c2.yyyy_mm >= c.yyyy_mm_2y and c2.yyyy_mm<c.yyyy_mm
			      group by c2.inn) as c4 on true
--where /*first_winning_date is null*/  c.carrier_name = 'ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ "ФЕНИКС"' --'Серов Михаил Игоревич'
--order by inn, yyyy_mm desc
;

--select inn, /*yyyy_mm,*/ went, price_of_transport, * from temp_final where inn = '3811126641' order by inn, yyyy_mm desc
--where went=0 and price_of_transport>0 --/*first_winning_date is null*/  carrier_name = 'ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ "ФЕНИКС"' --'Серов Михаил Игоревич'  
--order by inn, yyyy_mm desc

if vh = 1 then
	drop table if exists temp_cym, temp_car;
end if;


--drop table if exists psv.t_rating_of_carries;
--create table psv.t_rating_of_carries
--as
--select *
--from temp_final;

	--do
	begin
		truncate psv.t_rating_of_carries;
		insert into psv.t_rating_of_carries
		select * from temp_final;
	exception when others
		then rollback;
	end;


end;
$procedure$
;
