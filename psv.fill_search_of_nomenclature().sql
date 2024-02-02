-- DROP PROCEDURE psv.fill_search_of_nomenclature();

CREATE OR REPLACE PROCEDURE psv.fill_search_of_nomenclature()
 LANGUAGE plpgsql
AS $procedure$
declare 
	perem_date timestamptz := (now()::date - 365)::timestamptz;
	glubina_vigruzki timestamptz := (now()::date - 400)::timestamptz;
	--perem_date timestamptz := now() - '365 days':interval;
	--perem_date timestamptz := date_trunc('day', now() - '365 days'::interval)
	i integer := 1;
begin


--drop table if exists temp_vh;
--create temp table temp_vh --для удаления использованных временных таблиц
--as
--select 1::int as enter;
	

drop table if exists temp_xpl_rwp;
create temp table temp_xpl_rwp
as
with cte as (SELECT ldh.cargo_id,
			        ldh.date,
			        row_number() OVER (PARTITION BY ldh.cargo_id ORDER BY ldh.loading_date) AS num
			 FROM xpl_new.loading_dates_history as ldh
			 WHERE ldh.is_dispatch AND ldh.date >= glubina_vigruzki)
select cargo_id, date
from cte
where num=1;
create index ci_xpl_rwp on temp_xpl_rwp(cargo_id);
--select * from temp_xpl_rwp
        

drop table if exists temp_transportation;
create temp table temp_transportation
as
with cte as (
			SELECT transportations.number,
			    transportations.status,
			    transportations.carrier_id,
			    transportations.first_driver_id,
			    transportations.second_driver_id,
			    transportations.trailer_id,
			    transportations.truck_id,
			    transportations.negotiation_id,
			    transportations.created_at,
			    transportations.voyage_id,
			    row_number() OVER (PARTITION BY transportations.number ORDER BY transportations.created_at DESC) AS num
				--select *
			    FROM xpl_new.transportations
			WHERE transportations.created_at >= glubina_vigruzki AND transportations.status <> 'CANCELED'::text)
select *
from cte
where num=1;
create index ci_transportation on temp_transportation(number);
create index ci_transportation2 on temp_transportation(negotiation_id);
--select * from temp_transportation
--86378


drop table if exists temp_waypoints;
create temp table temp_waypoints
as
SELECT vl.voyage_id,
    min(vwo.actual_date) AS fact_loading_date,
    max(vwd.actual_date) AS fact_unloading_date,
    so.cargo_id
FROM xpl_new.voyage_legs vl
LEFT JOIN xpl_new.shipping_operations so ON so.leg_id = vl.id
LEFT JOIN xpl_new.voyage_waypoints vwo ON vwo.id = vl.origin_id
LEFT JOIN xpl_new.voyage_waypoints vwd ON vwd.id = vl.destination_id
WHERE vl.created_at >= glubina_vigruzki
GROUP BY vl.voyage_id, so.cargo_id;
--select * from waypoints


drop table if exists temp_negotiation; --проверить, долго
create temp table temp_negotiation
as
with cte as (
		SELECT n.number,
		    n.id,
		    n.start_price,
		    n.status,
		    n.winner_id,
		    n.type,
		    n.tariff_type,
		    n.final_price,
		    n.customer_price,
		    nor.order_id,
		    row_number() OVER (PARTITION BY nor.order_id ORDER BY (
		        CASE
		            WHEN tra.negotiation_id IS NOT NULL THEN 'infinity'::timestamp without time zone::timestamp with time zone
		            ELSE n.created_at
		        END) DESC) AS num
		FROM xpl_new.negotiations n
		LEFT JOIN temp_transportation tra ON tra.negotiation_id = n.id --AND tra.num = 1
		LEFT JOIN xpl_new.negotiation_orders nor ON nor.negotiation_id = n.id
		WHERE n.created_at >= glubina_vigruzki)
select *
from cte
where num=1;
create index ci_negotiation on temp_negotiation(order_id);
--select count(*) from negotiation --1847770


drop table if exists temp_legacy_drivers;
create temp table temp_legacy_drivers
as
with cte as (
			SELECT legacy_drivers.id,
			    legacy_drivers.first_name,
			    legacy_drivers.middle_name,
			    legacy_drivers.last_name,
			    legacy_drivers.citizenship,
			    legacy_drivers.phone,
			    legacy_drivers.additional_phone,
			    legacy_drivers.passport_series,
			    legacy_drivers.passport_number,
			    legacy_drivers.passport_issue_by,
			    legacy_drivers.status,
			    legacy_drivers.org_id,
			    legacy_drivers.version,
			    legacy_drivers.created_user,
			    legacy_drivers.modified_user,
			    legacy_drivers.created_date,
			    legacy_drivers.modified_date,
			    legacy_drivers.passport_issued_date,
			    legacy_drivers.driver_id,
			    row_number() OVER (PARTITION BY legacy_drivers.driver_id, legacy_drivers.org_id ORDER BY legacy_drivers.created_date DESC) AS num
			FROM xpl_new.legacy_drivers
			WHERE legacy_drivers.status::text = 'READY'::text AND legacy_drivers.driver_id <> '00000000-0000-0000-0000-000000000000'::text)
select *
from cte
where num=1;
create index ci_legacy_drivers on temp_legacy_drivers(driver_id,org_id);



drop table if exists temp_cargo_items;
create temp table temp_cargo_items
as
SELECT cargo_items.cargo_id
	 , string_agg(DISTINCT cargo_items.product_name, '; '::text) AS nomenclature --проверить
     , sum(cargo_items.estimated_cost) AS cargo_price
    
FROM xpl_new.cargo_items
GROUP BY cargo_items.cargo_id;
create index ci_cargo_items on temp_cargo_items(cargo_id);



drop table if exists temp_cargo_places;
create temp table temp_cargo_places
as
SELECT cargo_places.cargo_id,
    sum(cargo_places.weight * cargo_places.quantity::numeric) AS weight,
    sum(cargo_places.volume * cargo_places.quantity::numeric) AS volume
FROM xpl_new.cargo_places
GROUP BY cargo_places.cargo_id;
create index ci_cargo_places on temp_cargo_places(cargo_id);

        

drop table if exists temp_xpl; --проверить, долго
create temp table temp_xpl
as
SELECT cp.name AS initiator_company,
            (u.first_name::text || ' '::text) || u.last_name::text AS initiator_name,
            c.number AS document_number,
            o.created_at AS created_date,
            n.number AS route_list,
                CASE
                    WHEN COALESCE(n.tariff_type, o.tariff_type)::text = 'FIXED'::text THEN 'Автотранспорт'::text
                    ELSE 'Автотранспорт (руб/час)'::text
                END AS delivery_type,
            cesc.name AS sender,
            ces.address AS sender_address,
            ces.location_name AS sender_city,
            ces.region_name AS sender_region,
            cerc.name AS recipient,
            cer.address AS recipient_address,
            cer.location_name AS recipient_city,
            cer.region_name AS recipient_region,
            cpl.weight AS cargo_weight,
            cpl.volume AS cargo_volume,
            o.created_at AS deal_created_date,
                case
	                -- tra.status приоритетнее o.status
	                WHEN tra.status = 'SHIPPING'::text THEN 'В пути'::text
                    WHEN tra.status = 'SHIPPED'::text THEN 'Доставлено'::text
                    WHEN tra.status = 'SHIPPED_AND_CONFIRMED'::text THEN 'Выполнено'::text
                    WHEN tra.status = 'DRIVER_ASSIGNED'::text THEN 'Водитель назначен'::text
                    WHEN tra.status = 'READY_FOR_ASSIGNMENT'::text THEN 'Прикрепляет ТС'::text
                    WHEN tra.status = 'CANCELED'::text THEN 'Отмена'::text
                    
                    WHEN o.status::text = 'CANCELED'::text THEN 'Отмена'::text
                    WHEN o.status::text = ANY (ARRAY['DRAFT'::character varying::text, 'ON_APPROVED'::character varying::text]) THEN 'Черновик'::text
                    WHEN o.status::text = ANY (ARRAY['ROUTING'::text, 'ROUTED'::text]) THEN 'Консолидация'::text
                    WHEN o.status::text = 'ON_CONTROL'::text THEN 'Контроль'::text
                    WHEN o.status::text = ANY (ARRAY['APPROVED'::character varying::text, 'APPROVING'::character varying::text, 'REVISING'::character varying::text, 'CREATED'::character varying::text]) THEN 'Новая'::text
                    WHEN o.status::text = 'EXPIRED'::text THEN 'Просрочено'::text
                    WHEN o.status::text = 'SHIPPED'::text THEN 'Доставлено'::text
                    WHEN o.status::text = 'DRIVER_ASSIGNED'::text THEN 'Водитель назначен'::text
                    WHEN o.status::text = 'SHIPPING'::text THEN 'В пути'::text
                    WHEN o.status::text = 'READY_FOR_ASSIGNMENT'::text THEN 'Прикрепляет ТС'::text
                    WHEN o.status::text = 'SHIPPED_AND_CONFIRMED'::text THEN 'Выполнена'::text
                    
                    WHEN (n.status::text = 'IN_PROGRESS'::text OR o.status::text = 'NEGOTIATING'::text) /*AND COALESCE(bids.bids_count, 0::bigint) = 0*/ THEN 'На редукционе'::text  /*:(*/
                    --WHEN (n.status::text = 'IN_PROGRESS'::text OR o.status::text = 'NEGOTIATING'::text) AND COALESCE(bids.bids_count, 0::bigint) > 0 THEN 'На редукционе'::text  /*:)*/
                    
                    ELSE NULL::text
                END AS status,
            NULL::text AS planned_carrier_price,
                CASE
                    WHEN n.type::text = 'TENDER'::text THEN 'Тендер'::text
                    WHEN n.type::text = 'REDUCTION'::text THEN 'Редукцион'::text
                    ELSE 'Без торгов'::text
                END AS bid_type,
            NULL::text AS canceler_type,
            NULL::text AS cancel_reason,
            car.full_name AS carrier,
            COALESCE(ces.evaluated_due_date, ces.due_date_from) + COALESCE(rusut.utc, '03:00:00'::interval) - '03:00:00'::interval AS plan_dispatch_date,
            COALESCE(w.fact_loading_date, ces.actual_due_date) + COALESCE(rusut.utc, '03:00:00'::interval) AS actual_loading_date,
            COALESCE(cer.evaluated_due_date, cer.due_date_from) + COALESCE(rusut.utc, '03:00:00'::interval) - '03:00:00'::interval AS plan_arrival_date,
            COALESCE(w.fact_unloading_date, cer.actual_due_date) + COALESCE(rusut.utc, '03:00:00'::interval) AS actual_unloading_date,
            o.planned_distance AS distance,
            tra.created_at AS route_list_date,
            ci.nomenclature,
            cp.name AS payer,
            cp.inn AS payer_inn,
            cp.kpp AS payer_kpp,
            rt.route_type,
            ''::text AS mobile,
            ''::text AS recipient_manager,
            ''::text AS recipient_phone,
            ''::text AS recipient_email,
            cesc.inn AS sender_inn, -----
            cesc.kpp AS sender_kpp, -----
            cerc.inn AS recipient_inn, -----
            cerc.kpp AS recipient_kpp, -----
            cp.inn AS initiator_inn,
            cp.kpp AS initiator_kpp,
            COALESCE(ttrs.description, trs.description) AS trailer_type,
            COALESCE(ttrs.description, trs.description) AS vehicle_type,
            rwp.date AS initial_loading_date,
            0::numeric AS faster_koef,
            (((fdr.first_name::text || ' '::text) || fdr.last_name::text) || '; '::text) || COALESCE((sdr.first_name::text || ' '::text) || sdr.last_name::text, ''::text) AS driver,  -----
            fdr.phone::text || COALESCE('; '::text || sdr.phone::text, ''::text) AS driver_phone, -----
            v.reg,
			ci.cargo_price,
            o.created_at AS filter_date,
            trl.capacity AS trailer_capacity,
            v.capacity AS vehicle_capacity,
            trl.volume AS trailer_volume,
            v.volume AS vehicle_volume,
            o.external_number AS org_type_document_number,
            COALESCE(compt.company_type, 'Внутренний'::character varying) AS company_type,
            ''::text AS route_id,
            car.inn AS carrier_inn, -----
            car.kpp AS carrier_kpp, -----
                CASE
                    WHEN o.is_external_customer THEN 'Внешний'::text
                    ELSE 'Внутренний'::text
                END AS xpl_external_customer,
            pay.name AS payer_name,
            m.name AS vehicle_model,
            o.id AS order_id,
            c.id AS guid
--select *
FROM xpl_new.cargos as c
LEFT JOIN xpl_new.orders o ON o.id = c.order_id -----
LEFT JOIN xpl.trailer_types tt ON tt.id::text = o.trailer_type_id
LEFT JOIN xpl.translations trs ON trs.id = tt.translation_id
LEFT JOIN temp_negotiation n ON n.order_id = o.id --AND n.num = 1 --условие реализовано в запросе таблицы
LEFT JOIN xpl_new.cargo_endpoints ces ON ces.id = c.origin_id
LEFT JOIN xpl_new.counterparties cesc ON cesc.id = ces.company_id
LEFT JOIN xpl_new.cargo_endpoints cer ON cer.id = c.destination_id
LEFT JOIN xpl_new.counterparties cerc ON cerc.id = cer.company_id
LEFT JOIN xpl_new.counterparties pay ON pay.id = o.payer_id
LEFT JOIN xpl_new.users u ON u.id = o.initiator_id
LEFT JOIN xpl_new.branches cp ON cp.org_id = u.org_id
LEFT JOIN temp_cargo_items ci ON ci.cargo_id = c.id
LEFT JOIN xpl_new.route_types rt ON rt.negotiation_id = n.id
--LEFT JOIN bids ON bids.negotiation_id = n.id
LEFT JOIN temp_xpl_rwp as rwp ON rwp.cargo_id = c.id --AND rwp.num = 1 --условие реализовано в запросе таблицы xpl_rwp
LEFT JOIN temp_transportation tra ON tra.number = n.number --AND tra.num = 1 --AND fdr.num = 1 --условие реализовано в запросе таблицы
LEFT JOIN temp_waypoints w ON w.cargo_id = c.id AND w.voyage_id = tra.voyage_id
LEFT JOIN xpl.trailer_types ttt ON ttt.id::text = tra.trailer_id
LEFT JOIN xpl.translations ttrs ON ttrs.id = ttt.translation_id
LEFT JOIN temp_legacy_drivers fdr ON fdr.driver_id = tra.first_driver_id AND fdr.org_id = tra.carrier_id --AND fdr.num = 1 --условие реализовано в запросе таблицы
LEFT JOIN temp_legacy_drivers sdr ON sdr.driver_id = tra.second_driver_id AND sdr.org_id = tra.carrier_id --AND sdr.num = 1 --условие реализовано в запросе таблицы
LEFT JOIN xpl_new.branches car ON car.org_id = tra.carrier_id
LEFT JOIN dict.russian_utc rusut ON rusut.region::text = ces.region_name
LEFT JOIN dict.russian_utc rurut ON rurut.region::text = cer.region_name
LEFT JOIN dict.companies_types compt ON compt.inn::text = cp.inn::text AND compt.kpp::text = cp.kpp::text
LEFT JOIN xpl.vehicles v ON v.id::text = tra.truck_id
LEFT JOIN xpl.models m ON m.id = v.model_id
LEFT JOIN xpl.trailers trl ON trl.id::text = tra.trailer_id
LEFT JOIN temp_cargo_places cpl ON cpl.cargo_id = c.id
--LEFT JOIN xpl_cost ON xpl_cost.order_id = o.id
WHERE o.created_at >= perem_date  AND COALESCE(car.inn, ''::character varying)::text <> '7706452837'::text --salair trans
and left(c.number,3) not in ('ABS','ABL','ALS','WMS'); --Рома подсказал, баги
--WINDOW price AS (PARTITION BY c.number);
--select * from temp_xpl where document_number = 'LLS399682'
--------------------------------------------------------------------------------------------------------------------------------------------------
 
	if i = 1 then 
		drop table if exists temp_xpl_rwp, temp_transportation, temp_waypoints, temp_negotiation, temp_legacy_drivers, temp_cargo_items, temp_cargo_places;
	end if;

--------------------------------------------------------------------------------------------------------------------------------------------------
drop table if exists temp_dov;
create temp table temp_dov
as
select distinct zayavka_na_transport
from upp_work.doverennost
where "data" > glubina_vigruzki and pometka_udaleniya = false;
create index ci_temp_dov on temp_dov(zayavka_na_transport);
--select * from temp_dov where zayavka_na_transport='800380f3-e0d8-11ec-8106-00505601119b'


drop table if exists temp_route_lists; --проверить, долго
create temp table temp_route_lists
as
SELECT rl.guid,
    rl.route_list_number,
    rl.datetime,
    rl.carrier_price,
    rl.customer_price,
    rl.plan_loading_date,
    rlr.fact_loading_date,
    rl.plan_unloading_date,
    rlr.fact_unloading_date,
    rlr.distance,
    rlr.request,
    rl.carrier,
    rl.delivery_type,
    rlr.carrier_payer,
    rl.request_status,
    rlr.payer,
    rlr.line_number,
    rl.consolidation_warehouse,
    cw."name",
    rl.on_warehouse,
    rl.driver,
    rl.driver_info,
    rl.driver_phone_number ,
    rl.vehicle_info,
    rl.vehicle,
    rlr.loading_address AS rl_loading_address,
    rlr.unloading_address AS rl_unloading_address,
        CASE
            WHEN rl.request_status = '687015be-bd65-4e9a-a184-c21a3a836ee8'::uuid THEN row_number() OVER (PARTITION BY rlr.request ORDER BY rl.datetime, rl.plan_loading_date)
            ELSE 0::bigint
        END AS num,
    row_number() OVER (PARTITION BY rl.route_list_number, rlr.request ORDER BY rl.datetime DESC) AS last_route_list
    , rl.consolidation_warehouse as wh
--select *
FROM salair_upp.route_list_requests rlr 
LEFT JOIN salair_upp.route_lists rl ON rl.guid = rlr.route_list
LEFT JOIN salair_upp.consolidation_warehouses cw ON cw.guid = rl.consolidation_warehouse
WHERE rl.datetime>= glubina_vigruzki and rl.is_posted = true AND rl.is_deleted = false;
--select * from temp_route_lists where request='3b890b9c-b8bf-4a23-8bd6-2b5611ffab75' --wh a0c4e9b8-5056-11ed-b3e7-3a68dd0e0ca7



drop table if exists temp_history; --проверить, долго, огромная таблица
create temp table temp_history
as
with cte as (SELECT rh.request,
			     rh.status,
			     rh.type,
			     rh.datetime,
			     rh.fact_date,
			     rh.actual_set_status_date
			     ,
			     row_number() OVER (PARTITION BY rh.request
			     					ORDER BY 
			     							case when rh.fact_date is null or rh.fact_date<'2011-01-01' then 
			     									  case when rh.actual_set_status_date is null or rh.actual_set_status_date<'2011-01-01' then rh.datetime else rh.actual_set_status_date end
			     								 else rh.fact_date
			     							end 
			     								  
			     								 DESC) AS num
--			 , row_number() OVER (PARTITION BY rh.request ORDER BY fact_date desc) as rn_fd
--		     , row_number() OVER (PARTITION BY rh.request ORDER BY actual_set_status_date desc nulls last) as rn_assd
--		     , case when row_number() OVER (PARTITION BY rh.request ORDER BY fact_date desc)=1 and (fact_date is null and fact_date < '2011-01-01') then 1 else 0 end as rn_priznak
			 FROM salair_upp.request_status_history rh
			 WHERE --rh.datetime >=  (now()::date - 365)::timestamptz
			 	   rh.request='3b890b9c-b8bf-4a23-8bd6-2b5611ffab75'
			 	   )
select *
from cte
where num=1;
--where (rn_fd=1 and rn_priznak=0) or (rn_assd=1 and rn_priznak=1);
create index ci_history on temp_history(request);
--select * from temp_history where request='10035e69-e5eb-496b-a778-9994f8930036'

--select rsh.*, rss."name" 
--from temp_history rsh
--LEFT JOIN salair_upp.request_status rss ON rss.guid = rsh.status
--where request='1e9f85e0-85b5-11ec-964f-3a68dd4bd4bf'

drop table if exists temp_upp; --проверить, долго
create temp table temp_upp
as
SELECT r.guid::text AS guid,
COALESCE(pay.name, rpay.name) AS payer,
r.request_number,
r.datetime,
rl.route_list_number,
rl.datetime AS route_list_date,
COALESCE(dt.name, dtr.name) AS delivery_type,
rl.driver_phone_number,
sen.name AS sender,
r.loading_address,
gs.city AS sender_city,
gs.region AS sender_region,
gs.country AS sender_country,
rec.name AS recipient,
rec.inn AS recipient_inn,
rec.kpp AS recipient_kpp,
r.unloading_address,
gr.city AS recipient_city,
gr.region AS recipient_region,
gr.country AS recipient_country,
r.weight,
r.volume,
case when rs.name in ('Доставлен') then
		  rs.name
	 when rss.name in ('Доставлен') then 
		  rss.name
	 when rs.name in ('Отменено заказчиком') then
		  rs.name
	 when rss.name in ('Отменено заказчиком') then 
		  rss.name
	 else
		  COALESCE(rs.name, rss.name)
end AS status,

rl.carrier_price /
    CASE
        WHEN pt.type = '826382f2-e959-11e6-80e9-00505601119a'::uuid OR gs.country::text <> 'RU'::text OR gr.country::text <> 'RU'::text THEN 1::numeric
        ELSE 1.2
    END AS carrier_price,
rl.customer_price /
    CASE
        WHEN gs.country::text <> 'RU'::text OR gr.country::text <> 'RU'::text THEN 1::numeric
        ELSE 1.2
    END AS customer_price,
r.customer_price / count(*) OVER (PARTITION BY r.guid)::numeric /
    CASE
        WHEN gs.country::text <> 'RU'::text OR gr.country::text <> 'RU'::text THEN 1::numeric
        ELSE 1.2
    END AS r_customer_price,
car.full_name AS carrier, -----
car.inn AS carrier_inn, -----
car.kpp AS carrier_kpp, -----
rl.plan_loading_date,
rl.fact_loading_date,
rl.plan_unloading_date,
rl.fact_unloading_date,
rl.distance,
    CASE
        WHEN r.creation_place = 'a24987c8-7b76-471d-a977-e9d03c86206b'::uuid THEN 'УПП'::text
        ELSE 'xPlanet'::text
    END AS creation_place,
r.recipient_phone,
r.recipient_email,
r.document_manager_comment,
first_value(gs.region) OVER (PARTITION BY (COALESCE(rl.route_list_number, r.request_number)) ORDER BY rl.line_number) AS region_id,
first_value(gs.city) OVER (PARTITION BY (COALESCE(rl.route_list_number, r.request_number)) ORDER BY rl.line_number) AS city_id,
first_value(gr.region) OVER (PARTITION BY (COALESCE(rl.route_list_number, r.request_number)) ORDER BY rl.line_number) AS r_region_id,
first_value(gr.city) OVER (PARTITION BY (COALESCE(rl.route_list_number, r.request_number)) ORDER BY rl.line_number) AS r_city_id,
first_value(
    CASE
        WHEN rl.plan_loading_date < r.datetime THEN r.datetime + '1 day'::interval
        ELSE rl.plan_loading_date
    END) OVER (PARTITION BY (COALESCE(rl.route_list_number, r.request_number)) ORDER BY rl.line_number) AS dispatch_id,
sen.inn AS sender_inn,
sen.kpp AS sender_kpp,
u.name AS responsible_user,
rtp.type AS route_type,
    CASE
        WHEN r.trawl = true THEN 'Трал'::character varying
        ELSE trtp.name
    END AS trailer_type,
COALESCE(pay.inn, rpay.inn) AS initiator_inn,
COALESCE(pay.kpp, rpay.kpp) AS initiator_kpp,
rl.name::text AS consolidation_warehouse,
rl.on_warehouse,
max(
    CASE
        WHEN rs.name::text = 'Консолидация'::text THEN 1
        ELSE 0
    END) OVER (PARTITION BY r.guid) AS is_consolidation,
row_number() OVER (PARTITION BY r.guid ORDER BY (COALESCE(rl.plan_loading_date, r.datetime + '1 day'::interval)) DESC) AS warehouse_number,
    CASE
        WHEN r.weight <= 0::numeric THEN 0.1
        ELSE r.weight
    END / sum(
    CASE
        WHEN r.weight <= 0::numeric THEN 0.1
        ELSE r.weight
    END) OVER (PARTITION BY rl.guid) AS part_in_rl,
ind.name AS driver,
rl.driver_info,
pay.inn AS payer_inn,
pay.kpp AS payer_kpp,
COALESCE(v.number, rl.vehicle_info) AS vehicle_number,
rl.num AS route_number,
r.datetime AS filter_date,
v.name AS vehicle_model,
--s.sale_number,
    CASE
        WHEN rl.consolidation_warehouse <> '00000000-0000-0000-0000-000000000000'::uuid THEN 1
        ELSE 0
    END AS transit,
rl.rl_loading_address,
rl.rl_unloading_address,

rsh.datetime AS status_date,
r.fourpl,
    CASE
        WHEN r.cargo_width >= r.cargo_hieght AND r.cargo_width >= r.cargo_length THEN r.cargo_width
        WHEN r.cargo_hieght >= r.cargo_width AND r.cargo_hieght >= r.cargo_length THEN r.cargo_hieght
        WHEN r.cargo_length >= r.cargo_hieght AND r.cargo_width >= r.cargo_width THEN r.cargo_length
        ELSE NULL::numeric
    END AS max_dimension,
rl.line_number,
ptp.name AS package_type,
max(
    CASE
        WHEN (r.cargo_width >= 13.61 OR r.cargo_hieght >= 13.61 OR r.cargo_length >= 13.61) AND r.weight < 40::numeric THEN 40::numeric
        WHEN (r.cargo_width >= 8.01 AND r.cargo_width < 13.61 OR r.cargo_hieght >= 8.01 AND r.cargo_hieght < 13.61 OR r.cargo_length >= 8.01 AND r.cargo_length < 13.61) AND r.weight < 20::numeric THEN 20::numeric
        WHEN (r.cargo_width >= 6.01 AND r.cargo_width < 8.01 OR r.cargo_hieght >= 6.01 AND r.cargo_hieght < 8.01 OR r.cargo_length >= 6.01 AND r.cargo_length < 8.01) AND r.weight < 10::numeric THEN 10::numeric
        WHEN (r.cargo_width >= 5.01 AND r.cargo_width < 6.01 OR r.cargo_hieght >= 5.01 AND r.cargo_hieght < 6.01 OR r.cargo_length >= 5.01 AND r.cargo_length < 6.01) AND r.weight < 5::numeric THEN 5::numeric
        WHEN (r.cargo_width >= 4.01 AND r.cargo_width < 5.01 OR r.cargo_hieght >= 4.01 AND r.cargo_hieght < 5.01 OR r.cargo_length >= 4.01 AND r.cargo_length < 5.01) AND r.weight < 3::numeric THEN 3::numeric
        WHEN (r.cargo_width < 4.01 OR r.cargo_hieght < 4.01 OR r.cargo_length < 4.01) AND r.weight < 0.001 THEN 0.001
        ELSE r.weight
    END) OVER (PARTITION BY rl.route_list_number) AS rl_weight_max,
    CASE
        WHEN (r.cargo_width >= 13.61 OR r.cargo_hieght >= 13.61 OR r.cargo_length >= 13.61) AND r.weight < 40::numeric THEN 40::numeric
        WHEN (r.cargo_width >= 8.01 AND r.cargo_width < 13.61 OR r.cargo_hieght >= 8.01 AND r.cargo_hieght < 13.61 OR r.cargo_length >= 8.01 AND r.cargo_length < 13.61) AND r.weight < 20::numeric THEN 20::numeric
        WHEN (r.cargo_width >= 6.01 AND r.cargo_width < 8.01 OR r.cargo_hieght >= 6.01 AND r.cargo_hieght < 8.01 OR r.cargo_length >= 6.01 AND r.cargo_length < 8.01) AND r.weight < 10::numeric THEN 10::numeric
        WHEN (r.cargo_width >= 5.01 AND r.cargo_width < 6.01 OR r.cargo_hieght >= 5.01 AND r.cargo_hieght < 6.01 OR r.cargo_length >= 5.01 AND r.cargo_length < 6.01) AND r.weight < 5::numeric THEN 5::numeric
        WHEN (r.cargo_width >= 4.01 AND r.cargo_width < 5.01 OR r.cargo_hieght >= 4.01 AND r.cargo_hieght < 5.01 OR r.cargo_length >= 4.01 AND r.cargo_length < 5.01) AND r.weight < 3::numeric THEN 3::numeric
        WHEN (r.cargo_width < 4.01 OR r.cargo_hieght < 4.01 OR r.cargo_length < 4.01) AND r.weight < 0.001 THEN 0.001
        ELSE r.weight
    END AS weight_by_dimension
, rsh.actual_set_status_date
, coalesce(case when dov.zayavka_na_transport is null then 0 else 1 end, 0) as doverennost_10
, rl.consolidation_warehouse as cwh
--select *
FROM salair_upp.requests r --where request_number='WMS050281'
LEFT JOIN temp_route_lists rl ON rl.request = r.guid AND rl.last_route_list = 1
LEFT JOIN geo.addresses gs ON gs.address_upp::text = r.loading_address::text
LEFT JOIN geo.addresses gr ON gr.address_upp::text = r.unloading_address::text
LEFT JOIN salair_upp.counterparties sen ON sen.guid = r.sender
LEFT JOIN salair_upp.counterparties rec ON rec.guid = r.recipient
LEFT JOIN salair_upp.counterparties pay ON pay.guid = rl.carrier_payer
LEFT JOIN salair_upp.counterparties cp ON cp.guid = rl.payer
LEFT JOIN salair_upp.counterparties car ON car.guid = rl.carrier
LEFT JOIN salair_upp.counterparties rpay ON rpay.guid = r.payer
LEFT JOIN salair_upp.request_status rs ON rs.guid = rl.request_status
LEFT JOIN salair_upp.delivery_type dt ON dt.guid = rl.delivery_type
LEFT JOIN salair_upp.delivery_type dtr ON dtr.guid = r.delivery_type
LEFT JOIN salair_upp.counterparty_contracts cc ON cc.guid = car.main_contract
LEFT JOIN salair_upp.price_types pt ON pt.guid = cc.price_type
--LEFT JOIN salair_upp.consolidation_warehouses cw ON cw.guid = rl.consolidation_warehouse
LEFT JOIN salair_upp.users u ON u.guid = r.responsible_manager
LEFT JOIN salair_upp.route_types rtp ON rtp.guid = rl.guid
LEFT JOIN salair_upp.trailer_types trtp ON trtp.guid = r.trailer_type
LEFT JOIN salair_upp.individuals ind ON ind.guid = rl.driver
LEFT JOIN temp_history rsh ON rsh.request = r.guid --AND rsh.num = 1
LEFT JOIN salair_upp.request_status rss ON rss.guid = rsh.status
LEFT JOIN salair_upp.vehicles v ON v.guid = rl.vehicle
LEFT JOIN salair_upp.package_types ptp ON ptp.guid = r.package_type
left join temp_dov as dov on dov.zayavka_na_transport = r.guid 
WHERE r.datetime >= perem_date AND r.is_deleted = false AND r.is_posted = true;
--select * from temp_upp where request_number='LLS399682' --guid='3b890b9c-b8bf-4a23-8bd6-2b5611ffab75' --ALS047931
       
--------------------------------------------------------------------------------------------------------------------------------------------------

	if i = 1 then
		drop table if exists temp_route_lists, temp_history;
	end if;

--------------------------------------------------------------------------------------------------------------------------------------------------
        
drop table if exists temp_fin;
create temp table temp_fin
as
SELECT COALESCE(xpl.document_number, upp.request_number::text, 'Нет данных'::text) AS request_number/*НомерЗаявки*/, 

    COALESCE(xpl.created_date, upp.datetime::timestamp with time zone, '2000-01-01 00:00:00+03'::timestamp with time zone)::timestamp without time zone AS request_date/*ДатаЗаявки*/,
    COALESCE(
        CASE
            WHEN COALESCE(upp.creation_place, 'xPlanet'::text) = 'УПП'::text THEN upp.route_list_number::text
            ELSE COALESCE(xpl.route_list, upp.route_list_number::text)
        END) AS route_list_number/*НомерМЛ*/,
    COALESCE(upp.route_list_date::timestamp with time zone, xpl.route_list_date) AS route_list_date/*ДатаМЛ*/,
    COALESCE(xpl.sender_city, upp.sender_city::text, 'Нет данных'::text) AS sender_city/*ГородОтправиления*/,
    COALESCE(xpl.sender, upp.sender::text, 'Нет данных'::text) AS sender_organization/*ОтправительЮрЛ*/,
    COALESCE(xpl.recipient_city, upp.recipient_city::text, 'Нет данных'::text) AS recipient_city/*ГородПолучения*/,
    COALESCE(xpl.recipient, upp.recipient::text, 'Нет данных'::text) AS recipient_organization/*ПолучательЮрЛ*/,
    CASE
        WHEN upp.is_consolidation = 1 THEN
        CASE
            WHEN upp.status::text = 'Консолидация'::text THEN upp.status::text || upp.route_number
            ELSE 'Консолидация - со склада - '::text || upp.status::text
        END::character varying
        ELSE upp.status
    END AS status_upp/*Место нахождения*/,
    COALESCE(xpl.delivery_type, upp.delivery_type::text, 'Нет данных'::text) AS delivery_type,
    COALESCE(xpl.plan_dispatch_date::timestamp with time zone, xpl.deal_created_date + '1 day'::interval, upp.plan_loading_date::timestamp with time zone, (upp.datetime + '1 day'::interval)::timestamp with time zone)::timestamp without time zone AS plan_loading_date/*ДатаПогрузкиПлан*/,
    COALESCE(xpl.actual_loading_date, upp.fact_loading_date) AS fact_loading_date/*ДатаПогрузкиФакт*/,
    COALESCE(xpl.plan_arrival_date, upp.plan_unloading_date) AS plan_unloading_date/*ДатаПланРазгрузки*/,
    COALESCE(xpl.actual_unloading_date, upp.fact_unloading_date) AS fact_unloading_date/*ДатаФактРазгрузки*/,
    COALESCE(upp.rl_loading_address, xpl.sender_address::character varying) AS rl_loading_address,
    COALESCE(upp.rl_unloading_address, xpl.recipient_address::character varying) AS rl_unloading_address,
    rank() OVER (PARTITION BY (COALESCE(xpl.route_list, upp.route_list_number::text, xpl.document_number, upp.request_number::text)) ORDER BY (COALESCE(xpl.plan_dispatch_date, upp.plan_loading_date))) AS route_number,
    upp.status_date,
    upp.consolidation_warehouse/*Для определения места нахождения*/,
    upp.on_warehouse,
    COALESCE(xpl.guid, upp.guid) AS guid,
    upp.is_consolidation,

	--max(
	case when upp.status is not null
		 	then case when xpl.status = 'Отмена'
		 				   then xpl.status 
		 				   else upp.status::text
		 	     end
		 else 
		 	xpl.status 
	end as final_status
    
    ,xpl.status as xpl_status
    ,upp.status as upp_status
    ,COALESCE(cmp.alt_name, xpl.payer_name::character varying, upp.payer) AS payer
    ,cmp.main     
    ,case when xpl.status='Отмена' then 1 else 0 end as xpl --для условия: если в xpl статус ОТМЕНА, то учитываем только него. Делаю этот признак, что оставить строчки с =1
    , upp.doverennost_10
    , upp.cwh
    , COALESCE(xpl.driver, upp.driver::text) AS driver
    , COALESCE(xpl.driver_phone, upp.driver_phone_number::text) AS driver_phone
	, COALESCE(xpl.carrier, upp.carrier) as carrier 
    , coalesce(xpl.carrier_inn, upp.carrier_inn) as carrier_inn
    , coalesce(xpl.carrier_kpp, upp.carrier_kpp) as carrier_kpp
    
FROM temp_xpl as xpl
FULL JOIN temp_upp as upp ON upp.request_number::text = xpl.document_number AND coalesce(upp.route_list_number::text, '') = coalesce(xpl.route_list, '')
LEFT JOIN salair_upp.company cmp ON cmp.inn::text = COALESCE(xpl.payer_inn, upp.initiator_inn)::text AND cmp.kpp::text = COALESCE(xpl.payer_kpp, upp.initiator_kpp)::text

left join psv.t_search_of_request__requests_for_exclusion as rfe on rfe.request_number = COALESCE(xpl.document_number, upp.request_number::text, 'Нет данных'::text) -- хардкодинг: исключаем грузы, согласованные с Сергеем Номоконовым

WHERE COALESCE(xpl.filter_date, upp.filter_date::timestamptz) >= perem_date
and rfe.request_number is null -- хардкодинг: исключаем грузы, согласованные с Сергеем Номоконовым
WINDOW weight_price AS (PARTITION BY (COALESCE(upp.route_list_number, xpl.route_list::character varying, xpl.document_number::character varying, upp.request_number)))
--ORDER BY (COALESCE(xpl.route_list, upp.route_list_number::text, 'Нет данных'::text))
--  		 , (COALESCE(xpl.plan_dispatch_date::timestamptz, xpl.deal_created_date + '1 day'::interval
--  		 , upp.plan_loading_date::timestamptz
--  		 , (upp.datetime + '1 day'::interval)::timestamptz))
 ;
create index ci_tf on temp_fin(request_number,route_list_number,rl_loading_address,rl_unloading_address);
--select * from temp_fin where request_number='LLS399682' --10035e69-e5eb-496b-a778-9994f8930036
----------  		
delete from temp_fin --приоритет заявка из xpl со статусом 'Выполнена','Доставлен','Доставлено'
where request_number in (select distinct request_number from temp_fin where final_status in ('Выполнена','Доставлен','Доставлено')) and final_status not in ('Выполнена','Доставлен','Доставлено');
----------  		
delete from temp_fin --приоритет заявка из xpl со статусом ОТМЕНА
where request_number in (select distinct request_number from temp_fin where xpl=1) and xpl=0;
----------	
-- удаляет заявки, у которых нет МЛ и дат	
--delete from temp_fin --приоритет заявка из xpl со статусом ОТМЕНА
--where temp_fin.route_list_number is null and temp_fin.rl_loading_address is null and temp_fin.rl_unloading_address is null
----------
--оставляет заявки, у которых нет МЛ и дат, но у которых всего одна запись; если записей
delete from temp_fin --приоритет заявка из xpl со статусом ОТМЕНА
where request_number in (select request_number
							    from temp_fin as tf
							    group by request_number
							    having count(*)>1)
	  and temp_fin.route_list_number is null and temp_fin.rl_loading_address is null and temp_fin.rl_unloading_address is null
	;
----------


drop table if exists temp_fin2;
create temp table temp_fin2
as
select *
, row_number() over (partition by request_number, rl_loading_address order by plan_loading_date desc nulls last, plan_unloading_date desc nulls last, route_list_date desc nulls last) as rn_count -- для посчета в следующем запросе кол-ва уникальных отправок из разных складов; будет считать один раз склад, который отправил одну заявку двумя и более рейсами
from temp_fin;
--select * from temp_fin2 where request_number='ФЗС014047'
--------------------------------------------------------------------------------------------------------------------------------------------------

	if i = 1 then
		drop table if exists temp_xpl, temp_upp, temp_fin;
	end if;

--------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------------------------------
drop table if exists temp_nomen;
create temp table temp_nomen
as
select distinct rn.request
, rn.nomenclature
, n."name"::text as "name"
from salair_upp.request_nomenclature rn 
join  salair_upp.nomenclature n on n.guid = rn.nomenclature and n.is_deleted = false
--group by rn.request
;
create index ci_nomen on temp_nomen(request);
--select distinct * from temp_nomen --1240625


drop table if exists temp_nomen_st;
create temp table temp_nomen_st
as
select distinct rn.request
, string_agg(n."name"::text, ', ') as "name"
from temp_nomen rn 
join  salair_upp.nomenclature n on n.guid = rn.nomenclature and n.is_deleted = false
group by rn.request
;
create index ci_nomen_st on temp_nomen_st(request);
--select distinct * from temp_nomen --1240625


drop table if exists temp_final_;
create temp table temp_final_
as
with cte as (
			select *
			--, row_number() over (partition by request_number order by coalesce(route_list_date,'2001-01-01') desc/*request_number*/) as rn
			
			, row_number() over (partition by request_number order by plan_loading_date desc nulls last, plan_unloading_date desc nulls last, route_list_date desc nulls last) as rn
			
--			, case when final_status in ('Выполнена','Доставлен','Доставлено') then 1 else 0 end as rn_vipolneno
--			, max(case when final_status in ('Выполнена','Доставлен','Доставлено') then 1 else 0 end) over (partition by request_number) as rn_vpln_max
			
			, min(fact_loading_date) over (partition by request_number ) as First_Fact_Loading_Date
			, min(plan_loading_date) over (partition by request_number) as First_plan_Loading_Date
			
			--, count(*) over (partition by request_number ) as count_point -- количество движений заявки
			, sum(case when rn_count=1 then 1 else 0 end) over (partition by request_number ) as count_point -- количество движений заявки - альтернативный подсчет; начинается расчетом rn_count в предыдущем запросе
			
			--, rank() over (partition by request_number , rl_loading_address/*sender_city*/) as rank_sender -- вычисляем ранги для случаев, когда из одной точки заявка пошла двумя и более рейсами (маршрутными листами) - отключил, т. к. включил альтернативный подсчет кол-ва уникальных движений
			
			, lag(fact_unloading_date,-1) over (partition by request_number order by plan_loading_date desc nulls last, plan_unloading_date desc nulls last, route_list_date desc nulls last) as pred_fact_unloading_date
			, lag(plan_unloading_date,-1) over (partition by request_number order by plan_loading_date desc nulls last, plan_unloading_date desc nulls last, route_list_date desc nulls last) as pred_plan_unloading_date
			, lag(consolidation_warehouse,-1) over (partition by request_number order by plan_loading_date desc nulls last, plan_unloading_date desc nulls last, route_list_date desc nulls last) as pred_consolidation_warehouse
			from temp_fin2
			--where request_number='LLS399682' --5fd845b9-c480-11ec-8106-00505601119b
			)
  --, cte2 as (
  			 select request_number, request_date, route_list_number, route_list_date
			 , sender_city, sender_organization, recipient_city, recipient_organization, status_upp, delivery_type
			 , payer, main
			 , plan_loading_date, fact_loading_date, plan_unloading_date, fact_unloading_date
			 , pred_plan_unloading_date , pred_fact_unloading_date
			 , rl_loading_address, rl_unloading_address, status_date
			 , consolidation_warehouse, on_warehouse
			 , final_status, xpl_status, upp_status
			 , first_fact_loading_date
			 , case when final_status in ('Новая','В работе','Отправлен на доработку','На корректировке'
										 ,'На согласовании','Не согласовано','Согласовано','На оценке','На редукционе'
										 ,'Водитель назначен','Просрочено','Прикрепляет ТС')
						 then case when count_point>1 --and rank_sender>1 - отключил rank_sender>1, т. к. включил альтернативный подсчет кол-ва уникальных движений
				   					    then /*'На складе консолидации - ' ||*/ coalesce(consolidation_warehouse,pred_consolidation_warehouse)
				   					    else /*'В городе отправки - ' ||*/ sender_city
				   			  end --/*'Ждет забора у отправителя - ' ||*/ sender_organization
				    when final_status in ('Отменено заказчиком','Отмена')
						 then 'Заявка отменена'
				    when final_status in ('Отгружен','В пути')
				   		 then /*'в пути из ' ||*/ upper(sender_city) || ' в ' || upper(recipient_city)
				   		
				    when final_status = 'Консолидация' and route_list_number is null
				   		 then case when count_point>1 --and rank_sender>1 -- отключил rank_sender>1, т. к. включил альтернативный подсчет кол-ва уникальных движений
				   					    then /*'На складе консолидации - ' ||*/ coalesce(consolidation_warehouse,pred_consolidation_warehouse)
				   					    else /*'В городе отправки - ' ||*/ sender_city
				   			  end
				    when final_status = 'Консолидация' and (fact_loading_date is not null or fact_loading_date > '2011-01-01')
				   									   and (on_warehouse=false or on_warehouse is null)
				   									   and (fact_unloading_date is null or fact_unloading_date < '2011-01-01')
				   		 then /*'в пути из ' ||*/ upper(sender_city) || ' в ' || upper(recipient_city)
				    when final_status = 'Консолидация' and (on_warehouse=true or (fact_unloading_date is not null or fact_unloading_date > '2011-01-01'))
				   		 then /*'На складе консолидации - ' ||*/ /*coalesce(*/consolidation_warehouse/*,pred_consolidation_warehouse)*/
				    when final_status = 'Консолидация' and (fact_loading_date is null or fact_loading_date<'2011-01-01')
				   									   and (on_warehouse=false or on_warehouse is null)
				   									   and (fact_unloading_date is null and fact_unloading_date<'2011-01-01')
				   		 then /*'в пути из ' ||*/ upper(sender_city) || ' в ' || upper(recipient_city)
				   		
			  	    when final_status in ('Выполнена','Доставлен','Доставлено')
			  	   		 then /*'В конечном пункте - ' ||*/ recipient_city
			  	   
			  	    when final_status is null and plan_loading_date is not null
			  	   		 and fact_loading_date is null and (plan_unloading_date is null or plan_unloading_date<'2011-01-01') and (fact_unloading_date is null or fact_unloading_date<'2011-01-01')
			  	   		 then case when count_point>1 --and rank_sender>1 -- отключил rank_sender>1, т. к. включил альтернативный подсчет кол-ва уникальных движений
				   					    then /*'На складе консолидации - ' ||*/ coalesce(consolidation_warehouse,pred_consolidation_warehouse)
				   					    else /*'В городе отправки - ' ||*/ sender_city
				   			  end
				    when final_status is null and plan_unloading_date is not null
			  	   		 and fact_loading_date is null and plan_loading_date is null and fact_unloading_date is null
			  	   		 then /*'в пути из ' ||*/ upper(sender_city) || ' в ' || upper(recipient_city)
				    else
				   		 'Не опред.'
			   end as "location"
			  
			 ,   case when final_status in ('Выполнена','Доставлен','Доставлено') then 
						 case when first_fact_loading_date>'2011-01-01' then 
							 	 (extract(epoch from case when fact_unloading_date is null or fact_unloading_date<'2011-01-01' then plan_unloading_date::timestamptz else fact_unloading_date::timestamptz end/*::timestamptz*/-first_fact_loading_date::date::timestamptz)/60./60./24.)::numeric(10,1)
							  else
					 			 (extract(epoch from case when fact_unloading_date is null or fact_unloading_date<'2011-01-01' then plan_unloading_date::timestamptz else fact_unloading_date::timestamptz end/*::timestamptz*/-request_date::date::timestamptz)/60./60./24.)::numeric(10,1)	
					 	 end
					  else 
					 	 case when first_fact_loading_date>'2011-01-01' then
					 			 (extract(epoch from now()::timestamptz-first_fact_loading_date::date::timestamptz)/60./60./24.)::numeric(10,1)
					 		  else
					 	  		 (extract(epoch from now()::timestamptz-request_date::date::timestamptz)/60./60./24.)::numeric(10,1)
					 	 end
			 	 end as longest
			  
			 , n."name" as nomenclatura
				   --
			 , case when route_list_number is null and count_point=1 and doverennost_10=1 and final_status not in ('Отменено заказчиком','Отмена') then 
			 			 'проблема при погрузке'
				 	when final_status in ('Новая','В работе','Отправлен на доработку','На корректировке'
										 ,'На согласовании','Не согласовано','Согласовано','На оценке','На редукционе'
										 ,'Водитель назначен','Просрочено','Прикрепляет ТС')
						 then case when count_point>1 --and rank_sender>1 -- отключил rank_sender>1, т. к. включил альтернативный подсчет кол-ва уникальных движений
				   					    then 'на складе консолидации'
				   					    else 'в начальном пункте'
				   			  end --'ждет забора у отправителя'
				   --
				    when final_status in ('Отменено заказчиком','Отмена')
						 then 'заявка отменена'
				   --
				    when final_status in ('Отгружен','В пути')
				   		 then 'в пути до потребителя'
				   		
				    when final_status = 'Консолидация' and route_list_number is null
				   		 then case when count_point>1 --and rank_sender>1 -- отключил rank_sender>1, т. к. включил альтернативный подсчет кол-ва уникальных движений
				   					    then 'на складе консолидации'
				   					    else 'в начальном пункте'
				   			  end
				    when final_status = 'Консолидация' and (fact_loading_date is not null or fact_loading_date>'2011-01-01')
				   									   and (on_warehouse=false or on_warehouse is null)
				   									   and (fact_unloading_date is null or fact_unloading_date<'2011-01-01')
				   		 then 'в пути до консолидации'
				   --
				    when final_status = 'Консолидация' and (on_warehouse=true or (fact_unloading_date is not null or fact_unloading_date>'2011-01-01'))
				   		 then 'на складе консолидации'
				   		
				    when final_status = 'Консолидация' and (fact_loading_date is null or fact_loading_date<'2011-01-01')
				   									   and (on_warehouse=false or on_warehouse is null)
				   									   and (fact_unloading_date is null or fact_unloading_date<'2011-01-01')
				   		 then 'в пути до консолидации'
				   --
			  	    when final_status in ('Выполнена','Доставлен','Доставлено')
			  	   		 then 'доставлено'
			  	   --
			  	    when final_status is null and plan_loading_date is not null
			  	   		 and fact_loading_date is null and plan_unloading_date is null and fact_unloading_date is null
			  	   		 then case when count_point>1 --and rank_sender>1 -- отключил rank_sender>1, т. к. включил альтернативный подсчет кол-ва уникальных движений
				   					    then 'на складе консолидации'
				   					    else 'в начальном пункте'
				   			  end
				   			 
				    when final_status is null and plan_unloading_date is not null
			  	   		 and fact_loading_date is null and plan_loading_date is null and fact_unloading_date is null
			  	   		 then 'в пути до потребителя'
				    else
				   		 'не опред.'
			   end as location_short
			 , pred_consolidation_warehouse
			 , driver
		     , driver_phone
			 , carrier 
		     , carrier_inn
		     , carrier_kpp
			 --, row_number() over (partition by request_number, rn_vipolneno order by plan_loading_date desc nulls last, plan_unloading_date desc nulls last, route_list_date desc nulls last) as rn_rn
			 , count_point
			 , c.guid
			 from cte as c
			 left join temp_nomen_st as n on n.request::text=c.guid
			 where rn=1--(rn=1 and rn_vpln_max=0) or rn_vipolneno=1
			 --)
;

--select *
--from cte2
--where rn_rn=1

--order by 1
--select * from temp_final_ where request_number='BZS309060' 'WMS026842'


------------------------------------------------------
	if i = 1 then
		drop table if exists temp_fin2;
	end if;
------------------------------------------------------


drop table if exists temp_final_st;
create temp table temp_final_st
as
with cte as (
			select *
			, 
			  case when location_short='на складе консолидации'
			  
				   		then case when final_status='Консолидация' then
				   			 	  		(extract(epoch from now()::timestamptz - case when fact_unloading_date is null or fact_unloading_date<'2011-01-01' then plan_unloading_date::timestamptz else fact_unloading_date end::timestamptz)/60./60./24.)::numeric(10,1)
			  					  else 
			  					  		(extract(epoch from now()::timestamptz - case when pred_fact_unloading_date is null or pred_fact_unloading_date<'2011-01-01' then pred_plan_unloading_date::timestamptz else pred_fact_unloading_date end::timestamptz)/60./60./24.)::numeric(10,1)
			  				 end
			  end  as longest_on_consolidation
			  
			, case when location_short='в пути до потребителя'
				   		then (extract(epoch from now()::timestamptz - case when fact_loading_date is null or fact_loading_date<'2011-01-01' then plan_Loading_Date::timestamptz else fact_loading_date::timestamptz end)/60./60./24.)::numeric(10,1)
			  end as longest_last_road
			----select *
			from temp_final_ as tf)
select request_number, request_date, route_list_number, route_list_date
, sender_city, sender_organization, recipient_city, recipient_organization, status_upp, delivery_type
, payer, main
, plan_loading_date, fact_loading_date, plan_unloading_date, fact_unloading_date
, rl_loading_address, rl_unloading_address, status_date
, coalesce(consolidation_warehouse, pred_consolidation_warehouse) as consolidation_warehouse
, on_warehouse
, final_status, xpl_status, upp_status
, first_fact_loading_date
, "location"
, case when longest>365.0 then 365.0
	   when longest<0 then 0
	   else longest
  end as longest
, nomenclatura
, location_short
, case when longest_on_consolidation<0 then 0 else longest_on_consolidation end as longest_on_consolidation
, case when longest_last_road<0 then 0 else longest_last_road end as longest_last_road
, driver
, driver_phone
, carrier 
, carrier_inn
, carrier_kpp
, guid
from cte
where request_date>= perem_date;
--where longest_on_consolidation<0


drop table if exists temp_final;
create temp table temp_final
as
select f.request_number, f.request_date, f.route_list_number, f.route_list_date
, f.sender_city, f.sender_organization, f.recipient_city, f.recipient_organization, f.status_upp, f.delivery_type
, f.payer, f.main
, f.plan_loading_date, f.fact_loading_date, f.plan_unloading_date, f.fact_unloading_date
, f.rl_loading_address, f.rl_unloading_address, f.status_date
, f.consolidation_warehouse
, f.on_warehouse
, f.final_status, f.xpl_status, f.upp_status
, f.first_fact_loading_date
, f."location"
, f.longest
, n."name" as nomenclatura
, f.location_short
, f.longest_on_consolidation
, f.longest_last_road
, f.driver
, f.driver_phone
, f.carrier 
, f.carrier_inn
, f.carrier_kpp
, f.guid
from temp_final_st as f
left join temp_nomen as n on n.request::text=f.guid
where request_date>= perem_date
;

------------------------------------------------------
	if i = 1 then
		drop table if exists temp_nomen;
	end if;
------------------------------------------------------

--select * from temp_final_st where request_number = 'LLS399682'

--266710
--                                                                                           

	begin
		truncate table "psv"."t_search_of_request";
		insert into "psv"."t_search_of_request"
		select *
		from temp_final_st;
	exception when others
		then rollback ;
	end;
	


--	create table "psv"."t_search_of_nomenclature" (like "psv"."t_search_of_request")	

	begin
		truncate table "psv"."t_search_of_nomenclature";
		insert into "psv"."t_search_of_nomenclature"
		select *
		from temp_final;
	exception when others
		then rollback ;
	end;

--select * from "psv"."t_search_of_nomenclature"

end;
$procedure$
;
