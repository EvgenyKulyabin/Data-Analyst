-- DROP PROCEDURE reports.fill_requests_in_route_lists();

CREATE OR REPLACE PROCEDURE reports.fill_requests_in_route_lists()
 LANGUAGE plpgsql
AS $procedure$
begin
--=============================================================================================================================================================--        
--=============================================================================================================================================================--

truncate reports.t_requests_in_route_lists;
insert into reports.t_requests_in_route_lists 
select request_number, 
request_date, 
route_list_number, 
route_list_date, 
sender_city, 
sender_region, 
sender_address, 
sender_organization, 
recipient_city, 
recipient_region, 
recipient_address, 
recipient_organization, 
recipient_inn, 
recipient_kpp, 
initiator, 
initiator_inn, 
initiator_kpp, 
payer, 
payer_inn, 
payer_kpp, 
status_xpl, 
status_upp, 
distance, 
delivery_type, 
weight, 
volume, 
carrier_price, 
customer_price, 
start_price_amount, 
rl_weight, 
rl_volume, 
carrier, 
carrier_inn, 
carrier_kpp, 
vehicle_number, 
vehicle_type, 
driver, 
driver_phone, 
plan_loading_date, 
fact_loading_date, 
plan_unloading_date, 
fact_unloading_date, 
document_create_place, 
route_type, 
transit, 
rl_loading_address, 
rl_unloading_address, 
bid_type, 
red_carrier_count, 
ten_carrier_count, 
capacity, 
class1, 
class2, 
class3, 
class4, 
class5, 
class6, 
class7, 
class8, 
class9, 
fast, 
top, 
separate, 
insurance, 
side, 
circular, 
rcargo_length, 
rcargo_width, 
rcargo_height, 
rcargo_weight, 
full_cost, 
manager_upp, 
manager_xpl, 
update_date, 
route_number, 
main, 
sender_city_id, 
sender_region_id, 
recipient_city_id, 
recipient_region_id, 
fourpl::boolean, 
consolidation_warehouse, 
trailer_type, 
max_dimension, 
is_consolidation, 
final_status, 
sender_country, 
recipient_country, 
sender_inn, 
sender_kpp, 
initial_loading_date, 
r_customer_price, 
rl_count, 
rl_weight_max, 
weight_by_dimension, 
case when route_list_number is not null then 1 else 0 end 
from dm_transportations.v_requests_in_route_lists;

--LPS319681
--select route_list_number, r_customer_price, r_customer_price*1.2, *
--from temp_final
--where request_number in ('LLS395722', 'LLS395721')


end 
$procedure$
;
