-- DROP PROCEDURE reports.fill_wms();

CREATE OR REPLACE PROCEDURE reports.fill_wms()
 LANGUAGE plpgsql
AS $procedure$
begin

-- Расходный ордер -----------------------------------------------------------------------------------------------------------
	
drop table if exists temp_ro;
create temp table temp_ro
as
select 	
	r.ssylka as id_ro,
	r.nomer as nomer_ro,
	r."data" as data_ro,
	r.data_otgruzki as data_otgruzki,
	so.operatsiya as skladskaya_operaciya,
	sro.status as status,
	r.sr_zakaz_lokotekh as id_dol,
	r.khtk_elektronnaya_zayavka_zavoda as id_h,
	"string_agg" as rashirenie,
	case when otpravlen_v_sed is true then 'Да' else 'Нет' end as otpravlen_v_sed,
	ras.data_sozdaniya
from erp_sld.raskhodnyy_order_na_tovary r
	left join erp_sld.skladskie_operatsii so on so.ssylka = r.skladskaya_operatsiya
	left join erp_sld.statusy_raskhodnykh_orderov sro on sro.ssylka = r.status
	left join
			(select 
				ssylka ,
				"string_agg",
				case 
					when otpravlen_v_sed !=0 then true 
					when ecp !=0 then true 
					else false 
				end as otpravlen_v_sed ,
				case 
					when ecp = 0 then false
					when ecp != 0 then true
				end as ecp,
				data_sozdaniya
			from
			(
				select 
				ssylka,
				string_agg(distinct rasshirenie,',') as "string_agg",
				sum(otpravlen_v_sed) as otpravlen_v_sed,
				sum(ecp) as ecp,
				data_sozdaniya	
				from (
						select 
							r.ssylka , 
							ot.rasshirenie,
							case 
								when f.otpravlen is null then 0
								when f.otpravlen is false then 0
								when f.otpravlen is true then 1
							end	as otpravlen_v_sed,
							case 
								when ot.podpisane_p is null then 0
								when ot.podpisane_p is false then 0
								when ot.podpisane_p is true then 1
							end as ecp,		
							first_value (ot.data_sozdaniya) over (partition by r.ssylka order by ot.data_sozdaniya desc) as data_sozdaniya
						from erp_sld.raskhodnyy_order_na_tovary r
							left join erp_sld.raskhodnyy_order_na_tovary_tovary_po_rasporyazheniyam rtr on rtr.ssylka = r.ssylka 
							left join erp_sld.otgruzka_tovarov_s_khraneniya_prisoedinennye_fayly ot on ot.vladelets_fayla = rtr.rasporyazhenie
							left join erp_sld.sr_otpravka_faylov_v_lokotekh f on f.fayl = ot.ssylka  
						where (r."data" >= '2021-01-01' or r."data" is null) 
							and r.pometka_udaleniya is false
							) t
			group by ssylka	,data_sozdaniya) t1
				) ras on ras.ssylka = r.ssylka 
	where r.pometka_udaleniya is false 
	and r.proveden is true 
	and r."data" >= '2021-01-01'
	and r.status = 'bdc2dd3b-b02c-4815-af56-2538b334fbf4' -- Отгружен
;
-- select * from temp_ro

-- Расходный ордер -----------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
-- Временные ряды-------------------------------------------------------------------------------------------------------------

drop table if exists temp_time_oh;
create temp table temp_time_oh
as
select 
	dol.ssylka as id_zakaz,
	(dol.data_sozdaniya_v_baze_lokotekh + interval '1 hours'*s.sr_raznitsa_vo_vremeni)::TIMESTAMP as data_sozdaniya_v_baze_lokotekh_gmt,
	(dol.data_zagruzki_izlts+ interval '1 hours'*s.sr_raznitsa_vo_vremeni)::TIMESTAMP as data_zagruzki_izlts_gmt,
	(dol.data_prokhozhdeniya_byudzhetnogo_kontrolya + interval '1 hours'*s.sr_raznitsa_vo_vremeni)::TIMESTAMP as data_prokhozhdeniya_byudzhetnogo_kontrolya_gmt,
	greatest (dol.data_zagruzki_izlts+ interval '1 hours'*s.sr_raznitsa_vo_vremeni, dol.data_prokhozhdeniya_byudzhetnogo_kontrolya + interval '1 hours'*s.sr_raznitsa_vo_vremeni)::TIMESTAMP as data_otscheta_gmt,
	to_char(greatest (dol.data_zagruzki_izlts+ interval '1 hours'*s.sr_raznitsa_vo_vremeni, dol.data_prokhozhdeniya_byudzhetnogo_kontrolya + interval '1 hours'*s.sr_raznitsa_vo_vremeni),'ID')::INT as order_day_of_week_gmt,
	(ro.data_ro + interval '1 hours'*s.sr_raznitsa_vo_vremeni)::TIMESTAMP as data_ro_gmt,
	(ro.data_otgruzki+ interval '1 hours'*s.sr_raznitsa_vo_vremeni)::TIMESTAMP as data_otgruzki_ro_gmt,
	to_char(ro.data_otgruzki+ interval '1 hours'*s.sr_raznitsa_vo_vremeni,'ID')::INT as otgruzka_day_of_week_gmt,
	(coalesce(ro.data_sozdaniya,current_timestamp)  + interval '1 hours'*s.sr_raznitsa_vo_vremeni)::TIMESTAMP as data_prikrepleniya_filya_gmt,
	case 
		when kl.naimenovanie  = 'Круглосуточно, 7 дней в неделю' then (date_trunc('day',  greatest(dol.data_zagruzki_izlts,dol.data_prokhozhdeniya_byudzhetnogo_kontrolya) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '0 hours')::TIMESTAMP
		when kl.naimenovanie  = '07:00-19:00, 7 дней в неделю' then (date_trunc('day',  greatest(dol.data_zagruzki_izlts,dol.data_prokhozhdeniya_byudzhetnogo_kontrolya) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '7 hours')::TIMESTAMP
		when kl.naimenovanie  = '08:00-16:12, 7 дней в неделю' then (date_trunc('day',  greatest(dol.data_zagruzki_izlts,dol.data_prokhozhdeniya_byudzhetnogo_kontrolya) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '8 hours')::TIMESTAMP
		when kl.naimenovanie  = '08:00-17:00, 7 дней в неделю' then (date_trunc('day',  greatest(dol.data_zagruzki_izlts,dol.data_prokhozhdeniya_byudzhetnogo_kontrolya) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '8 hours')::TIMESTAMP
		when kl.naimenovanie  = '08:00-20:00, 7 дней в неделю' then (date_trunc('day',  greatest(dol.data_zagruzki_izlts,dol.data_prokhozhdeniya_byudzhetnogo_kontrolya) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '8 hours')::TIMESTAMP
		when kl.naimenovanie  = '09:00-21:00, 7 дней в неделю' then (date_trunc('day',  greatest(dol.data_zagruzki_izlts,dol.data_prokhozhdeniya_byudzhetnogo_kontrolya) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '9 hours')::TIMESTAMP
		when kl.naimenovanie  = '08:00-16:30, 5 дней в неделю' then (date_trunc('day',  greatest(dol.data_zagruzki_izlts,dol.data_prokhozhdeniya_byudzhetnogo_kontrolya) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '8 hours')::TIMESTAMP
		when kl.naimenovanie  = '08:00-17:00, 5 дней в неделю' then (date_trunc('day',  greatest(dol.data_zagruzki_izlts,dol.data_prokhozhdeniya_byudzhetnogo_kontrolya) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '8 hours')::TIMESTAMP
	end as nachalo_rab_dnya,
	case 
		when kl.naimenovanie  = 'Круглосуточно, 7 дней в неделю' then (date_trunc('day',  greatest(dol.data_zagruzki_izlts,dol.data_prokhozhdeniya_byudzhetnogo_kontrolya) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '24 hours')::TIMESTAMP
		when kl.naimenovanie  = '07:00-19:00, 7 дней в неделю' then (date_trunc('day',  greatest(dol.data_zagruzki_izlts,dol.data_prokhozhdeniya_byudzhetnogo_kontrolya) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '19 hours')::TIMESTAMP
		when kl.naimenovanie  = '08:00-16:12, 7 дней в неделю' then (date_trunc('day',  greatest(dol.data_zagruzki_izlts,dol.data_prokhozhdeniya_byudzhetnogo_kontrolya) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '16 hours'+ interval '12 minute')::TIMESTAMP
		when kl.naimenovanie  = '08:00-17:00, 7 дней в неделю' then (date_trunc('day',  greatest(dol.data_zagruzki_izlts,dol.data_prokhozhdeniya_byudzhetnogo_kontrolya) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '17 hours')::TIMESTAMP
		when kl.naimenovanie  = '08:00-20:00, 7 дней в неделю' then (date_trunc('day',  greatest(dol.data_zagruzki_izlts,dol.data_prokhozhdeniya_byudzhetnogo_kontrolya) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '20 hours')::TIMESTAMP
		when kl.naimenovanie  = '09:00-21:00, 7 дней в неделю' then (date_trunc('day',  greatest(dol.data_zagruzki_izlts,dol.data_prokhozhdeniya_byudzhetnogo_kontrolya) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '21 hours')::TIMESTAMP
		when kl.naimenovanie  = '08:00-16:30, 5 дней в неделю' then (date_trunc('day',  greatest(dol.data_zagruzki_izlts,dol.data_prokhozhdeniya_byudzhetnogo_kontrolya) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '16 hours'+ interval '30 minute')::TIMESTAMP
		when kl.naimenovanie  = '08:00-17:00, 5 дней в неделю' then (date_trunc('day',  greatest(dol.data_zagruzki_izlts,dol.data_prokhozhdeniya_byudzhetnogo_kontrolya) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '17 hours')::TIMESTAMP
	end as konec_rab_dnya,
	case 
		when kl.naimenovanie  = 'Круглосуточно, 7 дней в неделю' then 0
		when kl.naimenovanie  = '07:00-19:00, 7 дней в неделю' or kl.naimenovanie  = '08:00-20:00, 7 дней в неделю' or kl.naimenovanie  = '09:00-21:00, 7 дней в неделю' then 720
		when kl.naimenovanie  = '08:00-16:12, 7 дней в неделю' then 948
		when kl.naimenovanie  = '08:00-17:00, 7 дней в неделю' or kl.naimenovanie  = '08:00-17:00, 5 дней в неделю' then 900
		when kl.naimenovanie  = '08:00-16:30, 5 дней в неделю' then 930
	end as nerabochee_vremya,
   coalesce (
   (date(coalesce(ro.data_sozdaniya,current_timestamp)  + interval '1 hours'*s.sr_raznitsa_vo_vremeni) - date(greatest (dol.data_zagruzki_izlts+ interval '1 hours'*s.sr_raznitsa_vo_vremeni, dol.data_prokhozhdeniya_byudzhetnogo_kontrolya + interval '1 hours'*s.sr_raznitsa_vo_vremeni)))::int,
   (date(CURRENT_TIMESTAMP) - date(greatest (dol.data_zagruzki_izlts, dol.data_prokhozhdeniya_byudzhetnogo_kontrolya)))::int)
   	as day_dif
from erp_sld.sr_dannye_otgruzok_lokotekh dol
left join temp_ro ro on ro.id_dol = dol.ssylka
left join erp_sld.sklady s on s.ssylka = dol.sklad_salair 
left join erp_sld.kalendari kl on kl.ssylka = s.kalendar
where 
	dol.pometka_udaleniya is false 
	and dol.data_zagruzki_izlts >= '2022-01-01'
	and dol.status not in ('Отменена в базе Локотеха!', 'Зарегистрирована к отменене в ЛТС')
;
--select * from temp_time_oh where id_zakaz = 'a612454a-535e-11ed-83af-3a68dd4bb987'

-- Временные ряды ОХ----------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
-- Временные ряды 4PL---------------------------------------------------------------------------------------------------------

drop table if exists temp_time_4pl;
create temp table temp_time_4pl
as
select 
	h.ssylka as id_zakaz,
	(h.data_vnutrennego_zakaza + interval '1 hours'*s.sr_raznitsa_vo_vremeni)::TIMESTAMP as data_sozdaniya_v_baze_lokotekh_gmt,
	(h."data" + interval '1 hours'*s.sr_raznitsa_vo_vremeni)::TIMESTAMP as data_zagruzki_izlts_gmt,
	greatest (h.data_vnutrennego_zakaza+ interval '1 hours'*s.sr_raznitsa_vo_vremeni, h."data" + interval '1 hours'*s.sr_raznitsa_vo_vremeni)::TIMESTAMP as data_otscheta_gmt,
	to_char(greatest (h.data_vnutrennego_zakaza+ interval '1 hours'*s.sr_raznitsa_vo_vremeni, h."data" + interval '1 hours'*s.sr_raznitsa_vo_vremeni),'ID')::INT as order_day_of_week_gmt,
	(ro.data_ro + interval '1 hours'*s.sr_raznitsa_vo_vremeni)::TIMESTAMP as data_ro_gmt,
	(ro.data_otgruzki+ interval '1 hours'*s.sr_raznitsa_vo_vremeni)::TIMESTAMP as data_otgruzki_ro_gmt,
	to_char(ro.data_otgruzki+ interval '1 hours'*s.sr_raznitsa_vo_vremeni,'ID')::INT as otgruzka_day_of_week_gmt,
	(coalesce(ro.data_sozdaniya,current_timestamp)  + interval '1 hours'*s.sr_raznitsa_vo_vremeni)::TIMESTAMP as data_prikrepleniya_filya_gmt,
	case 
		when kl.naimenovanie  = 'Круглосуточно, 7 дней в неделю' then (date_trunc('day',  greatest(h."data",h.data_vnutrennego_zakaza) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '0 hours')::TIMESTAMP
		when kl.naimenovanie  = '07:00-19:00, 7 дней в неделю' then (date_trunc('day',  greatest(h."data",h.data_vnutrennego_zakaza) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '7 hours')::TIMESTAMP
		when kl.naimenovanie  = '08:00-16:12, 7 дней в неделю' then (date_trunc('day',  greatest(h."data",h.data_vnutrennego_zakaza) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '8 hours')::TIMESTAMP
		when kl.naimenovanie  = '08:00-17:00, 7 дней в неделю' then (date_trunc('day',  greatest(h."data",h.data_vnutrennego_zakaza) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '8 hours')::TIMESTAMP
		when kl.naimenovanie  = '08:00-20:00, 7 дней в неделю' then (date_trunc('day',  greatest(h."data",h.data_vnutrennego_zakaza) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '8 hours')::TIMESTAMP
		when kl.naimenovanie  = '09:00-21:00, 7 дней в неделю' then (date_trunc('day',  greatest(h."data",h.data_vnutrennego_zakaza) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '9 hours')::TIMESTAMP
		when kl.naimenovanie  = '08:00-16:30, 5 дней в неделю' then (date_trunc('day',  greatest(h."data",h.data_vnutrennego_zakaza) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '8 hours')::TIMESTAMP
		when kl.naimenovanie  = '08:00-17:00, 5 дней в неделю' then (date_trunc('day',  greatest(h."data",h.data_vnutrennego_zakaza) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '8 hours')::TIMESTAMP
	end as nachalo_rab_dnya,
	case 
		when kl.naimenovanie  = 'Круглосуточно, 7 дней в неделю' then (date_trunc('day',  greatest(h."data",h.data_vnutrennego_zakaza) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '24 hours')::TIMESTAMP
		when kl.naimenovanie  = '07:00-19:00, 7 дней в неделю' then (date_trunc('day',  greatest(h."data",h.data_vnutrennego_zakaza) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '19 hours')::TIMESTAMP
		when kl.naimenovanie  = '08:00-16:12, 7 дней в неделю' then (date_trunc('day',  greatest(h."data",h.data_vnutrennego_zakaza) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '16 hours'+ interval '12 minute')::TIMESTAMP
		when kl.naimenovanie  = '08:00-17:00, 7 дней в неделю' then (date_trunc('day',  greatest(h."data",h.data_vnutrennego_zakaza) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '17 hours')::TIMESTAMP
		when kl.naimenovanie  = '08:00-20:00, 7 дней в неделю' then (date_trunc('day',  greatest(h."data",h.data_vnutrennego_zakaza) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '20 hours')::TIMESTAMP
		when kl.naimenovanie  = '09:00-21:00, 7 дней в неделю' then (date_trunc('day',  greatest(h."data",h.data_vnutrennego_zakaza) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '21 hours')::TIMESTAMP
		when kl.naimenovanie  = '08:00-16:30, 5 дней в неделю' then (date_trunc('day',  greatest(h."data",h.data_vnutrennego_zakaza) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '16 hours'+ interval '30 minute')::TIMESTAMP
		when kl.naimenovanie  = '08:00-17:00, 5 дней в неделю' then (date_trunc('day',  greatest(h."data",h.data_vnutrennego_zakaza) + interval '1 hours'*s.sr_raznitsa_vo_vremeni) + interval '17 hours')::TIMESTAMP
	end as konec_rab_dnya,
	case 
		when kl.naimenovanie  = 'Круглосуточно, 7 дней в неделю' then 0
		when kl.naimenovanie  = '07:00-19:00, 7 дней в неделю' or kl.naimenovanie  = '08:00-20:00, 7 дней в неделю' or kl.naimenovanie  = '09:00-21:00, 7 дней в неделю' then 720
		when kl.naimenovanie  = '08:00-16:12, 7 дней в неделю' then 948
		when kl.naimenovanie  = '08:00-17:00, 7 дней в неделю' or kl.naimenovanie  = '08:00-17:00, 5 дней в неделю' then 900
		when kl.naimenovanie  = '08:00-16:30, 5 дней в неделю' then 930
	end as nerabochee_vremya,
   coalesce (
   (date(coalesce(ro.data_sozdaniya,current_timestamp)  + interval '1 hours'*s.sr_raznitsa_vo_vremeni) - date(greatest (h."data"+ interval '1 hours'*s.sr_raznitsa_vo_vremeni, h.data_vnutrennego_zakaza + interval '1 hours'*s.sr_raznitsa_vo_vremeni)))::int,
   (date(CURRENT_TIMESTAMP) - date(greatest (h."data", h.data_vnutrennego_zakaza)))::int)
   	as day_dif
from erp_sld.khtk_elektronnaya_zayavka_zavoda h
left join temp_ro ro on ro.id_h = h.ssylka 
left join erp_sld.sklady s on s.ssylka = h.sklad 
left join erp_sld.kalendari kl on kl.ssylka =  s.kalendar
where 
	h.pometka_udaleniya is false 
	and h."data"  >= '2022-01-01'
	and h.otmenen is false
	and s.sr_vid_ispolzovaniya in ('22d54fe7-3354-44f3-8bcf-88baedfe6122' , 'a473c7e8-b348-48cd-a009-d3a07d837b9e')
;
-- select * from temp_time_4pl

-- Временные ряды 4PL---------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
-- Данные отгрузок ЛокоТех ---------------------------------------------------------------------------------------------------

drop table if exists temp_dol;
create temp table temp_dol
as
select 
	dol.ssylka as id_zakaz,
	dol.kod as nomer_zakaza,
	date(t.data_zagruzki_izlts_gmt) as data_zakaza,
	t.data_zagruzki_izlts_gmt as data_time_zakaza,
	ro.id_ro,
	ro.nomer_ro,
	t.data_ro_gmt as data_ro,
	t.data_otgruzki_ro_gmt as data_otgruzki_ro ,
	t.data_prikrepleniya_filya_gmt as data_prikrepleniya_filya,
	dol.nomer_nakladnoy_lokotekh as nakladnaya_locoteh,
	dol.kommentariy ,
	ro.skladskaya_operaciya,
	ro.status,
	s.naimenovanie as sklad,
	case when s.sr_vid_ispolzovaniya = '22d54fe7-3354-44f3-8bcf-88baedfe6122' then '4PL' when s.sr_vid_ispolzovaniya = 'a473c7e8-b348-48cd-a009-d3a07d837b9e' then 'ОХ' end as sr_vid_ispolzovaniya,
	su.naimenovanie_polnoe  as doroga,
	case 
		when s.sr_vid_ispolzovaniya isnull then 'ЖДРМ'
		when s.naimenovanie  like '%СЛД-%' then 'СЛД'
		when s.naimenovanie  like '%СО%' then 'СО'
		when s.naimenovanie  like '%СУ%' then 'СУ'
		when s.naimenovanie  like '%ЦС %' or s.naimenovanie  like '%ОП%' then 'ЦС'
		else 'ЖДРМ'
		end as tip_sklada,
   	rp.naimenovanie  as fio,
   	regexp_replace(initcap(f.naimenovanie_polnoe), '[а-я -]', '', 'g') as filial,
   /*	case 
	   	when ro.id_ro notnull then cast ((extract (epoch FROM ro.data_otgruzki) - extract (epoch FROM dol.data_zagruzki_izlts))/60 as int)
   		when ro.id_ro is null then cast ((extract (epoch FROM current_timestamp ) - extract (epoch FROM dol.data_zagruzki_izlts))/60 as int) end as min_otrabotki,
   	case 
	   	when kl.naimenovanie = 'Круглосуточно, 7 дней в неделю' then ((extract (epoch FROM t.data_otgruzki_ro_gmt) - extract (epoch FROM t.data_otscheta_gmt))/60)::int
	   	---------------------------------------------------------------------------------------------------------------------------------------------------------------
	   	when kl.naimenovanie = '07:00-19:00, 7 дней в неделю' and t.data_otscheta_gmt between date_trunc('day',t.data_otscheta_gmt) and t.nachalo_rab_dnya then ((extract (epoch FROM t.data_otgruzki_ro_gmt) - extract (epoch FROM t.nachalo_rab_dnya))/60 - (720 * t.day_dif))::int -- 1 До начала рабочего дня
	   	when kl.naimenovanie = '07:00-19:00, 7 дней в неделю' and t.data_otscheta_gmt between t.nachalo_rab_dnya and t.konec_rab_dnya then ((extract (epoch FROM t.data_otgruzki_ro_gmt) - extract (epoch FROM t.data_otscheta_gmt))/60 - 720 * t.day_dif)::int -- 2 В течение рабочего дня
	   	when kl.naimenovanie = '07:00-19:00, 7 дней в неделю' and t.data_otscheta_gmt between t.konec_rab_dnya and date_trunc('day', t.data_otscheta_gmt + interval '1 day')  then ((extract (epoch FROM t.data_otgruzki_ro_gmt) - extract (epoch FROM t.nachalo_rab_dnya + interval ' 1 day'))/60 -(720 * t.day_dif))::int -- 3 После окончания рабочего дня
	   	---------------------------------------------------------------------------------------------------------------------------------------------------------------
   	end as min_otrabotki_v2,*/
   coalesce(
   	case
	   	when t.data_otscheta_gmt between date_trunc('day',t.data_otscheta_gmt) and t.nachalo_rab_dnya then ((extract (epoch FROM t.data_prikrepleniya_filya_gmt) - extract (epoch FROM t.nachalo_rab_dnya))/60 - (t.nerabochee_vremya * t.day_dif))::int -- 1 До начала рабочего дня
	   	when t.data_otscheta_gmt between t.nachalo_rab_dnya and t.konec_rab_dnya then ((extract (epoch FROM t.data_prikrepleniya_filya_gmt) - extract (epoch FROM t.data_otscheta_gmt))/60 - t.nerabochee_vremya * t.day_dif)::int -- 2 В течение рабочего дня
	   	when t.data_otscheta_gmt between t.konec_rab_dnya and date_trunc('day', t.data_otscheta_gmt + interval '1 day')  then ((extract (epoch FROM t.data_prikrepleniya_filya_gmt) - extract (epoch FROM t.nachalo_rab_dnya + interval ' 1 day'))/60 - (t.nerabochee_vremya * (t.day_dif-1)))::int -- 3 После окончания рабочего дня
	   	end,
	((extract (epoch FROM CURRENT_TIMESTAMP + interval '1 hours'*s.sr_raznitsa_vo_vremeni) - extract (epoch FROM t.data_otscheta_gmt))/60 - (t.nerabochee_vremya * t.day_dif))::int) -- 4 По неотгруженным заказам
	as min_otrabotki,
   	s.sr_oboznachenie_chasovogo_poyasa as gmt,
   	s.sr_raznitsa_vo_vremeni as raznica_vo_vremeni,
   	kl.naimenovanie as grafik,
   	t.order_day_of_week_gmt as order_day_of_week,
  	t.otgruzka_day_of_week_gmt as otgruzka_day_of_week,
  	extract (hour from (dol.data_zagruzki_izlts + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) as "Hour_(+GMT)",--Не используется
  	/*case
       when ro.status is null or ro.status != 'Отгружен' then 'В работе'
       when ro.status = 'Отгружен' and (extract (epoch FROM ro.data_otgruzki) - extract (epoch FROM dol.data_zagruzki_izlts))/60 < 361 then 'Без нарушения сроков'
       when ro.status = 'Отгружен' and (extract (epoch FROM ro.data_otgruzki) - extract (epoch FROM dol.data_zagruzki_izlts))/60 > 360 and kl.naimenovanie  = 'Круглосуточно, 7 дней в неделю' then 'C нарушением сроков'
       when ro.status = 'Отгружен' and (extract (epoch FROM ro.data_otgruzki) - extract (epoch FROM dol.data_zagruzki_izlts))/60 > 360 and kl.naimenovanie = '07:00-19:00, 7 дней в неделю' and extract (hour from (dol.data_zagruzki_izlts + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) >= 17 and extract (hour from (ro.data_otgruzki + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) <= 9  then 'Без нарушения сроков'
       when ro.status = 'Отгружен' and (extract (epoch FROM ro.data_otgruzki) - extract (epoch FROM dol.data_zagruzki_izlts))/60 > 360 and kl.naimenovanie = '08:00-16:12, 7 дней в неделю' and extract (hour from (dol.data_zagruzki_izlts + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) >= 14 and extract (hour from (ro.data_otgruzki + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) <= 10  then 'Без нарушения сроков'
       when ro.status = 'Отгружен' and (extract (epoch FROM ro.data_otgruzki) - extract (epoch FROM dol.data_zagruzki_izlts))/60 > 360 and kl.naimenovanie = '08:00-17:00, 7 дней в неделю' and extract (hour from (dol.data_zagruzki_izlts + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) >= 15 and extract (hour from (ro.data_otgruzki + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) <= 10  then 'Без нарушения сроков'
       when ro.status = 'Отгружен' and (extract (epoch FROM ro.data_otgruzki) - extract (epoch FROM dol.data_zagruzki_izlts))/60 > 360 and kl.naimenovanie = '08:00-20:00, 7 дней в неделю' and extract (hour from (dol.data_zagruzki_izlts + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) >= 18 and extract (hour from (ro.data_otgruzki + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) <= 10  then 'Без нарушения сроков'
       when ro.status = 'Отгружен' and (extract (epoch FROM ro.data_otgruzki) - extract (epoch FROM dol.data_zagruzki_izlts))/60 > 360 and kl.naimenovanie = '09:00-21:00, 7 дней в неделю' and extract (hour from (dol.data_zagruzki_izlts + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) >= 19 and extract (hour from (ro.data_otgruzki + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) <= 11  then 'Без нарушения сроков'
       when ro.status = 'Отгружен' and (extract (epoch FROM ro.data_otgruzki) - extract (epoch FROM dol.data_zagruzki_izlts))/60 > 360 and kl.naimenovanie = '08:00-16:30, 5 дней в неделю' and extract (hour from (dol.data_zagruzki_izlts + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) >= 14 and extract (hour from (ro.data_otgruzki + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) <= 10 and to_char(dol.data_zagruzki_izlts + interval '1 hours'*s.sr_raznitsa_vo_vremeni,'ID') = to_char(ro.data_otgruzki+ interval '1 hours'*s.sr_raznitsa_vo_vremeni,'ID')  then 'Без нарушения сроков'
       when ro.status = 'Отгружен' and (extract (epoch FROM ro.data_otgruzki) - extract (epoch FROM dol.data_zagruzki_izlts))/60 > 360 and kl.naimenovanie = '08:00-17:00, 5 дней в неделю' and extract (hour from (dol.data_zagruzki_izlts + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) >= 15 and extract (hour from (ro.data_otgruzki + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) <= 10 and to_char(dol.data_zagruzki_izlts + interval '1 hours'*s.sr_raznitsa_vo_vremeni,'ID') = to_char(ro.data_otgruzki+ interval '1 hours'*s.sr_raznitsa_vo_vremeni,'ID')  then 'Без нарушения сроков'
       else 'C нарушением сроков'
     end as obrabotka_status,*/
   	case
	   	when ro.status is null or ro.status != 'Отгружен' then 'В работе'
	   	when t.data_otscheta_gmt between date_trunc('day',t.data_otscheta_gmt) and t.nachalo_rab_dnya and ((extract (epoch FROM t.data_prikrepleniya_filya_gmt) - extract (epoch FROM t.nachalo_rab_dnya))/60 - (t.nerabochee_vremya * t.day_dif))::int between 0 and 420 then 'Без нарушения сроков'
	   	when t.data_otscheta_gmt between t.nachalo_rab_dnya and t.konec_rab_dnya and ((extract (epoch FROM t.data_prikrepleniya_filya_gmt) - extract (epoch FROM t.data_otscheta_gmt))/60 - t.nerabochee_vremya * t.day_dif)::int between 0 and 420 then 'Без нарушения сроков'
	   	when t.data_otscheta_gmt between t.konec_rab_dnya and date_trunc('day', t.data_otscheta_gmt + interval '1 day')  and ((extract (epoch FROM t.data_prikrepleniya_filya_gmt) - extract (epoch FROM t.nachalo_rab_dnya + interval ' 1 day'))/60 - (t.nerabochee_vremya * (t.day_dif-1)))::int between 0 and 420 then 'Без нарушения сроков'
	   	else 'C нарушением сроков'
	end as obrabotka_status,  
     ro.rashirenie,
     ro.otpravlen_v_sed,
     otg.nomer as otgruzka,
     dol.status as status_otgruzki,
     CURRENT_TIMESTAMP as date_actual,
     case 
	     when dol.status_vygruzki_v_lokotekh = 0 or dol.status_vygruzki_v_lokotekh = 5 then 'Отменено в базе Локотех'
	     when dol.status_vygruzki_v_lokotekh = 1 or dol.status_vygruzki_v_lokotekh = 2 then 'В Работе'
	     when dol.status_vygruzki_v_lokotekh = 3 then 'Ошибка выгрузки в Локотех'
	     when dol.status_vygruzki_v_lokotekh = 4 then 'Без ошибок'
	 end as status_vigruzka,
	 dol.opisanie_oshibki as opisanie_oshibki,
	 otg.naimenovanie as kladovshik,
	 dol.tip as vid_rashoda,
	 dol.proyden_byudzhetnyy_kontrol as byudzhetnyy_kontrol
from 
	erp_sld.sr_dannye_otgruzok_lokotekh dol
	left join erp_sld.otgruzka_tovarov_s_khraneniya o on o.sr__zakaz_lokotekh = dol.ssylka
	left join temp_ro ro on ro.id_dol = dol.ssylka
	left join temp_time_oh t on t.id_zakaz = dol.ssylka
	left join erp_sld.sklady s on s.ssylka = dol.sklad_salair 
	left join erp_sld.fizicheskie_litsa rp on rp.ssylka  = s.sr_regionalnyy_predstavitel 
	left join erp_sld.sr_upravleniya su on su.ssylka = s.sr_upravlenie 
	left join erp_sld.kalendari kl on kl.ssylka =  s.kalendar
	left join erp_sld.sr_filialy f on f.ssylka = s.sr_filial
	left join (select
					o.nomer ,
					o.sr__zakaz_lokotekh,
					kl.naimenovanie 
				from erp_sld.otgruzka_tovarov_s_khraneniya o
				left join erp_sld.polzovateli kl on kl.ssylka = o.menedzher 
				where
					o.pometka_udaleniya is false
					and o.sr__zakaz_lokotekh != '00000000-0000-0000-0000-000000000000' ) as otg on otg.sr__zakaz_lokotekh = dol.ssylka 
where 
	dol.pometka_udaleniya is false 
	and dol.data_zagruzki_izlts >= '2022-01-01'
	and dol.status not in ('Отменена в базе Локотеха!', 'Зарегистрирована к отменене в ЛТС')
	and s.sr_vid_ispolzovaniya in ('22d54fe7-3354-44f3-8bcf-88baedfe6122' , 'a473c7e8-b348-48cd-a009-d3a07d837b9e')
;
--select * from temp_dol where id_zakaz = 'a612454a-535e-11ed-83af-3a68dd4bb987'

-- Данные отгрузок ЛокоТех ---------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
-- Электронная заявка Завода -------------------------------------------------------------------------------------------------	

drop table if exists temp_h;
create temp table temp_h
as
select
	h.ssylka as id_zakaz,
	h.nomer as nomer_zakaza,
	date(t.data_zagruzki_izlts_gmt) as data_zakaza,
	t.data_zagruzki_izlts_gmt as data_time_zakaza,
	ro.id_ro,
	ro.nomer_ro,
	t.data_ro_gmt as data_ro,
	t.data_otgruzki_ro_gmt as data_otgruzki_ro,
	t.data_prikrepleniya_filya_gmt as data_prikrepleniya_filya,
	h.nomer_vnutrennego_zakaza as nakladnaya_locoteh,
	h.kommentariy,
	ro.skladskaya_operaciya,
	ro.status,
	s.naimenovanie as sklad,
	case when s.sr_vid_ispolzovaniya = '22d54fe7-3354-44f3-8bcf-88baedfe6122' then '4PL' when s.sr_vid_ispolzovaniya = 'a473c7e8-b348-48cd-a009-d3a07d837b9e' then 'ОХ' end as sr_vid_ispolzovaniya,
	su.naimenovanie_polnoe  as doroga,
	case 
		when s.sr_vid_ispolzovaniya isnull then 'ЖДРМ'
		when s.naimenovanie  like '%СЛД-%' then 'СЛД'
		when s.naimenovanie  like '%СО%' then 'СО'
		when s.naimenovanie  like '%СУ%' then 'СУ'
		when s.naimenovanie  like '%ЦС %' or s.naimenovanie  like '%ОП%' then 'ЦС'
		else 'ЖДРМ'
		end as tip_sklada,
   	rp.naimenovanie  as fio,
   	regexp_replace(initcap(f.naimenovanie_polnoe), '[а-я -]', '', 'g') as filial,
   	/*case when ro.id_ro notnull then cast ((extract (epoch FROM ro.data_otgruzki) - extract (epoch FROM h."data"))/60 as int)
    	 when ro.id_ro is null then cast ((extract (epoch FROM current_timestamp ) - extract (epoch FROM h."data"))/60 as int) end as min_otrabotki,*/
    coalesce(
   	case
	   	when t.data_otscheta_gmt between date_trunc('day',t.data_otscheta_gmt) and t.nachalo_rab_dnya then ((extract (epoch FROM t.data_prikrepleniya_filya_gmt) - extract (epoch FROM t.nachalo_rab_dnya))/60 - (t.nerabochee_vremya * t.day_dif))::int -- 1 До начала рабочего дня
	   	when t.data_otscheta_gmt between t.nachalo_rab_dnya and t.konec_rab_dnya then ((extract (epoch FROM t.data_prikrepleniya_filya_gmt) - extract (epoch FROM t.data_otscheta_gmt))/60 - t.nerabochee_vremya * t.day_dif)::int -- 2 В течение рабочего дня
	   	when t.data_otscheta_gmt between t.konec_rab_dnya and date_trunc('day', t.data_otscheta_gmt + interval '1 day')  then ((extract (epoch FROM t.data_prikrepleniya_filya_gmt) - extract (epoch FROM t.nachalo_rab_dnya + interval ' 1 day'))/60 - (t.nerabochee_vremya * (t.day_dif-1)))::int -- 3 После окончания рабочего дня
	   	end,
	((extract (epoch FROM CURRENT_TIMESTAMP + interval '1 hours'*s.sr_raznitsa_vo_vremeni) - extract (epoch FROM t.data_otscheta_gmt))/60 - (t.nerabochee_vremya * t.day_dif))::int) -- 4 По неотгруженным заказам
	as min_otrabotki,    	 
    s.sr_oboznachenie_chasovogo_poyasa as gmt,
   	s.sr_raznitsa_vo_vremeni as raznica_vo_vremeni,
   	kl.naimenovanie as grafik,
   	t.order_day_of_week_gmt as order_day_of_week,
  	t.otgruzka_day_of_week_gmt as otgruzka_day_of_week,
  	extract (hour from (h."data" + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) as "Hour_(+GMT)",--Не используется
  	/*case
       when ro.status is null or ro.status != 'Отгружен' then 'В работе' 
       when ro.status = 'Отгружен' and (extract (epoch FROM ro.data_otgruzki) - extract (epoch FROM h."data"))/60 < 361 then 'Без нарушения сроков'
       when ro.status = 'Отгружен' and (extract (epoch FROM ro.data_otgruzki) - extract (epoch FROM h."data"))/60 > 360 and kl.naimenovanie = 'Круглосуточно, 7 дней в неделю' then 'C нарушением сроков'
       when ro.status = 'Отгружен' and (extract (epoch FROM ro.data_otgruzki) - extract (epoch FROM h."data"))/60 > 360 and kl.naimenovanie = '07:00-19:00, 7 дней в неделю' and extract (hour from (h."data" + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) >= 17 and extract (hour from (ro.data_otgruzki + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) <= 9  then 'Без нарушения сроков'
       when ro.status = 'Отгружен' and (extract (epoch FROM ro.data_otgruzki) - extract (epoch FROM h."data"))/60 > 360 and kl.naimenovanie = '08:00-16:12, 7 дней в неделю' and extract (hour from (h."data" + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) >= 14 and extract (hour from (ro.data_otgruzki + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) <= 10  then 'Без нарушения сроков'
       when ro.status = 'Отгружен' and (extract (epoch FROM ro.data_otgruzki) - extract (epoch FROM h."data"))/60 > 360 and kl.naimenovanie = '08:00-17:00, 7 дней в неделю' and extract (hour from (h."data" + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) >= 15 and extract (hour from (ro.data_otgruzki + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) <= 10  then 'Без нарушения сроков'
       when ro.status = 'Отгружен' and (extract (epoch FROM ro.data_otgruzki) - extract (epoch FROM h."data"))/60 > 360 and kl.naimenovanie = '08:00-20:00, 7 дней в неделю' and extract (hour from (h."data" + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) >= 18 and extract (hour from (ro.data_otgruzki + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) <= 10  then 'Без нарушения сроков'
       when ro.status = 'Отгружен' and (extract (epoch FROM ro.data_otgruzki) - extract (epoch FROM h."data"))/60 > 360 and kl.naimenovanie = '09:00-21:00, 7 дней в неделю' and extract (hour from (h."data" + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) >= 19 and extract (hour from (ro.data_otgruzki + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) <= 11  then 'Без нарушения сроков'
       when ro.status = 'Отгружен' and (extract (epoch FROM ro.data_otgruzki) - extract (epoch FROM h."data"))/60 > 360 and kl.naimenovanie = '08:00-16:30, 5 дней в неделю' and extract (hour from (h."data" + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) >= 14 and extract (hour from (ro.data_otgruzki + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) <= 10 and to_char(h."data" + interval '1 hours'*s.sr_raznitsa_vo_vremeni,'ID') = to_char(ro.data_otgruzki+ interval '1 hours'*s.sr_raznitsa_vo_vremeni,'ID')  then 'Без нарушения сроков'
       when ro.status = 'Отгружен' and (extract (epoch FROM ro.data_otgruzki) - extract (epoch FROM h."data"))/60 > 360 and kl.naimenovanie = '08:00-17:00, 5 дней в неделю' and extract (hour from (h."data" + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) >= 15 and extract (hour from (ro.data_otgruzki + interval '1 hours'*s.sr_raznitsa_vo_vremeni)) <= 10 and to_char(h."data" + interval '1 hours'*s.sr_raznitsa_vo_vremeni,'ID') = to_char(ro.data_otgruzki+ interval '1 hours'*s.sr_raznitsa_vo_vremeni,'ID')  then 'Без нарушения сроков'
	else 'C нарушением сроков'
    end as obrabotka_status,*/
    case
	   	when ro.status is null or ro.status != 'Отгружен' then 'В работе'
	   	when t.data_otscheta_gmt between date_trunc('day',t.data_otscheta_gmt) and t.nachalo_rab_dnya and ((extract (epoch FROM t.data_prikrepleniya_filya_gmt) - extract (epoch FROM t.nachalo_rab_dnya))/60 - (t.nerabochee_vremya * t.day_dif))::int between 0 and 420 then 'Без нарушения сроков'
	   	when t.data_otscheta_gmt between t.nachalo_rab_dnya and t.konec_rab_dnya and ((extract (epoch FROM t.data_prikrepleniya_filya_gmt) - extract (epoch FROM t.data_otscheta_gmt))/60 - t.nerabochee_vremya * t.day_dif)::int between 0 and 420 then 'Без нарушения сроков'
	   	when t.data_otscheta_gmt between t.konec_rab_dnya and date_trunc('day', t.data_otscheta_gmt + interval '1 day')  and ((extract (epoch FROM t.data_prikrepleniya_filya_gmt) - extract (epoch FROM t.nachalo_rab_dnya + interval ' 1 day'))/60 - (t.nerabochee_vremya * (t.day_dif-1)))::int between 0 and 420 then 'Без нарушения сроков'
	   	else 'C нарушением сроков'
	end as obrabotka_status,   
     ro.rashirenie,
     ro.otpravlen_v_sed,
     otg.nomer as otgruzka,
     h.sr_status_vypolneniya_zakaza as status_otgruzki,
     CURRENT_TIMESTAMP as date_actual,
     case 
	     when h.sr_status_vypolneniya_zakaza = 'Комплектация отгружена' or h.sr_status_vypolneniya_zakaza = 'Комплектации отгружена' then 'Без ошибок'
	     when h.sr_status_vypolneniya_zakaza = 'Комплектация готова к отгрузке' or h.sr_status_vypolneniya_zakaza = 'Комплектация сформирована' then 'В Работе'
	     when h.sr_status_vypolneniya_zakaza = 'С ошибками при формировании комплектации' or h.sr_status_vypolneniya_zakaza = 'С ошибками при подготовке комплектации к отгрузке' then 'Ошибка выгрузки в Локотех'
	     else 'Статус не определен'
	 end as status_vigruzka,
     h.sr_opisanie_oshibki as opisanie_oshibki,
     otg.naimenovanie as kladovshik,
     'Заказы на отгрузку 4pl' as vid_rashoda,
	 false as byudzhetnyy_kontrol
 from 
	erp_sld.khtk_elektronnaya_zayavka_zavoda h
	left join temp_ro ro on ro.id_h = h.ssylka
	left join temp_time_4pl  t on t.id_zakaz = h.ssylka 
	left join erp_sld.sklady s on s.ssylka = h.sklad 
	left join erp_sld.sr_filialy f on f.ssylka = s.sr_filial
	left join erp_sld.fizicheskie_litsa rp on rp.ssylka  = s.sr_regionalnyy_predstavitel 
	left join erp_sld.sr_upravleniya su on su.ssylka = s.sr_upravlenie 
	left join erp_sld.kalendari kl on kl.ssylka =  s.kalendar
	left join (select
					o.nomer ,
					o.khtk_elektronnaya_zayavka_zavoda,
					kl.naimenovanie 
				from erp_sld.otgruzka_tovarov_s_khraneniya o
				left join erp_sld.polzovateli kl on kl.ssylka = o.menedzher 
				where
					o.pometka_udaleniya is false
					and o.khtk_elektronnaya_zayavka_zavoda != '00000000-0000-0000-0000-000000000000') as otg on otg.khtk_elektronnaya_zayavka_zavoda = h.ssylka 
where
	h.pometka_udaleniya is false 
	and h."data"  >= '2022-01-01'
	and h.otmenen is false
	and s.sr_vid_ispolzovaniya in ('22d54fe7-3354-44f3-8bcf-88baedfe6122' , 'a473c7e8-b348-48cd-a009-d3a07d837b9e')
;
--select * from temp_h


-- Электронная заявка Завода -------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
-- ИТОГ-----------------------------------------------------------------------------------------------------------------------

drop table if exists temp_final;
create temp table temp_final
as
select * from temp_dol
union
select * from temp_h
;
--drop table if exists reports.t_wms -- Удаление старой таблицы
--create table reports.t_wms as select * from temp_final; 	--Создает таблицу с новой струкурой
truncate reports.t_wms;
insert into reports.t_wms
select * from temp_final;

end
$procedure$
;
