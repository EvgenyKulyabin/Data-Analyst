-- DROP PROCEDURE reports.fill_soika();

CREATE OR REPLACE PROCEDURE reports.fill_soika()
 LANGUAGE plpgsql
AS $procedure$
begin


drop table if exists temp_soika_ml;
create temp table temp_soika_ml
as
select ml."data" as Data_ML
, coalesce(ml.nomer, '         ') || '/' || coalesce(znt.nomer, '         ') as kluch
, ml.nomer as Nomer_ML
, znt.nomer as Nomer_ZNT
, znt.zayavka_po_4_pl as Zayavka_Po_4PL
, k.naimenovanie_polnoe as platelshchik_perevozki --------
--, ml.status 
, s."name" as status
--, ml.vid_dostavki -------
, vd."name" as vid_dostavki
, '' as Perevozchik
, mlz.sr_punkt_pogruzki 
, mlz.sr_punkt_vygruzki 
, mlz.sr_soica as Punkt_Vigruzki_Soika
, mlz.fakticheskaya_data_zagruzki as Data_Zagruzki_Fakticheskaya
, mlz.fakticheskaya_data_vygruzki as Data_Vigruzki_Fakticheskaya
, ml.data_vygruzki_fakticheskaya as Vigruzka_Fakticheskaya
							--, case when vdtn.zayavka_na_transport is not null then 'да' else 'нет' end as poluchena_iz_tn_soica
							--, case when vdtn.obrabotano is true then 'да' else 'нет' end otrabotana_v_1c
, mlz.ssylka as ml
, mlz.zayavka_na_transport as znt
--, ml.ssylka
--select mlz.*
from erp_buh_work.sr_marshrutnyy_list as ml 
join erp_buh_work.sr_marshrutnyy_list_zayavki as mlz using(ssylka)
join erp_buh_work.sr_zayavka_na_transport as znt on znt.ssylka = mlz.zayavka_na_transport and znt.pometka_udaleniya is false and znt.proveden is true
left join erp_buh_work.sr_statusy_marshrutnykh_listov as s on s.ssylka = ml.status
left join erp_buh_work.sr_vidy_dostavok vd on vd.ssylka = ml.vid_dostavki
left join erp_buh_work.kontragenty as k on k.ssylka = mlz.platelshchik_perevozki 
--left join temp_st as vdtn on vdtn.marshrutnyy_list = mlz.ssylka and vdtn.zayavka_na_transport = mlz.zayavka_na_transport
where ml."data" >'2023-01-01' and ml.pometka_udaleniya is false and ml.proveden is true and s."name" <> 'Отменено заказчиком'
--and ml.nomer = 'СЛ0611206' and mlz.zayavka_na_transport  = '185f2473-fbd7-11ed-8109-00505601011c'
;
--select * from temp_soika_ml --в первом запросе взять в окошке партицию именно по номерам, а не по гуидам
--================================================================================================================================================================================--
--================================================================================================================================================================================--
--СЛ0573567
--формирет список заявок для дальнейшей выборки в запросах
drop table if exists temp_znt;
create temp table temp_znt
as
select distinct znt
from temp_soika_ml
;
create index ci_znt on temp_znt(znt);
--select count(*) from temp_znt
--================================================================================================================================================================================--
--================================================================================================================================================================================--
drop table if exists temp_st_edo;
create temp table temp_st_edo 
as
with c1 as (select edo.dokument , sedo.name as status_edo
			, row_number() over (partition by edo.dokument order by edo."period" desc) as rn
			from erp_buh_work.sr_statusy_edo_dokumentov as edo
			left join erp_buh_work.ref_sr_statusy_edo_dokumentov as sedo on sedo.ssylka = edo.status 
			where edo.dokument <> '00000000-0000-0000-0000-000000000000')
select dokument , status_edo 
from c1
where c1.rn=1;
create index ci_edo on temp_st_edo(dokument);
--================================================================================================================================================================================--
--================================================================================================================================================================================--
drop table if exists temp_bum;
create temp table temp_bum 
as
with c1 as (select r.dokument, r."period", s."name" as status_na_bumage
			, row_number() over (partition by r.dokument order by r."period" desc) as rn
			from erp_buh_work.sr_statusy_bumazhnykh_dokumentov as r
			join erp_buh_work.ref_sr_statusy_bumazhnykh_dokumentov as s on s.ssylka = r.status)
select dokument, "period", status_na_bumage
from c1
where rn=1;
create index ci_bum on temp_bum(dokument);
--================================================================================================================================================================================--
--================================================================================================================================================================================--
drop table if exists temp_rtu;
create temp table temp_rtu 
as
select 
  coalesce(ml.nomer, '         ') || '/' || coalesce(z.nomer, '         ') as kluch
, 'РТУ №' || trim(tu.nomer) || ' от ' || tu."data"::date as realizatsiya_tovarov_uslug
, tu."data" as data_rtu
--, tu.status as status___ -- ?????
--, tu.sr_status as status_rtu
, sr."name" as status_rtu
, tu.summa_vzaimoraschetov as summa_rtu
, st_e.status_edo
, b.status_na_bumage
, ml.nomer as mmm
, z.nomer as zzz
--,
--*
--select *
from erp_buh_work.realizatsiya_tovarov_uslug as tu
left join erp_buh_work.sr_marshrutnyy_list as ml on ml.ssylka = tu.sr_marshrutnyy_list
left join erp_buh_work.sr_marshrutnyy_list_zayavki as mlz on mlz.ssylka = tu.sr_marshrutnyy_list and mlz.zayavka_na_transport = tu.sr_zayavka_na_transport 
left join erp_buh_work.sr_zayavka_na_transport as z on z.ssylka = mlz.zayavka_na_transport
left join temp_st_edo as st_e on st_e.dokument = tu.ssylka 
 --left join erp_buh_work.kontragenty k on k.ssylka = tu.kontragent 
left join temp_bum as b on b.dokument = tu.ssylka
left join erp_buh_work.sr_statusy_realizatsiy as sr on sr.ssylka = tu.sr_status
--where 
--tu."data" > '2023-01-01 00:00:00' 
--and 
--trim(tu.nomer) = 'NZS307746/0704-00001'
--and 
--k.sr_kontragent_dlya_edo <> '00000000-0000-0000-0000-000000000000'
where tu.sr_zayavka_na_transport in (select znt from temp_znt);
create index ci_rtu on temp_rtu(kluch);
--select * from temp_rtu
--================================================================================================================================================================================--
--================================================================================================================================================================================--
--Soika/SharePoint

drop table if exists temp_soika;
create temp table temp_soika 
as
with c1 as (select coalesce(ml.nomer, '         ') || '/' || coalesce(z.nomer, '         ') as kluch
			, ml.nomer as nomer_ml
			, z.nomer as znt
			, guid_paketa
			, vd.sld
			, vd.data_postupleniya_iz_soyki
			, vd.validnyy
			, vd.kommentariy_oshibki
			, vd.kommentariy_otkloneniya
			, vd.ssylka_na_tn
			, vd.obrabotano
			, row_number() over (partition by ml.nomer || '/' || z.nomer order by validnyy desc, data_postupleniya_iz_soyki desc) as rn
			--select *
			from erp_buh_work.sr_vkhodyashchie_dannye_po_tn_zayavok_na_transport as vd
			left join erp_buh_work.sr_marshrutnyy_list as ml on ml.ssylka = vd.marshrutnyy_list
			left join erp_buh_work.sr_marshrutnyy_list_zayavki as mlz on mlz.ssylka = vd.marshrutnyy_list and mlz.zayavka_na_transport = vd.zayavka_na_transport 
			left join erp_buh_work.sr_zayavka_na_transport as z on z.ssylka = mlz.zayavka_na_transport)
select kluch
	 , nomer_ml
	 , znt
	 , guid_paketa
	 , sld
	 , data_postupleniya_iz_soyki
	 , validnyy
	 , kommentariy_oshibki
	 , kommentariy_otkloneniya
	 , obrabotano
	 , ssylka_na_tn
from c1 
where rn = 1
;
create index ci_soika on temp_soika(kluch);
--select * from temp_soika


drop table if exists temp_final;
create temp table temp_final 
as
select ml.Data_ML
, ml.kluch kluch_ml
, ml.Nomer_ML
, ml.Nomer_ZNT
, ml.Zayavka_Po_4PL
, ml.platelshchik_perevozki
, ml.status
, ml.vid_dostavki
, ml.sr_punkt_pogruzki
, ml.sr_punkt_vygruzki
, ml.Punkt_Vigruzki_Soika
, case when extract(year from ml.data_zagruzki_fakticheskaya) < 1000 then ml.data_zagruzki_fakticheskaya + '2000 year'::interval else ml.data_zagruzki_fakticheskaya end
, case when extract(year from ml.data_vigruzki_fakticheskaya) < 1000 then ml.data_vigruzki_fakticheskaya + '2000 year'::interval else ml.data_vigruzki_fakticheskaya end
, case when extract(year from ml.Vigruzka_Fakticheskaya) < 1000 then ml.Vigruzka_Fakticheskaya + '2000 year'::interval else ml.Vigruzka_Fakticheskaya end
--, mlz.ssylka as ml
--, mlz.zayavka_na_transport as znt
, case when s.kluch is not null then 'да' else 'нет' end poluchena_iz_tn_soica
, case when s.obrabotano is true then 'да' else 'нет' end otrabotana_v_1c
--, '###################################' as vtoroy_blok
, s.kluch kluch_soika
--, s.nomer_ml as nomer_ml_soika
--, s.znt as nomer_znt_soika
, s.guid_paketa
, s.sld
, s.data_postupleniya_iz_soyki
, coalesce(s.validnyy, false)
, s.kommentariy_oshibki
, s.kommentariy_otkloneniya
--, s.obrabotano
, s.ssylka_na_tn
--, '###################################' as tretiy_blok
, r.kluch kluch_rtu
, r.realizatsiya_tovarov_uslug
, r.data_rtu
, r.status_rtu
, r.summa_rtu
, r.status_edo
, r.status_na_bumage
, c.year_month
, c.year_week
, c."date"
from temp_soika_ml as ml
join dict.calendar as c on c."date" = ml.Data_ML::date
left join temp_soika as s on trim(s.kluch) = trim(ml.kluch)
left join temp_rtu as r on trim(r.kluch) = trim(ml.kluch)
where ml.sr_punkt_vygruzki <> '' --пожелние Яны Красновой, письмо 20.07.2023 15:49, получатели Номоконов С.В., Полусмаков С.В.
;
--select * from temp_final;


--drop table if exists reports.t_soika;
--create table reports.t_soika 
--as 
--select * from temp_final;
--GRANT UPDATE, SELECT ON TABLE reports.t_soika TO readonly;
--GRANT UPDATE, SELECT ON TABLE reports.t_soika TO mustafinrf;
--GRANT UPDATE, SELECT ON TABLE reports.t_soika TO kulyabin;
--GRANT UPDATE, SELECT ON TABLE reports.t_soika TO grishinam;


truncate reports.t_soika;
insert into reports.t_soika
select *
from temp_final;
--select * from reports.t_soika

end 
$procedure$
;
