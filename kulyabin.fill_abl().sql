-- DROP PROCEDURE kulyabin.fill_abl();

CREATE OR REPLACE PROCEDURE kulyabin.fill_abl()
 LANGUAGE plpgsql
AS $procedure$

begin
	
-- Заказы поставщику
	
drop table if exists temp_zakaz;	
create temp table temp_zakaz as 
select
	id_zakaz,
	zakaz,
	dzo,
	nomenklatura,
	spetsifikatsiya_tmkh,
	--data_potrebnosti,
	kolichestvo
from
	(
	select 
		id_zakaz,
		zakaz,
		dzo,
		nomenklatura,
		spetsifikatsiya_tmkh,
		--data_potrebnosti,
		sum(kolichestvo) as kolichestvo
	from 
		(
		select 
			z.ssylka as id_zakaz,
			zt.zakaz ,
			zt.dzo ,
			zt.nomenklatura ,
			zt.spetsifikatsiya_tmkh ,
			zt.kolichestvo
			--date(zt.data_postavki) as data_potrebnosti
		from
			upp_abl.zakaz_postavshchiku z
			left join upp_abl.zakaz_postavshchiku_tovary zt on zt.ssylka = z.ssylka
		where
			z.pometka_udaleniya is false 
			and z.proveden is true
			and zt.zakaz notnull
			and zt.zakaz != '00000000-0000-0000-0000-000000000000'
	union all
		select
			z.ssylka ,
			kt.zakaz ,
			kt.dzo ,
			kt.nomenklatura ,
			kt.spetsifikatsiya_tmkh ,
			kt.kolichestvo
			--date(kt.data_postavki) as data_potrebnosti
		from 
			upp_abl.korrektirovka_zakaza_postavshchiku k
			left join upp_abl.korrektirovka_zakaza_postavshchiku_tovary kt on kt.ssylka = k.ssylka
			left join upp_abl.zakaz_postavshchiku z on z.ssylka = k.zakaz_postavshchiku
		where
			z.pometka_udaleniya is false
			and z.proveden is true
			and k.pometka_udaleniya is false
			and k.proveden is true
			and kt.zakaz notnull
			and kt.zakaz != '00000000-0000-0000-0000-000000000000'
		) as z1
	group by 
		id_zakaz, 
		zakaz,
		dzo,
		nomenklatura,
		spetsifikatsiya_tmkh
		--data_potrebnosti
	) as z2
where kolichestvo > 0;

-- 1.1-1.2 Заказы поствщику кратко

drop table if exists temp_zakaz2;	
create temp table temp_zakaz2 as
select
	zp.ssylka as id_zakaz,
	date(zp."data") as data_zakaz,
	zp.nomer as nomer_zakaz,
	k.naimenovanie as agregator_zakaz,
	k2.naimenovanie as postavshik_zakaz
from 
	upp_abl.zakaz_postavshchiku zp
	left join upp_abl.kontragenty k on k.ssylka = zp.kontragent
	left join upp_abl.kontragenty k2 on k2.ssylka = zp.gruzootpravitel
where 
	zp.pometka_udaleniya is false
	and zp.proveden is true;
--select * from temp_zakaz2 where nomer_zakaz = '00222173343' 

-- 1. Заявка на транспорт 2. Маршрутный лист

drop table if exists temp_znt;	
create temp table temp_znt as
select
	znt.ssylka as id_znt,
	znt.nomer as nomer_znt,
	date(znt."data") as data_znt,
	--znts.status as status_znt_abl,
	case when znt.nomer like 'ALP%' then coalesce (st.status, znts.status) else znts.status end as status_znt_abl,
	st.status as status_znt,
	znt.dokument_osnovanie as id_zakaz,
	znt.gruzopoluchatel as id_dzo,
	zntt.nomenklatura as id_nomenklatura,
	n.naimenovanie_polnoe  as nomenklatura_znt,
	zntt.artikul as skmtr_znt,
	zntt.kod_nsi as nsi_znt,
	sum(zntt.kolichestvo_zakaz) as kolichestvo_znt,
	sum(zntt.kolichestvo_zakaz_plan) as kolichestvo_plan_znt  ,
	sum(zntt.summa) as summa_znt,
	--zntt.spetsifikatsiya_tmkh ,	
	--date(zntt.data_postavki) as data_potrebnosti,
	date(znt.data_zagruzki) as data_zagruzki_znt,
	date(znt.data_vygruzki_plan) as data_vygruzki_plan_znt,
	k2.naimenovanie as postavshik,
	k.naimenovanie as potrebitel_znt,
	ml.route_list_number as nomer_ml,
	date(ml.route_list_date) as data_ml,
	ml.upp_status as status_ml,
	ml.location_short as info,
	date(ml.fact_loading_date) as data_pogruzki_fact,
	date(ml.fact_unloading_date) as data_vygruzki_fact
from 
	upp_abl.zayavka_na_transport znt
	left join upp_abl.zayavka_na_transport_zayavki zntt on zntt.ssylka = znt.ssylka
	left join upp_abl.statusy_zayavok_na_transport znts on znts.ssylka = znt.lk_status_soglasovaniya 
	left join upp_abl.kontragenty k on k.ssylka = znt.gruzopoluchatel
	left join upp_abl.kontragenty k2 on k2.ssylka = znt.gruzootpravitel
	left join upp_abl.nomenklatura n on n.ssylka = zntt.nomenklatura
	left join (
				select 
					zayavka_na_transport as id_znt,
					r.nomer as nomer_znt ,
					st."name" as status 
				from (
					select *, 
						row_number () over (partition by zayavka_na_transport order by fakticheskoe_vremya_ustanovki_statusa desc) as rang
					from upp_work.history_statusy_zayavok_na_transport s
					) as cte
				left join upp_work.statusy_zayavok_na_transport st on st.ssylka = cte.status_zayavki
				left join upp_work.zayavka_na_transport r on r.ssylka = cte.zayavka_na_transport
				where rang = 1 and r.nomer notnull
				and r.pometka_udaleniya is false 
				and r.proveden is true
				union 
				select
					z.ssylka ,
					z.nomer ,
					'Доставлен' as status
				from upp_abl.zayavka_na_transport z
					left join erp_sld.prihodnyy_order_na_tovary po on po.sr_nomer_zayavki_na_transport = z.nomer
				where z.nomer like 'ALP%'
					and po.nomer is not null
				) as st on st.nomer_znt = znt.nomer
	left join psv.t_search_of_request ml on ml.request_number = znt.nomer 
	--left join salair_upp.request_status st on st.guid = znt.lk_status_soglasovaniya 
where
	znt.pometka_udaleniya is false 
	and znt.proveden is true
	--and st."name" <> 'Отменено заказчиком' or st."name" is null
group by
	znt.ssylka ,
	znt.nomer ,
	date(znt."data"),
	znts.status,
	st.status,
	znt.dokument_osnovanie,
	znt.gruzopoluchatel,
	zntt.nomenklatura ,
	n.naimenovanie_polnoe,
	zntt.artikul,
	zntt.kod_nsi,
	--zntt.spetsifikatsiya_tmkh ,
	zntt.zakaz_postavshchiku ,
	--date(zntt.data_postavki) ,
	date(znt.data_zagruzki),
	date(znt.data_vygruzki_plan),
	k2.naimenovanie,
	k.naimenovanie,
	ml.route_list_number,
	date(ml.route_list_date),
	ml.upp_status ,
	ml.location_short ,
	date(ml.fact_loading_date),
	date(ml.fact_unloading_date);
--select * from temp_znt where temp_znt.nomer_znt = 'ALP111879'

-- 4. Ожидаемые послупления

drop table if exists temp_op;	
create temp table temp_op as
select 
	op.ssylka  as id_op,
	op."nomer" as nomer_op,
	date(op."data") as data_op,
	date(op.data_vygruzki_plan) as data_vygruzki_plan_op,
	date(op.data_fakticheskoy_vygruzki) as data_vygruzki_fact_op,
	op.zayavka_na_transport as id_znt,
	r.request_number as nomer_znt,
	op.marshrutnyy_list as id_ml,
	op.nomer_marshrutnogo_lista as nomer_ml_op,
	op.status_marshrutnogo_lista as status_ml_op,
	sop.status as status_op,
	op.gruzopoluchatel  as gruzopoluchatel_op,
	s.naimenovanie as sklad_op,
	op.sr_priemka as id_priemka
from 
	erp_sld.sr_ozhidaemoe_postuplenie op
	left join erp_sld.sr_statusy_podtverzhdeniya_ozhidaemogo_prikhoda sop on sop.ssylka = op.status
	left join erp_sld.sklady s on s.ssylka = op.sklad
	left join salair_upp.requests r on r.guid = op.zayavka_na_transport 
where 	
	op.pometka_udaleniya is false 
	and op.proveden  is true;
--select * from temp_op where nomer_op = '00000406648'


-- 6. Приходные ордера

drop table if exists temp_po;	
create temp table temp_po as
select 
	p.ssylka as id_po,
	p."_dokument_osnovanie" as id_priemka,
	pt.nomenklatura as id_nomenklatura,
	p."nomer" as nomer_po,
	date(p."data") as data_po,
	spo.status as status_po, 
	sp.naimenovanie  as pomeshchenie_po ,
	so.operatsiya,
	p.sr_nomer_zayavki_na_transport as nomer_znt_po,
	n.naimenovanie_polnoe as nomenklatura_po,
	n.nsi_kod as nsi_po,
	n.artikul as skmtr_po,
	sum(pt.kolichestvo) as kolichestvo_po
	--date(nz.sr_data_vovlecheniya) as data_potrebnosti_po
from 
	erp_sld.prihodnyy_order_na_tovary p
	left join erp_sld.prihodnyy_order_na_tovary_tovary pt on pt.ssylka = p.ssylka
	left join erp_sld.skladskie_pomeshcheniya sp on sp.ssylka  = p.pomeshchenie
	left join erp_sld.sklady s on s.ssylka  = p.sklad
	left join erp_sld.statusy_prikhodnykh_orderov spo on spo.ssylka = p.status
	left join erp_sld.skladskie_operatsii so on so.ssylka = p.skladskaya_operatsiya
	left join erp_sld.nomenklatura n on n.ssylka = pt.nomenklatura
	--left join erp_sld.naznacheniya nz on nz.ssylka = pt.naznachenie 
where 
	p.pometka_udaleniya  is false 
	and p.proveden  is true 
	and s.sr_vid_ispolzovaniya  = '22d54fe7-3354-44f3-8bcf-88baedfe6122' --4PL
	and p.skladskaya_operatsiya = '24c9c854-7d31-42b7-9a8a-e1011681a9ea' -- Приемка от поставщика
group by 
	p.ssylka ,
	p."_dokument_osnovanie" ,
	pt.nomenklatura ,
	p."nomer" ,
	date(p."data") ,
	spo.status , 
	sp.naimenovanie  ,
	so.operatsiya,
	p.sr_nomer_zayavki_na_transport,
	n.naimenovanie_polnoe,
	n.nsi_kod,
	n.artikul
	--nz.sr_data_vovlecheniya
	;
--select * from temp_po where nomer_po = '00-00036027'

-- 5. Приемка

drop table if exists temp_priemka;	
create temp table temp_priemka as
select
	p.ssylka  as id_priemka,
	p.sr_ozhidaemoe_postuplenie as id_op,
	pt.nomenklatura as id_nomenklatura,
	p."nomer" as nomer_priemka,
	date(p."data") as data_priemka,
	s.naimenovanie as sklad_priemka,
	k.naimenovanie as postavshik_priemka,
	n.naimenovanie_polnoe as nomenklatura_priemka,
	n.nsi_kod as nsi_priemka,
	n.artikul as skmtr_priemka,
	sum(pt.kolichestvo) as kolichestvo_priemka,
	ei.naimenovanie as ed_izm_priemka,
	sum(pt.summa) as summa_priemka,
	temp_po.nomer_znt_po
	--date(nz.sr_data_vovlecheniya) as data_potrebnosti_priemka 
from 
	erp_sld.priemka_tovarov_na_khranenie p
	left join erp_sld.sklady s on s.ssylka = p.sklad
	left join erp_sld.priemka_tovarov_na_khranenie_tovary pt on pt.ssylka = p.ssylka
	left join erp_sld.nomenklatura n on n.ssylka = pt.nomenklatura
	left join erp_sld.upakovki_edinitsy_izmereniya ei on ei.ssylka = n.edinitsa_izmereniya 
	--left join erp_sld.naznacheniya nz on nz.ssylka = pt.naznachenie 
	left join erp_sld.kontragenty k on k.ssylka = p.kontragent
	left join temp_po on (temp_po.id_priemka = p.ssylka  and temp_po.id_nomenklatura = pt.nomenklatura /*and temp_po.data_potrebnosti_po = date(nz.sr_data_vovlecheniya*/) 
where 
	p.pometka_udaleniya  is false 
	and p.proveden  is true
	and s.sr_vid_ispolzovaniya = '22d54fe7-3354-44f3-8bcf-88baedfe6122' -- 4PL
group by 
	p.ssylka ,
	p.sr_ozhidaemoe_postuplenie ,
	pt.nomenklatura ,
	p."nomer" ,
	date(p."data") ,
	s.naimenovanie ,
	k.naimenovanie,
	n.naimenovanie_polnoe,
	n.nsi_kod,
	n.artikul,
	ei.naimenovanie,
	temp_po.nomer_znt_po
	--date(nz.sr_data_vovlecheniya)
	;

--select * from temp_priemka where nomer_priemka = 'СЛ00-021813'

-- 7-8. Палнируемые и фактические поступления

drop table if exists temp_postuplenie;	
create temp table temp_postuplenie as
select 
	fp.sr_dokument_postavki as id_priemka,
	pp.nomer as nomer_pp,
	date(pp."data") as data_pp,
	pp.status as status_pp,
	fp.nomer as nomer_fp,
	date(fp."data") as data_fp
	--date(fpf.data_sozdaniya) as data_uvedomleniya
from
	erp_sld.sr_planiruemye_postupleniya_4pl pp
	join erp_sld.sr_fakticheskoe_postuplenie_4pl fp on fp.sr_planiruemoe_postuplenie = pp.ssylka
	--left join erp_sld.sr_fakticheskoe_postuplenie_4pl_prisoedinennye_fayly fpf on fpf.vladelets_fayla  = fp.ssylka  
where 
	pp.pometka_udaleniya is false 
	and pp.proveden is true
	and fp.pometka_udaleniya is false
	and fp.proveden is true;
--select * from temp_postuplenie where nomer_pp = '000073228'
--select * from temp_postuplenie where nomer_fp = '150166'

-- 9. Неотфактуровки

drop table if exists temp_nf;	
create temp table temp_nf as
select 
	nf.ssylka as id_nf,
	nf.nomer as nomer_nf,
	date(nf."data") as data_nf,
	nf.nomer_znt as znt_nf,
	nf.nomer_spetsifikatsii as specifikaciya_nf,
	nf.nomer_priemki as nomer_priemki_nf
from 
	abl_buh.sr_neotfakturovki_wms nf
where 
	nf.pometka_udaleniya is false 
	and nf.proveden is true ;
--select * from temp_nf where nomer_nf ='ALS061976'

-- 10. Ордер на перемещение

drop table if exists temp_onp;	
create temp table temp_onp as
select
	onp.ssylka as id_onp,
	onp.nomer as nomer_onp,
	date(onp."data") as data_onp,
	s.naimenovanie as sklad_onp,
	sp1.naimenovanie as pomeshchenie_otpravitel_onp ,
	sp2.naimenovanie as pomeshchenie_poluchatel_onp,
	sonp.status as status_onp ,
	date(onp.data_otgruzki) as  data_otgruzki_onp,
	n.naimenovanie_polnoe as nomenklatura_onp,
	n.nsi_kod as nsi_onp,
	n.artikul as skmtr_onp,
	--date(nz.sr_data_vovlecheniya) as data_potrebnosti,
	sum(onpt.kolichestvo) as kolichestvo_onp 
	--nz.sr_spetsifikatsiya as specifikaciya_onp
from 
	erp_sld.order_na_peremeshchenie_tovarov onp
	left join erp_sld.order_na_peremeshchenie_tovarov_otgruzhaemye_tovary onpt on onpt.ssylka = onp.ssylka
	--left join erp_sld.naznacheniya nz on nz.ssylka = onpt.naznachenie
	left join erp_sld.skladskie_pomeshcheniya sp1 on sp1.ssylka = onp.pomeshchenie_otpravitel 
	left join erp_sld.skladskie_pomeshcheniya sp2 on sp2.ssylka = onp.pomeshchenie_poluchatel
	left join erp_sld.nomenklatura n on n.ssylka = onpt.nomenklatura
	left join erp_sld.sklady s on s.ssylka = onp.sklad 
	left join erp_sld.statusy_orderov_na_peremeshchenie sonp on sonp.ssylka = onp.status
where 
	onp.pometka_udaleniya is false 
	and onp.proveden is true
group by 
	onp.ssylka ,
	onp.nomer,
	date(onp."data"),
	s.naimenovanie,
	sp1.naimenovanie ,
	sp2.naimenovanie,
	sonp.status ,
	date(onp.data_otgruzki),
	n.naimenovanie_polnoe,
	n.nsi_kod,
	n.artikul;

-- 12. Размещение товаров в ячейки хранения (Не нашел связку с документами поставки)

drop table if exists temp_rt;	
create temp table temp_rt as
select
	rt.nomer as nomer_rt,
	date(rt."data") as data_rt,
	s.status as status_rt,
	rtr.nomenklatura as id_nomenklatura,
	n.nsi_kod as nsi_rt ,
	n.artikul as skmtr_rt,
	n.naimenovanie_polnoe as nomenklatura_rt,
	--date(nz.sr_data_vovlecheniya) as data_potrebnosti, 
	rtr.sr_prikhodnyy_dokument as id_po
from 
	erp_sld.otbor_razmeshchenie_tovarov rt
	left join erp_sld.otbor_razmeshchenie_tovarov_tovary_razmeshchenie rtr on rtr.ssylka = rt.ssylka
	left join erp_sld.statusy_otborov_razmeshcheniy_tovarov s on s.ssylka = rt.status
	left join erp_sld.nomenklatura n on n.ssylka  = rtr.nomenklatura
	--left join erp_sld.naznacheniya nz on nz.ssylka = rtr.naznachenie 
where 
	rt.pometka_udaleniya  is false 
	and rt.proveden  is true
	and rt.vid_operatsii = '9770b3d2-99ef-4dc2-b5ac-9aa33b80bfa7' -- Размещение
	and rtr.sr_prikhodnyy_dokument !='00000000-0000-0000-0000-000000000000';

-- 13. Поступление товаров и услуг по неотфактуровке

drop table if exists temp_ptu;	
create temp table temp_ptu as
select distinct 
	ptu.ssylka as id_ptu,
	ptu.nomer as nomer_ptu,
	date(ptu."data") as data_ptu,
	s.naimenovanie as sklad_ptu ,
	ptut.nomenklatura as id_nomenklatura,
	ptu.sr_eto_neotfakturovka ,
	ptu.sr_neotfakturovka_wms as id_nf_erp, --id Планируемое поступление
	ptu.nomer_vkhodyashchego_dokumenta as nomer_upd,
	date(ptu.data_vkhodyashchego_dokumenta) as data_upd,
	nptu.nomer_nptu as nomer_nptu,
	nptu.data_nptu as data_nptu
from 
	abl_buh.postuplenie_tovarov_uslug ptu
	left join abl_buh.postuplenie_tovarov_uslug_tovary ptut on ptut.ssylka = ptu.ssylka 
	left join abl_buh.sklady s on s.ssylka = ptu.sklad
	left join (
				select
					nptu.ssylka as id_nptu,
					nptut.nomenklatura as id_nomenklatura,
					nptu.nomer as nomer_nptu,
					date(nptu."data") as data_nptu
				from 
					abl_buh.postuplenie_tovarov_uslug nptu
					left join abl_buh.postuplenie_tovarov_uslug_tovary nptut on nptut.ssylka = nptu.ssylka
				where 
					nptu.pometka_udaleniya is false 
					and nptu.proveden is true
					and nptu.sr_eto_neotfakturovka is true
				) as nptu on (nptu.id_nptu = ptu.sr_dokument_neotfakturovki and nptu.id_nomenklatura = ptut.nomenklatura)
where 
	ptu.pometka_udaleniya is false 
	and ptu.proveden is true
	and ptu.sr_eto_neotfakturovka is false
;

	-- select * from temp_ptu where nomer_ptu = '00AB-0000000000000000000102202'

drop table if exists temp_final;	
create temp table temp_final as
select 
	z2.nomer_zakaz,
	z2.data_zakaz,
	z2.agregator_zakaz,
	z2.postavshik_zakaz,
	znt.postavshik,
	znt.potrebitel_znt,
	znt.nomer_znt,
	znt.data_znt,
	znt.status_znt_abl,
	znt.status_znt,
	znt.nomenklatura_znt,
	znt.skmtr_znt, 
	znt.nsi_znt,
	znt.kolichestvo_plan_znt,
	znt.kolichestvo_znt,
	znt.summa_znt,
	--znt.data_potrebnosti,
	znt.nomer_ml,
	znt.data_ml,
	znt.status_ml,
	znt.info,
	znt.data_pogruzki_fact,
	case when znt.status_znt = 'Доставлен' then date(znt.data_vygruzki_fact) else null end as data_vigruzki_fact,
	op.nomer_op,
	op.data_op,
	op.sklad_op,
	op.status_ml_op as status_ml_erp,
	date(op.data_vygruzki_plan_op) as data_vygruzki_plan_erp,
	case when znt.status_znt = 'Доставлен' then date(op.data_vygruzki_fact_op) else null end as data_vygruzki_fact_erp,
	pr.nomer_priemka,
	pr.data_priemka,
	pr.nomenklatura_priemka,
	pr.skmtr_priemka,
	pr.kolichestvo_priemka,
	pr.ed_izm_priemka,
	--pr.data_potrebnosti_priemka,
	po.nomer_po,
	po.data_po,
	po.status_po,
	po.pomeshchenie_po,
	po.nomer_znt_po,
	po.nomenklatura_po ,
	po.skmtr_po,
	po.kolichestvo_po,
	p.nomer_pp,
	p.data_pp,
	p.status_pp,
	p.nomer_fp,
	p.data_fp,
	nf.nomer_nf,
	nf.data_nf,
	nf.specifikaciya_nf,
	ptu.nomer_ptu,
	ptu.data_ptu,
	ptu.sklad_ptu,
	ptu.nomer_upd,
	ptu.data_upd,
	ptu.nomer_nptu,
	ptu.data_nptu
	--p.data_uvedomleniya
from 
	temp_znt znt
	left join temp_zakaz2 z2 on z2.id_zakaz = znt.id_zakaz
	--left join psv.t_search_of_request ml on ml.request_number = znt.nomer_znt  -- 2-3. Маршрутный лист
	left join temp_op op on (op.nomer_znt = znt.nomer_znt and op.nomer_ml_op = znt.nomer_ml)
	left join temp_priemka pr on (pr.nomer_znt_po = znt.nomer_znt and pr.nsi_priemka = znt.nsi_znt /*and pr.data_potrebnosti_priemka = znt.data_potrebnosti*/)
	left join temp_po po on (po.nomer_znt_po = znt.nomer_znt and po.nsi_po = znt.nsi_znt /*and po.data_potrebnosti_po = znt.data_potrebnosti*/)
	left join temp_postuplenie p on p.id_priemka = pr.id_priemka
	left join temp_nf nf on (nf.nomer_priemki_nf = pr.nomer_priemka and nf.znt_nf = pr.nomer_znt_po)
	left join temp_ptu ptu on (ptu.id_nf_erp = nf.id_nf and ptu.id_nomenklatura = znt.id_nomenklatura)
 ;

--select * from temp_final where nomer_znt = 'ALS013767'

--drop table if exists kulyabin.t_abl 						--Удаляет старую таблицу
--create table kulyabin.t_abl as select * from temp_final; 	--Создает таблицу с новой струкурой
truncate table kulyabin.t_abl;								--Очищает таблицу без сканирования строк
insert into kulyabin.t_abl									--Записывает данные в таблицу
select * from temp_final;

end;
$procedure$
;
