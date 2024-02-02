-- DROP PROCEDURE reports.fill_ptnh();

CREATE OR REPLACE PROCEDURE reports.fill_ptnh()
 LANGUAGE plpgsql
AS $procedure$
begin

--Приемка------------------------------------------------------------------------------------------------------------------------------------
	
drop table if exists temp_priemka;
create temp table temp_priemka
as
select 
	pr_1.*,
	coalesce (string_agg(distinct fpf.rasshirenie ,','), string_agg(distinct foh.rasshirenie ,','))  as rasshirenie,
	case when coalesce (string_agg(distinct fpf.rasshirenie ,','), string_agg(distinct foh.rasshirenie ,',')) is not null then 'ДА' else 'НЕТ' end as file
		FROM (
				SELECT p.sklad,
				       p.ssylka,
				       p1.naimenovanie as menedzher,
				       p.nomer,
				       p.data,
				       pt.nomenklatura,
				       n.naimenovanie_polnoe AS nomenklatura_polnoe,
				       sum(pt.kolichestvo) AS kolichestvo
				FROM erp_sld.priemka_tovarov_na_khranenie p
				LEFT JOIN erp_sld.priemka_tovarov_na_khranenie_tovary pt ON p.ssylka = pt.ssylka
				LEFT JOIN erp_sld.nomenklatura n ON n.ssylka = pt.nomenklatura
				LEFT JOIN erp_sld.polzovateli p1 on p1.ssylka = p.menedzher
				WHERE p.pometka_udaleniya IS FALSE AND p.proveden IS TRUE
				GROUP BY p.sklad, p.ssylka, p.nomer, p.data, pt.nomenklatura, n.naimenovanie_polnoe, p1.naimenovanie
				) pr_1
	LEFT JOIN erp_sld.sr_fakticheskoe_postuplenie_4pl fp on pr_1.ssylka = fp.sr_dokument_postavki
	LEFT JOIN erp_sld.sr_fakticheskoe_postuplenie_4pl_prisoedinennye_fayly fpf on fpf.vladelets_fayla = fp.ssylka
	LEFT JOIN erp_sld.priemka_tovarov_na_khranenie_prisoedinennye_fayly foh on foh.vladelets_fayla = pr_1.ssylka
	GROUP BY pr_1.sklad,pr_1.ssylka,pr_1.nomer,pr_1."data",pr_1.nomenklatura,pr_1.nomenklatura_polnoe,pr_1.kolichestvo, pr_1.menedzher
;

create index ci_priemka on temp_priemka(ssylka, nomenklatura);
--select * from temp_priemka where nomer = '00-00485531'

--Приемка------------------------------------------------------------------------------------------------------------------------------------
--=========================================================================================================================================--
--Приходный_ордер----------------------------------------------------------------------------------------------------------------------------

drop table if exists temp_po;
create temp table temp_po
as
with po as (SELECT po_1.sklad,
			       po_1.ssylka,  -- размещение
			       p2.naimenovanie as menedzher, 
			       po_1.nomer,
			       po_1."data",
			       po_1.rasporyazhenie,  -- приемка
			       pot.nomenklatura,
			       n.naimenovanie_polnoe as nomenklatura_polnoe,
			       sum(pot.kolichestvo) AS kolichestvo,
			       n.nsi_kod,
			       p.ispolzovat_adresnoe_khranenie 
			FROM erp_sld.prihodnyy_order_na_tovary po_1
			LEFT JOIN erp_sld.prihodnyy_order_na_tovary_tovary pot ON po_1.ssylka = pot.ssylka
			LEFT JOIN erp_sld.nomenklatura n ON n.ssylka = pot.nomenklatura
			LEFT JOIN erp_sld.ks_nsi kn on kn.kod_nsi = n.nsi_kod 
			LEFT JOIN erp_sld.skladskie_pomeshcheniya p on p.ssylka = po_1.pomeshchenie
			LEFT JOIN erp_sld.polzovateli p2 on p2.ssylka = po_1.otvetstvennyy
			WHERE po_1.pometka_udaleniya IS FALSE 
				AND po_1.proveden IS true 
				AND po_1.sr_zakaz_na_peremeshchenie = '00000000-0000-0000-0000-000000000000' 
				AND po_1.skladskaya_operatsiya = '24c9c854-7d31-42b7-9a8a-e1011681a9ea'
			GROUP BY po_1.sklad, po_1.ssylka, po_1.nomer, po_1.data, po_1.rasporyazhenie, pot.nomenklatura, n.naimenovanie_polnoe, n.nsi_kod, p.ispolzovat_adresnoe_khranenie, p2.naimenovanie)
select po.sklad
	 , po.ssylka  -- размещение
	 , po.menedzher
	 , po.nomer
	 , po."data"
	 , po.rasporyazhenie  -- приемка
	 , po.nomenklatura
	 , po.nomenklatura_polnoe
	 , coalesce(sum(po.kolichestvo) over (partition by rasporyazhenie, nomenklatura),0) as sum_kol
	 , count(po.nomer) over (partition by rasporyazhenie, nomenklatura) as kol_po	 
	 , po.kolichestvo
	 , coalesce(kn.ves_za_ed_kg * po.kolichestvo, 0) as ves_kg_po
	 , coalesce(kn.ves_za_ed_kg * po.kolichestvo / kn.koeffitsient_perescheta_ves_v_obem, 0) as obem_m3_po
	 , coalesce(po.ispolzovat_adresnoe_khranenie,false) as ispolzovat_adresnoe_khranenie
from po
left join erp_sld.ks_nsi kn on kn.kod_nsi = po.nsi_kod
;
create index ci_po_1 on temp_po(ssylka, nomenklatura);
create index ci_po_2 on temp_po(rasporyazhenie, nomenklatura);
--select * from temp_po where rasporyazhenie = 'e4a2dee1-17b2-11ed-83ae-3a68dd4bb987'   

--Приходный_ордер----------------------------------------------------------------------------------------------------------------------------
--=========================================================================================================================================--
--Размещение ОХ---------------------------------------------------------------------------------------------------------------------------------

drop table if exists temp_raz;
create temp table temp_raz
as
SELECT 
	o.ssylka,
	o.sklad,
	first_value (case when ot.sr_prikhodnyy_dokument = '00000000-0000-0000-0000-000000000000' then null else ot.sr_prikhodnyy_dokument end) over (partition by ot.ssylka order by sr_prikhodnyy_dokument desc) as sr_prikhodnyy_dokument ,
	ot.nomenklatura,
	p3.naimenovanie as menedzher,
	o.nomer,
	o."data",	
	n.naimenovanie_polnoe as nomenklatura_polnoe,	
	sum(ot.kolichestvo) AS kolichestvo
FROM erp_sld.otbor_razmeshchenie_tovarov o
LEFT JOIN (
				select
					ort.ssylka ,
					ort.nomenklatura,
					first_value (case when ort.sr_prikhodnyy_dokument = '00000000-0000-0000-0000-000000000000' then null else ort.sr_prikhodnyy_dokument end) over (partition by ort.ssylka order by sr_prikhodnyy_dokument desc) as sr_prikhodnyy_dokument ,
					sum(ort.kolichestvo) AS kolichestvo
				from erp_sld.otbor_razmeshchenie_tovarov_tovary_razmeshchenie ort
				group by
					ort.ssylka,
					ort.nomenklatura,
					ort.sr_prikhodnyy_dokument
			) ot ON ot.ssylka = o.ssylka
LEFT JOIN erp_sld.nomenklatura n ON n.ssylka = ot.nomenklatura
LEFT JOIN erp_sld.polzovateli p3 on p3.ssylka = o.otvetstvennyy
LEFT JOIN (
			select
				po.ssylka ,
				po.sr_peremeshchenie_tovarov
			from erp_sld.prihodnyy_order_na_tovary po
			where 
				po.pometka_udaleniya is false 
				and po.proveden is true 
				and po.sr_peremeshchenie_tovarov = '00000000-0000-0000-0000-000000000000'
				and po.skladskaya_operatsiya = '24c9c854-7d31-42b7-9a8a-e1011681a9ea'
			) po on po.ssylka = ot.sr_prikhodnyy_dokument
WHERE 
	o.pometka_udaleniya IS FALSE 
	AND o.proveden IS TRUE 
	AND o.vid_operatsii = '9770b3d2-99ef-4dc2-b5ac-9aa33b80bfa7'::uuid --размещение 
	AND o.sr_order_na_peremeshchenie = '00000000-0000-0000-0000-000000000000'
	and ot.sr_prikhodnyy_dokument is not null
	AND po.sr_peremeshchenie_tovarov = '00000000-0000-0000-0000-000000000000'
GROUP BY o.sklad, o.ssylka, o.nomer, o."data", n.naimenovanie_polnoe,ot.sr_prikhodnyy_dokument, ot.nomenklatura, ot.ssylka, p3.naimenovanie
;
create index ci_raz_1 on temp_raz(ssylka, nomenklatura);
create index ci_raz_2 on temp_raz(sr_prikhodnyy_dokument, nomenklatura);
--create index ci_raz_3 on temp_raz(sr_order_na_peremeshchenie, nomenklatura);
--select * from temp_raz where nomer = '00-00485531' ; select * from temp_po where nomer = '00-00003019' and nomenklatura_polnoe like 'Ботинки%'

--Размещение ОХ---------------------------------------------------------------------------------------------------------------------------------
--=========================================================================================================================================--
--Планируемое_поступление--------------------------------------------------------------------------------------------------------------------

drop table if exists temp_pp;
create temp table temp_pp
as
select
	pp.ssylka ,
	pp.sr_sklad,
	pp.sr_order_na_peremeshchenie as peremeshenie,
	pp.sr_dokument_postavki as priemka,
	pp.nomer as nomer_pp,
	ot.sr_nomenklatura ,
	sum(ot.kolichestvo) as kol_pp
FROM erp_sld.sr_planiruemye_postupleniya_4pl pp
left JOIN erp_sld.sr_planiruemye_postupleniya_4pl_tablitsa_stroka ot ON ot.ssylka = pp.ssylka
left JOIN erp_sld.nomenklatura n ON n.ssylka = ot.sr_nomenklatura
left join erp_sld.sklady s on s.ssylka = pp.sr_sklad
left join (select
				po.ssylka as id_po,
				po.rasporyazhenie 
			from erp_sld.prihodnyy_order_na_tovary po
			where po.skladskaya_operatsiya = '24c9c854-7d31-42b7-9a8a-e1011681a9ea' -- Приемка от поставщика
				and po.pometka_udaleniya is false 
				and po.proveden is true) po on po.rasporyazhenie = pp.sr_dokument_postavki
WHERE 
	pp.pometka_udaleniya IS FALSE 
	AND pp.proveden IS TRUE 
	AND s.sr_vid_ispolzovaniya = '22d54fe7-3354-44f3-8bcf-88baedfe6122'::uuid --4PL
	AND pp.sr_order_na_peremeshchenie <>'00000000-0000-0000-0000-000000000000'
group by pp.ssylka,pp.sr_sklad,pp.sr_order_na_peremeshchenie,pp.nomer,pp.sr_dokument_postavki,ot.sr_nomenklatura
;
create index ci_pp_1 on temp_pp(peremeshenie, sr_nomenklatura);
create index ci_pp_2 on temp_pp(priemka, sr_nomenklatura);
--select * from temp_pp where nomer = '00-00039196'; select * from temp_raz where nomer = '00-00039196';

--Планируемое_поступление--------------------------------------------------------------------------------------------------------------------
--=========================================================================================================================================--
--Размещение 4PL-----------------------------------------------------------------------------------------------------------------------------

drop table if exists temp_raz_4;
create temp table temp_raz_4
as
select 
	raz4.*,
	pp.ssylka as id_pp,
	pp.priemka as id_priemka
from (
SELECT 
	o.ssylka,
	o.sklad,
	ot.sr_prikhodnyy_dokument ,
	ot.nomenklatura,
	o.sr_order_na_peremeshchenie,
	p4.naimenovanie as menedzher,
	o.nomer,
	o."data",	
	n.naimenovanie_polnoe as nomenklatura_polnoe,	
	sum(ot.kolichestvo) AS kolichestvo
FROM erp_sld.otbor_razmeshchenie_tovarov o
LEFT JOIN erp_sld.otbor_razmeshchenie_tovarov_tovary_razmeshchenie ot ON ot.ssylka = o.ssylka
LEFT JOIN erp_sld.nomenklatura n ON n.ssylka = ot.nomenklatura
LEFT JOIN erp_sld.polzovateli p4 on p4.ssylka = o.otvetstvennyy
LEFT JOIN (
			select
			po.ssylka ,
			po.sr_peremeshchenie_tovarov
		from erp_sld.prihodnyy_order_na_tovary po
		where 
			po.pometka_udaleniya is false 
			and po.proveden is true 
			and po.sr_peremeshchenie_tovarov = '00000000-0000-0000-0000-000000000000'
			) po on po.ssylka = ot.sr_prikhodnyy_dokument 
WHERE 
	o.pometka_udaleniya IS FALSE 
	AND o.proveden IS TRUE 
	AND o.vid_operatsii = '9770b3d2-99ef-4dc2-b5ac-9aa33b80bfa7'::uuid --размещение 
	AND o.sr_order_na_peremeshchenie != '00000000-0000-0000-0000-000000000000' 
	AND (po.sr_peremeshchenie_tovarov = '00000000-0000-0000-0000-000000000000' or po.sr_peremeshchenie_tovarov is null)
GROUP BY o.sklad, o.ssylka, o.nomer, o."data", ot.sr_prikhodnyy_dokument, n.naimenovanie_polnoe, ot.nomenklatura, o.sr_order_na_peremeshchenie, p4.naimenovanie) raz4
left join temp_pp pp on pp.peremeshenie = raz4.sr_order_na_peremeshchenie and pp.sr_nomenklatura = raz4.nomenklatura
WHERE pp.ssylka is not null or pp.ssylka != '00000000-0000-0000-0000-000000000000'
;
create index ci_raz_4_1 on temp_raz_4(ssylka, nomenklatura);
create index ci_raz_4_2 on temp_raz_4(sr_prikhodnyy_dokument, nomenklatura);
--create index ci_raz_3 on temp_raz(sr_order_na_peremeshchenie, nomenklatura);
--select * from temp_raz_4 where nomer = '00-00039196' ; select * from temp_po where nomer = '00-00003019' and nomenklatura_polnoe like 'Ботинки%'

--Размещение 4PL-----------------------------------------------------------------------------------------------------------------------------
--=========================================================================================================================================--
--Склады-------------------------------------------------------------------------------------------------------------------------------------

drop table if exists temp_ks;
create temp table temp_ks
as
SELECT s.ssylka AS id_sklad,
	s.naimenovanie AS sklad,
	CASE
	    WHEN s.naimenovanie::text ~~ '%4%PL%'::text THEN '4PL'::text
	    ELSE 'ОХ'::text
	END AS "4pl/oh",
	
	CASE
	    WHEN s.naimenovanie::text ~~ '%АБЛ%'::text THEN 'АБЛ'::text
	    WHEN s.naimenovanie::text ~~ '%Рэйл%'::text THEN 'АБЛ'::text
	    WHEN s.naimenovanie::text ~~ '%Делоникс%'::text THEN 'АБЛ'::text
	    WHEN s.naimenovanie::text ~~ '%ТМХ-И%'::text THEN 'АБЛ'::text
	    WHEN s.sr_vid_ispolzovaniya = '00000000-0000-0000-0000-000000000000'::uuid THEN 'ЖДРМ'::text
	    ELSE 'ЛТС'::text
	END AS "lts/zhdrm/abl",
	
	CASE
	    WHEN s.naimenovanie::text ~~ '%СЛД-%'::text THEN 'СЛД'::text
	    WHEN s.naimenovanie::text ~~ '%СО%'::text THEN 'СО'::text
	    WHEN s.naimenovanie::text ~~ '%СУ%'::text THEN 'СУ'::text
	    WHEN s.naimenovanie::text ~~ '%ЦС%'::text OR s.naimenovanie::text ~~ '%ОП%'::text THEN 'ЦС'::text
	    ELSE 'ЗАВОД'::text
	END AS tip,
	
	f.naimenovanie AS filial,
	regexp_replace(initcap(f.naimenovanie::text), '[а-я -]'::text, ''::text, 'g'::text) AS filial_short,
	u.naimenovanie AS upravlenie,
	COALESCE(fl.naimenovanie, 'Заводы ЖДРМ'::character varying) AS fio
FROM erp_sld.sklady s
LEFT JOIN erp_sld.sr_filialy f ON f.ssylka = s.sr_filial
LEFT JOIN erp_sld.sr_upravleniya u ON u.ssylka = s.sr_upravlenie
LEFT JOIN erp_sld.fizicheskie_litsa fl ON fl.ssylka = s.sr_regionalnyy_predstavitel
WHERE s.pometka_udaleniya IS FALSE AND s.eto_gruppa IS TRUE AND f.naimenovanie IS NOT NULL AND u.naimenovanie IS NOT null
;
create index ci_ks on temp_ks(id_sklad);
--select * from temp_ks

--Склады-------------------------------------------------------------------------------------------------------------------------------------
--=========================================================================================================================================--
--Справочник цен-----------------------------------------------------------------------------------------------------------------------------

drop table if exists temp_sc;
create temp table temp_sc
as
with spravochnik_cen (nomenklatura, cena, "period")
as
  (
            select nomenklatura, tsena , date("period") from 
                        (select
                            nomenklatura,
                            tsena,
                            "period" ,
                            row_number() over (partition by nomenklatura order by "period" desc) as last_price
                        from 
                        erp_sld.tseny_nomenklatury cn) as price
                        left join erp_sld.nomenklatura n on n.ssylka = price.nomenklatura
            where last_price = 1 and nomenklatura is not null 
  )
select *
from spravochnik_cen
union 
select r_nomenklatura, r_cena , date(r_date) from
(select distinct
            ot.nomenklatura as r_nomenklatura,            
            ot.tsena as r_cena,
            r."data" as r_date,
            --rank () over (partition by ot.nomenklatura order by r."data" desc, ot.tsena desc ) as prioritet
            row_number() over (partition by ot.nomenklatura order by r."data" desc) as prioritet
from 
            erp_sld.otgruzka_tovarov_s_khraneniya_tovary ot
            left join (select distinct ror.ssylka , ror.rasporyazhenie from erp_sld.raskhodnyy_order_na_tovary_tovary_po_rasporyazheniyam ror) ror2 
                                       on (ror2.rasporyazhenie = ot.ssylka)
            left join erp_sld.raskhodnyy_order_na_tovary r on r.ssylka = ror2.ssylka
            left join erp_sld.sklady s on s.ssylka = r.sklad
            left join erp_sld.sootvetstvie_skladov mw on mw.sklady_erp_salair = s.naimenovanie 
            left outer join spravochnik_cen on spravochnik_cen.nomenklatura = ot.nomenklatura
where 
            r.pometka_udaleniya is false
            and mw.filial is not null
    and mw.filial != 'Консолидация'
    and mw.tip_sklada != 'ЦС'
    and ot.tsena not in (0,1)
    and spravochnik_cen.nomenklatura is null or ot.nomenklatura is null
) as price2
where prioritet = 1 and r_nomenklatura is not null
;
create index ci_sc on temp_sc(nomenklatura);
--92 225
--select * from temp_sc where nomenklatura='349bdd68-142f-11ea-99e4-3a68dd0e0df7'

--Справочник цен-----------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
-- Итоговая таблица

drop table if exists temp_f;
create temp table temp_f
as
SELECT
CASE
    WHEN priemka."data" IS NOT NULL THEN date(priemka."data")
    WHEN priemka."data" IS NULL AND po.data IS NOT NULL THEN date(po.data)
    WHEN priemka."data" IS NULL AND po.data IS NULL AND COALESCE(raz_4.data, raz.data) IS NOT NULL THEN date(COALESCE(raz_4.data, raz.data))
    ELSE NULL::date end AS data_operacii,
	ks.fio,
	COALESCE(priemka.menedzher, po.menedzher , COALESCE(raz_4.menedzher, raz.menedzher)) as menedzher,
	ks.filial,
	ks.filial_short,
	ks.upravlenie,
	ks.sklad,
	ks."4pl/oh",
	ks."lts/zhdrm/abl",
	ks.tip,	
	priemka.ssylka as priemka_ssylka,
	priemka.nomer AS nomer_priemka,
	date(priemka."data") AS data_priemka,
	priemka.nomenklatura_polnoe AS nomenklatura_priemka,
	COALESCE(priemka.kolichestvo, 0::numeric) AS kolichestvo_priemka,
	COALESCE(priemka.kolichestvo * sc.cena, 0) as cena_priemka,
	priemka.rasshirenie,
	COALESCE(priemka.file, 'НЕТ') as file,
	po.ves_kg_po,
	po.obem_m3_po,
	po.nomer AS nomer_po,
	date(po.data) AS data_po,
	po.nomenklatura_polnoe AS nomenklatura_po,
	COALESCE(po.kolichestvo, 0::numeric) AS kolichestvo_po,
	COALESCE(po.kolichestvo * sc.cena, 0) as cena_po,
	po.sum_kol,
	po.kol_po,
   CASE
        WHEN priemka.nomer IS NULL THEN 0::bigint
        ELSE row_number() OVER (PARTITION BY priemka.nomer, (date(priemka."data")), priemka.nomenklatura)
    END AS zadvoenie,
	CASE WHEN po.kol_po > '1' THEN COALESCE(priemka.kolichestvo - po.sum_kol, 0) else COALESCE(priemka.kolichestvo - COALESCE(po.kolichestvo, 0), 0) END AS otklonenie_po,
	COALESCE(date(po.data) - date(priemka."data"), now()::date - date(priemka."data"), 0) AS interval_po,
	COALESCE(po.ispolzovat_adresnoe_khranenie, false) as adresnoe_khranenie,	
	COALESCE(raz_4.nomer, raz.nomer) AS nomer_razmesheniya,
	COALESCE(date(raz_4.data), date(raz.data)) AS data_razmeshcheniya,
	COALESCE(raz_4.nomenklatura_polnoe, raz.nomenklatura_polnoe) AS nomenklatura_razmeshcheniya,
	COALESCE(raz_4.kolichestvo, raz.kolichestvo) AS kolichestvo_razmeshcheno,
	COALESCE(COALESCE(raz_4.kolichestvo, raz.kolichestvo) * sc.cena, 0) as cena_razmeshcheno,
	COALESCE(po.kolichestvo - COALESCE(raz_4.kolichestvo, raz.kolichestvo), 0::numeric) AS otklonenie_razmeshcheniya,
	COALESCE(date(COALESCE(raz_4.data, raz.data)) - date(po.data)::date, now()::date - date(po.data)) AS interval_razmeshcheniya
FROM 
	temp_priemka as priemka
	full join temp_po po on po.rasporyazhenie = priemka.ssylka and po.nomenklatura = priemka.nomenklatura
	full join temp_pp pp on pp.priemka = priemka.ssylka and pp.sr_nomenklatura = priemka.nomenklatura
	full join temp_raz raz on raz.sr_prikhodnyy_dokument = po.ssylka AND raz.nomenklatura = po.nomenklatura
	LEFT JOIN temp_raz_4 raz_4 on raz_4.sr_order_na_peremeshchenie = pp.peremeshenie and raz_4.nomenklatura = pp.sr_nomenklatura
	LEFT JOIN temp_ks as ks on 
	CASE
		WHEN priemka.nomer IS NOT NULL THEN priemka.sklad
		WHEN priemka.nomer IS NULL AND po.nomer IS NOT NULL THEN po.sklad
		WHEN priemka.nomer IS NULL AND po.nomer IS NULL AND COALESCE(raz_4.nomer, raz.nomer) IS NOT NULL THEN COALESCE(raz_4.sklad, raz.sklad)
		ELSE NULL::uuid END = ks.id_sklad
	LEFT JOIN temp_sc as sc on 
	COALESCE(priemka.nomenklatura, po.nomenklatura , COALESCE(raz_4.nomenklatura, raz.nomenklatura)) = sc.nomenklatura
;
--select * from temp_f where priemka_ssylka = 'e4a2dee1-17b2-11ed-83ae-3a68dd4bb987'

-------------------------------------------------------------------------------------------------
--ИТОГ-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------

drop table if exists temp_final;
create temp table temp_final
as
select
CASE    
    WHEN f.nomer_priemka IS NOT NULL AND f.nomer_po IS NOT NULL AND (f.nomer_razmesheniya IS NULL AND f.adresnoe_khranenie is false) THEN 'ДА'::text
    WHEN f.nomer_priemka IS NOT NULL AND f.nomer_po IS NOT NULL AND (f.nomer_razmesheniya IS NOT NULL AND f.adresnoe_khranenie is true) THEN 'ДА'::text
    ELSE 'НЕТ'::text
END AS zaversheno,
CASE
    when nomer_priemka IS null or nomer_po is null or (nomer_razmesheniya IS NULL AND adresnoe_khranenie is true) THEN 'ДА'
    --when nomer_priemka IS not null and nomer_po is not null and (nomer_razmesheniya IS not NULL AND adresnoe_khranenie is false) THEN 'ДА'
	WHEN f.otklonenie_po != 0 THEN 'ДА'
    WHEN f.otklonenie_razmeshcheniya != 0 THEN 'ДА'
	WHEN f.kolichestvo_po = 0 and f.kol_po > 1 THEN 'ДА'::text --Задвоения
	WHEN "lts/zhdrm/abl" != 'ЖДРМ' and f.file = 'НЕТ' THEN 'ДА'::text    
    ELSE 'НЕТ'::text
END AS otkloneniya,
CASE
    WHEN nomer_priemka IS NULL THEN 'Не проведена приемка'::text --1
    WHEN nomer_po IS NULL THEN 'Не проведен приходный ордер'::text --2
    WHEN kolichestvo_po = 0 and kol_po > 1 THEN 'Задвоен приходный ордер'::text   --3 
    WHEN otklonenie_po != 0 and zadvoenie > 1 THEN 'Задвоен приходный ордер'::text --3
    WHEN otklonenie_po != 0 THEN 'Количество в приходном ордере не соответствует количеству в приемке'::text --4 
    WHEN nomer_razmesheniya IS NULL AND adresnoe_khranenie is true THEN 'Не проведено размещение в ячейки'::text--5
    WHEN otklonenie_razmeshcheniya != 0 THEN 'Количество в размещении не соответствует количеству в приходном ордере'::text--6
    WHEN (nomer_priemka is not null and nomenklatura_priemka is null) 
           or (nomer_po is not null and nomenklatura_po is null)
           or (nomer_razmesheniya is not null and nomenklatura_razmeshcheniya is NULL)
         then 'Отсутствует табличная часть документа'--7
    WHEN file = 'НЕТ' then 'Не приложен скан документа'--8
    ELSE 'Расхождений нет'::text--0
END AS tip_otkloneniya,
CASE
    WHEN nomer_priemka IS NULL THEN '1'::text --1
    WHEN nomer_po IS NULL THEN '2'::text --2
    WHEN kolichestvo_po = 0 and kol_po > 1 THEN '3'::text   --3 
    WHEN otklonenie_po != 0 and zadvoenie > 1 THEN '3'::text --3    
    WHEN otklonenie_po != 0 THEN '4'::text --4
    WHEN nomer_razmesheniya IS NULL AND adresnoe_khranenie is true THEN '5'::text--5
    WHEN otklonenie_razmeshcheniya != 0 THEN '6'::text--6
    WHEN (nomer_priemka is not null and nomenklatura_priemka is null) 
           or (nomer_po is not null and nomenklatura_po is null)
           or (nomer_razmesheniya is not null and nomenklatura_razmeshcheniya is NULL)
         then '7'--7
    WHEN file = 'НЕТ' then '8'--8
    ELSE '0'::text--0
END AS kod_otkloneniya,
data_operacii,
fio,
sklad,
nomer_priemka,
data_priemka,
nomenklatura_priemka,
kolichestvo_priemka,
cena_priemka,
nomer_po,
data_po,
nomenklatura_po,
kolichestvo_po,
cena_po,
zadvoenie,
otklonenie_po,
interval_po,
adresnoe_khranenie,---Удалить
nomer_razmesheniya,
data_razmeshcheniya,
nomenklatura_razmeshcheniya,
kolichestvo_razmeshcheno,
cena_razmeshcheno,
otklonenie_razmeshcheniya,
interval_razmeshcheniya,
"4pl/oh",
"lts/zhdrm/abl",
tip,
filial,
filial_short,
upravlenie,
rasshirenie,
file,
ves_kg_po,
obem_m3_po,
menedzher
from temp_f as f
where data_operacii is not null
;
--select * from temp_final where nomer_razmesheniya = '00-00388229'
--2 263 505

--209a2d6c-143d-11ea-99e4-3a68dd0e0df7 --nomen
--5655d3a6-d2b0-11ed-83b1-3a68dd4bb987 --priemka


--drop table if exists reports.t_ptnh;
--create table reports.t_ptnh as select * from temp_final;
--
--GRANT SELECT ON TABLE reports.t_ptnh TO grishinam;
--GRANT SELECT, DELETE, REFERENCES, TRUNCATE, INSERT, TRIGGER, UPDATE ON TABLE reports.t_ptnh TO kulyabin;
--GRANT SELECT ON TABLE reports.t_ptnh TO mustafinrf;
--GRANT SELECT, DELETE, REFERENCES, TRUNCATE, INSERT, TRIGGER, UPDATE ON TABLE reports.t_ptnh TO polusmakovsv;
--GRANT SELECT ON TABLE reports.t_ptnh TO readonly;


truncate reports.t_ptnh;
insert into reports.t_ptnh
(zaversheno,
otkloneniya,
tip_otkloneniya,
data_operacii,
fio,
sklad,
nomer_priemka,
data_priemka,
nomenklatura_priemka,
kolichestvo_priemka,
cena_priemka,
nomer_po,
data_po,
nomenklatura_po,
kolichestvo_po,
cena_po,
zadvoenie,
otklonenie_po,
interval_po,
nomer_razmesheniya,
data_razmeshcheniya,
nomenklatura_razmeshcheniya,
kolichestvo_razmeshcheno,
cena_razmeshcheno,
otklonenie_razmeshcheniya,
interval_razmeshcheniya,
"4pl/oh",
"lts/zhdrm/abl",
tip,
filial,
filial_short,
upravlenie,
kod_otkloneniya,
rasshirenie,
file,
ves_kg_po,
obem_m3_po,
menedzher)

select 
zaversheno,
otkloneniya,
tip_otkloneniya,
data_operacii,
fio,
sklad,
nomer_priemka,
data_priemka,
nomenklatura_priemka,
kolichestvo_priemka,
cena_priemka,
nomer_po,
data_po,
nomenklatura_po,
kolichestvo_po,
cena_po,
zadvoenie,
otklonenie_po,
interval_po,
nomer_razmesheniya,
data_razmeshcheniya,
nomenklatura_razmeshcheniya,
kolichestvo_razmeshcheno,
cena_razmeshcheno,
otklonenie_razmeshcheniya,
interval_razmeshcheniya,
"4pl/oh",
"lts/zhdrm/abl",
tip,
filial,
filial_short,
upravlenie,
kod_otkloneniya,
rasshirenie,
file,
ves_kg_po,
obem_m3_po,
menedzher
from temp_final;

end
$procedure$
;
