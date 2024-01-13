-- DROP PROCEDURE kulyabin.economics_2022();

CREATE OR REPLACE PROCEDURE kulyabin.economics_2022()
 LANGUAGE plpgsql
AS $procedure$
Begin

---------------------------------------------------------------------------------------------------------------------------------------------
--Движение за 2022 год	
---------------------------------------------------------------------------------------------------------------------------------------------	
drop table if exists temp_o;	
create temp table temp_o as 
select 
		date(t."period") as "data",
		t.sklad as id_skald ,	
		t.nomenklatura as id_nomenklatura,
		coalesce (lo.sr_lineynoe_oborudovanie, false) as lo,
		coalesce (asu.tip, 'Свободные остатки') as naznachenie,
		tt."name" as operaciya,
		ro.kommentariy ,
		case when ro.kommentariy like '%ыкуп%' then 'Выкуп' end vikup,
		case when t.vid_dvizheniya = 0 then 1 else -1 end * t.v_nalichii as kovichestvo
	from 
		erp_sld.tovary_na_skladah t
		left join erp_sld.tovary_na_skladah_types tt on tt."type" = t."type" 
		left join erp_sld.sklady s on s.ssylka = t.sklad
		left join erp_sld.naznacheniya n on n.ssylka = t.naznachenie 
		left join erp_sld.sr_tipy_naznacheniy_asu_mtrio asu on asu.ssylka = n.sr_tip_naznacheniya_asu_mtrio
		left join erp_sld.raskhodnyy_order_na_tovary ro on ro.ssylka = t.registrator
		left join erp_sld.prihodnyy_order_na_tovary po on po.ssylka = t.registrator
		left join erp_sld.nomenklatura_lineynoe_oborudovanie lo on lo.ssylka = t.nomenklatura
	where extract (year from t."period") = '2022'
		and t."type" != '00000348' --Корректировка назначения
;
---------------------------------------------------------------------------------------------------------------------------------------------
--Справочник цен
---------------------------------------------------------------------------------------------------------------------------------------------

drop table if exists temp_sc;	
create temp table temp_sc as
with spravochnik_cen (nomenklatura, cena, "period")
 as
  (
  	select nomenklatura, tsena , date("period") from 
		(select
			nomenklatura,
			tsena,
			"period" ,
			rank () over (partition by nomenklatura order by "period" desc) as last_price
		from 
		erp_sld.tseny_nomenklatury cn) as price
		left join erp_sld.nomenklatura n on n.ssylka = price.nomenklatura
	where last_price = 1 and nomenklatura is not null and tsena != 0
  )
select * from spravochnik_cen
union 
select r_nomenklatura, r_cena , date(r_date) from
(select distinct
	ot.nomenklatura as r_nomenklatura, 	
	ot.tsena as r_cena,
	r."data" as r_date,
	rank () over (partition by ot.nomenklatura order by r."data" desc, ot.tsena desc ) as prioritet
from 
	erp_sld.otgruzka_tovarov_s_khraneniya_tovary ot
	left join (select distinct ror.ssylka , ror.rasporyazhenie from erp_sld.raskhodnyy_order_na_tovary_tovary_po_rasporyazheniyam ror) ror2 
			   on (ror2.rasporyazhenie = ot.ssylka )
	left join erp_sld.raskhodnyy_order_na_tovary r on r.ssylka = ror2.ssylka
	left join erp_sld.sklady s	on s.ssylka = r.sklad
	left join erp_sld.sootvetstvie_skladov mw on mw.sklady_erp_salair = s.naimenovanie 
	full outer join spravochnik_cen on spravochnik_cen.nomenklatura = ot.nomenklatura
where 
	r.pometka_udaleniya is false
	and mw.filial is not null
    and mw.filial != 'Консолидация'
    and mw.tip_sklada != 'ЦС'
    and ot.tsena not in (0,1)
    and spravochnik_cen.nomenklatura is null or ot.nomenklatura is null
) as price2
where prioritet = 1 and r_nomenklatura is not null and r_cena != 0
;
---------------------------------------------------------------------------------------------------------------------------------------------
--Склады-------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------

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
---------------------------------------------------------------------------------------------------------------------------------------------
--Итоговая таблица
---------------------------------------------------------------------------------------------------------------------------------------------
drop table if exists temp_final;
create temp table temp_final
as
select 
	ks.filial_short,
	ks.upravlenie,
	ks.sklad,
	ks.tip,
	ks."lts/zhdrm/abl",
	ks."4pl/oh",
	o."data",
	o.lo,
	o.naznachenie,
	o.operaciya,
	o.vikup,
	o.kovichestvo,
	o.kovichestvo * sc.cena as summa
from temp_o o
LEFT JOIN temp_sc sc on sc.nomenklatura = o.id_nomenklatura
LEFT JOIN temp_ks ks on ks.id_sklad = o.id_skald
;
--select * from temp_final

--drop table if exists kulyabin.economics_2022 							--Удаляет старую таблицу
--create table kulyabin.economics_2022 as select * from temp_final;		--Создает таблицу с новой струкурой
--truncate table kulyabin.economics_2022;								--Очищает таблицу без сканирования строк
--insert into kulyabin.economics_2022									--Записывает данные в таблицу

end
$procedure$
;
