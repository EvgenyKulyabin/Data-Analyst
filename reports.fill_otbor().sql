-- DROP PROCEDURE reports.fill_otbor();

CREATE OR REPLACE PROCEDURE reports.fill_otbor()
 LANGUAGE plpgsql
AS $procedure$
begin

--Огрузка------------------------------------------------------------------------------------------
	
drop table if exists otgruzka;
create temp table otgruzka as 	
SELECT o.ssylka,
       o.sklad,
       ot.nomenklatura AS id_nomenklatura,
       p.naimenovanie as menedzher,
       o.nomer,
       date(o.data) as "data",
       n.naimenovanie_polnoe AS nomenklatura,
       sum(ot.kolichestvo) AS kolichestvo
FROM erp_sld.otgruzka_tovarov_s_khraneniya o
     LEFT JOIN erp_sld.otgruzka_tovarov_s_khraneniya_tovary ot ON ot.ssylka = o.ssylka
     LEFT JOIN erp_sld.nomenklatura n ON n.ssylka = ot.nomenklatura
     LEFT JOIN erp_sld.polzovateli p ON p.ssylka = o.menedzher
WHERE o.pometka_udaleniya IS FALSE AND o.proveden IS TRUE
GROUP BY o.ssylka, o.sklad, o.nomer, o.data, ot.nomenklatura, n.naimenovanie_polnoe, p.naimenovanie
having sum(ot.kolichestvo) != 0
;	
-- select * from otgruzka where nomer = 'СЛ-00420835'
--Расходный ордер----------------------------------------------------------------------------------

drop table if exists ro;
create temp table ro as
SELECT 
	ro_1.ssylka,
    ro_1.sklad,
    rot.rasporyazhenie,
    rott.nomenklatura,
    p1.naimenovanie as menedzher,
    ro_1.nomer,
    date(ro_1.data) as "data",
    st.status,
    n.naimenovanie_polnoe,
    sum(rott.kolichestvo) AS kolichestvo,
    p.ispolzovat_adresnoe_khranenie,
    p.naimenovanie as pomeshchenie 
FROM erp_sld.raskhodnyy_order_na_tovary ro_1
	 LEFT join (select 
				t.ssylka ,
				t.nomenklatura,
				sum(t.kolichestvo) as kolichestvo
			from erp_sld.raskhodnyy_order_na_tovary_otgruzhaemye_tovary t
			group by 
				t.ssylka ,
				t.nomenklatura) rott on rott.ssylka = ro_1.ssylka
     LEFT JOIN (
			     select 
					t.ssylka ,
					t.nomenklatura,
					t.rasporyazhenie  
			from erp_sld.raskhodnyy_order_na_tovary_tovary_po_rasporyazheniyam t
			group by 
					t.ssylka ,
					t.nomenklatura,
					t.rasporyazhenie) rot ON rot.ssylka = rott.ssylka and rot.nomenklatura = rott.nomenklatura
     LEFT JOIN erp_sld.nomenklatura n ON n.ssylka = rott.nomenklatura
     LEFT JOIN erp_sld.statusy_raskhodnykh_orderov st ON st.ssylka = ro_1.status
     LEFT JOIN erp_sld.skladskie_pomeshcheniya p on p.ssylka = ro_1.pomeshchenie
     LEFT JOIN erp_sld.polzovateli p1 ON p1.ssylka = ro_1.otvetstvennyy
WHERE 
	ro_1.pometka_udaleniya IS FALSE 
	AND ro_1.proveden IS true 
	AND ro_1.sr_zakaz_na_peremeshchenie = '00000000-0000-0000-0000-000000000000' 
	AND ro_1.skladskaya_operatsiya != 'fc6182a2-b2fa-45b2-bb3a-8fe142570b38'
GROUP BY ro_1.ssylka, ro_1.sklad, rot.rasporyazhenie, rott.nomenklatura, p1.naimenovanie, ro_1.nomer, date(ro_1.data), st.status, n.naimenovanie_polnoe, p.ispolzovat_adresnoe_khranenie, p.naimenovanie
having sum(rott.kolichestvo) != 0
;
--select * from ro where nomer = '00-00436692' 

--Отбор--------------------------------------------------------------------------------------------

drop table if exists otbor_1;
create temp table otbor_1 as
SELECT 
	o.ssylka,
	o.sklad,
	ot.nomenklatura,
	o.rasporyazhenie,
	p2.naimenovanie as menedzher,
	o.nomer,
	date(o.data) as "data",
	n.naimenovanie_polnoe,
	sum(ot.kolichestvo) AS kolichestvo,
	sum(ot.kolichestvo_otobrano) AS kolichestvo_razmeshcheno
FROM erp_sld.otbor_razmeshchenie_tovarov o
	LEFT JOIN erp_sld.otbor_razmeshchenie_tovarov_tovary_otbor ot ON ot.ssylka = o.ssylka
	LEFT JOIN erp_sld.nomenklatura n ON n.ssylka = ot.nomenklatura
	LEFT JOIN erp_sld.raskhodnyy_order_na_tovary r on r.ssylka = o.rasporyazhenie
	LEFT JOIN erp_sld.polzovateli p2 ON p2.ssylka = o.otvetstvennyy
WHERE 
	o.pometka_udaleniya IS FALSE 
	AND o.proveden IS TRUE 
	AND o.vid_operatsii = '15aaeeba-b007-4219-a5f6-ba302f02fe9e'::uuid --Отбор
	and r.skladskaya_operatsiya != 'fc6182a2-b2fa-45b2-bb3a-8fe142570b38' -- Не является отгрузкой по перемещению
GROUP BY o.ssylka, o.sklad, ot.nomenklatura, o.rasporyazhenie, o.nomer, date(o.data), o.proveden, n.naimenovanie_polnoe, p2.naimenovanie
having sum(ot.kolichestvo) != 0
;

--Карта складов------------------------------------------------------------------------------------

drop table if exists ks;
create temp table ks as
SELECT 
	s.ssylka AS id_sklad,
    s.naimenovanie AS sklad,
    CASE WHEN s.naimenovanie::text ~~ '%4%PL%'::text THEN '4PL'::text ELSE 'ОХ'::text END AS "4pl/oh",
    CASE WHEN s.naimenovanie::text ~~ '%АБЛ%'::text THEN 'АБЛ'::text
         WHEN s.naimenovanie::text ~~ '%Рэйл%'::text THEN 'АБЛ'::text
         WHEN s.naimenovanie::text ~~ '%Делоникс%'::text THEN 'АБЛ'::text
         WHEN s.naimenovanie::text ~~ '%ТМХ-И%'::text THEN 'АБЛ'::text
         WHEN s.sr_vid_ispolzovaniya = '00000000-0000-0000-0000-000000000000'::uuid THEN 'ЖДРМ'::text ELSE 'ЛТС'::text END AS "lts/zhdrm/abl",
    CASE WHEN s.naimenovanie::text ~~ '%СЛД-%'::text THEN 'СЛД'::text
         WHEN s.naimenovanie::text ~~ '%СО%'::text THEN 'СО'::text
         WHEN s.naimenovanie::text ~~ '%СУ%'::text THEN 'СУ'::text
         WHEN s.naimenovanie::text ~~ '%ЦС%'::text OR s.naimenovanie::text ~~ '%ОП%'::text THEN 'ЦС'::text ELSE 'ЗАВОД'::text END AS tip,
    f.naimenovanie AS filial,
    regexp_replace(initcap(f.naimenovanie::text), '[а-я -]'::text, ''::text, 'g'::text) AS filial_short,
    u.naimenovanie AS upravlenie,
    COALESCE(fl.naimenovanie, 'Заводы ЖДРМ'::character varying) AS fio
FROM erp_sld.sklady s
     LEFT JOIN erp_sld.sr_filialy f ON f.ssylka = s.sr_filial
     LEFT JOIN erp_sld.sr_upravleniya u ON u.ssylka = s.sr_upravlenie
     LEFT JOIN erp_sld.fizicheskie_litsa fl ON fl.ssylka = s.sr_regionalnyy_predstavitel
WHERE s.pometka_udaleniya IS FALSE AND s.eto_gruppa IS TRUE AND f.naimenovanie IS NOT NULL AND u.naimenovanie IS NOT NULL
;

--Приложенные сканы--------------------------------------------------------------------------------

drop table if exists ras;
create temp table ras as
select
	id_otgruzka ,
	ras, 
	otpravlen_v_sed 
from(
	select 
		ot.vladelets_fayla as id_otgruzka , 
		string_agg(distinct ot.rasshirenie,',') as ras,
		coalesce (f.otpravlen, false) as otpravlen_v_sed,
		rank () over (partition by ot.vladelets_fayla order by f.otpravlen asc) as "rank"
	from erp_sld.otgruzka_tovarov_s_khraneniya_prisoedinennye_fayly ot 
		left join erp_sld.sr_otpravka_faylov_v_lokotekh f on f.fayl = ot.ssylka 
	group by  ot.vladelets_fayla , f.otpravlen) as "rank"
	where "rank" = 1
;

--Итоговая таблица---------------------------------------------------------------------------------

drop table if exists t_final;
create temp table t_final as
SELECT
    CASE
		WHEN otgruzka.data IS NOT NULL THEN date(otgruzka.data)
        WHEN otgruzka.data IS NULL AND ro.data IS NOT NULL THEN date(ro.data)
        WHEN otgruzka.data IS NULL AND ro.data IS NULL AND otbor_1.data IS NOT NULL THEN date(otbor_1.data)
        ELSE NULL::date END AS data_operacii,
    ks.fio,
	CASE
        WHEN otgruzka.data IS NOT NULL THEN otgruzka.menedzher
        WHEN otgruzka.data IS NULL AND ro.data IS NOT NULL THEN ro.menedzher
        WHEN otgruzka.data IS NULL AND ro.data IS NULL AND otbor_1.data IS NOT NULL THEN otbor_1.menedzher
        ELSE NULL END AS menedzher,
    ks."4pl/oh",
    ks."lts/zhdrm/abl",
    ks.tip,
    ks.filial,
    ks.filial_short,
    ks.upravlenie,
    ks.sklad,
    otgruzka.nomer AS nomer_otgruzka,
    date(otgruzka.data) AS data_otgruzka,
    otgruzka.nomenklatura AS nomenklatura_otgruzka,
    COALESCE(otgruzka.kolichestvo, 0::numeric) AS kolichestvo_otgruzka,
    ro.nomer AS nomer_ro,
    date(ro.data) AS data_ro,
    ro.naimenovanie_polnoe AS nomenklatura_ro,
    COALESCE(ro.kolichestvo, 0::numeric) AS kolichestvo_ro,
    CASE
        WHEN otgruzka.nomer IS NULL THEN 0::bigint
        ELSE row_number() OVER (PARTITION BY otgruzka.nomer, (date(otgruzka.data)), otgruzka.id_nomenklatura)
        END AS zadvoenie,
    COALESCE(otgruzka.kolichestvo - ro.kolichestvo, 0::numeric) AS otklonenie_ro,
    COALESCE(date(ro.data) - date(otgruzka.data), 0) AS interval_ro,
    otbor_1.nomer AS nomer_otbora,
    date(otbor_1.data) AS data_otbora,
    otbor_1.naimenovanie_polnoe AS nomenklatura_otbora,
    COALESCE(otbor_1.kolichestvo, 0::numeric) AS kolichestvo_k_otboru,
    COALESCE(otbor_1.kolichestvo_razmeshcheno, 0::numeric) AS kolichestvo_otbora,
    COALESCE(ro.kolichestvo - otbor_1.kolichestvo, 0::numeric) AS otklonenie_otbora,
    COALESCE(date(otbor_1.data) - date(ro.data), 0) AS interval_otbora,
    ras.ras as rasshirenie,
    coalesce(ras.otpravlen_v_sed, false ) as sed,
    ro.ispolzovat_adresnoe_khranenie as adresnoe_khranenie,
    ro.pomeshchenie
FROM otgruzka
	LEFT JOIN ras ON ras.id_otgruzka = otgruzka.ssylka
    FULL JOIN ro ON ro.rasporyazhenie = otgruzka.ssylka AND ro.nomenklatura = otgruzka.id_nomenklatura
    FULL JOIN otbor_1 ON otbor_1.rasporyazhenie = ro.ssylka AND otbor_1.nomenklatura = ro.nomenklatura
    LEFT JOIN ks ON
    CASE
        WHEN otgruzka.data IS NOT NULL THEN otgruzka.sklad
        WHEN otgruzka.data IS NULL AND ro.data IS NOT NULL THEN ro.sklad
        WHEN otgruzka.data IS NULL AND ro.data IS NULL AND otbor_1.data IS NOT NULL THEN otbor_1.sklad
        ELSE NULL::uuid END = ks.id_sklad
;
--select * from t_final where nomer_otgruzka = 'СЛ-00420835'
---------------------------------------------------------------------------------------------------
--ИТОГ---------------------------------------------------------------------------------------------

drop table if exists temp_final;
create temp table temp_final as
SELECT
	CASE
		WHEN nomer_otgruzka IS NOT NULL AND nomer_ro IS NOT NULL AND nomer_otbora IS NOT NULL AND adresnoe_khranenie is true /*and rasshirenie IS NOT null and sed is true*/ THEN 'ДА'::text
        WHEN nomer_otgruzka IS NOT NULL AND nomer_ro IS NOT NULL AND nomer_otbora IS NULL AND adresnoe_khranenie is false /*and rasshirenie IS NOT null and sed is true*/ THEN 'ДА'::text
        ELSE 'НЕТ'::text END AS zaversheno,
    CASE
        WHEN zadvoenie > 1 THEN 'ДА'::text
        WHEN otklonenie_ro != 0 THEN 'ДА'::text
        WHEN otklonenie_otbora != 0 THEN 'ДА'::text
        WHEN "lts/zhdrm/abl" = 'ЛТС' and rasshirenie IS NULL THEN 'ДА'::text
        WHEN "4pl/oh" = '4PL' and "lts/zhdrm/abl" = 'ЛТС' and sed is false and pomeshchenie not like '%ЭТП %' THEN 'ДА'::text
        ELSE 'НЕТ'::text END AS otkloneniya,
	CASE
        WHEN nomer_otgruzka IS NULL THEN 'Не проведена отгрузка'::text 
        WHEN nomer_ro IS NULL THEN 'Не проведен расходный ордер'::text 
        WHEN otklonenie_ro != 0 THEN 'Количество в расходном ордере не соответствует количеству в отгрузке'::text
        WHEN adresnoe_khranenie is true and nomer_otbora IS NULL THEN 'Не проведен отбор из ячейки'::text
        WHEN otklonenie_otbora != 0 THEN 'Количество в отборе не соответствует количеству в расходном ордере'::text
        WHEN /*"4pl/oh" = '4PL' and */"lts/zhdrm/abl" = 'ЛТС' and rasshirenie IS null THEN 'Не приложен скан документа'::text
        WHEN "4pl/oh" = '4PL' and "lts/zhdrm/abl" = 'ЛТС' and sed is false and pomeshchenie not like '%ЭТП %' then 'Документ не отправлен в СЭД'::text       
        WHEN zadvoenie > 1 THEN 'Задвоен расходный ордер'::text
        ELSE 'Расхождений нет'::text END AS tip_otkloneniya,
    CASE
        WHEN nomer_otgruzka IS NULL THEN '1'::text
        WHEN nomer_ro IS NULL THEN '2'::text
        WHEN otklonenie_ro != 0 THEN '3'::text
        WHEN adresnoe_khranenie is true and nomer_otbora IS NULL THEN '4'::text
        WHEN otklonenie_otbora != 0 THEN '5'::text
        WHEN /*"4pl/oh" = '4PL' and */"lts/zhdrm/abl" = 'ЛТС' and rasshirenie IS null THEN '6'::text
        WHEN "4pl/oh" = '4PL' and "lts/zhdrm/abl" = 'ЛТС' and sed is false and pomeshchenie not like '%ЭТП %' then '7'::text        
        WHEN zadvoenie > 1 THEN '8'::text
        ELSE '0'::text END AS kod_otkloneniya,
        t_final.*
from t_final
;
--select * from temp_final
       
       
--drop table if exists reports.t_otbor;
--create table reports.t_otbor as select * from temp_final;
       
--GRANT SELECT ON TABLE reports.t_otbor TO grishinam;
--GRANT SELECT, DELETE, REFERENCES, TRUNCATE, INSERT, TRIGGER, UPDATE ON TABLE reports.t_otbor TO kulyabin;
--GRANT SELECT ON TABLE reports.t_otbor TO mustafinrf;
--GRANT SELECT, DELETE, REFERENCES, TRUNCATE, INSERT, TRIGGER, UPDATE ON TABLE reports.t_otbor TO polusmakovsv;
--GRANT SELECT ON TABLE reports.t_otbor TO readonly; 

       
truncate reports.t_otbor;
insert into reports.t_otbor
select * from temp_final;
       

end
$procedure$
;
