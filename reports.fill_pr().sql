-- DROP PROCEDURE reports.fill_pr();

CREATE OR REPLACE PROCEDURE reports.fill_pr()
 LANGUAGE plpgsql
AS $procedure$

begin

drop table if exists temp_final;
create temp table temp_final
as
WITH 	   znp AS (
         SELECT z.ssylka,
            	z.nomer,
            	z.data
          FROM erp_sld.zakaz_na_peremeshchenie z
          WHERE z.pometka_udaleniya IS FALSE AND z.proveden IS TRUE
        ), pr AS (
         SELECT pr_1.ssylka,
	            pr_1.sklad_otpravitel,
	            pr_1.sklad_poluchatel,
	            pr_1.zakaz_na_peremeshchenie,
	            prt.nomenklatura,
	            pr_1.nomer,
	            pr_1.data,
	            sum(prt.kolichestvo) AS kolichestvo
          FROM erp_sld.peremeshchenie_tovarov pr_1
          LEFT JOIN erp_sld.peremeshchenie_tovarov_tovary prt ON prt.ssylka = pr_1.ssylka
          WHERE pr_1.pometka_udaleniya IS FALSE AND pr_1.proveden IS TRUE AND pr_1.zakaz_na_peremeshchenie <> '00000000-0000-0000-0000-000000000000'::uuid
          GROUP BY pr_1.ssylka, pr_1.sklad_otpravitel, pr_1.sklad_poluchatel, pr_1.zakaz_na_peremeshchenie, prt.nomenklatura, pr_1.nomer, pr_1.data
        ), ro AS (
         SELECT ro_1.ssylka,
	            ro_1.sr_peremeshchenie_tovarov,
	            ro_1.sr_zayavka_na_ts,
	            ro_1.sr_zakaz_na_peremeshchenie,
	            ro_1.sklad,
	            rot.nomenklatura,
	            ro_1.nomer,
	            ro_1.data,
	            st.status,
	            n.naimenovanie_polnoe,
	            sum(rot.kolichestvo) AS kolichestvo,
	            sp.ispolzovat_adresnoe_khranenie 
          FROM erp_sld.raskhodnyy_order_na_tovary ro_1
          LEFT JOIN erp_sld.raskhodnyy_order_na_tovary_otgruzhaemye_tovary rot ON rot.ssylka = ro_1.ssylka
          LEFT JOIN erp_sld.nomenklatura n ON n.ssylka = rot.nomenklatura
          LEFT JOIN erp_sld.statusy_raskhodnykh_orderov st ON st.ssylka = ro_1.status
          left join erp_sld.skladskie_pomeshcheniya sp on sp.ssylka =ro_1.pomeshchenie  
          WHERE ro_1.pometka_udaleniya IS FALSE AND ro_1.proveden IS TRUE AND ro_1.sr_zakaz_na_peremeshchenie <> '00000000-0000-0000-0000-000000000000'::uuid
          GROUP BY ro_1.ssylka, ro_1.sr_peremeshchenie_tovarov, ro_1.sr_zayavka_na_ts, ro_1.sr_zakaz_na_peremeshchenie, ro_1.sklad, ro_1.nomer, ro_1.data, rot.nomenklatura, st.status, n.naimenovanie_polnoe, sp.ispolzovat_adresnoe_khranenie
        ), otbor AS (
         SELECT o.ssylka,
	            o.sklad,
	            ot.nomenklatura,
	            o.rasporyazhenie,
	            o.nomer,
	            o.data,
	            n.naimenovanie_polnoe,
	            sum(ot.kolichestvo) AS kolichestvo
          FROM erp_sld.otbor_razmeshchenie_tovarov o
          LEFT JOIN erp_sld.otbor_razmeshchenie_tovarov_tovary_otbor ot ON ot.ssylka = o.ssylka
          LEFT JOIN erp_sld.nomenklatura n ON n.ssylka = ot.nomenklatura
          WHERE o.pometka_udaleniya IS FALSE AND o.proveden IS TRUE AND o.vid_operatsii = '15aaeeba-b007-4219-a5f6-ba302f02fe9e'::uuid
          GROUP BY o.ssylka, o.sklad, ot.nomenklatura, o.rasporyazhenie, o.nomer, o.data, o.proveden, n.naimenovanie_polnoe
        ), po AS (
         SELECT po_1.ssylka,
	            po_1.sr_peremeshchenie_tovarov,
	            po_1.sr_zayavka_na_ts,
	            po_1.sr_zakaz_na_peremeshchenie,
	            po_1.sklad,
	            pot.nomenklatura,
	            po_1.rasporyazhenie,
	            po_1.nomer,
	            po_1.data,
	            n.naimenovanie_polnoe,
	            sum(pot.kolichestvo) AS kolichestvo,
	            sp.ispolzovat_adresnoe_khranenie 
          FROM erp_sld.prihodnyy_order_na_tovary po_1
          LEFT JOIN erp_sld.prihodnyy_order_na_tovary_tovary pot ON po_1.ssylka = pot.ssylka
          LEFT JOIN erp_sld.nomenklatura n ON n.ssylka = pot.nomenklatura
          left join erp_sld.skladskie_pomeshcheniya sp on sp.ssylka = po_1.pomeshchenie 
          WHERE po_1.pometka_udaleniya IS FALSE AND po_1.proveden IS TRUE AND po_1.sr_zakaz_na_peremeshchenie <> '00000000-0000-0000-0000-000000000000'::uuid
          GROUP BY po_1.ssylka, po_1.sr_peremeshchenie_tovarov, po_1.sr_zayavka_na_ts, po_1.sr_zakaz_na_peremeshchenie, po_1.sklad, pot.nomenklatura, po_1.rasporyazhenie, po_1.nomer, po_1.data, n.naimenovanie_polnoe,sp.ispolzovat_adresnoe_khranenie
        ), raz AS (
         SELECT o.ssylka,
	            o.sklad,
	            ot.sr_prikhodnyy_dokument,
	            ot.nomenklatura,
	            o.nomer,
	            o.data,
	            n.naimenovanie_polnoe,
	            sum(ot.kolichestvo) AS kolichestvo
          FROM erp_sld.otbor_razmeshchenie_tovarov o
          LEFT JOIN erp_sld.otbor_razmeshchenie_tovarov_tovary_razmeshchenie ot ON ot.ssylka = o.ssylka
          LEFT JOIN erp_sld.nomenklatura n ON n.ssylka = ot.nomenklatura
          WHERE o.pometka_udaleniya IS FALSE AND o.proveden IS TRUE AND o.vid_operatsii = '9770b3d2-99ef-4dc2-b5ac-9aa33b80bfa7'::uuid AND ot.sr_prikhodnyy_dokument <> '00000000-0000-0000-0000-000000000000'::uuid
          GROUP BY o.ssylka, o.sklad, ot.sr_prikhodnyy_dokument, ot.nomenklatura, o.nomer, o.data, n.naimenovanie_polnoe
        ), ks AS (
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
          WHERE s.pometka_udaleniya IS FALSE AND s.eto_gruppa IS TRUE AND f.naimenovanie IS NOT NULL AND u.naimenovanie IS NOT NULL
        ), ks_gp AS (
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
          WHERE s.pometka_udaleniya IS FALSE AND s.eto_gruppa IS TRUE AND f.naimenovanie IS NOT NULL AND u.naimenovanie IS NOT NULL
        )
 SELECT
        CASE
            WHEN znp.data IS NOT NULL THEN date(znp.data)
            WHEN znp.data IS NULL AND ro.data IS NOT NULL THEN date(ro.data)
            WHEN znp.data IS NULL AND ro.data IS NULL AND otbor.data IS NOT NULL THEN date(otbor.data)
            WHEN znp.data IS NULL AND ro.data IS NULL AND otbor.data IS NULL AND po.data IS NOT NULL THEN date(po.data)
            ELSE NULL::date
        END AS data_operacii,
        CASE
            WHEN znp.nomer IS NOT NULL 
            	AND ro.nomer IS NOT NULL 
            	AND ((otbor.nomer IS NOT null and ro.ispolzovat_adresnoe_khranenie is true) or (otbor.nomer IS null and ro.ispolzovat_adresnoe_khranenie is false)) 
            	AND po.nomer IS NOT null
            	AND ((raz.nomer IS NOT null and po.ispolzovat_adresnoe_khranenie is true) or (raz.nomer IS null and po.ispolzovat_adresnoe_khranenie is false)) THEN 'ДА'::text
            ELSE 'НЕТ'::text
        END AS zaversheno,
        CASE
            WHEN row_number() OVER (PARTITION BY ro.nomer, (date(ro.data)), ro.nomenklatura, pr.nomer) > 1 THEN 'ДА'::text
            WHEN ro.nomer IS NOT NULL and ro.ispolzovat_adresnoe_khranenie is true  and po.nomer IS NOT null and po.ispolzovat_adresnoe_khranenie is true  AND ro.kolichestvo = po.kolichestvo AND ro.kolichestvo = otbor.kolichestvo AND po.kolichestvo = raz.kolichestvo THEN 'НЕТ'::text
            WHEN ro.nomer IS NOT NULL AND ro.ispolzovat_adresnoe_khranenie is false and po.nomer IS NOT null and po.ispolzovat_adresnoe_khranenie is true  and ro.kolichestvo = po.kolichestvo AND po.kolichestvo = raz.kolichestvo THEN 'НЕТ'::text
            WHEN ro.nomer IS NOT NULL and ro.ispolzovat_adresnoe_khranenie is true  and po.nomer IS NOT null and po.ispolzovat_adresnoe_khranenie is false AND ro.kolichestvo = po.kolichestvo AND ro.kolichestvo = otbor.kolichestvo THEN 'НЕТ'::text
            WHEN ro.nomer IS NOT NULL and ro.ispolzovat_adresnoe_khranenie is false and po.nomer IS NOT null and po.ispolzovat_adresnoe_khranenie is false AND ro.kolichestvo = po.kolichestvo THEN 'НЕТ'::text
            ELSE 'ДА'::text 
        END AS otkloneniya,
        CASE
            WHEN ro.kolichestvo <> otbor.kolichestvo and ro.ispolzovat_adresnoe_khranenie is true THEN 'Количество в расходном ордере не соответствует количеству в отборе'::text
            WHEN ro.kolichestvo <> po.kolichestvo THEN 'Количество в расходном ордере не соответствует количеству в приходном ордере'::text
            WHEN po.kolichestvo <> raz.kolichestvo and po.ispolzovat_adresnoe_khranenie is true THEN 'Количество в приходном ордере не соответствует количеству в размещении'::text
            WHEN znp.data IS NULL AND ro.data IS NOT NULL THEN 'Не проведен заказ на перемещение'::text
            WHEN pr.data IS NULL AND ro.data IS NOT NULL THEN 'Не проведено перемещение'::text
            WHEN otbor.data IS NULL AND ro.data IS NOT null and ro.ispolzovat_adresnoe_khranenie is true  THEN 'Не проведен отбор из ячеек'::text
            WHEN (ro.data IS NULL AND po.data IS NOT null) or ro.nomer IS NULL THEN 'Не проведен расходный ордер'::text
            WHEN ro.data IS NOT NULL AND po.data IS NULL THEN 'Не проведен приходный ордер'::text
            WHEN po.data IS NOT NULL AND raz.data IS null and po.ispolzovat_adresnoe_khranenie is true then  'Не проведено размещение в ячейки'::text
            WHEN row_number() OVER (PARTITION BY po.nomer, (date(po.data)), po.nomenklatura, pr.nomer) > 1 THEN 'Задвоен приходный ордер'::text
            ELSE 'Расхождений нет'::text
        END AS tip_otkloneniya,
        CASE
            WHEN ro.kolichestvo <> otbor.kolichestvo and ro.ispolzovat_adresnoe_khranenie is true THEN '7'::text
            WHEN ro.kolichestvo <> po.kolichestvo THEN '8'::text
            WHEN po.kolichestvo <> raz.kolichestvo and po.ispolzovat_adresnoe_khranenie is true THEN '9'::text
            WHEN znp.data IS NULL AND ro.data IS NOT NULL THEN '1'::text
            WHEN pr.data IS NULL AND ro.data IS NOT NULL THEN '2'::text
            WHEN otbor.data IS NULL AND ro.data IS NOT null and ro.ispolzovat_adresnoe_khranenie is true THEN '3'::text
            WHEN ro.data IS NULL AND po.data IS NOT NULL THEN '4'::text
            WHEN ro.data IS NOT NULL AND po.data IS NULL THEN '5'::text
            WHEN po.data IS NOT NULL AND raz.data IS null and po.ispolzovat_adresnoe_khranenie is true THEN '6'::text
            WHEN row_number() OVER (PARTITION BY po.nomer, (date(po.data)), po.nomenklatura, pr.nomer) > 1 THEN '10'::text
            ELSE '0'::text
        END AS kod_otkloneniya,
	    ks.fio AS fio_go,
	    ks.sklad AS sklad_go,
	    ks."4pl/oh" AS "4pl/oh_go",
	    ks."lts/zhdrm/abl" AS "lts/zhdrm/abl_go",
	    ks.tip AS tip_go,
	    ks.filial AS filial_go,
	    ks.filial_short AS filial_short_go,
	    ks.upravlenie AS upravlenie_go,
	    znp.nomer AS nomer_znp,
	    date(znp.data) AS data_znp,
	    pr.nomer AS nomer_pr,
	    date(pr.data) AS data_pr,
	    ro.nomer AS nomer_ro,
	    date(ro.data) AS data_ro,
	    ro.naimenovanie_polnoe AS nomenklatura_ro,
	    COALESCE(ro.kolichestvo, 0::numeric) AS kolichestvo_ro,
	    otbor.nomer AS nomer_otbora,
	    date(otbor.data) AS data_otbora,
	    otbor.naimenovanie_polnoe AS nomenklatura_otbora,
	    COALESCE(otbor.kolichestvo, 0::numeric) AS kolichestvo_otbora,
	    COALESCE(ro.kolichestvo - otbor.kolichestvo, 0::numeric) AS otklonenie_otbora,
	    COALESCE(date(otbor.data) - date(ro.data), 0) AS interval_otbora,
	    ks_gp.fio AS fio_gp,
	    ks_gp.sklad AS sklad_gp,
	    ks_gp."lts/zhdrm/abl" AS "lts/zhdrm/abl_gp",
	    ks_gp."4pl/oh" AS "4pl/oh_gp",
	    ks_gp.tip AS tip_gp,
	    ks_gp.filial AS filial_gp,
	    ks_gp.filial_short AS filial_short_gp,
	    ks_gp.upravlenie AS upravlenie_gp,
	    po.nomer AS nomer_po,
	    date(po.data) AS data_po,
	    po.naimenovanie_polnoe AS nomenklatura_po,
	    COALESCE(po.kolichestvo, 0::numeric) AS kolichestvo_po,
	        CASE
	            WHEN po.nomer IS NULL THEN 0::bigint
	            ELSE row_number() OVER (PARTITION BY po.nomer, (date(po.data)), po.nomenklatura, pr.nomer)
	        END AS zadvoenie,
	    COALESCE(ro.kolichestvo - po.kolichestvo, 0::numeric) AS otklonenie_po,
	    COALESCE(date(po.data) - date(ro.data), 0) AS interval_po,
	    raz.nomer AS nomer_razmesheniya,
	    date(raz.data) AS data_razmeshcheniya,
	    raz.naimenovanie_polnoe AS nomenklatura_razmeshcheniya,
	    COALESCE(raz.kolichestvo, 0::numeric) AS kolichestvo_razmeshcheniy,
	    COALESCE(po.kolichestvo - raz.kolichestvo, 0::numeric) AS otklonenie_razmeshcheniya,
	    COALESCE(date(raz.data) - date(po.data), 0) AS interval_razmeshcheniya
  FROM 
  pr
  left join znp on znp.ssylka = pr.zakaz_na_peremeshchenie
  full JOIN ro ON ro.sr_peremeshchenie_tovarov = pr.ssylka and ro.nomenklatura = pr.nomenklatura
  LEFT JOIN otbor ON otbor.rasporyazhenie = ro.ssylka AND otbor.nomenklatura = ro.nomenklatura
  FULL JOIN po ON po.sr_peremeshchenie_tovarov = ro.sr_peremeshchenie_tovarov AND po.sr_zayavka_na_ts = ro.sr_zayavka_na_ts AND po.sr_peremeshchenie_tovarov = ro.sr_peremeshchenie_tovarov AND po.nomenklatura = ro.nomenklatura
  LEFT JOIN raz ON raz.sr_prikhodnyy_dokument = po.ssylka AND raz.nomenklatura = po.nomenklatura 
  LEFT JOIN ks ON
        CASE
            WHEN ro.data IS NOT NULL THEN ro.sklad
            WHEN ro.data IS NULL AND pr.data IS NOT NULL THEN pr.sklad_otpravitel
            WHEN ro.data IS NULL AND pr.data IS NULL AND otbor.data IS NOT NULL THEN otbor.sklad
            ELSE NULL::uuid
        END = ks.id_sklad
  LEFT JOIN ks_gp ON
        CASE
            WHEN po.data IS NOT NULL THEN po.sklad
            WHEN po.data IS NULL AND raz.data IS NOT NULL THEN raz.sklad
            ELSE pr.sklad_poluchatel
            --NULL::uuid
        END = ks_gp.id_sklad;
       
       
--create table reports.t_pr
--as
--select * from temp_final;
       
--GRANT SELECT ON TABLE reports.t_pr TO grishinam;
--GRANT SELECT, DELETE, REFERENCES, TRUNCATE, INSERT, TRIGGER, UPDATE ON TABLE reports.t_pr TO kulyabin;
--GRANT SELECT ON TABLE reports.t_pr TO mustafinrf;
--GRANT SELECT, DELETE, REFERENCES, TRUNCATE, INSERT, TRIGGER, UPDATE ON TABLE reports.t_pr TO polusmakovsv;
--GRANT SELECT ON TABLE reports.t_pr TO readonly; 

truncate reports.t_pr;
insert into reports.t_pr
select * from temp_final;
       
end
$procedure$
;
