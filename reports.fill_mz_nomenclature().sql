-- DROP PROCEDURE reports.fill_mz_nomenclature();

CREATE OR REPLACE PROCEDURE reports.fill_mz_nomenclature()
 LANGUAGE plpgsql
AS $procedure$
begin
--------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------
drop table if exists temp_cargo_places
;
create temp table temp_cargo_places
as
SELECT cargo_places.cargo_id,
sum(cargo_places.weight) AS weight,
--sum(cargo_places.volume) AS volume,
max(cargo_places.width) AS width,
max(cargo_places.height) AS height,
max(cargo_places.length) AS length,
count(*) AS places_count
FROM xpl_new.cargo_places
GROUP BY cargo_places.cargo_id
;
create index ci_cargo_places on temp_cargo_places(cargo_id) include(weight, "width", "height", "length")
;
--------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------
drop table if exists temp_nlo
;
create temp table temp_nlo
as
SELECT DISTINCT nlo.artikul,
nlo.nsi_kod,
nlo.sr_lineynoe_oborudovanie AS lo
FROM erp_sld.nomenklatura_lineynoe_oborudovanie as nlo
;
create index ci_nlo on temp_nlo(nsi_kod, artikul) include(lo)
;
--------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------
drop table if exists temp_cargo_items
;
create temp table temp_cargo_items
as
SELECT c.cargo_id,
sum(c.quantity) AS nomenclature_count,
sum(c.estimated_cost) AS price,
sum(c.estimated_cost) AS amount,
string_agg(c.product_name, '; '::text) AS name,
string_agg(c.skmtr_code, '; '::text) AS skmtr,
string_agg(c.nsi_code, '; '::text) AS nsi,
string_agg(case WHEN nlo.lo IS NULL THEN '0'::text
		   	    ELSE '1'::text
		   END, '; '::text) AS lin_ob
FROM xpl_new.cargo_items as c
LEFT JOIN temp_nlo as nlo ON nlo.nsi_kod::text = c.nsi_code OR nlo.artikul::text = c.skmtr_code
GROUP BY c.cargo_id
;
create index si_cargo_items on temp_cargo_items (cargo_id)
;
--------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------
drop table if exists temp_final
;
create temp table temp_final
as
SELECT rn.ssylka::text AS request,
    rn.tonnazh weight,
    rn.kolichestvo_zakaz nomenclature_count,
    rn.tsena price,
    rn.summa amount,
    n.naimenovanie "name",
    n.kod skmtr,
    n.nsi_kod nsi,
    r.shirina  AS width,
    r.vysota  AS height,
    r.glubina  AS length,
    pt.name AS package_type,
    rn.kolichestvo_zakaz AS places_count,
    CASE
        WHEN nlo.sr_lineynoe_oborudovanie IS TRUE THEN '1'::text
        ELSE '0'::text
    END AS lineynoe_oborudovanie
FROM upp_work.zayavka_na_transport as r
LEFT JOIN upp_work.zayavka_na_transport_zayavki as rn ON rn.ssylka = r.ssylka
LEFT JOIN upp_work.nomenklatura as n ON n.ssylka = rn.nomenklatura
LEFT JOIN upp_work.vidy_transportnoy_upakovki as pt ON pt.ssylka = r.vid_upakovki 
LEFT JOIN erp_sld.nomenklatura_lineynoe_oborudovanie as nlo ON nlo.ssylka = rn.nomenklatura
WHERE r.sr_mesto_rozhdeniya_dokumenta = 'a24987c8-7b76-471d-a977-e9d03c86206b'::uuid

UNION all

SELECT c.id AS request,
cp.weight,
ci.nomenclature_count,
ci.price,
ci.amount,
ci.name,
ci.skmtr,
ci.nsi,
cp.width,
cp.height,
cp.length,
''::character varying AS package_type,
cp.places_count,
CASE
    WHEN strpos(COALESCE(ci.lin_ob, '0'::text), '1'::text) > 0 THEN '1'::text
    ELSE '0'::text
END AS lineynoe_oborudovanie
FROM xpl_new.cargos as c
LEFT JOIN temp_cargo_items as ci ON ci.cargo_id = c.id
LEFT JOIN temp_cargo_places as cp ON cp.cargo_id = c.id
;
--------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------
truncate reports.t_mz_nomenclature
;
insert into reports.t_mz_nomenclature
select * from temp_final
;
--------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------
end
$procedure$
;
