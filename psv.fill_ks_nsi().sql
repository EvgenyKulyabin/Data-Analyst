-- DROP PROCEDURE psv.fill_ks_nsi();

CREATE OR REPLACE PROCEDURE psv.fill_ks_nsi()
 LANGUAGE plpgsql
AS $procedure$
begin
--===================================================================================================================================================--
--===================================================================================================================================================--
drop table if exists temp_py_nsi;
create temp table temp_py_nsi
as
with c1 as (select 
				   case length(НСИ) when 0 then '000000000'
									when 1 then '00000000' || НСИ
									when 2 then '0000000' || НСИ
									when 3 then '000000' || НСИ
									when 4 then '00000' || НСИ
									when 5 then '0000' || НСИ
									when 6 then '000' || НСИ
									when 7 then '00' || НСИ
									when 8 then '0' || НСИ
				   end as 
				   НСИ
			, replace(СКМТР, '.0', '') as СКМТР, Номенклатура, "Единицы измерения", "Вес 1 ед", Группа, 350 as koefficient
			, row_number() over (partition by НСИ order by "Вес 1 ед" desc) as rn
			from psv.t_py_nsi tpn )
select НСИ, СКМТР, Номенклатура, "Единицы измерения", "Вес 1 ед", Группа, koefficient
from c1
where c1.rn = 1
;
--select * from temp_py_nsi
--===================================================================================================================================================--
--===================================================================================================================================================--
update erp_sld.ks_nsi
set ves_za_ed_kg = case when ves_za_ed_kg is null or ves_za_ed_kg = 0 then temp_py_nsi."Вес 1 ед" else ves_za_ed_kg end
  , edinitsa_izmereniya = case when ves_za_ed_kg is null or ves_za_ed_kg = 0 then "Единицы измерения" else edinitsa_izmereniya end
--select *
from temp_py_nsi
where temp_py_nsi.НСИ = erp_sld.ks_nsi.kod_nsi
;
--selec * from erp_sld.ks_nsi
--===================================================================================================================================================--
--===================================================================================================================================================--
drop table if exists temp_ks_nsi;
create temp table temp_ks_nsi 
as
select kod_nsi 
, sk_mtr 
, klass_tmts 
, rab_name 
, tekhnicheskie_kharakteristiki 
, marka_materiala 
, chertezh 
, normativno_tekhnicheskiy_dokument 
, status_zapisi 
, lineynoe_oborudovanie 
, edinitsa_izmereniya 
, ves_za_ed_kg -----
, eto_pomeshchenie 
, koeffitsient_perescheta_ves_v_obem 
from erp_sld.ks_nsi kn 
;
--select * from temp_ks_nsi where rab_name = 'Изолятор №2627'    ; select * from temp_py_nsi
--===================================================================================================================================================--
--===================================================================================================================================================--
drop table if exists temp_inc;
create temp table temp_inc
as
select НСИ as kod_nsi from temp_py_nsi as py
except
select kod_nsi from temp_ks_nsi as ks 
;
--===================================================================================================================================================--
--===================================================================================================================================================--
insert into erp_sld.ks_nsi(kod_nsi, sk_mtr, rab_name, edinitsa_izmereniya, ves_za_ed_kg, koeffitsient_perescheta_ves_v_obem)
select НСИ
, СКМТР, Номенклатура, "Единицы измерения", "Вес 1 ед", 350 as koefficient
from temp_py_nsi
where НСИ in (select * from temp_inc)
;
--===================================================================================================================================================--
--===================================================================================================================================================--
end
$procedure$
;
