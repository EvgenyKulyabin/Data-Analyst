-- DROP PROCEDURE reports.fill_ostatki();

CREATE OR REPLACE PROCEDURE reports.fill_ostatki()
 LANGUAGE plpgsql
AS $procedure$
begin
-----------------------------------------------------------------------
drop table if exists temp_lts;
create temp table temp_lts
as	
select 
t.ib ,
t1."СкладWMS" ,
--t.sklad ,
t.nomenklatura ,
coalesce (t.skmtr, t.artikul) as skmtr ,
t.nsi ,
t.id_nsi ,
case 
	when t.seriya ilike '%Без%' then '' 
	when t.seriya ilike '%б/н%' then ''
	when t.seriya = '%бн%' then ''
else coalesce (t.seriya, '') end as seriya ,
t.edenica_izmereniya ,
sum(t.kolichestvo) as kolichestvo_lts,
coalesce (lo.sr_lineynoe_oborudovanie, false) as lo
from lts.ostatki t
right join modus_demo.svyaz_skladov t1 on t1."СкладГуидЛокотех" = t.uid_sklada
left join erp_sld.nomenklatura_lineynoe_oborudovanie lo on lo.identifikator_nsi::text = t.id_nsi
where t1."СкладWMS" != '-'
group by 1,2,3,4,5,6,7,8,10
;
--select * from temp_lts
----------------------------------------------------------------------
drop table if exists temp_slr;
create temp table temp_slr
as
select
t1."BAZA",
t."Склад" ,
--t."Помещение" ,
t."Номенклатура" ,
t."СКМТР" ,
t."НСИ" ,
t."ИдентификаторНСИ" ,
case 
		when t."Серия" ilike '%б/н%' then ''
		when t."Серия" ilike '%Без%Серии%' then ''
		when t."Серия" ilike '%Без%Номера%' then ''
else coalesce (split_part("Серия", ' от', 1), '') end as "Серия" ,
t."ЕдИзм" ,
sum(replace(replace(t."ВНаличииОстаток",' ',''),',','.')::numeric) as kolichestvo_slr,
coalesce (lo.sr_lineynoe_oborudovanie, false) as lo
from modus_demo."ostatki_3.0" t
right join (select distinct "СкладWMS", "BAZA" from modus_demo.svyaz_skladov) t1 on t."Склад" = t1."СкладWMS"
left join erp_sld.nomenklatura_lineynoe_oborudovanie lo on lo.identifikator_nsi::text = t."ИдентификаторНСИ"
where t."Помещение" in 
('A-3',
'А-6',
'ДМТО',
'Излишки',
'Логимат',
'Основное',
'Основное',
'основное',
'Основное ',
'Основное Зелёная',
'Основное Салтыкова-Щедрина',
'Открытая площадка',
'Открытая площадка Салтыкова-Щедрина',
'ОХ ТД ЛокоТех')
group by 1,2,3,4,5,6,7,8,10
;
--select * from temp_slr
------------------------------------------------------------
drop table if exists temp_final;
create temp table temp_final
as
select
coalesce ("СкладWMS","Склад") as sklad,
coalesce (nomenklatura, "Номенклатура") as nomenklatura,
coalesce (skmtr, "СКМТР") as skmtr,
coalesce (nsi,"НСИ" ) as nsi,
coalesce (id_nsi, "ИдентификаторНСИ") as id_nsi,
coalesce (edenica_izmereniya, "ЕдИзм") as ed_izm,
coalesce (seriya, "Серия") as seriya,
coalesce (kolichestvo_lts, 0) as kolichestvo_lts,
coalesce (kolichestvo_slr, 0) as kolichestvo_slr,
coalesce (kolichestvo_lts, 0) - coalesce (kolichestvo_slr, 0) as raznica ,
coalesce (l.lo , s.lo)
from temp_lts l 
full outer join temp_slr s on l."СкладWMS" = s."Склад" and l.id_nsi = s."ИдентификаторНСИ" and l.seriya = s."Серия"
where l.kolichestvo_lts - s.kolichestvo_slr !=  0 or l.kolichestvo_lts - s.kolichestvo_slr is null
and coalesce (nomenklatura, "Номенклатура") is not null
order by sklad , nomenklatura 
;

--select * from temp_final where sklad = 'СУ-021 Шушары'

/*create table reports.t_ostatki
as
select * from temp_final;

GRANT SELECT ON TABLE reports.t_ostatki TO grishinam;
GRANT SELECT, DELETE, REFERENCES, TRUNCATE, INSERT, TRIGGER, UPDATE ON TABLE reports.t_ostatki TO kulyabin;
GRANT SELECT ON TABLE reports.t_ostatki TO mustafinrf;
GRANT SELECT, DELETE, REFERENCES, TRUNCATE, INSERT, TRIGGER, UPDATE ON TABLE reports.t_ostatki TO kapustinav;
GRANT SELECT ON TABLE reports.t_ostatki TO readonly; 
*/


truncate reports.t_ostatki;
insert into reports.t_ostatki
select * from temp_final;
       
end
$procedure$
;
