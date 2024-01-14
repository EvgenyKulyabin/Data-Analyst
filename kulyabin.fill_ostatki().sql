-- DROP PROCEDURE psv.fill_ostatki();

CREATE OR REPLACE PROCEDURE psv.fill_ostatki()
 LANGUAGE plpgsql
AS $procedure$
declare 
		vhod int := 1;

begin

	
drop table if exists temp_ostatki; 
create temp table temp_ostatki
as 
select
--date(date_trunc('day', date_trunc('month', tns."period"::TIMESTAMP) + '1 month'::interval - '1 hour'::interval )) as last_day_month,
extract (year from tns."period") as "yaer",
extract (month from tns."period") as "month",
extract (year from tns."period")::text || '-' || case when extract (month from tns."period") < 10 then '0' || extract (month from tns."period")::text else extract (month from tns."period")::text end as yyyy_mm,
extract (year from current_date)::text || '-' || case when extract (month from current_date)+1 < 10 then '0' || (extract (month from current_date)+1)::text else (extract (month from current_date)+1)::text end as yyyy_mm_now,
--case when extract (month from tns."period")=1 then
--(extract(year from tns."period")::integer-1)::text || '-12'
--else
--extract (year from tns."period")::text || '-' || case when extract(month from tns."period")::integer-1 < 10 then '0' || (extract(month from tns."period")::integer-1)::text else (extract(month from tns."period")::integer-1)::text end
--end as yyyy_mm_1,
s.naimenovanie as sklad ,

--sp.naimenovanie as pomeshchenie ,

n.nsi_kod as nsi ,
n.artikul as skmtr ,
tns.nomenklatura as id_nomenklatura,
n.naimenovanie_polnoe as nomenklatura,
sum(case when tns.vid_dvizheniya = '0' then 1 else -1 end * tns.v_nalichii) as ostatok
--select *
from erp_sld.tovary_na_skladah tns --limit 100
left join erp_sld.sklady s on s.ssylka = tns.sklad
left join erp_sld.nomenklatura n on n.ssylka = tns.nomenklatura

--left join erp_sld.skladskie_pomeshcheniya sp on sp.ssylka = tns.pomeshchenie

where sklad is not null
group by 
--date(date_trunc('day', date_trunc('month', tns."period"::TIMESTAMP) + '1 month'::interval - '1 hour'::interval )),
extract (year from tns."period"),
extract (month from tns."period"),
s.naimenovanie ,
--sp.naimenovanie ,
n.nsi_kod ,
n.artikul ,
tns.nomenklatura,
n.naimenovanie_polnoe,
extract (year from tns."period")::text || '-' || case when extract (month from tns."period") < 10 then '0' || extract (month from tns."period")::text else extract (month from tns."period")::text end,
extract (year from current_date)::text || '-' || case when extract (month from current_date)+1 < 10 then '0' || (extract (month from current_date)+1)::text else (extract (month from current_date)+1)::text end 
;
--order by 1,2,5
--limit 5


drop table if exists temp_ostatki_2;
create temp table temp_ostatki_2
as
select *,
sum(ostatok) over (partition by sklad,nsi,skmtr,id_nomenklatura,nomenklatura/*,pomeshchenie*/ order by yyyy_mm ) as ostatok_m,
lag(yyyy_mm,-1,yyyy_mm_now) over (partition by sklad,nsi,skmtr,id_nomenklatura,nomenklatura/*,pomeshchenie*/ order by yyyy_mm) as data_lag
from temp_ostatki;

--select * from temp_ostatki o
--where o.sklad = '10012 Основной '
--and o.skmtr = '8542110100'


drop table if exists temp_calendar; 
create temp table temp_calendar as 
select distinct 
date(date_trunc('day', date_trunc('month', c."date"::TIMESTAMP) + '1 month'::interval - '1 hour'::interval )) as last_day_month,
extract (year from c."date") as "year",
extract (month from c."date") as "month",
extract (year from c."date")::text ||'-' || case when extract (month from c."date") < 10 then '0' || extract (month from c."date")::text else extract (month from c."date")::text end as yyyy_mm
from dict.calendar c
where "date">='2018-07-31'
order by 1;
create index ci_calendar on temp_calendar(yyyy_mm);
--select * from temp_calendar


drop table if exists temp_final;
create temp table temp_final
as
select c.*, o.sklad/*, o.pomeshchenie*/, o.nsi, o.skmtr, o.id_nomenklatura, o.nomenklatura, o.ostatok_m
from temp_calendar c
left join temp_ostatki_2 o on (o.yyyy_mm <= c.yyyy_mm and o.data_lag > c.yyyy_mm) or (o.data_lag=o.yyyy_mm and o.yyyy_mm=c.yyyy_mm)
where
o.ostatok_m<>0;
--and 
--and o.sklad = '10012 Основной '
--and o.skmtr = '8542110100'
--and c.last_day_month = '2023-02-28';
--select * from temp_final



--drop table if exists psv.t_ostatki;
--create table psv.t_ostatki as select * from temp_final;

--	begin
		truncate psv.t_ostatki;
		insert into psv.t_ostatki
		select * from temp_final;
--	exception when others
--		then rollback;
--	end;


end;
$procedure$
;
