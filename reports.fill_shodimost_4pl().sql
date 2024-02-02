-- DROP PROCEDURE reports.fill_shodimost_4pl();

CREATE OR REPLACE PROCEDURE reports.fill_shodimost_4pl()
 LANGUAGE plpgsql
AS $procedure$

begin
	
--Блок Салаир--------------------------------------------------------------------------------	
	
drop table if exists salair;
create temp table salair as 	
with 
ro as 
(
select
	t1.khtk_elektronnaya_zayavka_zavoda as  id_zakaz, 
	t1.nomer as nomer_ro,
	date(t1."data") as data_ro,
	t1.pometka_udaleniya as udalen_ro,
	t4.naimenovanie as menedzher,
	'Кладовщик' as kladovshik,
	t2.nomenklatura as id_nomenklatura,
	t3.naimenovanie_polnoe ,
	t3.identifikator_nsi ,
	t3.artikul as skmtr,
	t3.nsi_kod as nsi,
	sum(t2.kolichestvo) as kolichestvo
from 
	erp_sld.raskhodnyy_order_na_tovary t1
	left join erp_sld.raskhodnyy_order_na_tovary_otgruzhaemye_tovary t2 on t2.ssylka = t1.ssylka
	left join erp_sld.nomenklatura t3 on t3.ssylka = t2.nomenklatura
	left join erp_sld.polzovateli t4 on t4.ssylka =  t1.otvetstvennyy 
where t1.khtk_elektronnaya_zayavka_zavoda != '00000000-0000-0000-0000-000000000000'
group by 1,2,3,4,5,6,7,8,9,10,11
)
select
	t1.identifikator_vnutrennego_zakaza ,
	t1.pometka_udaleniya ,
    t1.otmenen, 
	t1.proveden ,
	s.naimenovanie ,
	t1.sklad_poluchatel ,
	ro.menedzher,
	ro.kladovshik,
	t1.nomer,
	date(t1."data") as "data",
	t1.nomer_vnutrennego_zakaza,
	t1.sr_status_vypolneniya_zakaza,
	ro.naimenovanie_polnoe ,
	ro.id_nomenklatura as nomenklatura, 
	ro.identifikator_nsi ,
	ro.skmtr as artikul ,
	ro.nsi as nsi_kod ,
 	ro.kolichestvo,
	ro.nomer_ro,
	ro.data_ro,
	ro.udalen_ro
from erp_sld.khtk_elektronnaya_zayavka_zavoda t1
left join erp_sld.sklady s on s.ssylka = t1.sklad
left join ro on ro.id_zakaz = t1.ssylka 
where t1."data" >= '2020-10-20'
;

--Блок ЛокоТех--------------------------------------------------------------------------------

drop table if exists lt;
create temp table lt as
select
	t1.id_zakaza as identifikator_vnutrennego_zakaza_lt,
	t1.pometka_udaleniya as pometka_udaleniya_erp_lt ,
	t1.pts_udalen as pts_udalen_lt,
	t1.nomer_zakaza as nomer_vnutrennego_zakaza_lt,
	date(t1.data_zakaza) as data_vnutrennego_zakaza_lt,
	t1.nomenklatura as naimenovanie_polnoe_lt,
	t1.id_nsi as identifikator_nsi_lt,
	t1.skmtr as skmyr_lt ,
	t1.nsi as nsi_lt ,
	t1.kolichestvo as kolichestvo_lt,
	t2.predstavlenie as dokument_sea_lt,
	t2.pometka_udaleniya as pometka_udaleniya_sea_lt ,
	t2.fail as fail_sea_lt,
	t2.otvetstvenniy_salair ,
	t2.otvetstvenniy_oco ,
	t2.komentarii 
from lts.erp_4pl t1
	left join lts.sea_4pl t2 on t1.pts_document = t2.id 
;	
--Сводна таблица-----------------------------------------------------------------------------

drop table if exists svod;
create temp table svod as
select 
case when 
		pometka_udaleniya is false 
		and otmenen is false 
		and proveden is true 
		and kolichestvo = kolichestvo_lt 
		and pometka_udaleniya_erp_lt = 'Нет' 
		and pts_udalen_lt = 'Нет'
		and pometka_udaleniya_sea_lt = 'Нет'
		and fail_sea_lt = 'Да'
		and otvetstvenniy_salair = 'Согласован'
		and otvetstvenniy_oco = 'Согласован'
		and komentarii = 'Согласован'
		then 'Нет' 
	when otmenen is true and identifikator_vnutrennego_zakaza_lt is null then 'Нет' 
	when pometka_udaleniya is true and pometka_udaleniya_erp_lt = 'Да' then 'Нет'
	when otmenen is true and proveden is true and pts_udalen_lt = 'Да' and pometka_udaleniya_sea_lt = 'Да' then 'Нет'
		else 'Да' end as nalichie_oshibki,
* 
from 
	salair 
	left join lt on salair.identifikator_vnutrennego_zakaza = lt.identifikator_vnutrennego_zakaza_lt and salair.identifikator_nsi = lt.identifikator_nsi_lt
;
--select * from svod

--ИТОГ--------------------------------------------------------------------------------------

drop table if exists temp_final;
create temp table temp_final as
select 
	case		
		when svod.pometka_udaleniya is false and pometka_udaleniya_erp_lt = 'Да' then 'Заказ удален в базе ЛокоТех и не удален в базе Салаир'--1
		when svod.naimenovanie is null and svod.identifikator_vnutrennego_zakaza_lt is null then 'Заказ удален в базе ЛокоТех и не удален в базе Салаир'--1
		when svod.otmenen is true and pometka_udaleniya_erp_lt = 'Нет' then 'Заказ отменен в базе Салаир и не удален в базе ЛокоТех'--2
		when pts_udalen_lt = 'Да' then 'Удалено поступление товаров на склад в базе ЛокоТех'--3
		when kolichestvo - kolichestvo_lt != 0 then 'Расхождение по количеству'--4
		when dokument_sea_lt is null then 'Не создан документ СЭА'--5
		when fail_sea_lt = 'Нет' then 'Не вложен скан в документ СЭА'--6
		when svod.identifikator_vnutrennego_zakaza is null or svod.identifikator_vnutrennego_zakaza_lt is null then 'Номенклатура не соответсвует справочнику НСИ в ЛТ'--7
		when komentarii like '1)%' then 'Неполный комплект первичных документов'--8
		when komentarii like '2)%Некорректное%' then 'Некорректное оформление первичных документов'--9
		when komentarii like '2)%Ошибка%' then 'Ошибка в системном и/или первичном документе'--10
		when komentarii like '3)%Неудовлетворительное%' then 'Неудовлетворительное качество «скан-образов» первичных документов'--11
		when komentarii like '3)%Несоответствие%' then 'Несоответствие предоставленных первичных документов, документу, заведенному в 1С'--12
		when komentarii like '4)%' then 'Неудовлетворительное качество «скан-образов» первичных документов'--11
		when komentarii like '5)%' then 'Системный документ не прошел контроль'--13
		when komentarii like '6)%' then 'Возврат на доработку по инициативе функционального подразделения'--14
		when komentarii like 'Системный документ не проведен%' then 'Системный документ не проведен в оперативном управленческом учете программы 1С:ЕРП ЛТС'--15
		when svod.pometka_udaleniya is false and svod.otmenen is false and svod.proveden is false then 'Не проведен Заказ в базе Салаир'--16
		else 'Прочие ошибки'--17
	end as tip_oshibki,
	case		
		when svod.pometka_udaleniya is false and pometka_udaleniya_erp_lt = 'Да' then '1'--1
		when svod.naimenovanie is null and svod.identifikator_vnutrennego_zakaza_lt is null then '1'--1
		when svod.otmenen is true and pometka_udaleniya_erp_lt = 'Нет' then '2'--2
		when pts_udalen_lt = 'Да' then '3'--3
		when kolichestvo - kolichestvo_lt != 0 then '4'--4
		when dokument_sea_lt is null then '5'--5
		when fail_sea_lt = 'Нет' then '6'--6
		when svod.identifikator_vnutrennego_zakaza is null or svod.identifikator_vnutrennego_zakaza_lt is null then '7'--7
		when komentarii like '1)%' then '8'--8
		when komentarii like '2)%Некорректное%' then '9'--9
		when komentarii like '2)%Ошибка%' then '10'--10
		when komentarii like '3)%Неудовлетворительное%' then '11'--11
		when komentarii like '3)%Несоответствие%' then '12'--12
		when komentarii like '4)%' then '11'--11
		when komentarii like '5)%' then '13'--13
		when komentarii like '6)%' then '14'--14
		when komentarii like 'Системный документ не проведен%' then '15'--15
		when svod.pometka_udaleniya is false and svod.otmenen is false and svod.proveden is false then '16'--16
		else '17'--17
	end as kod_oshibki,
	z.sklad as id_sklad,
	svod.* 
from svod 
	left join erp_sld.khtk_elektronnaya_zayavka_zavoda z on z.identifikator_vnutrennego_zakaza = coalesce (svod.identifikator_vnutrennego_zakaza, svod.identifikator_vnutrennego_zakaza_lt)
where nalichie_oshibki = 'Да'
;
-- select * from temp_final

--drop table if exists reports.t_shodimost_4pl;
--create table reports.t_shodimost_4pl as select * from temp_final	
truncate reports.t_shodimost_4pl;
insert into reports.t_shodimost_4pl
select * from temp_final;
       
end
$procedure$
;
