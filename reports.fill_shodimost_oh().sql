-- DROP PROCEDURE reports.fill_shodimost_oh();

CREATE OR REPLACE PROCEDURE reports.fill_shodimost_oh()
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
	t1.sr_zakaz_lokotekh as id_zakaz,					  --ОХ
	--t1.khtk_elektronnaya_zayavka_zavoda as as id_zakaz, --4PL
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
where t1.sr_zakaz_lokotekh !='00000000-0000-0000-0000-000000000000'
--t1.khtk_elektronnaya_zayavka_zavoda != '00000000-0000-0000-0000-000000000000'
group by 1,2,3,4,5,6,7,8,9,10,11
)
select 
	t1.identifikator_dokumenta_v_lokotekh ,
	t1.pometka_udaleniya ,
	t1.status_vygruzki_v_lokotekh ,
	t1.sklad_salair as id_sklad,
	s.naimenovanie as sklad,
	t1.sklad_predstavlenie ,
	ro.menedzher,
	ro.kladovshik,
	t1.kod ,
	t1.nomer_nakladnoy_lokotekh ,
	date(t1.data_sozdaniya_v_baze_lokotekh) as data_sozdaniya_v_baze_lokotekh,
	t1.status ,
	ro.naimenovanie_polnoe,
	ro.id_nomenklatura as nomenklatura,
	ro.identifikator_nsi,
	ro.skmtr as artikul,
	ro.nsi as nsi_kod,
	ro.kolichestvo,
	ro.nomer_ro,
	ro.data_ro,
	ro.udalen_ro,
	row_number() over (partition by t1.identifikator_dokumenta_v_lokotekh,ro.identifikator_nsi order by ro.kolichestvo) as partiya_s
from
	erp_sld.sr_dannye_otgruzok_lokotekh t1
	left join erp_sld.sklady s on s.ssylka = t1.sklad_salair
	left join ro on ro.id_zakaz = t1.ssylka
;
--select * from salair where kod in ('001727226','001727202')

--Блок ЛокоТех--------------------------------------------------------------------------------

drop table if exists lt;
create temp table lt as
select
	t1.id_zakaza as identifikator_vnutrennego_zakaza_lt,
	t1.pometka_udaleniya as pometka_udaleniya_erp_lt,
	t1.proveden as proveden_lt ,
	t1.nomer_zakaza as nomer_vnutrennego_zakaza_lt,
	date(t1.data_zakaza) as data_vnutrennego_zakaza_lt,
	t1.status_zakaza as status_zakaza_lt,
	t1.nomenklatura as naimenovanie_polnoe_lt,
	t1.id_nsi as identifikator_nsi_lt,
	t1.skmtr as skmtr_lt,
	t1.nsi as nsi_lt,
	t1.kolichestvo as kolichestvo_lt,
	t2.postuplenie as dokument_sea_lt,
	t2.fail as fail_sea_lt,
	t2.otvetstvenniy_salair ,
	t2.otvetstvenniy_oco ,
	t2.komentarii,
	row_number() over (partition by t1.id_zakaza, t1.id_nsi order by t1.kolichestvo) as partiya_l
from lts.erp_ox t1
	left join lts.sea_ox t2 on t2.guid = t1.id_documenta_sea 	
;
--select * from lt where identifikator_vnutrennego_zakaza_lt = '2ace78d3-7abb-11ee-ada4-90e2baeb8731'

--Сводна таблица-----------------------------------------------------------------------------

drop table if exists svod;
create temp table svod as
select 
	case when
			pometka_udaleniya is false
			--and status_vygruzki_v_lokotekh = 4
			and kolichestvo = kolichestvo_lt
			and pometka_udaleniya_erp_lt = 'Нет'
			and proveden_lt = 'Да'
			and fail_sea_lt = 'Есть файл'
			and otvetstvenniy_salair = 'Согласован'
			and otvetstvenniy_oco = 'Согласован'
			and komentarii = 'Согласован'
			then 'Нет'
		when pometka_udaleniya is true and 	identifikator_vnutrennego_zakaza_lt is null then 'Нет'
		when pometka_udaleniya is true and 	pometka_udaleniya_erp_lt = 'Да' and dokument_sea_lt is null then 'Нет'
		when pometka_udaleniya is true and 	pometka_udaleniya_erp_lt = 'Да' and dokument_sea_lt is not null and otvetstvenniy_salair = 'Согласован' and otvetstvenniy_oco = 'Согласован' and komentarii = 'Согласован' then 'Нет'
			else 'Да' end as nalichie_oshibki,
* 
from salair 
	 left join lt on salair.identifikator_dokumenta_v_lokotekh = lt.identifikator_vnutrennego_zakaza_lt and salair.identifikator_nsi = lt.identifikator_nsi_lt and salair.partiya_s = lt.partiya_l
;
--select * from svod where kod in ('001727226','001727202')

--ИТОГ--------------------------------------------------------------------------------------

drop table if exists temp_final;
create temp table temp_final as
select
	case
		when pometka_udaleniya is false and identifikator_vnutrennego_zakaza_lt is null then 'Заказ удален в базе ЛокоТех и не удален в базе Салаир'--1
		when pometka_udaleniya is false and identifikator_vnutrennego_zakaza_lt is not null and pometka_udaleniya_erp_lt = 'Да'  then 'Заказ удален в базе ЛокоТех и не удален в базе Салаир'--1
		when pometka_udaleniya is true and pometka_udaleniya_erp_lt = 'Нет' then 'Заказ отменен в базе Салаир и не удален в базе ЛокоТех'--2
		when kolichestvo != kolichestvo_lt then 'Расхождение по количеству'--3
		when pometka_udaleniya is false and identifikator_vnutrennego_zakaza_lt is not null and pometka_udaleniya_erp_lt = 'Нет' and dokument_sea_lt is null then 'Не создан документ СЭА'--4
		when pometka_udaleniya is false and identifikator_vnutrennego_zakaza_lt is not null and pometka_udaleniya_erp_lt = 'Нет' and dokument_sea_lt is not null and fail_sea_lt = 'Нет файла'  then 'Не вложен скан в документ СЭА'--5
		when komentarii like '1)%' then 'Неполный комплект первичных документов'--6
		when komentarii like '2)%Некорректное%' then 'Некорректное оформление первичных документов'--7
		when komentarii like '2)%Ошибка%' then 'Ошибка в системном и/или первичном документе'--8
		when komentarii like '3)%Несоответствие%' then 'Несоответствие предоставленных первичных документов, документу, заведенному в 1С'--9
		when komentarii like '3)%Неудовлетворительное%' then 'Неудовлетворительное качество «скан-образов» первичных документов'--10
		when komentarii like '4)%' then 'Возврат на доработку по инициативе функционального подразделения'--11
		when komentarii like '5)%' then 'Системный документ не прошел контроль'--12
		when komentarii like '6)%' then 'Возврат на доработку по инициативе функционального подразделения'--11
		when komentarii like '7)%' then 'Период закрыт. Документы не могут быть приняты к учету'--13
		when komentarii like '8)%' then 'Отсутствует системный документ'--14
		when komentarii like '9)%' then 'Отсутсвуют связи с документом'--15
		when otvetstvenniy_oco != 'Согласован' then 'Прочие ошибки'--17
		when pometka_udaleniya is false and pometka_udaleniya_erp_lt = 'Нет' and proveden_lt = 'Нет' then 'Заказ не проведен и не удален в базе ЛокоТех'--16
	end as 	tip_oshibki,
	case
		when pometka_udaleniya is false and identifikator_vnutrennego_zakaza_lt is null then '1'--1
		when pometka_udaleniya is false and identifikator_vnutrennego_zakaza_lt is not null and pometka_udaleniya_erp_lt = 'Да'  then '1'--1
		when pometka_udaleniya is true and pometka_udaleniya_erp_lt = 'Нет' then '2'--2
		when kolichestvo != kolichestvo_lt then '3'--3
		when pometka_udaleniya is false and identifikator_vnutrennego_zakaza_lt is not null and pometka_udaleniya_erp_lt = 'Нет' and dokument_sea_lt is null then '4'--4
		when pometka_udaleniya is false and identifikator_vnutrennego_zakaza_lt is not null and pometka_udaleniya_erp_lt = 'Нет' and dokument_sea_lt is not null and fail_sea_lt = 'Нет файла'  then '5'--5
		when komentarii like '1)%' then '6'--6
		when komentarii like '2)%Некорректное%' then '7'--7
		when komentarii like '2)%Ошибка%' then '8'--8
		when komentarii like '3)%Несоответствие%' then '9'--9
		when komentarii like '3)%Неудовлетворительное%' then '10'--10
		when komentarii like '4)%' then '11'--11
		when komentarii like '5)%' then '12'--12
		when komentarii like '6)%' then '11'--11
		when komentarii like '7)%' then '13'--13
		when komentarii like '8)%' then '14'--14
		when komentarii like '9)%' then '15'--15
		when otvetstvenniy_oco != 'Согласован' then '17'--17
		when pometka_udaleniya is false and pometka_udaleniya_erp_lt = 'Нет' and proveden_lt = 'Нет' then '16'--16
	end as 	kod_oshibki,
svod.*
from svod
where nalichie_oshibki = 'Да'
;
--select * from temp_final

--drop table if exists reports.t_shodimost_oh;
--create table reports.t_shodimost_oh as select * from temp_final	
truncate reports.t_shodimost_oh;
insert into reports.t_shodimost_oh
select * from temp_final;
	
end
$procedure$
;
