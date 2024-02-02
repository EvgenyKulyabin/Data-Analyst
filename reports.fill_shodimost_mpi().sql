-- DROP PROCEDURE reports.fill_shodimost_mpi();

CREATE OR REPLACE PROCEDURE reports.fill_shodimost_mpi()
 LANGUAGE plpgsql
AS $procedure$
begin
------------------------------------------------------------
drop table if exists temp_mpi;
create temp table temp_mpi
as
select * from (
select
m.data_zagruzki,
m.organizaciya as sld,
m.sklad,
m.tip_dvizheniya,
m.registrator,
m.udalen,
m.proveden,
m.nomer,
m."data",
m.id_nsi,
m.nsi,
m.skmtr,
m.nomenklatura,
m.seriya,
m.kolichestvo,
sm.predstavlenie,
sm.udalen as udalen_predstavlenie,
sm.fail,
sm.otvetstvenniy_salair,
sm.otvetstvenniy_oco,
sm.komentarii,
case when m.proveden = 'Да' and m.udalen = 'Нет' and sm.fail = 'Да' and sm.udalen = 'Нет' and sm.otvetstvenniy_salair = 'Согласован' and sm.otvetstvenniy_oco = 'Согласован' and sm.komentarii = 'Согласован' then 'Нет' else 'Да' end as oshibki
from lts.mpi m
left join lts.sea_mpi sm on sm.id_dokumenta_oco = m.id_dokumenta_oco 
where
m.tip_dvizheniya in 
('Перемещение товаров',
'Производство без заказа',
'Поступление товаров на склад',
'Поступление сырья от давальца',
'Передача давальцу',
'Внутреннее потребление товаров'
'Отгрузка товаров с хранения')
) t where oshibki = 'Да'
;
-- select * from temp_mpi

----------------------------------------------------------------

drop table if exists temp_final;
create temp table temp_final as
select
	case
		when predstavlenie is null then 'Не создан документ СЭА'--1
		when udalen_predstavlenie = 'Да' then 'Удален документ СЭА'--2
		when fail = 'Нет' then 'Не вложен скан в документ СЭА'--3
		when komentarii like '1)%' then 'Неполный комплект первичных документов'--4
		when komentarii like '2)%Некорректное%' then 'Некорректное оформление первичных документов'--5
		when komentarii like '2)%Ошибка%' then 'Ошибка в системном и/или первичном документе'--6
		when komentarii like '3)%Неудовлетворительное%' then 'Неудовлетворительное качество «скан-образов» первичных документов'--7
		when komentarii like '3)%Несоответствие%' then 'Несоответствие предоставленных первичных документов, документу, заведенному в 1С'--8
		when komentarii like '4)%' then 'Неудовлетворительное качество «скан-образов» первичных документов'--9
		when komentarii like '5)%' then 'Системный документ не прошел контроль'--10
		when komentarii like '6)%' then 'Возврат на доработку по инициативе функционального подразделения'--11
		else 'Прочие ошибки'--12		
	end as tip_oshibki,
	case
		when predstavlenie is null then '1'--1
		when udalen_predstavlenie = 'Да' then '2'--2
		when fail = 'Нет' then '3'--3
		when komentarii like '1)%' then '4'--4
		when komentarii like '2)%Некорректное%' then '5'--5
		when komentarii like '2)%Ошибка%' then '6'--6
		when komentarii like '3)%Неудовлетворительное%' then '7'--7
		when komentarii like '3)%Несоответствие%' then '8'--8
		when komentarii like '4)%' then '9'--9
		when komentarii like '5)%' then '10'--10
		when komentarii like '6)%' then '11'--11
		else '12'--12		
	end as kod_oshibki,
	temp_mpi.*
from temp_mpi
;

--select * from temp_final

--drop table if exists reports.t_shodimost_mpi;
--create table reports.t_shodimost_mpi as select * from temp_final	
truncate reports.t_shodimost_mpi;
insert into reports.t_shodimost_mpi
select * from temp_final;
       
end
$procedure$
;
