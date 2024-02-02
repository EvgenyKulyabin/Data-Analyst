-- DROP PROCEDURE psv.fill_increment_relation_erp_sld_2();

CREATE OR REPLACE PROCEDURE psv.fill_increment_relation_erp_sld_2()
 LANGUAGE plpgsql
AS $procedure$
declare
	q text;
	q1 text; --основной запрос
	ctlg varchar; --каталог (initial)
	t_schm varchar; --схема каталога, в которой ищем связи
	t_n text; --имя таблицы в которой/которых ищем связи
	c_n varchar; --имя столбца, для которого ищем связи
	i int := 1;
	y int;
	table_target varchar;
	column_target varchar;
	dt timestamptz := date_trunc('seconds', now()::timestamp); 
	cd timestamptz;
	p int := 0;
begin
	
	
drop table if exists temp_tbl;
create temp table temp_tbl 
as 
select table_name
from information_schema.tables
where table_schema = 'erp_sld'
except 
select distinct replace(table_name, 'erp_sld.', '') as table_name
--select *
from psv.t_relations_in_erp_sld;
create index ci_tbl on temp_tbl(table_name);
--truncate psv.t_increment_relations_in_erp_sld
--select * from temp_tbl
	
--if (select count(*) from temp_tbl)>0 then

	truncate psv.buffer_increment_t_erp_sld_relations;

	drop table if exists temp_nulev_guidy;
	create temp table temp_nulev_guidy 
	as 
	select schemaname , tablename , attname --, *
	from pg_stats
	where schemaname = 'erp_sld'
	and (most_common_vals::text = '{00000000-0000-0000-0000-000000000000}' or attname in ('roditel', 'imya_predopredelennykh_dannykh'))
	;
	create index ci_ng on temp_nulev_guidy(schemaname , tablename , attname)
	;
	--select * from temp_nulev_guidy
	
	
	drop table if exists temp_data
	;
	create temp table temp_data 
	as
	select cl.table_catalog, cl.table_schema, cl.table_name, cl.column_name, cl.data_type
	--, row_number() over (order by cl.table_catalog, cl.table_schema, cl.table_name) as id
	from information_schema.columns as cl
	left join temp_nulev_guidy as ps on ps.schemaname = cl.table_schema and ps.tablename = cl.table_name and ps.attname = cl.column_name 
	where 
	cl.table_catalog = 'initial'
	and 
	cl.table_schema = 'erp_sld'
	and
	cl.data_type = 'uuid'
	and 
	cl.column_name not in ('roditel', 'imya_predopredelennykh_dannykh')
	and 
	ps.tablename is null
	;
	create index ci_td on temp_data(table_name) include(table_catalog, table_schema, table_name, column_name, data_type)
	;
	--select * from temp_data
	
	
	--ИСКЛЮЧИТЬ ВЗАИМОЗАМЕНЯЕМЫЕ ПАРЫ ЗАПРОСОВ 1-2 = 2-1
	
	drop table if exists temp_dt;
	create temp table temp_dt (table_catalog varchar, table_schema varchar, table_name varchar, column_name varchar, data_type varchar, tn varchar, cn varchar, id serial primary key, id_del int);
	insert into temp_dt (table_catalog, table_schema, table_name, column_name, data_type, tn, cn)
	select d1.*, d2.table_name as tn, d2.column_name as cn
	from temp_data as d1, temp_data as d2
	where d1.table_name <> d2.table_name and d1.table_name in (select table_name from temp_tbl)
	;
	create index ci_dt on temp_dt(table_name, column_name, tn, cn);
	--select * from temp_dt

if (select count(*) from temp_dt)>0 then


	update temp_dt 
	set id_del = d.id
	from temp_dt as d
	where temp_dt.table_name = d.tn and temp_dt.column_name = d.cn and temp_dt.tn = d.table_name and temp_dt.cn = d.column_name;
	--select * from temp_dt
	
	
	drop table if exists temp_d;
	create temp table temp_d (q_ text, q1_ text, ctlg_ varchar, t_schm_ varchar, t_n_ varchar, c_n_ varchar, id serial primary key);
	insert into temp_d
	select
	'insert into temp_znach select distinct "' || column_name || '" from ' || table_schema || '.' || table_name || ' where "' || column_name || '" <> ' || quote_literal('00000000-0000-0000-0000-000000000000') as q_
	
	--, 'select ''' || table_schema || '.' || tn || ''' as tn, ''' || table_schema || '.' || cn || ''' as cn from ' || table_schema || '.' || tn
	--   || ' where ' || cn || ' in (select ' || column_name || ' from temp_znach);' as q1_
	   
	, 'select ''' || table_schema || '.' || tn || ''' as tn, ''' || cn || ''' as cn from temp_znach'
	   || ' where ' || 'znach' || ' in (select ' || cn || ' from ' || table_schema || '.' || tn || ');' as q1_
	   
	, table_catalog::varchar as ctlg_
	, table_schema::varchar as t_schm_
	, table_name::varchar as t_n_
	, column_name::varchar as c_n_
	--, id, id_del
	from temp_dt
	where id < id_del or id_del is null
	--limit 1000 --для првоерки
	;
	--select * from temp_d where id = 31
	
	y = (select count(*) from temp_d);
	
	drop table if exists temp_znach;
	create temp table temp_znach (znach uuid);
	create index ci_znach on temp_znach(znach);
	--select * from temp_znach
		
		<<L1>>
		loop
			--raise notice '%',i;
			select  q_, q1_, ctlg_, t_schm_, t_n_, c_n_  -- можно отказаться от этого запроса: писать в последующих действиях писать этот подзапросик
			into q, q1, ctlg , t_schm , t_n , c_n
			from temp_d
			where id = i;
		
			truncate temp_znach;
			execute(q);
			---------------------------------------------------------------------------------
			if (select count(*) from temp_znach) > 0 then
	--				do
	--				$p2$
				begin
					execute (q1) into /*strict*/ table_target, column_target;
					if table_target is not null and (t_n <> table_target and c_n <> column_target) then
						--для проверки небольшого количество значений
--						if p > 0 then exit L1; end if;
--						p = p + 1;
						
				--		raise notice '%',q1;
						insert into psv.buffer_increment_t_erp_sld_relations
							select ctlg, t_schm, t_schm || '.' || t_n, c_n, /*t_schm || '.' ||*/ table_target, column_target, dt, 'успешное обновление инкремента';
				--		raise notice '%', t_schm || '.' || t_n;
					end if;
					
				
				exception --если запрос не возвращает ни одной строчки, то всё падает с ошибкой; поэтому оборачиваем эту часть во внутреннюю транзакцию, чтобы такие ошибки пропускать
					when others then
						if i = y then
							exit L1;
						end if;
						i = i + 1;
						continue L1;
				end
	--				$p2$
				;
			end if;
	
			if i = y then
				exit L1;
			end if;
			i = i + 1;
	--		raise notice '%', 'зашел';
		end loop;
		---------------------------------------------------------------------------------
		--обработка на случай, если таблица _buffer будет пустая, оставит предыдущие значения в таблице
		if (select count(*) from psv.buffer_increment_t_erp_sld_relations) > 0 then
			
			---------------------------------------------------------------------------------
			--меняем таблицы и таблицы-связи местами, чтобы создать обратную пару 1-2 = 2-1
			drop table if exists temp_buf;
			create temp table temp_buf 
			as
			select table_catalog, table_schema, table_name, column_name, rel_table_name, rel_column_name, update_date, "comment"
			from psv.buffer_increment_t_erp_sld_relations
			union 
			select table_catalog, table_schema, rel_table_name, rel_column_name, table_name, column_name, update_date, "comment"
			from psv.buffer_increment_t_erp_sld_relations where rel_table_name is not null;
			--select * from temp_buf
			
			truncate psv.buffer_increment_t_erp_sld_relations;
			insert into psv.buffer_increment_t_erp_sld_relations (table_catalog, table_schema, table_name, column_name, rel_table_name, rel_column_name, update_date, "comment")
			select table_catalog, table_schema, table_name, column_name, rel_table_name, rel_column_name, update_date, "comment"
			from temp_buf;
			---------------------------------------------------------------------------------
			--добавляем столбцы, по которым не нашли связи
			insert into psv.buffer_increment_t_erp_sld_relations (table_catalog, table_schema, table_name, column_name, rel_table_name, rel_column_name, update_date, "comment")
			select d.table_catalog, d.table_schema, d.table_schema || '.' || d.table_name, d.column_name, null, null, dt, 'успешное обновление инкремента'
			from temp_data as d
			left join psv.buffer_increment_t_erp_sld_relations as bf on bf.table_catalog = d.table_catalog and bf.table_schema = d.table_schema and bf.table_name = d.table_schema || '.' || d.table_name and bf.column_name = d.column_name
			where bf.column_name is null;
			--select * from temp_data
			--select * from psv.buffer_increment_t_erp_sld_relations
			---------------------------------------------------------------------------------
			truncate psv.t_increment_relations_in_erp_sld;
			insert into psv.t_increment_relations_in_erp_sld(table_catalog
											  , table_schema
											  , table_name
											  , column_name
											  , rel_table_name
											  , rel_column_name
											  , update_date
											  , "comment")
			select table_catalog
				 , table_schema
				 , table_name
				 , column_name
				 , rel_table_name
				 , rel_column_name
				 , update_date
				 , "comment"
			from psv.buffer_increment_t_erp_sld_relations
			;
			insert into psv.log__fill_relations_in_schemas
			select 'psv.fill_relation_erp_sld', dt, clock_timestamp(), 'успешное обновление инкремента'
			;
			insert into psv.t_relations_in_erp_sld 
			select * from psv.t_increment_relations_in_erp_sld
			;
		else 
			update psv.t_increment_relations_in_erp_sld
			set "comment" = 'обновление инкремента ' || dt || ' не прошло: данные от ' || date_trunc('seconds', update_date::timestamptz)
			;
			update psv.t_increment_relations_in_erp_sld
			set update_date = dt
			;
			insert into psv.log__fill_relations_in_schemas
			select 'psv.fill_relation_erp_sld', dt, clock_timestamp(), 'обновление инкремента ' || dt || ' не прошло'
			;
		end if
		;
end if;


call psv.fill_erp_sld_comment_to_column();

end
$procedure$
;
