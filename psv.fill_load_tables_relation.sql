-- DROP PROCEDURE psv.fill_load_tables_relation(text, varchar, varchar, varchar, varchar);

CREATE OR REPLACE PROCEDURE psv.fill_load_tables_relation(par_q text, par_catalog character varying, par_sche character varying, par_t_n character varying, par_c_n character varying)
 LANGUAGE plpgsql
AS $procedure$
declare
	q1 text := par_q; --основной запрос
	ctlg varchar := par_catalog; --каталог (initial)
	t_schm varchar := par_sche; --схема каталога, в которой ищем связи
	t_n text := par_t_n; --имя таблицы в которой/которых ищем связи
	c_n varchar := par_c_n; --имя столбца, для которого ищем связи
	table_target varchar;
	column_target varchar;
	dt timestamptz := date_trunc('seconds', now()::timestamp); --дата и время обновления
begin
	---------------------------------------------------------------------------------
	--выполняем основной запрос
	--insert into psv.temp_ttt values('Прошло');
	execute (q1) into /*strict*/ table_target, column_target;
	if table_target is not null and (t_n <> table_target and c_n <> column_target) then
--		raise notice '%',q1;
		insert into psv.buffer_t_abl_buh_relations
			select ctlg, t_schm, t_schm || '.' || t_n, c_n, t_schm || '.' || table_target, column_target, dt, 'успешное обновление';
--		raise notice '%', t_schm || '.' || t_n;
	end if;
	---------------------------------------------------------------------------------
	--обработка на случай, если таблица _buffer будет пустая, оставит предыдущие значения в таблице
	if (select count(*) from psv.buffer_t_abl_buh_relations) <> 0 then
		truncate psv.t_relations_in_abl_buh;
		insert into psv.t_relations_in_abl_buh(table_catalog
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
		from psv.buffer_t_abl_buh_relations
		;
		insert into log__fill_relations_in_schemas
		select 'psv.fill_relation_abl_buh', dt, now(), 'успешное обновление'
		;
--	else 
--		update psv.t_relations_in_abl_buh
--		set "comment" = 'обновление ' || dt || ' не прошло: данные от ' || date_trunc('seconds', update_date::timestamptz)
--		;
--		update psv.t_relations_in_abl_buh
--		set update_date = dt
--		;
--		insert into log__fill_relations_in_schemas
--		select 'psv.fill_relation_abl_buh', dt, now(), 'обновление ' || dt || ' не прошло: данные от ' || date_trunc('seconds', update_date::timestamptz)
--		;
	end if
	;

--exception --если запрос не возвращает ни одной строчки, то всё падает с ошибкой; поэтому оборачиваем эту часть во внутреннюю транзакцию, чтобы такие ошибки пропускать
--	when others then
--		null;
	
end
$procedure$
;
