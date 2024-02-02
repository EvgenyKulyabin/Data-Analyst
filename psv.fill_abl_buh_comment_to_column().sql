-- DROP PROCEDURE psv.fill_abl_buh_comment_to_column();

CREATE OR REPLACE PROCEDURE psv.fill_abl_buh_comment_to_column()
 LANGUAGE plpgsql
AS $procedure$
declare
	q text;
	i int := 1;
	y int := 1;
begin


drop table if exists temp_d;
create temp table temp_d 
as 
select *
from psv.t_relations_in_abl_buh as abl;
--select * from temp_d


drop table if exists temp_q;
create temp table temp_q(id serial primary key, query text);

insert into temp_q(query)
select 
--table_catalog, table_schema, table_name, column_name
--,
'comment on column ' || table_name || '.' || column_name || ' is ''' || string_agg(replace(rel_table_name, 'abl_buh.', '') || '.' || replace(rel_column_name, 'abl_buh.', ''), '; ') || ''';'
--select *
from temp_d
where rel_table_name is not null and rel_column_name is not null and table_name is not null and column_name is not null
group by table_catalog, table_schema, table_name, column_name;
--select * from temp_q

y = (select count(*) from temp_q);

<<L1>>
loop
	raise notice '%', i;
	select query 
	into q 
	from temp_q 
	where id = i;
	
	execute q;
	
	if i = y then
		exit L1;
	end if;
	i = i + 1;
end loop;

end 
$procedure$
;
