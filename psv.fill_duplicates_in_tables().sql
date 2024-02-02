-- DROP PROCEDURE psv.fill_duplicates_in_tables();

CREATE OR REPLACE PROCEDURE psv.fill_duplicates_in_tables()
 LANGUAGE plpgsql
AS $procedure$
begin

	drop table if exists temp_pk;
	create temp table temp_pk 
	as
	select table_schema , table_name 
	from information_schema.table_constraints as tc
	where tc.constraint_type <> 'PRIMARY KEY' and tc.table_schema in ('abl_buh', 'erp_buh_work', 'upp_work')
	;
	create index ci_pk on temp_pk(table_schema , table_name);
	--select * from temp_pk
	--=========================================================================================================================--
	--=========================================================================================================================--
	drop table if exists temp_t;
	create temp table temp_t 
	as
	with c1 as (select t.table_catalog
				, t.table_schema
				, t.table_name
				, t.table_type
				, t.table_schema || '.' || t.table_name as tbl_name
				, string_agg(c.column_name, ', ' order by c.column_name) as all_columns
				, replace(replace(string_agg(c.column_name, ', ' order by c.column_name), ', update_date', ''), 'update_date,', '') as all_columns_not_update_date
				from information_schema.tables as t
				join information_schema.columns as c using(table_schema, table_name)
				left join temp_pk as pk on pk.table_schema = t.table_schema and pk.table_name = t.table_name
				where 
					pk.table_name is null 
	--			and
	--				c.column_name <> 'update_date'
				and
					t.table_schema in ('abl_buh', 'erp_buh_work', 'upp_work')
				and 
					t.table_type = 'BASE TABLE'
				group by t.table_catalog
				, t.table_schema
				, t.table_name
				, t.table_type)
	select t.table_catalog
		 , t.table_schema
		 , t.table_name
		 , t.table_type
		 , t.all_columns as column_name
		 , ' select count(*) as cn' || ' from ' || t.tbl_name || ' ' as query
		 , ' select count(*) as cn from (select distinct ' || t.all_columns_not_update_date || ' from ' || t.tbl_name || ') as t ' as query_distinct
		 , ' drop table if exists temp_q3;' as q0
		 , ' create temp table temp_q3 as select ' || all_columns || ' from ' || t.tbl_name || ' where 1=0;' as q1
		 , ' insert into temp_q3 select ' || t.all_columns || ' from (' as q2
		 , ' select ' || t.all_columns || ', row_number() over (partition by ' || all_columns_not_update_date || ' order by update_date desc) as rn' 
		   || ' from ' || t.tbl_name || ') as t where rn=1;' as q3
		 , ' truncate ' || t.tbl_name || '; ' as q4
		 , ' insert into ' || t.tbl_name || '(' || t.all_columns || ') select ' || t.all_columns || ' from temp_q3; ' as q5
		 , ' drop table temp_q3; ' as q6
		 , row_number() over (order by table_name) as id
	from c1 as t
	;
	create index ci_t on temp_t(id);
	--select *, q2 || q3 || q4 || q5 || q6 from temp_t where id=2
	--=========================================================================================================================--
	--=========================================================================================================================--
	--drop table if exists temp_tbl;
	--create temp table temp_tbl(table_name varchar, rows_all varchar, rows_distinct varchar); --select * from temp_tbl
	--=========================================================================================================================--
	do $p2$
	declare
		i int := 1;
		ii int := (select count(*) from temp_t);
		qwr text;
		qwr_d text;
		qq0 text;
		qq1 text;
		qq3 text;
		r1 varchar;
		r2 varchar;
		tbl varchar;
	begin
		loop
			select query, query_distinct, q0, q1, q2 || q3 || q4 || q5 || q6, table_schema || '.' || table_name into qwr, qwr_d, qq0, qq1, qq3, tbl from temp_t where id = i;
			--select query_distinct into qq2 from temp_t where id = i;
			--==========================================================--
			execute (qwr) into r1;
			execute (qwr_d) into r2;
			--==========================================================--
			if r1::int <> r2::int then
				--insert into temp_tbl select tbl, r1, r2;
				insert into psv.log_deleting_duplicates select tbl, r1::int, r2::int, r1::int-r2::int, now();
				execute (qq0);
				execute (qq1);
				execute (qq3);
			end if;
			--==========================================================--
			if i = ii then
				exit;
			end if;
			--==========================================================--
			i = i + 1;
		end loop;
	end
	$p2$
	;

end
$procedure$
;
