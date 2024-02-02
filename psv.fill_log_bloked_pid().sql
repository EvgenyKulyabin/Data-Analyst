-- DROP PROCEDURE psv.fill_log_bloked_pid();

CREATE OR REPLACE PROCEDURE psv.fill_log_bloked_pid()
 LANGUAGE plpgsql
AS $procedure$
begin

	drop table if exists temp_block;
	create temp table temp_block 
	as
	SELECT blocked_locks.pid     AS zablokirovannaya_sessiya,
	         blocked_activity.usename  AS zablokirovannyi_polzovatel,
	         blocking_locks.pid     AS blokiruuschaya_sessiya,
	         blocking_activity.usename AS blokiruuschii_polzovatel,
	         blocked_activity.query    AS zablokirovannyi_zapros,
	         blocking_activity.query   AS blokiruuschii_zapros,
	         now() as update_date,
	         ''::text as kommentarii
	FROM  pg_catalog.pg_locks         blocked_locks
	JOIN pg_catalog.pg_stat_activity blocked_activity  ON blocked_activity.pid = blocked_locks.pid
	JOIN pg_catalog.pg_locks blocking_locks 
	        ON blocking_locks.locktype = blocked_locks.locktype
	        AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
	        AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
	        AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
	        AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
	        AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
	        AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
	        AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
	        AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
	        AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
	        AND blocking_locks.pid != blocked_locks.pid
	JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
	WHERE NOT blocked_locks.GRANTED;
	--select * from temp_block;
	
	
--	drop table if exists psv.log_bloked_pid;
--	select *
--	into psv.log_bloked_pid
--	from temp_block;
	
	delete from psv.log_bloked_pid
	where update_date < date_trunc('month', now()) - '1 month'::interval;
	
	if (select count(*) from temp_block) = 0 then
		insert into psv.log_bloked_pid(update_date, kommentarii)
		select now(), 'блокировок нет';
	else
		insert into psv.log_bloked_pid
		select * from temp_block;
	end if;

end 
$procedure$
;
