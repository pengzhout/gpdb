-- Tests FTS can handle DNS error.
create extension if not exists gp_inject_fault;

-- to make test deterministic and fast
!\retcode gpconfig -c gp_fts_probe_retries -v 2 --masteronly;

-- Allow extra time for mirror promotion to complete recovery to avoid
-- gprecoverseg BEGIN failures due to gang creation failure as some primaries
-- are not up. Setting these increase the number of retries in gang creation in
-- case segment is in recovery. Approximately we want to wait 30 seconds.
!\retcode gpconfig -c gp_gang_creation_retry_count -v 127 --skipvalidation --masteronly;
!\retcode gpconfig -c gp_gang_creation_retry_timer -v 250 --skipvalidation --masteronly;
!\retcode gpstop -u;

1:BEGIN;
1:END;
2:BEGIN;
3:BEGIN;
3:CREATE TEMP TABLE xxxx3 (c1 int, c2 int);
3:DECLARE c1 CURSOR for select * from xxxx3;
4:CREATE TEMP TABLE xxxx4 (c1 int, c2 int);
5:BEGIN;
5:CREATE TEMP TABLE xxxx5 (c1 int, c2 int);
5:SAVEPOINT s1;
5:CREATE TEMP TABLE xxxx51 (c1 int, c2 int);

-- trigger a fts to mark a segment to down
SELECT gp_inject_fault_infinite('fts_handle_message', 'error', dbid)
  FROM gp_segment_configuration WHERE content = 0 AND role = 'p';
-- do fts probe request twice to guarantee the fault is triggered
SELECT gp_request_fts_probe_scan();
SELECT gp_request_fts_probe_scan();

SELECT gp_inject_fault_infinite('fts_handle_message', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 0 AND role = 'p';
-- verify a segment is down
select count(*) from gp_segment_configuration where status = 'd';
select pg_sleep(5);
-- session 1: in no transaction and no temp table created, it's safe to
--            update cdb_component_dbs and use the new promoted primary 
1:BEGIN;
1:END;
-- session 2: in transaction, gxid is dispatched to writer gang, cann't
--            update cdb_component_dbs, following query should fail
2:END;
-- session 3: in transaction and has a cursor, cann't update
--            cdb_component_dbs, following query should fail 
3:FETCH ALL FROM c1;
3:END;
-- session 4: not in transaction but has temp table, cann't update
--            cdb_component_dbs, following query should fail and session
--            is reset 
4:select * from xxxx4;
4:select * from xxxx4;
-- session 5: has a subtransaction, cann't update cdb_component_dbs,
--            following query should fail 
5:select * from xxxx51;
5:ROLLBACK TO SAVEPOINT s1;
5:END;
1q:
2q:
3q:
4q:
5q:

select pg_sleep(2);
select now();
-- fully recover the failed primary as new mirror
!\retcode gprecoverseg -aF;

select now();
-- loop while segments come in sync
do $$
begin /* in func */
  for i in 1..120 loop /* in func */
    if (select mode = 's' from gp_segment_configuration where content = 0 limit 1) then /* in func */
      return; /* in func */
    end if; /* in func */
    perform gp_request_fts_probe_scan(); /* in func */
  end loop; /* in func */
end; /* in func */
$$;

!\retcode gprecoverseg -ar;

-- loop while segments come in sync
do $$
begin /* in func */
  for i in 1..120 loop /* in func */
    if (select mode = 's' from gp_segment_configuration where content = 0 limit 1) then /* in func */
      return; /* in func */
    end if; /* in func */
    perform gp_request_fts_probe_scan(); /* in func */
  end loop; /* in func */
end; /* in func */
$$;

-- verify no segment is down after recovery
select count(*) from gp_segment_configuration where status = 'd';

!\retcode gpconfig -r gp_fts_probe_retries --masteronly;
!\retcode gpconfig -r gp_gang_creation_retry_count --skipvalidation --masteronly;
!\retcode gpconfig -r gp_gang_creation_retry_timer --skipvalidation --masteronly;
!\retcode gpstop -u;


