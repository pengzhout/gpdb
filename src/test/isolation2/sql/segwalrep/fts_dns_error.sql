-- Tests FTS can handle DNS error.
create extension if not exists gp_inject_fault;

-- skip FTS probes firstly
select gp_inject_fault_infinite('fts_probe', 'skip', 1);
select gp_request_fts_probe_scan();
select gp_wait_until_triggered_fault('fts_probe', 1, 1);

-- inject dns error
select gp_inject_fault_infinite('get_dns_cached_address', 'skip', 1);
select gp_inject_fault_infinite('before_fts_notify', 'suspend', 1);

-- trigger a DNS error in session 0, Before error out,
-- QD will notify the FTS to do a probe, fts probe is
-- currently skipped, so this query will get stuck 
0&: create table fts_dns_error_test (c1 int, c2 int);

select gp_wait_until_triggered_fault('before_fts_notify', 10, 1);

-- enable fts probe
select gp_inject_fault_infinite('fts_probe', 'reset', 1);

-- resume fts notify for session 0 to notify fts to probe
select gp_inject_fault_infinite('before_fts_notify', 'resume', 1);

-- wait until fts done the probe and this query will fail 
0<:

-- verify a fts failover happens
select count(*) from gp_segment_configuration where status = 'd';

select gp_inject_fault_infinite('get_dns_cached_address', 'reset', 1);
-- fts failover happens, so the following query will success.
0: create table fts_dns_error_test (c1 int, c2 int); 

-- fully recover the failed primary as new mirror
!\retcode gprecoverseg -aF;

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

!\retcode gprecoverseg -ar

-- loop while segments come in sync
do $$
begin
  for i in 1..120 loop
    if (select count(*) = 0 from gp_segment_configuration where content = 0 and mode != 's') then
      return;
    end if;
    perform gp_request_fts_probe_scan();
  end loop;
end;
$$;
