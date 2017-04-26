-- start_ignore
0: drop role r1;
0: drop role r2;
0: drop resource group g1;
0: drop resource group g2;
0: drop function round_percentage(text);
0: drop view cpu_status;
0: drop view busy;
0: drop table bigtable;
-- end_ignore

--
-- create helper functions and views
--

create table bigtable as select i as c1, 'abc' as c2 from generate_series(1,100000) i;

-- the cpu usage limitation has an error rate about 10%,
-- and to satisfy the 0.1:0.4 rate we round the cpu rate to 9%
create function round_percentage(text) returns text as $$
    select (round(rtrim($1, '%') :: double precision / 9) * 9) :: text || '%'
$$ language sql;

create view cpu_status as
    select g.rsgname, round_percentage(s.cpu_usage)
    from gp_toolkit.gp_resgroup_status s, pg_resgroup g
    where s.groupid=g.oid
    order by g.oid;

create view busy as
    select count(*)
    from
    bigtable t1,
    bigtable t2,
    bigtable t3,
    bigtable t4,
    bigtable t5
    where 0 = (t1.c1 + 10000)!
      and 0 = (t2.c1 + 10000)!
      and 0 = (t3.c1 + 10000)!
      and 0 = (t4.c1 + 10000)!
      and 0 = (t5.c1 + 10000)!
    ;

--
-- check gpdb cgroup configuration
-- cfs_quota_us := cfs_period_us * ncores * gp_resource_group_cpu_limit
-- shares := 1024 * ncores
--

! python -c "print $(cat /sys/fs/cgroup/cpu/gpdb/cpu.cfs_quota_us) == $(cat /sys/fs/cgroup/cpu/gpdb/cpu.cfs_period_us) * $(nproc) * $(psql -d isolation2resgrouptest -Aqtc "show gp_resource_group_cpu_limit;")";

! python -c "print $(cat /sys/fs/cgroup/cpu/gpdb/cpu.shares) == 1024 * $(nproc)";

--
-- check default groups configuration
-- SUB/shares := TOP/shares * cpu_rate_limit
--

! python -c "print $(cat /sys/fs/cgroup/cpu/gpdb/$(psql -d isolation2resgrouptest -Aqtc "select oid from pg_resgroup where rsgname='default_group';")/cpu.shares) == $(cat /sys/fs/cgroup/cpu/gpdb/cpu.shares) * $(psql -d isolation2resgrouptest -Aqtc "select value from pg_resgroupcapability c, pg_resgroup g where c.resgroupid=g.oid and reslimittype=2 and g.rsgname='default_group'")";

! python -c "print $(cat /sys/fs/cgroup/cpu/gpdb/$(psql -d isolation2resgrouptest -Aqtc "select oid from pg_resgroup where rsgname='admin_group';")/cpu.shares) == $(cat /sys/fs/cgroup/cpu/gpdb/cpu.shares) * $(psql -d isolation2resgrouptest -Aqtc "select value from pg_resgroupcapability c, pg_resgroup g where c.resgroupid=g.oid and reslimittype=2 and g.rsgname='admin_group'")";

0: select * from cpu_status;

0: create resource group g1 with (cpu_rate_limit=0.1, memory_limit=0.1);
0: create resource group g2 with (cpu_rate_limit=0.4, memory_limit=0.4);

-- check g1 configuration
! python -c "print $(cat /sys/fs/cgroup/cpu/gpdb/$(psql -d isolation2resgrouptest -Aqtc "select oid from pg_resgroup where rsgname='g1';")/cpu.shares) == $(cat /sys/fs/cgroup/cpu/gpdb/cpu.shares) * 0.1";

-- check g2 configuration
! python -c "print $(cat /sys/fs/cgroup/cpu/gpdb/$(psql -d isolation2resgrouptest -Aqtc "select oid from pg_resgroup where rsgname='g2';")/cpu.shares) == $(cat /sys/fs/cgroup/cpu/gpdb/cpu.shares) * 0.4";

0: create role r1 resource group g1;
0: create role r2 resource group g2;
0: grant all on busy to r1;
0: grant all on busy to r2;

10: set role to r1;
11: set role to r1;
12: set role to r1;
13: set role to r1;
14: set role to r1;

20: set role to r2;
21: set role to r2;
22: set role to r2;
23: set role to r2;
24: set role to r2;

0: select * from cpu_status;

10&: select * from busy;
11&: select * from busy;
12&: select * from busy;
13&: select * from busy;
14&: select * from busy;

20&: select * from busy;
21&: select * from busy;
22&: select * from busy;
23&: select * from busy;
24&: select * from busy;

0: select pg_sleep(10);
0: select * from cpu_status;
0: select pg_sleep(1);
0: select * from cpu_status;
0: select pg_sleep(1);
0: select * from cpu_status;
0: select pg_sleep(1);
0: select * from cpu_status;
0: select pg_sleep(1);
0: select * from cpu_status;

-- start_ignore
0: select pg_cancel_backend(procpid) from pg_stat_activity where current_query like 'select * from busy%';

10<:
11<:
12<:
13<:
14<:
20<:
21<:
22<:
23<:
24<:
-- end_ignore
