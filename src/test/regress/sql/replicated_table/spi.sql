set search_path to rpt_test;
drop table if exists test cascade;
drop table if exists test1 cascade;
create table test (d integer, a integer, b integer) distributed fully;
insert into test select a, a, a%25 from generate_series(1,100) a;

create table test1 (d integer, a integer, b integer);
insert into test1 select a, a, a%25 from generate_series(1,100) a;

-- function only access replicated table
create or replace function f1() returns bigint as $$
	select count(*) from test as results;
$$ LANGUAGE SQL;

-- function access replicated table and partition table
create or replace function f2() returns bigint as $$
	select count(*) from test, test1 as results;
$$ LANGUAGE SQL;

select f1() from gp_dist_random('gp_id');
select f2() from gp_dist_random('gp_id');
