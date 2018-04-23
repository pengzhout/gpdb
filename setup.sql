drop table test;
create table test (c1 int, c2 int, c3 int) distributed by (c1);

set allow_system_table_mods to 'DML';

drop function segment (integer);
CREATE FUNCTION segment(hashvalue integer) RETURNS integer
 AS $$
  DECLARE
	  ret integer;
  BEGIN
--      ret := 0;
--	  if hashvalue > 10 then
--		ret = 0;
--	  else
--		ret = 1;
--	  end if;
--      RETURN ret;
      RETURN hashvalue % 3;
  END;
$$ LANGUAGE plpgsql STABLE;

update gp_distribution_policy set distfunc = pg_proc.oid from pg_class, pg_proc where gp_distribution_policy.localoid = pg_class.oid and pg_class.relname = 'test' and proname = 'segment';
update gp_distribution_policy set regionid = 1 from pg_class where gp_distribution_policy.localoid = pg_class.oid and pg_class.relname = 'test';
update gp_distribution_policy set regionname = 'default' from pg_class where gp_distribution_policy.localoid = pg_class.oid and pg_class.relname = 'test';

drop table test1;
create table test1 (c1 int, c2 int, c3 int) distributed by (c1);

set allow_system_table_mods to 'DML';

update gp_distribution_policy set distfunc = pg_proc.oid from pg_class, pg_proc where gp_distribution_policy.localoid = pg_class.oid and pg_class.relname = 'test1' and proname = 'segment';
update gp_distribution_policy set regionid = 1 from pg_class where gp_distribution_policy.localoid = pg_class.oid and pg_class.relname = 'test1';
update gp_distribution_policy set regionname = 'default' from pg_class where gp_distribution_policy.localoid = pg_class.oid and pg_class.relname = 'test1';

drop table abc;
create table abc (c1 int, c2 int, c3 int) distributed by (c1);

drop function segment1 (integer);
CREATE FUNCTION segment1(hashvalue integer) RETURNS integer
 AS $$
  BEGIN
      RETURN hashvalue % 3;
  END;
$$ LANGUAGE plpgsql STABLE;


set allow_system_table_mods to 'DML';

update gp_distribution_policy set distfunc = pg_proc.oid from pg_class, pg_proc where gp_distribution_policy.localoid = pg_class.oid and pg_class.relname = 'abc' and proname = 'segment1';
update gp_distribution_policy set regionid = 1 from pg_class where gp_distribution_policy.localoid = pg_class.oid and pg_class.relname = 'abc';
update gp_distribution_policy set regionname = 'default' from pg_class where gp_distribution_policy.localoid = pg_class.oid and pg_class.relname = 'abc';
