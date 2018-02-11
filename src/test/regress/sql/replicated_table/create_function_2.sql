set search_path to rpt_test;

--
-- CREATE_FUNCTION_2
--
CREATE FUNCTION hobbies(person)
   RETURNS setof hobbies_r 
   AS 'select * from hobbies_r where person = $1.name'
   LANGUAGE SQL READS SQL DATA;


CREATE FUNCTION hobby_construct(text, text)
   RETURNS hobbies_r
   AS 'select $1 as name, $2 as hobby'
   LANGUAGE SQL READS SQL DATA;


CREATE FUNCTION hobbies_by_name(hobbies_r.name%TYPE)
   RETURNS hobbies_r.person%TYPE
   -- GPDB: use an order by to force the later test in 'misc' to return
   -- a particular person, when multiple persons have the same hobby.
   AS 'select person from hobbies_r where name = $1 order by person'
   LANGUAGE SQL READS SQL DATA;


CREATE FUNCTION equipment(hobbies_r)
   RETURNS setof equipment_r
   AS 'select * from equipment_r where hobby = $1.name'
   LANGUAGE SQL READS SQL DATA;


CREATE FUNCTION user_relns()
   RETURNS setof name
   AS 'select relname 
       from pg_class c, pg_namespace n
       where relnamespace = n.oid and
             (nspname !~ ''pg_.*'' and nspname <> ''information_schema'') and
             relkind <> ''i'' '
   LANGUAGE SQL READS SQL DATA;

CREATE FUNCTION pt_in_widget(point, widget)
   RETURNS bool
   AS '/home/gpadmin/gpdb.master/lib/postgresql/regress.so'
   LANGUAGE C IMMUTABLE NO SQL;

CREATE FUNCTION overpaid(emp)
   RETURNS bool
   AS '/home/gpadmin/gpdb.master/lib/postgresql/regress.so'
   LANGUAGE C READS SQL DATA;

CREATE FUNCTION boxarea(box)
   RETURNS float8
   AS '/home/gpadmin/gpdb.master/lib/postgresql/regress.so'
   LANGUAGE C READS SQL DATA;

CREATE FUNCTION interpt_pp(path, path)
   RETURNS point
   AS '/home/gpadmin/gpdb.master/lib/postgresql/regress.so'
   LANGUAGE C READS SQL DATA;

CREATE FUNCTION reverse_name(name)
   RETURNS name
   AS '/home/gpadmin/gpdb.master/lib/postgresql/regress.so'
   LANGUAGE C IMMUTABLE NO SQL;

CREATE FUNCTION oldstyle_length(int4, text)
   RETURNS int4
   AS '/home/gpadmin/gpdb.master/lib/postgresql/regress.so'
   LANGUAGE C READS SQL DATA;

--
-- Function dynamic loading
--
LOAD '/home/gpadmin/gpdb.master/lib/postgresql/regress.so';

-- Persistent table check
-- It's not the right place, but good fit to put misc UDFs.
CREATE FUNCTION checkRelationAfterInvalidation() RETURNS boolean
AS '/home/gpadmin/gpdb.master/lib/postgresql/regress.so', 'checkRelationAfterInvalidation' LANGUAGE C;

SELECT checkRelationAfterInvalidation();
