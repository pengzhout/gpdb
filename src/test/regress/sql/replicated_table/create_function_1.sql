set search_path to rpt_test;

--
-- CREATE_FUNCTION_1
--

CREATE FUNCTION widget_in(cstring)
   RETURNS widget
   AS '/home/gpadmin/gpdb.master/lib/postgresql/regress.so'
   LANGUAGE C IMMUTABLE STRICT NO SQL;

CREATE FUNCTION widget_out(widget)
   RETURNS cstring
   AS '/home/gpadmin/gpdb.master/lib/postgresql/regress.so'
   LANGUAGE C IMMUTABLE STRICT NO SQL;

CREATE FUNCTION int44in(cstring)
   RETURNS city_budget
   AS '/home/gpadmin/gpdb.master/lib/postgresql/regress.so'
   LANGUAGE C IMMUTABLE STRICT NO SQL;

CREATE FUNCTION int44out(city_budget)
   RETURNS cstring
   AS '/home/gpadmin/gpdb.master/lib/postgresql/regress.so'
   LANGUAGE C IMMUTABLE STRICT NO SQL;

CREATE FUNCTION check_primary_key ()
	RETURNS trigger
	AS '/home/gpadmin/gpdb.master/lib/postgresql/regress.so'
	LANGUAGE C READS SQL DATA;

CREATE FUNCTION check_foreign_key ()
	RETURNS trigger
	AS '/home/gpadmin/gpdb.master/lib/postgresql/regress.so'
	LANGUAGE C READS SQL DATA;

CREATE FUNCTION autoinc ()
	RETURNS trigger
	AS '/home/gpadmin/gpdb.master/lib/postgresql/regress.so'
	LANGUAGE C READS SQL DATA;

CREATE FUNCTION funny_dup17 ()
        RETURNS trigger
        AS '/home/gpadmin/gpdb.master/lib/postgresql/regress.so'
        LANGUAGE C READS SQL DATA;

CREATE FUNCTION ttdummy ()
        RETURNS trigger
        AS '/home/gpadmin/gpdb.master/lib/postgresql/regress.so'
        LANGUAGE C READS SQL DATA;

CREATE FUNCTION set_ttdummy (int4)
        RETURNS int4
        AS '/home/gpadmin/gpdb.master/lib/postgresql/regress.so'
        LANGUAGE C STRICT READS SQL DATA;

CREATE FUNCTION test_atomic_ops()
    RETURNS bool
    AS '/home/gpadmin/gpdb.master/lib/postgresql/regress.so'
    LANGUAGE C IMMUTABLE STRICT NO SQL;

-- Things that shouldn't work:

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE SQL CONTAINS SQL
    AS 'SELECT ''not an integer'';';

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE SQL CONTAINS SQL
    AS 'not even SQL';

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE SQL CONTAINS SQL
    AS 'SELECT 1, 2, 3;';

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE SQL CONTAINS SQL
    AS 'SELECT $2;';

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE SQL CONTAINS SQL
    AS 'a', 'b';

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE C NO SQL
    AS 'nosuchfile';

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE C NO SQL
    AS '/home/gpadmin/gpdb.master/lib/postgresql/regress.so', 'nosuchsymbol';

CREATE FUNCTION test1 (int) RETURNS int LANGUAGE internal NO SQL
    AS 'nosuch';
