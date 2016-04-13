#include "postgres.h"
#include "funcapi.h"
#include "tablefuncapi.h"

#include <float.h>
#include <math.h>
#include <stdlib.h>

#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_language.h"
#include "catalog/pg_type.h"
#include "cdb/memquota.h"
#include "commands/sequence.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "port/atomics.h"
#include "parser/parse_expr.h"
#include "libpq/auth.h"
#include "libpq/hba.h"
#include "utils/builtins.h"
#include "utils/geo_decls.h"
#include "utils/lsyscache.h"
#include "utils/resscheduler.h"

extern Datum slice_sleep(PG_FUNCTION_ARGS);
extern Datum slice_error(PG_FUNCTION_ARGS);
extern Datum slice_fatal(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(slice_sleep);
Datum
slice_sleep(PG_FUNCTION_ARGS)
{
	int32 time = PG_GETARG_INT32(0);
	pg_usleep(1000000*time);
}


PG_FUNCTION_INFO_V1(slice_error);
Datum
slice_error(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("QE quit error")));
}

PG_FUNCTION_INFO_V1(slice_fatal);
Datum
slice_fatal(PG_FUNCTION_ARGS)
{
	ereport(FATAL, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("QE quit fatal")));
}

