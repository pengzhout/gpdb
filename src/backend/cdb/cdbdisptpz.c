#include "cdb/cdbdisptpz.h"
#include "postgres.h"
#include <pthread.h>
#include <limits.h>
#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif

#include "catalog/catquery.h"
#include "executor/execdesc.h"  /* QueryDesc */
#include "storage/ipc.h"                /* For proc_exit_inprogress  */
#include "miscadmin.h"
#include "utils/memutils.h"

#include "utils/tqual.h"                        /*for the snapshot */
#include "storage/proc.h"                       /* MyProc */
#include "storage/procarray.h"      /* updateSharedLocalSnapshot */
#include "access/xact.h"                        /*for GetCurrentTransactionId */


#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "utils/datum.h"
#include "utils/guc.h"
#include "executor/executor.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "tcop/tcopprot.h"
#include "cdb/cdbplan.h"
#include "postmaster/syslogger.h"
#include "cdb/cdbselect.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbfts.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbsrlz.h"
#include "cdb/cdbsubplan.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbdtxcontextinfo.h"
#include "cdb/cdbllize.h"
#include "cdb/cdbsreh.h"
#include "cdb/cdbrelsize.h"
#include "gp-libpq-fe.h"
#include "libpq/libpq-be.h"
#include "cdb/cdbutil.h"

#include "parser/parsetree.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "utils/gp_atomic.h"
#include "utils/builtins.h"
#include "utils/portal.h"

int getGangGroup(struct SliceTable*);

List OldGangGroupID = NULL;
List CurrentGangGroupID = NULL;

CdbDispatcher CurrentDispatcher = {
		"dispatcher test",
		getGangGroup
};

CdbDispatcher* GetCurrentDispatcher(void)
{
	return &CurrentDispatcher;
}

int getGangGroup(struct SliceTable* table)
{
	Slice	*slice;

	foreach(slice, table->slices) 	
	{	
				
	}

	return 1;
}
