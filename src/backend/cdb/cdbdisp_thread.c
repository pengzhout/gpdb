/*-------------------------------------------------------------------------
 *
 * cdbdisp.c
 *	  Functions to dispatch commands to QExecutors.
 *
 *
 * Copyright (c) 2005-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */

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
#include "executor/execdesc.h"	/* QueryDesc */
#include "storage/ipc.h"		/* For proc_exit_inprogress  */
#include "miscadmin.h"
#include "utils/memutils.h"

#include "utils/tqual.h" 			/*for the snapshot */
#include "storage/proc.h"  			/* MyProc */
#include "storage/procarray.h"      /* updateSharedLocalSnapshot */
#include "access/xact.h"  			/*for GetCurrentTransactionId */


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

#define DISPATCH_WAIT_TIMEOUT_SEC 2
extern bool Test_print_direct_dispatch_info;

extern pthread_t main_tid;
#ifndef _WIN32
#define mythread() ((unsigned long) pthread_self())
#else
#define mythread() ((unsigned long) pthread_self().p)
#endif 

/*
 * default directed-dispatch parameters: don't direct anything.
 */
CdbDispatchDirectDesc default_dispatch_direct_desc = {false, 0, {0}};

/*
 * Static Helper functions
 */
							
void cdbdisp_dispatchToGang_V1(struct Gang *gp, const char* query, CdbDispatchDirectDesc *disp_direct)
int getDispatchThreadCount(Gang *gp, CdbDispatchDirectDesc *disp_direct);

/*
 * Counter to indicate there are some dispatch threads running.  This will
 * be incremented at the beginning of dispatch threads and decremented at
 * the end of them.
 */
static volatile int32 RunningThreadCount = 0;
static int* ThreadHandles = NULL;

int getDispatchThreadCount(Gang *gp, CdbDispatchDirectDesc *disp_direct)
{
	if (disp_direct && disp_direct->directed_dispatch)
		return 1;
	else if (gp_connections_per_thread == 0)
		return 0;
	else
		return 1 + gp->size / gp_connections_per_thread;
}

void cdbdisp_dispatchToGang_V1(struct Gang *gp, const char* query, CdbDispatchDirectDesc *disp_direct)
{

	if (gp->writer_gang && gp->writer_gang->dispatcherActive)
	{
		/* Are we dispatching to the writer-gang when it is already busy ? */
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("query plan with multiple segworker groups is not supported"),
						 errhint("likely caused by a function that reads or modifies data in a distributed table")));
			}

	}

	gp->CurrentQuery = query;

	//bool result = gp->dispatchQuery(gp, newQueryText, disp_direct);
	//bool result = async_dispatchToGang(gp, disp_direct);
	bool result = thread_dispatchToGang(gp, disp_direct);

	if (result == false)
	{
		ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("query plan with multiple segworker groups is not supported"),
						 errhint("likely caused by a function that reads or modifies data in a distributed table")));
	}
}	/* cdbdisp_dispatchToGang */

/*
 *	Thread way to dispatch gang
 *
 **/
bool thread_dispatchToGang(struct Gang *gp, CdbDispatchDirectDesc *disp_direct)
{
	int	i, ThreadNeeded = 0;	
	/*
	 * Compute the thread count based on how many segdbs were added into the
	 * thread pool, knowing that each thread handles gp_connections_per_thread
	 * segdbs.
	 */
	ThreadNeeded = getDispatchThreadCount(gp, disp_direct);	

	/*
	 * Alloc memorys for thread handles
	 *
	 **/
	ThreadHandles = malloc(ThreadNeeded * sizeof(int));

	/*
	 * start thread to dispatch Query
	 */
	if (ThreadNeeded == 0)
	{
		//thread_DispatchOut(pParms);
		ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("ThreadHandles is zero"),
						 errhint("thread needed is zero")));
	}
	else
	{
		for (i = 0; i < ThreadNeeded; i++)
		{
			int pthread_err = 0;
			//pthread_err = gp_thread_create(&pParms[i]->thread, gp->thread_dispatch_internal, pParms[i], "dispatchToGang");
			pthread_err = gp_thread_create(&ThreadHandles[i], thread_dispatch_internal, (void*)gp, "dispatchToGang");
			if (pthread_err != 0)
			{
				ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("could not create thread %d of %d", i + 1, ThreadNeeded),
								errdetail("pthread_create() failed with err %d", pthread_err)));
			}
		}
	}
}	/* cdbdisp_dispatchToGang */

/*
 * thread_DispatchCommand is the thread proc used to dispatch the command to one or more of the qExecs.
 *
 * NOTE: This function MUST NOT contain elog or ereport statements. (or most any other backend code)
 *		 elog is NOT thread-safe.  Developers should instead use something like:
 *
 *	if (DEBUG3 >= log_min_messages)
 *			write_log("my brilliant log statement here.");
 *
 * NOTE: In threads, we cannot use palloc, because it's not thread safe.
 */
void thread_dispatch_internal(void* arg)
{

}
