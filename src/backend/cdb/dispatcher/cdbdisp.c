
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
#include <limits.h>

#include "storage/ipc.h"		/* For proc_exit_inprogress */
#include "tcop/tcopprot.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdisp_thread.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbfts.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbsreh.h"
#include "cdb/cdbvars.h"

/*
 * default directed-dispatch parameters: don't direct anything.
 */
CdbDispatchDirectDesc default_dispatch_direct_desc = { false, 0, {0}};

static void cdbdisp_clearGangActiveFlag(CdbDispatcherState * ds);

/*
 * cdbdisp_dispatchToGang:
 * Send the strCommand SQL statement to the subset of all segdbs in the cluster
 * specified by the gang parameter.  cancelOnError indicates whether an error
 * occurring on one of the qExec segdbs should cause all still-executing commands to cancel
 * on other qExecs. Normally this would be true.  The commands are sent over the libpq
 * connections that were established during cdblink_setup.	They are run inside of threads.
 * The number of segdbs handled by any one thread is determined by the
 * guc variable gp_connections_per_thread.
 *
 * The caller must provide a CdbDispatchResults object having available
 * resultArray slots sufficient for the number of QEs to be dispatched:
 * i.e., resultCapacity - resultCount >= gp->size.	This function will
 * assign one resultArray slot per QE of the Gang, paralleling the Gang's
 * db_descriptors array.  Success or failure of each QE will be noted in
 * the QE's CdbDispatchResult entry; but before examining the results, the
 * caller must wait for execution to end by calling CdbCheckDispatchResult().
 *
 * The CdbDispatchResults object owns some malloc'ed storage, so the caller
 * must make certain to free it by calling cdbdisp_destroyDispatcherState().
 *
 * When dispatchResults->cancelOnError is false, strCommand is to be
 * dispatched to every connected gang member if possible, despite any
 * cancellation requests, QE errors, connection failures, etc.
 *
 * NB: This function should return normally even if there is an error.
 * It should not longjmp out via elog(ERROR, ...), ereport(ERROR, ...),
 * PG_THROW, CHECK_FOR_INTERRUPTS, etc.
 */
void
cdbdisp_dispatchToGang(struct CdbDispatcherState *ds,
					   struct Gang *gp,
					   int sliceIndex,
					   CdbDispatchDirectDesc * disp_direct)
{
	struct CdbDispatchResults *dispatchResults = ds->primaryResults;

	Assert(Gp_role == GP_ROLE_DISPATCH);
	Assert(gp && gp->size > 0);
	Assert(dispatchResults && dispatchResults->resultArray);

	if (dispatchResults->writer_gang)
	{
		/*
		 * Are we dispatching to the writer-gang when it is already busy ?
		 */
		if (gp == dispatchResults->writer_gang)
		{
			if (dispatchResults->writer_gang->dispatcherActive)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("query plan with multiple segworker groups is not supported"),
						 errhint("likely caused by a function that reads or modifies data in a distributed table")));
			}

			dispatchResults->writer_gang->dispatcherActive = true;
		}
	}

	/*
	 * WIP: will use a function pointer for implementation later, currently just use an internal function to move dispatch
	 * thread related code into a separate file.
	 */
	cdbdisp_dispatchToGang_internal(ds, gp, sliceIndex, disp_direct);
}

/*
 * CdbCheckDispatchResult:
 *
 * Waits for completion of threads launched by cdbdisp_dispatchToGang().
 *
 * QEs that were dispatched with 'cancelOnError' true and are not yet idle
 * will be canceled/finished according to waitMode.
 */
void
CdbCheckDispatchResult(struct CdbDispatcherState *ds,
					   DispatchWaitMode waitMode)
{
	PG_TRY();
	{
		CdbCheckDispatchResult_internal(ds, NULL, NULL, waitMode);
	}
	PG_CATCH();
	{
		cdbdisp_clearGangActiveFlag(ds);
		PG_RE_THROW();
	}
	PG_END_TRY();

	cdbdisp_clearGangActiveFlag(ds);

	if (log_dispatch_stats)
		ShowUsage("DISPATCH STATISTICS");

	if (DEBUG1 >= log_min_messages)
	{
		char msec_str[32];

		switch (check_log_duration(msec_str, false))
		{
			case 1:
			case 2:
				ereport(LOG,
						(errmsg("duration to dispatch result received from all QEs: %s ms", msec_str)));
				break;
		}
	}
}

/*
 * cdbdisp_getDispatchResults:
 *
 * Block until all QEs return results or report errors.
 *
 * Return Values:
 *   Return NULL If one or more QEs got Error in which case errorMsg contain
 *   QE error messages.
 */
struct CdbDispatchResults *
cdbdisp_getDispatchResults(struct CdbDispatcherState *ds, StringInfoData **errorMsg)
{
	int errorcode;
	StringInfoData *errorMsgBuf = NULL;

	if (!ds || !ds->primaryResults)
		return NULL;

	/* wait QEs to return results or report errors*/
	CdbCheckDispatchResult(ds, DISPATCH_WAIT_CANCEL);

	/* check if any error reported */
	errorcode = ds->primaryResults->errcode;

	if (errorcode)
	{
		errorMsgBuf = palloc0(sizeof(StringInfoData));
		initStringInfo(errorMsgBuf);
		cdbdisp_dumpDispatchResults(ds->primaryResults, errorMsgBuf, false);
		*errorMsg = errorMsgBuf;
		return NULL;
	}

	*errorMsg = NULL;

	return ds->primaryResults;
}

/*
 * Create and initialize CdbDispatcherState.
 *
 * Call cdbdisp_destroyDispatcherState to free it.
 *
 *	 maxResults: max number of results, normally equals to max number of QEs.
 *	 maxSlices: max number of slices of the query/command.
 */
CdbDispatcherState *
cdbdisp_createDispatcherState(int maxResults,
							int maxSlices, bool cancelOnError)
{
	MemoryContext oldContext = NULL;

	CdbDispatcherState * ds = palloc0(sizeof(CdbDispatcherState));

	ds->dispatchStateContext = AllocSetContextCreate(TopMemoryContext,
													 "Dispatch Context",
													 ALLOCSET_DEFAULT_MINSIZE,
													 ALLOCSET_DEFAULT_INITSIZE,
													 ALLOCSET_DEFAULT_MAXSIZE);

	oldContext = MemoryContextSwitchTo(ds->dispatchStateContext);
	ds->primaryResults = cdbdisp_makeDispatchResults(maxResults,
													 maxSlices,
													 cancelOnError);

	ds->dispatchThreads = cdbdisp_makeDispatchThreads(maxSlices);
	MemoryContextSwitchTo(oldContext);

	return ds;
}

/*
 * Free memory in CdbDispatcherState
 *
 * Free the PQExpBufferData allocated in libpq.
 * Free dispatcher memory context.
 */
void
cdbdisp_destroyDispatcherState(CdbDispatcherState * ds)
{
	if (!ds)
		return;

	CdbDispatchResults *results = ds->primaryResults;

	if (results != NULL && results->resultArray != NULL)
	{
		int i;

		for (i = 0; i < results->resultCount; i++)
		{
			cdbdisp_termResult(&results->resultArray[i]);
		}
		results->resultArray = NULL;
	}

	if (ds->dispatchStateContext != NULL)
	{
		MemoryContextDelete(ds->dispatchStateContext);
		ds->dispatchStateContext = NULL;
	}

	ds->dispatchStateContext = NULL;
	ds->dispatchThreads = NULL;
	ds->primaryResults = NULL;
}

void cdbdisp_cancelDispatch(CdbDispatcherState *ds)
{
	CdbCheckDispatchResult(ds, DISPATCH_WAIT_CANCEL);	
}

void cdbdisp_finishDispatch(CdbDispatcherState *ds)
{
	CdbCheckDispatchResult(ds, DISPATCH_WAIT_FINISH);	
}

/*
 * Clear our "active" flags; so that we know that the writer gangs are busy -- and don't stomp on
 * internal dispatcher structures.
 */
static void
cdbdisp_clearGangActiveFlag(CdbDispatcherState * ds)
{
	if (ds && ds->primaryResults && ds->primaryResults->writer_gang)
	{
		ds->primaryResults->writer_gang->dispatcherActive = false;
	}
}
