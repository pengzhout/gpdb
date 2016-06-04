/*-------------------------------------------------------------------------
 *
 * cdbdisp.h
 * routines for dispatching commands from the dispatcher process
 * to the qExec processes.
 *
 * Copyright (c) 2005-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBDISP_H
#define CDBDISP_H

#include "lib/stringinfo.h" /* StringInfo */

#include "cdb/cdbtm.h"

#define CDB_MOTION_LOST_CONTACT_STRING "Interconnect error master lost contact with segment."

struct CdbDispatchResults; /* #include "cdb/cdbdispatchresult.h" */
struct Gang; /* #include "cdb/cdbgang.h" */

/*
 * Types of message to QE when we wait for it.
 */
typedef enum DispatchWaitMode
{
	DISPATCH_WAIT_NONE = 0,			/* wait until QE fully completes */
	DISPATCH_WAIT_FINISH,			/* send query finish */
	DISPATCH_WAIT_CANCEL			/* send query cancel */
} DispatchWaitMode;

typedef struct CdbDispatchDirectDesc
{
	bool directed_dispatch;
	uint16 count;
	uint16 content[1];
} CdbDispatchDirectDesc;

extern CdbDispatchDirectDesc default_dispatch_direct_desc;
#define DEFAULT_DISP_DIRECT (&default_dispatch_direct_desc)

typedef struct CdbDispatcherState
{
	struct CdbDispatchResults *primaryResults;
	struct CdbDispatchCmdThreads *dispatchThreads;
	MemoryContext dispatchStateContext;
} CdbDispatcherState;

/*--------------------------------------------------------------------*/
/*
 * cdbdisp_dispatchToGang:
 * Send the strCommand SQL statement to the subset of all segdbs in the cluster
 * specified by the gang parameter. cancelOnError indicates whether an error
 * occurring on one of the qExec segdbs should cause all still-executing commands to cancel
 * on other qExecs. Normally this would be true. The commands are sent over the libpq
 * connections that were established during cdblink_setup. They are run inside of threads.
 * The number of segdbs handled by any one thread is determined by the
 * guc variable gp_connections_per_thread.
 *
 * The caller must provide a CdbDispatchResults object having available
 * resultArray slots sufficient for the number of QEs to be dispatched:
 * i.e., resultCapacity - resultCount >= gp->size. This function will
 * assign one resultArray slot per QE of the Gang, paralleling the Gang's
 * db_descriptors array. Success or failure of each QE will be noted in
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
					   CdbDispatchDirectDesc *direct);

/*
 * CdbCheckDispatchResult:
 *
 * Waits for completion of threads launched by cdbdisp_dispatchToGang().
 *
 * QEs that were dispatched with 'cancelOnError' true and are not yet idle
 * will be canceled/finished according to waitMode.
 */
void
CdbCheckDispatchResult(struct CdbDispatcherState *ds, DispatchWaitMode waitMode);

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
cdbdisp_getDispatchResults(struct CdbDispatcherState *ds, StringInfoData **errorMsg);

/*
 * Create and initialize CdbDispatcherState.
 *
 * Call cdbdisp_destroyDispatcherState to free it.
 *
 *   maxResults: max number of results, normally equals to max number of QEs.
 *   maxSlices: max number of slices of the query/command.
 */

CdbDispatcherState *
cdbdisp_createDispatcherState(int maxResults,
							int maxSlices,
							bool cancelOnError);

/*
 * Free memory in CdbDispatcherState
 *
 * Free the PQExpBufferData allocated in libpq.
 * Free dispatcher memory context.
 */
void cdbdisp_destroyDispatcherState(CdbDispatcherState *ds);

/*
 * Cancel all still runing QEs
 */
void cdbdisp_cancelDispatch(CdbDispatcherState *ds);

/*
 * gracefully finish still running QEs
 */
void cdbdisp_finishDispatch(CdbDispatcherState *ds);

#endif   /* CDBDISP_H */
