
/*-------------------------------------------------------------------------
 *
 * cdbdisp.c
 *	  Functions to dispatch commands to QExecutors.
 *
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/dispatcher/cdbdisp.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <limits.h>

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif

#include "storage/ipc.h"		/* For proc_exit_inprogress */
#include "tcop/tcopprot.h"
#include "cdb/cdbdisp_new.h"
#include "cdb/cdbdispatchresult.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "cdb/cdbfts.h"
#include "cdb/cdbdisp_gang.h"
#include "cdb/cdbsreh.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbpq.h"
#include "cdb/cdbpq.h"

DispatcherState * TopDispatcherState = NULL;
DispatcherState * TopTransactionDispatcherState = NULL;
DispatcherState * CurrentDispatcherState = NULL;

static void DispatcherState_ReplaceDummySliceIndex(char *queryText, int sliceIndex);
static void DispatcherState_DispatchCommandToGang(struct Gang *gang, char *formatQuery, int queryLen);
static CdbDispatchResult * DispatcherState_MakeResult(struct SegmentDatabaseDescriptor *segdbDesc, Gang *gang);

static void dispatchCommand(CdbDispatchResult *dispatchResult,
				const char *query_text,
				int query_text_len);

static void DispatcherState_CheckSegmentAlive(void);
static void DispatcherState_SignalQEs(void);
static void DispatcherState_HandlePollSuccess(struct pollfd *fds);
static void DispatcherState_HandlePollError(void);
static DispatcherWaitMode DispatcherState_GetWaitMode(void);
static void DispatcherState_AppendMessage(CdbDispatchResult *dispatchResult, int elevel, const char *fmt,...);
static bool processResults(CdbDispatchResult *dispatchResult);
static void DispatcherState_InventorySliceTree(SliceTable *sliceTbl, int sliceIndex, bool extendedQuery);
static Gang *DispatcherState_AllocateGang(GangType type, List *segments, bool extendedQuery);

static void noTrailingNewlinePQ(PQExpBuffer buf);
static void oneTrailingNewlinePQ(PQExpBuffer buf);

static int cdbdisp_snatchPGresults(CdbDispatchResult *dispatchResult, struct pg_result **pgresultptrs, int maxresults);

typedef struct
{
	int			sliceIndex;
	int			children;
	Slice	   *slice;
} SliceVec;

/*
 * Quick and dirty bit mask operations
 */
static void
mark_bit(char *bits, int nth)
{
	int			nthbyte = nth >> 3;
	char		nthbit = 1 << (nth & 7);

	bits[nthbyte] |= nthbit;
}

static void
or_bits(char *dest, char *src, int n)
{
	int			i;

	for (i = 0; i < n; i++)
		dest[i] |= src[i];
}

static int
count_bits(char *bits, int nbyte)
{
	int			i;
	int			nbit = 0;

	int			bitcount[] =
	{
		0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4
	};

	for (i = 0; i < nbyte; i++)
	{
		nbit += bitcount[bits[i] & 0x0F];
		nbit += bitcount[(bits[i] >> 4) & 0x0F];
	}

	return nbit;
}

/*
 * We use a bitmask to count the dep. childrens.
 * Because of input sharing, the slices now are DAG. We cannot simply go down the
 * tree and add up number of children, which will return too big number.
 */
static int
markbit_dep_children(SliceTable *sliceTable, int sliceIdx,
					 SliceVec *sliceVec, int bitmasklen, char *bits)
{
	ListCell   *sublist;
	Slice	   *slice = (Slice *) list_nth(sliceTable->slices, sliceIdx);

	foreach(sublist, slice->children)
	{
		int			childIndex = lfirst_int(sublist);
		char	   *newbits = palloc0(bitmasklen);

		markbit_dep_children(sliceTable, childIndex,
							 sliceVec, bitmasklen, newbits);
		or_bits(bits, newbits, bitmasklen);
		mark_bit(bits, childIndex);
		pfree(newbits);
	}

	sliceVec[sliceIdx].sliceIndex = sliceIdx;
	sliceVec[sliceIdx].children = count_bits(bits, bitmasklen);
	sliceVec[sliceIdx].slice = slice;

	return sliceVec[sliceIdx].children;
}

static int
count_dependent_children(SliceTable *sliceTable, int sliceIndex,
						 SliceVec *sliceVector, int len)
{
	int			ret = 0;
	int			bitmasklen = (len + 7) >> 3;
	char	   *bitmask = palloc0(bitmasklen);

	ret = markbit_dep_children(sliceTable, sliceIndex, sliceVector, bitmasklen, bitmask);
	pfree(bitmask);

	return ret;
}

static int
compare_slice_order(const void *aa, const void *bb)
{
	SliceVec   *a = (SliceVec *) aa;
	SliceVec   *b = (SliceVec *) bb;

	if (a->slice == NULL)
		return 1;
	if (b->slice == NULL)
		return -1;

	/*
	 * Put the slice not going to dispatch in the last
	 */
	if (a->slice->primaryGang == NULL)
	{
		Assert(a->slice->gangType == GANGTYPE_UNALLOCATED);
		return 1;
	}
	if (b->slice->primaryGang == NULL)
	{
		Assert(b->slice->gangType == GANGTYPE_UNALLOCATED);
		return -1;
	}

	/*
	 * sort the writer gang slice first, because he sets the shared snapshot
	 */
	if (a->slice->primaryGang->gang_id == 1)
	{
		Assert(b->slice->primaryGang->gang_id != 1);
		return -1;
	}
	if (b->slice->primaryGang->gang_id == 1)
	{
		return 1;
	}

	if (a->children == b->children)
		return 0;
	else if (a->children > b->children)
		return 1;
	else
		return -1;
}

static int
fillSliceVector(SliceTable *sliceTbl, int rootIdx,
				SliceVec *sliceVector, int nTotalSlices)
{
	int			top_count;

	/*
	 * count doesn't include top slice add 1, note that sliceVector would be
	 * modified in place by count_dependent_children.
	 */
	top_count = 1 + count_dependent_children(sliceTbl, rootIdx, sliceVector, nTotalSlices);

	qsort(sliceVector, nTotalSlices, sizeof(SliceVec), compare_slice_order);

	return top_count;
}

void
DispatcherState_Init(void)
{
	Assert(!TopDispatcherState);

	TopDispatcherState = DispatcherState_Create("TopDispatcherState");

	CurrentDispatcherState = TopDispatcherState;
}

struct DispatcherState *
DispatcherState_Create(const char* name)
{
	struct DispatcherState *ds = palloc0(sizeof(struct DispatcherState));

	ds->connected = false;
	ds->allocatedGangs = NIL;
	ds->results = NIL;
	ds->waitMode = DISPATCHER_WAIT_BLOCK;
	ds->errcode = 0;
	ds->cancelOnError = false;
	/* Initialize DispatcherState */

	return ds;
}

void
DispatcherState_Connect(void)
{
	Assert(CurrentDispatcherState);

	if (CurrentDispatcherState->connected)
		elog(FATAL, "Dispatcher connected");

	//DispatcherState_Reset();
	CurrentDispatcherState->connected = true;
}

static Gang *
DispatcherState_AllocateGang(GangType type, List *segments, bool extendedQuery)
{
	Gang	*gang = NULL;

	gang = GangState_AllocateGang(type, segments, extendedQuery);

	CurrentDispatcherState->allocatedGangs = lappend(CurrentDispatcherState->allocatedGangs, gang);

	return gang;
}

void
DispatcherState_DispatchCommand(char *formatQuery, int queryLen)
{
	Gang			*gang;

	Assert(CurrentDispatcherState != NULL);

	gang = DispatcherState_AllocateGang(GANGTYPE_PRIMARY_WRITER, makeDefaultSegments(), false);

	DispatcherState_DispatchCommandToGang(gang, formatQuery, queryLen);
}

static void
DispatcherState_DispatchCommandToGang(struct Gang *gp, char *formatQuery, int queryLen)
{
	int i;

	Assert(CurrentDispatcherState);

	/*
	 * Start the dispatching
	 */
	for (i = 0; i < gp->size; i++)
	{
		CdbDispatchResult *qeResult;

		SegmentDatabaseDescriptor *segdbDesc = gp->db_descriptors[i];

		Assert(segdbDesc != NULL);

		/*
		 * Initialize the QE's CdbDispatchResult object.
		 */
		qeResult = DispatcherState_MakeResult(segdbDesc, gp); 

		/*
		 * writer_gang could be NULL if this is an extended query.
		 */
		if (qeResult == NULL)
			elog(FATAL, "could not allocate resources for segworker communication");

		CurrentDispatcherState->results = lappend(CurrentDispatcherState->results, qeResult);

		dispatchCommand(qeResult, formatQuery, queryLen);
	}
}

static DispatcherWaitMode
DispatcherState_GetWaitMode(void)
{
	/*
	 * escalate waitMode to cancel if: - user interrupt has occurred, - or
	 * an error has been reported by any QE, - in case the caller wants
	 * cancelOnError
	 */
	if ((InterruptPending || CurrentDispatcherState->errcode) && CurrentDispatcherState->cancelOnError)
		CurrentDispatcherState->waitMode = DISPATCHER_WAIT_CANCEL;

	return CurrentDispatcherState->waitMode;
}


void
DispatcherState_Join(DispatcherWaitMode waitMode)
{
	SegmentDatabaseDescriptor	*segdbDesc;
	CdbDispatchResult		*dispatchResult;
	int				db_count = 0;
	int				timeout = 0;
	struct 				pollfd *fds;
	uint8 				ftsVersion = 0;
	List				*qeResults;
	ListCell			*lc;

	Assert(CurrentDispatcherState);

	qeResults = CurrentDispatcherState->results;

	db_count = list_length(qeResults);

	fds = (struct pollfd *) palloc(db_count * sizeof(struct pollfd));

	/*
	 * OK, we are finished submitting the command to the segdbs. Now, we have
	 * to wait for them to finish.
	 */
	for (;;)
	{
		int		sock;
		int		n;
		int		nfds = 0;
		PGconn		*conn;

		/*
		 * bail-out if we are dying. Once QD dies, QE will recognize it
		 * shortly anyway.
		 */
		if (proc_exit_inprogress)
			break;

		/*
		 * Which QEs are still running and could send results to us?
		 */
		foreach (lc, qeResults)
		{
			dispatchResult = (CdbDispatchResult *)lfirst(lc);
			segdbDesc = dispatchResult->segdbDesc;
			conn = segdbDesc->conn;

			/*
			 * Already finished with this QE?
			 */
			if (!dispatchResult->stillRunning)
				continue;

			Assert(!cdbconn_isBadConnection(segdbDesc));

			/*
			 * Flush out buffer in case some commands are not fully
			 * dispatched to QEs, this can prevent QD from polling
			 * on such QEs forever.
			 */
			if (conn->outCount > 0)
			{
				/*
				 * Don't error out here, let following poll() routine to
				 * handle it.
				 */
				if (pqFlush(conn) < 0)
					elog(LOG, "Failed flushing outbound data to %s:%s",
						 segdbDesc->whoami, PQerrorMessage(conn));
			}

			/*
			 * Add socket to fd_set if still connected.
			 */
			sock = PQsocket(conn);
			Assert(sock >= 0);
			fds[nfds].fd = sock;
			fds[nfds].events = POLLIN;
			nfds++;
		}

		/*
		 * Break out when no QEs still running.
		 */
		if (nfds <= 0)
		{
			//CurrentDispatcherState->stillRunning = false;
			break;
		}
		/*
		 * Wait for results from QEs
		 *
		 * Don't wait if: - this is called from interconnect to check if
		 * there's any error.
		 *
		 * Lower the timeout if: - we need send signal to QEs.
		 */
		timeout = 20;

		n = poll(fds, nfds, timeout);

		/*
		 * poll returns with an error, including one due to an interrupted
		 * call
		 */
		if (n < 0)
		{
			int			sock_errno = SOCK_ERRNO;

			if (sock_errno == EINTR)
				continue;

			elog(LOG, "handlePollError poll() failed; errno=%d", sock_errno);

			DispatcherState_HandlePollError();

			/*
			 * Since an error was detected for the segment, request
			 * FTS to perform a probe before checking the segment
			 * state.
			 */
			FtsNotifyProber();
			DispatcherState_CheckSegmentAlive();
			DispatcherState_SignalQEs();
		}
		/* If the time limit expires, poll() returns 0 */
		else if (n == 0)
		{
			/*
			 * This code relies on FTS being triggered at regular
			 * intervals. Iff FTS detects change in configuration
			 * then check segment state. FTS probe is not triggered
			 * explicitly in this case because this happens every
			 * DISPATCH_WAIT_TIMEOUT_MSEC.
			 */
			if (ftsVersion == 0 || ftsVersion != getFtsVersion())
			{
				ftsVersion = getFtsVersion();
				DispatcherState_CheckSegmentAlive();
			}

			if (waitMode == DISPATCHER_WAIT_NONBLOCK)
				break;
		}
		/* We have data waiting on one or more of the connections. */
		else
			DispatcherState_HandlePollSuccess(fds);
	}

	pfree(fds);


	/*
	 * It looks like everything went fine, make sure we don't miss a user
	 * cancellation?
	 *
	 * The waitMode argument is NONE when we are doing "normal work".
	 */
	if (waitMode == DISPATCHER_WAIT_BLOCK || waitMode == DISPATCHER_WAIT_FINISH)
		CHECK_FOR_INTERRUPTS();
}

void
DispatcherState_Close(void)
{
	ListCell	*lc;
	Gang		*gang;	

	Assert(CurrentDispatcherState);

	foreach (lc, CurrentDispatcherState->allocatedGangs)	
	{
		gang = (Gang *)lfirst(lc);				

		releaseGang(gang, CurrentDispatcherState->destroy);
	}

	//DispatcherState_Reset()
	CurrentDispatcherState->connected = false;
	CurrentDispatcherState->cancelOnError = false;
	CurrentDispatcherState->errcode = 0;
	CurrentDispatcherState->waitMode = DISPATCHER_WAIT_BLOCK;
	CurrentDispatcherState->allocatedGangs = NIL;
	CurrentDispatcherState->results = NIL;
	CurrentDispatcherState->queryDesc = NULL;
}

static void
dispatchCommand(CdbDispatchResult *dispatchResult,
				const char *query_text,
				int query_text_len)
{
	TimestampTz beforeSend = 0;
	long		secs;
	int			usecs;

	if (DEBUG1 >= log_min_messages)
		beforeSend = GetCurrentTimestamp();

	/*
	 * Submit the command asynchronously.
	 */
	if (PQsendGpQuery_shared(dispatchResult->segdbDesc->conn, (char *) query_text, query_text_len, true) == 0)
	{
		char	   *msg = PQerrorMessage(dispatchResult->segdbDesc->conn);

		dispatchResult->stillRunning = false;
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				 errmsg("Command could not be dispatch to segment %s: %s",
						dispatchResult->segdbDesc->whoami, msg ? msg : "unknown error")));
	}

	if (DEBUG1 >= log_min_messages)
	{
		TimestampDifference(beforeSend, GetCurrentTimestamp(), &secs, &usecs);

		if (secs != 0 || usecs > 1000)	/* Time > 1ms? */
			elog(LOG, "time for PQsendGpQuery_shared %ld.%06d", secs, usecs);
	}

	/*
	 * We'll keep monitoring this QE -- whether or not the command was
	 * dispatched -- in order to check for a lost connection or any other
	 * errors that libpq might have in store for us.
	 */
	dispatchResult->stillRunning = true;
	dispatchResult->hasDispatched = true;

	ELOG_DISPATCHER_DEBUG("Command dispatched to QE (%s)", dispatchResult->segdbDesc->whoami);
}

static CdbDispatchResult *
DispatcherState_MakeResult(struct SegmentDatabaseDescriptor *segdbDesc, Gang *gang)
{
	CdbDispatchResult *dispatchResult;

	/*
	 * Allocate a slot for the new CdbDispatchResult object.
	 */
	dispatchResult = (CdbDispatchResult *)palloc0(sizeof(CdbDispatchResult)); 

	/*
	 * Initialize CdbDispatchResult.
	 */
	dispatchResult->meleeResults = NULL;
	dispatchResult->meleeIndex = -1;
	dispatchResult->gang = gang;
	dispatchResult->segdbDesc = segdbDesc;
	dispatchResult->resultbuf = createPQExpBuffer();
	dispatchResult->error_message = createPQExpBuffer();
	dispatchResult->numrowsrejected = 0;
	dispatchResult->numrowscompleted = 0;

	/*
	 * Reset summary indicators.
	 */
	cdbdisp_resetResult(dispatchResult);

	return dispatchResult;
}

static void
DispatcherState_CheckSegmentAlive(void)
{
	ListCell		*lc;
	CdbDispatchResult	*dispatchResult;
	SegmentDatabaseDescriptor *segdbDesc;

	Assert(CurrentDispatcherState);

	/*
	 * check the connection still valid
	 */
	foreach (lc, CurrentDispatcherState->results)
	{
		dispatchResult = (CdbDispatchResult *)lfirst(lc);
		segdbDesc = dispatchResult->segdbDesc;

		/*
		 * Skip if already finished or didn't dispatch.
		 */
		if (!dispatchResult->stillRunning)
			continue;

		/*
		 * Skip the entry db.
		 */
		if (segdbDesc->segindex < 0)
			continue;

		if (!FtsIsSegmentUp(segdbDesc->segment_database_info))
		{
			char	   *msg = PQerrorMessage(segdbDesc->conn);

			dispatchResult->stillRunning = false;
			DispatcherState_AppendMessage(dispatchResult, LOG,
						   "FTS detected connection lost during dispatch to %s: %s",
						   dispatchResult->segdbDesc->whoami, msg ? msg : "unknown error");

			/*
			 * Not a good idea to store into the PGconn object. Instead, just
			 * close it.
			 */
			PQfinish(segdbDesc->conn);
			segdbDesc->conn = NULL;
		}
	}
}

static void
DispatcherState_SignalQEs(void)
{
	DispatcherWaitMode	waitMode;
	ListCell		*lc;

	Assert(CurrentDispatcherState);
	
	waitMode = DispatcherState_GetWaitMode();

	foreach (lc, CurrentDispatcherState->results)
	{
		char		errbuf[256];
		bool		sent = false;
		CdbDispatchResult *dispatchResult = (CdbDispatchResult *)lfirst(lc);

		Assert(dispatchResult != NULL);
		SegmentDatabaseDescriptor *segdbDesc = dispatchResult->segdbDesc;

		/*
		 * Don't send the signal if - QE is finished or canceled - the signal
		 * was already sent - connection is dead
		 */

		if (!dispatchResult->stillRunning ||
			dispatchResult->wasCanceled ||
			cdbconn_isBadConnection(segdbDesc))
			continue;

		memset(errbuf, 0, sizeof(errbuf));

		sent = cdbconn_signalQE(segdbDesc, errbuf, waitMode == DISPATCHER_WAIT_CANCEL);

		if (sent)
			dispatchResult->sentSignal = waitMode;
		else
			elog(LOG, "Unable to cancel: %s",
				 strlen(errbuf) == 0 ? "cannot allocate PGCancel" : errbuf);
	}

}

static void
DispatcherState_HandlePollSuccess(struct pollfd *fds)
{
	ListCell		*lc;
	int			currentFdNumber = 0;

	/*
	 * We have data waiting on one or more of the connections.
	 */
	foreach (lc, CurrentDispatcherState->results)
	{
		bool		finished;
		int			sock;
		CdbDispatchResult *dispatchResult = (CdbDispatchResult *)lfirst(lc);
		SegmentDatabaseDescriptor *segdbDesc = dispatchResult->segdbDesc;

		/*
		 * Skip if already finished or didn't dispatch.
		 */
		if (!dispatchResult->stillRunning)
			continue;

		sock = PQsocket(segdbDesc->conn);
		Assert(sock >= 0);
		Assert(sock == fds[currentFdNumber].fd);

		/*
		 * Skip this connection if it has no input available.
		 */
		if (!(fds[currentFdNumber++].revents & POLLIN))
			continue;

		/*
		 * Receive and process results from this QE.
		 */
		finished = processResults(dispatchResult);

		/*
		 * Are we through with this QE now?
		 */
		if (finished)
		{
			dispatchResult->stillRunning = false;

			if (DEBUG1 >= log_min_messages)
			{
				char		msec_str[32];

				switch (check_log_duration(msec_str, false))
				{
					case 1:
					case 2:
						elog(LOG, "duration to dispatch result received from ? (seg %d): %s ms",
							  dispatchResult->segdbDesc->segindex, msec_str);
						break;
				}
			}

			if (PQisBusy(dispatchResult->segdbDesc->conn))
				elog(LOG, "We thought we were done, because finished==true, but libpq says we are still busy");
		}
	}

}

static void
DispatcherState_HandlePollError(void)
{
	ListCell	*lc;

	foreach (lc, CurrentDispatcherState->results)
	{
		CdbDispatchResult *dispatchResult = (CdbDispatchResult *)lfirst(lc);
		SegmentDatabaseDescriptor *segdbDesc = dispatchResult->segdbDesc;

		/* Skip if already finished or didn't dispatch. */
		if (!dispatchResult->stillRunning)
			continue;

		/* We're done with this QE, sadly. */
		if (PQstatus(segdbDesc->conn) == CONNECTION_BAD)
		{
			char	   *msg = PQerrorMessage(segdbDesc->conn);

			if (msg)
				elog(LOG, "Dispatcher encountered connection error on %s: %s", segdbDesc->whoami, msg);

			elog(LOG, "Dispatcher noticed bad connection in handlePollError()");

			/* Save error info for later. */
			DispatcherState_AppendMessage(dispatchResult, LOG,
							   "Error after dispatch from %s: %s",
						   		segdbDesc->whoami,
						   		msg ? msg : "unknown error");

			PQfinish(segdbDesc->conn);
			segdbDesc->conn = NULL;
			dispatchResult->stillRunning = false;
		}
	}

	return;

}

static void
DispatcherState_AppendMessage(CdbDispatchResult *dispatchResult, int elevel, const char *fmt,...)
{
	va_list		args;
	int		msgoff;

#if 0
	/*
	 * Remember first error.
	 */
	cdbdisp_seterrcode(ERRCODE_GP_INTERCONNECTION_ERROR, -1, dispatchResult);
#endif

	/*
	 * Allocate buffer if first message. Insert newline between previous
	 * message and new one.
	 */
	Assert(dispatchResult->error_message != NULL);
	oneTrailingNewlinePQ(dispatchResult->error_message);

	msgoff = dispatchResult->error_message->len;

	/*
	 * Format the message and append it to the buffer.
	 */
	va_start(args, fmt);
	appendPQExpBufferVA(dispatchResult->error_message, fmt, args);
	va_end(args);

	/*
	 * Display the message on stderr for debugging, if requested. This helps
	 * to clarify the actual timing of threaded events.
	 */
	if (elevel >= log_min_messages)
	{
		oneTrailingNewlinePQ(dispatchResult->error_message);
		elog(LOG, "%s", dispatchResult->error_message->data + msgoff);
	}

	/*
	 * In case the caller wants to hand the buffer to ereport(), follow the
	 * ereport() convention of not ending with a newline.
	 */
	noTrailingNewlinePQ(dispatchResult->error_message);
}

static void
noTrailingNewlinePQ(PQExpBuffer buf)
{
	while (buf->len > 0 && buf->data[buf->len - 1] <= ' ' && buf->data[buf->len - 1] > '\0')
		buf->data[--buf->len] = '\0';
}

static void
oneTrailingNewlinePQ(PQExpBuffer buf)
{
	noTrailingNewlinePQ(buf);
	if (buf->len > 0)
		appendPQExpBufferChar(buf, '\n');
}

static bool
processResults(CdbDispatchResult *dispatchResult)
{
	SegmentDatabaseDescriptor *segdbDesc = dispatchResult->segdbDesc;
	char	   *msg;

	/*
	 * Receive input from QE.
	 */
	if (PQconsumeInput(segdbDesc->conn) == 0)
	{
		msg = PQerrorMessage(segdbDesc->conn);
		DispatcherState_AppendMessage(dispatchResult, LOG,
						   "Error on receive from %s: %s",
						   segdbDesc->whoami, msg ? msg : "unknown error");
		return true;
	}

	/*
	 * If we have received one or more complete messages, process them.
	 */
	while (!PQisBusy(segdbDesc->conn))
	{
		/* loop to call PQgetResult; won't block */
		PGresult   *pRes;
		ExecStatusType resultStatus;
		int			resultIndex;

		/*
		 * PQisBusy() does some error handling, which can cause the connection
		 * to die -- we can't just continue on as if the connection is happy
		 * without checking first.
		 *
		 * For example, cdbdisp_numPGresult() will return a completely bogus
		 * value!
		 */
		if (cdbconn_isBadConnection(segdbDesc))
		{
			msg = PQerrorMessage(segdbDesc->conn);
			DispatcherState_AppendMessage(dispatchResult, LOG,
										   "Connection lost when receiving from %s: %s",
										   segdbDesc->whoami, msg ? msg : "unknown error");
			return true;
		}

		/*
		 * Get one message.
		 */
		pRes = PQgetResult(segdbDesc->conn);

		/*
		 * Command is complete when PGgetResult() returns NULL. It is critical
		 * that for any connection that had an asynchronous command sent thru
		 * it, we call PQgetResult until it returns NULL. Otherwise, the next
		 * time a command is sent to that connection, it will return an error
		 * that there's a command pending.
		 */
		if (!pRes)
		{
			/* this is normal end of command */
			return true;
		}

		/*
		 * Attach the PGresult object to the CdbDispatchResult object.
		 */
		resultIndex = cdbdisp_numPGresult(dispatchResult);
		cdbdisp_appendResult(dispatchResult, pRes);

		/*
		 * Did a command complete successfully?
		 */
		resultStatus = PQresultStatus(pRes);
		if (resultStatus == PGRES_COMMAND_OK ||
			resultStatus == PGRES_TUPLES_OK ||
			resultStatus == PGRES_COPY_IN ||
			resultStatus == PGRES_COPY_OUT ||
			resultStatus == PGRES_EMPTY_QUERY)
		{
			ELOG_DISPATCHER_DEBUG("%s -> ok %s",
								  segdbDesc->whoami,
								  PQcmdStatus(pRes) ? PQcmdStatus(pRes) : "(no cmdStatus)");

			if (resultStatus == PGRES_EMPTY_QUERY)
				ELOG_DISPATCHER_DEBUG("QE received empty query.");

			/*
			 * Save the index of the last successful PGresult. Can be given to
			 * cdbdisp_getPGresult() to get tuple count, etc.
			 */
			dispatchResult->okindex = resultIndex;

			/*
			 * SREH - get number of rows rejected from QE if any
			 */
			if (pRes->numRejected > 0)
				dispatchResult->numrowsrejected += pRes->numRejected;

			/*
			 * COPY FROM ON SEGMENT - get the number of rows completed by QE
			 * if any
			 */
			if (pRes->numCompleted > 0)
				dispatchResult->numrowscompleted += pRes->numCompleted;

			if (resultStatus == PGRES_COPY_IN ||
				resultStatus == PGRES_COPY_OUT)
				return true;
		}

		/*
		 * Note QE error. Cancel the whole statement if requested.
		 */
		else
		{
			/* QE reported an error */
			char	   *sqlstate = PQresultErrorField(pRes, PG_DIAG_SQLSTATE);
			int			errcode = 0;

			msg = PQresultErrorMessage(pRes);

			ELOG_DISPATCHER_DEBUG("%s -> %s %s  %s",
								  segdbDesc->whoami,
								  PQresStatus(resultStatus),
								  sqlstate ? sqlstate : "(no SQLSTATE)",
								  msg);

			/*
			 * Convert SQLSTATE to an error code (ERRCODE_xxx). Use a generic
			 * nonzero error code if no SQLSTATE.
			 */
			if (sqlstate && strlen(sqlstate) == 5)
				errcode = sqlstate_to_errcode(sqlstate);

			/*
			 * Save first error code and the index of its PGresult buffer
			 * entry.
			 */
			//cdbdisp_seterrcode(errcode, resultIndex, dispatchResult);
		}
	}

	return false;				/* we must keep on monitoring this socket */
}

/* assign gangs and fill sliceTable db_descriptions */
void
DispatcherState_AssignGangs(SliceTable *sliceTbl, int rootSliceIdx, bool extendedQuery)
{
	Assert(sliceTbl);
	Assert(rootSliceIdx >= 0);

	DispatcherState_InventorySliceTree(sliceTbl, rootSliceIdx, extendedQuery);
}

void
DispatcherState_DispatchCommandToGangs(char *queryText, int queryTextLength,
						SliceTable *sliceTbl, int rootIdx)
{
	SliceVec   *sliceVector = NULL;
	int			nSlices = 1;	/* slices this dispatch cares about */
	int			nTotalSlices = 1;	/* total slices in sliceTbl */

	int			iSlice;

	if (log_dispatch_stats)
		ResetUsage();

	Assert(CurrentDispatcherState);
	Assert(sliceTbl != NULL);
	Assert(rootIdx == 0 ||
		   (rootIdx > sliceTbl->nMotions &&
			rootIdx <= sliceTbl->nMotions + sliceTbl->nInitPlans));

	/*
	 * Traverse the slice tree in sliceTbl rooted at rootIdx and build a
	 * vector of slice indexes specifying the order of [potential] dispatch.
	 */
	nTotalSlices = list_length(sliceTbl->slices);
	sliceVector = palloc0(nTotalSlices * sizeof(SliceVec));

	nSlices = fillSliceVector(sliceTbl, rootIdx, sliceVector, nTotalSlices);

	/* Start dispatching routine */

	cdb_total_plans++;
	cdb_total_slices += nSlices;
	if (nSlices > cdb_max_slices)
		cdb_max_slices = nSlices;

	if (DEBUG1 >= log_min_messages)
	{
		char		msec_str[32];

		switch (check_log_duration(msec_str, false))
		{
			case 1:
			case 2:
				ereport(LOG,
						(errmsg("duration to start of dispatch send (root %d): %s ms",
								rootIdx, msec_str)));
				break;
		}
	}

	for (iSlice = 0; iSlice < nSlices; iSlice++)
	{
		Gang	   *primaryGang = NULL;
		Slice	   *slice = NULL;
		int			si = -1;

		Assert(sliceVector != NULL);

		slice = sliceVector[iSlice].slice;
		si = slice->sliceIndex;

		/*
		 * Is this a slice we should dispatch?
		 */
		if (slice && slice->gangType == GANGTYPE_UNALLOCATED)
		{
			Assert(slice->primaryGang == NULL);

			/*
			 * Most slices are dispatched, however, in many cases the root
			 * runs only on the QD and is not dispatched to the QEs.
			 */
			continue;
		}

		primaryGang = slice->primaryGang;
		Assert(primaryGang != NULL);

#if 0
		we need to consider direct dispatch here
#endif
		/*
		 * Bail out if already got an error or cancellation request.
		 */
		if (CurrentDispatcherState->errcode)
			break;
		if (InterruptPending)
			break;

		/* replcae dummy slice index with real index */
		DispatcherState_ReplaceDummySliceIndex(queryText, si);

		DispatcherState_DispatchCommandToGang(primaryGang, queryText, queryTextLength);

		SIMPLE_FAULT_INJECTOR(AfterOneSliceDispatched);
	}

	pfree(sliceVector);

	DispatcherState_WaitDispatchFinish();

	/*
	 * If bailed before completely dispatched, stop QEs and throw error.
	 */
	if (iSlice < nSlices)
	{
		elog(FATAL, "unable to dispatch plan");

#if 0
		elog(Debug_cancel_print ? LOG : DEBUG2,
			 "Plan dispatch canceled; dispatched %d of %d slices",
			 iSlice, nSlices);

		/*
		 * Cancel any QEs still running, and wait for them to terminate.
		 */
		cdbdisp_cancelDispatch(ds);

		/*
		 * Check and free the results of all gangs. If any QE had an error,
		 * report it and exit via PG_THROW.
		 */
		cdbdisp_finishCommand(ds, NULL, NULL, true);

		/*
		 * Wasn't an error, must have been an interrupt.
		 */
		CHECK_FOR_INTERRUPTS();

		/*
		 * Strange! Not an interrupt either.
		 */
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg_internal("Unable to dispatch plan.")));
#endif
	}

	if (DEBUG1 >= log_min_messages)
	{
		char		msec_str[32];

		switch (check_log_duration(msec_str, false))
		{
			case 1:
			case 2:
				ereport(LOG,
						(errmsg("duration to dispatch out (root %d): %s ms",
								rootIdx, msec_str)));
				break;
		}
	}
}

void
DispatcherState_WaitDispatchFinish(void)
{	
#define DISPATCH_POLL_TIMEOUT 500

	struct pollfd	*fds;
	int		nfds;
	int		dispatchCount;
	ListCell	*lc;

	Assert(CurrentDispatcherState);

	dispatchCount = list_length(CurrentDispatcherState->results);

	fds = (struct pollfd *) palloc(dispatchCount * sizeof(struct pollfd));

	while (true)
	{
		int			pollRet;

		nfds = 0;
		memset(fds, 0, dispatchCount * sizeof(struct pollfd));

		foreach (lc, CurrentDispatcherState->results)
		{
			CdbDispatchResult *qeResult = (CdbDispatchResult *)lfirst(lc);
			SegmentDatabaseDescriptor *segdbDesc = qeResult->segdbDesc;
			PGconn	   *conn = segdbDesc->conn;
			int			ret;

			/* skip already completed connections */
			if (conn->outCount == 0)
				continue;

			/*
			 * call send for this connection regardless of its POLLOUT status,
			 * because it may be writable NOW
			 */
			ret = pqFlushNonBlocking(conn);

			if (ret == 0)
				continue;
			else if (ret > 0)
			{
				int			sock = PQsocket(segdbDesc->conn);

				Assert(sock >= 0);
				fds[nfds].fd = sock;
				fds[nfds].events = POLLOUT;
				nfds++;
			}
			else if (ret < 0)
			{
				pqHandleSendFailure(conn);
				char	   *msg = PQerrorMessage(conn);

				qeResult->stillRunning = false;
				ereport(ERROR,
						(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
						 errmsg("Command could not be dispatch to segment %s: %s", qeResult->segdbDesc->whoami, msg ? msg : "unknown error")));
			}
		}

		if (nfds == 0)
			break;

		/* guarantee poll() is interruptible */
		do
		{
			CHECK_FOR_INTERRUPTS();

			pollRet = poll(fds, nfds, DISPATCH_POLL_TIMEOUT);
			if (pollRet == 0)
				ELOG_DISPATCHER_DEBUG("cdbdisp_waitDispatchFinish_async(): Dispatch poll timeout after %d ms", DISPATCH_POLL_TIMEOUT);
		}
		while (pollRet == 0 || (pollRet < 0 && (SOCK_ERRNO == EINTR || SOCK_ERRNO == EAGAIN)));

		if (pollRet < 0)
			elog(ERROR, "Poll failed during dispatch");
	}

	pfree(fds);
}

static void
DispatcherState_InventorySliceTree(SliceTable *sliceTbl, int sliceIndex, bool extendedQuery)
{
	ListCell	*cell;
	int		childIndex;
	Slice	   	*slice;
	List		*segments = NIL;

	slice = list_nth(sliceTbl->slices, sliceIndex);

	if (slice->gangType == GANGTYPE_UNALLOCATED)
	{
		slice->primaryGang = NULL;
		slice->primaryProcesses = getCdbProcessesForQD(true);
	}
	else
	{
		switch(slice->gangType)
		{
			case GANGTYPE_PRIMARY_WRITER:
			case GANGTYPE_PRIMARY_READER:
				if (slice->directDispatch.isDirectDispatch)
					segments = slice->directDispatch.contentIds;
				else
					segments = makeDefaultSegments();
				break;
			case GANGTYPE_SINGLETON_READER:
				segments = list_make1_int(gp_singleton_segindex);
				break;
			case GANGTYPE_ENTRYDB_READER:
				segments = list_make1_int(-1);
			default:
				Assert(false);
		}

		slice->primaryGang = DispatcherState_AllocateGang(slice->gangType, segments, extendedQuery);
		slice->primaryGang->sliceIndex = slice->sliceIndex;

		slice->primaryProcesses = getCdbProcessList(slice->primaryGang,
						slice->sliceIndex,
						NULL);
	}

	foreach(cell, slice->children)
	{
		childIndex = lfirst_int(cell);
		DispatcherState_InventorySliceTree(sliceTbl, childIndex, extendedQuery);
	}
}

List *
makeDefaultSegments(void)
{
    List *segments = NULL;
	int i;
	for (i = 0; i < GpIdentity.numsegments; i++)
	{
    		segments = lappend_int(segments, i);
	}
    return segments;
}

void
DispatcherState_GetResults(CdbPgResults *cdb_pgresults)
{
	CdbDispatchResult *dispatchResult;
	int			nslots;
	int			nresults = 0;
	ListCell		*lc;

	if (!cdb_pgresults)
		return;

	/*
	 * Allocate result set ptr array. The caller must PQclear() each PGresult
	 * and free() the array.
	 */
	nslots = 0;

	foreach (lc,  CurrentDispatcherState->results)
	{
		dispatchResult = (CdbDispatchResult *)lfirst(lc);
		nslots += cdbdisp_numPGresult(dispatchResult);
	}


	cdb_pgresults->pg_results = (struct pg_result **) calloc(nslots, sizeof(struct pg_result *));

	if (!cdb_pgresults->pg_results)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
						errmsg
						("cdbdisp_returnResults failed: out of memory")));

	/*
	 * Collect results from primary gang.
	 */
	foreach (lc, CurrentDispatcherState->results)
	{
		dispatchResult = (CdbDispatchResult *)lfirst(lc);

		/*
		 * Take ownership of this QE's PGresult object(s).
		 */
		nresults += cdbdisp_snatchPGresults(dispatchResult,
											cdb_pgresults->pg_results + nresults,
											nslots - nresults);
	}

	Assert(nresults == nslots);

	/* tell the caller how many sets we're returning. */
	cdb_pgresults->numResults = nresults;
}

static int
cdbdisp_snatchPGresults(CdbDispatchResult *dispatchResult,
						struct pg_result **pgresultptrs, int maxresults)
{
	PQExpBuffer buf = dispatchResult->resultbuf;
	PGresult  **begp = (PGresult **) buf->data;
	PGresult  **endp = (PGresult **) (buf->data + buf->len);
	PGresult  **p;
	int			nresults = 0;

	/*
	 * Snatch the PGresult objects.
	 */
	for (p = begp; p < endp; ++p)
	{
		Assert(*p != NULL);
		Assert(nresults < maxresults);
		pgresultptrs[nresults++] = *p;
		*p = NULL;
	}

	/*
	 * Empty our PGresult array.
	 */
	resetPQExpBuffer(buf);
	dispatchResult->errindex = -1;
	dispatchResult->okindex = -1;

	return nresults;
}

static void
DispatcherState_ReplaceDummySliceIndex(char *queryText, int sliceIndex)
{
	int queryTextLen;
	int newSliceIndex;

	memcpy(&queryTextLen, queryText + 1, sizeof(int));

	queryTextLen = ntohl(queryTextLen);

	newSliceIndex = htonl(sliceIndex);

	memcpy(queryText + queryTextLen + 1 - sizeof(int), &newSliceIndex, sizeof(int));
}
