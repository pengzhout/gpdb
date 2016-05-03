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
#include "cdb/cdbdisp_thread.h"
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
extern volatile int32 RunningThreadCount;
extern pthread_t main_tid;
#ifndef _WIN32
#define mythread() ((unsigned long) pthread_self())
#else
#define mythread() ((unsigned long) pthread_self().p)
#endif 

pthread_mutex_t dbmutex = PTHREAD_MUTEX_INITIALIZER;
/*
 * default directed-dispatch parameters: don't direct anything.
 */

/*
 * Static Helper functions
 */
							
int getDispatchThreadCount(Gang *gp, CdbDispatchDirectDesc *disp_direct);

void cdbdisp_dispatchToGang_V1(struct Gang *gp, char* query, int len, CdbDispatchDirectDesc *disp_direct);
bool thread_dispatchToGang(struct Gang *gp, CdbDispatchDirectDesc *disp_direct);
void* thread_dispatch_func(void* arg);

struct SegmentDatabaseDescriptor** thread_DispatchOutV1(Gang* gp);
void thread_DispatchWaitV1(struct SegmentDatabaseDescriptor** dbs);

bool dispatchCommandV1(SegmentDatabaseDescriptor* segDesc, char* query, int len);

void dispatchCommandInternal(SegmentDatabaseDescriptor* segdbDesc,
                const char *query_text, int query_len);

int getPollFds(struct SegmentDatabaseDescriptor** dbs, struct pollfd* poll_fds);
void handleConnectionData(struct SegmentDatabaseDescriptor** dbs, struct pollfd* poll_fds);
void cdbdisp_appendResultV1(SegmentDatabaseDescriptor *segdbDesc, struct pg_result  *res);
bool processResultsV1(SegmentDatabaseDescriptor *segdbDesc);
void CdbCheckDispatchResultInt_V1(struct CdbDispatcherState *ds);

int getThreadCount(void);
/*
 * Counter to indicate there are some dispatch threads running.  This will
 * be incremented at the beginning of dispatch threads and decremented at
 * the end of them.
 */
static pthread_t * ThreadHandles = NULL;
static int ThreadNeeded = 0;

int getDispatchThreadCount(Gang *gp, CdbDispatchDirectDesc *disp_direct)
{
	if (disp_direct && disp_direct->directed_dispatch)
		return 1;
	else if (gp_connections_per_thread == 0)
		return 0;
	else
		return 1 + gp->size / gp_connections_per_thread;
}

void cdbdisp_dispatchToGang_V1(struct Gang *gp, char* query, int len, CdbDispatchDirectDesc *disp_direct)
{

	if (gp->isWriterGang && gp->dispatcherActive)
	{
		/* Are we dispatching to the writer-gang when it is already busy ? */
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("query plan with multiple segworker groups is not supported"),
						 errhint("likely caused by a function that reads or modifies data in a distributed table")));

	}

	gp->CurrentQuery = query;
	gp->QueryLen = len;

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
	int	i;	
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

	memset(ThreadHandles, 0 , ThreadNeeded * sizeof(int));

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
			pthread_err = gp_pthread_create(&ThreadHandles[i], thread_dispatch_func, (void*)gp, "dispatchToGang");
			if (pthread_err != 0)
			{
				ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("could not create thread %d of %d", i + 1, ThreadNeeded),
								errdetail("pthread_create() failed with err %d", pthread_err)));
			}
		}
	}

	return true;
}

static void
DecrementRunningCount(void *arg)
{
	pg_atomic_sub_fetch_u32((pg_atomic_uint32 *)&RunningThreadCount, 1);
}


/*
 *	Helper function for thread dispatcher
 *
 */
void* thread_dispatch_func(void* arg)
{
	Gang	*gp = (Gang*)arg;

	gp_set_thread_sigmasks();

	pg_atomic_add_fetch_u32((pg_atomic_uint32 *)&RunningThreadCount, 1);
	/*
	 * We need to make sure the value will be decremented once the thread
	 * finishes.  Currently there is not such case but potentially we could
	 * have pthread_exit or thread cancellation in the middle of code, in
	 * which case we would miss to decrement value if we tried to do this
	 * without the cleanup callback facility.
	 */
	pthread_cleanup_push(DecrementRunningCount, NULL);
	{
		struct SegmentDatabaseDescriptor** dbs =
							thread_DispatchOutV1(gp);
		/*
		 * thread_DispatchWaitSingle might have a problem with interupts
		 */
		thread_DispatchWaitV1(dbs);
	}
	pthread_cleanup_pop(1);

	return (void*)NULL;
}

struct SegmentDatabaseDescriptor**
thread_DispatchOutV1(Gang* gp)
{
	int i = 0;
	int dbcount = 0;

	Assert(gp_connections_per_thread > 0);

	struct SegmentDatabaseDescriptor** dbs_managed_by_me = (struct SegmentDatabaseDescriptor**) malloc(gp_connections_per_thread * sizeof(SegmentDatabaseDescriptor*));

	memset(dbs_managed_by_me, 0, gp_connections_per_thread * sizeof(SegmentDatabaseDescriptor*));

	struct SegmentDatabaseDescriptor* segDesc = NULL; 

	for (i = 0; i < gp->size; i++)
	{
		segDesc = &gp->db_descriptors[i];
		// Lock thread gang lock
		pthread_mutex_lock(&dbmutex);
		/* Don't use elog, it's not thread-safe */
		if (segDesc->dispatched == true)
		{
			// release lock
			pthread_mutex_unlock(&dbmutex);
			continue;
		}
		else
		{
			segDesc->dispatched = true;
			pthread_mutex_unlock(&dbmutex);
			// release lock
		}

		dispatchCommandV1(segDesc, gp->CurrentQuery, gp->QueryLen);
		dbs_managed_by_me[dbcount++] = segDesc;
		segDesc->stillRunning = true;
	}

	return dbs_managed_by_me;
}


void thread_DispatchWaitV1(struct SegmentDatabaseDescriptor** dbs)
{

	struct pollfd* poll_fds = malloc(gp_connections_per_thread * sizeof(struct pollfd));

	/*
	 * OK, we are finished submitting the command to the segdbs.
	 * Now, we have to wait for them to finish.
	 */
	for (;;)
	{							/* some QEs running */
		int			n;
		int			nfds = 0;

		/*
		 * Which QEs are still running and could send results to us?
		 */
		nfds = getPollFds(dbs, poll_fds); 

		/* Break out when no QEs still running. */
		if (nfds <= 0)
			break;

		/*
		 * bail-out if we are dying.  We should not do much of cleanup
		 * as the main thread is waiting on this thread to finish.  Once
		 * QD dies, QE will recognize it shortly anyway.
		 */
		if (proc_exit_inprogress)
			break;

		/*
		 * Wait for results from QEs.
		 */

		/* Block here until input is available. */
		n = poll(poll_fds, nfds, DISPATCH_WAIT_TIMEOUT_SEC * 1000);

		if (n < 0)
		{
			int			sock_errno = SOCK_ERRNO;

			if (sock_errno == EINTR)
				continue;

			//handlePollError(pParms, db_count, sock_errno);
			continue;
		}

		if (n == 0)
		{
			//handlePollTimeout(pParms, db_count, &timeoutCounter, true);
			continue;
		}

		/*
		 * We have data waiting on one or more of the connections.
		 */
		handleConnectionData(dbs, poll_fds);

	}							/* some QEs running */
	
}

bool
dispatchCommandV1(SegmentDatabaseDescriptor* segdbDesc, char* queryText, int queryLen)
{
	if (PQstatus(segdbDesc->conn) == CONNECTION_BAD)	
	{
		char *msg = PQerrorMessage(segdbDesc->conn);
		appendPQExpBuffer(&segdbDesc->error_message, "Connection is bad before sending the query from %s : %s",
								segdbDesc->whoami, msg ? msg : "unknow error");

		PQfinish(segdbDesc->conn);
		segdbDesc->conn = NULL;
		segdbDesc->stillRunning = false;

		return false;
	}

	dispatchCommandInternal(segdbDesc, queryText, queryLen);	

	return true;

#ifdef USE_NONBLOCKING
	/* add flush code here*/
#endif
}

void dispatchCommandInternal(SegmentDatabaseDescriptor* segdbDesc,
		const char *query_text, int query_text_len)
{
	PGconn	   *conn = segdbDesc->conn;
	TimestampTz beforeSend = 0;
	long		secs;
	int			usecs;

	if (DEBUG1 >= log_min_messages)
		beforeSend = GetCurrentTimestamp();

	/*
	 * Submit the command asynchronously.
	 */
	if (PQsendGpQuery_shared(conn, (char *)query_text, query_text_len) == 0)
	{
		char	   *msg = PQerrorMessage(segdbDesc->conn);

		if (DEBUG3 >= log_min_messages)
			write_log("PQsendMPPQuery_shared error %s %s",
					  segdbDesc->whoami, msg ? msg : "");

		/* Note the error. */
		appendPQExpBuffer(&segdbDesc->error_message,
							  "Command could not be sent to segment db %s;  %s",
							  segdbDesc->whoami, msg ? msg : "");
		PQfinish(conn);
		segdbDesc->conn = NULL;
		segdbDesc->stillRunning = false;
	}
	
	if (DEBUG1 >= log_min_messages)
	{
		TimestampDifference(beforeSend,
							GetCurrentTimestamp(),
							&secs, &usecs);
		
		if (secs != 0 || usecs > 1000) /* Time > 1ms? */
			write_log("time for PQsendGpQuery_shared %ld.%06d", secs, usecs);
	}

	/*
	 * We'll keep monitoring this QE -- whether or not the command
	 * was dispatched -- in order to check for a lost connection
	 * or any other errors that libpq might have in store for us.
	 */
}	/* dispatchCommand */

bool							/* returns true if command complete */
processResultsV1(SegmentDatabaseDescriptor *segdbDesc)
{
	char	   *msg;
	int			rc;

	/* Receive input from QE. */
	rc = PQconsumeInput(segdbDesc->conn);

	/* If PQconsumeInput fails, we're hosed. */
	if (rc == 0)
	{ /* handle PQconsumeInput error */
		goto connection_error;
	}

	/* If we have received one or more complete messages, process them. */
	while (!PQisBusy(segdbDesc->conn))
	{							/* loop to call PQgetResult; won't block */
		PGresult   *pRes;
		ExecStatusType resultStatus;

		/* MPP-2518: PQisBusy() does some error handling, which can
		 * cause the connection to die -- we can't just continue on as
		 * if the connection is happy without checking first. 
		 *
		 * For example, cdbdisp_numPGresult() will return a completely
		 * bogus value! */
		if (PQstatus(segdbDesc->conn) == CONNECTION_BAD || segdbDesc->conn->sock == -1)
		{ /* connection is dead. */
			goto connection_error;
		}

		/* Get one message. */
		pRes = PQgetResult(segdbDesc->conn);
		
		/*
		 * Command is complete when PGgetResult() returns NULL. It is critical
		 * that for any connection that had an asynchronous command sent thru
		 * it, we call PQgetResult until it returns NULL. Otherwise, the next
		 * time a command is sent to that connection, it will return an error
		 * that there's a command pending.
		 */
		if (!pRes)
			return true;		/* this is normal end of command */

		
		/* Attach the PGresult object to the CdbDispatchResult object. */
		cdbdisp_appendResultV1(segdbDesc, pRes);

		/* Did a command complete successfully? */
		resultStatus = PQresultStatus(pRes);

		if (resultStatus == PGRES_COMMAND_OK ||
			resultStatus == PGRES_TUPLES_OK ||
			resultStatus == PGRES_COPY_IN ||
			resultStatus == PGRES_COPY_OUT)
		{						/* QE reported success */

			/*
			 * Save the index of the last successful PGresult. Can be given to
			 * cdbdisp_getPGresult() to get tuple count, etc.
			 */
			
			/* SREH - get number of rows rejected from QE if any */
			if(pRes->numRejected > 0)
				segdbDesc->numrowsrejected += pRes->numRejected;

			if (resultStatus == PGRES_COPY_IN ||
				resultStatus == PGRES_COPY_OUT)
				return true;
		}						/* QE reported success */

		/* Note QE error.  Cancel the whole statement if requested. */
		else
		{						/* QE reported an error */
			char	   *sqlstate = PQresultErrorField(pRes, PG_DIAG_SQLSTATE);
			int			errcode = 0;

			msg = PQresultErrorMessage(pRes);

			/*
			 * Convert SQLSTATE to an error code (ERRCODE_xxx). Use a generic
			 * nonzero error code if no SQLSTATE.
			 */
			if (sqlstate &&
				strlen(sqlstate) == 5)
				errcode = cdbdisp_sqlstate_to_errcode(sqlstate);

			/*
			 * Save first error code and the index of its PGresult buffer
			 * entry.
			 */
			//cdbdisp_seterrcode(errcode, resultIndex, dispatchResult);
		}						/* QE reported an error */
	}							/* loop to call PQgetResult; won't block */
	
	return false;				/* we must keep on monitoring this socket */

connection_error:
	msg = PQerrorMessage(segdbDesc->conn);

	if (msg)
		write_log("Dispatcher encountered connection error on %s: %s", segdbDesc->whoami, msg);

	/* Save error info for later. */
	appendPQExpBuffer(&segdbDesc->error_message,
						  "Error on receive from %s: %s",
						  segdbDesc->whoami,
						  msg ? msg : "unknown error");

	/* Can't recover, so drop the connection. */
	PQfinish(segdbDesc->conn);
	segdbDesc->conn = NULL;
	segdbDesc->stillRunning = false;

	return true; /* connection is gone! */	
}	/* processResults */

int getPollFds(struct SegmentDatabaseDescriptor** dbs, struct pollfd* poll_fds)
{
	int 	i;
	int	sock;
	int	nfds = 0;

	SegmentDatabaseDescriptor	*segdbDesc;

	for (i = 0; i < gp_connections_per_thread; i++)
	{						/* loop to check connection status */
		segdbDesc = dbs[i];

		/* Already finished with this QE? */
		if (!segdbDesc || !segdbDesc->stillRunning)
			continue;

		/* Add socket to fd_set if still connected. */
		sock = PQsocket(segdbDesc->conn);
		if (sock >= 0 &&
			PQstatus(segdbDesc->conn) != CONNECTION_BAD)
		{
			poll_fds[nfds].fd = sock;
			poll_fds[nfds].events = POLLIN;
			nfds++;
		}

		/* Lost the connection. */
		else
		{
			char	   *msg = PQerrorMessage(segdbDesc->conn);

			/* Save error info for later. */
			appendPQExpBuffer(&segdbDesc->error_message,
								  "Lost connection to %s.  %s",
								  segdbDesc->whoami,
								  msg ? msg : "");

			/* Free the PGconn object. */
			PQfinish(segdbDesc->conn);
			segdbDesc->conn = NULL;
			segdbDesc->stillRunning = false;	/* he's dead, Jim */
		}
	}						/* loop to check connection status */

	return nfds;
}


void handleConnectionData(struct SegmentDatabaseDescriptor** dbs, struct pollfd* poll_fds)
{
	int cur_fds_num = 0;
	int		i;
	int		sock;

	SegmentDatabaseDescriptor	*segdbDesc;

	for (i = 0; i < gp_connections_per_thread ; i++)
	{						/* input available; receive and process it */
		bool		finished ;

		segdbDesc = dbs[i];

		/* Skip if already finished or didn't dispatch. */
		if (!segdbDesc || !segdbDesc->stillRunning)
			continue;

		/* Skip this connection if it has no input available. */
		sock = PQsocket(segdbDesc->conn);
		/*
		 * The fds array is shorter than conn array, so the following
		 * match method will use this assumtion.
		 */
		if (sock >= 0)
			Assert(sock == poll_fds[cur_fds_num].fd);

		if (sock >= 0 && (sock == poll_fds[cur_fds_num].fd))
		{
			cur_fds_num++;
			if (!(poll_fds[cur_fds_num - 1].revents & POLLIN))
				continue;
		}

		/* Receive and process results from this QE. */
		finished = processResultsV1(segdbDesc);

		/* Are we through with this QE now? */
		if (finished)
		{
			segdbDesc->stillRunning = false;
			segdbDesc->dispatched = false;
			if (PQisBusy(segdbDesc->conn))
				write_log("We thought we were done, because finished==true, but libpq says we are still busy");
			
		}
		else
			if (DEBUG4 >= log_min_messages)
				write_log("processResults says we have more to do with %d: %s",i+1,segdbDesc->whoami);
	}						/* input available; receive and process it */
}

void
cdbdisp_appendResultV1(SegmentDatabaseDescriptor *segdbDesc,
                     struct pg_result  *res)
{
    Assert(segdbDesc && res);

    /* Attach the QE identification string to the PGresult */
    if (segdbDesc &&
        segdbDesc->whoami)
        pqSaveMessageField(res, PG_DIAG_GP_PROCESS_TAG,
                           segdbDesc->whoami);

    appendBinaryPQExpBuffer(segdbDesc->resultbuf, (char *)&res, sizeof(res));
}

void CdbCheckDispatchResultInt_V1(struct CdbDispatcherState *ds)
{
	int			i;

	/*
	 * Wait for threads to finish.
	 */
	for (i = 0; i < ThreadNeeded; i++)
	{							/* loop over threads */		
		pthread_t fd = ThreadHandles[i];

		if (fd > 0)
		{
			int			pthread_err = 0;
			pthread_err = pthread_join(fd, NULL);
			if (pthread_err != 0)
				elog(FATAL, "CheckDispatchResult: pthread_join failed on thread %d (%lu) of %d (returned %d attempting to join to %lu)",
					 i + 1, fd, 
						 ThreadNeeded, pthread_err, (unsigned long)mythread());
		}

		HOLD_INTERRUPTS();
		ThreadHandles[i] = 0;
		RESUME_INTERRUPTS();

	}							/* loop over threads */

	/* reset thread state (will be destroyed later on in finishCommand) */
	ds->dispatchThreads->threadCount = 0;
			
}

int getThreadCount(void)
{
	return ThreadNeeded;
}
