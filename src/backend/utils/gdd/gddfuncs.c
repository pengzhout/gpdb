/*-------------------------------------------------------------------------
 *
 * gddfuncs.c
 *	  Global DeadLock Detector - Helper Functions
 *
 *
 * Copyright (c) 2018-Present Pivotal Software, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbvars.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "storage/proc.h"
#include "utils/builtins.h"

typedef struct GddWaitStatusCtx GddWaitStatusCtx;

struct GddWaitStatusCtx
{
	LockData	*lockData;
	CdbPgResults cdb_pgresults;

	int			waiter;
	int			holder;

	int			seg;
	int			row;
};

static bool isGranted(PROCLOCK *proclock);
static bool lockEqual(LOCK *lock1, LOCK *lock2);
static bool lockIsPersistent(LOCK *lock);

static bool
isGranted(PROCLOCK *proclock)
{
	return proclock->holdMask != 0;
}

static bool
lockEqual(LOCK *lock1, LOCK *lock2)
{
	LOCKTAG		*tag1 = &lock1->tag;
	LOCKTAG		*tag2 = &lock2->tag;

	return memcmp(tag1, tag2, sizeof(LOCKTAG)) == 0;
}

static bool
lockIsPersistent(LOCK *lock)
{
	LOCKTAG		*tag = &lock->tag;

	if (lock->persistent)
		return true;

	if (tag->locktag_type == LOCKTAG_TRANSACTION)
		return true;

	/* FIXME: other types */

	return false;
}

/*
 * pg_dist_wait_status - produce a view with one row per waiting relation
 */
Datum
pg_dist_wait_status(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	HeapTuple	tuple;
	Datum		result;
	Datum		values[4];
	bool		nulls[4];
	GddWaitStatusCtx *ctx;

	MemSet(values, 0, sizeof(values));
	MemSet(nulls, false, sizeof(nulls));

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		tupdesc = CreateTemplateTupleDesc(4, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "segid",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "waiter_dxid",
						   XIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "holder_dxid",
						   XIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "persistent",
						   BOOLOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		ctx = palloc(sizeof(*ctx));
		funcctx->user_fctx = ctx;

		ctx->cdb_pgresults.pg_results = NULL;
		ctx->cdb_pgresults.numResults = 0;
		ctx->waiter = 0;
		ctx->holder = 0;
		ctx->seg = 0;
		ctx->row = 0;

		/*
		 * We need to collect the wait status from all the segments,
		 * so a dispatch is needed on QD.
		 */

		if (Gp_role == GP_ROLE_DISPATCH)
		{
			int			i;

			/*
			 * GPDB_84_MERGE_FIXME: Should we rewrite this in a different way now that we have
			 * ON SEGMENT/ ON MASTER attributes on functions?
			 */
			CdbDispatchCommand("SELECT * FROM pg_catalog.pg_dist_wait_status()",
							   DF_WITH_SNAPSHOT, &ctx->cdb_pgresults);

			if (ctx->cdb_pgresults.numResults == 0)
				elog(ERROR, "pg_dist_wait_status() didn't get back any data from the segDBs");

			for (i = 0; i < ctx->cdb_pgresults.numResults; i++)
			{
				/*
				 * Any error here should have propagated into errbuf,
				 * so we shouldn't ever see anything other that tuples_ok here.
				 * But, check to be sure.
				 */
				if (PQresultStatus(ctx->cdb_pgresults.pg_results[i]) != PGRES_TUPLES_OK)
				{
					cdbdisp_clearCdbPgResults(&ctx->cdb_pgresults);
					elog(ERROR,"pg_dist_wait_status(): resultStatus not tuples_Ok");
				}

				/*
				 * This query better match the tupledesc we just made above.
				 */
				if (PQnfields(ctx->cdb_pgresults.pg_results[i]) != tupdesc->natts)
					elog(ERROR, "unexpected number of columns returned from pg_lock_status() on segment (%d, expected %d)",
						 PQnfields(ctx->cdb_pgresults.pg_results[i]), tupdesc->natts);
			}
		}

		/*
		 * QD information must be collected after the dispatch.
		 * Thus the wait relations on QD returned by this function
		 * guarantee to be more later than the wait relations on QEs.
		 * And this information can help while reducing edges on QD.
		 *
		 * Rule: When we are to reduce a dotted edge on QD, we have to think
		 * more. If the lock-holding vertex(transaction) of this QD-edge,
		 * is blocked on any QE. We should not reduce the edge at least
		 * now.
		 *
		 * If any QEs wait relation is later than QD's, we cannot use the
		 * above rule.
		 */
		ctx->lockData = GetLockStatusData();

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	ctx = funcctx->user_fctx;

	/*
	 * Find out all the waiting relations
	 *
	 * A relation is that:
	 * (waiter.granted == false &&
	 *  holder.granted == true &&
	 *  waiter.pid != holder.pid &&
	 *  waiter.locktype == holder.locktype &&
	 *  waiter.locktags == holder.locktags)
	 *
	 * The actual logic here is to perform a nested loop like this:
	 *
	 *     for (waiter = 0; waiter < nelements; waiter++)
	 *         for (holder = 0; holder < nelements; holder++)
	 *             if (is_a_relation(waiter, holder))
	 *                 record the relation;
	 *
	 * But as we can only return one relation per call, we need to
	 * simulate the loops manually.
	 */
	while (ctx->waiter < ctx->lockData->nelements)
	{
		int			waiter = ctx->waiter;
		PROCLOCK   *w_proclock = &ctx->lockData->proclocks[waiter];
		LOCK	   *w_lock = &ctx->lockData->locks[waiter];
		PGPROC	   *w_proc = &ctx->lockData->procs[waiter];

		/* A waiter should have granted == false */
		if (isGranted(w_proclock))
		{
			ctx->waiter++;
			ctx->holder = 0;
			continue;
		}

		while (ctx->holder < ctx->lockData->nelements)
		{
			TransactionId w_dxid;
			TransactionId h_dxid;
			int			holder = ctx->holder++;
			PROCLOCK   *h_proclock = &ctx->lockData->proclocks[holder];
			LOCK	   *h_lock = &ctx->lockData->locks[holder];
			PGPROC	   *h_proc = &ctx->lockData->procs[holder];

			if (holder == waiter)
				continue;
			/* A holder should have granted == true */
			if (!isGranted(h_proclock))
				continue;
			if (w_proc->pid == h_proc->pid)
				continue;
			if (!lockEqual(w_lock, h_lock))
				continue;

			/* A valid waiting relation is found */

			/* Find out dxid differently on QD and QE */
			if (Gp_role == GP_ROLE_DISPATCH)
			{
				w_dxid = w_proc->gxact.gxid;
				h_dxid = h_proc->gxact.gxid;
			}
			else
			{
				w_dxid = w_proc->localDistribXactData.distribXid;
				h_dxid = h_proc->localDistribXactData.distribXid;
			}

			values[0] = Int32GetDatum(Gp_segment);
			values[1] = TransactionIdGetDatum(w_dxid);
			values[2] = TransactionIdGetDatum(h_dxid);
			values[3] = BoolGetDatum(lockIsPersistent(w_lock));

			tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
			result = HeapTupleGetDatum(tuple);

			SRF_RETURN_NEXT(funcctx, result);
		}

		ctx->waiter++;
		ctx->holder = 0;
	}

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		while (ctx->seg < ctx->cdb_pgresults.numResults)
		{
			int seg = ctx->seg;
			struct pg_result *pg_result = ctx->cdb_pgresults.pg_results[seg];

			while (ctx->row < PQntuples(pg_result))
			{
				int row = ctx->row++;

				values[0] = Int32GetDatum(atoi(PQgetvalue(pg_result, row, 0)));
				values[1] = TransactionIdGetDatum(atoi(PQgetvalue(pg_result, row, 1)));
				values[2] = TransactionIdGetDatum(atoi(PQgetvalue(pg_result, row, 2)));
				values[3] = BoolGetDatum(strncmp(PQgetvalue(pg_result, row, 3), "t", 1) == 0);

				tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
				result = HeapTupleGetDatum(tuple);
				SRF_RETURN_NEXT(funcctx, result);
			}

			ctx->seg++;
			ctx->row = 0;
		}
	}

	cdbdisp_clearCdbPgResults(&ctx->cdb_pgresults);

	SRF_RETURN_DONE(funcctx);
}
