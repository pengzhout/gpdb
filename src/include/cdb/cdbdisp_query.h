/*-------------------------------------------------------------------------
 *
 * cdbdisp_query.h
 * routines for dispatching command string or plan to the qExec processes.
 *
 * Copyright (c) 2005-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBDISP_QUERY_H
#define CDBDISP_QUERY_H

#include "lib/stringinfo.h" /* StringInfo */
#include "cdb/cdbtm.h"

struct QueryDesc;
struct CdbDispatcherState;

/* Compose and dispatch the MPPEXEC commands corresponding to a plan tree
 * within a complete parallel plan.
 *
 * The CdbDispatchResults objects allocated for the plan are
 * returned in *pPrimaryResults
 * The caller, after calling CdbCheckDispatchResult(), can
 * examine the CdbDispatchResults objects, can keep them as
 * long as needed, and ultimately must free them with
 * cdbdisp_destroyDispatchState() prior to deallocation
 * of the caller's memory context.
 *
 * NB: Callers should use PG_TRY()/PG_CATCH() if needed to make
 * certain that the CdbDispatchResults objects are destroyed by
 * cdbdisp_destroyDispatchState() in case of error.
 * To wait for completion, check for errors, and clean up, it is
 * suggested that the caller use cdbdisp_finishCommand().
 */
void
cdbdisp_dispatchPlan(struct QueryDesc *queryDesc,
					 bool planRequiresTxn,
					 bool cancelOnError,
					 struct CdbDispatcherState *ds);

/*
 * Special for sending SET commands that change GUC variables, so they go to all
 * gangs, both reader and writer
 */
void
CdbSetGucOnAllGangs(const char *strCommand, bool cancelOnError, bool needTwoPhase);

/*
 * cdbdisp_dispatchCommand:
 * Send ths strCommand SQL statement to all segdbs in the cluster
 * cancelOnError indicates whether an error
 * occurring on one of the qExec segdbs should cause all still-executing commands to cancel
 * on other qExecs. Normally this would be true.  The commands are sent over the libpq
 * connections that were established during gang creation.	They are run inside of threads.
 * The number of segdbs handled by any one thread is determined by the
 * guc variable gp_connections_per_thread.
 *
 * The needTwoPhase flag is used to express intent on whether the command to
 * be dispatched should be done inside of a global transaction or not.
 *
 * The CdbDispatchResults objects allocated for the command
 * are returned in *pPrimaryResults
 * The caller, after calling CdbCheckDispatchResult(), can
 * examine the CdbDispatchResults objects, can keep them as
 * long as needed, and ultimately must free them with
 * cdbdisp_destroyDispatchState() prior to deallocation
 * of the memory context from which they were allocated.
 *
 * NB: Callers should use PG_TRY()/PG_CATCH() if needed to make
 * certain that the CdbDispatchResults objects are destroyed by
 * cdbdisp_destroyDispatchState() in case of error.
 * To wait for completion, check for errors, and clean up, it is
 * suggested that the caller use cdbdisp_finishCommand().
 */
void
cdbdisp_dispatchCommand(const char *strCommand,
						char *serializedQuerytree,
						int	serializedQuerytreelen,
						bool cancelOnError,
						bool needTwoPhase,
						bool withSnapshot,
						struct CdbDispatcherState *ds); /* OUT */


/* Dispatch a command - already parsed and in the form of a Node
 * tree - to all primary segdbs.  Does not wait for
 * completion.  Does not start a global transaction.
 *
 *
 * The needTwoPhase flag indicates whether you want the dispatched 
 * statement to participate in a distributed transaction or not.
 *
 * NB: Callers should use PG_TRY()/PG_CATCH() if needed to make
 * certain that the CdbDispatchResults objects are destroyed by
 * cdbdisp_destroyDispatchState() in case of error.
 * To wait for completion, check for errors, and clean up, it is
 * suggested that the caller use cdbdisp_finishCommand().
 */
void
cdbdisp_dispatchUtilityStatement(struct Node *stmt,
								 bool cancelOnError,
								 bool needTwoPhase,
								 bool withSnapshot,
								 struct CdbDispatcherState *ds,
								 char* debugCaller __attribute__((unused)));

/* Dispatch a command - already parsed and in the form of a Node
 * tree - to all primary segdbs, and wait for completion.
 * Starts a global transaction first, if not already started.
 * If not all QEs in the given gang(s) executed the command successfully,
 * throws an error and does not return.
 */
void
CdbDispatchUtilityStatement(struct Node *stmt, char* debugCaller __attribute__((unused)));

void
CdbDispatchUtilityStatement_NoTwoPhase(struct Node *stmt, char* debugCaller __attribute__((unused)));

/*
 ****************************************
 * Execute Command On Primary Writer Gang
 ****************************************
 */
#define CdbDoCommand(strCommand) \
	CdbExecCommandOnQEs(strCommand, false, false, false)

#define CdbDoCommand_SNAPSHOT(strCommand) \
	CdbExecCommandOnQEs(strCommand, false, false, true)

#define CdbDoCommand_COE_SNAPSHOT(strCommand) \
	CdbExecCommandOnQEs(strCommand, true, true, true)

#define CdbDoCommand_2PC_SNAPSHOT(strCommand) \
	CdbExecCommandOnQEs(strCommand, true, true, true)

#define CdbDoCommand_COE_2PC_SNAPSHOT(strCommand) \
	CdbExecCommandOnQEs(strCommand, true, true, true)

#define CdbDoCommandV1(strCommand, resultNumPtr) \
	CdbExecCommandOnQEsExpectResults(strCommand, false, false,\
									 false, resultNumPtr)

#define CdbDoCommandV1_SNAPSHOT(strCommand, resultNumPtr) \
	CdbExecCommandOnQEsExpectResults(strCommand, false, false,\
									 true, resultNumPtr)

/*
 * CdbExecCommandOnQEs
 *
 * Execute plain command on all primary writer QEs.
 * If one or more QEs got error, throw a Error.
 *
 * This function is used to execute command like 'DROP DATABASE IF EXISTS' on
 * each segments, caller don't need to further process the result of each
 * segments.
 */
void
CdbExecCommandOnQEs(const char* strCommand,
					bool cancelOnError,
					bool needTwoPhase,
					bool withSnapshot);


/*
 * CdbExecCommandOnQEsExpectResults
 *
 * Another version of CdbExecCommandOnQEs which return pg_result array
 *
 * This function is used to execute command like 'select pg_relation_size()',
 * caller normally need to further process the result of each QEs.
 */
struct pg_result**
CdbExecCommandOnQEsExpectResults(const char* strCommand,
								 bool cancelOnError,
								 bool needTwoPhase,
								 bool withSnapshot,
								 int* numResults);

#endif   /* CDBDISP_QUERY_H */
