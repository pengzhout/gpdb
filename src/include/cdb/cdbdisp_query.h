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
struct CdbDispatcherState*
cdbdisp_dispatchPlan(struct QueryDesc *queryDesc,
					 bool planRequiresTxn,
					 bool cancelOnError);

/*
 * Special for sending SET commands that change GUC variables, so they go to all
 * gangs, both reader and writer
 */
void
CdbSetGucOnAllGangs(const char *strCommand, bool cancelOnError, bool needTwoPhase);


/*
 *****************************************************************

 * Execute Command On Primary Writer Gang
 *
 *****************************************************************
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

/*
 *************************************************************************

 * Execute Parsed Utility Statement On Primary Writer Gang
 *
 *************************************************************************
 */

#define CdbDoUtility_SNAPSHOT(stmt, debugCaller)\
	CdbExecUtilityStatementOnQEs(stmt, false, false, true, debugCaller)

#define CdbDoUtility_COE_SNAPSHOT(stmt, debugCaller)\
	CdbExecUtilityStatementOnQEs(stmt, true, false, true, debugCaller)

#define CdbDoUtility_COE_2PC_SNAPSHOT(stmt, debugCaller)\
	CdbExecUtilityStatementOnQEs(stmt, true, true, true, debugCaller)

#define CdbDoUtility_2PC_SNAPSHOT(stmt, debugCaller)\
	CdbExecUtilityStatementOnQEs(stmt, false, true, true, debugCaller)

#define CdbDoUtilityV1_COE_2PC_SNAPSHOT(stmt, numResultPtr, debugCaller)\
	CdbExecUtilityStatementOnQEsExpectResults(stmt, true, true, true, numResultPtr, debugCaller)
/* 
 * CdbExecUtilityStatementOnQEs
 *
 * Dispatch a command - already parsed and in the form of a Node
 * tree to all primary writer segdbs, and wait for completion.
 * If not all QEs in the given gang(s) executed the command successfully,
 * throws an error and does not return.
 */

void
CdbExecUtilityStatementOnQEs(Node* stmt,
							 bool cancelOnError,
							 bool needTwoPhase,
							 bool withSnapshot,
							 char* debugCaller __attribute__((unused)));

/*
 * CdbExecUtilityCommandOnQEsExpectResults:
 *
 * Dispath plain command to all primary writer QEs, wait until all QEs finish
 * the execution successfully. If one or more QEs got error, throw a Error.
 *
 * This function is used to execute command like 'select pg_relation_size()',
 * caller normally need to further process the result of each QEs.
 */
struct pg_result**
CdbExecUtilityStatementOnQEsExpectResults(Node* stmt,
										  bool cancelOnError,
										  bool needTwoPhase,
										  bool withSnapshot,
										  int* numResults,
										  char* debugCaller __attribute__((unused)));


#endif   /* CDBDISP_QUERY_H */
