#include "postgres.h"

#include "access/xact.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "cdb/cdbconn.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbmutate.h"
#include "cdb/cdbsrlz.h"
#include "cdb/tupleremap.h"
#include "nodes/execnodes.h"
#include "tcop/tcopprot.h"
#include "utils/datum.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/faultinjector.h"
#include "utils/resgroup.h"
#include "utils/resource_manager.h"
#include "utils/session_state.h"
#include "utils/typcache.h"
#include "miscadmin.h"

#include "cdb/cdbdisp.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdisp_thread.h" /* for CdbDispatchCmdThreads and
								 * DispatchCommandParms */
#include "cdb/cdbdisp_dtx.h"	/* for qdSerializeDtxContextInfo() */
#include "cdb/cdbdispatchresult.h"

#include "cdb/cdbdisp_api.h"
#include "access/appendonlywriter.h"
#include "cdb/cdbsreh.h"

static int64 dispatcher_sumCmdTuples(List *results, int sliceIndex);
static HTAB * dispatcher_sumAoPartTupCount(PartitionNode *parts, List *results);
static void dispatcher_sumRejectedRows(List *results);
static Oid dispatcher_maxLastOid(List *results, int sliceIndex);

static void ExecUpdateAOtupCount(ResultRelInfo *result_rels,
					 Snapshot shapshot,
					 int num_result_rels,
					 EState* estate,
					 uint64 tupadded);

/*
 * Parameter structure for DTX protocol commands
 */
typedef struct DispatchCommandDtxProtocolParms
{
	DtxProtocolCommand dtxProtocolCommand;
	int			flags;
	char	   *dtxProtocolCommandLoggingStr;
	char		gid[TMGIDSIZE];
	DistributedTransactionId gxid;
	char	   *serializedDtxContextInfo;
	int			serializedDtxContextInfoLen;
} DispatchCommandDtxProtocolParms;


/*
 * Parameter structure for Greenplum Database Queries
 */
typedef struct DispatchCommandQueryParms
{
	/*
	 * The SQL command
	 */
	const char *strCommand;
	int			strCommandlen;
	char	   *serializedQuerytree;
	int			serializedQuerytreelen;
	char	   *serializedPlantree;
	int			serializedPlantreelen;
	char	   *serializedQueryDispatchDesc;
	int			serializedQueryDispatchDesclen;
	char	   *serializedParams;
	int			serializedParamslen;

	/*
	 * Additional information.
	 */
	char	   *serializedOidAssignments;
	int			serializedOidAssignmentslen;

	/*
	 * serialized DTX context string
	 */
	char	   *serializedDtxContextInfo;
	int			serializedDtxContextInfolen;

	int			rootIdx;

	/*
	 * the sequence server info.
	 */
	char	   *seqServerHost;	/* If non-null, sequence server host name. */
	int			seqServerHostlen;
	int			seqServerPort;	/* If seqServerHost non-null, sequence server
								 * port. */

	/* the map from sliceIndex to gang_id, in array form */
	int			numSlices;
	int		   *sliceIndexGangIdMap;
} DispatchCommandQueryParms;

static DispatchCommandQueryParms * buildGpCommandQueryParms(const char *strCommand,
				char *serializedQuerytree,
				int serializedQuerytreelen,
				char *serializedQueryDispatchDesc,
				int serializedQueryDispatchDesclen,
				int flags);

static char * formatDispatchQueryText(const char *strCommand, 
					char *serializedQuerytree,
					int serializedQuerytreelen,
					char *serializedQueryDispatchDesc,
					int serializedQueryDispatchDesclen,
					int flag, int *formatQueryLen);

static char *buildGpDtxProtocolCommand(DispatchCommandDtxProtocolParms *pDtxProtocolParms,
						  int *finalLen);

/*
 * Serialization of query parameters (ParamListInfos).
 *
 * When a query is dispatched from QD to QE, we also need to dispatch any
 * query parameters, contained in the ParamListInfo struct. We need to
 * serialize ParamListInfo, but there are a few complications:
 *
 * - ParamListInfo is not a Node type, so we cannot use the usual
 * nodeToStringBinary() function directly. We turn the array of
 * ParamExternDatas into a List of SerializedParamExternData nodes,
 * which we can then pass to nodeToStringBinary().
 *
 * - The paramFetch callback, which could be used in this process to fetch
 * parameter values on-demand, cannot be used in a different process.
 * Therefore, fetch all parameters before serializing them. When
 * deserializing, leave the callbacks NULL.
 *
 * - In order to deserialize correctly, the receiver needs the typlen and
 * typbyval information for each datatype. The receiver has access to the
 * catalogs, so it could look them up, but for the sake of simplicity and
 * robustness in the receiver, we include that information in
 * SerializedParamExternData.
 *
 * - RECORD types. Type information of transient record is kept only in
 * backend private memory, indexed by typmod. The recipient will not know
 * what a record type's typmod means. And record types can also be nested.
 * Because of that, if there are any RECORD, we include a copy of the whole
 * transient record type cache.
 *
 * If there are no record types involved, we dispatch a list of
 * SerializedParamListInfos, i.e.
 *
 * List<SerializedParamListInfo>
 *
 * With record types, we dispatch:
 *
 * List(List<TupleDescNode>, List<SerializedParamListInfo>)
 *
 * XXX: Sending *all* record types can be quite bulky, but ATM there is no
 * easy way to extract just the needed record types.
 */
static char *
serializeParamListInfo(ParamListInfo paramLI, int *len_p)
{
	int			i;
	List	   *sparams;
	bool		found_records = false;

	/* Construct a list of SerializedParamExternData */
	sparams = NIL;
	for (i = 0; i < paramLI->numParams; i++)
	{
		ParamExternData *prm = &paramLI->params[i];
		SerializedParamExternData *sprm;

		/*
		 * First, use paramFetch to fetch any "lazy" parameters. (The callback
		 * function is of no use in the QE.)
		 */
		if (paramLI->paramFetch && !OidIsValid(prm->ptype))
			(*paramLI->paramFetch) (paramLI, i + 1);

		sprm = makeNode(SerializedParamExternData);

		sprm->value = prm->value;
		sprm->isnull = prm->isnull;
		sprm->pflags = prm->pflags;
		sprm->ptype = prm->ptype;

		if (OidIsValid(prm->ptype))
		{
			get_typlenbyval(prm->ptype, &sprm->plen, &sprm->pbyval);

			if (prm->ptype == RECORDOID && !prm->isnull)
			{
				/*
				 * Note: We don't want to use lookup_rowtype_tupdesc_copy here, because
				 * it copies defaults and constraints too. We don't want those.
				 */
				found_records = true;
			}
		}
		else
		{
			sprm->plen = 0;
			sprm->pbyval = true;
		}

		sparams = lappend(sparams, sprm);
	}

	/*
	 * If there were any record types, include the transient record type cache.
	 */
	if (found_records)
		sparams = lcons(build_tuple_node_list(0), sparams);

	return nodeToBinaryStringFast(sparams, len_p);
}


static DispatchCommandQueryParms *
cdbdisp_buildPlanQueryParms(struct QueryDesc *queryDesc,
							bool planRequiresTxn)
{
	char	   *splan,
			   *sddesc,
			   *sparams;

	int			splan_len,
				splan_len_uncompressed,
				sddesc_len,
				sparams_len,
				rootIdx;

	rootIdx = RootSliceIndex(queryDesc->estate);

#ifdef USE_ASSERT_CHECKING
	SliceTable *sliceTbl = queryDesc->estate->es_sliceTable;

	Assert(rootIdx == 0 ||
		   (rootIdx > sliceTbl->nMotions
			&& rootIdx <= sliceTbl->nMotions + sliceTbl->nInitPlans));
#endif

	CdbComponentDatabaseInfo *qdinfo;

	DispatchCommandQueryParms *pQueryParms = (DispatchCommandQueryParms *) palloc0(sizeof(*pQueryParms));

	/*
	 * serialized plan tree. Note that we're called for a single slice tree
	 * (corresponding to an initPlan or the main plan), so the parameters are
	 * fixed and we can include them in the prefix.
	 */
	splan = serializeNode((Node *) queryDesc->plannedstmt, &splan_len, &splan_len_uncompressed);

	uint64		plan_size_in_kb = ((uint64) splan_len_uncompressed) / (uint64) 1024;

	elog(((gp_log_gang >= GPVARS_VERBOSITY_TERSE) ? LOG : DEBUG1),
		 "Query plan size to dispatch: " UINT64_FORMAT "KB", plan_size_in_kb);

	if (0 < gp_max_plan_size && plan_size_in_kb > gp_max_plan_size)
	{
		ereport(ERROR,
				(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
				 (errmsg("Query plan size limit exceeded, current size: "
						 UINT64_FORMAT "KB, max allowed size: %dKB",
						 plan_size_in_kb, gp_max_plan_size),
				  errhint("Size controlled by gp_max_plan_size"))));
	}

	Assert(splan != NULL && splan_len > 0 && splan_len_uncompressed > 0);

	if (queryDesc->params != NULL && queryDesc->params->numParams > 0)
	{
		sparams = serializeParamListInfo(queryDesc->params, &sparams_len);
	}
	else
	{
		sparams = NULL;
		sparams_len = 0;
	}

	sddesc = serializeNode((Node *) queryDesc->ddesc, &sddesc_len, NULL /* uncompressed_size */ );

	pQueryParms->strCommand = queryDesc->sourceText;
	pQueryParms->serializedQuerytree = NULL;
	pQueryParms->serializedQuerytreelen = 0;
	pQueryParms->serializedPlantree = splan;
	pQueryParms->serializedPlantreelen = splan_len;
	pQueryParms->serializedParams = sparams;
	pQueryParms->serializedParamslen = sparams_len;
	pQueryParms->serializedQueryDispatchDesc = sddesc;
	pQueryParms->serializedQueryDispatchDesclen = sddesc_len;
	pQueryParms->rootIdx = rootIdx;

	/*
	 * sequence server info
	 */
	qdinfo = &(getComponentDatabases()->entry_db_info[0]);
	Assert(qdinfo != NULL && qdinfo->hostip != NULL);
	pQueryParms->seqServerHost = pstrdup(qdinfo->hostip);
	pQueryParms->seqServerHostlen = strlen(qdinfo->hostip) + 1;
	pQueryParms->seqServerPort = seqServerCtl->seqServerPort;

	/*
	 * Serialize a version of our snapshot, and generate our transction
	 * isolations. We generally want Plan based dispatch to be in a global
	 * transaction. The executor gets to decide if the special circumstances
	 * exist which allow us to dispatch without starting a global xact.
	 */
	pQueryParms->serializedDtxContextInfo =
		qdSerializeDtxContextInfo(&pQueryParms->serializedDtxContextInfolen,
								  true /* wantSnapshot */ ,
								  queryDesc->extended_query,
								  mppTxnOptions(planRequiresTxn),
								  "cdbdisp_buildPlanQueryParms");

	return pQueryParms;
}
static void
preDispatchPlan(struct QueryDesc *queryDesc)
{
	PlannedStmt	*stmt;
	bool		is_SRI = false;

	/*
	 * This function is called only for planned statements.
	 */
	stmt = queryDesc->plannedstmt;
	Assert(stmt);

	/*
	 * Let's evaluate STABLE functions now, so we get consistent values on the
	 * QEs
	 *
	 * Also, if this is a single-row INSERT statement, let's evaluate
	 * nextval() and currval() now, so that we get the QD's values, and a
	 * consistent value for everyone
	 */
	if (queryDesc->operation == CMD_INSERT)
	{
		Assert(stmt->commandType == CMD_INSERT);

		/*
		 * We might look for constant input relation (instead of SRI), but I'm
		 * afraid that wouldn't scale.
		 */
		is_SRI = IsA(stmt->planTree, Result) &&stmt->planTree->lefttree == NULL;
	}

	if (queryDesc->operation == CMD_INSERT ||
			queryDesc->operation == CMD_SELECT ||
			queryDesc->operation == CMD_UPDATE ||
			queryDesc->operation == CMD_DELETE)
	{
		List	   *cursors;

		/*
		 * Need to be careful not to modify the original PlannedStmt, because
		 * it might be a cached plan. So make a copy. A shallow copy of the
		 * fields we don't modify should be enough.
		 */
		stmt = palloc(sizeof(PlannedStmt));
		memcpy(stmt, queryDesc->plannedstmt, sizeof(PlannedStmt));
		stmt->subplans = list_copy(stmt->subplans);

		stmt->planTree = (Plan *) exec_make_plan_constant(stmt, queryDesc->estate, is_SRI, &cursors);
		queryDesc->plannedstmt = stmt;

		queryDesc->ddesc->cursorPositions = (List *) copyObject(cursors);
	}
}

/*
 * Build a query string to be dispatched to QE.
 */
static char *
dispatcher_buildGpQueryString(DispatchCommandQueryParms *pQueryParms,
				   int *finalLen)
{
	const char *command = pQueryParms->strCommand;
	int			command_len = strlen(pQueryParms->strCommand) + 1;
	const char *querytree = pQueryParms->serializedQuerytree;
	int			querytree_len = pQueryParms->serializedQuerytreelen;
	const char *plantree = pQueryParms->serializedPlantree;
	int			plantree_len = pQueryParms->serializedPlantreelen;
	const char *params = pQueryParms->serializedParams;
	int			params_len = pQueryParms->serializedParamslen;
	const char *sddesc = pQueryParms->serializedQueryDispatchDesc;
	int			sddesc_len = pQueryParms->serializedQueryDispatchDesclen;
	const char *dtxContextInfo = pQueryParms->serializedDtxContextInfo;
	int			dtxContextInfo_len = pQueryParms->serializedDtxContextInfolen;
	int			flags = 0;		/* unused flags */
	int			rootIdx = pQueryParms->rootIdx;
	const char *seqServerHost = pQueryParms->seqServerHost;
	int			seqServerHostlen = pQueryParms->seqServerHostlen;
	int			seqServerPort = pQueryParms->seqServerPort;
	int			numSlices = pQueryParms->numSlices;
	int		   *sliceIndexGangIdMap = pQueryParms->sliceIndexGangIdMap;
	int64		currentStatementStartTimestamp = GetCurrentStatementStartTimestamp();
	Oid			sessionUserId = GetSessionUserId();
	Oid			outerUserId = GetOuterUserId();
	Oid			currentUserId = GetUserId();
	StringInfoData resgroupInfo;

	int			tmp,
				len,
				i;
	uint32		n32;
	int			total_query_len;
	char	   *shared_query,
			   *pos;

	initStringInfo(&resgroupInfo);
	if (IsResGroupActivated())
		SerializeResGroupInfo(&resgroupInfo);

	total_query_len = 1 /* 'M' */ +
		sizeof(len) /* message length */ +
		sizeof(gp_command_count) +
		sizeof(sessionUserId) /* sessionUserIsSuper */ +
		sizeof(outerUserId) /* outerUserIsSuper */ +
		sizeof(currentUserId) +
		sizeof(rootIdx) +
		sizeof(n32) * 2 /* currentStatementStartTimestamp */ +
		sizeof(command_len) +
		sizeof(querytree_len) +
		sizeof(plantree_len) +
		sizeof(params_len) +
		sizeof(sddesc_len) +
		sizeof(dtxContextInfo_len) +
		dtxContextInfo_len +
		sizeof(flags) +
		sizeof(seqServerHostlen) +
		sizeof(seqServerPort) +
		command_len +
		querytree_len +
		plantree_len +
		params_len +
		sddesc_len +
		seqServerHostlen +
		sizeof(numSlices) +
		sizeof(int) * numSlices +
		sizeof(resgroupInfo.len) +
		resgroupInfo.len + sizeof(int);


	shared_query = palloc0(total_query_len);

	pos = shared_query;

	*pos++ = 'M';

	pos += 4;					/* placeholder for message length */

	tmp = htonl(gp_command_count);
	memcpy(pos, &tmp, sizeof(gp_command_count));
	pos += sizeof(gp_command_count);

	tmp = htonl(sessionUserId);
	memcpy(pos, &tmp, sizeof(sessionUserId));
	pos += sizeof(sessionUserId);

	tmp = htonl(outerUserId);
	memcpy(pos, &tmp, sizeof(outerUserId));
	pos += sizeof(outerUserId);

	tmp = htonl(currentUserId);
	memcpy(pos, &tmp, sizeof(currentUserId));
	pos += sizeof(currentUserId);

	tmp = htonl(rootIdx);
	memcpy(pos, &tmp, sizeof(rootIdx));
	pos += sizeof(rootIdx);

	/*
	 * High order half first, since we're doing MSB-first
	 */
	n32 = (uint32) (currentStatementStartTimestamp >> 32);
	n32 = htonl(n32);
	memcpy(pos, &n32, sizeof(n32));
	pos += sizeof(n32);

	/*
	 * Now the low order half
	 */
	n32 = (uint32) currentStatementStartTimestamp;
	n32 = htonl(n32);
	memcpy(pos, &n32, sizeof(n32));
	pos += sizeof(n32);

	tmp = htonl(command_len);
	memcpy(pos, &tmp, sizeof(command_len));
	pos += sizeof(command_len);

	tmp = htonl(querytree_len);
	memcpy(pos, &tmp, sizeof(querytree_len));
	pos += sizeof(querytree_len);

	tmp = htonl(plantree_len);
	memcpy(pos, &tmp, sizeof(plantree_len));
	pos += sizeof(plantree_len);

	tmp = htonl(params_len);
	memcpy(pos, &tmp, sizeof(params_len));
	pos += sizeof(params_len);

	tmp = htonl(sddesc_len);
	memcpy(pos, &tmp, sizeof(tmp));
	pos += sizeof(tmp);

	tmp = htonl(dtxContextInfo_len);
	memcpy(pos, &tmp, sizeof(tmp));
	pos += sizeof(tmp);

	if (dtxContextInfo_len > 0)
	{
		memcpy(pos, dtxContextInfo, dtxContextInfo_len);
		pos += dtxContextInfo_len;
	}

	tmp = htonl(flags);
	memcpy(pos, &tmp, sizeof(tmp));
	pos += sizeof(tmp);

	tmp = htonl(seqServerHostlen);
	memcpy(pos, &tmp, sizeof(tmp));
	pos += sizeof(tmp);

	tmp = htonl(seqServerPort);
	memcpy(pos, &tmp, sizeof(tmp));
	pos += sizeof(tmp);

	memcpy(pos, command, command_len);
	pos += command_len;

	if (querytree_len > 0)
	{
		memcpy(pos, querytree, querytree_len);
		pos += querytree_len;
	}

	if (plantree_len > 0)
	{
		memcpy(pos, plantree, plantree_len);
		pos += plantree_len;
	}

	if (params_len > 0)
	{
		memcpy(pos, params, params_len);
		pos += params_len;
	}

	if (sddesc_len > 0)
	{
		memcpy(pos, sddesc, sddesc_len);
		pos += sddesc_len;
	}

	if (seqServerHostlen > 0)
	{
		memcpy(pos, seqServerHost, seqServerHostlen);
		pos += seqServerHostlen;
	}

	tmp = htonl(numSlices);
	memcpy(pos, &tmp, sizeof(tmp));
	pos += sizeof(tmp);

	if (numSlices > 0)
	{
		for (i = 0; i < numSlices; ++i)
		{
			tmp = htonl(sliceIndexGangIdMap[i]);
			memcpy(pos, &tmp, sizeof(tmp));
			pos += sizeof(tmp);
		}
	}

	tmp = htonl(resgroupInfo.len);
	memcpy(pos, &tmp, sizeof(resgroupInfo.len));
	pos += sizeof(resgroupInfo.len);

	if (resgroupInfo.len > 0)
	{
		memcpy(pos, resgroupInfo.data, resgroupInfo.len);
		pos += resgroupInfo.len;
	}

	/*
	 * a placeholder for sliceIndex
	 */
	int dummy_sliceId = 777;
	tmp = htonl(dummy_sliceId);
	memcpy(pos, &tmp, sizeof(dummy_sliceId));
	pos += sizeof(dummy_sliceId);

	len = pos - shared_query - 1;

	/*
	 * fill in length placeholder
	 */
	tmp = htonl(len);
	memcpy(shared_query + 1, &tmp, sizeof(len));

	Assert(len + 1 == total_query_len);

	if (finalLen)
		*finalLen = len + 1;

	return shared_query;
}

/*
 * Free memory allocated in DispatchCommandQueryParms
 */
static void
cdbdisp_destroyQueryParms(DispatchCommandQueryParms *pQueryParms)
{
	if (pQueryParms == NULL)
		return;

	Assert(pQueryParms->strCommand != NULL);

	if (pQueryParms->serializedQuerytree != NULL)
	{
		pfree(pQueryParms->serializedQuerytree);
		pQueryParms->serializedQuerytree = NULL;
	}

	if (pQueryParms->serializedPlantree != NULL)
	{
		pfree(pQueryParms->serializedPlantree);
		pQueryParms->serializedPlantree = NULL;
	}

	if (pQueryParms->serializedParams != NULL)
	{
		pfree(pQueryParms->serializedParams);
		pQueryParms->serializedParams = NULL;
	}

	if (pQueryParms->serializedQueryDispatchDesc != NULL)
	{
		pfree(pQueryParms->serializedQueryDispatchDesc);
		pQueryParms->serializedQueryDispatchDesc = NULL;
	}

	if (pQueryParms->serializedDtxContextInfo != NULL)
	{
		pfree(pQueryParms->serializedDtxContextInfo);
		pQueryParms->serializedDtxContextInfo = NULL;
	}

	if (pQueryParms->seqServerHost != NULL)
	{
		pfree(pQueryParms->seqServerHost);
		pQueryParms->seqServerHost = NULL;
	}

	if (pQueryParms->sliceIndexGangIdMap != NULL)
	{
		pfree(pQueryParms->sliceIndexGangIdMap);
		pQueryParms->sliceIndexGangIdMap = NULL;
	}

	pfree(pQueryParms);
}
/*
 * -----------------------------------------------------------------
 * Dispatch a Command: API_DispatchPlan
 * -----------------------------------------------------------------
 */
void
API_DispatchPlanStart(struct QueryDesc *queryDesc,
			bool planRequiresTxn,
			bool cancelOnError)
{
	DispatchCommandQueryParms	*pQueryParms;
	char				*queryText;
	int				queryTextLen;

	Assert(Gp_role == GP_ROLE_DISPATCH);
	Assert(queryDesc != NULL && queryDesc->estate != NULL);

	preDispatchPlan(queryDesc);

	/*
	 * Cursor queries and bind/execute path queries don't run on the
	 * writer-gang QEs; but they require snapshot-synchronization to get
	 * started.
	 *
	 * initPlans, and other work (see the function pre-evaluation above) may
	 * advance the snapshot "segmateSync" value, so we're best off setting the
	 * shared-snapshot-ready value here. This will dispatch to the writer gang
	 * and force it to set its snapshot; we'll then be able to serialize the
	 * same snapshot version (see qdSerializeDtxContextInfo() below).
	 *
	 */
	if (queryDesc->extended_query)
	{
		verify_shared_snapshot_ready();
	}

	DispatcherState_Connect();

	CurrentDispatcherState->queryDesc = queryDesc;

	/*
	 * Allocate gangs and associate it with slicetable, this muse be done before
	 * cdbdisp_buildPlanQueryParms
	 */
	DispatcherState_AssignGangs(queryDesc->estate->es_sliceTable, RootSliceIndex(queryDesc->estate), queryDesc->extended_query);

	pQueryParms = cdbdisp_buildPlanQueryParms(queryDesc, planRequiresTxn);
	queryText = dispatcher_buildGpQueryString(pQueryParms, &queryTextLen);

	DispatcherState_DispatchCommandToGangs(queryText, queryTextLen, queryDesc->estate->es_sliceTable, RootSliceIndex(queryDesc->estate));

	cdbdisp_destroyQueryParms(pQueryParms);
}


/*
 * -----------------------------------------------------------------
 * Dispatch a Command: API_DispatchDtxCommand
 * -----------------------------------------------------------------
 */

/*
 * -----------------------------------------------------------------
 * Dispatch a Command: API_DispatchCommand or API_DispatchUtility
 * -----------------------------------------------------------------
 */
void
API_DispatchCommand(const char* queryText, 
			char *serializedQuerytree,
			int serializedQuerytreelen,
			char *serializedQueryDispatchDesc,
			int serializedQueryDispatchDesclen,
			int flags, CdbPgResults *cdb_pgresults)
{
	char		*formatQuery;
	int		formatQueryLen;
	bool		needTwoPhase = flags & DF_NEED_TWO_PHASE;
	bool		withSnapshot = flags & DF_WITH_SNAPSHOT;

	dtmPreCommand("cdbdisp_dispatchCommandInternal", queryText,
				  NULL, needTwoPhase, withSnapshot,
				  false /* inCursor */ );

	formatQuery = formatDispatchQueryText(queryText, serializedQuerytree, serializedQuerytreelen,
						serializedQueryDispatchDesc, serializedQueryDispatchDesclen, flags, &formatQueryLen);

	DispatcherState_Connect();

	PG_TRY();
	{
		DispatcherState_DispatchCommand(formatQuery, formatQueryLen);
		DispatcherState_Join(DISPATCHER_WAIT_BLOCK);
	}
	PG_CATCH();
	{
		DispatcherState_Join(DISPATCHER_WAIT_CANCEL);
	}
	PG_END_TRY();

	DispatcherState_GetResults(cdb_pgresults);

	DispatcherState_Close();
}

static char *
formatDispatchQueryText(const char *strCommand,
			char *serializedQuerytree,
			int serializedQuerytreelen,
			char *serializedQueryDispatchDesc,
			int serializedQueryDispatchDesclen,
			int flag, int *formatQueryLen)
{
	DispatchCommandQueryParms       *pQueryParms;
	char                            *queryText;

	pQueryParms = buildGpCommandQueryParms(strCommand, serializedQuerytree, serializedQuerytreelen,
						serializedQueryDispatchDesc, serializedQueryDispatchDesclen, flag);

	queryText = dispatcher_buildGpQueryString(pQueryParms, formatQueryLen);

	return queryText;
}


static DispatchCommandQueryParms *
buildGpCommandQueryParms(const char *strCommand,
				char *serializedQuerytree,
				int serializedQuerytreelen,
				char *serializedQueryDispatchDesc,
				int serializedQueryDispatchDesclen,
				int flags)

{
	DispatchCommandQueryParms *pQueryParms;
	CdbComponentDatabaseInfo *qdinfo;

	bool            needTwoPhase = flags & DF_NEED_TWO_PHASE;
	bool            withSnapshot = flags & DF_WITH_SNAPSHOT;

	elogif((Debug_print_full_dtm || log_min_messages <= DEBUG5), LOG,
			"cdbdisp_dispatchCommandInternal: %s (needTwoPhase = %s)",
			strCommand, (needTwoPhase ? "true" : "false"));

	pQueryParms = palloc0(sizeof(*pQueryParms));
	pQueryParms->strCommand = strCommand;
	pQueryParms->serializedQuerytree = serializedQuerytree;
	pQueryParms->serializedQuerytreelen = serializedQuerytreelen;
	pQueryParms->serializedQueryDispatchDesc = serializedQueryDispatchDesc;
	pQueryParms->serializedQueryDispatchDesclen = serializedQueryDispatchDesclen;
	/*
	 * Serialize a version of our DTX Context Info
	 */
	pQueryParms->serializedDtxContextInfo =
		qdSerializeDtxContextInfo(&pQueryParms->serializedDtxContextInfolen,
				withSnapshot, false,
				mppTxnOptions(needTwoPhase),
				"cdbdisp_dispatchCommandInternal");

	/*
	 * sequence server info
	 */
	qdinfo = &(getComponentDatabases()->entry_db_info[0]);
	Assert(qdinfo != NULL && qdinfo->hostip != NULL);
	pQueryParms->seqServerHost = pstrdup(qdinfo->hostip);

	pQueryParms->seqServerHost = pstrdup(qdinfo->hostip);
	pQueryParms->seqServerHostlen = strlen(qdinfo->hostip) + 1;
	pQueryParms->seqServerPort = seqServerCtl->seqServerPort;

	return pQueryParms;
}


/*
 * -------------------------------------------------------------
 *
 * API_DispatchDTXCommand
 *
 * ------------------------------------------------------------
 *
 *
 */
struct pg_result **
API_DispatchDTXCommand(DtxProtocolCommand dtxProtocolCommand,
			int flags,
			char *dtxProtocolCommandLoggingStr,
			char *gid,
			DistributedTransactionId gxid,
			ErrorData **qeError,
			int *numresults,
			bool *badGangs, CdbDispatchDirectDesc *direct,
			char *serializedDtxContextInfo,
			int serializedDtxContextInfoLen)
{
	DispatchCommandDtxProtocolParms		dtxProtocolParms;
	char	   				*queryText = NULL;
	int					queryTextLen = 0;
	CdbPgResults 				cdb_pgresults = {NULL, 0};


	elog((Debug_print_full_dtm ? LOG : DEBUG5),
		 "CdbDispatchDtxProtocolCommand: %s for gid = %s, direct content #: %d",
		 dtxProtocolCommandLoggingStr, gid,
		 direct->directed_dispatch ? direct->content[0] : -1);

	*badGangs = false;
	*qeError = NULL;

	MemSet(&dtxProtocolParms, 0, sizeof(dtxProtocolParms));
	dtxProtocolParms.dtxProtocolCommand = dtxProtocolCommand;
	dtxProtocolParms.flags = flags;
	dtxProtocolParms.dtxProtocolCommandLoggingStr =
		dtxProtocolCommandLoggingStr;
	if (strlen(gid) >= TMGIDSIZE)
		elog(PANIC, "Distribute transaction identifier too long (%d)", (int) strlen(gid));
	memcpy(dtxProtocolParms.gid, gid, TMGIDSIZE);
	dtxProtocolParms.gxid = gxid;
	dtxProtocolParms.serializedDtxContextInfo = serializedDtxContextInfo;
	dtxProtocolParms.serializedDtxContextInfoLen = serializedDtxContextInfoLen;

	queryText = buildGpDtxProtocolCommand(&dtxProtocolParms, &queryTextLen);

	/* do actual dispatch routine */
	DispatcherState_Connect();

	PG_TRY();
	{
		DispatcherState_DispatchCommand(queryText, queryTextLen);
		DispatcherState_Join(DISPATCHER_WAIT_BLOCK);
	}
	PG_CATCH();
	{
		DispatcherState_Join(DISPATCHER_WAIT_CANCEL);
	}
	PG_END_TRY();

	DispatcherState_GetResults(&cdb_pgresults);

	DispatcherState_Close();

	*numresults = cdb_pgresults.numResults;

	return cdb_pgresults.pg_results;
}

static char *
buildGpDtxProtocolCommand(DispatchCommandDtxProtocolParms *pDtxProtocolParms,
						 int *finalLen)
{
	int			dtxProtocolCommand = (int) pDtxProtocolParms->dtxProtocolCommand;
	int			flags = pDtxProtocolParms->flags;
	char	   *dtxProtocolCommandLoggingStr = pDtxProtocolParms->dtxProtocolCommandLoggingStr;
	char	   *gid = pDtxProtocolParms->gid;
	int			gxid = pDtxProtocolParms->gxid;
	char	   *serializedDtxContextInfo = pDtxProtocolParms->serializedDtxContextInfo;
	int			serializedDtxContextInfoLen = pDtxProtocolParms->serializedDtxContextInfoLen;
	int			tmp = 0;
	int			len = 0;

	int			loggingStrLen = strlen(dtxProtocolCommandLoggingStr) + 1;
	int			gidLen = strlen(gid) + 1;
	int			total_query_len = 1 /* 'T' */ +
	sizeof(len) +
	sizeof(dtxProtocolCommand) +
	sizeof(flags) +
	sizeof(loggingStrLen) +
	loggingStrLen +
	sizeof(gidLen) +
	gidLen +
	sizeof(gxid) +
	sizeof(serializedDtxContextInfoLen) +
	serializedDtxContextInfoLen;

	char	   *shared_query = NULL;
	char	   *pos = NULL;

	shared_query = palloc0(total_query_len);
	pos = shared_query;

	*pos++ = 'T';

	pos += sizeof(len);			/* placeholder for message length */

	tmp = htonl(dtxProtocolCommand);
	memcpy(pos, &tmp, sizeof(tmp));
	pos += sizeof(tmp);

	tmp = htonl(flags);
	memcpy(pos, &tmp, sizeof(tmp));
	pos += sizeof(tmp);

	tmp = htonl(loggingStrLen);
	memcpy(pos, &tmp, sizeof(tmp));
	pos += sizeof(tmp);

	memcpy(pos, dtxProtocolCommandLoggingStr, loggingStrLen);
	pos += loggingStrLen;

	tmp = htonl(gidLen);
	memcpy(pos, &tmp, sizeof(tmp));
	pos += sizeof(tmp);

	memcpy(pos, gid, gidLen);
	pos += gidLen;

	tmp = htonl(gxid);
	memcpy(pos, &tmp, sizeof(tmp));
	pos += sizeof(tmp);

	tmp = htonl(serializedDtxContextInfoLen);
	memcpy(pos, &tmp, sizeof(tmp));
	pos += sizeof(tmp);

	if (serializedDtxContextInfoLen > 0)
	{
		memcpy(pos, serializedDtxContextInfo, serializedDtxContextInfoLen);
		pos += serializedDtxContextInfoLen;
	}

	len = pos - shared_query - 1;

	/*
	 * fill in length placeholder
	 */
	tmp = htonl(len);
	memcpy(shared_query + 1, &tmp, sizeof(tmp));

	if (finalLen)
		*finalLen = len + 1;

	return shared_query;
}

void
API_DispatchPlanEnd(void)
{
	HTAB			*aopartcounts = NULL;
	DispatcherWaitMode	waitMode = DISPATCHER_WAIT_BLOCK;
	EState			*estate;
	QueryDesc		*queryDesc;

	Assert(CurrentDispatcherState);
	Assert(CurrentDispatcherState->queryDesc != NULL);
	Assert(CurrentDispatcherState->queryDesc->estate != NULL);

	queryDesc = CurrentDispatcherState->queryDesc;	
	estate = queryDesc->estate;

	/*
	 * If we are finishing a query before all the tuples of the query
	 * plan were fetched we must call ExecSquelchNode before checking
	 * the dispatch results in order to tell the nodes below we no longer
	 * need any more tuples.
	 */
	if (!estate->es_got_eos)
	{
		ExecSquelchNode(queryDesc->planstate);
	}

	/*
	 * Wait for completion of all QEs.  We send a "graceful" query
	 * finish, not cancel signal.  Since the query has succeeded,
	 * don't confuse QEs by sending erroneous message.
	 */
	if (estate->cancelUnfinished)
		waitMode = DISPATCHER_WAIT_FINISH;

	DispatcherState_Join(waitMode);

	/* If top slice was delegated to QEs, get num of rows processed. */
	int primaryWriterSliceIndex = PrimaryWriterSliceIndex(estate);

	//if (sliceRunsOnQE(currentSlice))
	{
		estate->es_processed +=
			dispatcher_sumCmdTuples(CurrentDispatcherState->results, primaryWriterSliceIndex);
		estate->es_lastoid =
			dispatcher_maxLastOid(CurrentDispatcherState->results, primaryWriterSliceIndex);
		aopartcounts = dispatcher_sumAoPartTupCount(estate->es_result_partitions, CurrentDispatcherState->results);
	}

	/* sum up rejected rows if any (single row error handling only) */
	dispatcher_sumRejectedRows(CurrentDispatcherState->results);

	/* sum up inserted rows into any AO relation */
	if (aopartcounts)
	{
		/* counts from a partitioned AO table */

		ListCell *lc;

		foreach(lc, estate->es_result_aosegnos)
		{
			SegfileMapNode *map = lfirst(lc);
			struct {
				Oid relid;
				int64 tupcount;
			} *entry;
			bool found;

			entry = hash_search(aopartcounts,
					&(map->relid),
					HASH_FIND,
					&found);

			/*
			 * Must update the mod count only for segfiles where actual tuples were touched 
			 * (added/deleted) based on entry->tupcount.
			 */
			if (found && entry->tupcount)
			{
				bool was_delete = estate->es_plannedstmt && (estate->es_plannedstmt->commandType == CMD_DELETE);

				Relation r = heap_open(map->relid, AccessShareLock);
				if (was_delete)
				{
					UpdateMasterAosegTotals(r, map->segno, 0, 1);
				}
				else
				{
					UpdateMasterAosegTotals(r, map->segno, entry->tupcount, 1);	
				}
				heap_close(r, NoLock);
			}
		}
	}
	else
	{
		/* counts from a (non partitioned) AO table */

		ExecUpdateAOtupCount(estate->es_result_relations,
				estate->es_snapshot,
				estate->es_num_result_relations,
				estate,
				estate->es_processed);
	}

	DispatcherState_Close();
}

/*
 * Return sum of the cmdTuples values from CdbDispatchResult
 * entries that have a successful PGresult. If sliceIndex >= 0,
 * uses only the results belonging to the specified slice.
 */
static int64
dispatcher_sumCmdTuples(List *results, int sliceIndex)
{
	CdbDispatchResult	*dispatchResult;
	PGresult   		*pgresult;
	int64			sum = 0;
	ListCell		*lc;

	foreach (lc, results)
	{
		dispatchResult = (CdbDispatchResult *)lfirst(lc);

		if (dispatchResult->gang->sliceIndex != sliceIndex)
		{
			continue;
		}

		pgresult = cdbdisp_getPGresult(dispatchResult, dispatchResult->okindex);
		if (pgresult && !dispatchResult->errcode)
		{
			char	   *cmdTuples = PQcmdTuples(pgresult);

			if (cmdTuples)
				sum += atoll(cmdTuples);
		}
	}
	return sum;
}

/*
 * If several tuples were eliminated/rejected from the result because of
 * bad data formatting (this is currenly only possible in external tables
 * with single row error handling) - sum up the total rows rejected from
 * all QE's and notify the client.
 */
static void
dispatcher_sumRejectedRows(List *results)
{
	CdbDispatchResult	*dispatchResult;
	PGresult		*pgresult;
	int			totalRejected = 0;
	ListCell		*lc;

	foreach (lc, results)
	{
		dispatchResult = (CdbDispatchResult *)lfirst(lc);

		pgresult = cdbdisp_getPGresult(dispatchResult, dispatchResult->okindex);

		if (pgresult && !dispatchResult->errcode)
		{
			/*
			 * add num rows rejected from this QE to the total
			 */
			totalRejected += dispatchResult->numrowsrejected;
		}
	}

	if (totalRejected > 0)
		ReportSrehResults(NULL, totalRejected);

}

/*
 * sum tuple counts that were added into a partitioned AO table
 */
static HTAB *
dispatcher_sumAoPartTupCount(PartitionNode *parts, List *results)
{
	HTAB		*ht = NULL;
	ListCell	*lc; 

	if (!parts)
		return NULL;

	foreach (lc, results)
	{
		CdbDispatchResult *dispatchResult = (CdbDispatchResult *)lfirst(lc);
		int			nres = cdbdisp_numPGresult(dispatchResult);
		int			ires;

		for (ires = 0; ires < nres; ++ires)
		{
			PGresult   *pgresult = cdbdisp_getPGresult(dispatchResult, ires);

			ht = PQprocessAoTupCounts(parts, ht, (void *) pgresult->aotupcounts,
									  pgresult->naotupcounts);
		}
	}

	return ht;
}

/*
 * Find the max of the lastOid values returned from the QEs
 */
static Oid
dispatcher_maxLastOid(List *results, int sliceIndex)
{
	CdbDispatchResult	*dispatchResult;
	PGresult		*pgresult;
	Oid			oid = InvalidOid;
	ListCell		*lc;

	foreach (lc, results)
	{
		dispatchResult = (CdbDispatchResult *)lfirst(lc);

		if (dispatchResult->gang->sliceIndex != sliceIndex)
			continue;

		pgresult = cdbdisp_getPGresult(dispatchResult, dispatchResult->okindex);

		if (pgresult && !dispatchResult->errcode)
		{
			Oid			tmpoid = PQoidValue(pgresult);

			if (tmpoid > oid)
				oid = tmpoid;
		}
	}

	return oid;
}

/*
 * ExecUpdateAOtupCount
 *		Update the tuple count on the master for an append only relation segfile.
 */
static void
ExecUpdateAOtupCount(ResultRelInfo *result_rels,
					 Snapshot shapshot,
					 int num_result_rels,
					 EState* estate,
					 uint64 tupadded)
{
	int		i;

	Assert(Gp_role == GP_ROLE_DISPATCH);

	bool was_delete = estate && estate->es_plannedstmt &&
		(estate->es_plannedstmt->commandType == CMD_DELETE);

	for (i = num_result_rels; i > 0; i--)
	{
		if(RelationIsAppendOptimized(result_rels->ri_RelationDesc))
		{
			Assert(result_rels->ri_aosegno != InvalidFileSegNumber);

			if (was_delete && tupadded > 0)
			{
				/* Touch the ao seg info */
				UpdateMasterAosegTotals(result_rels->ri_RelationDesc,
									result_rels->ri_aosegno,
									0,
									1);
			} 
			else if (!was_delete)
			{
				UpdateMasterAosegTotals(result_rels->ri_RelationDesc,
									result_rels->ri_aosegno,
									tupadded,
									1);
			}
		}

		result_rels++;
	}
}
