/*-------------------------------------------------------------------------
 *
 * cdbgang.c
 *	  Query Executor Factory for gangs of QEs
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/dispatcher/cdbgang.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>				/* getpid() */
#include <pthread.h>
#include <limits.h>

#include "libpq-fe.h"
#include "miscadmin.h"			/* MyDatabaseId */
#include "storage/proc.h"		/* MyProc */
#include "storage/ipc.h"
#include "utils/memutils.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/variable.h"
#include "nodes/execnodes.h"	/* CdbProcess, Slice, SliceTable */
#include "postmaster/postmaster.h"
#include "tcop/tcopprot.h"
#include "utils/portal.h"
#include "utils/sharedsnapshot.h"
#include "tcop/pquery.h"

#include "libpq-fe.h"
#include "libpq-int.h"
#include "cdb/cdbconn.h"		/* SegmentDatabaseDescriptor */
#include "cdb/cdbfts.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbgang.h"		/* me */
#include "cdb/cdbgang_thread.h"
#include "cdb/cdbgang_async.h"
#include "cdb/cdbtm.h"			/* discardDtxTransaction() */
#include "cdb/cdbutil.h"		/* CdbComponentDatabaseInfo */
#include "cdb/cdbvars.h"		/* Gp_role, etc. */
#include "storage/bfz.h"
#include "libpq/libpq-be.h"
#include "libpq/ip.h"

#include "utils/guc_tables.h"

#define MAX_CACHED_1_GANGS 1

/*
 * Which gang this QE belongs to; this would be used in PostgresMain to find out
 * the slice this QE should execute
 */
int			qe_gang_id = 0;

/*
 * number of primary segments on this host
 */
int			host_segments = 0;

MemoryContext GangContext = NULL;
Gang	   *CurrentGangCreating = NULL;

CreateGangFunc pCreateGangFunc = NULL;

/*
 * Points to the result of getCdbComponentDatabases()
 */
static CdbComponentDatabases *cdb_component_dbs = NULL;

static int	largest_gangsize = 0;

static bool NeedResetSession = false;
static Oid	OldTempNamespace = InvalidOid;

List *allocatedGangs = NIL;
List *twophaseSegments = NIL;
Gang *primaryWriterGang = NULL;
Bitmapset *twophaseRegion = NULL;
static SegdbDescsManager *SessionSegdbDescsManager = NULL;

/*
 * Every gang created must have a unique identifier
 */
static Gang *createGang(GangType type, List *region);
static void disconnectAndDestroyAllGangs(bool destroyAllocated);

static bool isTargetPortal(const char *p1, const char *p2);
//static bool cleanupGang(Gang *gp);
static void resetSessionForPrimaryGangLoss(void);
static CdbComponentDatabaseInfo *copyCdbComponentDatabaseInfo(
							 CdbComponentDatabaseInfo *dbInfo);
static CdbComponentDatabaseInfo *findDatabaseInfoBySegIndex(
						   CdbComponentDatabases *cdbs, int segIndex);
static void addGangToAllocated(Gang *gp);

static void addToTwoPhaseRegion(Gang *gang);

void releaseGang(Gang *gp, bool noReuse);

static void cleanupSessionSegdbDescsManager(void);

static SegmentDatabaseDescriptor* allocateSegdbDesc(int segindex);

static void recycleSegdbDesc(SegmentDatabaseDescriptor *segdbDesc);

static void InitSessionSegdbDescManager(void);

static SegmentDatabaseDescriptor ** createSegdbDescForGang(List *segments);

static void destroySegdbDesc(SegmentDatabaseDescriptor *segdbDesc);

#if 0
bool
isPrimaryWriterGangAlive(void)
{
	if (primaryWriterGang == NULL)
		return false;

	int			size = primaryWriterGang->size;
	int			i = 0;

	Assert(size == getgpsegmentCount());

	for (i = 0; i < size; i++)
	{
		SegmentDatabaseDescriptor *segdb = &primaryWriterGang->db_descriptors[i];

		if (!isSockAlive(segdb->conn->sock))
			return false;
	}

	return true;
}
#endif

/*
 * Check the segment failure reason by comparing connection error message.
 */
bool
segment_failure_due_to_recovery(const char *error_message)
{
	char	   *fatal = NULL,
			   *ptr = NULL;
	int			fatal_len = 0;

	if (error_message == NULL)
		return false;

	fatal = _("FATAL");
	fatal_len = strlen(fatal);

	/*
	 * it would be nice if we could check errcode for
	 * ERRCODE_CANNOT_CONNECT_NOW, instead we wind up looking for at the
	 * strings.
	 *
	 * And because if LC_MESSAGES gets set to something which changes the
	 * strings a lot we have to take extreme care with looking at the string.
	 */
	ptr = strstr(error_message, fatal);
	if ((ptr != NULL) && ptr[fatal_len] == ':')
	{
		if (strstr(error_message, _(POSTMASTER_IN_STARTUP_MSG)))
		{
			return true;
		}
		if (strstr(error_message, _(POSTMASTER_IN_RECOVERY_MSG)))
		{
			return true;
		}
		/* We could do retries for "sorry, too many clients already" here too */
	}

	return false;
}

/*
 * Read gp_segment_configuration catalog table and build a CdbComponentDatabases.
 *
 * Read the catalog if FTS is reconfigured.
 *
 * We don't want to destroy cdb_component_dbs when one gang get destroyed, so allocate
 * it in GangContext instead of perGangContext.
 */
CdbComponentDatabases *
getComponentDatabases(void)
{
	Assert(Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_UTILITY);
	Assert(GangContext != NULL);

	uint8		ftsVersion = getFtsVersion();
	MemoryContext oldContext = MemoryContextSwitchTo(GangContext);

	if (cdb_component_dbs == NULL)
	{
		cdb_component_dbs = getCdbComponentDatabases();
		cdb_component_dbs->fts_version = ftsVersion;
	}
	else if (cdb_component_dbs->fts_version != ftsVersion)
	{
		ELOG_DISPATCHER_DEBUG("FTS rescanned, get new component databases info.");
		freeCdbComponentDatabases(cdb_component_dbs);
		cdb_component_dbs = getCdbComponentDatabases();
		cdb_component_dbs->fts_version = ftsVersion;
	}

	MemoryContextSwitchTo(oldContext);

	return cdb_component_dbs;
}

/*
 * Make a copy of CdbComponentDatabaseInfo.
 *
 * Caller destroy it.
 */
static CdbComponentDatabaseInfo *
copyCdbComponentDatabaseInfo(
							 CdbComponentDatabaseInfo *dbInfo)
{
	int			i = 0;
	int			size = sizeof(CdbComponentDatabaseInfo);
	CdbComponentDatabaseInfo *newInfo = palloc0(size);

	memcpy(newInfo, dbInfo, size);

	if (dbInfo->hostip)
		newInfo->hostip = pstrdup(dbInfo->hostip);

	/* So far, we don't need them. */
	newInfo->address = NULL;
	newInfo->hostname = NULL;
	for (i = 0; i < COMPONENT_DBS_MAX_ADDRS; i++)
		newInfo->hostaddrs[i] = NULL;

	return newInfo;
}

/*
 * Find CdbComponentDatabases in the array by segment index.
 */
static CdbComponentDatabaseInfo *
findDatabaseInfoBySegIndex(
						   CdbComponentDatabases *cdbs, int segIndex)
{
	Assert(cdbs != NULL);
	int			i = 0;
	CdbComponentDatabaseInfo *cdbInfo = NULL;

	for (i = 0; i < cdbs->total_segment_dbs; i++)
	{
		cdbInfo = &cdbs->segment_db_info[i];
		if (segIndex == cdbInfo->segindex)
			break;
	}

	return cdbInfo;
}

/*
 * Add one GUC to the option string.
 */
static void
addOneOption(StringInfo string, struct config_generic *guc)
{
	Assert(guc && (guc->flags & GUC_GPDB_ADDOPT));
	switch (guc->vartype)
	{
		case PGC_BOOL:
			{
				struct config_bool *bguc = (struct config_bool *) guc;

				appendStringInfo(string, " -c %s=%s", guc->name, *(bguc->variable) ? "true" : "false");
				break;
			}
		case PGC_INT:
			{
				struct config_int *iguc = (struct config_int *) guc;

				appendStringInfo(string, " -c %s=%d", guc->name, *iguc->variable);
				break;
			}
		case PGC_REAL:
			{
				struct config_real *rguc = (struct config_real *) guc;

				appendStringInfo(string, " -c %s=%f", guc->name, *rguc->variable);
				break;
			}
		case PGC_STRING:
			{
				struct config_string *sguc = (struct config_string *) guc;
				const char *str = *sguc->variable;
				int			i;

				appendStringInfo(string, " -c %s=", guc->name);

				/*
				 * All whitespace characters must be escaped. See
				 * pg_split_opts() in the backend.
				 */
				for (i = 0; str[i] != '\0'; i++)
				{
					if (isspace((unsigned char) str[i]))
						appendStringInfoChar(string, '\\');

					appendStringInfoChar(string, str[i]);
				}
				break;
			}
		case PGC_ENUM:
			{
				struct config_enum *eguc = (struct config_enum *) guc;
				int			value = *eguc->variable;
				const char *str = config_enum_lookup_by_value(eguc, value);
				int			i;

				appendStringInfo(string, " -c %s=", guc->name);

				/*
				 * All whitespace characters must be escaped. See
				 * pg_split_opts() in the backend. (Not sure if an enum value
				 * can have whitespace, but let's be prepared.)
				 */
				for (i = 0; str[i] != '\0'; i++)
				{
					if (isspace((unsigned char) str[i]))
						appendStringInfoChar(string, '\\');

					appendStringInfoChar(string, str[i]);
				}
				break;
			}
		default:
			Insist(false);
	}
}

/*
 * Add GUCs to option string.
 */
char *
makeOptions(void)
{
	struct config_generic **gucs = get_guc_variables();
	int			ngucs = get_num_guc_variables();
	CdbComponentDatabaseInfo *qdinfo = NULL;
	StringInfoData string;
	int			i;

	initStringInfo(&string);

	Assert(Gp_role == GP_ROLE_DISPATCH);

	qdinfo = &cdb_component_dbs->entry_db_info[0];
	appendStringInfo(&string, " -c gp_qd_hostname=%s", qdinfo->hostip);
	appendStringInfo(&string, " -c gp_qd_port=%d", qdinfo->port);

	/*
	 * Transactions are tricky. Here is the copy and pasted code, and we know
	 * they are working. The problem, is that QE may ends up with different
	 * iso level, but postgres really does not have read uncommited and
	 * repeated read. (is this true?) and they are mapped.
	 *
	 * Put these two gucs in the generic framework works (pass make
	 * installcheck-good) if we make assign_defaultxactisolevel and
	 * assign_XactIsoLevel correct take string "readcommitted" etc.	(space
	 * stripped).  However, I do not want to change this piece of code unless
	 * I know it is broken.
	 */
	if (DefaultXactIsoLevel != XACT_READ_COMMITTED)
	{
		if (DefaultXactIsoLevel == XACT_REPEATABLE_READ)
			appendStringInfo(&string, " -c default_transaction_isolation=repeatable\\ read");
		else if (DefaultXactIsoLevel == XACT_SERIALIZABLE)
			appendStringInfo(&string, " -c default_transaction_isolation=serializable");
	}

	if (XactIsoLevel != XACT_READ_COMMITTED)
	{
		if (XactIsoLevel == XACT_REPEATABLE_READ)
			appendStringInfo(&string, " -c transaction_isolation=repeatable\\ read");
		else if (XactIsoLevel == XACT_SERIALIZABLE)
			appendStringInfo(&string, " -c transaction_isolation=serializable");
	}

	for (i = 0; i < ngucs; ++i)
	{
		struct config_generic *guc = gucs[i];

		if ((guc->flags & GUC_GPDB_ADDOPT) && (guc->context == PGC_USERSET || procRoleIsSuperuser()))
			addOneOption(&string, guc);
	}

	return string.data;
}

/*
 * build_gpqeid_param
 *
 * Called from the qDisp process to create the "gpqeid" parameter string
 * to be passed to a qExec that is being started.  NB: Can be called in a
 * thread, so mustn't use palloc/elog/ereport/etc.
 */
void
build_gpqeid_param(char *buf, int bufsz, int segIndex,
				   bool is_writer, int gangId, int hostSegs)
{
#ifdef HAVE_INT64_TIMESTAMP
#define TIMESTAMP_FORMAT INT64_FORMAT
#else
#ifndef _WIN32
#define TIMESTAMP_FORMAT "%.14a"
#else
#define TIMESTAMP_FORMAT "%g"
#endif
#endif

	snprintf(buf, bufsz, "%d;%d;" TIMESTAMP_FORMAT ";%s;%d;%d",
			 gp_session_id, segIndex, PgStartTime,
			 (is_writer ? "true" : "false"), gangId, hostSegs);
}

static bool
gpqeid_next_param(char **cpp, char **npp)
{
	*cpp = *npp;
	if (!*cpp)
		return false;

	*npp = strchr(*npp, ';');
	if (*npp)
	{
		**npp = '\0';
		++*npp;
	}
	return true;
}

/*
 * cdbgang_parse_gpqeid_params
 *
 * Called very early in backend initialization, to interpret the "gpqeid"
 * parameter value that a qExec receives from its qDisp.
 *
 * At this point, client authentication has not been done; the backend
 * command line options have not been processed; GUCs have the settings
 * inherited from the postmaster; etc; so don't try to do too much in here.
 */
void
			cdbgang_parse_gpqeid_params(struct Port *port __attribute__((unused)),
										const char *gpqeid_value)
{
	char	   *gpqeid = pstrdup(gpqeid_value);
	char	   *cp;
	char	   *np = gpqeid;

	/* The presence of an gpqeid string means this backend is a qExec. */
	SetConfigOption("gp_session_role", "execute", PGC_POSTMASTER, PGC_S_OVERRIDE);

	/* gp_session_id */
	if (gpqeid_next_param(&cp, &np))
		SetConfigOption("gp_session_id", cp, PGC_POSTMASTER, PGC_S_OVERRIDE);

	/* gp_segment */
	if (gpqeid_next_param(&cp, &np))
		SetConfigOption("gp_segment", cp, PGC_POSTMASTER, PGC_S_OVERRIDE);

	/* PgStartTime */
	if (gpqeid_next_param(&cp, &np))
	{
#ifdef HAVE_INT64_TIMESTAMP
		if (!scanint8(cp, true, &PgStartTime))
			goto bad;
#else
		PgStartTime = strtod(cp, NULL);
#endif
	}

	/* Gp_is_writer */
	if (gpqeid_next_param(&cp, &np))
		SetConfigOption("gp_is_writer", cp, PGC_POSTMASTER, PGC_S_OVERRIDE);

	if (gpqeid_next_param(&cp, &np))
	{
		qe_gang_id = (int) strtol(cp, NULL, 10);
	}

	if (gpqeid_next_param(&cp, &np))
	{
		host_segments = (int) strtol(cp, NULL, 10);
	}

	/* Too few items, or too many? */
	if (!cp || np)
		goto bad;

	if (gp_session_id <= 0 || PgStartTime <= 0 || qe_gang_id <= 0 || host_segments <= 0)
		goto bad;

	pfree(gpqeid);
	return;

bad:
	elog(FATAL, "Segment dispatched with invalid option: 'gpqeid=%s'", gpqeid_value);
}

#if 0
/*
 * This is where we keep track of all the gangs that exist for this session.
 * On a QD, gangs can either be "available" (not currently in use), or "allocated".
 *
 * On a Dispatch Agent, we just store them in the "available" lists, as the DA doesn't
 * keep track of allocations (it assumes the QD will keep track of what is allocated or not).
 *
 */
List *
getAllIdleReaderGangs()
{
	List	   *res = NIL;
	ListCell   *le;

	/*
	 * Do not use list_concat() here, it would destructively modify the lists!
	 */
	foreach(le, availableReaderGangsN)
	{
		res = lappend(res, lfirst(le));
	}
	foreach(le, availableReaderGangs1)
	{
		res = lappend(res, lfirst(le));
	}

	return res;
}

List *
getAllAllocatedReaderGangs()
{
	List	   *res = NIL;
	ListCell   *le;

	/*
	 * Do not use list_concat() here, it would destructively modify the lists!
	 */
	foreach(le, allocatedReaderGangsN)
	{
		res = lappend(res, lfirst(le));
	}
	foreach(le, allocatedReaderGangs1)
	{
		res = lappend(res, lfirst(le));
	}

	return res;
}
#endif

static void
addGangToAllocated(Gang *gp)
{
	Assert(CurrentMemoryContext == GangContext);

	allocatedGangs = lappend(allocatedGangs, gp);
}

struct SegmentDatabaseDescriptor *
getSegmentDescriptorFromGang(const Gang *gp, int seg)
{
	int			i = 0;

	if (gp == NULL)
		return NULL;

	for (i = 0; i < gp->size; i++)
	{
		if (gp->db_descriptors[i]->segindex == seg)
			return gp->db_descriptors[i];
	}

	return NULL;
}

static CdbProcess *
makeCdbProcess(SegmentDatabaseDescriptor *segdbDesc)
{
	CdbProcess *process = (CdbProcess *) makeNode(CdbProcess);
	CdbComponentDatabaseInfo *qeinfo = segdbDesc->segment_database_info;

	if (qeinfo == NULL)
	{
		elog(ERROR, "required segment is unavailable");
	}
	else if (qeinfo->hostip == NULL)
	{
		elog(ERROR, "required segment IP is unavailable");
	}

	process->listenerAddr = pstrdup(qeinfo->hostip);

	if (Gp_interconnect_type == INTERCONNECT_TYPE_UDPIFC)
		process->listenerPort = (segdbDesc->motionListener >> 16) & 0x0ffff;
	else if (Gp_interconnect_type == INTERCONNECT_TYPE_TCP)
		process->listenerPort = (segdbDesc->motionListener & 0x0ffff);

	process->pid = segdbDesc->backendPid;
	process->contentid = segdbDesc->segindex;
	return process;
}

/*
 * Create a list of CdbProcess and initialize with Gang information.
 *
 * 1) For primary reader gang and primary writer gang, the elements
 * in this list is order by segment index.
 * 2) For entry DB gang and singleton gang, the list length is 1.
 *
 * @directDispatch: might be null
 */
List *
getCdbProcessList(Gang *gang, int sliceIndex, DirectDispatchInfo *directDispatch)
{
	List	   *list = NULL;
	int			i = 0;

	ELOG_DISPATCHER_DEBUG("getCdbProcessList slice%d gangtype=%d gangsize=%d",
						  sliceIndex, gang->type, gang->size);

	Assert(Gp_role == GP_ROLE_DISPATCH);

	if (directDispatch != NULL && directDispatch->isDirectDispatch)
	{
		/* Currently, direct dispatch is to one segment db. */
		Assert(list_length(directDispatch->contentIds) == 1);

		int			directDispatchContentId = linitial_int(directDispatch->contentIds);
		SegmentDatabaseDescriptor *segdbDesc = gang->db_descriptors[directDispatchContentId];
		CdbProcess *process = makeCdbProcess(segdbDesc);

		setQEIdentifier(segdbDesc, sliceIndex, GangContext);
		list = lappend(list, (void*)process);
	}
	else
	{
		for (i = 0; i < gang->size; i++)
		{
			SegmentDatabaseDescriptor *segdbDesc = gang->db_descriptors[i];
			CdbProcess *process = makeCdbProcess(segdbDesc);

			setQEIdentifier(segdbDesc, sliceIndex, GangContext);
			list = lappend(list, process);

			ELOG_DISPATCHER_DEBUG("Gang assignment (gang_id %d): slice%d seg%d %s:%d pid=%d",
								  gang->gang_id, sliceIndex, process->contentid,
								  process->listenerAddr, process->listenerPort, process->pid);
		}
	}

	return list;
}

/*
 * getCdbProcessForQD:	Manufacture a CdbProcess representing the QD,
 * as if it were a worker from the executor factory.
 *
 * NOTE: Does not support multiple (mirrored) QDs.
 */
List *
getCdbProcessesForQD(int isPrimary)
{
	List	   *list = NIL;

	CdbProcess *proc;
#if 0
	CdbComponentDatabaseInfo *qdinfo;

	Assert(Gp_role == GP_ROLE_DISPATCH);
	//Assert(cdb_component_dbs != NULL);

	if (!isPrimary)
	{
		elog(FATAL, "getCdbProcessesForQD: unsupported request for master mirror process");
	}

	qdinfo = &(cdb_component_dbs->entry_db_info[0]);

	Assert(qdinfo->segindex == -1);
	Assert(SEGMENT_IS_ACTIVE_PRIMARY(qdinfo));
	Assert(qdinfo->hostip != NULL);
#endif

	proc = makeNode(CdbProcess);

	/*
	 * Set QD listener address to NULL. This will be filled during starting up
	 * outgoing interconnect connection.
	 */
	proc->listenerAddr = NULL;

	if (Gp_interconnect_type == INTERCONNECT_TYPE_UDPIFC)
		proc->listenerPort = (Gp_listener_port >> 16) & 0x0ffff;
	else if (Gp_interconnect_type == INTERCONNECT_TYPE_TCP)
		proc->listenerPort = (Gp_listener_port & 0x0ffff);

	proc->pid = MyProcPid;
	proc->contentid = -1;

	list = lappend(list, proc);
	return list;
}

/*
 * Destroy or recycle Gangs
 */
/*
 * Disconnect and destroy a Gang.
 *
 * Loop through all the connections of this Gang and disconnect it.
 * Free the resource of this Gang.
 *
 * Caller needs to free all the reader Gangs if this is a writer gang.
 * Caller needs to reset session id if this is a writer gang.
 */
void
DisconnectAndDestroyGang(Gang *gp)
{
	return releaseGang(gp, true);
}


/*
 * disconnectAndDestroyAllReaderGangs
 *
 * Here we destroy all reader gangs regardless of the portal they belong to.
 * TODO: This may need to be done more carefully when multiple cursors are
 * enabled.
 * If the parameter destroyAllocated is true, then destroy allocated as well as
 * available gangs.
 */
static void
disconnectAndDestroyAllGangs(bool destroyAllocated)
{
	Gang	   *gp = NULL;
	ListCell   *lc = NULL;

	if (destroyAllocated)
	{
		foreach(lc, allocatedGangs)
		{
			gp = (Gang *) lfirst(lc);
			releaseGang(gp, true);
		}
		allocatedGangs = NIL;
	}

	cleanupSessionSegdbDescsManager();
}

void
DisconnectAndDestroyAllGangs(bool resetSession)
{
	if (Gp_role != GP_ROLE_DISPATCH)
		return;

	ELOG_DISPATCHER_DEBUG("DisconnectAndDestroyAllGangs");

#if 0
	/* Destroy CurrentGangCreating before GangContext is reset */
	if (CurrentGangCreating != NULL)
	{
		DisconnectAndDestroyGang(CurrentGangCreating);
		CurrentGangCreating = NULL;
	}
#endif

	/* for now, destroy all readers, regardless of the portal that owns them */
	disconnectAndDestroyAllGangs(true);

	if (resetSession)
		resetSessionForPrimaryGangLoss();

	/*
	 * As all the reader and writer gangs are destroyed, reset the
	 * corresponding GangContext to prevent leaks
	 */
	if (NULL != GangContext)
	{
		MemoryContextReset(GangContext);
		cdb_component_dbs = NULL;
	}

	ELOG_DISPATCHER_DEBUG("DisconnectAndDestroyAllGangs done");
}

/*
 * Destroy all idle (i.e available) reader gangs.
 * It is always safe to get rid of the reader gangs.
 *
 * call only from an idle session.
 */
void
disconnectAndDestroyIdleGangs(void)
{
	ELOG_DISPATCHER_DEBUG("disconnectAndDestroyIdleReaderGangs beginning");

	disconnectAndDestroyAllGangs(false);

	ELOG_DISPATCHER_DEBUG("disconnectAndDestroyIdleReaderGangs done");

	return;
}

#if 0
/*
 * Cleanup a Gang, make it recyclable.
 *
 * A return value of "true" means that the gang was intact (or NULL).
 *
 * A return value of false, means that a problem was detected and the
 * gang has been disconnected (and so should not be put back onto the
 * available list). Caller should call DisconnectAndDestroyGang on it.
 */
static bool
cleanupGang(Gang *gp)
{
	int			i = 0;

	ELOG_DISPATCHER_DEBUG("cleanupGang: cleaning gang id %d type %d size %d, "
						  "was used for portal: %s",
						  gp->gang_id, gp->type, gp->size,
						  (gp->portal_name ? gp->portal_name : "(unnamed)"));

	if (gp->noReuse)
		return false;

	if (gp->allocated)
		ELOG_DISPATCHER_DEBUG("cleanupGang called on a gang that is allocated");

	/*
	 * if the process is in the middle of blowing up... then we don't do
	 * anything here.  making libpq and other calls can definitely result in
	 * things getting HUNG.
	 */
	if (proc_exit_inprogress)
		return true;

	/*
	 * Loop through the segment_database_descriptors array and, for each
	 * SegmentDatabaseDescriptor: 1) discard the query results (if any)
	 */
	for (i = 0; i < gp->size; i++)
	{
		SegmentDatabaseDescriptor *segdbDesc = gp->db_descriptors[i];

		Assert(segdbDesc != NULL);

		if (cdbconn_isBadConnection(segdbDesc))
			return false;

		/* if segment is down, the gang can not be reused */
		if (!FtsIsSegmentUp(segdbDesc->segment_database_info))
			return false;

		/* Note, we cancel all "still running" queries */
		if (!cdbconn_discardResults(segdbDesc, 20))
			elog(FATAL, "cleanup called when a segworker is still busy");

		/* QE is no longer associated with a slice. */
		setQEIdentifier(segdbDesc, /* slice index */ -1, gp->perGangContext);
	}

	/* disassociate this gang with any portal that it may have belonged to */
	if (gp->portal_name != NULL)
	{
		pfree(gp->portal_name);
		gp->portal_name = NULL;
	}

	gp->allocated = false;

	ELOG_DISPATCHER_DEBUG("cleanupGang done");
	return true;
}

/*
 * Get max maxVmemChunksTracked of a gang.
 *
 * return in MB.
 */
static int
getGangMaxVmem(Gang *gp)
{
	int64		maxmop = 0;
	int			i = 0;

	for (i = 0; i < gp->size; ++i)
	{
		SegmentDatabaseDescriptor *segdbDesc = gp->db_descriptors[i];

		Assert(segdbDesc != NULL);

		if (!cdbconn_isBadConnection(segdbDesc))
			maxmop = Max(maxmop, segdbDesc->conn->mop_high_watermark);
	}

	return (maxmop >> 20);
}
#endif
/*
 * the gang is working for portal p1. we are only interested in gangs
 * from portal p2. if p1 and p2 are the same portal return true. false
 * otherwise.
 */
static
bool
isTargetPortal(const char *p1, const char *p2)
{
	/* both are unnamed portals (represented as NULL) */
	if (!p1 && !p2)
		return true;

	/* one is unnamed, the other is named */
	if (!p1 || !p2)
		return false;

	/* both are the same named portal */
	if (strcmp(p1, p2) == 0)
		return true;

	return false;
}

#if 0
/*
 * remove elements from gang list when:
 * 1. list size > cachelimit
 * 2. max mop of this gang > gp_vmem_protect_gang_cache_limit
 */
static List *
cleanupPortalGangList(List *gplist, int cachelimit)
{
	ListCell   *cell = NULL;
	ListCell   *prevcell = NULL;
	int			nLeft = list_length(gplist);

	if (gplist == NULL)
		return NULL;

	cell = list_head(gplist);
	while (cell != NULL)
	{
		Gang	   *gang = (Gang *) lfirst(cell);

		Assert(gang->type != GANGTYPE_PRIMARY_WRITER);

		if (nLeft > cachelimit ||
			getGangMaxVmem(gang) > gp_vmem_protect_gang_cache_limit)
		{
			DisconnectAndDestroyGang(gang);
			gplist = list_delete_cell(gplist, cell, prevcell);
			nLeft--;

			if (prevcell != NULL)
				cell = lnext(prevcell);
			else
				cell = list_head(gplist);
		}
		else
		{
			prevcell = cell;
			cell = lnext(cell);
		}
	}

	return gplist;
}
#endif
/*
 * Portal drop... Clean up what gangs we hold
 */
void
cleanupPortalGangs(Portal portal)
{
	MemoryContext oldContext;
	const char *portal_name;

	if (portal->name && strcmp(portal->name, "") != 0)
	{
		portal_name = portal->name;
		ELOG_DISPATCHER_DEBUG("cleanupPortalGangs %s", portal_name);
	}
	else
	{
		portal_name = NULL;
		ELOG_DISPATCHER_DEBUG("cleanupPortalGangs (unamed portal)");
	}

	if (GangContext)
		oldContext = MemoryContextSwitchTo(GangContext);
	else
		oldContext = MemoryContextSwitchTo(TopMemoryContext);

	disconnectAndDestroyAllGangs(true);

	MemoryContextSwitchTo(oldContext);
}

/*
 * freeGangsForPortal
 *
 * Free all gangs that were allocated for a specific portal
 * (could either be a cursor name or an unnamed portal)
 *
 * Be careful when moving gangs onto the available list, if
 * cleanupGang() tells us that the gang has a problem, the gang has
 * been free()ed and we should discard it -- otherwise it is good as
 * far as we can tell.
 */
void
freeGangsForPortal(char *portal_name)
{
	MemoryContext oldContext;
	ListCell   *cur_item = NULL;
	ListCell   *prev_item = NULL;

	if (Gp_role != GP_ROLE_DISPATCH)
		return;

	/*
	 * Now we iterate through the list of allocated reader gangs and we free
	 * all the gangs that belong to the portal that was specified by our
	 * caller.
	 */
	if (!list_length(allocatedGangs))
		return;

	Assert(GangContext != NULL);
	oldContext = MemoryContextSwitchTo(GangContext);

	cur_item = list_head(allocatedGangs);
	while (cur_item != NULL)
	{
		Gang	   *gp = (Gang *) lfirst(cur_item);
		ListCell   *next_item = lnext(cur_item);

		if (isTargetPortal(gp->portal_name, portal_name))
		{
			ELOG_DISPATCHER_DEBUG("Returning a reader N-gang to the available list");

			/* cur_item must be removed */
			allocatedGangs = list_delete_cell(allocatedGangs,
													 cur_item, prev_item);

			releaseGang(gp, false);

			cur_item = next_item;
		}
		else
		{
			ELOG_DISPATCHER_DEBUG("Skipping the release of a reader N-gang. It is used by another portal");

			/* cur_item must be preserved */
			prev_item = cur_item;
			cur_item = next_item;
		}
	}

	MemoryContextSwitchTo(oldContext);
}

/*
 * Drop any temporary tables associated with the current session and
 * use a new session id since we have effectively reset the session.
 *
 * Call this procedure outside of a transaction.
 */
void
CheckForResetSession(void)
{
	int			oldSessionId = 0;
	int			newSessionId = 0;
	Oid			dropTempNamespaceOid;

	if (!NeedResetSession)
		return;

	/* Do the session id change early. */

	/* If we have gangs, we can't change our session ID. */

	oldSessionId = gp_session_id;
	ProcNewMppSessionId(&newSessionId);

	gp_session_id = newSessionId;
	gp_command_count = 0;

	/* Update the slotid for our singleton reader. */
	if (SharedLocalSnapshotSlot != NULL)
	{
		LWLockAcquire(SharedLocalSnapshotSlot->slotLock, LW_EXCLUSIVE);
		SharedLocalSnapshotSlot->slotid = gp_session_id;
		LWLockRelease(SharedLocalSnapshotSlot->slotLock);
	}

	elog(LOG, "The previous session was reset because its gang was disconnected (session id = %d). "
		 "The new session id = %d", oldSessionId, newSessionId);

	if (IsTransactionOrTransactionBlock())
	{
		NeedResetSession = false;
		return;
	}

	dropTempNamespaceOid = OldTempNamespace;
	OldTempNamespace = InvalidOid;
	NeedResetSession = false;

	if (dropTempNamespaceOid != InvalidOid)
	{
		PG_TRY();
		{
			DropTempTableNamespaceForResetSession(dropTempNamespaceOid);
		} PG_CATCH();
		{
			/*
			 * But first demote the error to something much less scary.
			 */
			if (!elog_demote(WARNING))
			{
				elog(LOG, "unable to demote error");
				PG_RE_THROW();
			}

			EmitErrorReport();
			FlushErrorState();
		} PG_END_TRY();
	}
}

static void
resetSessionForPrimaryGangLoss(void)
{
	if (ProcCanSetMppSessionId())
	{
		/*
		 * Not too early.
		 */
		NeedResetSession = true;

		/*
		 * Keep this check away from transaction/catalog access, as we are
		 * possibly just after releasing ResourceOwner at the end of Tx. It's
		 * ok to remember uncommitted temporary namespace because
		 * DropTempTableNamespaceForResetSession will simply do nothing if the
		 * namespace is not visible.
		 */
		if (TempNamespaceOidIsValid())
		{
			/*
			 * Here we indicate we don't have a temporary table namespace
			 * anymore so all temporary tables of the previous session will be
			 * inaccessible.  Later, when we can start a new transaction, we
			 * will attempt to actually drop the old session tables to release
			 * the disk space.
			 */
			OldTempNamespace = ResetTempNamespace();

			elog(WARNING,
				 "Any temporary tables for this session have been dropped "
				 "because the gang was disconnected (session id = %d)",
				 gp_session_id);
		}
		else
			OldTempNamespace = InvalidOid;
	}
}

/*
 * Helper functions
 */

int			gp_pthread_create(pthread_t *thread, void *(*start_routine) (void *),
							  void *arg, const char *caller)
{
	int			pthread_err = 0;
	pthread_attr_t t_atts;

	/*
	 * Call some init function. Before any thread is created, we need to init
	 * some static stuff. The main purpose is to guarantee the non-thread safe
	 * stuff are called in main thread, before any child thread get running.
	 * Note these staic data structure should be read only after init.	Thread
	 * creation is a barrier, so there is no need to get lock before we use
	 * these data structures.
	 *
	 * So far, we know we need to do this for getpwuid_r (See MPP-1971, glibc
	 * getpwuid_r is not thread safe).
	 */
#ifndef WIN32
	get_gp_passwdptr();
#endif

	/*
	 * save ourselves some memory: the defaults for thread stack size are
	 * large (1M+)
	 */
	pthread_err = pthread_attr_init(&t_atts);
	if (pthread_err != 0)
	{
		elog(LOG, "%s: pthread_attr_init failed.  Error %d", caller, pthread_err);
		return pthread_err;
	}

	pthread_err = pthread_attr_setstacksize(&t_atts,
											Max(PTHREAD_STACK_MIN, (256 * 1024)));
	if (pthread_err != 0)
	{
		elog(LOG, "%s: pthread_attr_setstacksize failed.  Error %d", caller, pthread_err);
		pthread_attr_destroy(&t_atts);
		return pthread_err;
	}

	pthread_err = pthread_create(thread, &t_atts, start_routine, arg);

	pthread_attr_destroy(&t_atts);

	return pthread_err;
}

const char *
gangTypeToString(GangType type)
{
	const char *ret = "";

	switch (type)
	{
		case GANGTYPE_PRIMARY_WRITER:
			ret = "primary writer";
			break;
		case GANGTYPE_PRIMARY_READER:
			ret = "primary reader";
			break;
		case GANGTYPE_SINGLETON_READER:
			ret = "singleton reader";
			break;
		case GANGTYPE_ENTRYDB_READER:
			ret = "entry DB reader";
			break;
		case GANGTYPE_UNALLOCATED:
			ret = "unallocated";
			break;
		default:
			Assert(false);
	}
	return ret;
}

bool
GangOK(Gang *gp)
{
	int			i;

	if (gp == NULL)
		return false;

	if (gp->gang_id < 1 ||
		gp->gang_id > 100000000 ||
		gp->type > GANGTYPE_PRIMARY_WRITER ||
		(gp->size != getgpsegmentCount() && gp->size != 1))
		return false;

	/*
	 * Gang is direct-connect (no agents).
	 */

	for (i = 0; i < gp->size; i++)
	{
		SegmentDatabaseDescriptor *segdbDesc = gp->db_descriptors[i];

		if (cdbconn_isBadConnection(segdbDesc))
			return false;
		if (!FtsIsSegmentUp(segdbDesc->segment_database_info))
			return false;
	}

	return true;
}

#if 0
bool
GangsExist(void)
{
	return (primaryWriterGang != NULL ||
			allocatedReaderGangsN != NIL ||
			availableReaderGangsN != NIL ||
			allocatedReaderGangs1 != NIL ||
			availableReaderGangs1 != NIL);
}

static bool
readerGangsExist(void)
{
	return (allocatedReaderGangsN != NIL ||
			availableReaderGangsN != NIL ||
			allocatedReaderGangs1 != NIL ||
			availableReaderGangs1 != NIL);
}
#endif

int
largestGangsize(void)
{
	return largest_gangsize;
}

void
setLargestGangsize(int size)
{
	if (largest_gangsize < size)
		largest_gangsize = size;
}

void
cdbgang_setAsync(bool async)
{
	if (async)
		pCreateGangFunc = pCreateGangFuncAsync;
	else
		pCreateGangFunc = pCreateGangFuncThreaded;
}

/*
 * Create a writer gang.
 */
Gang *
AllocateWriterGang(List *segments)
{
	Gang	   *newGang = NULL;

	ELOG_DISPATCHER_DEBUG("AllocateWriterGang begin.");

	if (Gp_role != GP_ROLE_DISPATCH)
	{
		elog(FATAL, "dispatch process called with role %d", Gp_role);
	}

	Assert(allocatedGangs == NIL);

	newGang = AllocateGang(GANGTYPE_PRIMARY_WRITER, segments);

	addToTwoPhaseRegion(newGang);

	primaryWriterGang = newGang;

	return newGang;
}

Gang *
AllocateGang(GangType type, List *segments)
{
	MemoryContext oldContext = NULL;
	Gang	   *newGang = NULL;

	if (Gp_role != GP_ROLE_DISPATCH)
	{
		elog(FATAL, "dispatch process called with role %d", Gp_role);
	}

	if (segments == NULL)
		return NULL;

	insist_log(IsTransactionOrTransactionBlock(),
			   "cannot allocate segworker group outside of transaction");

	if (GangContext == NULL)
	{
		GangContext = AllocSetContextCreate(TopMemoryContext, "Gang Context", ALLOCSET_DEFAULT_MINSIZE,
											ALLOCSET_DEFAULT_INITSIZE,
											ALLOCSET_DEFAULT_MAXSIZE);
	}
	Assert(GangContext != NULL);
	oldContext = MemoryContextSwitchTo(GangContext);

	newGang = createGang(type, segments);
	newGang->allocated = true;

	addGangToAllocated(newGang);

	MemoryContextSwitchTo(oldContext);
	return newGang;
}

/*
 * Creates a new gang by logging on a session to each segDB involved.
 *
 * call this function in GangContext memory context.
 * elog ERROR or return a non-NULL gang.
 */
static Gang *
createGang(GangType type, List *segments)
{
	return createGang_asyncV1(type, segments);
}

Gang *
buildGangDefinition(GangType type, List *segments)
{
	Gang	   *newGangDefinition = NULL;

	Assert(CurrentMemoryContext == GangContext);

	/* allocate a gang */
	newGangDefinition = (Gang *) palloc0(sizeof(Gang));
	newGangDefinition->type = type;
	newGangDefinition->size = list_length(segments);
	newGangDefinition->gang_id = -1;
	newGangDefinition->allocated = false;
	newGangDefinition->noReuse = false;
	newGangDefinition->dispatcherActive = false;
	newGangDefinition->portal_name = NULL;
	newGangDefinition->db_descriptors = createSegdbDescForGang(segments);

	ELOG_DISPATCHER_DEBUG("buildGangDefinition done");
	return newGangDefinition;
}


static void
addToTwoPhaseRegion(Gang *gang)
{
	Assert(GangContext != NULL);

	MemoryContext oldContext = NULL;
	oldContext = MemoryContextSwitchTo(GangContext);

	int m;
	for(m = 0; m < gang->size; m++)
	{
		SegmentDatabaseDescriptor *desc = gang->db_descriptors[m];

		if (bms_is_member(desc->segindex, twophaseRegion))	
			continue;

		twophaseRegion = bms_add_member(twophaseRegion,
													desc->segindex);

		twophaseSegments = lappend_int(twophaseSegments, desc->segindex);
	}

	MemoryContextSwitchTo(oldContext);
}

void
releaseGang(Gang *gp, bool noReuse)
{
	int			i = 0;

	if (!gp)
		return;

	ELOG_DISPATCHER_DEBUG("cleanupGang: cleaning gang id %d type %d size %d, "
						  "was used for portal: %s",
						  gp->gang_id, gp->type, gp->size,
						  (gp->portal_name ? gp->portal_name : "(unnamed)"));

	/*
	 * if the process is in the middle of blowing up... then we don't do
	 * anything here.  making libpq and other calls can definitely result in
	 * things getting HUNG.
	 */
	if (proc_exit_inprogress)
		return;

	for (i = 0; i < gp->size; i++)
	{
		SegmentDatabaseDescriptor *segdbDesc = gp->db_descriptors[i];	

		Assert(segdbDesc != NULL);

		if (noReuse || gp->noReuse)
			goto destroy;

		if (cdbconn_isBadConnection(segdbDesc))
			goto destroy;

		/* if segment is down, the gang can not be reused */
		if (!FtsIsSegmentUp(segdbDesc->segment_database_info))
			goto destroy;

		/* Note, we cancel all "still running" queries */
		if (!cdbconn_discardResults(segdbDesc, 20))
			elog(FATAL, "cleanup called when a segworker is still busy");

		recycleSegdbDesc(segdbDesc);

		continue;

destroy:
		destroySegdbDesc(segdbDesc);
	}

	pfree(gp);
}

static void
destroySegdbDesc(SegmentDatabaseDescriptor *segdbDesc)
{
	SegdbDescCache *cache = segdbDesc->cache; 
	cdbconn_disconnect(segdbDesc);
	cdbconn_termSegmentDescriptor(segdbDesc);

	if (segdbDesc->isWriter)
		cache->hasWriter = false;

	pfree(segdbDesc);
}

static void
recycleSegdbDesc(SegmentDatabaseDescriptor *segdbDesc)
{
	SegdbDescCache *cache = segdbDesc->cache; 

	/* alway put writer to header */
	if (segdbDesc->isWriter)	
	{
		DoublyLinkedHead_AddFirst(offsetof(SegmentDatabaseDescriptor, cachelist),
								  &cache->idle_dbs,
								  segdbDesc);
	}
	else
	{
		DoublyLinkedHead_AddLast(offsetof(SegmentDatabaseDescriptor, cachelist),
								  &cache->idle_dbs,
								  segdbDesc);
	}

	/* Add it to freelist either */
	DoublyLinkedHead_AddLast(offsetof(SegmentDatabaseDescriptor, freelist),
								  &SessionSegdbDescsManager->idle_dbs,
								  segdbDesc);

}

static void
InitSessionSegdbDescManager(void)
{
	HASHCTL     info;
	int         hash_flags;

	Assert(SessionSegdbDescsManager == NULL);

	SessionSegdbDescsManager = palloc(sizeof(SegdbDescsManager));

	/* Initialize HTAB */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(int32);
	info.entrysize = sizeof(SegdbDescCache);
	info.hash = int32_hash;
	info.num_partitions = NUM_LOCK_PARTITIONS;
	hash_flags = (HASH_ELEM | HASH_FUNCTION);

	SessionSegdbDescsManager->cacheHash = hash_create("segdb description cache", 300, &info, hash_flags);

	/* initial quick access list */
	DoublyLinkedHead_Init(&SessionSegdbDescsManager->idle_dbs);
}

static SegmentDatabaseDescriptor **
createSegdbDescForGang(List *segments)
{
	SegmentDatabaseDescriptor	**db_descriptors;
	SegmentDatabaseDescriptor	*segdbDesc;
	int 						count, size, segindex;
	ListCell					*lc;

	if (SessionSegdbDescsManager == NULL)
		InitSessionSegdbDescManager();

	size = list_length(segments);
	db_descriptors = (SegmentDatabaseDescriptor **)
							palloc0(size * sizeof(SegmentDatabaseDescriptor *));

	count = 0;	
	foreach(lc, segments)
	{
		segindex = lfirst_int(lc);
	
		segdbDesc = allocateSegdbDesc(segindex);

		db_descriptors[count++] = segdbDesc;
	}

	return db_descriptors;
}

static SegmentDatabaseDescriptor*
allocateSegdbDesc(int segindex)
{
	SegmentDatabaseDescriptor 	*segdbDesc;
	SegmentDatabaseDescriptor 	*result = NULL;
	CdbComponentDatabaseInfo	*cdbInfoCopy;
	CdbComponentDatabaseInfo	*cdbinfo;
	SegdbDescCache				*cache = NULL;
	bool						found = false;	

	/* read gp_segment_configuration and build CdbComponentDatabases */
	cdb_component_dbs = getComponentDatabases();

	if (cdb_component_dbs == NULL ||
		cdb_component_dbs->total_segments <= 0 ||
		cdb_component_dbs->total_segment_dbs <= 0)
		insist_log(false, "schema not populated while building segworker group");

	if (segindex == -1)
		cdbinfo = &cdb_component_dbs->entry_db_info[0];
	else
	{
		cdbinfo = &cdb_component_dbs->segment_db_info[2*segindex];

		/* Q: if both primary and mirror are down, what is their role? */
		if (!SEGMENT_IS_ACTIVE_PRIMARY(cdbinfo))
		{
			/* FATAL level error, process will be cleanup */
			elog(FATAL, "segment %d is down", segindex);
		}
	}

	cache = hash_search(SessionSegdbDescsManager->cacheHash, (void *)&segindex, HASH_ENTER, &found); 

	if (!found)
	{
		cache->segindex = segindex;
		cache->hasWriter = false;
		DoublyLinkedHead_Init(&cache->idle_dbs);
	}

	while (cache->idle_dbs.count != 0)
	{
		/* get a cached segdbDesc from cachelist */
		segdbDesc = (SegmentDatabaseDescriptor*)DoublyLinkedHead_RemoveFirst(offsetof(SegmentDatabaseDescriptor, cachelist),
																			 &cache->idle_dbs);

		/* remove from quick access list */
		DoubleLinks_Remove(offsetof(SegmentDatabaseDescriptor, freelist),
							 	 &SessionSegdbDescsManager->idle_dbs,
							 	 segdbDesc);

		/* check if cache match to latest cdbinfo */
		if (segdbDesc->segment_database_info->segindex != cdbinfo->segindex ||
			segdbDesc->segment_database_info->dbid != cdbinfo->dbid ||
			cdbconn_isBadConnection(segdbDesc) ||
			!FtsIsSegmentUp(segdbDesc->segment_database_info))
		{
			destroySegdbDesc(segdbDesc);
			continue;
		}
		else
		{
			result = segdbDesc;	
			break;
		}
	}

	if (!result)
	{
		segdbDesc = palloc0(sizeof(SegmentDatabaseDescriptor)); 
		cdbInfoCopy = copyCdbComponentDatabaseInfo(cdbinfo);
		cdbconn_initSegmentDescriptor(segdbDesc, cdbInfoCopy);
		setQEIdentifier(segdbDesc, -1, GangContext);
		DoubleLinks_Init(&segdbDesc->cachelist);	
		DoubleLinks_Init(&segdbDesc->freelist);	
		segdbDesc->isWriter = !cache->hasWriter;
		segdbDesc->cache = cache;
		result = segdbDesc;
	}

	return result;
}

static void
cleanupSessionSegdbDescsManager(void)
{
	SegmentDatabaseDescriptor	*segdbDesc;

	while(SessionSegdbDescsManager->idle_dbs.count != 0)
	{
		segdbDesc = (SegmentDatabaseDescriptor*)DoublyLinkedHead_RemoveFirst(offsetof(SegmentDatabaseDescriptor, freelist),
																			 &SessionSegdbDescsManager->idle_dbs);
		/* remove from quick access list */
		DoubleLinks_Remove(offsetof(SegmentDatabaseDescriptor, cachelist),
						   &segdbDesc->cache->idle_dbs, segdbDesc);

		cdbconn_disconnect(segdbDesc);
		cdbconn_termSegmentDescriptor(segdbDesc);

		if (segdbDesc->isWriter)
			segdbDesc->cache->hasWriter = false;

		pfree(segdbDesc);
	}
}

List *
makeIdleSegments(void)
{
	SegmentDatabaseDescriptor	*segdbDesc;
	List						*segments = NIL;	

	if (SessionSegdbDescsManager == NULL)
		return NULL;

	segdbDesc = DoublyLinkedHead_First(offsetof(SegmentDatabaseDescriptor, freelist),
									   &SessionSegdbDescsManager->idle_dbs);

	while (segdbDesc)
	{
		segments = lappend_int(segments, segdbDesc->segindex);	

		segdbDesc = DoublyLinkedHead_Next(offsetof(SegmentDatabaseDescriptor, freelist),
									   &SessionSegdbDescsManager->idle_dbs,
									   segdbDesc);
	}

	return segments;
}
