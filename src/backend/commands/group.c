/*-------------------------------------------------------------------------
 *
 * group.c
 *	  Commands for manipulating resource group.
 *
 * Copyright (c) 2006-2017, Greenplum inc.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/commands/group.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_resqueue.h"
#include "nodes/makefuncs.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbdisp_query.h"
#include "commands/comment.h"
#include "commands/defrem.h"
#include "commands/group.h"
#include "libpq/crypt.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/flatfiles.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "executor/execdesc.h"
#include "utils/syscache.h"
#include "cdb/memquota.h"
#include "utils/guc_tables.h"

#define RESGROUP_DEFAULT_CONCURRENCY (20)
#define RESGROUP_DEFAULT_REDZONE_LIMIT (0.8)


typedef ResourceGroupOptions
{
	int concurrency;
	float cpuRateLimit;
	float memoryLimit;
	float redzoneLimit;	
}ResourceGroupOptions;

void
CreateResourceGroup(CreateResourceGroupStmt *stmt)
{
	Relation	pg_resgroup_rel;
	TupleDesc	pg_resgroup_dsc;
	ScanKeyData scankey;
	SysScanDesc sscan;
	HeapTuple	tuple;
	Oid			groupid;
	Datum		new_record[Natts_pg_resgroup];
	bool		new_record_nulls[Natts_pg_resgroup];
	ListCell    *pWithList = NULL;
	ResourceGroupOptions options;

	/* Permission check - only superuser can create groups. */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to create resource groups")));

	/*
	 * Check for an illegal name ('none' is used to signify no group in ALTER
	 * ROLE).
	 */
	if (strcmp(stmt->name, "none") == 0)
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("resource group name \"none\" is reserved")));

	MemSet(&options, 0, sizeof(options));
	parseStmtOptions(stmt, &options, true);

	/*
	 * Check the pg_resgroup relation to be certain the group doesn't already
	 * exist. 
	 */
	pg_resgroup_rel = heap_open(ResGroupRelationId, RowExclusiveLock);
	pg_resgroup_dsc = RelationGetDescr(pg_resgroup_rel);

	ScanKeyInit(&scankey,
				Anum_pg_resgroup_rsgname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(stmt->name));

	sscan = systable_beginscan(pg_resgroup_rel, ResGroupRsgnameIndexId, true,
							   SnapshotNow, 1, &scankey);

	if (HeapTupleIsValid(systable_getnext(sscan)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("resource group \"%s\" already exists",
						stmt->name)));

	systable_endscan(sscan);

	/*
	 * Build a tuple to insert
	 */
	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, false, sizeof(new_record_nulls));

	new_record[Anum_pg_resgroup_rsgname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(stmt->name));

	new_record[Anum_pg_resgroup_parent - 1] = Int64GetDatum(0);

	tuple = heap_form_tuple(pg_resgroup_dsc, new_record, new_record_nulls);

	/*
	 * Insert new record in the pg_resgroup table
	 */
	groupid = simple_heap_insert(pg_resgroup_rel, tuple);
	CatalogUpdateIndexes(pg_resgroup_rel, tuple);

	/* process the remainder of the WITH (...) list items */
	UpdateResgroupCapability(groupid, options);

	/* 
	 * We must bump the command counter to make the new entry 
	 * in the pg_resqueuecapability table visible 
	 */
	CommandCounterIncrement();

	/*
	 * Create the in-memory resource queue, if resource scheduling is on,
	 * otherwise don't - and gripe a little about it.
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		if (ResourceScheduler)
		{
			LWLockAcquire(ResQueueLock, LW_EXCLUSIVE);

			thresholds[RES_COUNT_LIMIT]	 = activelimit;
			thresholds[RES_COST_LIMIT]	 = costlimit;

			thresholds[RES_MEMORY_LIMIT] = 
					ResourceQueueGetMemoryLimit(queueid);
			queueok	= ResCreateResourceGroup(queueid, 
									 thresholds, 
									 overcommit, 
									 ignorelimit);
			
			/**
			 * Ensure that the shared data structures are consistent with
			 * the catalog table on memory limits.
			 */
#ifdef USE_ASSERT_CHECKING
			AssertMemoryLimitsMatch();
#endif
		
			LWLockRelease(ResQueueLock);

			if (!queueok)
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
						 errmsg("insufficient resource queues available"),
						 errhint("Increase max_resource_queues")));
		}
		else
		{
				ereport(WARNING,
						(errmsg("resource scheduling is disabled"),
						 errhint("To enable set resource_scheduler=on")));
		}
	}

	/* MPP-6929, MPP-7583: metadata tracking */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbDispatchUtilityStatement((Node *) stmt,
									DF_CANCEL_ON_ERROR|
									DF_WITH_SNAPSHOT|
									DF_NEED_TWO_PHASE,
									GetAssignedOidsForDispatch(),
									NULL);
		MetaTrackAddObject(ResQueueRelationId,
						   queueid,
						   GetUserId(), /* not ownerid */
						   "CREATE", "RESOURCE QUEUE"
				);
	}

	heap_close(resqueueCapabilityRel, NoLock);
	heap_close(pg_resqueue_rel, NoLock);

/*
 * bCreate == true means this is called by Create stmt
 */
static void
UpdateResgroupCapability(Oid groupid,
						ResourceGroupOptions *options)
{
	char *value; 
	Relation resgroup_capability_rel = heap_open(ResQueueCapabilityRelationId, ExclusiveLock);

	validateCapabilities(resgroup_capability_rel, options);

	value = psprintf("%d", options->concurrency);
	insertTupleInCapability(resgroup_capability_rel, groupid, RESGROUP_LIMIT_TYPE_CONCURRENCY, value);
	value = psprintf("%d", options->:wa);
	insertTupleInCapability(resgroup_capability_rel, groupid, RESGROUP_LIMIT_TYPE_CPU, value);
	value = psprintf("%d", options->concurrency);
	insertTupleInCapability(resgroup_capability_rel, groupid, RESGROUP_LIMIT_TYPE_MEMORY, value);
	value = psprintf("%d", options->concurrency);
	insertTupleInCapability(resgroup_capability_rel, groupid, RESGROUP_LIMIT_TYPE_MEMORY_REDZONE, value);
}

static void insertTupleInCapability(Relation rel,
									Oid groupid,
									uint16 type,
									char *value)
{
	Datum new_record[Natts_pg_resgroupcapability];
	bool new_record_nulls[Natts_pg_resgroupcapability];
	HeapTuple tuple;
	TupleDesc tupleDesc = RelationGetDescr(rel);

	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, false, sizeof(new_record_nulls));

	new_record[Anum_pg_resgroupcapability_resgroupid - 1] = ObjectIdGetDatum(groupid);
	new_record[Anum_pg_resgroupcapability_reslimittype - 1] = UInt16GetDatum(type);
	new_record[Anum_pg_resgroupcapability_value - 1] = CStringGetDatum(value);
	new_record[Anum_pg_resgroupcapability_proposed - 1] = CStringGetDatum(proposed);

	tuple = heap_form_tuple(tupleDesc, new_record, new_record_nulls);
	simple_heap_insert(resgroup_capability_rel, tuple);
}

static ResgroupOptionType
getResgroupOptionType(const char* defname)
{
	if (strcmp(defel->defname, "cpu_rate_limit") == 0)
		return RESGROUP_LIMIT_TYPE_CPU;
	else if (strcmp(defel->defname, "memory_limit") == 0)
		return RESGROUP_LIMIT_TYPE_MEMORY;
	else if (strcmp(defel->defname, "concurrency") == 0)
		return RESGROUP_LIMIT_TYPE_CONCURRENCY;
	else if (strcmp(defel->defname, "memory_redzone_limit") == 0)
		return RESGROUP_LIMIT_TYPE_MEMORY_REDZONE;
	else
		return RESGROUP_LIMIT_TYPE_UNKNOW;	
}

static void 
parseStmtOptions(CreateResourceGroupStmt *stmt, ResourceGroupOptions *options, bool isCreate)
{
	ListCell *cell;

	foreach(cell, stmt->options)
	{
		DefElem *defel = (DefElem *) lfirst(cell);
		int type = getResgroupOptionType(defel->defname);

		if (type == RESGROUP_LIMIT_TYPE_UNKNOWN)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("option \"%s\" not recognized", defel->defname)));

		switch (type)
		{
			case RESGROUP_LIMIT_TYPE_CONCURRENCY:
				options->concurrency = defGetInt64(defel);	
				if (options->concurrency < 0 || options->concurrency > MaxConnections)
					ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 	errmsg("concurrency range is [0, MaxConnections]")));
				break;

			case RESGROUP_LIMIT_TYPE_CPU:
				options->cpuRateLimit = roundf(defGetNumeric(defel) * 100) / 100;
				if (options->cpuRateLimit <= .01f || options->cpuRateLimit >= 1.0)
					ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 	errmsg("cpu_rate_limit range is (.01, 1)")));
				break;

			case RESGROUP_LIMIT_TYPE_MEMORY:
				options->memoryLimit = roundf(defGetNumeric(defel) * 100) / 100; 
				if (options->memoryLimit <= .01f || options->memoryLimit >= 1.0)
					ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 	errmsg("memory_limit range is (.01, 1)")));
				break;

			case RESGROUP_LIMIT_TYPE_MEMORY_REDZONE:
				options->redzoneLimit = roundf(defGetNumeric(defel) * 100) / 100;
				if (options->redzoneLimit <= .5f || options->redzoneLimit > 1.0)
					ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 	errmsg("memory_redzone_limit range is (.5, 1]")));
				break;

			default:
				Assert(!"unexpected options");
				break;
		}
	}

	if (isCreate && (options->memoryLimit == 0 || options->cpuRateLimit == 0))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 	errmsg("must specify both memory_limit and cpu_rate_limit")));

	if (options->concurrency == 0)
		options->concurrency = RESGROUP_DEFAULT_CONCURRENCY;

	if (options->redzoneLimit == 0)
		options->concurrency = RESGROUP_DEFAULT_REDZONE_LIMIT;
}

static void 
validateCapabilities(Relation rel, ResourceGroupOptions *options)
{
	HeapTuple tuple;
	SysScanDesc sscan;
	float totalCpu = options->cpuRateLimit;
	float totalMem = options->memoryLimit;

	sscan = systable_beginscan(rel, InvalidOid, false, SnapshotNow, 0, NULL);

	while (HeapTupleIsValid(tuple = systable_getnext(sscan)))
	{
		FormData_pg_resgroupcapability resgCapability = 
						(FormData_pg_resgroupcapability)GETSTRUCT(tuple);
		if (resgCapability->reslimittype == RESGROUP_LIMIT_TYPE_CPU)
		{
			totalCpu += resgCapability->value;
			if (totalCpu > 1.0)
				ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 	errmsg("total cpu_rate_limit exceeded the limit of 1.0")));
		}
		else if (resgCapability->reslimittype == RESGROUP_LIMIT_TYPE_MEMORY)
		{
			totalMem += resgCapability->value;
			if (totalCpu > 1.0)
				ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 	errmsg("total memory_limit exceeded the limit of 1.0")));
		}
	}

	systable_endscan(sscan);
}
