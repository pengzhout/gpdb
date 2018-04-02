/*-------------------------------------------------------------------------
 *
 * regioncmds.c
 *	  Commands for manipulating regions.
 *
 * Portions Copyright (c) 2006-2017, Greenplum inc.
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 * IDENTIFICATION
 *    src/backend/commands/regioncmds.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "funcapi.h"
#include "libpq-fe.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_resgroup.h"
#include "catalog/pg_resgroupcapability.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbvars.h"
#include "commands/comment.h"
#include "commands/defrem.h"
#include "commands/resgroupcmds.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/fmgroids.h"
#include "utils/resgroup.h"
#include "utils/resgroup-ops.h"
#include "utils/resource_manager.h"
#include "utils/resowner.h"
#include "utils/syscache.h"

void
CreateRegion(CreateRegionStmt *stmt)
{
	Relation	pg_resgroup_rel;
	Relation	pg_resgroupcapability_rel;
	TupleDesc	pg_resgroup_dsc;
	ScanKeyData scankey;
	SysScanDesc sscan;
	HeapTuple	tuple;
	Oid			groupid;
	Datum		new_record[Natts_pg_resgroup];
	bool		new_record_nulls[Natts_pg_resgroup];
	ResGroupOptions groupOptions;

	List		*members;
	GpRegion	*region;

	/* Permission check - only superuser can create groups. */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to create region")));

	region = makeNode();
	foreach(cell, stmt->def)
	{
		DefElem *defel = (DefElem *) lfirst(cell);
		if (strcmp(defel->defname, "hosts") == 0)	
		{
					//TODO			
		}
		else if (strcmp(defel->defname, "dbs") == 0)
		{
		
		}
		else
		{
			ereport(ERROR, "unrecognized option: %s", defel->defname);
		}
	}

	pg_resgroup_rel = heap_open(ResGroupRelationId, RowExclusiveLock);

	/* Check if MaxResourceGroups limit is reached */
	sscan = systable_beginscan(pg_resgroup_rel, ResGroupRsgnameIndexId, false,
							   SnapshotNow, 0, NULL);
	nResGroups = 0;
	while (systable_getnext(sscan) != NULL)
		nResGroups++;
	systable_endscan(sscan);

	if (nResGroups >= MaxResourceGroups)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("insufficient resource groups available")));

	/*
	 * Check the pg_resgroup relation to be certain the group doesn't already
	 * exist.
	 */
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
	new_record[Anum_pg_resgroup_parent - 1] = ObjectIdGetDatum(0);
	new_record[Anum_pg_resgroup_memauditor - 1] = Int32GetDatum(groupOptions.memAuditor);

	pg_resgroup_dsc = RelationGetDescr(pg_resgroup_rel);
	tuple = heap_form_tuple(pg_resgroup_dsc, new_record, new_record_nulls);

	/*
	 * Insert new record in the pg_resgroup table
	 */
	groupid = simple_heap_insert(pg_resgroup_rel, tuple);
	CatalogUpdateIndexes(pg_resgroup_rel, tuple);

	/* process the WITH (...) list items */
	validateCapabilities(pg_resgroupcapability_rel, groupid, &groupOptions.caps, true);
	insertResgroupCapabilities(pg_resgroupcapability_rel, groupid, &groupOptions.caps);

	/* Dispatch the statement to segments */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbDispatchUtilityStatement((Node *) stmt,
									DF_CANCEL_ON_ERROR|
									DF_WITH_SNAPSHOT|
									DF_NEED_TWO_PHASE,
									GetAssignedOidsForDispatch(),
									NULL);
		MetaTrackAddObject(ResGroupRelationId,
						   groupid,
						   GetUserId(), /* not ownerid */
						   "CREATE", "RESOURCE GROUP");
	}

	heap_close(pg_resgroup_rel, NoLock);
	heap_close(pg_resgroupcapability_rel, NoLock);

	/* Add this group into shared memory */
	if (IsResGroupActivated())
	{
		Oid			*callbackArg;

		AllocResGroupEntry(groupid, &groupOptions);

		/* Argument of callback function should be allocated in heap region */
		callbackArg = (Oid *)MemoryContextAlloc(TopMemoryContext, sizeof(Oid));
		*callbackArg = groupid;
		RegisterXactCallbackOnce(createResgroupCallback, (void *)callbackArg);

		/* Create os dependent part for this resource group */
		ResGroupOps_CreateGroup(groupid);
		ResGroupOps_SetCpuRateLimit(groupid, groupOptions.caps.cpuRateLimit);
		ResGroupOps_SetMemoryLimit(groupid, groupOptions.caps.memLimit);
	}
	else if (Gp_role == GP_ROLE_DISPATCH)
		ereport(WARNING,
				(errmsg("resource group is disabled"),
				 errhint("To enable set gp_resource_manager=group")));
}
