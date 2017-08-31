/*-------------------------------------------------------------------------
 *
 * resgroup.c
 *	  GPDB resource group management code.
 *
 *
 * Copyright (c) 2006-2017, Greenplum inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_resgroup.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/memquota.h"
#include "commands/resgroupcmds.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "storage/pg_shmem.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/resgroup-ops.h"
#include "utils/resgroup.h"
#include "utils/resource_manager.h"
#include "utils/resowner.h"
#include "utils/session_state.h"
#include "utils/vmem_tracker.h"

#define InvalidSlotId	(-1)
#define RESGROUP_MAX_SLOTS	300

#define SLOT_RELEASE 'r'
#define SLOT_DROP    'd'
#define SLOT_ALTER   'a'

/*
 * GUC variables.
 */
char                		*gp_resgroup_memory_policy_str = NULL;
ResManagerMemoryPolicy     	gp_resgroup_memory_policy = RESMANAGER_MEMORY_POLICY_NONE;
bool						gp_log_resgroup_memory = false;
int							gp_resgroup_memory_policy_auto_fixed_mem;
bool						gp_resgroup_print_operator_memory_limits = false;
int							memory_spill_ratio=20;

/*
 * Data structures
 */

typedef struct ResGroupHashEntry
{
	Oid		groupId;
	int		index;
} ResGroupHashEntry;

/*
 * Per slot resource group information.
 *
 * Resource group have 'concurrency' number of slots.
 * Each transaction acquires a slot on master before running.
 * The information shared by QE processes on each segments are stored
 * in this structure.
 */
typedef struct ResGroupSlotData
{
	int				slotId;
	int				sessionId;

	ResGroupCaps	caps;

	uint32	memQuota;	/* memory quota of current slot */
	uint32	memInSlot;	/* total memory usage of procs belongs to this slot */
	uint32	memInShare;	/* total memory usage of procs belongs to this slot */
	int				nProcs;		/* number of procs in this slot */
	bool			inUse;
} ResGroupSlotData;

/*
 * Resource group information.
 */
typedef struct ResGroupData
{
	Oid			groupId;		/* Id for this group */
	int			index;
	ResGroupCaps	caps;
	uint32		nRunning;		/* number of running trans */
	PROC_QUEUE	waitProcs;
	int			totalExecuted;	/* total number of executed trans */
	int			totalQueued;	/* total number of queued trans	*/
	Interval	totalQueuedTime;/* total queue time */

	uint32	memGranted;	/* realtime group mem limit */
	uint32	memQuotaGranted;	/* memory chunks for quota part */
	uint32	memSharedGranted;	/* memory chunks for shared part */

	bool		lockForDrop;

	/*
	 * memory usage of this group, should always equal to the
	 * sum of session memory(session_state->sessionVmem) that
	 * belongs to this group
	 */
	uint32	memUsage;
	uint32	memQuotaUsage;		/* memory chunks assigned to all the running slots */
	uint32	memSharedUsage;

	ResGroupSlotData slots[RESGROUP_MAX_SLOTS];
} ResGroupData;

/*
 * All backend will be assigned with a governor to manage
 * memory and slot.
 */
typedef struct ResGroupGovernor
{
	Oid		groupId;
	ResGroupData* group; // link to the group in the shared memory
	ResGroupSlotData* slot; // link to the slot of group in the shared memory
} ResGroupGovernor;

typedef struct ResGroupMemSets
{
	ResGroupGovernor		governor;
	int						memUsage;
} ResGroupMemSets;

ResGroupGovernor CurrentResGroupGovernor = {InvalidOid, NULL, NULL};

static ResGroupMemSets rg_mem_sets = {{InvalidOid, NULL, NULL}, 0};

#define IsMemSetGoverned(sets) \
	(rg_mem_sets.governor.groupId != InvalidOid)

#define IsMemSetGovernedByMe(gonvernor) \
	(rg_mem_sets.governor.groupId == governor->groupId && \
	 rg_mem_sets.governor.slot != NULL &&\
	 rg_mem_sets.governor.slot->slotId == governor->slot->slotId)

#define AssignMemSetGovernor(gonvernor) \
	(rg_mem_sets.governor = *governor)

typedef struct ResGroupControl
{
	HTAB			*htbl;
	int 			segmentsOnMaster;

	/*
	 * The hash table for resource groups in shared memory should only be populated
	 * once, so we add a flag here to implement this requirement.
	 */
	bool			loaded;

	uint32	totalChunks;	/* total memory chuncks on this segment */
	uint32  freeChunks;		/* memory chuncks not allocated to any group */

	int				nGroups;
	LWLockId		baseLWLockId;
	ResGroupData	groups[1];
} ResGroupControl;

/* GUC */
int		MaxResourceGroups;

/* static variables */
static ResGroupControl *pResGroupControl = NULL;

static Oid newGroupIdFromQD = InvalidOid;
static int newSlotIdFromQD = InvalidSlotId;
static ResGroupCaps newCapsFromQD;


/* static functions */
static int32 getSegmentChunks(void);

static int32 groupGetMemExpected(const ResGroupCaps *caps);
static int32 groupGetMemQuotaExpected(const ResGroupCaps *caps);
static int32 groupGetMemSharedExpected(const ResGroupCaps *caps);
//static int32 groupGetMemGranted(const ResGroupCaps *caps);
static int32 groupGetMemSpillTotal(const ResGroupCaps *caps);

static int32 slotGetMemQuotaExpected(const ResGroupCaps *caps);
static int32 slotGetMemSpill(const ResGroupCaps *caps);

static void PutFreeMemToSegment(uint32 value);
static uint32 GetFreeMemFromSegment(uint32 value);

static ResGroupData *ResGroupHashNew(Oid groupId);
static ResGroupData *ResGroupHashFind(Oid groupId);
static bool ResGroupHashRemove(Oid groupId);
static ResGroupData *ResGroupCreate(Oid groupId, const ResGroupCaps *caps);
static int GetFreeSlot(ResGroupData *group);
static int ResGroupSlotAcquire(ResGroupGovernor *governor);
static void addTotalQueueDuration(ResGroupData *group);
static void ResGroupSlotRelease(ResGroupGovernor *governor);
static void ResGroupSetMemorySpillRatio(int ratio);
static void ResGroupCheckMemorySpillRatio(const ResGroupCaps *caps);
static bool ShouldAssignResGroup(void);
static void AssignResGroupCommon(ResGroupGovernor* governor);
static void UnAssignResGroupCommon(ResGroupGovernor* governor);
static uint32 AddMemImpl(uint32 *dst, uint32 limit, uint32 memoryChunks);
static uint32 SubMemImpl(uint32 *dst, uint32 memoryChunks);
static void AddToGroupWaitQueue(ResGroupData *group, PGPROC *proc);
static void RemoveFromGroupWaitQueue(ResGroupData *group, PGPROC *proc);
static void WakeupFirstWaiterInGroup(ResGroupData *group, char reason);
static void RemoveMySelfAndWakeupNextWaiter(ResGroupData *group, PGPROC *proc);
static void AcquireGroupLock(ResGroupData *group, LWLockMode mode);
static void ReleaseGroupLock(ResGroupData *group);
static ResGroupData* GovernorSetGroup(ResGroupGovernor* governor, int32 groupId);
static void GovernorStartMonitorMem(ResGroupGovernor* governor);
static bool TransforMemToNewGovernor(ResGroupGovernor* governor);
static void GovernorStartMonitorCpu(ResGroupGovernor* governor);
static void GovernorSetGroupSlot(ResGroupGovernor *governor, int32 slotId);
static void ResetResGroupGovernor(ResGroupGovernor *governor);
static uint32 ReserveMemFromGroupSlot(uint32 chunks);
static uint32 ReleaseMemToGroupSlot(uint32 chunks);
static uint32 ReserveMemFromGroupShare(uint32 chunks);
static uint32 ReleaseMemToGroupShare(uint32 chunks);
static void WakeupWaitersInOtherGroups(Oid skipGroupId);
static bool AdjustGroupMemGranted(ResGroupData *group);
static void AdjustGroupMemQuotaGranted(ResGroupData *group);
static bool SatisfyGroupSlotGranted(ResGroupData *group);
static void RefreshGroupMemLimitCap(ResGroupData *group);

static void
RefreshGroupMemLimitCap(ResGroupData *group)
{
	group->caps.memLimit.value = group->memGranted * 100.0 / pResGroupControl->totalChunks;
}

static void PutFreeMemToSegment(uint32 value)
{
	uint32 freed = AddMemImpl(&pResGroupControl->freeChunks, pResGroupControl->totalChunks, value);
	Assert(freed == value);
}

static uint32 GetFreeMemFromSegment(uint32 value)
{
	return SubMemImpl(&pResGroupControl->freeChunks, value);	
}

void
ResGroupSetNewProposed(Oid groupId, ResGroupLimitType limitType, int32 value)
{
	int total = 0;
	ResGroupData *group, *targetGroup;

	/* do we really need check concurrency <= max_connections */
	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

	targetGroup = ResGroupHashFind(groupId);
	ResGroupCap *targetCap = (ResGroupCap*)(&targetGroup->caps) + limitType;

	/* skip concurrency */
	if (limitType != RESGROUP_LIMIT_TYPE_CPU &&
		limitType != RESGROUP_LIMIT_TYPE_MEMORY)
	{
		targetCap->newProposed = value;
		LWLockRelease(ResGroupLock);
		return;
	}

	/* otherwise, need to check cpu/memory exceed 100% */
	for (int i = 0; i < MaxResourceGroups; i++)
	{
		group = &pResGroupControl->groups[i];

		if (group->groupId == InvalidOid ||
			group->groupId == groupId)
			continue;

		ResGroupCap *cap = (ResGroupCap*)(&group->caps) + limitType;

		if (cap->newProposed>= 0)
			total += cap->newProposed;
		else
			total += cap->proposed;
	}

	if (value + total > 100)
	{
		LWLockRelease(ResGroupLock);
		elog(ERROR, "total value is exceeded 100");
	}

	targetCap->newProposed = value;

	LWLockRelease(ResGroupLock);
}

static uint32
ReserveMemFromGroupSlot(uint32 chunks)
{
	uint32 sizeInSlot;
	uint32 sizeInGroup;
	ResGroupGovernor *governor; 
	ResGroupData *group;
	ResGroupSlotData *slot;

	if (chunks == 0)
		return 0;

	governor = &rg_mem_sets.governor;
	group = governor->group;
	slot = governor->slot;

	if (!OidIsValid(governor->groupId) || governor->groupId != group->groupId)
	{
		rg_mem_sets.memUsage += chunks;
		return chunks;
	}

	AcquireGroupLock(group, LW_SHARED);

	sizeInSlot = AddMemImpl(&slot->memInSlot, slot->memQuota, chunks);
	sizeInGroup = AddMemImpl(&group->memUsage, group->memGranted, sizeInSlot);

	ReleaseGroupLock(group);

	Assert(sizeInSlot == sizeInGroup);
	rg_mem_sets.memUsage += sizeInSlot;

	return sizeInSlot;
}

static uint32
ReleaseMemToGroupSlot(uint32 chunks)
{
	uint32 sizeInSlot;
	uint32 sizeInGroup;
	ResGroupGovernor *governor; 
	ResGroupSlotData *slot;
	ResGroupData *group;

	if (chunks == 0)
		return 0;

	if (rg_mem_sets.memUsage < chunks)
		chunks = rg_mem_sets.memUsage;

	governor = &rg_mem_sets.governor;
	group = governor->group;
	slot = governor->slot;

	if (!OidIsValid(governor->groupId) || governor->groupId != governor->group->groupId)
	{
		rg_mem_sets.memUsage -= chunks;
		return chunks;
	}

	sizeInSlot = SubMemImpl(&slot->memInSlot, chunks);
	sizeInGroup = SubMemImpl(&group->memUsage, sizeInSlot);

	Assert(sizeInSlot == sizeInGroup);

	rg_mem_sets.memUsage -= sizeInSlot;

	return sizeInSlot;
}

static uint32
ReserveMemFromGroupShare(uint32 chunks)
{
	uint32 sizeInShare;
	uint32 sizeInGroup;
	ResGroupGovernor *governor; 
	ResGroupData *group;
	ResGroupSlotData *slot;

	if (chunks == 0)
		return 0;

	governor = &rg_mem_sets.governor;
	group = governor->group;
	slot = governor->slot;

	if (!OidIsValid(governor->groupId) || governor->groupId != group->groupId)
	{
		rg_mem_sets.memUsage += chunks;
		return chunks;
	}

	AcquireGroupLock(group, LW_SHARED);

	sizeInShare = AddMemImpl(&group->memSharedUsage, group->memSharedGranted, chunks);
	sizeInGroup = AddMemImpl(&group->memUsage, group->memGranted, sizeInShare);

	ReleaseGroupLock(group);

	AddMemImpl(&slot->memInShare, UINT32_MAX, sizeInShare);

	Assert(sizeInShare == sizeInGroup);

	rg_mem_sets.memUsage += sizeInShare;
	return sizeInShare;
}

static uint32
ReleaseMemToGroupShare(uint32 chunks)
{
	uint32 sizeInShare;
	uint32 sizeInGroup;
	ResGroupGovernor *governor; 
	ResGroupSlotData *slot;
	ResGroupData *group;

	if (chunks == 0)
		return 0;

	if (rg_mem_sets.memUsage < chunks)
		chunks = rg_mem_sets.memUsage;

	governor = &rg_mem_sets.governor;
	group = governor->group;
	slot = governor->slot;

	if (!OidIsValid(governor->groupId) || governor->groupId != governor->group->groupId)
	{
		rg_mem_sets.memUsage -= chunks;
		return chunks;
	}

	sizeInShare = SubMemImpl(&slot->memInShare, chunks);
	sizeInGroup = SubMemImpl(&group->memUsage, sizeInShare);
	SubMemImpl(&group->memSharedUsage, sizeInShare);

	Assert(sizeInShare == sizeInGroup);

	rg_mem_sets.memUsage -= sizeInShare;

	return sizeInShare;
}


static void
ResetResGroupGovernor(ResGroupGovernor *governor)
{
	governor->groupId = InvalidOid;
	governor->group = NULL;
	governor->slot = NULL;
}

static void
AcquireGroupLock(ResGroupData *group, LWLockMode mode)
{
	LWLockId lock = pResGroupControl->baseLWLockId + group->index;

	if (LWLockHeldByMe(lock))
		return;

	LWLockAcquire(lock, mode);
}

static void
ReleaseGroupLock(ResGroupData *group)
{
	LWLockId lock = pResGroupControl->baseLWLockId + group->index;
	LWLockRelease(lock);
}

static void
GovernorStartMonitorCpu(ResGroupGovernor* governor)
{
	ResGroupOps_AssignGroup(governor->groupId, MyProcPid);
}

static void AddToGroupWaitQueue(ResGroupData *group, PGPROC *proc)
{
	PGPROC *headProc;
	PROC_QUEUE *waitQueue;
	waitQueue = &(group->waitProcs);
	headProc = (PGPROC *) &(waitQueue->links);
	SHMQueueInsertBefore(&(headProc->links), &(proc->links));
	waitQueue->size++;
}

static void WakeupFirstWaiterInGroup(ResGroupData *group, char reason)
{
	PGPROC	*waitProc;
	PROC_QUEUE *waitQueue = &(group->waitProcs);


	if (waitQueue->size > 0)
	{
		/* wake up one process in the wait queue */
		waitProc = (PGPROC *) MAKE_PTR(waitQueue->links.next);
		waitProc->resWaiting = false;
		waitProc->resWakeReason = reason;
		SetLatch(&waitProc->procLatch);
	}
}

static void RemoveFromGroupWaitQueue(ResGroupData *group, PGPROC *proc)
{
	PROC_QUEUE *waitQueue = &(group->waitProcs);
	SHMQueueDelete(&proc->links);
	waitQueue->size--;
}

static void RemoveMySelfAndWakeupNextWaiter(ResGroupData *group, PGPROC *proc)
{
	RemoveFromGroupWaitQueue(group, proc);
	WakeupFirstWaiterInGroup(group, SLOT_RELEASE);
}

void
AtStart_ResourceGroup(void)
{
	ResGroupGovernor governor;

	if (!ShouldAssignResGroup())
		return;

	ResetResGroupGovernor(&governor);

	PG_TRY();
	{

		AssignResGroupCommon(&governor);
		
		CHECK_FOR_INTERRUPTS();
	}
	PG_CATCH();
	{
		UnAssignResGroupCommon(&governor);
		PG_RE_THROW();
	}
	PG_END_TRY();

	CurrentResGroupGovernor = governor;

	/* Do not remove this check */
	CHECK_FOR_INTERRUPTS();
}

void AtEOXact_ResGroup(bool isCommit)
{
	/*
	 * users can define/alter resource group
	 * when resource group is disabled
	 */
	HandleResGroupDDLCallbacks(isCommit);

	if (!ShouldAssignResGroup())
		return;

	UnAssignResGroupCommon(&CurrentResGroupGovernor);

	ResetResGroupGovernor(&CurrentResGroupGovernor);
}

/*
 * Estimate size the resource group structures will need in
 * shared memory.
 */
Size
ResGroupShmemSize(void)
{
	Size		size = 0;

	/* The hash of groups. */
	size = hash_estimate_size(MaxResourceGroups, sizeof(ResGroupHashEntry));

	/* The control structure. */
	size = add_size(size, sizeof(ResGroupControl) - sizeof(ResGroupData));

	/* The control structure. */
	size = add_size(size, mul_size(MaxResourceGroups, sizeof(ResGroupData)));

	/* Add a safety margin */
	size = add_size(size, size / 10);

	return size;
}

/*
 * Initialize the global ResGroupControl struct of resource groups.
 */
void
ResGroupControlInit(void)
{
	int			i;
    bool        found;
    HASHCTL     info;
    int         hash_flags;
	int			size;

	size = sizeof(*pResGroupControl) - sizeof(ResGroupData);
	size += mul_size(MaxResourceGroups, sizeof(ResGroupData));

    pResGroupControl = ShmemInitStruct("global resource group control",
                                       size, &found);
    if (found)
        return;
    if (pResGroupControl == NULL)
        goto error_out;

    /* Set key and entry sizes of hash table */
    MemSet(&info, 0, sizeof(info));
    info.keysize = sizeof(Oid);
    info.entrysize = sizeof(ResGroupHashEntry);
    info.hash = tag_hash;

    hash_flags = (HASH_ELEM | HASH_FUNCTION);

    LOG_RESGROUP_DEBUG(LOG, "Creating hash table for %d resource groups", MaxResourceGroups);

    pResGroupControl->htbl = ShmemInitHash("Resource Group Hash Table",
                                           MaxResourceGroups,
                                           MaxResourceGroups,
                                           &info, hash_flags);

    if (!pResGroupControl->htbl)
        goto error_out;

    /*
     * No need to acquire LWLock here, since this is expected to be called by
     * postmaster only
     */
    pResGroupControl->loaded = false;
    pResGroupControl->nGroups = MaxResourceGroups;
	pResGroupControl->totalChunks = 0;
	pResGroupControl->freeChunks = 0;
	pResGroupControl->baseLWLockId = FirstGroupLock;

	for (i = 0; i < MaxResourceGroups; i++)
		pResGroupControl->groups[i].groupId = InvalidOid;

    return;

error_out:
	ereport(FATAL,
			(errcode(ERRCODE_OUT_OF_MEMORY),
			 errmsg("not enough shared memory for resource group control")));
}

/*
 * Allocate a resource group entry from a hash table
 */
void
AllocResGroupEntry(Oid groupId, const ResGroupOpts *opts)
{
	ResGroupData	*group;
	ResGroupCaps	caps;

	ResGroupOptsToCaps(opts, &caps);
	group = ResGroupCreate(groupId, &caps);
	if (!group)
	{
		ereport(PANIC,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				errmsg("not enough shared memory for resource groups")));
	}
}

/*
 * Remove a resource group entry from the hash table
 */
void
FreeResGroupEntry(Oid groupId)
{
#ifdef USE_ASSERT_CHECKING
	bool groupOK = 
#endif
		ResGroupHashRemove(groupId);
	Assert(groupOK);
}

/*
 * Load the resource groups in shared memory. Note this
 * can only be done after enough setup has been done. This uses
 * heap_open etc which in turn requires shared memory to be set up.
 */
void
InitResGroups(void)
{
	HeapTuple	tuple;
	SysScanDesc	sscan;
	int			numGroups;
	CdbComponentDatabases *cdbComponentDBs;
	CdbComponentDatabaseInfo *qdinfo;
	ResGroupCaps		caps;
	Relation			relResGroup;
	Relation			relResGroupCapability;

	/*
	 * On master, the postmaster does the initializtion
	 * On segments, the first QE does the initializtion
	 */
	if (Gp_role == GP_ROLE_DISPATCH && GpIdentity.segindex != MASTER_CONTENT_ID)
		return;

	if (pResGroupControl->loaded)
		return;
	/*
	 * Need a resource owner to keep the heapam code happy.
	 */
	Assert(CurrentResourceOwner == NULL);
	ResourceOwner owner = ResourceOwnerCreate(NULL, "InitResGroups");
	CurrentResourceOwner = owner;

	if (Gp_role == GP_ROLE_DISPATCH && pResGroupControl->segmentsOnMaster == 0)
	{
		Assert(GpIdentity.segindex == MASTER_CONTENT_ID);
		cdbComponentDBs = getCdbComponentDatabases();
		qdinfo = &cdbComponentDBs->entry_db_info[0];
		pResGroupControl->segmentsOnMaster = qdinfo->hostSegs;
		Assert(pResGroupControl->segmentsOnMaster > 0);
	}

	/*
	 * The resgroup shared mem initialization must be serialized. Only the first session
	 * should do the init.
	 * Serialization is done by LW_EXCLUSIVE ResGroupLock. However, we must obtain all DB
	 * locks before obtaining LWlock to prevent deadlock.
	 */
	relResGroup = heap_open(ResGroupRelationId, AccessShareLock);
	relResGroupCapability = heap_open(ResGroupCapabilityRelationId, AccessShareLock);
	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

	if (pResGroupControl->loaded)
		goto exit;

	/* These initialization must be done before ResGroupCreate() */
	pResGroupControl->totalChunks = getSegmentChunks();
	pResGroupControl->freeChunks = pResGroupControl->totalChunks;

	ResGroupOps_Init();

	numGroups = 0;
	sscan = systable_beginscan(relResGroup, InvalidOid, false, SnapshotNow, 0, NULL);
	while (HeapTupleIsValid(tuple = systable_getnext(sscan)))
	{
		ResGroupData	*group;
		int cpuRateLimit;
		Oid groupId = HeapTupleGetOid(tuple);

		GetResGroupCapabilities(groupId, &caps);
		cpuRateLimit = caps.cpuRateLimit.value;

		group = ResGroupCreate(groupId, &caps);
		if (!group)
			ereport(PANIC,
					(errcode(ERRCODE_OUT_OF_MEMORY),
			 		errmsg("not enough shared memory for resource groups")));

		ResGroupOps_CreateGroup(groupId);
		ResGroupOps_SetCpuRateLimit(groupId, cpuRateLimit);

		numGroups++;
		Assert(numGroups <= MaxResourceGroups);
	}
	systable_endscan(sscan);

	pResGroupControl->loaded = true;
	LOG_RESGROUP_DEBUG(LOG, "initialized %d resource groups", numGroups);

exit:
	LWLockRelease(ResGroupLock);
	heap_close(relResGroup, AccessShareLock);
	heap_close(relResGroupCapability, AccessShareLock);
	CurrentResourceOwner = NULL;
	ResourceOwnerDelete(owner);
}

/*
 * Check resource group status when DROP RESOURCE GROUP
 *
 * Errors out if there're running transactions, otherwise lock the resource group.
 * New transactions will be queued if the resource group is locked.
 */
void
ResGroupCheckForDrop(Oid groupId, char *name)
{
	ResGroupData	*group;

	group = ResGroupHashFind(groupId);

	Assert(group != NULL);

	AcquireGroupLock(group, LW_EXCLUSIVE);

	if (group->nRunning > 0)
	{
		int nQuery = group->nRunning + group->waitProcs.size;

		Assert(name != NULL);
		ereport(ERROR,
				(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
				 errmsg("Cannot drop resource group \"%s\"", name),
				 errhint(" The resource group is currently managing %d query(ies) and cannot be dropped.\n"
						 "\tTerminate the queries first or try dropping the group later.\n"
						 "\tThe view pg_stat_activity tracks the queries managed by resource groups.", nQuery)));
	}

	group->lockForDrop = true;

	ReleaseGroupLock(group);
}

/*
 * Wake up the backends in the wait queue when DROP RESOURCE GROUP finishes.
 * Unlock the resource group if the transaction is abortted.
 * Remove the resource group entry in shared memory if the transaction is committed.
 *
 * This function is called in the callback function of DROP RESOURCE GROUP.
 */
void
ResGroupDropCheckForWakeup(Oid groupId, bool isCommit)
{
	ResGroupData *group;
	bool	groupOK;
	bool	hasWaiters = true;

	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

	group = ResGroupHashFind(groupId);
	Assert(group != NULL);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		while (hasWaiters)
		{
			AcquireGroupLock(group, LW_EXCLUSIVE);

			if (group->waitProcs.size <= 0)
				hasWaiters = false;

			WakeupFirstWaiterInGroup(group, SLOT_DROP);

			ReleaseGroupLock(group);
		}
	}

	if (isCommit)
	{
		PutFreeMemToSegment(group->memGranted);
		group->memQuotaGranted = 0;
		group->memSharedGranted = 0;
		group->memGranted = 0;
		group->groupId = InvalidOid;
		group->lockForDrop = false;

		groupOK = ResGroupHashRemove(groupId);
		Assert(groupOK);
	}
	else
	{
		group->lockForDrop = false;
	}

	LWLockRelease(ResGroupLock);

	if (isCommit)
		WakeupWaitersInOtherGroups(group->groupId);
}

/*
 * Apply the new resgroup caps.
 */
void
ResGroupAlterOnCommit(Oid groupId,
					  ResGroupLimitType limitType,
					  int value)
{
	ResGroupData	*group;
	ResGroupCap	*cap;

	group = ResGroupHashFind(groupId);
	Assert(group != NULL);

	AcquireGroupLock(group, LW_EXCLUSIVE);

	cap = (ResGroupCap*)(&group->caps) + limitType;
	cap->proposed = cap->newProposed;
	cap->newProposed = -1;

	AdjustGroupMemGranted(group);
	AdjustGroupMemQuotaGranted(group);

	WakeupFirstWaiterInGroup(group, SLOT_ALTER);

	ReleaseGroupLock(group);
}

/*
 *  Retrieve statistic information of type from resource group
 */
Datum
ResGroupGetStat(Oid groupId, ResGroupStatType type)
{
	ResGroupData	*group;
	Datum result;

	Assert(IsResGroupActivated());

	group = ResGroupHashFind(groupId);
	if (group == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Cannot find resource group with Oid %d in shared memory", groupId)));
	}

	switch (type)
	{
		case RES_GROUP_STAT_NRUNNING:
			result = Int32GetDatum(group->nRunning);
			break;
		case RES_GROUP_STAT_NQUEUEING:
			result = Int32GetDatum(group->waitProcs.size);
			break;
		case RES_GROUP_STAT_TOTAL_EXECUTED:
			result = Int32GetDatum(group->totalExecuted);
			break;
		case RES_GROUP_STAT_TOTAL_QUEUED:
			result = Int32GetDatum(group->totalQueued);
			break;
		case RES_GROUP_STAT_TOTAL_QUEUE_TIME:
			result = IntervalPGetDatum(&group->totalQueuedTime);
			break;
		case RES_GROUP_STAT_MEM_USAGE:
			result = Int32GetDatum(VmemTracker_ConvertVmemChunksToMB(group->memUsage));
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Invalid stat type %d", type)));
	}

	return result;
}

/*
 * Dump memory information for current resource group.
 */
void
ResGroupDumpMemoryInfo(void)
{
#if 0
	ResGroupSlotData	*slot;
	ResGroupProcData	*procInfo = MyResGroupProcInfo;
	ResGroupData		*sharedInfo = MyResGroupSharedInfo;

	if (sharedInfo)
	{
		Assert(procInfo->slotId != InvalidSlotId);

		slot = &sharedInfo->slots[procInfo->slotId];

		write_log("Resource group memory information: "
				  "group memory limit is %d MB, "
				  "shared quota in current resource group is %d MB, "
				  "memory usage in current resource group is %d MB, "
				  "memory quota in current slot is %d MB, "
				  "memory usage in current slot is %d MB, "
				  "memory usage in current proc is %d MB",
				  VmemTracker_ConvertVmemChunksToMB(sharedInfo->memExpected),
				  VmemTracker_ConvertVmemChunksToMB(sharedInfo->memSharedGranted),
				  VmemTracker_ConvertVmemChunksToMB(sharedInfo->memUsage),
				  VmemTracker_ConvertVmemChunksToMB(slot->memQuota),
				  VmemTracker_ConvertVmemChunksToMB(slot->memUsage),
				  VmemTracker_ConvertVmemChunksToMB(procInfo->memUsage));
	}
	else
	{
		write_log("Resource group memory information: "
				  "memory usage in current proc is %d MB",
				  VmemTracker_ConvertVmemChunksToMB(procInfo->memUsage));
	}
#endif
}

static uint32
AddMemImpl(uint32 *dst, uint32 limit, uint32 memoryChunks)
{
	uint32 newDstValue;
	uint32 oldDstValue;

	while (true)
	{
		oldDstValue = pg_atomic_read_u32((pg_atomic_uint32*)dst);

		newDstValue = oldDstValue + memoryChunks;	

		if (newDstValue > limit)
		{
			newDstValue = limit;	
		}

		if (pg_atomic_compare_exchange_u32((pg_atomic_uint32*)dst, &oldDstValue, newDstValue))
		{
			/* successfully added, return */
			break;	
		}
	}

	return newDstValue - oldDstValue;
}

static uint32
SubMemImpl(uint32 *dst, uint32 memoryChunks)
{
	int32 newDstValue;
	uint32 oldDstValue;

	while (true)
	{
		oldDstValue = pg_atomic_read_u32((pg_atomic_uint32*)dst);

		newDstValue = oldDstValue - memoryChunks;	

		if (newDstValue < 0)
		{
			newDstValue = 0;
		}

		if (pg_atomic_compare_exchange_u32((pg_atomic_uint32*)dst, &oldDstValue, newDstValue))
		{
			/* successfully added, return */
			break;	
		}
	}

	return oldDstValue - newDstValue;
}

/*
 * Reserve 'memoryChunks' number of chunks for current resource group.
 * It will first try to reserve memory from the resource group slot; if the slot
 * quota exceeded, it will reserve memory from the shared zone. It fails if the
 * shared quota is also exceeded, and no memory is reserved.
 *
 * 'overuseChunks' number of chunks can be overused for error handling,
 * in such a case waiverUsed is marked as true.
 */
bool
ResGroupReserveMemory(int32 memoryChunks, int32 overuseChunks, bool *waiverUsed)
{
	uint32 reservedInGroupSlot;
	uint32 reservedInGroupShare;

	if (!IsResGroupEnabled())
		return true;

	Assert(memoryChunks >= 0);

	/* reserve memory from slot first */
	reservedInGroupSlot = ReserveMemFromGroupSlot(memoryChunks);	

	if (reservedInGroupSlot >= memoryChunks)
		return true;

	/* reserve memory from shared if slot has no enough memory */
	reservedInGroupShare = ReserveMemFromGroupShare(memoryChunks - reservedInGroupSlot);

	if (reservedInGroupShare + reservedInGroupSlot >= memoryChunks)
		return true;

	/* check if overuse chunks works */
	if (CritSectionCount == 0 &&
		 reservedInGroupShare + reservedInGroupSlot + overuseChunks >= memoryChunks)
	{
		*waiverUsed = true;
		return true;
	}

	/* there is no enough memory */
	ReleaseMemToGroupSlot(reservedInGroupSlot);
	ReleaseMemToGroupShare(reservedInGroupShare);
	return false;
}

/*
 * Release the memory of resource group
 */
void
ResGroupReleaseMemory(int32 memoryChunks)
{
	uint32 releasedToGroupSlot;
	uint32 releasedToGroupShare;

	if (!IsResGroupEnabled())
		return;

	Assert(memoryChunks >= 0);

	releasedToGroupShare = ReleaseMemToGroupShare(memoryChunks);

	if (memoryChunks - releasedToGroupShare <= 0)
		return;

	releasedToGroupSlot = ReleaseMemToGroupSlot(memoryChunks - releasedToGroupShare);

	Assert(releasedToGroupShare + releasedToGroupSlot <= memoryChunks);
}

int64
ResourceGroupGetQueryMemoryLimit(void)
{
	ResGroupData		*group;
	int64				memSpill;

	group = CurrentResGroupGovernor.group;

	ResGroupCheckMemorySpillRatio(&group->caps);

	if (IsResManagerMemoryPolicyNone())
		return 0;

	memSpill = slotGetMemSpill(&group->caps);

	return memSpill << VmemTracker_GetChunkSizeInBits();
}

/*
 * ResGroupCreate -- initialize the elements for a resource group.
 *
 * Notes:
 *	It is expected that the appropriate lightweight lock is held before
 *	calling this - unless we are the startup process.
 */
static ResGroupData *
ResGroupCreate(Oid groupId, const ResGroupCaps *caps)
{
	ResGroupData	*group;
	int ret;

	Assert(OidIsValid(groupId));

	group = ResGroupHashNew(groupId);
	if (group == NULL)
		return NULL;

	group->groupId = groupId;
	group->caps = *caps;
	group->nRunning = 0;
	ProcQueueInit(&group->waitProcs);
	group->totalExecuted = 0;
	group->totalQueued = 0;
	group->memUsage = 0;
	group->memSharedUsage = 0;
	group->memQuotaUsage = 0;
	group->lockForDrop = false;
	memset(&group->totalQueuedTime, 0, sizeof(group->totalQueuedTime));
	memset(group->slots, 0, sizeof(group->slots));

	group->memQuotaGranted = groupGetMemQuotaExpected(caps);
	group->memSharedGranted = groupGetMemSharedExpected(caps);

	group->memGranted = groupGetMemExpected(caps);

	ret = GetFreeMemFromSegment(group->memGranted);

	Assert(group->memGranted == ret);

	return group;
}

/*
 * Attach current proc to a resource group & slot.
 *
 * Current proc's memory usage will be added to the group & slot.
 */
static bool
TransforMemToNewGovernor(ResGroupGovernor* governor)
{
	ResGroupGovernor old_governor;
	int memNeedToTransfer;
	bool waveUsed = false;

	old_governor = rg_mem_sets.governor; 
	memNeedToTransfer = rg_mem_sets.memUsage;

	ResGroupReleaseMemory(memNeedToTransfer);

	/* set new governor to rg_mem_sets */
	AssignMemSetGovernor(governor);

	if (ResGroupReserveMemory(memNeedToTransfer, 0, &waveUsed))
		return true;

	/* rollback and return memory to old group */
	AssignMemSetGovernor(old_governor);
	
	ResGroupReserveMemory(memNeedToTransfer, 0, &waveUsed);

	return false;
}

/*
 * Get a free resource group slot.
 *
 * A free resource group slot has inUse == false, no other information is checked.
 */
static int
GetFreeSlot(ResGroupData *group)
{
	int i;

	group->nRunning += 1;

	for (i = 0; i < RESGROUP_MAX_SLOTS; i++)
	{
		if (group->slots[i].inUse)
			continue;

		group->slots[i].inUse = true;
		return i;
	}

	Assert(false && "No free slot available");
	return InvalidSlotId;
}

/*
 * Get a slot with memory quota granted.
 *
 * A slot can be got with this function if there is enough memory quota
 * available and the concurrency limit is not reached.
 *
 * On success the memory quota is marked as granted, nRunning is increased
 * and the slot's inUse flag is also set, the slot id is returned.
 *
 * On failure nothing is changed and InvalidSlotId is returned.
 */
static bool
HasAvailableSlot(ResGroupData *group, bool beWakedUp)
{
	if (group->lockForDrop)
		return false;

	if (group->caps.concurrency.proposed == 0 ||
		group->nRunning >= group->caps.concurrency.proposed)
		return false;

	/* If someone is waiting, let them wake up first */
	if (!beWakedUp && group->waitProcs.size > 0)
		return false;
	
	AdjustGroupMemGranted(group);
	AdjustGroupMemQuotaGranted(group);

	/* check if we have enough slot quota */
	if (!SatisfyGroupSlotGranted(group))
		return false;

	return true;
}

static bool
SatisfyGroupSlotGranted(ResGroupData *group)
{
	int needed;
	int available;

	int32 slotQuota = slotGetMemQuotaExpected(&group->caps);
	int32 memGranted = group->memGranted; 

	if (group->memQuotaUsage + slotQuota <=  group->memQuotaGranted)
	{
		return true;
	}
	else
	{
		/* try to get some from shared part */
		needed = group->memQuotaUsage + slotQuota - group->memQuotaGranted;

		available = memGranted - group->memQuotaGranted - group->memSharedUsage;

		if (available > 0 && available >= needed)
		{
			group->memQuotaGranted += needed;	
			group->memSharedGranted = memGranted - group->memQuotaGranted;
			group->caps.memSharedQuota.value = 100 - (group->memQuotaGranted * 100 / group->memGranted);
			return true;
		}
		else
		{
			return false;
		}
	}

	return false;
}

/*
 * Acquire a resource group slot
 *
 * Call this function at the start of the transaction.
 */
static int
ResGroupSlotAcquire(ResGroupGovernor * governor)
{
	ResGroupData	*group = governor->group;

	/* if has free slot, return directly */
	if (HasAvailableSlot(group, false))
	{
		return GetFreeSlot(group);	
	}

	AddToGroupWaitQueue(group, MyProc);

	group->totalQueued++;

wait:
	/* otherwise, we need to wait others release a slot */
	MyProc->resWaiting = true;

	/* update waiting status in pg_stat_activity, and record the time before waiting */
	pgstat_report_resgroup(GetCurrentTimestamp(), group->groupId);

	ReleaseGroupLock(group);

	for(;;)
	{
		if (!MyProc->resWaiting || QueryCancelPending)
			break;

		ResetLatch(&MyProc->procLatch);

		WaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, -1);
	}

	AcquireGroupLock(group, LW_EXCLUSIVE);

	/* record time spend for waiting */
	addTotalQueueDuration(group);
	pgstat_report_waiting(PGBE_WAITING_NONE);

	/* someone cancelled me */
	if (QueryCancelPending)
	{
		RemoveMySelfAndWakeupNextWaiter(group, MyProc);	
		return InvalidSlotId;
	}
	/* someone waked me up */
	else
	{
		/*
		 * special case, the group has been dropped 
		 * all the queries pending on the group should
		 * try to acquire slot in the new group
		 */
		if (MyProc->resWakeReason == SLOT_DROP)
		{
			RemoveFromGroupWaitQueue(group, MyProc);	
			return InvalidSlotId;
		}

		/* normal case: someone released a slot, do the check again */
		if (MyProc->resWakeReason == SLOT_RELEASE ||
			MyProc->resWakeReason == SLOT_ALTER)	
		{
			if (HasAvailableSlot(group, true))
			{
				RemoveMySelfAndWakeupNextWaiter(group, MyProc);	
				return GetFreeSlot(group);
			}
			else
			{
				goto wait;
			}
		}
	}

	return InvalidSlotId;
}

static bool
SqueezeGroupMemLimit(ResGroupData* group)
{
	int32 expected;
	int32 granted;
	int32 needed;
	int32 available;

	/* if value is equal to proposed, do nothing */
	if (group->caps.memLimit.value <= group->caps.memLimit.proposed)
		return false;

	expected = groupGetMemExpected(&group->caps);
	granted = group->memGranted;
	needed = granted - expected;

	/* otherwise, make sure memQuotaUsage + group->memSharedUsage <= new memLimit */
	/* TODO add a lock here to avoid race condition */
	available = granted - group->memQuotaUsage - group->memSharedUsage;

	Assert(available >= 0);

	if (available == 0)
		return false;

	if (needed < available)
	{
		group->memGranted -= needed;
		RefreshGroupMemLimitCap(group);
		PutFreeMemToSegment(needed);
	}
	else
	{
		group->memGranted -= available;
		RefreshGroupMemLimitCap(group);
		PutFreeMemToSegment(available);
	}

	return true;
}

/*
 * Adjust memQuotaGranted of current group,
 * It should be called after
 * AdjustGroupMemGranted()
 */
static void 
AdjustGroupMemQuotaGranted(ResGroupData* group)
{
	int needed, available;
	int32 memGranted;
	int32 quotaGranted;
	int32 quotaExpected;

	memGranted = group->memGranted;
	quotaGranted = group->memQuotaGranted;
	quotaExpected = groupGetMemQuotaExpected(&group->caps);

	/* Adjust memQuotaGanted according to change of memLimit and shared_quota */
	if (quotaGranted == quotaExpected)
	{
		/* do nothing */
	}
	else if (quotaGranted < quotaExpected)
	{
		/* we need to increase slot Quota*/
		needed = quotaExpected - quotaGranted;

		/* we need get some memory from share */
		available = memGranted - group->memQuotaUsage - group->memSharedUsage;

		if (available > 0 && available >= needed)
		{
			group->memQuotaGranted += needed;
		}
		else if (available > 0 && available < needed )
		{
			group->memQuotaGranted += available;
		}

		group->memSharedGranted = memGranted - group->memQuotaGranted;
		group->caps.memSharedQuota.value = 100.0 - (group->memQuotaGranted * 100.0 / group->memGranted);
	}
	else
	{
		needed =  quotaGranted - quotaExpected;				

		available = quotaGranted - group->memQuotaUsage;

		if (available > 0 && available >= needed)
		{
			group->memQuotaGranted -= needed;	
		}
		else if (available > 0 && available < needed)
		{
			group->memQuotaGranted -= available;	
		}

		group->memSharedGranted = memGranted - group->memQuotaGranted;
		group->caps.memSharedQuota.value = 100.0 - (group->memQuotaGranted * 100.0 / group->memGranted);

	}

	return;
}

/*
 * Adjust the memGranted of a group, if current memLimit is
 * larger than proposed, then current group may own memory
 * to other groups
 */
static bool
AdjustGroupMemGranted(ResGroupData* group)
{
	int32 expected;
	int32 granted;
	int32 needed;
	int32 available;

	expected = groupGetMemExpected(&group->caps);
	granted = group->memGranted; 

	if (expected == granted)
		return false;

	if (expected > granted)
	{
		needed = expected - granted;
		available = GetFreeMemFromSegment(needed);

		if (available == 0)
			return false;

		Assert(available > 0);
		/* segments has enough memory, i can acquire expected quota */
		if (needed <= available)
		{
			group->memGranted += needed;
			RefreshGroupMemLimitCap(group);
			return false;
		}
		else
		{
			/* other groups must own me memory */
			group->memGranted += available;
			RefreshGroupMemLimitCap(group);
			return false;
		}
	}
	else
	{
		/* I own other groups some memory, Squeeze myself and notify them */
		return SqueezeGroupMemLimit(group);
	}
}

/*
 * Calculate the total memory chunks of the segment
 */
static int32
getSegmentChunks(void)
{
	int nsegments = Gp_role == GP_ROLE_EXECUTE ? host_segments : pResGroupControl->segmentsOnMaster;

	Assert(nsegments > 0);

	return ResGroupOps_GetTotalMemory() * gp_resource_group_memory_limit / nsegments;
}

/*
 * Get total expected memory quota of a group in chunks
 */
static int32
groupGetMemExpected(const ResGroupCaps *caps)
{
	Assert(pResGroupControl->totalChunks > 0);
	return pResGroupControl->totalChunks * caps->memLimit.proposed / 100;
}

void
ResGroupGetMemInfo(int *memLimit, int *slotQuota, int *sharedQuota)
{
	const ResGroupCaps *caps = &CurrentResGroupGovernor.group->caps;

	*memLimit = groupGetMemExpected(caps);
	*slotQuota = slotGetMemQuotaExpected(caps);
	*sharedQuota = groupGetMemSharedExpected(caps);
}

#if 0
static int32
groupGetMemGranted(const ResGroupCaps *caps)
{
	Assert(pResGroupControl->totalChunks > 0);
	return pResGroupControl->totalChunks * caps->memLimit.value / 100;
}
#endif

/*
 * Get per-group expected memory quota in chunks
 */
static int32
groupGetMemQuotaExpected(const ResGroupCaps *caps)
{
	return groupGetMemExpected(caps) *
		(100 - caps->memSharedQuota.proposed) / 100;
}

/*
 * Get per-group expected memory shared quota in chunks
 */
static int32
groupGetMemSharedExpected(const ResGroupCaps *caps)
{
	return groupGetMemExpected(caps) - groupGetMemQuotaExpected(caps);
}

/*
 * Get per-group expected memory spill in chunks
 */
static int32
groupGetMemSpillTotal(const ResGroupCaps *caps)
{
	return groupGetMemExpected(caps) * memory_spill_ratio / 100;
}

/*
 * Get per-slot expected memory quota in chunks
 */
static int32
slotGetMemQuotaExpected(const ResGroupCaps *caps)
{
	if (caps->concurrency.proposed == 0)
	{
		/* 
		 * TODO
		 */
		return 1;
	}
	return groupGetMemQuotaExpected(caps) / caps->concurrency.proposed;
}

/*
 * Get per-slot expected memory spill in chunks
 */
static int32
slotGetMemSpill(const ResGroupCaps *caps)
{
	return groupGetMemSpillTotal(caps) / caps->concurrency.proposed;
}

static void
WakeupWaitersInOtherGroups(Oid skipGroupId)
{
	if (Gp_role != GP_ROLE_DISPATCH);
		return;

	for (int i = 0; i < MaxResourceGroups; i++)
	{
		ResGroupData	*group = &pResGroupControl->groups[i];

		if (group->groupId == InvalidOid ||
			group->groupId == skipGroupId ||
			group->waitProcs.size == 0)
			continue;

		AcquireGroupLock(group, LW_EXCLUSIVE);

		WakeupFirstWaiterInGroup(group, SLOT_RELEASE);

		ReleaseGroupLock(group);
	}
}

static void
addTotalQueueDuration(ResGroupData *group)
{
	if (group == NULL)
		return;

	TimestampTz start = pgstat_fetch_resgroup_queue_timestamp();
	TimestampTz now = GetCurrentTimestamp();
	Datum durationDatum = DirectFunctionCall2(timestamptz_age, TimestampTzGetDatum(now), TimestampTzGetDatum(start));
	Datum sumDatum = DirectFunctionCall2(interval_pl, IntervalPGetDatum(&group->totalQueuedTime), durationDatum);
	memcpy(&group->totalQueuedTime, DatumGetIntervalP(sumDatum), sizeof(Interval));
}

/*
 * Release the resource group slot
 *
 * Call this function at the end of the transaction.
 */
static void
ResGroupSlotRelease(ResGroupGovernor *governor)
{
	ResGroupData		*group;
	ResGroupSlotData	*slot;
	bool shouldWakeupOtherGroup;

	group = governor->group;
	slot = governor->slot;

	Assert(Gp_role == GP_ROLE_DISPATCH);
	Assert(group->nRunning > 0);

	AcquireGroupLock(group, LW_EXCLUSIVE);	

	group->nRunning -= 1;
	group->memQuotaUsage -= slot->memQuota;
	slot->inUse = false;
	elog(LOG, "tpztpz release groupId %d granted: %d quota %d, %d", group->groupId, group->memGranted, group->memQuotaUsage, slot->memQuota);
	shouldWakeupOtherGroup = AdjustGroupMemGranted(group);

	/* adjust memoryGranted if we own someone in the same slot */
	AdjustGroupMemQuotaGranted(group);

	WakeupFirstWaiterInGroup(group, SLOT_RELEASE);

	ReleaseGroupLock(group);

	if (shouldWakeupOtherGroup)
	{
		WakeupWaitersInOtherGroups(group->groupId);
	}
}

/*
 * Serialize the resource group information that need to dispatch to segment.
 */
void
SerializeResGroupInfo(StringInfo str)
{
	int i;
	int tmp;
	Oid groupId;
	ResGroupCap *caps;
	ResGroupData *group;
	ResGroupSlotData *slot;

	groupId = CurrentResGroupGovernor.groupId;
	group = CurrentResGroupGovernor.group;
	slot = CurrentResGroupGovernor.slot;

	if (groupId == InvalidOid)
		return;

	caps = (ResGroupCap *) &group->caps;

	tmp = htonl(groupId);
	appendBinaryStringInfo(str, (char *) &tmp, sizeof(groupId));

	tmp = htonl(slot->slotId);
	appendBinaryStringInfo(str, (char *) &tmp, sizeof(slot->slotId));

	for (i = 0; i < RESGROUP_LIMIT_TYPE_COUNT; i++)
	{
		tmp = htonl(caps[i].value);
		appendBinaryStringInfo(str, (char *) &tmp, sizeof(caps[i].value));

		tmp = htonl(caps[i].proposed);
		appendBinaryStringInfo(str, (char *) &tmp, sizeof(caps[i].proposed));
	}
}

/*
 * Deserialize the resource group information dispatched by QD.
 */
void
DeserializeResGroupInfo(const char *buf, int len)
{
	int			i;
	int			tmp;
	const char	*ptr = buf;
	ResGroupCap *caps = (ResGroupCap *) &newCapsFromQD;

	Assert(len > 0);

	memcpy(&tmp, ptr, sizeof(newGroupIdFromQD));
	newGroupIdFromQD = ntohl(tmp);
	ptr += sizeof(newGroupIdFromQD);

	memcpy(&tmp, ptr, sizeof(newSlotIdFromQD));
	newSlotIdFromQD = ntohl(tmp);
	ptr += sizeof(newSlotIdFromQD);

	for (i = 0; i < RESGROUP_LIMIT_TYPE_COUNT; i++)
	{
		memcpy(&tmp, ptr, sizeof(caps[i].value));
		caps[i].value = ntohl(tmp);
		ptr += sizeof(caps[i].value);

		memcpy(&tmp, ptr, sizeof(caps[i].proposed));
		caps[i].proposed = ntohl(tmp);
		ptr += sizeof(caps[i].proposed);
	}

	Assert(len == ptr - buf);
}

/*
 * Check whether should assign resource group on master.
 */
bool
ShouldAssignResGroup(void)
{
	return IsResGroupActivated() &&
		IsNormalProcessingMode() &&
		!AmIInSIGUSR1Handler();
}

static ResGroupData*
GovernorSetGroup(ResGroupGovernor* governor, int32 groupId)
{
	ResGroupData	*group = ResGroupHashFind(groupId);

	if (group == NULL)
	{
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("Cannot find resource group %d in shared memory", groupId)));
	}

	governor->groupId = groupId;
	governor->group = group;

	return group;
}

static void
GovernorSetGroupSlot(ResGroupGovernor *governor, int32 slotId)
{
	ResGroupData *group = governor->group;

	governor->slot = &group->slots[slotId];
	governor->slot->slotId = slotId;
	governor->slot->memQuota = slotGetMemQuotaExpected(&group->caps);
	
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		ResGroupSetMemorySpillRatio(group->caps.memSpillRatio.proposed);
		group->memQuotaUsage += governor->slot->memQuota;
		elog(LOG, "tpztpz acquired groupId %d granted: %d quota %d, %d", group->groupId, group->memGranted, group->memQuotaUsage, governor->slot->memQuota);
	}
}

/*
 * On master, QD is assigned to a resource group at the beginning of a transaction.
 * It will first acquire a slot from the resource group, and then, it will get the
 * current capability snapshot, update the memory usage information, and add to
 * the corresponding cgroup.
 */
static void
AssignResGroupCommon(ResGroupGovernor* governor)
{
	int slotId;
	int groupId;

	Assert(Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_EXECUTE);

retry:
	CHECK_FOR_INTERRUPTS();

	/* Step1: determin the resource group ID */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		groupId = GetResGroupIdForRole(GetUserId());
		if (groupId == InvalidOid)
			groupId = superuser() ? ADMINRESGROUP_OID : DEFAULTRESGROUP_OID;

		Assert(groupId != InvalidOid);
	}
	else if (Gp_role == GP_ROLE_EXECUTE)
	{
		groupId = newGroupIdFromQD;
	}
	
	if (groupId == InvalidOid)
	{
		return;
	}

	LWLockAcquire(ResGroupLock, LW_SHARED);

	ResGroupData *group = GovernorSetGroup(governor, groupId);

	if (group == NULL)
	{
		elog(ERROR, "group %d has been dropped concurrently", groupId);
	}

	AcquireGroupLock(group, LW_EXCLUSIVE);

	/* Is safe to release ResGroupLock now */
	LWLockRelease(ResGroupLock);

	/* Step2: acqurie a slot */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		slotId = ResGroupSlotAcquire(governor); 

		/* slot resource temporary unavailable, do the retry */
		if (slotId == InvalidSlotId)
		{
			ReleaseGroupLock(group);
			goto retry;
		}
	}
	else if (Gp_role == GP_ROLE_EXECUTE)
	{
		slotId = newSlotIdFromQD;
	}
	
	GovernorSetGroupSlot(governor, slotId);

	pgstat_report_resgroup(0, group->groupId);

	ReleaseGroupLock(group);

	/* 
	 * Step3: transfer memory to new group if needed,
	 */
	GovernorStartMonitorMem(governor);
		
	/* Step4: do cpu */
	GovernorStartMonitorCpu(governor);

	group->totalExecuted++;
}

/*
 * Detach from a resource group at the end of the transaction.
 */
static void
UnAssignResGroupCommon(ResGroupGovernor* governor)
{

	if (governor->groupId == InvalidOid)
	{
		return;
	}

	Assert(governor->groupId != InvalidOid);
	Assert(governor->group != NULL);

	if (Gp_role == GP_ROLE_DISPATCH && governor->slot != NULL)
	{
		/* Relesase the slot */
		ResGroupSlotRelease(governor);
	}

	newGroupIdFromQD = InvalidOid;
	newSlotIdFromQD = InvalidSlotId;
}

static void
GovernorStartMonitorMem(ResGroupGovernor* governor)
{
	if (!IsMemSetGovernedByMe(governor))
	{
		if (!TransforMemToNewGovernor(governor))
			elog(ERROR, "resgroup: out of memory when transfer to new group");
	}
}

/*
 * ResGroupHashNew -- return a new (empty) group object to initialize.
 *
 * Notes
 *	The resource group lightweight lock (ResGroupLock) *must* be held for
 *	this operation.
 */
static ResGroupData *
ResGroupHashNew(Oid groupId)
{
	int			i;
	bool		found;
	ResGroupHashEntry *entry;

	if (groupId == InvalidOid)
		return NULL;

	LWLockAcquire(ResGroupHashLock, LW_EXCLUSIVE);

	for (i = 0; i < pResGroupControl->nGroups; i++)
	{
		if (pResGroupControl->groups[i].groupId == InvalidOid)
			break;
	}

	Assert(i < pResGroupControl->nGroups);

	entry = (ResGroupHashEntry *)
		hash_search(pResGroupControl->htbl, (void *) &groupId, HASH_ENTER_NULL, &found);

	LWLockRelease(ResGroupHashLock);

	/* caller should test that the group does not exist already */
	Assert(!found);
	entry->index = i;

	return &pResGroupControl->groups[i];
}

/*
 * ResGroupHashFind -- return the group for a given oid.
 *
 * Notes
 *	The resource group lightweight lock (ResGroupLock) *must* be held for
 *	this operation.
 */
static ResGroupData *
ResGroupHashFind(Oid groupId)
{
	bool				found;
	ResGroupHashEntry	*entry;

	LWLockAcquire(ResGroupHashLock, LW_SHARED);
	entry = (ResGroupHashEntry *)
		hash_search(pResGroupControl->htbl, (void *) &groupId, HASH_FIND, &found);
	LWLockRelease(ResGroupHashLock);

	if (!found)
	{
		return NULL;
	}
	Assert(entry->index < pResGroupControl->nGroups);
	ResGroupData* group = &pResGroupControl->groups[entry->index];
	group->index = entry->index;
	return group; 
}


/*
 * ResGroupHashRemove -- remove the group for a given oid.
 *
 * Notes
 *	The resource group lightweight lock (ResGroupLock) *must* be held for
 *	this operation.
 */
static bool
ResGroupHashRemove(Oid groupId)
{
	bool		found;

	LWLockAcquire(ResGroupHashLock, LW_EXCLUSIVE);
	hash_search(pResGroupControl->htbl, (void *) &groupId, HASH_REMOVE, &found);
	LWLockRelease(ResGroupHashLock);

	return found;
}

static void
ResGroupSetMemorySpillRatio(int ratio)
{
	char value[64];

	snprintf(value, sizeof(value), "%d", ratio);
	set_config_option("memory_spill_ratio", value, PGC_USERSET, PGC_S_RESGROUP, GUC_ACTION_SET, true);
}

static void
ResGroupCheckMemorySpillRatio(const ResGroupCaps *caps)
{
	if (caps->memSharedQuota.proposed + memory_spill_ratio > RESGROUP_MAX_MEMORY_LIMIT)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("The sum of memory_shared_quota (%d) and memory_spill_ratio (%d) exceeds %d",
						caps->memSharedQuota.proposed, memory_spill_ratio,
						RESGROUP_MAX_MEMORY_LIMIT)));
}

/*
 * Convert ResGroupOpts to ResGroupCaps
 */
void
ResGroupOptsToCaps(const ResGroupOpts *optsIn, ResGroupCaps *capsOut)
{
	int i;
	ResGroupCap		*caps = (ResGroupCap *) capsOut;
	const int32		*opts = (int32 *) optsIn;

	for (i = 0; i < RESGROUP_LIMIT_TYPE_COUNT; i++)
	{
		caps[i].value = opts[i];
		caps[i].proposed = opts[i];
		caps[i].newProposed = -1;
	}
}

/*
 * Convert ResGroupCaps to ResGroupOpts
 */
void
ResGroupCapsToOpts(const ResGroupCaps *capsIn, ResGroupOpts *optsOut)
{
	int i;
	const ResGroupCap	*caps = (ResGroupCap *) capsIn;
	int32				*opts = (int32 *) optsOut;

	for (i = 0; i < RESGROUP_LIMIT_TYPE_COUNT; i++)
		opts[i] = caps[i].proposed;
}
