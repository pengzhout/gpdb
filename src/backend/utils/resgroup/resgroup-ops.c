/*-------------------------------------------------------------------------
 *
 * cgroup-ops.c
 *	  OS dependent resource group operations.
 *
 *
 * Copyright (c) 2006-2017, Greenplum inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "postmaster/backoff.h"
#include "utils/resgroup.h"

#ifdef __linux__
#include "cgroup.h"
#endif//__linux__

/*
 * Interfaces for OS dependent operations.
 *
 * Resource group replies on OS dependent group implementation to manage
 * resources like cpu usage, such as cgroup on Linux system.
 * We call it OS group in below function description.
 *
 * All the functions with bool return type will return false if the OS group
 * is not available or not properly configured or the operation failed.
 */

#define unsupported_system() \
	elog(ERROR, "resource group is unsupported on this system")

/* Return the name for the OS group implementation, such as cgroup */
const char *
ResGroupOps_Name(void)
{
#ifdef __linux__
	return "cgroup";
#else
	unsupported_system();
	return "unsupported";
#endif
}

/* Check whether the OS group implementation is available and useable */
void
ResGroupOps_CheckPermission(void)
{
#ifdef __linux__
	CGroupCheckPermission(0);
#else
	unsupported_system();
#endif
}

/* Initialize the OS group */
void
ResGroupOps_Init(void)
{
#ifdef __linux__
	CGroupInitTop();
#else
	unsupported_system();
#endif
}

/*
 * Create the OS group for group.
 */
void
ResGroupOps_CreateGroup(Oid group)
{
#ifdef __linux__
	CGroupCreateSub(group);
#else
	unsupported_system();
#endif
}

/*
 * Destroy the OS group for group.
 *
 * Fail if any process is running under it.
 */
void
ResGroupOps_DestroyGroup(Oid group)
{
#ifdef __linux__
	CGroupDestroySub(group);
#else
	unsupported_system();
#endif
}

/*
 * Assign a process to the OS group. A process can only be assigned to one
 * OS group, if it's already running under other OS group then it'll be moved
 * out that OS group.
 *
 * pid is the process id.
 */
void
ResGroupOps_AssignGroup(Oid group, int pid)
{
#ifdef __linux__
	CGroupAssignGroup(group, pid);
#else
	unsupported_system();
#endif
}

/*
 * Set the cpu rate limit for the OS group.
 *
 * cpu_rate_limit should be within (0.0, 1.0].
 */
void
ResGroupOps_SetCpuRateLimit(Oid group, float cpu_rate_limit)
{
#ifdef __linux__
	CGroupSetCpuRateLimit(group, cpu_rate_limit);
#else
	unsupported_system();
#endif
}

/*
 * Get the cpu usage of the OS group, that is the total cpu time obtained
 * by this OS group, in nano seconds.
 */
int64
ResGroupOps_GetCpuUsage(Oid group)
{
#ifdef __linux__
	return CGroupGetCpuUsage(group);
#else
	unsupported_system();
	return 0;
#endif
}

/*
 * Get the count of cpu cores on the system.
 */
int
ResGroupOps_GetCpuCores(void)
{
#ifdef __linux__
	return CGroupGetCpuCores();
#else
	unsupported_system();
	return -1;
#endif
}
