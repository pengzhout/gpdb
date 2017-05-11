/*-------------------------------------------------------------------------
 *
 * resgroup-ops-cgroup.c
 *	  OS dependent resource group operations - cgroup implementation
 *
 *
 * Copyright (c) 2017, Pivotal Software Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cdb/cdbvars.h"
#include "postmaster/backoff.h"
#include "utils/resgroup-ops.h"

#ifndef __linux__
#error  cgroup is only available on linux
#endif

#include <unistd.h>
#include <sched.h>
#include <sys/stat.h>

/*
 * Interfaces for OS dependent operations.
 *
 * Resource group relies on OS dependent group implementation to manage
 * resources like cpu usage, such as cgroup on Linux system.
 * We call it OS group in below function description.
 *
 * So far these operations are mainly for CPU rate limitation and accounting.
 */

#define CGROUP_ERROR_PREFIX "cgroup is not properly configured: "
#define CGROUP_ERROR(...) do { \
	elog(ERROR, CGROUP_ERROR_PREFIX __VA_ARGS__); \
} while(false);

static char * buildPath(Oid group, const char *comp, const char *prop, char *path, size_t pathsize);
static bool createDir(Oid group, const char *comp);
static bool removeDir(Oid group, const char *comp);
static int getCpuCores(void);
static size_t readData(Oid group, const char *comp, const char *prop, char *data, size_t datasize);
static void writeData(Oid group, const char *comp, const char *prop, char *data, size_t datasize);
static int64 readInt64(Oid group, const char *comp, const char *prop);
static void writeInt64(Oid group, const char *comp, const char *prop, int64 x);
static void checkPermission(Oid group);

static int cpucores = 0;

static char *
buildPath(Oid group,
		  const char *comp,
		  const char *prop,
		  char *path,
		  size_t pathsize)
{
	if (group)
		snprintf(path, pathsize, "/sys/fs/cgroup/%s/gpdb/%d/%s", comp, group, prop);
	else
		snprintf(path, pathsize, "/sys/fs/cgroup/%s/gpdb/%s", comp, prop);

	return path;
}

static bool
createDir(Oid group, const char *comp)
{
	char path[MAXPGPATH];
	size_t pathsize = sizeof(path);

	buildPath(group, comp, "", path, pathsize);

	if (access(path, F_OK))
	{
		/* the dir is not created yet, create it */
		if (mkdir(path, 0755) && errno != EEXIST)
			return false;
	}

	return true;
}

static bool
removeDir(Oid group, const char *comp)
{
	char path[128];
	size_t pathsize = sizeof(path);

	buildPath(group, comp, "", path, pathsize);

	if (!access(path, F_OK))
	{
		/* the dir exists, remove it */
		if (rmdir(path) && errno != ENOENT)
			return false;
	}

	return true;
}

static int
getCpuCores(void)
{
	if (cpucores == 0)
	{
		/*
		 * cpuset ops requires _GNU_SOURCE to be defined,
		 * and _GNU_SOURCE is forced on in src/template/linux,
		 * so we assume these ops are always available on linux.
		 */
		cpu_set_t cpuset;
		int i;

		if (sched_getaffinity(0, sizeof(cpuset), &cpuset) < 0)
			CGROUP_ERROR("can't get cpu cores: %s", strerror(errno));

		for (i = 0; i < CPU_SETSIZE; i++)
		{
			if (CPU_ISSET(i, &cpuset))
				cpucores++;
		}
	}

	if (cpucores == 0)
		CGROUP_ERROR("can't get cpu cores");

	return cpucores;
}

static size_t
readData(Oid group, const char *comp, const char *prop, char *data, size_t datasize)
{
	char path[128];
	size_t pathsize = sizeof(path);

	buildPath(group, comp, prop, path, pathsize);

	int fd = open(path, O_RDONLY);
	if (fd < 0)
		CGROUP_ERROR("can't open file '%s': %s", path, strerror(errno));

	size_t ret = read(fd, data, datasize);
	close(fd);

	if (ret < 0)
		CGROUP_ERROR("can't read data from file '%s': %s", path, strerror(errno));

	return ret;
}

static void
writeData(Oid group, const char *comp, const char *prop, char *data, size_t datasize)
{
	char path[128];
	size_t pathsize = sizeof(path);

	buildPath(group, comp, prop, path, pathsize);

	int fd = open(path, O_WRONLY);
	if (fd < 0)
		CGROUP_ERROR("can't open file '%s': %s", path, strerror(errno));

	size_t ret = write(fd, data, datasize);
	close(fd);

	if (ret < 0)
		CGROUP_ERROR("can't write data to file '%s': %s", path, strerror(errno));
	if (ret != datasize)
		CGROUP_ERROR("can't write all data to file '%s'", path);
}

static int64
readInt64(Oid group, const char *comp, const char *prop)
{
	int64 x;
	char data[64];
	size_t datasize = sizeof(data);

	readData(group, comp, prop, data, datasize);

	if (sscanf(data, "%lld", (long long *) &x) != 1)
		CGROUP_ERROR("invalid number '%s'", data);

	return x;
}

static void
writeInt64(Oid group, const char *comp, const char *prop, int64 x)
{
	char data[64];
	size_t datasize = sizeof(data);

	snprintf(data, datasize, "%lld", (long long) x);

	writeData(group, comp, prop, data, strlen(data));
}

static void
checkPermission(Oid group)
{
	char path[128];
	size_t pathsize = sizeof(path);
	const char *comp = "cpu";

	if (access(buildPath(group, comp, "", path, pathsize), R_OK | W_OK | X_OK))
		CGROUP_ERROR("can't access directory '%s': %s", path, strerror(errno));

	if (access(buildPath(group, comp, "cgroup.procs", path, pathsize), R_OK | W_OK))
		CGROUP_ERROR("can't access file '%s': %s", path, strerror(errno));
	if (access(buildPath(group, comp, "cpu.cfs_period_us", path, pathsize), R_OK | W_OK))
		CGROUP_ERROR("can't access file '%s': %s", path, strerror(errno));
	if (access(buildPath(group, comp, "cpu.cfs_quota_us", path, pathsize), R_OK | W_OK))
		CGROUP_ERROR("can't access file '%s': %s", path, strerror(errno));
	if (access(buildPath(group, comp, "cpu.shares", path, pathsize), R_OK | W_OK))
		CGROUP_ERROR("can't access file '%s': %s", path, strerror(errno));

	comp = "cpuacct";

	if (access(buildPath(group, comp, "", path, pathsize), R_OK | W_OK | X_OK))
		CGROUP_ERROR("can't access directory '%s': %s", path, strerror(errno));

	if (access(buildPath(group, comp, "cpuacct.usage", path, pathsize), R_OK))
		CGROUP_ERROR("can't access file '%s': %s", path, strerror(errno));
	if (access(buildPath(group, comp, "cpuacct.stat", path, pathsize), R_OK))
		CGROUP_ERROR("can't access file '%s': %s", path, strerror(errno));
}

/* Return the name for the OS group implementation */
const char *
ResGroupOps_Name(void)
{
	return "cgroup";
}

/* Check whether the OS group implementation is available and useable */
void
ResGroupOps_Bless(void)
{
	checkPermission(0);
}

/* Initialize the OS group */
void
ResGroupOps_Init(void)
{
	/* cfs_quota_us := cfs_period_us * ncores * gp_resource_group_cpu_limit */
	/* shares := 1024 * 256 (max possible value) */

	int64 cfs_period_us;
	int ncores = getCpuCores();
	const char *comp = "cpu";

	cfs_period_us = readInt64(0, comp, "cpu.cfs_period_us");
	writeInt64(0, comp, "cpu.cfs_quota_us",
			   cfs_period_us * ncores * gp_resource_group_cpu_limit);
	writeInt64(0, comp, "cpu.shares", 1024 * 256);
}

/* Adjust GUCs for this OS group implementation */
void
ResGroupOps_AdjustGUCs(void)
{
	/*
	 * cgroup cpu limitation works best when all processes have equal
	 * priorities, so we force all the segments and postmaster to
	 * work with nice=0.
	 *
	 * this function should be called before GUCs are dispatched to segments.
	 */
	/* TODO: when cgroup is enabled we should move postmaster and maybe
	 *       also other processes to a separate group or gpdb toplevel */
	if (gp_segworker_relative_priority != 0)
	{
		/* TODO: produce a warning */
		gp_segworker_relative_priority = 0;
	}
}

/*
 * Create the OS group for group.
 */
void
ResGroupOps_CreateGroup(Oid group)
{
	if (!createDir(group, "cpu") || !createDir(group, "cpuacct"))
	{
		CGROUP_ERROR("can't create cgroup for resgroup '%d': %s",
			 group, strerror(errno));
	}

	/* check the permission */
	checkPermission(group);
}

/*
 * Destroy the OS group for group.
 *
 * Fail if any process is running under it.
 */
void
ResGroupOps_DestroyGroup(Oid group)
{
	if (!removeDir(group, "cpu") || !removeDir(group, "cpuacct"))
	{
		CGROUP_ERROR("can't remove cgroup for resgroup '%d': %s",
			 group, strerror(errno));
	}
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
	writeInt64(group, "cpu", "cgroup.procs", pid);
	writeInt64(group, "cpuacct", "cgroup.procs", pid);
}

/*
 * Set the cpu rate limit for the OS group.
 *
 * cpu_rate_limit should be within (0.0, 1.0].
 */
void
ResGroupOps_SetCpuRateLimit(Oid group, float cpu_rate_limit)
{
	const char *comp = "cpu";

	/* SUB/shares := TOP/shares * cpu_rate_limit */

	int64 shares = readInt64(0, comp, "cpu.shares");
	writeInt64(group, comp, "cpu.shares", shares * cpu_rate_limit);
}

/*
 * Get the cpu usage of the OS group, that is the total cpu time obtained
 * by this OS group, in nano seconds.
 */
int64
ResGroupOps_GetCpuUsage(Oid group)
{
	const char *comp = "cpuacct";

	return readInt64(group, comp, "cpuacct.usage");
}

/*
 * Get the count of cpu cores on the system.
 */
int
ResGroupOps_GetCpuCores(void)
{
	return getCpuCores();
}
