/*-------------------------------------------------------------------------
 *
 * cgroup.c
 *	  CGroup based cpu resource group implementation.
 *
 *
 * Copyright (c) 2006-2017, Greenplum inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cdb/cdbvars.h"
#include "postmaster/backoff.h"

#include "cgroup.h"

/* cgroup is only available on linux */

#include <fcntl.h>
#include <unistd.h>
#include <sched.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>

#define CGROUP_ERROR_PREFIX "cgroup is not properly configured: "
#define CGROUP_ERROR(...) do { \
	elog(ERROR, CGROUP_ERROR_PREFIX __VA_ARGS__); \
} while(false);

static bool isLinuxPlatform(void);
static char * buildPath(Oid group, const char *comp, const char *prop, char *path, size_t pathsize);
static bool createDir(Oid group, const char *comp);
static bool removeDir(Oid group, const char *comp);
static int getCpuCores(void);
static size_t readData(Oid group, const char *comp, const char *prop, char *data, size_t datasize);
static void writeData(Oid group, const char *comp, const char *prop, char *data, size_t datasize);
static int64 readInt64(Oid group, const char *comp, const char *prop);
static void writeInt64(Oid group, const char *comp, const char *prop, int64 x);

static bool
isLinuxPlatform(void)
{
#ifdef __linux__
	return true;
#else
	return false;
#endif
}

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
#ifdef __linux__
	cpu_set_t set;

	if (sched_getaffinity (0, sizeof (set), &set) == 0)
	{
		unsigned long count;

#ifdef CPU_COUNT
		/* glibc >= 2.6 has the CPU_COUNT macro.  */
		count = CPU_COUNT (&set);
#else
		size_t i;

		count = 0;
		for (i = 0; i < CPU_SETSIZE; i++)
			if (CPU_ISSET (i, &set))
				count++;
#endif
		if (count > 0)
			return count;
	}

	CGROUP_ERROR("can't get cpu cores");

	return 1;
#else
	CGROUP_ERROR("unsupported platform");
	return -1;
#endif
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

void
CGroupCheckPermission(Oid group)
{
	char path[128];
	size_t pathsize = sizeof(path);
	const char *comp = "cpu";

	if (!isLinuxPlatform())
		CGROUP_ERROR("unsupported platform");

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

void
CGroupInitTop(void)
{
	/* cfs_quota_us := cfs_period_us * ncores * gp_resource_group_cpu_limit */
	/* shares := 1024 * ncores */

	int64 cfs_period_us;
	int ncores = getCpuCores();
	const char *comp = "cpu";

	cfs_period_us = readInt64(0, comp, "cpu.cfs_period_us");
	writeInt64(0, comp, "cpu.cfs_quota_us",
			   cfs_period_us * ncores * gp_resource_group_cpu_limit);
	writeInt64(0, comp, "cpu.shares", 1024 * ncores);
}

void
CGroupAdjustGUCs(void)
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

void
CGroupCreateSub(Oid group)
{
	if (!createDir(group, "cpu") || !createDir(group, "cpuacct"))
	{
		CGROUP_ERROR("can't create cgroup for resgroup '%d': %s",
			 group, strerror(errno));
	}

	/* check the permission */
	CGroupCheckPermission(group);
}

void
CGroupDestroySub(Oid group)
{
	if (!removeDir(group, "cpu") || !removeDir(group, "cpuacct"))
	{
		CGROUP_ERROR("can't remove cgroup for resgroup '%d': %s",
			 group, strerror(errno));
	}
}

void
CGroupAssignGroup(Oid group, int pid)
{
	writeInt64(group, "cpu", "cgroup.procs", pid);
	writeInt64(group, "cpuacct", "cgroup.procs", pid);
}

void
CGroupSetCpuRateLimit(Oid group, float cpu_rate_limit)
{
	const char *comp = "cpu";

	/* SUB/shares := TOP/shares * cpu_rate_limit */

	int64 shares = readInt64(0, comp, "cpu.shares");
	writeInt64(group, comp, "cpu.shares", shares * cpu_rate_limit);
}

int64
CGroupGetCpuUsage(Oid group)
{
	const char *comp = "cpuacct";

	return readInt64(group, comp, "cpuacct.usage");
}

int
CGroupGetCpuCores(void)
{
	return getCpuCores();
}
