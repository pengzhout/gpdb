/*-------------------------------------------------------------------------
 *
 * cgroup.h
 *	  CGroup based cpu resource management.
 *
 *
 * Copyright (c) 2006-2017, Greenplum inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CGROUP_H
#define CGROUP_H

#ifdef __linux__

/* cgroup is only available on linux */

extern void CGroupCheckPermission(Oid group);
extern void CGroupInitTop(void);
extern void CGroupHackGUCs(void);
extern void CGroupCreateSub(Oid group);
extern void CGroupDestroySub(Oid group);
extern void CGroupAssignGroup(Oid group, int pid);
extern void CGroupSetCpuRateLimit(Oid group, float cpu_rate_limit);
extern int64 CGroupGetCpuUsage(Oid group);
extern int CGroupGetCpuCores(void);

#endif//__linux__

#endif   /* CGROUP_H */
