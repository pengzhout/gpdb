/*-------------------------------------------------------------------------
 *
 * resgroupcmds.h
 *	  Commands for manipulating resource group.
 *
 * Copyright (c) 2006-2017, Greenplum inc.
 *
 * IDENTIFICATION
 * 		src/include/commands/resgroupcmds.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RESGROUPCMDS_H
#define RESGROUPCMDS_H

#include "nodes/parsenodes.h"

extern void CreateResourceGroup(CreateResourceGroupStmt *stmt);
extern void DropResourceGroup(DropResourceGroupStmt *stmt);

/* catalog access function */
extern Oid GetResGroupIdForName(char *name, LOCKMODE lockmode);
extern char *GetResGroupNameForId(Oid oid, LOCKMODE lockmode);
extern int GetConcurrencyForGroup(int groupId);
extern float GetCpuRateLimitForGroup(int groupId);
extern Oid GetResGroupIdForRole(Oid roleid);

#endif   /* RESGROUPCMDS_H */
