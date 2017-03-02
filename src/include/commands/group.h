/*-------------------------------------------------------------------------
 *
 * group.h
 *	  Commands for manipulating resource group.
 *
 * Copyright (c) 2006-2010, Greenplum inc.
 *
 * IDENTIFICATION
 * 		src/include/commands/group.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GROUP_H
#define GROUP_H

#include "nodes/parsenodes.h"

extern void CreateResourceGroup(CreateResourceGroupStmt *stmt);

#endif
