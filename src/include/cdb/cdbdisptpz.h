/*-------------------------------------------------------------------------
 *
 * cdbdisp.h
 * routines for dispatching commands from the dispatcher process
 * to the qExec processes.
 *
 * Copyright (c) 2005-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBDISPTPZ_H
#define CDBDISPTPZ_H

//#include "access/aosegfiles.h"
//#include "lib/stringinfo.h"         /* StringInfo */

#define MAX_DISPATCH_NAME 128

extern int OldGangGroupID;
extern int CurrentGangGroupID;

extern struct SliceTable;

typedef struct CdbDispatcher
{
	char name[MAX_DISPATCH_NAME];
	int (*getGangGroup) (struct SliceTable*);
} CdbDispatcher;

CdbDispatcher* GetCurrentDispatcher(void);
#endif   /* CDBDISPTPZ_H */
