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
#ifndef CDBDISP_THREAD_H
#define CDBDISP_THREAD_H

struct Gang;                        /* #include "cdb/cdbgang.h" */

struct CdbDispatchDirectDesc;
struct CdbDispatcherState;

void cdbdisp_dispatchToGang_V1(struct Gang *gp, char* query, int len, CdbDispatchDirectDesc *disp_direct);

void CdbCheckDispatchResultInt_V1(struct CdbDispatcherState *ds);

int getThreadCount(void);

#endif   /* CDBDISP_H */
