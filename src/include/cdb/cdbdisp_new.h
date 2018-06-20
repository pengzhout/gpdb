#ifndef CDBDISP_NEW_H
#define CDBDISP_NEW_H

#include "cdb/cdbdisp.h"

struct CdbPgResults;
struct SliceTable;
struct Estate;
struct Portal;

typedef enum DispatcherWaitMode
{
	DISPATCHER_WAIT_BLOCK = 0,		/* wait until QE fully completes */
	DISPATCHER_WAIT_NONBLOCK,		/* wait until QE fully completes */
	DISPATCHER_WAIT_FINISH,			/* send query finish */
	DISPATCHER_WAIT_CANCEL			/* send query cancel */
} DispatcherWaitMode;

typedef struct DispatchResults
{
	int	count;
	List	*members;
} DispatchResults;

typedef struct DispatcherState
{
	/* status info */
	bool	connected;

	/* error handle */
	bool	cancelOnError;
	bool	destroy;
	int	errcode;
	DispatcherWaitMode	waitMode;	

	/* resource info */
	List	*allocatedGangs;
	List	*results;

	/* for plan */
	struct QueryDesc *queryDesc;
	struct PortalData *portal;
} DispatcherState;

extern struct DispatcherState * TopDispatcherState;
extern struct DispatcherState * TopTransactionDispatcherState;
extern struct DispatcherState * CurrentDispatcherState;

void DispatcherState_Init(void);
struct DispatcherState * DispatcherState_Create(const char* name);
void DispatcherState_Connect(void);
void DispatcherState_Close(void);
void DispatcherState_Join(DispatcherWaitMode waitMode);
void DispatcherState_GetResults(struct CdbPgResults *cdb_pgresults);

void DispatcherState_DispatchCommand(char *query, int len);

void DispatcherState_AssignGangs(struct SliceTable *sliceTable, int rootSliceIdx, bool extendedQuery);
void DispatcherState_DispatchCommandToGangs(char *queryText, int queryTextLen, struct SliceTable *sliceTable, int rootSliceIdx);
void DispatcherState_WaitDispatchFinish(void);

List * makeDefaultSegments(void);
#endif
