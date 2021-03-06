/*-------------------------------------------------------------------------
 *
 * execGrouping.c
 *	  executor utility routines for grouping, hashing, and aggregation
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/executor/execGrouping.c,v 1.22 2007/01/05 22:19:27 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/executor.h"
#include "parser/parse_oper.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"


static TupleHashTable CurTupleHashTable = NULL;

static uint32 TupleHashTableHash(const void *key, Size keysize);
static int TupleHashTableMatch(const void *key1, const void *key2,
					Size keysize);


/*****************************************************************************
 *		Utility routines for grouping tuples together
 *****************************************************************************/

/*
 * execTuplesMatch
 *		Return true if two tuples match in all the indicated fields.
 *
 * This actually implements SQL's notion of "not distinct".  Two nulls
 * match, a null and a not-null don't match.
 *
 * slot1, slot2: the tuples to compare (must have same columns!)
 * numCols: the number of attributes to be examined
 * matchColIdx: array of attribute column numbers
 * eqFunctions: array of fmgr lookup info for the equality functions to use
 * evalContext: short-term memory context for executing the functions
 *
 * NB: evalContext is reset each time!
 */
bool
execTuplesMatch(TupleTableSlot *slot1,
				TupleTableSlot *slot2,
				int numCols,
				AttrNumber *matchColIdx,
				FmgrInfo *eqfunctions,
				MemoryContext evalContext)
{
	MemoryContext oldContext;
	bool		result;
	int			i;

	/* Reset and switch into the temp context. */
	MemoryContextReset(evalContext);
	oldContext = MemoryContextSwitchTo(evalContext);

	/*
	 * We cannot report a match without checking all the fields, but we can
	 * report a non-match as soon as we find unequal fields.  So, start
	 * comparing at the last field (least significant sort key). That's the
	 * most likely to be different if we are dealing with sorted input.
	 */
	result = true;

	for (i = numCols; --i >= 0;)
	{
		AttrNumber	att = matchColIdx[i];
		Datum		attr1,
					attr2;
		bool		isNull1,
					isNull2;

		attr1 = slot_getattr(slot1, att, &isNull1);

		attr2 = slot_getattr(slot2, att, &isNull2);

		if (isNull1 != isNull2)
		{
			result = false;		/* one null and one not; they aren't equal */
			break;
		}

		if (isNull1)
			continue;			/* both are null, treat as equal */

		/* Apply the type-specific equality function */

		if (!DatumGetBool(FunctionCall2(&eqfunctions[i],
										attr1, attr2)))
		{
			result = false;		/* they aren't equal */
			break;
		}
	}

	MemoryContextSwitchTo(oldContext);

	return result;
}

/*
 * execTuplesUnequal
 *		Return true if two tuples are definitely unequal in the indicated
 *		fields.
 *
 * Nulls are neither equal nor unequal to anything else.  A true result
 * is obtained only if there are non-null fields that compare not-equal.
 *
 * Parameters are identical to execTuplesMatch.
 */
bool
execTuplesUnequal(TupleTableSlot *slot1,
				  TupleTableSlot *slot2,
				  int numCols,
				  AttrNumber *matchColIdx,
				  FmgrInfo *eqfunctions,
				  MemoryContext evalContext)
{
	MemoryContext oldContext;
	bool		result;
	int			i;

	/* Reset and switch into the temp context. */
	MemoryContextReset(evalContext);
	oldContext = MemoryContextSwitchTo(evalContext);

	/*
	 * We cannot report a match without checking all the fields, but we can
	 * report a non-match as soon as we find unequal fields.  So, start
	 * comparing at the last field (least significant sort key). That's the
	 * most likely to be different if we are dealing with sorted input.
	 */
	result = false;

	for (i = numCols; --i >= 0;)
	{
		AttrNumber	att = matchColIdx[i];
		Datum		attr1,
					attr2;
		bool		isNull1,
					isNull2;

		attr1 = slot_getattr(slot1, att, &isNull1);

		if (isNull1)
			continue;			/* can't prove anything here */

		attr2 = slot_getattr(slot2, att, &isNull2);

		if (isNull2)
			continue;			/* can't prove anything here */

		/* Apply the type-specific equality function */

		if (!DatumGetBool(FunctionCall2(&eqfunctions[i],
										attr1, attr2)))
		{
			result = true;		/* they are unequal */
			break;
		}
	}

	MemoryContextSwitchTo(oldContext);

	return result;
}


/*
 * execTuplesMatchPrepare
 *		Look up the equality functions needed for execTuplesMatch or
 *		execTuplesUnequal.
 *
 * The result is a palloc'd array.
 */
FmgrInfo *
execTuplesMatchPrepare(TupleDesc tupdesc,
					   int numCols,
					   AttrNumber *matchColIdx)
{
	FmgrInfo   *eqfunctions = (FmgrInfo *) palloc(numCols * sizeof(FmgrInfo));
	int			i;

	for (i = 0; i < numCols; i++)
	{
		AttrNumber	att = matchColIdx[i];
		Oid			typid = tupdesc->attrs[att - 1]->atttypid;
		Oid			eq_function;

		eq_function = equality_oper_funcid(typid);
		fmgr_info(eq_function, &eqfunctions[i]);
	}

	return eqfunctions;
}

/*
 * execTuplesHashPrepare
 *		Look up the equality and hashing functions needed for a TupleHashTable.
 *
 * This is similar to execTuplesMatchPrepare, but we also need to find the
 * hash functions associated with the equality operators.  *eqfunctions and
 * *hashfunctions receive the palloc'd result arrays.
 */
void
execTuplesHashPrepare(TupleDesc tupdesc,
					  int numCols,
					  AttrNumber *matchColIdx,
					  FmgrInfo **eqfunctions,
					  FmgrInfo **hashfunctions)
{
	int			i;

	*eqfunctions = (FmgrInfo *) palloc(numCols * sizeof(FmgrInfo));
	*hashfunctions = (FmgrInfo *) palloc(numCols * sizeof(FmgrInfo));

	for (i = 0; i < numCols; i++)
	{
		AttrNumber	att = matchColIdx[i];
		Oid			typid = tupdesc->attrs[att - 1]->atttypid;
		Operator	optup;
		Oid			eq_opr;
		Oid			eq_function;
		Oid			hash_function;

		optup = equality_oper(typid, false);
		eq_opr = oprid(optup);
		eq_function = oprfuncid(optup);
		ReleaseOperator(optup);
		hash_function = get_op_hash_function(eq_opr);
		if (!OidIsValid(hash_function)) /* should not happen */
			elog(ERROR, "could not find hash function for hash operator %u",
				 eq_opr);
		fmgr_info(eq_function, &(*eqfunctions)[i]);
		fmgr_info(hash_function, &(*hashfunctions)[i]);
	}
}


/*****************************************************************************
 *		Utility routines for all-in-memory hash tables
 *
 * These routines build hash tables for grouping tuples together (eg, for
 * hash aggregation).  There is one entry for each not-distinct set of tuples
 * presented.
 *****************************************************************************/

/*
 * Construct an empty TupleHashTable
 *
 *	numCols, keyColIdx: identify the tuple fields to use as lookup key
 *	eqfunctions: equality comparison functions to use
 *	hashfunctions: datatype-specific hashing functions to use
 *	nbuckets: initial estimate of hashtable size
 *	entrysize: size of each entry (at least sizeof(TupleHashEntryData))
 *	tablecxt: memory context in which to store table and table entries
 *	tempcxt: short-lived context for evaluation hash and comparison functions
 *
 * The function arrays may be made with execTuplesHashPrepare().  Note they
 * are not cross-type functions, but expect to see the table datatype(s)
 * on both sides.
 *
 * Note that keyColIdx, eqfunctions, and hashfunctions must be allocated in
 * storage that will live as long as the hashtable does.
 */
TupleHashTable
BuildTupleHashTable(int numCols, AttrNumber *keyColIdx,
					FmgrInfo *eqfunctions,
					FmgrInfo *hashfunctions,
					int nbuckets, Size entrysize,
					MemoryContext tablecxt, MemoryContext tempcxt)
{
	TupleHashTable hashtable;
	HASHCTL		hash_ctl;

	Assert(nbuckets > 0);
	Assert(entrysize >= sizeof(TupleHashEntryData));

	hashtable = (TupleHashTable) MemoryContextAlloc(tablecxt,
												 sizeof(TupleHashTableData));

	hashtable->numCols = numCols;
	hashtable->keyColIdx = keyColIdx;
	hashtable->eqfunctions = eqfunctions;
	hashtable->hashfunctions = hashfunctions;
	hashtable->tablecxt = tablecxt;
	hashtable->tempcxt = tempcxt;
	hashtable->entrysize = entrysize;
	hashtable->tableslot = NULL;	/* will be made on first lookup */
	hashtable->inputslot = NULL;

	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(TupleHashEntryData);
	hash_ctl.entrysize = entrysize;
	hash_ctl.hash = TupleHashTableHash;
	hash_ctl.match = TupleHashTableMatch;
	hash_ctl.hcxt = tablecxt;
	hashtable->hashtab = hash_create("TupleHashTable", (long) nbuckets,
									 &hash_ctl,
					HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

	return hashtable;
}

/*
 * Find or create a hashtable entry for the tuple group containing the
 * given tuple.  The tuple must be the same type as the hashtable entries.
 *
 * If isnew is NULL, we do not create new entries; we return NULL if no
 * match is found.
 *
 * If isnew isn't NULL, then a new entry is created if no existing entry
 * matches.  On return, *isnew is true if the entry is newly created,
 * false if it existed already.  Any extra space in a new entry has been
 * zeroed.
 */
TupleHashEntry
LookupTupleHashEntry(TupleHashTable hashtable, TupleTableSlot *slot,
					 bool *isnew)
{
	TupleHashEntry entry;
	MemoryContext oldContext;
	TupleHashTable saveCurHT;
	TupleHashEntryData dummy;
	bool		found;

	/* If first time through, clone the input slot to make table slot */
	if (hashtable->tableslot == NULL)
	{
		TupleDesc	tupdesc;

		oldContext = MemoryContextSwitchTo(hashtable->tablecxt);

		/*
		 * We copy the input tuple descriptor just for safety --- we assume
		 * all input tuples will have equivalent descriptors.
		 */
		tupdesc = CreateTupleDescCopy(slot->tts_tupleDescriptor);
		hashtable->tableslot = MakeSingleTupleTableSlot(tupdesc);
		MemoryContextSwitchTo(oldContext);
	}

	/* Need to run the hash functions in short-lived context */
	oldContext = MemoryContextSwitchTo(hashtable->tempcxt);

	/*
	 * Set up data needed by hash and match functions
	 *
	 * We save and restore CurTupleHashTable just in case someone manages to
	 * invoke this code re-entrantly.
	 */
	hashtable->inputslot = slot;

	saveCurHT = CurTupleHashTable;
	CurTupleHashTable = hashtable;

	/* Search the hash table */
	dummy.firstTuple = NULL;	/* flag to reference inputslot */
	entry = (TupleHashEntry) hash_search(hashtable->hashtab,
										 &dummy,
										 isnew ? HASH_ENTER : HASH_FIND,
										 &found);

	if (isnew)
	{
		if (found)
		{
			/* found pre-existing entry */
			*isnew = false;
		}
		else
		{
			/*
			 * created new entry
			 *
			 * Zero any caller-requested space in the entry.  (This zaps the
			 * "key data" dynahash.c copied into the new entry, but we don't
			 * care since we're about to overwrite it anyway.)
			 */
			MemSet(entry, 0, hashtable->entrysize);

			/* Copy the first tuple into the table context */
			MemoryContextSwitchTo(hashtable->tablecxt);
			entry->firstTuple = ExecCopySlotMemTuple(slot);

			*isnew = true;
		}
	}

	CurTupleHashTable = saveCurHT;

	MemoryContextSwitchTo(oldContext);

	return entry;
}

/*
 * Compute the hash value for a tuple
 *
 * The passed-in key is a pointer to TupleHashEntryData.  In an actual hash
 * table entry, the firstTuple field points to a tuple (in MinimalTuple
 * format).  LookupTupleHashEntry sets up a dummy TupleHashEntryData with a
 * NULL firstTuple field --- that cues us to look at the inputslot instead.
 * This convention avoids the need to materialize virtual input tuples unless
 * they actually need to get copied into the table.
 *
 * CurTupleHashTable must be set before calling this, since dynahash.c
 * doesn't provide any API that would let us get at the hashtable otherwise.
 *
 * Also, the caller must select an appropriate memory context for running
 * the hash functions. (dynahash.c doesn't change CurrentMemoryContext.)
 */
static uint32
TupleHashTableHash(const void *key, Size keysize)
{
	MemTuple tuple = ((const TupleHashEntryData *) key)->firstTuple;
	TupleTableSlot *slot;
	TupleHashTable hashtable = CurTupleHashTable;
	int			numCols = hashtable->numCols;
	AttrNumber *keyColIdx = hashtable->keyColIdx;
	uint32		hashkey = 0;
	int			i;

	if (tuple == NULL)
	{
		/* Process the current input tuple for the table */
		slot = hashtable->inputslot;
	}
	else
	{
		/* Process a tuple already stored in the table */
		/* (this case never actually occurs in current dynahash.c code) */
		slot = hashtable->tableslot;
		ExecStoreMemTuple(tuple, slot, false);
	}

	for (i = 0; i < numCols; i++)
	{
		AttrNumber	att = keyColIdx[i];
		Datum		attr;
		bool		isNull;

		/* rotate hashkey left 1 bit at each step */
		hashkey = (hashkey << 1) | ((hashkey & 0x80000000) ? 1 : 0);

		attr = slot_getattr(slot, att, &isNull);

		if (!isNull)			/* treat nulls as having hash key 0 */
		{
			uint32		hkey;

			hkey = DatumGetUInt32(FunctionCall1(&hashtable->hashfunctions[i],
												attr));
			hashkey ^= hkey;
		}
	}

	return hashkey;
}

/*
 * See whether two tuples (presumably of the same hash value) match
 *
 * As above, the passed pointers are pointers to TupleHashEntryData.
 *
 * CurTupleHashTable must be set before calling this, since dynahash.c
 * doesn't provide any API that would let us get at the hashtable otherwise.
 *
 * Also, the caller must select an appropriate memory context for running
 * the compare functions.  (dynahash.c doesn't change CurrentMemoryContext.)
 */
static int
TupleHashTableMatch(const void *key1, const void *key2, Size keysize)
{
	MemTuple tuple1 = ((const TupleHashEntryData *) key1)->firstTuple;

#ifdef USE_ASSERT_CHECKING
	MemTuple tuple2 = ((const TupleHashEntryData *) key2)->firstTuple;
#endif
	TupleTableSlot *slot1;
	TupleTableSlot *slot2;
	TupleHashTable hashtable = CurTupleHashTable;

	/*
	 * We assume that dynahash.c will only ever call us with the first
	 * argument being an actual table entry, and the second argument being
	 * LookupTupleHashEntry's dummy TupleHashEntryData.  The other direction
	 * could be supported too, but is not currently used by dynahash.c.
	 */
	Assert(tuple1 != NULL);
	slot1 = hashtable->tableslot;
	ExecStoreMemTuple(tuple1, slot1, false);
	Assert(tuple2 == NULL);
	slot2 = hashtable->inputslot;

	if (execTuplesMatch(slot1,
						slot2,
						hashtable->numCols,
						hashtable->keyColIdx,
						hashtable->eqfunctions,
						hashtable->tempcxt))
		return 0;
	else
		return 1;
}
