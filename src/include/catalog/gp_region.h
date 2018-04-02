/*-------------------------------------------------------------------------
 *
 * gp_region.h
 *	  definitions for the gp_distribution_region catalog table
 *
 * Portions Copyright (c) 2005-2011, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/catalog/gp_region.h
 *
 * NOTES
 *
 *-------------------------------------------------------------------------
 */

#ifndef _GP_REGION_H_
#define _GP_REGION_H_

#include "access/attnum.h"
#include "catalog/genbki.h"
#include "nodes/pg_list.h"
#include "utils/palloc.h"

/*
 * Defines for gp_region
 */
#define GpRegionRelationName		"gp_distribution_region"

#define GpRegionRelationId  5004

CATALOG(gp_distribution_region,5004) BKI_SHARED_RELATION 
{
	NameData	regname; /* region name */
	int2		dbid;
	int2		content;
} FormData_gp_region;

/* GPDB added foreign key definitions for gpcheckcat. */
FOREIGN_KEY(dbid REFERENCES gp_segment_configuration(dbid));
FOREIGN_KEY(content REFERENCES gp_segment_configuration(content));

#define Natts_gp_region		4
#define Anum_gp_region_regid	1
#define Anum_gp_region_regname	2
#define Anum_gp_region_dbid 3
#define Anum_gp_region_content 4

typedef enum GpRegionType
{
	REGIONTYPE_ALL,
	REGIONTYPE_PARTIAL,
} GpRegionType;

typedef struct GpRegion
{
	//GpRegionType ptype;
	List	*members;
	Bitmapset	*index;
} GpRegion;

#endif
