
#include "config.h"
#include "porting.h"
#include <stdio.h>

#include "esg_custom_table.h"

#include "Genrand.h"
#include "esg_print.h"


typedef struct 
{
    decimal_t val;
    decimal_t min;
    decimal_t max;
} decimal_comb_t;

typedef struct
{
    date_t val;
    date_t min;
    date_t max;
} date_comb_t;

/* define a buffer to store generated data */
union {
    int uInt;
    short uShort;
    float uFloat;
    double uDouble;
    decimal_t uDecimal;
    decimal_comb_t uDecComb;
    ds_key_t uKey;//long long
    char uStr[2048];
    date_t uDate;
    date_comb_t uDateComb;
    ds_addr_t uAddr;

    ds_pricing_t uPrice;//?

}
buffer;





int esg_init_col(cus_col_t *col, char *name, int type, int len, int isNull, int precision, int scale, char *min, char *max, long long seq)
{
	strcpy(col->col_name, name);
    col->type = type;
    col->length = len;
    col->isNull = isNull;
	col->precision = precision;
    col->scale = scale;
	if (NULL != min)
	{
		strcpy(col->min, min);
	}

	if (NULL != max)
	{
		strcpy(col->max, max);
	}
	col->seq = seq;

}


void esg_mk_pr_col(cus_col_t *col, int col_num, int col_count)
{

    int isLastCol = col_num == col_count;

    if (col->nullable && genrand_boolean(NULL, col_num))
    {
        esg_print_null(col_num, !isLastCol);
        return;
    }

    
	switch(col->type)
	{
		case CUS_INT:
            buffer.uKey = 0;
		    //genrand_integer(&buffer.uInt, DIST_UNIFORM, col->min, col->max, 0, col_idx);
            genrand_key(&buffer.uKey, DIST_UNIFORM, atoll(col->min), atoll(col->max), 0, col_num);
            esg_print_key(col_num, buffer.uKey, !isLastCol);
		    break;

	    case CUS_CHAR:
            gen_text(buffer.uStr, 1, col.length, col_num);
            esg_print_varchar(col_num, buffer.uStr, !isLastCol);
            break;
            
		case CUS_DECIMAL:
            
            strtodec(&buffer.uDecComb.min, col->min);
            strtodec(&buffer.uDecComb.max, col->max);
            genrand_decimal(&buffer.uDecComb.val, DIST_UNIFORM, &buffer.uDecComb.min, &buffer.uDecComb.max, NULL, col_num);
            esg_print_decimal(col_num, &buffer.uDecComb.val, !isLastCol);
            break;
            
		case CUS_DATE:

            esg_strtodate(&buffer.uDateComb.min, col->min);
            esg_strtodate(&buffer.uDateComb.max, col->max);
            genrand_date(&buffer.uDateComb.val, DIST_UNIFORM, &buffer.uDateComb.min, &buffer.uDateComb.max, NULL, col_num);
            esg_print_date(col_num, &buffer.uDateComb.val, !isLastCol);
            break;
            
		case CUS_TIME:
		case CUS_TIMESTAMP:
		case CUS_RANDOM:
		case CUS_EMAIL:
		case CUS_INT_YEAR:
		case CUS_INT_MONTH:
		case CUS_INT_DAY:
		case CUS_INT_HOUR:
		case CUS_INT_MINUTE:
		case CUS_INT_SECOND:
		case CUS_INT_YM:
		case CUS_INT_MD:
		case CUS_INT_DH:
		case CUS_INT_HM:
		case CUS_INT_MS:
        default:
            assert(1);
            break;
		
		
	}
	
}


void esg_gen_data(cus_table_t *table, ds_key_t kFirstRow, ds_key_t kRowCount)
{
    int col_idx;
	int direct,
		bIsVerbose,
		nLifeFreq,
		nMultiplier,
      nChild;
	ds_key_t i,
		kTotalRows;
   //tdef *pT = getSimpleTdefsByNumber(tabid);
   //tdef *pC;
   //table_func_t *pF = getTdefFunctionsByNumber(tabid);
	
	kTotalRows = kRowCount;
	direct = 0;//is_set("DBLOAD");
	bIsVerbose = is_set("VERBOSE") && !is_set("QUIET");
	/**
	set the frequency of progress updates for verbose output
	to greater of 1000 and the scale base
	*/
	nLifeFreq = 1;
	nMultiplier = kTotalRows / 10;//dist_member(NULL, "rowcounts", tabid + 1, 2);
	for (i=0; nLifeFreq < nMultiplier; i++)
		nLifeFreq *= 10;
	if (nLifeFreq < 1000)
		nLifeFreq = 1000;

	/*if (bIsVerbose)
	{
		if (pT->flags & FL_PARENT)
      {
         nChild = pT->nParam;
         pC = getSimpleTdefsByNumber(nChild);
			fprintf(stderr, "%s %s and %s ... ", 
			(direct)?"Loading":"Writing", 
			pT->name, pC->name);
      }
		else
			fprintf(stderr, "%s %s ... ", 
			(direct)?"Loading":"Writing", 
			pT->name);
	}*/
		
	/*
    * small tables use a constrained set of geography information
    */
   if (pT->flags & FL_SMALL)
      resetCountCount();

   for (i=kFirstRow; kRowCount; i++,kRowCount--)
	{
		if (bIsVerbose && i && (i % nLifeFreq) == 0)
			fprintf(stderr, "%3d%%\b\b\b\b",(int)(((kTotalRows - kRowCount)*100)/kTotalRows));
		
		/* not all rows that are built should be printed. Use return code to deterine output * /
		if (!pF->builder(NULL, i))
			if (pF->loader[direct](NULL))
			{
				fprintf(stderr, "ERROR: Load failed on %s!\n", getTableNameByID(tabid));
				exit(-1);
			}
			row_stop(tabid);*/
		for (col_idx = 0; col_idx < table->col_num; col_idx++)
		{
            esg_mk_pr_col(table->cols + col_idx, col_idx + 1, table->col_num);
		}
	}
	if (bIsVerbose)
			fprintf(stderr, "Done    \n");	
	esg_print_close(ta);

	return;
}


void esg_gen_stream(cus_table_t *tal)
{
	rng_t stream;
    int idx;

    memset(Streams, 0, CUS_MAX_COLUMNS * sizeof(rng_t));

    for (idx = 1; idx < tal->col_num + 1; idx++)
    {
        stream = Streams[idx];

        stream.nUsed = 0;
        stream.nUsedPerRow = 0;
        stream.nSeed = 0;
        stream.nInitialSeed = 0;
        stream.nColumn = idx;
        stream.nTable = 1;
        stream.nDuplicateOf = stream.nColumn;
        
    }
	
}


int
esg_split_work (ds_key_t * pkFirstRow, ds_key_t * pkRowCount)
{
  ds_key_t kTotalRows, kRowsetSize, kExtraRows;
  int nParallel, nChild;

  kTotalRows = get_rowcount(tnum);
  nParallel = get_int ("PARALLEL");
  nChild = get_int ("CHILD");

  /* 
   * 1. small tables aren't paralelized 
   * 2. nothing is parallelized unless a command line arg is supplied 
   */
  *pkFirstRow = 1;
  *pkRowCount = kTotalRows;

  if (kTotalRows < 1000000)
	 {
		if (nChild > 1)			  /* small table; only build it once */
		  {
			 *pkFirstRow = 1;
			 *pkRowCount = 0;
			 return (0);
		  }
		return (1);
	 }

  if (!is_set ("PARALLEL"))
	 {
		return (1);
	 }

  /*
   * at this point, do the calculation to set the rowcount for this part of a parallel build
   */
  kExtraRows = kTotalRows % nParallel;
  kRowsetSize = (kTotalRows - kExtraRows) / nParallel;

  /* start the starting row id */
  *pkFirstRow += (nChild - 1) * kRowsetSize;
  if (kExtraRows && (nChild - 1))
	 *pkFirstRow += ((nChild - 1) < kExtraRows) ? (nChild - 1) : kExtraRows;

  /* set the rowcount for this child */
  *pkRowCount = kRowsetSize;
  if (kExtraRows && (nChild <= kExtraRows))
	 *pkRowCount += 1;

  return (1);
}


#ifdef ESG_TEST
cus_table_t* esg_gen_table()
{
	cus_table_t *tab = malloc(sizeof(cus_table_t));
    assert(tab == NULL);
    
	tab->col_num = 6;
    
	memset(tab->cols, 0, CUS_MAX_COLUMNS * sizeof(cus_col_t));
	
	esg_init_col(&tab->cols[0], "AA", CUS_INT, 0, 1, 0, 0, "1", "1000", 0);
	esg_init_col(&tab->cols[1], "BB", CUS_INT, 0, 1, 0, 0, NULL, NULL, 123);
	esg_init_col(&tab->cols[2], "CC", CUS_CHAR, 20, 0, 0, 0, NULL, NULL, 0);
	esg_init_col(&tab->cols[3], "DD", CUS_DECIMAL, 1, 0, 8, 4, "1.0001", "8000.0001", 0);
	esg_init_col(&tab->cols[4], "EE", CUS_DATE, 0, 0, 0, 0, NULL, NULL, 0);
	esg_init_col(&tab->cols[5], "FF", CUS_DATE, 0, 1, 0, 0, "2011-01-01", "2015-01-01", 0);

    return tab;
}
#endif
