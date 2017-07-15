
#include "config.h"
#include "porting.h"
#include <stdio.h>
#include <assert.h>


#include "genrand.h"
#include "esg_custom_table.h"
#include "esg_print.h"
#include "esg_hdfs.h"

#include "pricing.h"
#include "r_params.h"
#include "misc.h"


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


rng_t Streams[CUS_MAX_COLUMNS];


int esg_init_col(cus_col_t *col, char *name, int type, int len, int nullable, int precision, int scale, char *min, char *max, long long seq)
{
	strcpy(col->col_name, name);
    col->type = type;
    col->length = len;
    col->nullable= nullable;
	col->precision = precision;
    col->scale = scale;
	if (NULL != min)
	{
		strcpy(col->min, min);
	}
    else
        col->min[0] = '\0';

	if (NULL != max)
	{
		strcpy(col->max, max);
	}
    else
        col->max[0] = '\0';
    
    col->seq = malloc(sizeof (*(col->seq)));
    *(col->seq) = seq;
}


void esg_mk_pr_col(cus_io_func_t *io, cus_col_t *col, int col_num, int col_count, ds_key_t row_num)
{

    int isLastCol = col_num == col_count;
    ds_key_t min;
    ds_key_t max;

    if (col->nullable && genrand_boolean(NULL, col_num))
    {
        esg_print_null(col_num, !isLastCol);
        return;
    }

    
	switch(col->type)
	{
        case CUS_SEQ:
            assert(col->seq != NULL);
            io->out_key(col_num, row_num + *(col->seq), !isLastCol);
            break;

		case CUS_INT:

            if (strlen(col->min) > 0)
                min = atoll(col->min);
            else
                min = 0xffffffff;

            if (strlen(col->max) > 0)
                max = atoll(col->max);
            else
                max = 0x7fffffff;
            
            buffer.uKey = 0;
		    //genrand_integer(&buffer.uInt, DIST_UNIFORM, col->min, col->max, 0, col_idx);
            genrand_key(&buffer.uKey, DIST_UNIFORM, min, max, 0, col_num);
            io->out_key(col_num, buffer.uKey, !isLastCol);
		    break;

	    case CUS_CHAR:
            gen_text(buffer.uStr, 1, col->length, col_num);
            io->out_varchar(col_num, buffer.uStr, !isLastCol);
            break;
            
		case CUS_DECIMAL:
            
            strtodec(&buffer.uDecComb.min, col->min);
            strtodec(&buffer.uDecComb.max, col->max);
            genrand_decimal(&buffer.uDecComb.val, DIST_UNIFORM, &buffer.uDecComb.min, &buffer.uDecComb.max, NULL, col_num);
            io->out_decimal(col_num, &buffer.uDecComb.val, !isLastCol);
            break;
            
		case CUS_DATE:

            if (strlen(col->min) > 0)
                esg_strtodate(&buffer.uDateComb.min, col->min);
            else
                esg_strtodate(&buffer.uDateComb.min, "1999-1-1");

            if (strlen(col->max) > 0)
                esg_strtodate(&buffer.uDateComb.max, col->max);
            else
                esg_strtodate(&buffer.uDateComb.max, "2017-1-1");
            
            genrand_date(&buffer.uDateComb.val, DIST_UNIFORM, &buffer.uDateComb.min, &buffer.uDateComb.max, NULL, col_num);
            io->out_date(col_num, &buffer.uDateComb.val, !isLastCol);
            break;
            
		case CUS_EMAIL:
            
		case CUS_TIME:
		case CUS_TIMESTAMP:
		case CUS_RANDOM:

            
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
            assert(0);
            break;
		
		
	}
	
}


int esg_pick_nUsedPerRow(cus_col_t *col)
{

    int ret = 0;

    switch (col->type)
    {
	    case CUS_CHAR:
            ret = col->length;
            break;
            
		case CUS_DATE:
        case CUS_SEQ:
		case CUS_INT:
		case CUS_DECIMAL:
        default:
            ret = 1;
            break;

    }

    return ret;
}



int
esg_checkSeeds (int nFirstColumn, int nLastColumn)
{
  int i, res, nReturnCode = 0;
  static int bInit = 0, bSetSeeds = 0;

  if (!bInit)
	 {
		bSetSeeds = is_set ("CHKSEEDS");
		bInit = 1;
	 }

  for (i = nFirstColumn; i <= nLastColumn; i++)
	 {
		while (Streams[i].nUsed < Streams[i].nUsedPerRow)
		  genrand_integer (&res, DIST_UNIFORM, 1, 100, 0, i);
		if (bSetSeeds)
		  {
			 if (Streams[i].nUsed > Streams[i].nUsedPerRow)
				{
				  fprintf (stderr, "Seed overrun on column %d. Used: %d\n",
							  i, Streams[i].nUsed);
				  Streams[i].nUsedPerRow = Streams[i].nUsed;
				  nReturnCode = 1;
				}
		  }
		Streams[i].nUsed = 0;	  /* reset for the next time */
	 }

  return (nReturnCode);
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
		

   esg_print_start(table);
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
            esg_mk_pr_col(&table->io, table->cols + col_idx, col_idx + 1, table->col_num, i);
		}
        esg_checkSeeds(1, table->col_num);
        esg_print_end(table);
	}
	if (bIsVerbose)
			fprintf(stderr, "Done    \n");	
	esg_print_close(table);

	return;
}

void esg_gen_stream(cus_table_t *table)
{
	rng_t *stream;
    cus_col_t *col;
    int idx;

    memset(Streams, 0, CUS_MAX_COLUMNS * sizeof(rng_t));

    for (idx = 1, col = table->cols; idx < table->col_num + 1; idx++, col++)
    {
        stream = &Streams[idx];

        stream->nUsed = 0;
        stream->nUsedPerRow = esg_pick_nUsedPerRow(col);
        if (col->nullable)
        {
            stream->nUsedPerRow++;
        }
        stream->nSeed = 0;
        stream->nInitialSeed = 0;
        stream->nColumn = idx;
        stream->nTable = 1;//fix to 1
        stream->nDuplicateOf = stream->nColumn;
        
    }

    stream = &Streams[idx];
    stream->nUsed = -1;
    stream->nUsedPerRow = -1;
    stream->nSeed = -1;
    stream->nInitialSeed = -1;
    stream->nColumn = -1;
    stream->nTable = -1;
    stream->nDuplicateOf = -1;
	
}


int
esg_split_work (ds_key_t * pkFirstRow, ds_key_t * pkRowCount)
{
  ds_key_t kTotalRows, kRowsetSize, kExtraRows;
  int nParallel, nChild;

  kTotalRows = get_ll("RCOUNT");
  nParallel = get_int ("PARALLEL");
  nChild = get_int ("CHILD");

  /* 
   * 1. small tables aren't paralelized 
   * 2. nothing is parallelized unless a command line arg is supplied 
   */
  *pkFirstRow = 1;
  *pkRowCount = kTotalRows;

  /*if (kTotalRows < 1000000)
	 {
		if (nChild > 1)			  /* small table; only build it once * /
		  {
			 *pkFirstRow = 1;
			 *pkRowCount = 0;
			 return (0);
		  }
		return (1);
	 }*/

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


cus_table_t* esg_gen_table()
{
	cus_table_t *tab = malloc(sizeof(cus_table_t));
    assert(tab != NULL);

    memset(tab, 0, sizeof(cus_table_t));

    return tab;
}

void esg_init_io(cus_table_t *tab)
{

    if (is_set("HDFS"))
    {
        tab->io.start = esg_hdfs_start;
        tab->io.end = esg_hdfs_end;
        tab->io.close = esg_hdfs_close;
        tab->io.out_separator = esg_hdfs_separator;
        tab->io.out_integer = esg_hdfs_integer;
        tab->io.out_varchar = esg_hdfs_varchar;
        tab->io.out_char = esg_hdfs_char;
        tab->io.out_date = esg_hdfs_date;
        tab->io.out_time = esg_hdfs_time;
        tab->io.out_decimal = esg_hdfs_decimal;
        tab->io.out_key = esg_hdfs_key;
        tab->io.out_id = esg_hdfs_id;
        tab->io.out_boolean = esg_hdfs_boolean;
        tab->io.out_string = esg_hdfs_string;
        tab->io.out_null = esg_hdfs_null;
    }
    else
    {
        tab->io.start = esg_print_start;
        tab->io.end = esg_print_end;
        tab->io.close = esg_print_close;
        tab->io.out_separator = esg_print_separator;
        tab->io.out_integer = esg_print_integer;
        tab->io.out_varchar = esg_print_varchar;
        tab->io.out_char = esg_print_char;
        tab->io.out_date = esg_print_date;
        tab->io.out_time = esg_print_time;
        tab->io.out_decimal = esg_print_decimal;
        tab->io.out_key = esg_print_key;
        tab->io.out_id = esg_print_id;
        tab->io.out_boolean = esg_print_boolean;
        tab->io.out_string = esg_print_string;
        tab->io.out_null = esg_print_null;
    }

}

cus_table_t* esg_gen_table_test()
{
	cus_table_t *tab = malloc(sizeof(cus_table_t));
    assert(tab != NULL);
    
	tab->col_num = 6;
    strcpy(tab->tal_name, "test_table");
	memset(tab->cols, 0, CUS_MAX_COLUMNS * sizeof(cus_col_t));
	
	esg_init_col(&tab->cols[0], "AA", CUS_INT, 0, 0, 0, 0, "1", "100", 0);
	esg_init_col(&tab->cols[1], "BB", CUS_INT, 0, 0, 0, 0, NULL, NULL, 123);
	esg_init_col(&tab->cols[2], "CC", CUS_CHAR, 20, 0, 0, 0, NULL, NULL, 0);
	esg_init_col(&tab->cols[3], "DD", CUS_DECIMAL, 0, 0, 8, 4, "1.0001", "80.0001", 0);
	esg_init_col(&tab->cols[4], "EE", CUS_DATE, 0, 0, 0, 0, NULL, NULL, 0);
	esg_init_col(&tab->cols[5], "FF", CUS_DATE, 0, 0, 0, 0, "2011-1-1", "2015-1-1", 0);

    return tab;
}

