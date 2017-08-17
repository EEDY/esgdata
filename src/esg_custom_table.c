
#include "config.h"
#include "porting.h"
#include <stdio.h>
#include <assert.h>
#include <math.h>

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


typedef struct
{

    ds_key_t uKey_1;//long long
    ds_key_t uKey_2;//long long
    time_p val;
	time_p min;
	time_p max;
	int pre_count;
} time_comb_t;


typedef struct
{

    ds_key_t uKey_1;//long long
    ds_key_t uKey_2;//long long
    ds_key_t uKey_3;//long long
    interval_t val;
	interval_t min;
	interval_t max;
} interval_comb_t;

typedef struct 
{
    date_comb_t uDateComb;
	time_comb_t uTime;
} timestamp_comb_t;


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
	time_comb_t uTime;
    interval_comb_t uInterval;
    timestamp_comb_t uTimestamp;
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


void esg_gen_data_date(cus_col_t *col, int col_num, date_comb_t *date_comb)
{
    memset(date_comb, 0, sizeof(*date_comb));

    if (strlen(col->min) > 0)
        esg_strtodate(&date_comb->min, col->min);
    else
        esg_strtodate(&date_comb->min, "1999-1-1");

    if (strlen(col->max) > 0)
        esg_strtodate(&date_comb->max, col->max);
    else
        esg_strtodate(&date_comb->max, "2017-1-1");

    genrand_date(&date_comb->val, DIST_UNIFORM, &date_comb->min, &date_comb->max, NULL, col_num);

    return ;
}

void esg_gen_data_time(cus_col_t *col, int col_num, int range_enable, time_comb_t *time_comb)
{
    int max_time, max_pre, min_time, min_pre ;

    if (col->precision < 0)
        col->precision = 0;
    else if (col->precision >6)
        col->precision = 6;

    memset(time_comb, 0, sizeof(*time_comb));

    if (range_enable && strlen(col->min) > 0)
        esg_strtotime(&time_comb->min, col->min, col->precision);
    else
        esg_strtotime(&time_comb->min, "00:00:00.000000", col->precision);

    if (range_enable && strlen(col->max) > 0)
        esg_strtotime(&time_comb->max, col->max, col->precision);
    else 
        esg_strtotime(&time_comb->max, "23:59:59.999999", col->precision);

    min_time = time_comb->min.hour * 3600 + time_comb->min.minute * 60 + time_comb->min.second;
    min_pre = time_comb->min.precision;

    max_time = time_comb->max.hour * 3600 + time_comb->max.minute * 60 + time_comb->max.second;
    max_pre = time_comb->max.precision;
				
    genrand_key(&time_comb->uKey_1, DIST_UNIFORM, min_time, max_time, 0, col_num);
    genrand_key(&time_comb->uKey_2, DIST_UNIFORM, min_pre, max_pre, 0, col_num);

    return ;
}


void esg_mk_pr_col(cus_io_func_t *io, cus_col_t *col, int col_num, int col_count, ds_key_t row_num)
{

    int isLastCol = col_num == col_count;
    ds_key_t min;
    ds_key_t max;
	ds_key_t max_time, max_pre, min_time, min_pre ;

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
            {
                min = atoll(col->min);
            }
            else
                min = 0;

            if (strlen(col->max) > 0)
            {
                max = atoll(col->max);
                if (max > MAXINT)
                {
                    fprintf (stderr, "WARNING: max value of INT type exceeds MAXINT, column %d.\n", col_num);
                }
            }
            else
                max = 2147438647;

            buffer.uKey = 0;
		    //genrand_integer(&buffer.uInt, DIST_UNIFORM, col->min, col->max, 0, col_idx);
            genrand_key(&buffer.uKey, DIST_UNIFORM, min, max, 0, col_num);
            io->out_key(col_num, buffer.uKey, !isLastCol);
		    break;

        case CUS_BIG_INT:

            buffer.uKey = 0;

            if (strlen(col->min) > 0)
                min = atoll(col->min);
            else
                min = 0;

            if (strlen(col->max) > 0)
                max = atoll(col->max);
            else
                max = 9223372036854775807;
            
            genrand_key(&buffer.uKey, DIST_LONG, min, max, 0, col_num);
            io->out_key(col_num, buffer.uKey, !isLastCol);
		    break;

	    case CUS_CHAR:

			buffer.uStr[0] = '\0';
			
            gen_text(buffer.uStr, 1, col->length, col_num);
            io->out_varchar(col_num, buffer.uStr, !isLastCol);
            break;
            
		case CUS_DECIMAL:
        {
            int isset_min = 0;
            int isset_max = 0;
            int tmp_pre = 0;
            int tmp_scale = 0;
            
			memset(&buffer.uDecComb, 0, sizeof(buffer.uDecComb));
            //get min, max if set
            if (strlen(col->min) > 0)
            {
                strtodec(&buffer.uDecComb.min, col->min);
            }
            else
            {
                buffer.uDecComb.min.precision = col->precision;
                buffer.uDecComb.min.scale = col->scale;
            }

            if (strlen(col->max) > 0)
            {
                strtodec(&buffer.uDecComb.max, col->max);
            }
            else
            {
                buffer.uDecComb.max.number = pow(10, col->precision + col->scale) - 1;
                buffer.uDecComb.max.precision = col->precision;
                buffer.uDecComb.max.scale = col->scale;
            }

            buffer.uDecComb.val.precision = col->precision;
            buffer.uDecComb.val.scale = col->scale;

            if (buffer.uDecComb.val.scale != buffer.uDecComb.max.scale || buffer.uDecComb.max.scale != buffer.uDecComb.min.scale)
                decimal_transform(&buffer.uDecComb.val, &buffer.uDecComb.min, &buffer.uDecComb.max);
            
            genrand_decimal(&buffer.uDecComb.val, DIST_LONG, &buffer.uDecComb.min, &buffer.uDecComb.max, NULL, col_num);
            io->out_decimal(col_num, &buffer.uDecComb.val, !isLastCol);
        }
            break;
            
		case CUS_DATE:

            esg_gen_data_date(col, col_num, &buffer.uDateComb);
            io->out_date(col_num, &buffer.uDateComb.val, !isLastCol);
            break;
            
            
		case CUS_TIME:

            esg_gen_data_time(col, col_num, 1, &buffer.uTime); //need to consider time range
			io->out_time(col_num, col->precision, buffer.uTime.uKey_1, buffer.uTime.uKey_2, !isLastCol);

			break;
			
		case CUS_TIMESTAMP:

            esg_gen_data_date(col, col_num, &buffer.uTimestamp.uDateComb);
            esg_gen_data_time(col, col_num, 0, &buffer.uTimestamp.uTime); //need to consider time range
            io->out_timestamp(col_num, &buffer.uTimestamp.uDateComb.val, col->precision, buffer.uTimestamp.uTime.uKey_1, buffer.uTimestamp.uTime.uKey_2, !isLastCol);
            break;
            
		case CUS_INT_YEAR:
		case CUS_INT_MONTH:
        case CUS_INT_DAY:
		case CUS_INT_HOUR:
		case CUS_INT_MINUTE:
            
            if (col->precision <= 0)
				col->precision = 2;
			else if (col->precision >18)
				col->precision = 18;

            memset(&buffer.uInterval, 0, sizeof(buffer.uInterval));

            buffer.uInterval.val.l_precision = col->precision;

            max = pow(10, buffer.uInterval.val.l_precision) - 1;
            genrand_key(&buffer.uInterval.uKey_1, DIST_LONG, 0, max, 0, col_num);

            io->out_key(col_num, buffer.uInterval.uKey_1, !isLastCol);
            break;

		case CUS_INT_SECOND:

            if (col->precision <= 0)
				col->precision = 2;
			else if (col->precision > 18)
				col->precision = 18;

            memset(&buffer.uInterval, 0, sizeof(buffer.uInterval));

            buffer.uInterval.val.l_precision = col->precision;

            if (col->scale <= 0 || col->scale > 6)
                col->scale = 6;

            buffer.uInterval.val.f_precision = col->scale;
            buffer.uInterval.max.l_precision = pow(10, buffer.uInterval.val.l_precision) - 1;
            buffer.uInterval.max.f_precision = pow(10, buffer.uInterval.val.f_precision) - 1;

            genrand_key(&buffer.uInterval.uKey_1, DIST_LONG, 0, buffer.uInterval.max.l_precision, 0, col_num);
            genrand_key(&buffer.uInterval.uKey_2, DIST_LONG, 0, buffer.uInterval.max.f_precision, 0, col_num);

            io->out_interval(col_num, col->type, buffer.uInterval.uKey_1, buffer.uInterval.uKey_2, 0, buffer.uInterval.val.l_precision, buffer.uInterval.val.f_precision, !isLastCol);
            break;
            
		case CUS_INT_YM:

            if (col->precision <= 0)
				col->precision = 2;
			else if (col->precision > 18)
				col->precision = 18;

            memset(&buffer.uInterval, 0, sizeof(buffer.uInterval));

            buffer.uInterval.val.l_precision = col->precision;
            max = pow(10, buffer.uInterval.val.l_precision) - 1;
            
            genrand_key(&buffer.uInterval.uKey_1, DIST_LONG, 0, max, 0, col_num);
            genrand_key(&buffer.uInterval.uKey_2, DIST_LONG, 0, 11, 0, col_num);

		    io->out_interval(col_num, col->type, buffer.uInterval.uKey_1, buffer.uInterval.uKey_2, 0, buffer.uInterval.val.l_precision, 0, !isLastCol);
            break;
            
		case CUS_INT_DH:

            if (col->precision <= 0)
				col->precision = 2;
			else if (col->precision > 18)
				col->precision = 18;

            memset(&buffer.uInterval, 0, sizeof(buffer.uInterval));

            buffer.uInterval.val.l_precision = col->precision;
            max = pow(10, buffer.uInterval.val.l_precision) - 1;
            
            genrand_key(&buffer.uInterval.uKey_1, DIST_LONG, 0, max, 0, col_num);
            genrand_key(&buffer.uInterval.uKey_2, DIST_LONG, 0, 23, 0, col_num);

		    io->out_interval(col_num, col->type, buffer.uInterval.uKey_1, buffer.uInterval.uKey_2, 0, buffer.uInterval.val.l_precision, 0, !isLastCol);
            break;
            
		case CUS_INT_HM:

            if (col->precision <= 0)
				col->precision = 2;
			else if (col->precision > 18)
				col->precision = 18;

            memset(&buffer.uInterval, 0, sizeof(buffer.uInterval));

            buffer.uInterval.val.l_precision = col->precision;
            max = pow(10, buffer.uInterval.val.l_precision) - 1;

            genrand_key(&buffer.uInterval.uKey_1, DIST_LONG, 0, max, 0, col_num);
            genrand_key(&buffer.uInterval.uKey_2, DIST_LONG, 0, 59, 0, col_num);

            io->out_interval(col_num, col->type, buffer.uInterval.uKey_1, buffer.uInterval.uKey_2, 0, buffer.uInterval.val.l_precision, 0, !isLastCol);
            break;
            
		case CUS_INT_MS:
            
            if (col->precision <= 0)
				col->precision = 2;
			else if (col->precision > 18)
				col->precision = 18;

            if (col->scale <= 0 || col->scale >6)
                col->scale = 6;

            memset(&buffer.uInterval, 0, sizeof(buffer.uInterval));

            buffer.uInterval.val.l_precision = col->precision;
            buffer.uInterval.val.f_precision = col->scale;
            
            buffer.uInterval.max.l_precision = 60 * (pow(10, buffer.uInterval.val.l_precision) - 1) + 59;
            buffer.uInterval.max.f_precision = pow(10, buffer.uInterval.val.f_precision) - 1;

            genrand_key(&buffer.uInterval.uKey_1, DIST_LONG, 0, buffer.uInterval.max.l_precision, 0, col_num);
            genrand_key(&buffer.uInterval.uKey_2, DIST_LONG, 0, buffer.uInterval.max.f_precision, 0, col_num);
 
            io->out_interval(col_num, col->type, buffer.uInterval.uKey_1, buffer.uInterval.uKey_2, 0, buffer.uInterval.val.l_precision, buffer.uInterval.val.f_precision, !isLastCol);
            break;
            
        case CUS_INT_DS:
            
            if (col->precision <= 0)
				col->precision = 2;
			else if (col->precision > 18)
				col->precision = 18;

            if (col->scale <= 0 || col->scale >6)
                col->scale = 6;

            memset(&buffer.uInterval, 0, sizeof(buffer.uInterval));

            buffer.uInterval.val.l_precision = col->precision;
            buffer.uInterval.val.f_precision = col->scale;
            
            buffer.uInterval.max.l_precision = pow(10, buffer.uInterval.val.l_precision) - 1;
            buffer.uInterval.max.f_precision = pow(10, buffer.uInterval.val.f_precision) - 1;
            

            genrand_key(&buffer.uInterval.uKey_1, DIST_LONG, 0, buffer.uInterval.max.l_precision, 0, col_num);
            genrand_key(&buffer.uInterval.uKey_2, DIST_LONG, 0, 86399, 0, col_num);
            genrand_key(&buffer.uInterval.uKey_3, DIST_LONG, 0, buffer.uInterval.max.f_precision, 0, col_num);

            io->out_interval(col_num, col->type, buffer.uInterval.uKey_1, buffer.uInterval.uKey_2, buffer.uInterval.uKey_3, buffer.uInterval.val.l_precision, buffer.uInterval.val.f_precision, !isLastCol);
            break;
            
        case CUS_RANDOM:
        case CUS_EMAIL:
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
        case CUS_TIME:
			ret = 2;
			break;
		case CUS_TIMESTAMP:
			ret = 3;
			break;
        case CUS_BIG_INT:
            ret = 2;
            break;
        //using genrand_key(DIST_LONG)
		case CUS_DECIMAL:
        case CUS_INT_YEAR:
		case CUS_INT_MONTH:
        case CUS_INT_DAY:
		case CUS_INT_HOUR:
		case CUS_INT_MINUTE:
            ret = 2;
            break;
        case CUS_INT_SECOND:
        case CUS_INT_YM:
        case CUS_INT_DH:
        case CUS_INT_HM:
        case CUS_INT_MS:
            ret = 4;
            break;
        case CUS_INT_DS:
            ret = 6;
            break;
		case CUS_DATE:
        case CUS_SEQ:
		case CUS_INT:
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
		

   table->io.start(table);
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
        table->io.end(table);
	}
	if (bIsVerbose)
			fprintf(stderr, "Done    \n");	
	table->io.close(table);

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
        tab->io.out_timestamp = esg_hdfs_timestamp;
        tab->io.out_decimal = esg_hdfs_decimal;
        tab->io.out_key = esg_hdfs_key;
        tab->io.out_id = esg_hdfs_id;
        tab->io.out_boolean = esg_hdfs_boolean;
        tab->io.out_string = esg_hdfs_string;
        tab->io.out_null = esg_hdfs_null;
        tab->io.out_interval = esg_print_interval;
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
        tab->io.out_timestamp = esg_print_timestamp;
        tab->io.out_decimal = esg_print_decimal;
        tab->io.out_key = esg_print_key;
        tab->io.out_id = esg_print_id;
        tab->io.out_boolean = esg_print_boolean;
        tab->io.out_string = esg_print_string;
        tab->io.out_null = esg_print_null;
        tab->io.out_interval = esg_print_interval;
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

