#include "config.h"
#include "porting.h"
#include <stdio.h>
#ifdef WIN32
#include <io.h>
#else
#include <unistd.h>
#endif
#include "r_params.h"
#include "genrand.h"
#include "esg_custom_table.h"
#include "date.h"
#include "decimal.h"
#include "constants.h"
#include "build_support.h"

#include "hdfs.h"


static int current_table = -1;

static hdfsFS dfs;
static hdfsFile outFile;

static char buffer[2048];


void 
esg_hdfs_close(cus_table_t *table)
{
    hdfsCloseFile(dfs, outFile);

	return;
}

int
esg_hdfs_separator (int sep)
{
	int res = 0;
	static char *pDelimiter;
	static int init = 0;
	
	if (!init)
	{
        pDelimiter = get_str ("DELIMITER");
        init = 1;
	}
	
	if (sep)
	{
		//if (fwrite(pDelimiter, 1, 1, fpOutfile) != 1)
        if (hdfsWrite(dfs, outFile, pDelimiter, strlen(pDelimiter)) != 1)
        {
			fprintf(stderr, "ERROR: Failed to write delimiter\n");
			exit(-1);
		}
	}
	   
	return (res);
}


/*
* Routine: dbg_print()
* Purpose: genralized data print routine
* Algorithm:
* Data Structures:
*
* Params:
* Returns:
* Called By: 
* Calls: 
* Assumptions:
* Side Effects:
* TODO: 20000113 need better separator handling
*		20020125 cast to/from 64b is messy, assumes int/pointer are same size
*/
void
esg_hdfs_integer (int nColumn, int val, int sep)
{
    buffer[0] = '\0';
    sprintf(buffer, "%d", val);

	//if (fprintf (fpOutfile, "%d", val) < 0)
    if (hdfsWrite(dfs, outFile, buffer, strlen(buffer)) < 0)
	{
		fprintf(stderr, "ERROR: Failed to write output for column %d\n", nColumn);
		exit(-1);
	}
	esg_hdfs_separator (sep);
	
	return;
}

void
esg_hdfs_varchar (int nColumn, char *val, int sep)
{
	size_t nLength;

	if (val != NULL)
	{
      nLength = strlen(val);

		//if (fwrite (val, 1, nLength, fpOutfile) != nLength)
		if (hdfsWrite(dfs, outFile, val, strlen(val)) != nLength)
			{
				fprintf(stderr, "ERROR: Failed to write output for column %d\n", nColumn);
				exit(-1);
			}
	}
	esg_hdfs_separator (sep);
	
   return;
}


void
esg_hdfs_char (int nColumn, char val, int sep)
{
	//if (fwrite (&val, 1, 1, fpOutfile) != 1)
	if (hdfsWrite(dfs, outFile, &val, 1)
	{
		fprintf(stderr, "ERROR: Failed to write output for column %d\n", nColumn);
		exit(-1);
	}

	esg_hdfs_separator (sep);

   return;
}

void
esg_hfds_date (int nColumn, date_t *val, int sep)
{
	if (NULL != val)
	{
		//if (fwrite(dttostr(val), 1, 10, fpOutfile) != 10)
        if (hdfsWrite(dfs, outFile, dttostr(val), 10) != 10)
		{
			fprintf(stderr, "ERROR: Failed to write output for column %d\n", nColumn);
			exit(-1);
		}
	}

	esg_hdfs_separator (sep);

	return;
}

void
esg_hdfs_time (int nColumn, ds_key_t val, int sep)
{
	int nHours, nMinutes, nSeconds;

	nHours = (int)(val / 3600);
	val -= 3600 * nHours;
	nMinutes = (int)(val / 60);
	val -= 60 * nMinutes;
	nSeconds = (int)(val % 60);
	
	if (val != -1)
	{
        buffer="";
        sprintf(buffer, "%02d:%02d:%02d", nHours, nMinutes, nSeconds);
		//fprintf(fpOutfile, "%02d:%02d:%02d", nHours, nMinutes, nSeconds);

        hdfsWrite(dfs, outFile, buffer, strlen(buffer));
	}

	esg_hdfs_separator (sep);
	   
	return;
}

void
esg_hdfs_decimal (int nColumn, decimal_t * val, int sep)
{
	int i;
	double dTemp;

    if (NULL != val)
    {
	    dTemp = val->number;

	    for (i=0; i < val->precision; i++)
	    	dTemp /= 10.0;

        buffer="";
        sprintf(buffer, "%.*f", val->precision, dTemp);
	    //if (fprintf(fpOutfile, "%.*f", val->precision, dTemp) < 0)
	    if (hdfsWrite(dfs, outFile, buffer, strlen(buffer) ) < 0)
	    {
	    	fprintf(stderr, "ERROR: Failed to write output for column %d\n", nColumn);
	    	exit(-1);
	    }
    }

	esg_hdfs_separator (sep);
	
	return;
}

void
esg_hdfs_key (int nColumn, ds_key_t val, int sep)
{
	if (val != (ds_key_t) -1) /* -1 is a special value, indicating NULL */
	{
        buffer="";
        sprintf(buffer, HUGE_FORMAT, val);
		//if (fprintf (fpOutfile, HUGE_FORMAT, val) < 0)
		if (hdfsWrite(dfs,outFile, buffer, strlen(buffer)) < 0)
		{
			fprintf(stderr, "ERROR: Failed to write output for column %d\n", nColumn);
			exit(-1);
		}
	}

	esg_hdfs_separator (sep);
	
	return;
}

void
esg_hdfs_id (int nColumn, ds_key_t val, int sep)
{
   char szID[RS_BKEY + 1];
   
   if (val != (ds_key_t) -1) /* -1 is a special value, indicating NULL */
   {
        mk_bkey(szID, val, 0);

            //if (fwrite (szID, 1, RS_BKEY, fpOutfile) < RS_BKEY)
            if (hdfsWrite(dfs, outFile, szID, RS_BKEY) < RS_BKEY)
            {
               fprintf(stderr, "ERROR: Failed to write output for column %d\n", nColumn);
               exit(-1);
            }
  }

  esg_hdfs_separator (sep);
   
   return;
}

void
esg_hdfs_boolean (int nColumn, int val, int sep)
{
        int len = 0;

		//if (fwrite ( ((val)?"TRUE":"FALSE"), 1, 1, fpOutfile) != 1)
		if (val)
            len = 4;
        else
            len = 5;

        if (hdfsWrite(dfs, outFile, ((val)?"TRUE":"FALSE"), len) != len)
		{
			fprintf(stderr, "ERROR: Failed to write output for column %d\n", nColumn);
			exit(-1);
		}

	esg_hdfs_separator (sep);

	return;
}

/*
* Routine: print_start(tbl)
* Purpose: open the output file for a given table
* Algorithm:
* Data Structures:
*
* Params:
* Returns:
* Called By: 
* Calls: 
* Assumptions:
* Side Effects:
* TODO: None
*/
int
esg_hdfs_start (cus_table_t *table)
{
    int res = 0;
    char path[256];
    //tdef *pTdef = getSimpleTdefsByNumber(tbl);

    current_table = 1;

    dfs = hdfsConnect("default", 0);
    if (table->outfile == NULL)
    {
        if (is_set("PARALLEL"))
            sprintf (path, "%s%c%s_%d_%d%s",
                get_str ("DIR"),
                PATH_SEP, table->tal_name, 
                get_int("CHILD"), get_int("PARALLEL"), (is_set("VALIDATE"))?get_str ("VSUFFIX"):get_str ("SUFFIX"));
        else 
        {
            if (is_set("UPDATE"))
                sprintf (path, "%s%c%s_%d%s",
                    get_str ("DIR"),
                    PATH_SEP, table->tal_name, get_int("UPDATE"), (is_set("VALIDATE"))?get_str ("VSUFFIX"):get_str ("SUFFIX"));
            else
                sprintf (path, "%s%c%s%s",
                    get_str ("DIR"),
                    PATH_SEP, table->tal_name, (is_set("VALIDATE"))?get_str ("VSUFFIX"):get_str ("SUFFIX"));
        }
        if ((hdfsExists (dfs, path) == 0) && !is_set ("FORCE"))
        {
            fprintf (stderr, "ERROR: HDFS:%s exists. Either remove it or use the FORCE option to overwrite it.\n", path);
            exit (-1);
        }

        outFile = hdfsOpenFile(dfs, path, O_WRONLY |O_CREAT, 0, 0, 0);
    }


    res = (outFile != NULL);

    if (!res)/* open failed! */
    {
        INTERNAL ("Failed to open HDFS output file!");
        exit(0);
    }

    table->flags |= 0x0008;//FL_OPEN;

    return (0);
}

/*
* Routine: 
* Purpose: 
* Algorithm:
* Data Structures:
*
* Params:
* Returns:
* Called By: 
* Calls: 
* Assumptions:
* Side Effects:
* TODO: None
*/
int
esg_hdfs_end (cus_table_t *table)
{
   int res = 0;
   static int init = 0;
   static int add_term = 0;
   static char term[10];

   if (!init)
     {
        if (is_set ("TERMINATE"))
          {
             strncpy (term, get_str ("DELIMITER"), 9);
             add_term = strlen(term);
             add_term = 0;//do not need term for a row end for now
          }
        init = 1;
     }

   if (add_term)
      //fwrite(term, 1, add_term, fpOutfile);
      hdfsWrite(dfs, outFile, (void*)buffer, strlen(buffer));
      
   //fprintf (fpOutfile, "\n");
   hdfsWrite(dfs, outFile, "\n", 1);
   
   //fflush(fpOutfile);
   hdfsFlush(dfs, outFile);

   return (res);
}

/*
* Routine: print_string()
* Purpose: genralized data print routine
* Algorithm:
* Data Structures:
*
* Params:
* Returns:
* Called By: 
* Calls: 
* Assumptions:
* Side Effects:
*/
void
esg_hdfs_string (char *szMessage, ds_key_t val)
{
	//if (fprintf (fpOutfile, szMessage, val) < 0)
    if (hdfsWrite(dfs, outFile, szMessage, strlen(szMessage)) < 0)
	{
		fprintf(stderr, "ERROR: Failed to write string\n");
		exit(-1);
	}

	return;
}


void
esg_hdfs_null(int nColumn, int sep)
{
    if (is_set ("DISPLAY_NULL"))
    {
        //fwrite("NULL", 1, 4, fpOutfile);
        hdfsWrite(dfs, outFile, "NULL", 4);
    }

	esg_hdfs_separator (sep);
}



