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

static FILE *fpOutfile = NULL;
static FILE *fpDeleteFile;
static char *arDeleteFiles[3] = {"", "delete_", "inventory_delete_"};

static int current_table = -1;


void 
esg_print_close(cus_table_t *table)
{
    //tdef *pTdef = getSimpleTdefsByNumber(tbl);

	fpOutfile = NULL;
	if (table->outfile)
	{
		fclose(table->outfile);
		table->outfile = NULL;
	}

	/*if (table->flags & FL_PARENT)
      esg_print_close(pTdef->nParam);*/

	return;
}

int
esg_print_separator (int sep)
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
		if (fwrite(pDelimiter, 1, 1, fpOutfile) != 1)
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
esg_print_integer (int nColumn, int val, int sep)
{

	if (fprintf (fpOutfile, "%d", val) < 0)
	{
		fprintf(stderr, "ERROR: Failed to write output for column %d\n", nColumn);
		exit(-1);
	}
	esg_print_separator (sep);
	
	return;
}

void
esg_print_varchar (int nColumn, char *val, int sep)
{
	size_t nLength;

	if (val != NULL)
	{
      nLength = strlen(val);
		
#ifdef STR_QUOTES
		if ((fwrite ("\"", 1, 1, fpOutfile) != 1) ||
			(fwrite (val, 1, nLength, fpOutfile) != nLength) ||
			(fwrite ("\"", 1, 1, fpOutfile)) != 1)
#else
		if (fwrite (val, 1, nLength, fpOutfile) != nLength)
#endif
			{
				fprintf(stderr, "ERROR: Failed to write output for column %d\n", nColumn);
				exit(-1);
			}
	}
	esg_print_separator (sep);
	
   return;
}


void
esg_print_char (int nColumn, char val, int sep)
{
	if (fwrite (&val, 1, 1, fpOutfile) != 1)
	{
		fprintf(stderr, "ERROR: Failed to write output for column %d\n", nColumn);
		exit(-1);
	}

	esg_print_separator (sep);

   return;
}

void
esg_print_date (int nColumn, date_t *val, int sep)
{
	if (NULL != val)
	{
		if (fwrite(dttostr(val), 1, 10, fpOutfile) != 10)
		{
			fprintf(stderr, "ERROR: Failed to write output for column %d\n", nColumn);
			exit(-1);
		}
	}

	esg_print_separator (sep);

	return;
}

void
esg_print_time (int nColumn, int precision, ds_key_t val_time, ds_key_t val_pre, int sep)
{
	int nHours, nMinutes, nSeconds;
    char format[64] = "%02d:%02d:%02d.%0xd";
	
	nHours = (int)(val_time/ 3600);
	val_time -= 3600 * nHours;
	nMinutes = (int)(val_time / 60);
	val_time -= 60 * nMinutes;
	nSeconds = (int)(val_time % 60);
	
	if (precision != 0)
	{
	    format[17] = 48+precision;
		fprintf(fpOutfile, format, nHours, nMinutes, nSeconds, val_pre);
	}
	else 
	{
        fprintf(fpOutfile, "%02d:%02d:%02d", nHours, nMinutes, nSeconds);
	}

	esg_print_separator (sep);
	   
	return;

}


void esg_print_timestamp (int nColumn, date_t *date, int precision, ds_key_t time, ds_key_t time_pre, int sep)
{
    int nHours, nMinutes, nSeconds;
    char format[64] = "%02d:%02d:%02d.%0xd";

    //pirnt date
    if (NULL != date)
    {
        if (fwrite(dttostr(date), 1, 10, fpOutfile) != 10)
        {
            fprintf(stderr, "ERROR: Failed to write output for column %d\n", nColumn);
            exit(-1);
        }
    }

    //print blank
    if (fwrite(" ", 1, 1, fpOutfile) != 1)
    {
        fprintf(stderr, "ERROR: Failed to write output for column %d\n", nColumn);
        exit(-1);
    }

    //print time
	nHours = (int)(time/ 3600);
	time -= 3600 * nHours;
	nMinutes = (int)(time / 60);
	time -= 60 * nMinutes;
	nSeconds = (int)(time % 60);

	if (precision != 0)
	{
	    format[17] = 48+precision;
		fprintf(fpOutfile, format, nHours, nMinutes, nSeconds, time_pre);
	}
	else 
	{
        fprintf(fpOutfile, "%02d:%02d:%02d", nHours, nMinutes, nSeconds);
	}

	esg_print_separator (sep);

	return;
}


void
esg_print_interval (int nColumn, int Type, ds_key_t value_1, ds_key_t value_2, ds_key_t value_3, int l_precision, int f_precision, int sep)
{
    ds_key_t nHours, nMinutes, nSeconds;
    char format[64] = "%lld.%06lld";

    switch(Type)
    {
        case CUS_INT_SECOND:
        {    if (f_precision != 0)
            {
                format[7] = 48 + f_precision;
                fprintf(fpOutfile, format , value_1, value_2);
            }
            else
            {
                fprintf(fpOutfile, "%lld", value_1);
            }
            break;
        }    
        
        case CUS_INT_YM:
        {

            fprintf(fpOutfile, "%lld-%02lld", value_1, value_2);
            break;
        }    
        
		case CUS_INT_HM:
		case CUS_INT_DH:
        {
            fprintf(fpOutfile, "%lld:%02lld", value_1, value_2);
            break;
        }    
		case CUS_INT_MS:
        {
            strcpy(format, "%lld:%02lld.%06lld");
            nMinutes = value_1 / 60;
            nSeconds = value_1 % 60;    

            if (nSeconds < 60 && nSeconds >= 0)
            {
                if (f_precision != 0)
                {
                    format[14] = 48 + f_precision;
                    fprintf(fpOutfile, format , nMinutes, nSeconds, value_2);
                }
                else
                {
                    fprintf(fpOutfile, "%lld:%02lld" , nMinutes, nSeconds);
                }
            }
            else
            {
                fprintf(stderr,"ERROR:seconds cannot overhead 60s!");
                exit(-1);
            }
            break;
        }    
        case CUS_INT_DS:
        {
            strcpy(format, "%lld:%02lld:%02lld:%02lld.%06lld");
            nHours = value_2/ 3600;
        	value_2 -= 3600 * nHours;
        	nMinutes = value_2 / 60;
        	value_2 -= 60 * nMinutes;
        	nSeconds = value_2 % 60;

            if (nHours >= 24 || nHours < 0 || nMinutes >= 60 || nMinutes <0 || nSeconds >= 60 || nSeconds <0)
            {
                fprintf(stderr,"ERROR: The time format %02lld:%02lld:%02lld is not valid", nHours, nMinutes, nSeconds);
                exit(-1);
            }
            
            if (f_precision != 0)
        	{
        	    format[28] = 48 + f_precision;
        		fprintf(fpOutfile, format, value_1, nHours, nMinutes, nSeconds, value_3);
        	}
        	else 
        	{
                fprintf(fpOutfile, "%lld:%02lld:%02lld:%02lld", value_1, nHours, nMinutes, nSeconds);
        	}
            break;
        }
        default:
            exit(-1);
            break;
            
            }

    esg_print_separator (sep);
	   
	return 0;
}

void
esg_print_decimal (int nColumn, decimal_t * val, int sep)
{
	int i;
	double dTemp;

    if (NULL != val)
    {
	    dTemp = val->number;

	    for (i=0; i < val->scale; i++)
	    	dTemp /= 10.0;

	    if (fprintf(fpOutfile, "%.*f", val->scale, dTemp) < 0)
	    {
	    	fprintf(stderr, "ERROR: Failed to write output for column %d\n", nColumn);
	    	exit(-1);
	    }
    }

	esg_print_separator (sep);
	
	return;
}

void
esg_print_key (int nColumn, ds_key_t val, int sep)
{
	if (val != (ds_key_t) -1) /* -1 is a special value, indicating NULL */
	{
		if (fprintf (fpOutfile, HUGE_FORMAT, val) < 0)
		{
			fprintf(stderr, "ERROR: Failed to write output for column %d\n", nColumn);
			exit(-1);
		}
	}

	esg_print_separator (sep);
	
	return;
}

void
esg_print_id (int nColumn, ds_key_t val, int sep)
{
   char szID[RS_BKEY + 1];
   
   if (val != (ds_key_t) -1) /* -1 is a special value, indicating NULL */
   {
        mk_bkey(szID, val, 0);
#ifdef STR_QUOTES
        if ((fwrite ("\"", 1, 1, fpOutfile) < 1) ||
            (fwrite (szID, 1, RS_BKEY, fpOutfile) < RS_BKEY) ||
			(fwrite ("\"", 1, 1, fpOutfile) < 1))
#else
            if (fwrite (szID, 1, RS_BKEY, fpOutfile) < RS_BKEY)
#endif
            {
               fprintf(stderr, "ERROR: Failed to write output for column %d\n", nColumn);
               exit(-1);
            }
  }

  esg_print_separator (sep);
   
   return;
}

void
esg_print_boolean (int nColumn, int val, int sep)
{

#ifdef STR_QUOTES
		if (fwrite ((val?"\"TRUE\"":"\"FALSE\""), 1, 3, fpOutfile) != 3)
#else
		if (fwrite ( ((val)?"TRUE":"FALSE"), 1, 1, fpOutfile) != 1)
#endif
		{
			fprintf(stderr, "ERROR: Failed to write output for column %d\n", nColumn);
			exit(-1);
		}

	esg_print_separator (sep);

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
esg_print_start (cus_table_t *table)
{
   int res = 0;
   char path[256];
   //tdef *pTdef = getSimpleTdefsByNumber(tbl);


   current_table = 1;

   if (is_set ("FILTER"))
	   fpOutfile = stdout;
   else
   {
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
		   if ((access (path, F_OK) != -1) && !is_set ("FORCE"))
		   {
			   fprintf (stderr,
				   "ERROR: %s exists. Either remove it or use the FORCE option to overwrite it.\n",
				   path);
			   exit (-1);
		   }
#ifdef WIN32
		   table->outfile = fopen (path, "wt");
#else
		   table->outfile = fopen (path, "w");
#endif
	   }
   }
   
   fpOutfile = table->outfile;
   res = (fpOutfile != NULL);

   if (!res)                    /* open failed! */
     {
        INTERNAL ("Failed to open output file!");
	exit(0);
     }
#ifdef WIN32
   else if (setvbuf (fpOutfile, NULL, _IOFBF, 32767))
     {
        INTERNAL ("setvbuf() FAILED");
     }
#endif

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
esg_print_end (cus_table_t *table)
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
      fwrite(term, 1, add_term, fpOutfile);
   fprintf (fpOutfile, "\n");
   fflush(fpOutfile);

   return (res);
}

/*
* Routine: openDeleteFile(void)
* Purpose: open the output file for the delete keys for a given table
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
esg_openDeleteFile(int bOpen)
{
   int res = 0;
   char path[256];
   
   if (!bOpen)
      fclose(fpDeleteFile);
   else
   {
      sprintf (path, "%s%c%s%d%s",
         get_str ("DIR"),
         PATH_SEP, arDeleteFiles[bOpen], get_int("UPDATE"), get_str("SUFFIX"));
      if ((access (path, F_OK) != -1) && !is_set ("FORCE"))
      {
         fprintf (stderr,
            "ERROR: %s exists. Either remove it or use the FORCE option to overwrite it.\n",
            path);
         exit (-1);
      }
#ifdef WIN32
      fpDeleteFile = fopen (path, "wt");
#else
      fpDeleteFile = fopen (path, "w");
#endif
      
      res = (fpDeleteFile != NULL);
      
      if (!res)                    /* open failed! */
      {
         INTERNAL ("Failed to open output file!");
      }
#ifdef WIN32
      else if (setvbuf (fpDeleteFile, NULL, _IOFBF, 32767))
      {
         INTERNAL ("setvbuf() FAILED");
      }
#endif
   }
   
   return (0);
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
esg_print_string (char *szMessage, ds_key_t val)
{
	if (fprintf (fpOutfile, szMessage, val) < 0)
	{
		fprintf(stderr, "ERROR: Failed to write string\n");
		exit(-1);
	}

	return;
}


void
esg_print_null(int nColumn, int sep)
{
    if (is_set ("DISPLAY_NULL"))
    {
        fwrite("NULL", 1, 4, fpOutfile);
    }

	esg_print_separator (sep);
}


