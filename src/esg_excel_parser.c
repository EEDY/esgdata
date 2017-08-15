

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "genrand.h"
#include "esg_custom_table.h"

#include "excelformatlib.h"
#include "esg_excel_parser.h"



int esg_excel_init(char * file_path)
{
	int ret = 0;

	ret = excel_format_init(file_path);
	if (0 != ret)
	{
		fprintf (stderr, "Read excel file failed with errno: %d\n", ret);
		exit(ret);
	}

	return ret;
}


void esg_excel_destroy()
{
	excel_format_destroy();
}


int esg_excel_get_sheet_name(char *excel, char *buf)
{
    int ret;

    esg_excel_init(excel);
    
    ret = excel_format_get_sheet_name(0, buf);

    esg_excel_destroy();

    return ret;
}


int esg_excel_read_bool(int sheet, int col_num, int property_id)
{
	int ret = 0;
	const char *pstr;

	pstr = excel_format_get_string(sheet, col_num, property_id);
	if (NULL != pstr && 0 == strcmp("Y", pstr))
	{
		ret = 1;
	}

	return ret;
}


int esg_str_to_col_type(char * str)
{
	static char *type_name[] = {"int",   "varchar", "date",    "numeric",    "time",   "timestamp", \
                                "interval year", "interval month", "interval day", "interval hour", "interval minute","interval second", \
                                "interval year to month", "interval day to hour", "interval hour to minute", "interval minute to second", "interval day to second", NULL};//todo
	static int cus_type[] =    {CUS_INT, CUS_CHAR,  CUS_DATE,  CUS_DECIMAL,  CUS_TIME, CUS_TIMESTAMP, \
                                CUS_INT_YEAR, CUS_INT_MONTH, CUS_INT_DAY, CUS_INT_HOUR, CUS_INT_MINUTE, CUS_INT_SECOND, \
                                CUS_INT_YM,CUS_INT_DH,CUS_INT_HM,CUS_INT_MS,CUS_INT_DS, -1};

	char * tmp;
	int idx;

	int ret = CUS_UNKNOWN;

	if (NULL == str)
		return ret;

	for (idx = 0, tmp = type_name[idx]; tmp != NULL; idx++, tmp = type_name[idx])
	{
		if (0 == strcmp(tmp, str))
		{

			ret = cus_type[idx];
			break;
		}
	}

	return ret;
}



int esg_get_precision_time(char * val)
{
    char *str;
    int count = strlen(val);
    int pre = 0;

    for (str = val; str != '\0'; str++)
    {
        if (*str == '.')
        {
            pre = strlen(str) - 1;
            break;
        }
    }

    return pre;
}

int esg_excel_parse_col(cus_col_t * col, int sheet, int col_num)
{
	const char *pstr = NULL;
	int property_id = 0;

	for (property_id = COL_NAME; property_id < COL_END; property_id++)
	{
		switch(property_id)
		{
			case COL_NAME:
				pstr = excel_format_get_string(sheet, col_num, COL_NAME);
				if (NULL == pstr)
				{
					fprintf(stderr, "esg_excel_parse_col() : get column name failed at %d/%d.\n", col_num, COL_NAME);
					exit(-20);
				}
				col->col_name[CUS_NAME_LEN - 1] = '\0';
				strncpy(col->col_name, pstr, sizeof(col->col_name) - 1);
				esg_debug_printf("DEBUG: get col name %s\n", col->col_name);
				break;

			case COL_TYPE:
				col->type = esg_str_to_col_type(excel_format_get_string(sheet, col_num, COL_TYPE));
				if (CUS_UNKNOWN == col->type)
				{
					fprintf(stderr, "esg_excel_parse_col() : get column type failed at %d/%d.\n", col_num, COL_TYPE);
					exit(-21);
				}
				esg_debug_printf("DEBUG: get col type %d\n", col->type);
				break;

			case COL_TYPE_LEN:
				col->length = excel_format_get_int(sheet, col_num, COL_TYPE_LEN);
				esg_debug_printf("DEBUG: get col type size %d\n", col->length);
				break;

			case COL_TYPE_PRECISION:
				if (col->type != CUS_INT || col->type != CUS_CHAR || col->type != CUS_DATE || col->type != CUS_SEQ || col->type != CUS_UNKNOWN)
				{
					col->precision = excel_format_get_int(sheet, col_num, COL_TYPE_PRECISION);
					esg_debug_printf("DEBUG: get col type precision %d\n", col->precision);
				}
				break;

			case COL_TYPE_SCALE:
				if (CUS_DECIMAL == col->type || CUS_INT_SECOND == col->type || CUS_INT_MS == col->type || CUS_INT_DS == col->type)
				{
					col->scale = excel_format_get_int(sheet, col_num, COL_TYPE_SCALE);
					esg_debug_printf("DEBUG: get col type scale %d\n", col->scale);
				}
				break;

			case COL_PK:
				col->pk = esg_excel_read_bool(sheet, col_num, COL_PK);
				break;

			case COL_NULLABLE:
				col->nullable = esg_excel_read_bool(sheet, col_num, COL_NULLABLE);
				break;

			case COL_TYPE_MIN:
				if (CUS_INT == col->type || CUS_DATE == col->type || CUS_DECIMAL == col->type || CUS_TIME == col->type || CUS_TIMESTAMP == col->type)
				{
					pstr = excel_format_get_string(sheet, col_num, COL_TYPE_MIN);
					if (NULL != pstr)
					{
						col->min[CUS_NUM_LEN - 1] = '\0';
						strncpy(col->min, pstr, sizeof(col->min) - 1);
						esg_debug_printf("DEBUG: get min str %s\n", col->min);
                        if (CUS_TIME == col->type || CUS_TIMESTAMP == col->type || CUS_DECIMAL == col->type)
                        {
                            col->precision = esg_get_precision_time(col->min);

                        }
					}
					else
					{
						col->min[0] = '\0';
					}
				}
				break;

			case COL_TYPE_MAX:
				if (CUS_INT == col->type || CUS_DATE == col->type || CUS_DECIMAL == col->type || CUS_TIME == col->type || CUS_TIMESTAMP == col->type)
				{
					pstr = excel_format_get_string(sheet, col_num, COL_TYPE_MAX);
					if (NULL != pstr)
					{
						col->max[CUS_NUM_LEN - 1] = '\0';
						strncpy(col->max, pstr, sizeof(col->max) - 1);
						esg_debug_printf("DEBUG: get max str %s\n", col->max);

                        if (CUS_TIME == col->type || CUS_TIMESTAMP == col->type || CUS_DECIMAL == col->type)
                        {
                            int v;
                            v = esg_get_precision_time(col->max);
                            col->precision = col->precision < v ? v:col->precision;
                        }

						if (strlen(col->min) > 0)//todo: need to improve for time/timestamp 
						{
							ds_key_t min_val, max_val;

							min_val = atoll(col->min);
							max_val = atoll(col->max);

							if (max_val < min_val)
							{
								fprintf(stderr, "esg_excel_parse_col() : Invalid min(%s) and max(%s) setting.\n", col->min, col->max);
								exit(-22);
							}

						}
					}
					else
					{
						col->max[0] = '\0';
					}
				}
				break;

			case COL_SEQ_START:
				if (col->type == CUS_INT)
				{
					pstr = excel_format_get_string(sheet, col_num, COL_SEQ_START);
					if (NULL != pstr)
					{
						col->seq = malloc(sizeof (*(col->seq)));
						*(col->seq) = atoll(pstr);
						col->type = CUS_SEQ;
						esg_debug_printf("DEBUG: get seq start %lld.\n", col->seq);
					}
					else
					{
						col->seq = NULL;
					}
				}
				break;

			case COL_CONTENT:
			case COL_NOTE:
				//not implelemented
				break;

			default:
				break;

		}


	}


	return 0;

}


int esg_excel_parsefile(cus_table_t * table)
{
	cus_col_t *col;
	int idx = 0;
	int ret = 0;

	memset(table->cols, 0, sizeof(table->cols));

	table->col_num = excel_format_get_total_rows(0) - 1;//only support table ddl in first sheet
	if (CUS_MAX_COLUMNS < table->col_num)
	{
		fprintf(stderr, "esg_excel_parse2col(): Table columns number exceeds max %s.\n", CUS_MAX_COLUMNS);
		exit(-10);
	}

	ret = excel_format_get_sheet_name(0, table->tal_name);
	if (0 != ret)
	{
		fprintf(stderr, "esg_excel_parse2col(): Get sheet name failed with errno %d.\n", ret);
		exit(-11);
	}

	for (idx = 1, col = table->cols; idx < table->col_num + 1; idx++, col = col+1)
	{
		esg_excel_parse_col(col, 0, idx);
	}

	return 0;
}
 