

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


int esg_excel_get_ddl(cus_table_t * table)
{
#define BUFFER_SIZE 100000
    int ret = 0;
    int idx;
    cus_col_t * col;
    char buffer[BUFFER_SIZE];
    char * comma = " ";

    sprintf(buffer, "\n\n--Trafodion DDL \nCREATE TABLE %s (\n", table->tal_name);
    for (idx = 1, col = table->cols; idx < table->col_num + 1; idx++, col = col+1)
	{
		switch(col->type)
        {
            case CUS_INT:
                sprintf(buffer, "%s %s %-30s INT\n", buffer, comma, col->col_name);
                break;

            case CUS_BIG_INT:
				sprintf(buffer, "%s %s %-30s LARGEINT\n", buffer, comma, col->col_name);
				break;

            case CUS_SEQ:
                if (col->base_type == CUS_INT)
                    sprintf(buffer, "%s %s %-30s INT\n", buffer, comma, col->col_name);
                else if (col->base_type == CUS_BIG_INT)
                    sprintf(buffer, "%s %s %-30s LARGEINT\n", buffer, comma, col->col_name);
                break;

            case CUS_CHAR:
            case CUS_UNIQ_CHAR:
                sprintf(buffer, "%s %s %-30s VARCHAR(%d)\n", buffer, comma, col->col_name, col->length);
                break;

            case CUS_DECIMAL:
                sprintf(buffer, "%s %s %-30s DECIMAL(%d, %d)\n", 
                                  buffer, comma, col->col_name, col->precision + col->scale, col->scale);
                break;

            case CUS_DATE:
                sprintf(buffer, "%s %s %-30s DATE\n", 
                                  buffer, comma, col->col_name);
                break;

            case CUS_TIME:
                sprintf(buffer, "%s %s %-30s TIME(%d)\n",
                                  buffer, comma, col->col_name, col->precision);
                break;

            case CUS_TIMESTAMP:
                sprintf(buffer, "%s %s %-30s TIMESTAMP(%d)\n",
                                  buffer, comma, col->col_name, col->precision);
                break;

            case CUS_INT_YEAR:
                sprintf(buffer, "%s %s %-30s INTERVAL YEAR(%d)\n", 
                                  buffer, comma, col->col_name, col->precision);
                break;

	        case CUS_INT_MONTH:
                sprintf(buffer, "%s %s %-30s INTERVAL MONTH(%d)\n", 
                                  buffer, comma, col->col_name, col->precision);
                break;

            case CUS_INT_DAY:
                sprintf(buffer, "%s %s %-30s INTERVAL DAY(%d)\n", 
                                  buffer, comma, col->col_name, col->precision);
                break;

            case CUS_INT_HOUR:
                sprintf(buffer, "%s %s %-30s INTERVAL HOUR(%d)\n", 
                                  buffer, comma, col->col_name, col->precision);
                break;

            case CUS_INT_MINUTE:
                sprintf(buffer, "%s %s %-30s INTERVAL MINUTE(%d)\n", 
                                  buffer, comma, col->col_name, col->precision);
                break;

            case CUS_INT_SECOND:
                sprintf(buffer, "%s %s %-30s INTERVAL SECOND(%d, %d)\n", 
                                  buffer, comma, col->col_name, col->precision, col->scale);
                break;

            case CUS_INT_YM:
                sprintf(buffer, "%s %s %-30s INTERVAL YEAR(%d) TO MONTH\n", 
                                  buffer, comma, col->col_name, col->precision);
                break;

            case CUS_INT_DH:
                sprintf(buffer, "%s %s %-30s INTERVAL DAY(%d) TO HOUR\n", 
                                  buffer, comma, col->col_name, col->precision);
                break;

            case CUS_INT_HM:
                sprintf(buffer, "%s %s %-30s INTERVAL HOUR(%d) TO MINUTE\n", 
                                  buffer, comma, col->col_name, col->precision);
                break;

            case CUS_INT_MS:
                sprintf(buffer, "%s %s %-30s INTERVAL MINUTE(%d) TO SECOND(%d)\n", 
                                  buffer, comma, col->col_name, col->precision, col->scale);
                break;

            case CUS_INT_DS:
                sprintf(buffer, "%s %s %-30s INTERVAL DAY(%d) TO SECOND(%d)\n", 
                                  buffer, comma, col->col_name, col->precision, col->scale);
                break;

            default:
                break;
        }
        comma = ",";
	}
    sprintf(buffer, "%s);\n", buffer);

    fprintf(stdout, buffer);
    
    return ret;
}


int esg_excel_get_hive_ddl(cus_table_t * table)
{
#define BUFFER_SIZE 100000
    int ret = 0;
    int idx;
    cus_col_t * col;
    char buffer[BUFFER_SIZE];
    char * comma = " ";

    sprintf(buffer, "\n\n--Hive DDL \nCREATE TABLE %s (\n", table->tal_name);
    for (idx = 1, col = table->cols; idx < table->col_num + 1; idx++, col = col+1)
	{
		switch(col->type)
        {
            case CUS_INT:
                sprintf(buffer, "%s %s %-30s INT\n", buffer, comma, col->col_name);
                break;

            case CUS_BIG_INT:
				sprintf(buffer, "%s %s %-30s BIGINT\n", buffer, comma, col->col_name);
				break;

            case CUS_SEQ:
                if (col->base_type == CUS_INT)
                    sprintf(buffer, "%s %s %-30s INT\n", buffer, comma, col->col_name);
                else if (col->base_type == CUS_BIG_INT)
                    sprintf(buffer, "%s %s %-30s BIGINT\n", buffer, comma, col->col_name);
                break;

            case CUS_CHAR:
            case CUS_UNIQ_CHAR:
                sprintf(buffer, "%s %s %-30s VARCHAR(%d)\n", buffer, comma, col->col_name, col->length);
                break;

            case CUS_DECIMAL:
                sprintf(buffer, "%s %s %-30s DECIMAL(%d, %d)\n", 
                                  buffer, comma, col->col_name, col->precision + col->scale, col->scale);
                break;

            case CUS_DATE:
                sprintf(buffer, "%s %s %-30s DATE\n", 
                                  buffer, comma, col->col_name);
                break;

            case CUS_TIME:
                sprintf(buffer, "%s %s %-30s VARCHAR(20)\n",
                                  buffer, comma, col->col_name);
                break;

            case CUS_TIMESTAMP:
                sprintf(buffer, "%s %s %-30s VARCHAR(30)\n",
                                  buffer, comma, col->col_name);
                break;

            case CUS_INT_YEAR:
	        case CUS_INT_MONTH:
            case CUS_INT_DAY:
            case CUS_INT_HOUR:
            case CUS_INT_MINUTE:
            case CUS_INT_SECOND:
            case CUS_INT_YM:
            case CUS_INT_DH:
            case CUS_INT_HM:
            case CUS_INT_MS:
            case CUS_INT_DS:
                sprintf(buffer, "%s %s %-30s VARCHAR(30)\n", 
                                  buffer, comma, col->col_name);
                break;

            default:
                break;
        }
        comma = ",";
	}
    sprintf(buffer, "%s)\nROW FORMAT DELIMITED\n  FIELDS TERMINATED BY '|';\n", buffer);

    fprintf(stdout, buffer);
    
    return ret;
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
	static char *type_name[] = {"int", "varchar", "varchar_uniq", "date", "decimal", "time", "timestamp", \
                                "interval year", "interval month", "interval day", "interval hour", "interval minute","interval second", \
                                "interval year to month", "interval day to hour", "interval hour to minute", "interval minute to second", "interval day to second", "bigint", "content", NULL};//todo
	static int cus_type[] =    {CUS_INT, CUS_CHAR, CUS_UNIQ_CHAR, CUS_DATE,  CUS_DECIMAL,  CUS_TIME, CUS_TIMESTAMP, \
                                CUS_INT_YEAR, CUS_INT_MONTH, CUS_INT_DAY, CUS_INT_HOUR, CUS_INT_MINUTE, CUS_INT_SECOND, \
                                CUS_INT_YM,CUS_INT_DH,CUS_INT_HM,CUS_INT_MS,CUS_INT_DS, CUS_BIG_INT, CUS_CONTENT, -1};

	char * tmp;
	int idx;

	int ret = CUS_UNKNOWN;

	if (NULL == str)
		return ret;

	for (idx = 0, tmp = type_name[idx]; tmp != NULL; idx++, tmp = type_name[idx])
	{
		if (0 == strcasecmp(tmp, str))
		{

			ret = cus_type[idx];
			break;
		}
	}

	return ret;
}

void esg_get_decimal_precision_scale(char * str, int * precision, int * scale)
{
    char * d_pt = NULL;

	char valbuf[64];
	strcpy(valbuf, str);

    if ((d_pt = strchr(valbuf, '.')) == NULL)
    {
        *precision = strlen(valbuf);
        *scale = 0;
    }
    else
    {
        *d_pt = '\0';
        d_pt += 1;
        
        *precision = strlen(valbuf);
        *scale = strlen(d_pt);
    }

    return ;
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

int esg_excel_type_check(cus_col_t * col, int col_num)
{
    switch(col->type)
    {
        case CUS_INT:
        case CUS_BIG_INT:
            //do nothing
            break;

        case CUS_SEQ:
            if (NULL == col->seq)
            {
                fprintf(stderr, "esg_excel_type_check() : Type sequence has no sequence start value on excel record %d.\n", col_num);
				exit(-41);
            }
            break;

        case CUS_CHAR:
        case CUS_UNIQ_CHAR:
            if (col->length < 1)
            {
                fprintf(stderr, "esg_excel_type_check() : Type VARCHAR has no size value on excel record %d.\n", col_num);
				exit(-42);
            }
            else if (col->length > CUS_VARCHAR_MAX_LEN)
            {
                fprintf(stderr, "esg_excel_type_check() : Size of Type VARCHAR exceeds max size %d on excel record %d.\n", CUS_VARCHAR_MAX_LEN, col_num);
				exit(-43);
            }
            break;

        case CUS_DECIMAL:
            if (col->precision <= 0 || col->precision < col->scale)
            {
                fprintf(stderr, "esg_excel_type_check() : Invalid precision for type DECIMAL on excel record %d.\n", col_num);
				exit(-44);
            }
            break;

        case CUS_TIME:
            if (col->precision < 0 || col->precision > 6)
            {
                fprintf(stderr, "esg_excel_type_check() : Invalid precision for type TIME on excel record %d.\n", col_num);
				exit(-45);
            }
            break;

        case CUS_TIMESTAMP:
            if (col->precision < 0 || col->precision > 6)
            {
                fprintf(stderr, "esg_excel_type_check() : Invalid precision for type TIMESTAMP on excel record %d.\n", col_num);
				exit(-46);
            }
            break;


        case CUS_INT_SECOND:
        case CUS_INT_MS:
        case CUS_INT_DS:
            if (col->scale < 0 || col->scale > 6)
            {
                fprintf(stderr, "esg_excel_type_check() : Invalid fractional-precision for type INTERVAL on excel record %d.\n", col_num);
				exit(-47);
            }
            //break; //no break here

        case CUS_INT_YEAR:
        case CUS_INT_MONTH:
        case CUS_INT_DAY:
        case CUS_INT_HOUR:
        case CUS_INT_MINUTE:
        case CUS_INT_YM:
        case CUS_INT_MD:
        case CUS_INT_DH:
        case CUS_INT_HM:
            if (col->precision < 2 || col->precision > 18)
            {
                fprintf(stderr, "esg_excel_type_check() : Invalid leading-precision for type INTERVAL on excel record %d.\n", col_num);
				exit(-48);
            }
            break;

		case CUS_CONTENT:
			if (col->content_num < 1 || col->content_idx == NULL || col->contents == NULL)
			{
				fprintf(stderr, "esg_excel_type_check() : Invalid contents for type CONTENT on excel record %d.\n", col_num);
				exit(-49);
			}
			break;

        default:
            break;
    }

    return 0;
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

                    // default precision of interval type is 2
                    if (col->type == CUS_INT_DAY || col->type == CUS_INT_DH || col->type == CUS_INT_DS || col->type == CUS_INT_HM || col->type == CUS_INT_HOUR ||
                         col->type == CUS_INT_MD || col->type == CUS_INT_MINUTE || col->type == CUS_INT_MONTH || col->type == CUS_INT_MS || col->type == CUS_INT_SECOND || 
                         col->type == CUS_INT_YEAR || col->type == CUS_INT_YM)
                    {
                        if (col->precision <= 2)
                            col->precision = 2;
                        if (col->precision > 18)
                            col->precision = 18;
                    }

					esg_debug_printf("DEBUG: get col type precision %d\n", col->precision);
				}
				break;

			case COL_TYPE_SCALE:
				if (CUS_DECIMAL == col->type || CUS_INT_MS == col->type || CUS_INT_DS == col->type || CUS_INT_SECOND == col->type)
				{
                    col->scale = excel_format_get_int(sheet, col_num, COL_TYPE_SCALE);
                    if (col->scale > 6)
                        col->scale = 6;
                    else if (col->scale < 0)
                        col->scale = 0;

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
				if (CUS_INT == col->type || CUS_DATE == col->type || CUS_DECIMAL == col->type || CUS_TIME == col->type || CUS_TIMESTAMP == col->type || CUS_BIG_INT == col->type)
				{
					pstr = excel_format_get_string(sheet, col_num, COL_TYPE_MIN);
					if (NULL != pstr)
					{
						col->min[CUS_NUM_LEN - 1] = '\0';
						strncpy(col->min, pstr, sizeof(col->min) - 1);
						esg_debug_printf("DEBUG: get min str %s\n", col->min);
                        if (CUS_TIME == col->type || CUS_TIMESTAMP == col->type)
                        {
                            col->precision = esg_get_precision_time(col->min);

                        }
                        else if (CUS_DECIMAL == col->type)
                        {
                            esg_get_decimal_precision_scale(col->min, &col->precision, &col->scale);
                        }
					}
					else
					{
						col->min[0] = '\0';
					}
				}
				break;

			case COL_TYPE_MAX:
				if (CUS_INT == col->type || CUS_DATE == col->type || CUS_DECIMAL == col->type || CUS_TIME == col->type || CUS_TIMESTAMP == col->type || CUS_BIG_INT == col->type)
				{
					pstr = excel_format_get_string(sheet, col_num, COL_TYPE_MAX);
					if (NULL != pstr)
					{
						col->max[CUS_NUM_LEN - 1] = '\0';
						strncpy(col->max, pstr, sizeof(col->max) - 1);
						esg_debug_printf("DEBUG: get max str %s\n", col->max);

                        if (CUS_TIME == col->type || CUS_TIMESTAMP == col->type)
                        {
                            int v;
                            v = esg_get_precision_time(col->max);
                            col->precision = col->precision < v ? v:col->precision;

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
                        else if (CUS_DECIMAL == col->type)
                        {
                            int tmp_precision = 0;
                            int tmp_scale = 0;

                            esg_get_decimal_precision_scale(col->max, &tmp_precision, &tmp_scale);
                            if (strlen(col->min) > 0)
                            {
                                tmp_precision = col->precision > tmp_precision ? col->precision : tmp_precision;
                                tmp_scale = col->scale > tmp_scale ? col->scale : tmp_scale;
                            }

                            col->precision = tmp_precision;
                            col->scale = tmp_scale;
                        }

					}
					else
					{
						col->max[0] = '\0';
					}
				}
				break;

			case COL_SEQ_START:
				if (col->type == CUS_INT || col->type == CUS_BIG_INT)
				{
					pstr = excel_format_get_string(sheet, col_num, COL_SEQ_START);
					if (NULL != pstr)
					{
						col->seq = malloc(sizeof (*(col->seq)));
						*(col->seq) = atoll(pstr);
                        col->base_type = col->type;
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
				if (col->type == CUS_CONTENT)
				{
					pstr = excel_format_get_string(sheet, col_num, COL_CONTENT);
					if (NULL != pstr)
					{
						char *head;
						char *space;
						char *current;
						int comma_num = 0;
						int length;
						int current_size = 0;
						int space_size = 0;
						int dest_length = 0;

						//get total number of commas
						for (current = pstr; *current != '\0'; current++)
						{
							if (*current == ',')
							{
								comma_num++;
							}
						}

						//malloc contents space
						length = strlen(pstr);
						col->content_idx = (int*) malloc(sizeof(int) * (comma_num+1));
						memset(col->content_idx, 0, sizeof(int) * (comma_num+1) );
						
						col->contents = (char*)malloc(length + 100);
						memset(col->contents, 0, length + 100);

						col->content_num = 0;
						for (current = pstr; *current != '\0'; current++)
						{
							switch (*current)
							{
								case ',':

									if (current_size > 0)
									{
										col->content_idx[col->content_num] = dest_length;
										col->content_num++;
										strncpy(col->contents + dest_length, head, current_size);
										col->contents[dest_length + current_size] = '\0';
										dest_length += current_size + 1;
									}

									//reset
									head = NULL;
									current_size = 0;
									space_size = 0;
									break;

								case ' ':

									//ignore spaces before and after a content
									if (current_size != 0)
									{
										space_size++;
									}

									break;

								default:
									if (current_size == 0)
										head = current;
									
									if (space_size > 0)
									{
										current_size += space_size;
										space_size = 0;
									}
									current_size++;
									break;
							}

						}

					}
				}
                break;

			case COL_NOTE:
				break;

			default:
				break;

		}
        
	}
    
    //col check
    esg_excel_type_check(col, col_num);

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
 
