
#ifndef ESG_PRINT_H
#define ESG_PRINT_H

void  esg_print_close(cus_table_t *table);
int esg_print_separator (int sep);
void esg_print_integer (int nColumn, int val, int sep);
void esg_print_varchar (int nColumn, char *val, int sep);
void esg_print_char (int nColumn, char val, int sep);
void esg_print_date (int nColumn, date_t *val, int sep);
void esg_print_time (int nColumn, ds_key_t val, int sep);
void esg_print_decimal (int nColumn, decimal_t * val, int sep);
void esg_print_key (int nColumn, ds_key_t val, int sep);
void esg_print_id (int nColumn, ds_key_t val, int sep);
void esg_print_boolean (int nColumn, int val, int sep);
int esg_print_start (cus_table_t *table);
int esg_print_end (cus_table_t *table);
int esg_openDeleteFile(int bOpen);
void esg_print_string (char *szMessage, ds_key_t val);
void esg_print_null(int nColumn, int sep);

#endif

