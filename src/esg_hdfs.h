
#ifndef ESG_HDFS_H
#define ESG_HDFS_H


void  esg_hdfs_close(cus_table_t *table);
int esg_hdfs_separator (int sep);
void esg_hdfs_integer (int nColumn, int val, int sep);
void esg_hdfs_varchar (int nColumn, char *val, int sep);
void esg_hdfs_char (int nColumn, char val, int sep);
void esg_hdfs_date (int nColumn, date_t *val, int sep);
void esg_hdfs_time (int nColumn, ds_key_t val, int sep);
void esg_hdfs_timestamp (int nColumn, date_t *date, int precision, ds_key_t time, ds_key_t time_pre, int sep);
void esg_hdfs_decimal (int nColumn, decimal_t * val, int sep);
void esg_hdfs_key (int nColumn, ds_key_t val, int sep);
void esg_hdfs_id (int nColumn, ds_key_t val, int sep);
void esg_hdfs_boolean (int nColumn, int val, int sep);
int esg_hdfs_start (cus_table_t *table);
int esg_hdfs_end (cus_table_t *table);
void esg_hdfs_string (char *szMessage, ds_key_t val);
void esg_hdfs_null(int nColumn, int sep);


#endif

