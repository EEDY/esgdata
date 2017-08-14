
#ifndef _ESG_CUSTOM_TABLE_H_
#define _ESG_CUSTOM_TABLE_H_
#define STREAMS_H


#define CUS_NAME_LEN  64
#define CUS_NUM_LEN   128
#define CUS_PATH_LEN  512
#define CUS_CANDIDATE_LEN 256
#define CUS_MAX_COLUMNS  256

enum cus_types
{
	CUS_SEQ = 10,
	CUS_INT,
	CUS_CHAR,
	CUS_DECIMAL,
	CUS_DATE,
	CUS_TIME,
	CUS_TIMESTAMP,
	CUS_RANDOM,
	CUS_EMAIL,
	CUS_INT_YEAR,
	CUS_INT_MONTH,
	CUS_INT_DAY,
	CUS_INT_HOUR,
	CUS_INT_MINUTE,
	CUS_INT_SECOND,
	CUS_INT_YM, //INTERVAL TYPE FOR YEAR TO MONTH
	CUS_INT_MD, //INTERVAL TYPE FOR MONTH TO DAY, NOT VALID NOW
	CUS_INT_DH, //INTERVAL TYPE FOR DAY TO HOUR
	CUS_INT_HM, //INTERVAL TYPE FOR HOUR TO MINUTE
	CUS_INT_MS, //INTERVAL TYPE FOR MINUTE TO SECOND
	CUS_INT_DS, //INTERVAL TYPE FOR DAY TO SECOND
	
	CUS_UNKNOWN
};


struct CUS_TABLE;
typedef struct CUS_IO_FUNC
{
	int  (*start) (struct CUS_TABLE *table);
	int  (*end) (struct CUS_TABLE *table);
	void (*close)(struct CUS_TABLE *table);
	int  (*out_separator) (int sep);
	void (*out_integer) (int nColumn, int val, int sep);
	void (*out_varchar) (int nColumn, char *val, int sep);
	void (*out_char) (int nColumn, char val, int sep);
	void (*out_date) (int nColumn, date_t *val, int sep);
	void (*out_time) (int nColumn, int precision, ds_key_t val_time, ds_key_t val_pre, int sep);
	void (*out_decimal) (int nColumn, decimal_t * val, int sep);
	void (*out_key) (int nColumn, ds_key_t val, int sep);
	void (*out_id) (int nColumn, ds_key_t val, int sep);
	void (*out_boolean) (int nColumn, int val, int sep);
	void (*out_string) (char *szMessage, ds_key_t val);
	void (*out_null)(int nColumn, int sep);
    void (*out_interval) (int nColumn, int Type, ds_key_t value_1, ds_key_t value_2, ds_key_t value_3, int l_precision, int f_precision, int sep);

} cus_io_func_t;


typedef struct CUS_COLUMN
{
	char col_name[CUS_NAME_LEN];
	int type;
	int length;
	int precision;
	int scale;
	int pk;//bool - [0/1]
	int nullable;//bool
	char min[CUS_NUM_LEN];
	char max[CUS_NUM_LEN];
	long long *seq;
	
} cus_col_t;



typedef struct CUS_TABLE
{
	char tal_name[CUS_NAME_LEN];
	int col_num;
	cus_col_t cols[CUS_MAX_COLUMNS]; // = malloc(col_num * sizeof(cus_col_t))
	char *ddl_excel;
	char data_file[CUS_PATH_LEN];
	FILE *outfile;
	int flags;
	cus_io_func_t io;
} cus_table_t;


extern rng_t Streams[];


extern cus_table_t* esg_gen_table();
extern void esg_gen_stream(cus_table_t *tal);
extern void esg_gen_data(cus_table_t *tab, ds_key_t, ds_key_t);

extern cus_table_t* esg_gen_table();
extern void esg_init_io(cus_table_t *tab);


#endif

