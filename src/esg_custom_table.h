
#ifndef _ESG_CUSTOM_TABLE_H_
#define _ESG_CUSTOM_TABLE_H_
#define STREAMS_H


#define CUS_NAME_LEN  64
#define CUS_NUM_LEN   128

enum cus_types
{
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
	CUS_INT_MD, //INTERVAL TYPE FOR MONTH TO DAY
	CUS_INT_DH, //INTERVAL TYPE FOR DAY TO HOUR
	CUS_INT_HM, //INTERVAL TYPE FOR HOUR TO MINUTE
	CUS_INT_MS //INTERVAL TYPE FOR MINUTE TO SECOND
	
	
};

typedef struct CUS_COLUMN
{
	char col_name[CUS_NAME_LEN];
	long long type;
	long long length;
	long long precision;
	long long scale;
	char min[CUS_NUM_LEN];
	char max[CUS_NUM_LEN];
	long long seq ;
	
} cus_col_t;



typedef struct CUS_TABLE
{
	char tal_name[CUS_NAME_LEN];
	int col_num;
	cus_col_t *cols; // = malloc(col_num * sizeof(cus_col_t))
	
} cus_table_t;



rng_t *Streams = NULL;



#endif

