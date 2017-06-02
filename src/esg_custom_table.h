
#ifndef _ESG_CUSTOM_TABLE_H_
#define _ESG_CUSTOM_TABLE_H_
#define STREAMS_H


enum cus_types
{
	CUS_INT,
	CUS_CHAR,
	
	
	
};

typedef struct CUS_COLUMN
{
	char col_name[];
	int type;
	
	
} cus_col_t;



typedef struct CUS_TABLE
{
	char tal_name[];
	int col_num;
	cus_col_t *cols; //alloc(col_num * sizeof(cus_col_t))
	
} cus_table_t;



rng_t *Streams = NULL;



#endif

