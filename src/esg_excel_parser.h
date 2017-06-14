

#ifndef ESG_EXCEL_PARSER_H
#define ESG_EXCEL_PARSER_H


enum {
	COL_ID = 0,
	COL_NAME = 1,
	COL_TYPE,
	COL_TYPE_LEN,
	COL_TYPE_PRECISION,
	COL_TYPE_SCALE,
	COL_PK,
	COL_NULLABLE,
	COL_TYPE_MIN,
	COL_TYPE_MAX,
	COL_SEQ_START,
	COL_CONTENT,
	COL_NOTE,
	COL_END

};



extern int esg_excel_init(char * file_path);
extern void esg_excel_destroy();
extern int esg_excel_parsefile(cus_table_t * table);



#ifdef ESG_DEBUG
#define esg_debug_printf(format, x)  fprintf(stdout, format, x)
#else
#define esg_debug_printf(format, x)   
#endif

#endif
