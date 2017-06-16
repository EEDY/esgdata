
#ifndef EXCELFORMATLIB_H
#define EXCELFORMATLIB_H


#ifdef __cplusplus
extern "C" {
#endif
	
int excel_format_init(char * path);
int excel_format_destroy();

int excel_format_get_sheet_name(int sheet, char *name);
int excel_format_get_total_rows(int sheet);
int excel_format_get_total_cols(int sheet);

const char * excel_format_get_string(int sheet, int row, int col);
int excel_format_get_int(int sheet, int row, int col);
double excel_format_get_double(int sheet, int row, int col);
	

#ifdef __cplusplus
}
#endif


#endif
