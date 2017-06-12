


extern "C" {
	
int excel_format_init(char * path);
int excel_format_destroy();
	
const char * excel_format_get_string(int sheet, int row, int col);
int excel_format_get_int(int sheet, int row, int col);
double excel_format_get_double(int sheet, int row, int col);
	
}
