
#include <stdio.h>
#include <stdlib.h>
#include "excelformatlib.h"
#include "excelformat.h"


#define	FW_NORMAL	400
#define	FW_BOLD		700


using namespace ExcelFormat;


static char * g_path = "test_lib.xls";


static void write_test_sheet(const int row_max, const int col_max)
{
	BasicExcel xls;
	char buffer[16];

	 // create sheet 1 and get the associated BasicExcelWorksheet pointer
	xls.New(1);
	BasicExcelWorksheet* sheet = xls.GetWorksheet(0);

	XLSFormatManager fmt_mgr(xls);


	 // Create a table containing header row and column in bold.

	ExcelFont font_bold;
	font_bold._weight = FW_BOLD; // 700

	CellFormat fmt_bold(fmt_mgr);
	fmt_bold.set_font(font_bold);

	int col, row;

	BasicExcelCell* cell = sheet->Cell(0, 0);
	cell->Set("Row / Column");
	cell->SetFormat(fmt_bold);

	for(col=1; col<=col_max; ++col) {
		cell = sheet->Cell(0, col);

		sprintf(buffer, "Column %d", col);
		cell->Set(buffer);
		cell->SetFormat(fmt_bold);
	}

	for(row=1; row<=row_max; ++row) {
		cell = sheet->Cell(row, 0);

		sprintf(buffer, "Row %d", row);
		cell->Set(buffer);
		cell->SetFormat(fmt_bold);
	}

	for(row=1; row<=row_max - 1; ++row) {
		for(int col=1; col<=col_max; ++col) {
			sprintf(buffer, "%d / %d", row, col);

			sheet->Cell(row, col)->Set(buffer);
		}
	}

	int i_tmp = 1000;
	for(col=1; col<=col_max-3; ++col) {
		i_tmp += 10 * col;

		sheet->Cell(row, col)->Set(i_tmp);
	}

	double d_tmp1 = 123456780000012345;
	double d_tmp2 = 123456.7890123;
	sheet->Cell(row, col+1)->Set(d_tmp1);
	sheet->Cell(row, col+2)->Set(d_tmp2);

	xls.SaveAs(g_path);
	xls.Close();
}


int main()
{

	//generate test xls file
	write_test_sheet(5, 8);


	//run api tests
	if (0 != excel_format_init(g_path))
	{
		cout<<"error init lib";
	}

	cout<<"case01 : get string 0/2 = " << excel_format_get_string(0, 0, 2) << "\n";
	cout<<"case02 : get string 3/0 = " << excel_format_get_string(0, 3, 0) << "\n";
	cout<<"case03 : get int 5/2 = " << excel_format_get_int(0, 5, 2) << "\n";
	cout<<"case04 : get int 5/4 = " << excel_format_get_int(0, 5, 3) << "\n";
	cout<<"case05 : get double 5/7 = " << excel_format_get_double(0, 5, 7) << "\n";
	cout<<"case06 : get double 5/8 = " << excel_format_get_double(0, 5, 8) << "\n";

	excel_format_destroy();
}