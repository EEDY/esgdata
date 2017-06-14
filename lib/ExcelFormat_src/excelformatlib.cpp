
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "excelformat.h"
#include "excelformatlib.h"


using namespace ExcelFormat;

extern "C" {


static BasicExcel *xls = NULL;


int excel_format_init(char * path)
{
	if (NULL == path)
	{

		return -1;
	}

	xls = new BasicExcel(path);
	if (NULL == xls)
	{
		return -2;
	}


	return 0;
}

int excel_format_destroy()
{
	if (NULL != xls)
	{
		xls->Close();
	}

	return 0;
}


int excel_format_get_sheet_name(int sheet, char *name)
{
	BasicExcelWorksheet* p_sheet = NULL;

	if (NULL == xls)
		return -1;

	p_sheet = xls->GetWorksheet(sheet);
	if (NULL == p_sheet)
	{
		return -2;
	}

	if (false == p_sheet->GetSheetName(name))
	{
		return -3;
	}


	return 0;
}


int excel_format_get_total_rows(int sheet)
{
	BasicExcelWorksheet* p_sheet = NULL;

	if (NULL == xls)
		return -1;

	p_sheet = xls->GetWorksheet(sheet);
	if (NULL == p_sheet)
	{
		return -2;
	}

	return p_sheet->GetTotalRows();
}

int excel_format_get_total_cols(int sheet)
{
	BasicExcelWorksheet* p_sheet = NULL;

	if (NULL == xls)
		return -1;

	p_sheet = xls->GetWorksheet(sheet);
	if (NULL == p_sheet)
	{
		return -2;
	}

	return p_sheet->GetTotalCols();
}

int excel_format_get_sheet_num()
{
	return xls->GetTotalWorkSheets();
}
	
const char * excel_format_get_string(int sheet, int row, int col)
{
	BasicExcelWorksheet* p_sheet = NULL;

	if (NULL == xls)
		return NULL;

	p_sheet = xls->GetWorksheet(sheet);
	if (NULL == p_sheet)
	{
		return NULL;
	}

	BasicExcelCell* cell = p_sheet->Cell(row, col);

	return cell->GetString();
}

int excel_format_get_int(int sheet, int row, int col)
{
	BasicExcelWorksheet* p_sheet = NULL;

	if (NULL == xls)
		return 0;

	p_sheet = xls->GetWorksheet(sheet);
	if (NULL == p_sheet)
	{
		return 0;
	}

	BasicExcelCell* cell = p_sheet->Cell(row, col);

	return cell->GetInteger();
}


double excel_format_get_double(int sheet, int row, int col)
{
	BasicExcelWorksheet* p_sheet = NULL;

	if (NULL == xls)
		return 0;

	p_sheet = xls->GetWorksheet(sheet);
	if (NULL == p_sheet)
	{
		return 0;
	}

	BasicExcelCell* cell = p_sheet->Cell(row, col);

	return cell->GetDouble();
}


}
