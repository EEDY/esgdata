
cus_table_t tal3; //extern



int initCol(cus_col_t *col, char *name, int type, int len, int precision, int scale, char *min, char *max, long long seq)
{
	strcpy(col->col_name, name);
    col->type = type;
    col->length = len;
	col->precision = precision;
    col->scale = scale;
	if (NULL != min)
	{
		strcpy(col->min, min);
	}

	if (NULL != max)
	{
		strcpy(col->max, max);
	}
	col->seq = seq;

}


void printCol(cus_col_t *col)
{
	
	switch(col->type)
	{
		case CUS_INT:
		    printINT();
		    break;
		
		
	}
	
}


void genData(cus_table_t *tab)
{
	for (n = 0; n < tab.row_num; n++)
	{
		
	}
	for (i = 0; i < tab.col_num; i++)
	{
		printCol()
		printTerm()
		
	}
	
	
}


void genStream(cus_table_t *tal)
{
	
	
	
}



#ifdef TEST
cus_table_t* genTable()
{
	cus_table_t *tal = malloc(sizeof(cus_table_t)); //heap
	cus_table_t tal2; //stack, error
	tal3; //global
	
	
	tal->cols = malloc(tal->col_num * sizeof(cus_col_t));
	memset(tal->cols, 0, len);
	
	initCol(tab->cols, "AA", CUS_INT, 0, 0, 0, "1", "1000", 0);
	initCol(tab->cols + 1, "BB", CUS_INT, 0, 0, 0, NULL, NULL, 123);
	
}
#endif