
cus_table_t tal3; //extern




int initCol(cus_col_t *col, name, type, ....)
{
	
	
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


void genData(cus_table_t *tal)
{
	
	for (i = 0; i < tal.col_num; i++)
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
	
}
#endif