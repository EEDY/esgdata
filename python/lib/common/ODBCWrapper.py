__all__ = ('Connection', 'ODBCError')

import os
import pypyodbc


pypyodbc.lowercase = False  # dont convert field name to lowercase
os.environ['AppUnicodeType'] = 'utf16'  # required by trafodion odbc


ODBCError = pypyodbc.Error


class Connection(object):
    """ A wrapper class for an ODBC connection
    """
    def __init__(self, conn_str=None, autocommit=None, timeout=60, readonly=False):
        if not conn_str:
            raise ODBCGenericError('Connection string is empty')

        if __debug__:
            print ('Connect using {0}, autocommit = {1}, timeout={2}'.format(conn_str, autocommit, timeout))

        self.__conn = pypyodbc.connect(conn_str, autocommit=autocommit, timeout=timeout,
                                           readonly=readonly)
        if __debug__:
            print ('Connect finished')

    @staticmethod
    def __is_select(sqlstr):
        return sqlstr.strip().lower().startswith("select")


    def getall(self, sqlstr, params=None):
        """ execute a query, return a list of dictionary
        """
        #print("[DEBUG] execute query: " + sqlstr)
        cursor = self.__conn.cursor()
        try: 
          cursor.execute(sqlstr, params=params)
        except Exception as e:
          raise(e, "message")
        else:
          #When execute 'set' kind of statement without any result return, cursor.description will be None
          if cursor.description is not None:
            field_names = [desc[0] for desc in cursor.description]
          #for i in range(len(field_names)):
          #  if field_names[i].strip() == "(EXPR)":
          #    field_names[i] = "Col"+str(i+1)

            rows = cursor.fetchall()
            return rows

        #return None
          #return [dict(zip(field_names, row) for row in rows)]

    def getone(self, sqlstr, params=None):
        """ execute a query, return the first row selected, or the row affected
        """
        cursor = self.__conn.cursor()
        try:
          cursor.execute(sqlstr, params=params)
        except Exception as e:
          raise(e)
     
        if(self.__is_select(sqlstr)):
          return cursor.fetchone()
        else:
          return cursor.rowcount

    def execute(self, sqlstr, params=None):
        """ execute a sql string, returns the row affected """
        print("[DEBUG] execute query: " + sqlstr)
        cursor = self.__conn.cursor()
        try:
          cursor.execute(sqlstr, params=params).commit()
        except Exception as e:
          raise(e)

        return cursor.rowcount

    def prepare(self, sqlstr):
        ''' prepare a sql string, return a cursor'''
        cursor = self.__conn.cursor()
        #cursor = self.__cursor if self.__cursor is not None else self.__conn.cursor()
        try:
          cursor.prepare(sqlstr)
        except Exception as e:
          raise(e)

if __name__ == '__main__':
    from pypyodbc import *
    conn=Connection("DSN=traf;UID=zz;PWD=zz", autocommit = False)
    cursor=conn.cursor()
    cursor.execute("set schema tpcds;")
    cursor.prepare("select count(*) from item;")
    rows=cursor.execute("select count(*) from item;").fetchall()
    print rows
    cursor.prepare("prepare s from select count(*) from customer;")
  
    pass


