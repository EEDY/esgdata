import logging
import logging.config
import os

logger=logging.getLogger(__name__)

def read_file(file, case=None):
  lines = list()
  try:
    file_h = open(file, 'r')
  except Exception as exp:
    print("SQL query file does not exist: "+ file) 
    return lines 
  
  for line in file_h.readlines():
    line = line.strip()
    if not len(line) or line.startswith('#'):
      continue

    if not case == None:
      if case.upper() == 'UC':
        line = line.upper()
      if case.upper() == 'LC':
        line = line.lower() 

    lines.append(line)
  
  return lines

def load_query (sql_file, search_str=None, replace_str=None):
  '''Load a SQL query file which '''
  logger.info("load query from file "+ sql_file)
  try:
    file_h = open(sql_file, 'r')
  except Exception as exp:
    print("SQL query file does not exist: "+ sql_file) 
  queries = list()
  query = {'ID':'', 'TYPE':'', 'SQL':''}
  for line in file_h.readlines():
    line = line.lstrip()  
    if search_str is not None and replace_str is not None:
      line = line.replace(str(search_str), str(replace_str))
    tmp_line = line.upper().strip()
    if not len(tmp_line) or tmp_line.startswith('#'):
      continue
    elif tmp_line.startswith('--'):
      if tmp_line.find('ID:') >= 0:
        query['ID'] = line.split(':')[1].strip()
      elif tmp_line.find('TYPE:') >= 0:
        query['TYPE'] = line.split(':')[1].strip()
      else:
        continue
    elif tmp_line.endswith(';'):
      query['SQL'] = query['SQL'] + line
      queries.append(query)
      #If there is no ID and Type be specified, then we will reinitialize the 
      #value be the previous statement
      query = {'ID':query['ID'], 'TYPE':query['TYPE'], 'SQL':''}
    else:
      query['SQL'] = query['SQL'] + line

  return queries

#initialize from environment virable
#TPCDS_ROOT=os.environ['TPCDS_ROOT']
#FLATFILE_HDFS_ROOT=os.environ['FLATFILE_HDFS_ROOT']
#FLATFILE_LINUX_ROOT=os.environ['FLATFILE_LINUX_ROOT']
#TPCDS_SCALE_FACTOR=os.environ['TPCDS_SCALE_FACTOR']
#DSDGEN_NODES=int(os.environ['DSDGEN_NODES'])
#DSDGEN_THREADS_PER_NODE=int(os.environ['DSDGEN_THREADS_PER_NODE'])
#TPCDS_DBNAME=os.environ['TPCDS_DBNAME']
##print (type(DSDGEN_NODES), type(DSDGEN_THREADS_PER_NODE))
#DSDGEN_TOTAL_THREADS=DSDGEN_NODES*DSDGEN_THREADS_PER_NODE
#DSDGEN_MAX_THREADS_PER_NODE=int(os.environ['DSDGEN_MAX_THREADS_PER_NODE'])
