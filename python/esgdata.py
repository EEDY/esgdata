
import sys
import logging
import logging.config

from multiprocessing import Process, Queue
from datetime import datetime
import time
import random
import os
import platform
import re
import subprocess
import copy


from os.path import abspath, dirname
__currdir = dirname(abspath(__file__))
sys.path.append("%s/python/lib" % (__currdir))

from common import *
from common.threadpool import *


ESGDATA_HOME = os.environ['HOME'] + "/esgdata-kit/"


def get_cmd(cmd):
    import getpass
    global pre_cmd
    user = getpass.getuser()
    retval = run_linux_cmd("sudo su hdfs -c 'echo'")[0]
    if retval == 0:
        pre_cmd = "sudo su hdfs -c '" + cmd + "'"
    else:
        logger.WARN("the user:%s can't run tpcds script. " % user)
        print("the user:%s can't run tpcds-script. \n" % user)
        sys.exit(1)

logging.config.fileConfig("./python/conf/logging.conf")
logger = logging.getLogger(__name__)


def get_option(usage, version):
    from optparse import OptionParser

    parser = OptionParser(usage=usage, version=version)

    parser.add_option("-n", "--nodes", dest="nodes", default="nodes.conf",
                      help="a file contains all hostnames of available nodes to generate data, default is 'nodes.conf'")
    parser.add_option("-d", "--directory", dest="dirs", default="dirs.conf",
                      help="a file contains directories where generate data locates, default is 'dirs.conf'")
    parser.add_option("-p", "--parallel", dest="parallel", type="int", default=1,
                      help="total generation parallel number")
    parser.add_option("-e", "--excel-ddl", dest="excel", default=None,
                      help="input DDL excel file")
    parser.add_option("-c", "--row-count", dest="rcount", type="int", default=None,
                      help="total Row count for the given table")
    parser.add_option("--delimiter", dest="deli", default='|', 
                      help="set output field separator")
    '''parser.add_option("-H", "--hdfs-dir", dest="hdfs", default=None,
                      help="a file contains destination hdfs directories")'''
    parser.add_option("-G", "--generate", action="store_true", dest="generate", default=False,
                      help="generate data in linux")
    parser.add_option("-P", "--put", dest="put", default=None,
                      help="HDFS directory where to put all generated data to ")
    parser.add_option("-T", "--putthread", dest="putthread", default=2,
                      help="hdfs dfs -put thread per node")
    parser.add_option("-C", "--clean", action="store_true", dest="clean", default=False,
                      help="clean all generated data files")

    options, args = parser.parse_args()

    if not options.clean and not options.generate and options.put is None:
        parser.error("You must specify at least one of options : -G, -P, -C.")
        sys.exit(-1)

    if not options.excel:
        parser.error("You must specify a DDL Excel File by '-e' option.")
        sys.exit(-1)

    if options.generate:
        if not options.rcount:
            parser.error("You must specify a total Row Count by '-c' option when generating data.")
            sys.exit(-1)

    '''if options.hdfs and options.clean is False:
      logger.info("data is generating to HDFS:%s" % (options.hdfs))
    else:
      logger.info("data is generating to DISK:%s" % (options.dirs))'''
      
    if options.deli and len(options.deli) > 1:
        logger.warn("Your delimiter is too long, will use first character of your delimter : " + options.deli)

    return (options, args)


def cluster_scp(src_path, nodes, target_path):
  """ Copy source file or directory specified by src_path to the target_path on all nodes """
  for node in nodes:
    logger.info("Copying " + src_path + " to " + target_path)
    cmd = "mkdir -p " + target_path
    run_linux_cmd(cmd, node)
    cmd = "scp -rp " + src_path + " " + node + ":" + target_path
    run_linux_cmd(cmd)


def push_esgdata_kit(nodes):
  """ Push tpcds_kit to all nodes """
  logger.info("push esgdata-kit to cluster")
  esgdata_kit = " esgdata tpcds.idx " + options.excel + " "
  cluster_scp(esgdata_kit, nodes, ESGDATA_HOME)
  cluster_scp("convert_to_utf8.sh", nodes, ESGDATA_HOME)


def hdfs_mkdir(dim_tables, fact_tables):
  """ create directory in hdfs for all dim_tables and fact_tables """
  global pre_cmd
  combined_tables = copy.deepcopy(dim_tables)
  combined_tables[len(combined_tables):len(combined_tables)] = fact_tables
  for table in combined_tables:
    logger.info("making HDFS directory " + FLATFILE_HDFS_ROOT + "/" + table)
    run_linux_cmd("hdfs dfs -mkdir -p " + FLATFILE_HDFS_ROOT + "/" + table)
    run_linux_cmd("hdfs dfs -chmod -R 757 " + FLATFILE_HDFS_ROOT)
    #run_linux_cmd(pre_cmd+"su hdfs -c 'hdfs dfs -mkdir -p " + FLATFILE_HDFS_ROOT + "/" + table + "'")
    #run_linux_cmd(pre_cmd+"su hdfs -c 'hdfs dfs -chmod -R 757 " + FLATFILE_HDFS_ROOT + "'")

  logger.info("HDFS directories:")
  run_linux_cmd("hdfs dfs -ls " + FLATFILE_HDFS_ROOT)


def delete_hdfs_data(dim_tables, fact_tables):
  """ Delete directory from hdfs for all dim_tables and fact_tables """
  global pre_cmd
  combined_tables = copy.deepcopy(dim_tables)
  combined_tables[len(combined_tables):len(combined_tables)] = fact_tables
  for table in combined_tables:
    logger.info("Removing HDFS directory " + FLATFILE_HDFS_ROOT + "/" + table)
    run_linux_cmd("hdfs dfs -rm -f -skipTrash " + FLATFILE_HDFS_ROOT + "/" + table + "/*")
    #run_linux_cmd(pre_cmd+"su hdfs -c 'hdfs dfs -rm -skipTrash " + FLATFILE_HDFS_ROOT + "/" + table + "/*'")

  logger.info("HDFS directories:")
  run_linux_cmd("hdfs dfs -ls -R " + FLATFILE_HDFS_ROOT)


def delete_data_from_linux(linux_data_dir, nodes, table):
  """ Delete data files from Linux on all nodes which generated by generation function under directory ~/ and suffix with .dat """
  plist = []
  for node in nodes:
    for disk in linux_data_dir:
      proc = Process(target=run_linux_cmd, args=("rm -rf %s/%s" % (disk, table), node, True))
      plist.append(proc)

  for proc in plist:
    proc.start()

  for proc in plist:
    proc.join()


def run_linux_cmd(cmd, node=None, info=False):
  """ Run any linux build-in cmd or executable on local node or specified remote node """
  if cmd is None:
    logger.error("You must specify a command to run")

  if node is not None:
    cmd = "ssh -q -n " + node + " '" + cmd + "'"

  logger.debug("Run external command: " + cmd)
  p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

  output = p.stdout.readlines()

  retval = p.wait()
  if retval != 0:
    logger.error("*** ERROR *** Cmd run into error: " + cmd)
    logger.error("*** ERROR *** Return Value: %d" % retval)
    logger.error("detail error: %s" % ('\n'.join(output)))
  elif info is True:
    logger.info("*** run_linux_cmd *** Cmd executed successfully: " + cmd)

  return (retval, output)


def gen_data_thread(excel, dir, table, node, rcount, child, parallel):
  """ Generate 'DIM' data or part of 'FACT' data for specified table on given node """
  cmd = ESGDATA_HOME + "/esgdata -INPUT " + excel \
                   + " -RCOUNT " + str(rcount) \
                   + " -TERMINATE N " \
                   + " -DIR " + dir + "/" + table\
                   + " -QUIET Y " \
                   + " -RNGSEED 20161111 " \
                   + " -DELIMITER \"" + delimiter + "\"" \
                   + " -DISTRIBUTIONS " + ESGDATA_HOME + "/tpcds.idx "
  '''cmd2_to_utf8 = "bash " + TPCDS_ROOT + "/../convert_to_utf8.sh %s/%s.dat" % (dir, table)'''

  if parallel > 1:
    cmd = cmd + " -CHILD " + str(child) + " -PARALLEL " + str(parallel)
    '''cmd2_to_utf8 = "bash " + TPCDS_ROOT + "/../convert_to_utf8.sh %s/%s_%d_%s.dat" % (diskdir, table, child, DSDGEN_TOTAL_THREADS)
    cmd2_to_utf8 = "bash {0}/../convert_to_utf8.sh {1}/{2}_{4}_{5}.dat ; bash {0}/../convert_to_utf8.sh {1}/{3}_{4}_{5}.dat ;".format(
                                     TPCDS_ROOT, diskdir, table, table.replace("sales", "returns"), child, DSDGEN_TOTAL_THREADS)'''

  fail, output = run_linux_cmd(cmd, node)

  if fail == 1:
    logger.info("*** ERROR *** Generate data cmd failed: " + cmd)
    logger.info("*** ERROR *** Detail Error Info : " + ''.join(output))
  '''else:
    run_linux_cmd(cmd2_to_utf8, node, True)'''

  info = "[Node: " + node + "][" + str(child) + "] generating data"
  return (fail, info)

  logger.info("CMD: %s;; NODE: %s" % (cmd, node))
  return (0, ["debug"])


def gen_data_log(request, result):
  logger.info(result[1])


def gen_data_per_node(para_list):

  '''get max nproc at a time for per node'''
  max_nproc = 100

  requests = makeRequests(gen_data_thread, para_list, gen_data_log)

  pool = ThreadPool(max_nproc)

  '''[pool.putRequest(req) for req in requests]'''
  for req in requests:
      pool.putRequest(req)
      ''' The sleep call below and through out the script are to avoid
       certain issues with Linux to launch many processes and to open
       many SSH connection quickly.
       May be this can be controlled with the pooling mechanism, if we
       pair the max_nproc with Ssh MaxStartups.
       That is, max_nproc may need to be < SshMaxStartups
       A permanent solution will be provided in a later time'''
      time.sleep(0.1)

  pool.wait()


def gen_data(excel_file, dirs, table, nodes, rcount, parallel):

    plist = []
    para_list = []

    thread_extra = parallel % len(nodes)
    thread_per_node = parallel / len(nodes)

    for nid in range(len(nodes)):
        para_list.append([])

    diskid = 0
    ''' generate esgdata parameter per node '''
    for node, nid in zip(nodes, range(len(nodes))):
        for i in range(thread_per_node):
            para_list[nid].append((None, {"excel": excel_file, "dir": dirs[diskid], "table": table, "node": node, "rcount": rcount,
                                          "child": nid * thread_per_node + i + 1, "parallel": parallel}))
            diskid += 1
            diskid %= len(dirs)

    ''' put extra generation threads to first node'''
    enode_id = len(nodes)
    for idx in range(thread_extra):
        para_list[0].append((None, {"excel": excel_file, "dir": dirs[0], "table": table, "node": node, "rcount": rcount,
                                    "child": enode_id * thread_per_node + idx + 1, "parallel": parallel}))

    logger.info("Start to generate data..")

    '''call dsdgen tool per node with thread limit'''
    for nid in range(len(nodes)):
        proc = Process(target=gen_data_per_node, args=(para_list[nid],))
        plist.append(proc)
        proc.start()

    for proc in plist:
        proc.join()

    logger.info("generate data DONE..")


def put_data_to_hdfs(nodes, dirs, hdfs_dir, table):

    '''check whether hdfs permission '''
    ret, output = run_linux_cmd("hdfs dfs -mkdir -p " + hdfs_dir, nodes[-1])
    if 0 != ret:
        logger.error("*** run_linux_cmd *** hdfs -mkdir failed: " + ''.join(output))
        sys.exit(-11)

    ret, output = run_linux_cmd("hdfs dfs -touchz " + hdfs_dir + "/esgdata_test", nodes[-1])
    if 0 == ret:
        run_linux_cmd("hdfs dfs -rm -skipTrash " + hdfs_dir + "/esgdata_test", nodes[-1])
    else:
        logger.error("*** run_linux_cmd *** hdfs -touchz failed: " + ''.join(output))
        sys.exit(-11)

    plist = []
    for node in nodes:
        proc = Process(target=put_data_per_node, args=(node, dirs, hdfs_dir, table))
        plist.append(proc)

    for proc in plist:
        proc.start()

    for proc in plist:
        proc.join()


def put_data_per_node(node, linux_dir, hdfs_dir, table):
    import threading

    disk_num = len(linux_dir)
    put_cmd_temp = "hdfs dfs -put %s/%s/*.dat %s/"

    '''generate put cmds first'''
    clist = []

    diskid = 0
    while diskid < disk_num:
        #logger.info("Trying put data for table %s on node %s:%s" % (table, node, linux_dir[diskid]))
        logger.info("Trying put data from %s:%s/%s" % (node, linux_dir[diskid], table))
        put_cmd = put_cmd_temp % (linux_dir[diskid], table, hdfs_dir)
        #run_linux_cmd(put_cmd, node)
        clist.append(put_cmd)
        diskid += 1

    cmd_count = len(clist)
    if cmd_count < 1:
        return

    #logger.info("DEBUG : %s" % ('\n'.join(clist)))
    #return

    '''execute command list in total options.putthread subprocesses'''
    idx = 0
    finish_count = 0
    plist = []
    while finish_count < cmd_count:
        if len(plist) < options.putthread and idx < cmd_count:

            thread = threading.Thread(target=run_linux_cmd, args=(clist[idx], node, True))
            thread.start()
            plist.append([thread, idx])
            idx += 1

            # Is it possible to avoid the sleep when we are doing dim tables
            # (check for the type value)
            # The sleep call below and through out the script are to avoid
            # certain issues with Linux to launch many processes and to open
            # many SSH connection quickly.
            # May be this can be controlled with the pooling mechanism, if we
            # pair the max_nproc with Ssh MaxStartups.
            # That is, max_nproc may need to be < SshMaxStartups
            # A permanent solution will be provided in a later time
            time.sleep(0.1)

        ''' Clone/Slice the list for looping purposes since we remove from original list '''
        for item in plist[:]:
            thread = item[0]
            cmd_idx = item[1]

            thread.join(0)
            if thread.is_alive() is True:
                continue

            plist.remove(item)
            finish_count += 1


def check_and_put(tables, type="DIM"):

  put_nodes = nodes
  if type == "DIM":
    put_nodes = [nodes[0]]

  plist = []
  for node in put_nodes:
    proc = Process(target=check_and_put_per_node, args=(tables, node, type))
    plist.append(proc)

  for proc in plist:
    proc.start()

  for proc in plist:
    proc.join()


# I feel that this functiomn should be coded with ThreadPooling, otherwise, we
# will overload the machine with many processes in the case of FACT.
# It is true that HDFS put may not take too much time, so the problem is not as critical
# as the generation of data.
# However, remember that we may be doing 100TB Scale Factor, whcih would produce
# lot of HDFS files to add to HDFS.
def check_and_put_per_node(tables, node, type="DIM"):
    import threading

    expect_number_file = 0
    if type == "FACT":
       expect_disk_id = data_dir_num
       expect_number_file = DSDGEN_TOTAL_THREADS
       put_cmd_temp = "hdfs dfs -put %s/%s*.dat %s/%s/"
    else:
       expect_disk_id = 1
       expect_number_file = 1
       put_cmd_temp = "hdfs dfs -put %s/%s.dat %s/%s/"

    '''generate put cmds first'''
    clist = []
    for table in tables:
        logger.info("Checking the number of data files for table " + table)
        num_file = run_linux_cmd("hdfs dfs -ls %s/%s | grep %s | wc -l" % (FLATFILE_HDFS_ROOT, table, table))[1][0]
        if num_file.strip() != str(expect_number_file):
            logger.info("Expecting %d split files for table %s, but only found %s split files" % (expect_number_file, table, num_file.strip()))

            diskid = 0
            while diskid < expect_disk_id:
                logger.info("Trying put data for table %s on node %s:%s" % (table, node, linux_data_dir[diskid]))
                put_cmd = put_cmd_temp % (linux_data_dir[diskid], table, FLATFILE_HDFS_ROOT, table)
                #run_linux_cmd(put_cmd, node)
                clist.append(put_cmd)
                diskid += 1
        else:
            logger.info("table %s have enough split files of %s" % (table, num_file.strip()))

    cmd_count = len(clist)
    if cmd_count < 1:
        return

    '''execute command list in total options.putthread subprocesses'''
    idx = 0
    finish_count = 0
    plist = []
    while finish_count < cmd_count:
        if len(plist) < options.putthread and idx < cmd_count:

            thread = threading.Thread(target=run_linux_cmd, args=(clist[idx], node, True))
            thread.start()
            plist.append([thread, idx])
            idx += 1

            # Is it possible to avoid the sleep when we are doing dim tables
            # (check for the type value)
            # The sleep call below and through out the script are to avoid
            # certain issues with Linux to launch many processes and to open
            # many SSH connection quickly.
            # May be this can be controlled with the pooling mechanism, if we
            # pair the max_nproc with Ssh MaxStartups.
            # That is, max_nproc may need to be < SshMaxStartups
            # A permanent solution will be provided in a later time
            time.sleep(0.1)  #avoid sshd fail, sshd_config MaxStartups

        ''' Clone/Slice the list for looping purposes since we remove from original list '''
        for item in plist[:]:
            thread = item[0]
            cmd_idx = item[1]

            thread.join(0)
            if thread.is_alive() is True:
                continue

            plist.remove(item)
            finish_count += 1


def check_data_dir(data_dir_list, nodes, table):

    '''first check existence of directories'''
    for node in nodes:
        for dir in data_dir_list:
            retcode, output = run_linux_cmd("mkdir -p " + dir + "/" + table, node)

            if retcode != 0:
                return (retcode, output)

    '''second check write permission'''
    for node in nodes:
        for dir in data_dir_list:
            retcode, output = run_linux_cmd("touch -c " + dir + "/" + table + "/test", node)

            if retcode != 0:
                return (retcode, output)

    return (0, [])


def get_first_sheet_name(excel_path):

    ret, output = run_linux_cmd("./esgdata -INPUT " + excel_path + " -GETSHNAME Y")
    if ret != 0:
        logger.error("get first sheet name failed with error %d, error info: %s" % (ret, ''.join(output)))
        sys.exit(-1)

    name = ''.join(output)
    logger.info("got first sheet name from given excel file : " + name)

    return name


def main():
    """ Main function """
    global options, args
    global data_dir_list, data_dir_num, is_hdfs
    global sheet_name
    global delimiter
    options, args = get_option("%prog [Options]", "%prog 1.0")

    """ read data directory """
    '''if options.hdfs:
        data_dir_list = read_file(options.hdfs)
        data_dir_num = len(data_dir_list)
        is_hdfs = True
    else:'''
    
    
    data_dir_list = read_file(options.dirs)
    data_dir_num = len(data_dir_list)
    is_hdfs = False

    logger.info("Data directory list : %s, num: %d" % (';'.join(data_dir_list), data_dir_num))
    if data_dir_list is None or data_dir_num < 1:
        logger.error("No data directory set, exit.")
        sys.exit(-1)

    """ read available nodes """
    global nodes
    nodes = read_file(options.nodes)
    logger.info(nodes)

    '''start gen data'''
    if options.generate:
        sheet_name = get_first_sheet_name(options.excel)

        if options.deli:
            delimiter = options.deli[0]
        else:
            delimiter = '|'
        
        print "DEBUG del is " + delimiter

        '''do mkdir if data directory not existing'''
        ret, output = check_data_dir(data_dir_list, nodes, sheet_name)
        if ret != 0:
            logger.error("*** ERROR *** Data disk directory check failed : " + ''.join(output))
            sys.exit(-1)

        push_esgdata_kit(nodes)
        gen_data(ESGDATA_HOME + os.path.basename(options.excel), data_dir_list, sheet_name, nodes, options.rcount, options.parallel)

    if options.put is not None:
        sheet_name = get_first_sheet_name(options.excel)
        put_data_to_hdfs(nodes, data_dir_list, options.put, sheet_name)

    if options.clean:
        sheet_name = get_first_sheet_name(options.excel)

        delete_data_from_linux(data_dir_list, nodes, sheet_name)


if __name__ == "__main__":

  main()
