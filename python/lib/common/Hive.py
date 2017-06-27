__all__ = ('HiveShell',)

import ShellUtil

HIVE_SHELL='hive'

class HiveShell(ShellUtil.Shell):
  def __init__(self, cmdstr=HIVE_SHELL, promptstr=r'hive>'
                   , exitcmd='exit', timeout=120):
    if __debug__:
      print "Enter Hive Shell"

    super(HiveShell, self).__init__(cmdstr, promptstr, exitcmd, timeout)

  def run_cmd(self, cmd):
    """ Run a given cmd in hive shell """
    if len(cmd) <= 0:
      print ("No cmd specified to run")
      return None
    log, data = list(), list()
    status, duration = None, None

    if not cmd.strip().endswith(";"):
      cmd += ";"
    out = self.execute(cmd)   
    
    for line in out:
      data.append(line)
      if (line == "OK"):
        status = line
      elif(line.startswith("Time taken")):
        duration = line[12:line.index("seconds")]
        break

    if status == None:
      print out[1:len(out)]

    return status, data, duration 

if __name__ == '__main__':
  hive=HiveShell()
  hive.run_cmd('')
  hive.run_cmd('show tables;')
  hive.run_cmd('select count(*) from dimdate;')
  hive.run_cmd('select count(*) from t')
  hive.run_cmd('select * from t')
  hive.run_cmd('insert into t values(3),(4),(5)')
  hive.run_cmd('delete from t where id = 3')



