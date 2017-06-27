"""

   provide some common shell utilities

"""

__all__ = ( 'Shell', )


import sys
import pexpect


class Shell(object):
    """ 
       The base class used to interact with some shell like program
    """

    EOF = pexpect.EOF
    TIMEOUT = pexpect.TIMEOUT

    def __init__(self, cmdstr, promptstr, exitcmd, timeout):
        if __debug__: 
            print "Enter Shell"

        self.child = pexpect.spawn(cmdstr, timeout=timeout)
        self.child.maxread = 10000

        if __debug__:
            self.child.logfile_send = sys.stdout

        self.__prompt = promptstr
        self.__exitcmd = exitcmd

        self.child.expect([self.__prompt])


    def execute(self, cmdstr):
        """ execute a command and return the result """
        try:
            self.child.send('{0}\n'.format(cmdstr))
            self.child.expect([self.__prompt])
        except Shell.TIMEOUT:
            return "Error: Timeout"

        return self.child.before.split("\r\n")


    def close(self, exitcmd=None):
        """ close the connection to the shell """
        if exitcmd is None:
            exitcmd = self.__exitcmd

        self.child.send('{0}\n'.format(exitcmd))
        self.child.expect([self.EOF])
        self.child.close()


if __name__ == '__main__':
   trafci = Shell("hive", r'hive>', 'exit;', timeout=60)
   print trafci.execute("select count(*) from item;")
