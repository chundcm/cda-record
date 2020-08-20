#coding=utf-8
import sys
import logger
import mysql_version_by_shell
from plugins import Plugin


class MySQLVersionShellPlugin(Plugin):
    """
        Plugin set MySQL version by shell, depends on OS type.
    """
    def __init__(self):
        Plugin.__init__(self)
        self.__client = None
        self.__process = None
        self.__isWinOs = None
        self.__cmd = None

    def isApplicable(self, context):
        self.__client = context.client
        try:
            if self.__client.isWinOs():
                self.__isWinOs = 1
                self.__process = context.application.getProcess('mysqld-nt.exe')
                if not self.__process:
                    self.__process = context.application.getProcess('mysqld.exe')
            else:
                self.__process = context.application.getProcess('mysqld')
            if self.__process:
                return 1
        except:
            logger.errorException(sys.exc_info()[1])

    def process(self, context):
        applicationOsh = context.application.getOsh()
        mysql_version_by_shell.setVersion(applicationOsh, self.__process.executablePath, self.__client)