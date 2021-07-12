#coding=utf-8
from plugins import Plugin

import re
import logger
import sys

class PostgreSQLVersionShellPlugin(Plugin):
    
    def __init__(self):
        Plugin.__init__(self)
        self.__client = None
        self.__cmd = None
        self.__applicationOsh = None
        
    def isApplicable(self, context):
        self.__client = context.client
        self.__applicationOsh = context.application.getOsh()
        version = self.__applicationOsh.getAttributeValue('application_version_number')
        logger.debug('PostgreSQL version : ' + str(version))
        if version:
            return 0
        else:
            return 1
    
    def __parseVersion(self, output):
        match = re.search(r'(\d.+)', output)
        if match:
            return match.group(1)

    def getWindowsVersion(self, application):
        processes = application.getProcesses()
        for process in processes:
            path = process.executablePath
            if path:
                formatPath = path.replace('\\', '\\\\')
                self.__cmd = '"' + formatPath + '"' + ' --version'
            output = self.__client.execCmd(self.__cmd, 60000)
            if output and self.__client.getLastCmdReturnCode() == 0:
                version = self.__parseVersion(output)
                version = version.strip()
                if version:
                    self.__applicationOsh.setAttribute("application_version_number", version)
                    logger.debug('Windows PostgreSQL version: %s' % version)
                    break

    def getUnixVersion(self, application):
        process = application.getProcess('postmaster')
        if not process:
            process = application.getProcess('postgres')
        path = process.executablePath
        if path:
            self.__cmd = path + ' --version'
        output = self.__client.execCmd(self.__cmd, 60000)
        if output and self.__client.getLastCmdReturnCode() == 0:
            version = self.__parseVersion(output)
            version = version.strip()
            if version:
                self.__applicationOsh.setAttribute("application_version_number", version)
                logger.debug('PostgreSQL version: %s' % version)
            else:
                logger.debug('Cannot get PostgreSQL version.')
    
    def process(self, context):
        try:
            if self.__client.isWinOs():
                self.getWindowsVersion(context.application)
            else:
                self.getUnixVersion(context.application)
        except:
            errMsg = 'Failed executing command: ' + self.__cmd + '. Exception received: %s' % str((sys.exc_info()[1]))
            logger.errorException(errMsg)
        