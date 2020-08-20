#coding=utf-8
from plugins import Plugin

import file_ver_lib
import logger
import re


class IBMWebSphereMQVersionFromExecutableNTCMD(Plugin):

    def __init__(self):
        Plugin.__init__(self)
        self.__client = None
        self.__process = None
        self.__applicationOsh = None

    def isApplicable(self, context):
        self.__client = context.client
        self.__process = context.application.getProcess('amqsvc.exe')
        self.__applicationOsh = context.application.getOsh()
        if self.__client.isWinOs() and self.__process:
            return 1
        else:
            return 0

    def __parseVersion(self, version):
        if len(version) > 2:
            parsedVersion = version[0]+'.'+version[2]
            return parsedVersion
        else:
            return version

    def process(self, context):
        path = self.__process.executablePath
        if path:
            version = file_ver_lib.getWindowsWMICFileVer(self.__client, path)
            if not version:
                logger.debug('Failed getting version by WMIC.')
                logger.debug('Trying get version by Shell....')
                version = file_ver_lib.getWindowsShellFileVer(self.__client, path)
            if version:
                parsedVersion = self.__parseVersion(version)
                self.__applicationOsh.setAttribute("application_version_number", parsedVersion)
            else:
                logger.debug('Failed getting version for IBM WebSphere MQ using NTCMD protocol.')
        else:
            logger.debug('Filed getting full process path for IBM WebSphere MQ using NTCMD protocol.')

class IBMWebSphereMQVersionFromExecutableWMI(Plugin):

    def __init__(self):
        Plugin.__init__(self)
        self.__client = None
        self.__process = None
        self.__applicationOsh = None

    def isApplicable(self, context):
        self.__client = context.client
        self.__process = context.application.getProcess('amqsvc.exe')
        self.__applicationOsh = context.application.getOsh()
        if self.__process:
            return 1
        else:
            return 0

    def __parseVersion(self, version):
        if len(version) > 2:
            parsedVersion = version[0]+'.'+version[2]
            return parsedVersion
        else:
            return version

    def process(self, context):
        path = self.__process.executablePath
        if path:
            version = file_ver_lib.getWindowsWMIFileVer(self.__client, path)
            if version:
                parsedVersion = self.__parseVersion(version)
                self.__applicationOsh.setAttribute("application_version_number", parsedVersion)
            else:
                logger.debug('Failed getting version for IBM WebSphere MQ using WMI.')


class IBMWebSphereMQVersionFromExecutableSSH(Plugin):
    def __init__(self):
        Plugin.__init__(self)
        self.__client = None
        self.__process = None
        self.__applicationOsh = None

    def isApplicable(self, context):
        self.__client = context.client
        self.__process = context.application.getProcess('runmqlsr')
        if not self.__process:
            self.__process = context.application.getProcess('amqzmur0')
        self.__applicationOsh = context.application.getOsh()
        if self.__process:
            return 1
        else:
            return 0

    def __parseVersion(self, version):
        if version:
            match = re.match('Version:\s+([\d\.]+)', version)
            version = match and match.group(1)
            return version
        else:
            return version

    def process(self, context):
        disInfo = self.__client.execCmd('dspmqver |grep Version')
        if disInfo:
            parsedVersion = self.__parseVersion(disInfo)
            if parsedVersion:
                self.__applicationOsh.setAttribute("application_version_number", parsedVersion)
        else:
            logger.debug('Failed getting version for IBM WebSphere MQ by command dspmqver')