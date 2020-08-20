#coding=utf-8

import logger
import sys
from plugins import Plugin

class VMwareVROpsPlugin(Plugin):

    def __init__(self):
        Plugin.__init(self)
        self.__client = None
        # self.__isWinOs = None
        self.__process = None

    def isApplicable(self, context):
        self.__client = context.client
        try:
            if self.__client.isWinOs():
                # self.__isWinOs = True
                self.__process = context.application.getProcess(
                    'vmware-vcops.exe')
            else:
                self.__process = context.application.getProcess('vmware-vcops')
            if self.__process:
                return True
        except:
            logger.errorException(sys.exc_info()[1])

    def process(self, context):
        # vROps CI
        applicationOsh = context.application.getOsh()

        # TODO: use shell to access vROps config file and extract application IP
        # TODO: extract vROps version
        application_ip = None

        # set Application IP attribute of VMware vRealize Operations CI
        applicationOsh.setAttribute("application_ip", application_ip)

