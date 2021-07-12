# coding=utf-8
import ntpath
import posixpath

from appilog.common.system.types.vectors import ObjectStateHolderVector
from java.lang import Exception as JavaException

import errormessages
import logger
from file_mon_utils import FileMonitor
from tomcat_by_shell import TomcatDiscoverer


def discover(Framework, shell, client, applicationResult, OSHVResult, hostOsh):
    logger.debug('Start to discover Tomcat application by SCP')

    TempOSHVResult = ObjectStateHolderVector()

    configFileList = [applicationResult.application.getOsh().getAttributeValue('webserver_configfile')]
    logger.debug('configFileList = ', configFileList)

    custom_configList = Framework.getParameter('configFiles') or ''
    for x in custom_configList.split(','):
        configFileList.append(x.strip())

    if configFileList:
        try:
            discoverer = AsmTomcatDiscoverer(Framework, TempOSHVResult, shell, client, hostOsh)
            for configFile in configFileList:
                if configFile and configFile != 'NA':
                    try:
                        discoverer.discoverTomcat(configFile)
                    except Exception, ex:
                        logger.info('Failed to parse:', configFile)
                        logger.info("Exception: ", ex)
        except:
            logger.debugException('Failed to discover Apache Tomcat')
            Framework.reportError('Failed to discover Apache Tomcat. See logs')

    scpContext = Framework.getDestinationAttribute('service_context')
    appName = filter(None, scpContext.split('/'))[0]
    logger.debug('Get appName "%s" from SCP context "%s".' % (appName, scpContext))

    for i in range(TempOSHVResult.size()):
        osh = TempOSHVResult.get(i)
        if osh.getObjectClass() == 'webapplication':
            resourcePath = osh.getAttributeValue('resource_path')
            if shell.isWinOs():
                resourcePath = ntpath.normpath(resourcePath)
                webAppName = filter(None, resourcePath.split('\\'))[-1]
            else:
                resourcePath = posixpath.normpath(resourcePath)
                webAppName = filter(None, resourcePath.split('/'))[-1]

            logger.debug('Try to match web application "%s" with appName "%s".' % (webAppName, appName))
            if webAppName == appName:
                applicationResult.applicationresources.append(osh)


class AsmTomcatDiscoverer(TomcatDiscoverer):
    # Why need this class?
    # For Host Discovery by Shell job, host may be just discovered and hasn't been sent to UCMDB server.
    # In such case, it cannot create host osh by cmdb id in file_mon_utils.FileMonitorEx#connect method.
    # So what we can do is bypassing file_mon_utils.FileMonitorEx#connect method
    # and adding asm_tomcat_deep_discovery.AsmTomcatDiscoverer#connectWithHost method.
    def __init__(self, Framework, OSHVResult, shellUtils, client, hostOsh):
        TomcatDiscoverer.__init__(self, Framework, OSHVResult, shellUtils)
        self.connectWithHost(client, hostOsh)

    def connect(self):
        pass

    def connectWithHost(self, client, hostOsh):
        protocolName = client.getClientType()
        try:
            self.ipaddress = self.Framework.getDestinationAttribute('ip_address')
            self.hostOSH = hostOsh

            self.fileMonitor = FileMonitor(self.Framework, self.shellUtils, None, '', None)
            self.fileMonitor.hostOSH = hostOsh
            self.FileSeparator = self.fileMonitor.FileSeparator
        except JavaException, ex:
            strException = ex.getMessage()
            errormessages.resolveAndReport(strException, protocolName, self.Framework)
            self.shellUtils = None
            self.fileMonitor = None
        except:
            strException = logger.prepareJythonStackTrace('')
            errormessages.resolveAndReport(strException, protocolName, self.Framework)
            self.shellUtils = None
            self.fileMonitor = None
