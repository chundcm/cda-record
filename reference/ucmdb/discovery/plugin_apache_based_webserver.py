#coding=utf-8
"""
plugin_apache_based_webserver
~~~~~~~~~~~~~

Plug-in triggers on Apache WebServer signature.
Functionality:
    - Determine Apache-based server configuration files & change type to strong &
        set path to file as attribute 'webserver_configfile'
    - Determine IBM HTTP server, based on Apache. Set strong type to 'ibmhttpserver'
"""

from Apache import WindowsResourceFactory
from Apache import UnixResourceFactory
from modeling import setWebServerVersion

from plugins import Plugin
import logger
import modeling
import netutils
from appilog.common.system.types.vectors import ObjectStateHolderVector
from applications import IgnoreApplicationException

class ApacheBasedWebServerPlugin(Plugin):
    def __init__(self):
        Plugin.__init__(self)
        self.__processes = {}
        self.__cookies = None

    def isApplicable(self, context):
        return 1

    def process(self, context):
        component = context.application.getApplicationComponent()
        self.__cookies = component.env

        factory = self.__getResourceFactory(context)
        if not len(context.application.getMainProcesses()):
            logger.debug('No main process')

        for process in context.application.getMainProcesses():
            logger.debug('Got main process: %s %s %s %s' % (process.getPid(), process.owner, process.executablePath, process.commandLine))
            processDiscoverer = factory.createProcessDiscoverer(process.executablePath, process.commandLine)
            try:
                cfgFiles = processDiscoverer.getConfigFiles()
                cfgFile = cfgFiles and cfgFiles[0] #get first one
                #if configuration-file is not found we can not conform strong type
                if cfgFile:
                    self.__processes[process.getPid()] = process
                else:
                    logger.debug('No config found, skipping')
            except:
                logger.reportWarning("discovery of Apache is partial: cannot guess configuration path")
                logger.debugException("discovery of Apache is partial: cannot guess configuration path")

        serverOsh = context.application.getOsh()

        #sort command lines to get wider one first with more information or longer path =)
        pids = self.__processes.keys()
        for pid in pids:
            process = self.__processes[pid]
            try:
                logger.debug('Making apache strong type from: %s %s %s %s' % (process.getPid(), process.owner, process.executablePath, process.commandLine))
                processDiscoverer = factory.createProcessDiscoverer(process.executablePath, process.commandLine)

                cfgFiles = processDiscoverer.getConfigFiles()
                cfgFile = cfgFiles and cfgFiles[0] #get first one

                discoverer = factory.createConfigDiscoverer(cfgFile, processDiscoverer)

                discoverer.discoverServerRoot()
                discoverer.discoverIsIhs()
                discoverer.discoverVersion()

                objectClass = 'apache'
                if discoverer.isIHS:
                    objectClass = 'ibmhttpserver'
                    serverOsh.setStringAttribute('data_name', 'IBM HTTP WebServer')
                    serverOsh.setAttribute('vendor', 'ibm_corp')

                if discoverer.serverVersion:
                    setWebServerVersion(serverOsh, discoverer.serverVersion)

                if discoverer.mainConfigFileObject:
                    serverOsh.setObjectClass(objectClass)
                    serverOsh.setAttribute('webserver_configfile', discoverer.mainConfigFileObject.path.lower())
                    #save path to cookie
                    self.__savePathCookie(process.executablePath, cfgFile, 1)
                    logger.debug('Done')

                    #reportEndpointByConfigFile
                    self.reportEndpointByConfigFile(context,discoverer)
                    break
            except:
                msg = 'Failed to define server configuration files by command line: %s' % process.commandLine
                logger.debugException(msg)

    def __getResourceFactory(self, context):
        if context.client.isWinOs():
            resourceFactoryClass = WindowsResourceFactory
        else:
            resourceFactoryClass = UnixResourceFactory
        return resourceFactoryClass(context.hostOsh, context.client, context.framework)

    def __getCookieKey(self, processPath, config):
        if processPath:
            return '%s:cmdpath:%s:config:%s' % (__name__, processPath.lower(), config)

    def __hasPathCookie(self, processPath, config):
        if processPath:
            return self.__cookies.containsKey(self.__getCookieKey(processPath, config))

    def __savePathCookie(self, processPath, config,  value):
        if processPath:
            self.__cookies.put(self.__getCookieKey(processPath, config), value)

    def reportEndpointByConfigFile(self,context,apacheConfigDiscoverer):
        address = context.application.getApplicationIp()
        apacheOsh = context.application.getOsh()
        logger.debug("reporting endpoints for apache based webserver using configfile ")
        endpointOSHV = ObjectStateHolderVector()
        if apacheConfigDiscoverer.mainConfigFileObject:
            apacheConfigDiscoverer.discoverConfigFile(apacheConfigDiscoverer.mainConfigFileObject)
            for includeConfigFileObject in apacheConfigDiscoverer.includeConfigFileObjects:
                if includeConfigFileObject.path in apacheConfigDiscoverer.discoveredConfigFiles:
                    continue
                apacheConfigDiscoverer.discoverConfigFile(includeConfigFileObject)
                #apacheConfigDiscoverer.discoverJkWorkersFile(includeConfigFileObject)

            listenAddressesDict = apacheConfigDiscoverer.listenAddresses
            for ip, port in listenAddressesDict:
                if port:
                    if address != ip:
                        apacheOsh.setAttribute('application_ip', ip)
                    endpoint = netutils.Endpoint(port, netutils.ProtocolType.TCP_PROTOCOL, ip)
                    endpointOSH = modeling.createIpServerOSH(endpoint)
                    hostosh = modeling.createHostOSH(ip)
                    endpointOSH.setContainer(hostosh)
                    linkOsh = modeling.createLinkOSH("usage", apacheOsh, endpointOSH)
                    endpointOSHV.add(endpointOSH)
                    endpointOSHV.add(linkOsh)
                    logger.debug('Get ip using configfile:',ip)
                    logger.debug('Get port using configfile:', port)
        if endpointOSHV:
            context.resultsVector.addAll(endpointOSHV)