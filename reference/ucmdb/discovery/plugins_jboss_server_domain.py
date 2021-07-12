#coding=utf-8
from plugins import Plugin
from appilog.common.system.types.vectors import ObjectStateHolderVector
from appilog.common.system.types import ObjectStateHolder
from com.hp.ucmdb.discovery.library.communication.downloader.cfgfiles import GeneralSettingsConfigFile

import ip_addr
import entity
import re
import logger
import modeling
import file_system
import file_topology
import jee
import jboss_discoverer
import netutils
from fptools import partiallyApply
import fptools
from iteratortools import first, keep
import jee_discoverer
from com.hp.ucmdb.discovery.library.clients import ClientsConsts

class JbossServerPlugin(Plugin):

    def isApplicable(self, context):
        mainProcesses = context.application.getMainProcesses()
        if not (mainProcesses and mainProcesses[0]):
            logger.warn("No JBoss process found")
            return 0
        return 1


class Jboss3to6ServerPlugin(JbossServerPlugin):
    '''
    Purpose of plugin is reporing of server name and domain name by cmd-line
    '''
    def __init__(self):
        Plugin.__init__(self)

    def process(self, context):
        application = context.application
        osh = application.getOsh()
        process = application.getMainProcesses()[0]
        command_line = process.commandLine
        server_name = 'default'
        p = 'org\.jboss\.Main.*?\s+-{1,2}(?:c\s+|configuration\s*=\s*)([\w_\.-]+)'
        m = re.search(p, command_line)
        if m is not None:
            server_name = m.group(1)
            logger.debug('Found jboss ', server_name, ' configuration')
        else:
            logger.debug('Found jboss default configuration')
        osh.setAttribute('j2eeserver_servername', server_name)
        #TODO: replace to jee.ServerTopologyBuilder._composeFullName
        osh.setAttribute('j2eeserver_fullname', server_name)
        osh.setAttribute('name', server_name)
        modeling.setJ2eeServerAdminDomain(osh, server_name)
        modeling.setAppServerType(osh)

        if context.client.getClientType() != ClientsConsts.WMI_PROTOCOL_NAME:
            self.reportendPortByConfigfile(context)

    def reportendPortByConfigfile(self,context):
        logger.debug("reporting endpoints for jboss3-6 using configfile")
        endpointOSHV = ObjectStateHolderVector()
        ip = context.application.getApplicationIp()
        shell = context.client
        fs = file_system.createFileSystem(shell)
        globalSettings = GeneralSettingsConfigFile.getInstance()
        loadExternalDTD = globalSettings.getPropertyBooleanValue('loadExternalDTD', 0)
        application = context.application
        osh = application.getOsh()
        process = application.getMainProcesses()[0]
        path = file_system.getPath(fs)
        ### JBoss server System Properties discovery:
        cmdLineElements = jee.JvmCommandLineDescriptor(process.commandLine).parseElements()
        serverSystemProperties = jboss_discoverer.SystemPropertiesDiscoverer().discoverProperties(fs, cmdLineElements)
        # JBoss HomeDir path discovery:
        jbossHomePath = jboss_discoverer.discoverHomeDirPath(fs, serverSystemProperties, cmdLineElements)
        if not jbossHomePath:
            logger.warn("Failed to report endpoints for jboss3-6 using configfile, because of the configfile cannot be found.")
            return
        # JBoss version discovery:
        versionLayout = None
        try:
            versionLayout = jboss_discoverer.VersionLayout(fs, jbossHomePath)
        except:
            logger.debugException('Failed to create JBoss layout.')
            logger.reportWarning('Failed to create JBoss Layout. No endpoints from config file will be reported.')
            return
        versionInfoDiscoverer = jboss_discoverer.VersionInfoDiscovererByShell(shell, versionLayout)
        versionInfo = versionInfoDiscoverer.discoverVersion()
        platformTrait = jboss_discoverer.getPlatformTrait(versionInfo)
        # Setting JBoss bind address by default, if jboss.bind.address wasn't set:
        serverSystemProperties.setdefault('jboss.bind.address',
                              (platformTrait.majorVersion.value() == 3
                               and '0.0.0.0'
                               or '127.0.0.1'))
        # resolve JBoss File Separator:
        serverSystemProperties['/'] = fs.FileSeparator
        # set corresponding properties to found values:
        serverSystemProperties['jboss.home.dir'] = jbossHomePath
        serverSystemProperties['jboss.home.url'] = ''.join((jbossHomePath,'/'))
        # resolve relative properties with custom or default values:
        if jbossHomePath:
            serverSystemProperties.setdefault('jboss.lib.url', path.join(jbossHomePath, 'lib'))
            serverSystemProperties.setdefault('jboss.server.base.dir', path.join(jbossHomePath, 'server'))
            serverSystemProperties.setdefault('jboss.server.base.url', ''.join((serverSystemProperties.get('jboss.home.url'), '/server/')))
            serverSystemProperties.setdefault('jboss.common.base.url', ''.join((serverSystemProperties.get('jboss.home.url'), '/common/')))
            serverSystemProperties.setdefault('jboss.common.lib.url', ''.join((serverSystemProperties.get('jboss.common.base.url'), '/lib/')))
        # Setting JBoss default server name, if jboss.server.name wasn't set:
        serverSystemProperties.setdefault('jboss.server.name',
                              (platformTrait.majorVersion.value() == 4
                               and platformTrait.isEAP()
                               and 'production'
                               or 'default'))
        # ServerHomeDir path discovery:
        serverHomePath = jboss_discoverer.discoverServerHomeDirPath(fs, serverSystemProperties.get('jboss.server.name'), jbossHomePath, serverSystemProperties)
        # set corresponding properties to found values:
        serverSystemProperties['jboss.server.home.dir'] = serverHomePath
        serverSystemProperties['jboss.server.url'] = ''.join((serverHomePath,'/'))
        serverSystemProperties['jboss.server.home.url'] = ''.join((serverHomePath,'/'))
        if serverHomePath:
            serverSystemProperties.setdefault('jboss.server.temp.dir', path.join(serverHomePath, 'tmp'))
            serverSystemProperties.setdefault('jboss.server.tmp.dir', path.join(serverHomePath, 'tmp'))
            serverSystemProperties.setdefault('jboss.server.data.dir', path.join(serverHomePath, 'data'))
            serverSystemProperties.setdefault('jboss.server.log.dir', path.join(serverHomePath, 'log'))
            serverSystemProperties.setdefault('jboss.server.config.url', ''.join((serverSystemProperties.get('jboss.server.home.url'), '/conf/')))
            serverSystemProperties.setdefault('jboss.server.lib.url', ''.join((serverSystemProperties.get('jboss.server.home.url'), '/lib/')))
        # Server ConfigDir discovery:
        serverConfigPath = jboss_discoverer.discoverServerConfigPath(fs, serverSystemProperties.get('jboss.server.config.url'), serverHomePath)
        logger.debug('Found server config path: %s' % serverConfigPath)

        ### Config files / resources dirs paths discovery:
        serverConfigParser = jboss_discoverer.createServerConfigParser(loadExternalDTD, platformTrait)
        configFilePath = None
        # For JBoss 3.x - 4.x path to Binding Configuration stored in main-config (jboss-service.xml): 
        try:
            if platformTrait.majorVersion.value() in (3, 4):
                configFilePath = path.join(serverConfigPath, 'jboss-service.xml')
                configFile = fs.getFile(configFilePath, [file_topology.FileAttrs.CONTENT, file_topology.FileAttrs.PATH])
            # For 5.0, 5.0 EAP, 5.1, 5.1 EAP, 6.0, 6.1
            # there is some custom settings file can be defined in ${jboss.server.config.url}/bootstrap/profile.xml file:
            # - path to custom main config file
            # - path to bindings configuration
            # - list of JEE resources dirs
            elif platformTrait.majorVersion.value() in (5, 6):
                profileLayout = jboss_discoverer.ProfileLayout(fs, serverConfigPath)
                profileDiscoverer = jboss_discoverer.ProfileDiscoverer(shell, profileLayout, serverConfigParser)
                # parse settings from profile.xml and resolve expression in value:
                # find custom or get default path to jboss-service.xml
                configFilePath = serverSystemProperties.getFilePathFromURLValue(serverSystemProperties.resolveProperty(profileDiscoverer.discoverConfigFilePathName())) \
                                 or path.join(serverConfigPath, 'jboss-service.xml')
                configFile = fs.getFile(configFilePath, [file_topology.FileAttrs.CONTENT, file_topology.FileAttrs.PATH])
        except:
            logger.debugException('Failed to get JBoss service config file. Plug-in aborted.')
            raise Exception('Failed to get JBoss service config file. Plug-in aborted.')
        ### Bootstrap files discovery:
        bootstrapLayout = jboss_discoverer.BootstrapLayout(fs, serverConfigPath)
        bootstrapParser = jboss_discoverer.BootstrapParser()
        bootstrapDiscoverer = jboss_discoverer.BootstrapDiscovererByShell(shell, bootstrapLayout, bootstrapParser)
        bootstrapConfigFiles = bootstrapDiscoverer.discoverBootstrapConfigFiles(serverSystemProperties)
        configFiles, resourcesDirs, farmDirs, bindingsDirs, bindingsConfigs = bootstrapDiscoverer.discoverServerConfigAndResources(bootstrapConfigFiles, serverSystemProperties)
        if not configFiles:
            configFiles.append(configFile)
        configFiles = filter(lambda x: x.content, configFiles)

        configFilesContents = map(lambda x: x.content, configFiles)

        if not resourcesDirs:
            for configContent in configFilesContents:
                resourcesDirsListWithExpressions = serverConfigParser.parseResourcesDirsList(configContent)
                resourcesDirsList = map(serverSystemProperties.getFilePathFromURLValue, map(serverSystemProperties.resolveProperty, resourcesDirsListWithExpressions))
                for pathValue in resourcesDirsList:
                    absPath = path.isAbsolute(pathValue) and pathValue \
                              or path.join(serverSystemProperties.get('jboss.server.url'), pathValue)
                    resourcesDirs.append(path.normalizePath(absPath))
        bindingsConfigsLayout = \
            jboss_discoverer.BindingsConfigsLayout(fs, bindingsDirs)
        bindingsConfigsDiscoverer = \
            jboss_discoverer.BindingsConfigsDiscovererByShell(shell,
                                              bindingsConfigsLayout,
                                              bootstrapParser)
        bindingsConfigs.extend(
               bindingsConfigsDiscoverer.discoverBindingsConfigFiles())
        if not bindingsConfigs:
            for configContent in configFilesContents:
                bindingConfigWithExpressions = serverConfigParser.parseBindingManagerConfigPath(configContent)
                bindingConfig = serverSystemProperties.getFilePathFromURLValue(serverSystemProperties.resolveProperty(bindingConfigWithExpressions))
                bindingsConfigs.append(bindingConfig)
        bindingConfig = bindingsConfigs[0] if bindingsConfigs else None

        ipAddressList = context.framework.getTriggerCIDataAsList('ip_address_list')
        endpoints = []
        bindingsWithExpressions = []
        # at first read port binding configuration directly from jboss-services.xml
        if not bindingConfig:
            if configFile.content:
                bindingsWithExpressions = serverConfigParser.parseBindingsFromJBossServiceXml(configFile.content)
        else: # in case of binding configuration separated in custom bindings file
            bidingConfigContent = fs.getFile(bindingConfig, [file_topology.FileAttrs.CONTENT]).content
            # JBoss version 3.x - 4.x doesn't support portOffset, create endpoints as is
            if platformTrait.majorVersion.value() in (3, 4):
                bindingManagerName = serverConfigParser.parseBindingManagerConfigName(configFile.content)
                bindingsWithExpressions = serverConfigParser.parseBindingsFromBindingManagerConfig(bidingConfigContent, bindingManagerName)
            # In JBoss version 5.x - 6.x except endpoints, there are offset and default host
            if platformTrait.majorVersion.value() in (5, 6):
                # get ports configuration
                activeMetadataSetName = serverConfigParser.parseActiveMetadataSetName(bidingConfigContent)
                metadataSetWithExpressions = serverConfigParser.parseMetadataSetConfiguration(bidingConfigContent, activeMetadataSetName)
                # get offset and defaultHost configuration
                activeBindingSetNameWithExpression = serverConfigParser.parseActiveBindingSetName(bidingConfigContent)
                activeBindingSetName = serverSystemProperties.resolveProperty(activeBindingSetNameWithExpression)
                portOffsetWithExpression, defaultHostWithExpression = serverConfigParser.parseBindingSetConfiguration(bidingConfigContent, activeBindingSetName)
                # resolve expressions in portOffset and defaultHost:
                portOffset = entity.Numeric(int)
                defaultHost = None
                try:
                    portOffset.set(serverSystemProperties.resolveProperty(portOffsetWithExpression))
                    defaultHost = serverSystemProperties.resolveProperty(defaultHostWithExpression)
                except Exception:
                    logger.debug('Failed to get port-offset and defaultHost')
                # apply portOffset and set default host to bindings:
                for binding in metadataSetWithExpressions:
                    portOrigValue = entity.Numeric(int)
                    portWithOffset = entity.Numeric(int)
                    try:
                        portOrigValue.set(serverSystemProperties.resolveProperty(binding.getPort()))
                        offset = portOffset.value() or 0
                        portWithOffset.set(portOrigValue.value() + offset)
                        host = binding.getHost() or defaultHost
                        bindingsWithExpressions.append(jboss_discoverer.ServerSocketDescriptor(str(portWithOffset), host))
                    except Exception:
                        logger.debug('Failed to apply port offset or default host')

        # resolve system properties expressions in bindings:
        for binding in bindingsWithExpressions:
            try:
                portValue = serverSystemProperties.resolveProperty(binding.getPort())
                port = entity.Numeric(int)
                port.set(portValue)
                # in case of host doesn't defined jboss is using ${jboss.bind.address}
                host = serverSystemProperties.resolveProperty(binding.getHost() or '${jboss.bind.address}')
                host = (host == '127.0.0.1' and ip or host)
                hostAddresses = (host == '0.0.0.0' and ipAddressList
                                 or (host,))
                for address in hostAddresses:
                    endpoint = netutils.createTcpEndpoint(address, port.value())
                    endpoints.append(endpoint)
                    logger.debug('the binding port is,',port.value())
                    logger.debug('the address is:',address)

                    endpoint = netutils.Endpoint(port.value(), netutils.ProtocolType.TCP_PROTOCOL, address)
                    endpointOSH = modeling.createIpServerOSH(endpoint)
                    hostosh = modeling.createHostOSH(ip)
                    endpointOSH.setContainer(hostosh)
                    linkOsh = modeling.createLinkOSH("usage", context.application.getOsh(), endpointOSH)
                    endpointOSHV.add(endpointOSH)
                    endpointOSHV.add(linkOsh)
                    logger.debug('Get ip using configfile:',ip)
                    logger.debug('Get port using configfile:', port)
            except Exception:
                logger.debug('Failed to create server endpoint')
        if endpointOSHV:
            context.resultsVector.addAll(endpointOSHV)


class Jboss7StandaloneServerPlugin(JbossServerPlugin):
    '''
    Purpose of plugin is reporing of server name and domain name by config-file
    '''
    def __init__(self):
        Plugin.__init__(self)

    def process(self, context):
        application = context.application
        osh = application.getOsh()
        shell = context.client
        fs = file_system.createFileSystem(shell)
        ip = application.getConnectionIp()
        dns_resolver = jee_discoverer.DnsResolverDecorator(
                                netutils.createDnsResolverByShell(shell), ip)
        process = application.getMainProcesses()[0]
        cmd_line = process.commandLine
        server_runtime = jboss_discoverer.createServerRuntime(cmd_line, ip)
        home_dir = server_runtime.findHomeDirPath()
        serverBaseDir = server_runtime.findServerBaseDirPath()
        serverConfigDir = server_runtime.findServerConfigDirPath()
        config = server_runtime.extractOptionValue('--server-config') or server_runtime.extractOptionValue('-c')
        layout = jboss_discoverer.StandaloneModeLayout(fs, home_dir, config, serverBaseDir, serverConfigDir)
        loadDtd = 0
        server_config_parser = jboss_discoverer.ServerConfigParserV7(loadDtd)
        standalone_config_path = layout.getStandaloneConfigPath()  
        standalone_config_file = None
        try:
            standalone_config_file = layout.getFileContent(standalone_config_path)    
        except:
            logger.debugException('Failed getting JBoss config file. No extra data will be reported')
            raise Exception('Failed getting JBoss config file. No extra data will be reported')
        content = standalone_config_file.content
        standalone_config_with_expressions = (
                    server_config_parser.parseStandaloneServerConfig(content))
        server_properties = jboss_discoverer.SystemProperties()
        properties_from_cmd_line = server_runtime.findJbossProperties()
        server_properties.update(properties_from_cmd_line)
        config_props = standalone_config_with_expressions.getSystemProperties()
        server_properties.update(config_props)
        standalone_config = server_config_parser.resolveStandaloneServerConfig(
                         standalone_config_with_expressions, server_properties)
        server_name = standalone_config.getServerName()
        if not server_name:
            if serverBaseDir is not None:
                path_util = file_system.getPath(fs)
                server_name = path_util.baseName(serverBaseDir)
            else:
                p = '-Djboss.node.name=([\w_\.-]+)\s'
                jnn = re.search(p, cmd_line)
                p = '-Djboss.server.name=([\w_\.-]+)\s'
                jsn = re.search(p, cmd_line)
                if jnn is not None:
                    server_name = jnn.group(1)
                elif jsn is not None:
                    server_name = jsn.group(1)
                else:
                    try:
                        server_name = dns_resolver.resolveHostnamesByIp(ip)[0]
                    except netutils.ResolveException:
                        server_name = 'Default'
        if server_name is not None:
            osh.setAttribute('j2eeserver_servername', server_name)
            #TODO: replace to jee.ServerTopologyBuilder._composeFullName
            osh.setAttribute('j2eeserver_fullname', server_name)
            osh.setAttribute('name', server_name)
            modeling.setJ2eeServerAdminDomain(osh, server_name)
        modeling.setAppServerType(osh)

        ##reportEndpointByConfigFile
        self.reportEndpointByConfigFile(context, shell, standalone_config)

    def reportEndpointByConfigFile(self,context, shell, standalone_config):
        logger.debug("reporting endpoints for jboss7 using configfile")
        endpointOSHV = ObjectStateHolderVector()
        interfaces = standalone_config.getInterfaces()
        names = []
        addresses = []
        for interface in interfaces:
            names.append(interface.getName())
            addresses.append(interface.getInetAddress())
        interfaceDict = dict(zip(names,addresses))
        socketbindgroups = standalone_config.getSocketBindingGroup()
        socketbinds = socketbindgroups.getBindings()
        for socketbind in socketbinds:
            port = socketbind.getPort()
            host = interfaceDict.get(socketbind.getInterfaceName())
            ip = None
            if port:
                if not host or host == '*' or host == '127.0.0.1':
                    if context.application.getApplicationIp():
                        ip = context.application.getApplicationIp()
                elif netutils.isValidIp(host):
                    ip = host
                else:
                    ip = netutils.resolveIP(shell,host)
                if ip:
                    endpoint = netutils.Endpoint(port, netutils.ProtocolType.TCP_PROTOCOL, ip)
                    endpointOSH = modeling.createIpServerOSH(endpoint)
                    hostosh = modeling.createHostOSH(ip)
                    endpointOSH.setContainer(hostosh)
                    linkOsh = modeling.createLinkOSH("usage", context.application.getOsh(), endpointOSH)
                    endpointOSHV.add(endpointOSH)
                    endpointOSHV.add(linkOsh)
                    logger.debug('Get ip using configfile standalone.xml:',ip)
                    logger.debug('Get port using configfile standalone.xml:', port)
        if endpointOSHV:
            context.resultsVector.addAll(endpointOSHV)


class Jboss7ManagedServerPlugin(JbossServerPlugin):
    '''
    Purpose of plugin is reporing of server name and domain name by cmd-line
    '''
    def __init__(self):
        Plugin.__init__(self)

    def __parse_server_name_from_server_option(self, element):
        '''
        Parse server name from -D[Server:<name>] param
        @types: jee.CmdLineElement -> str?
        '''
        element_name = element.getName()
        if element_name.startswith('[Server:') != 0:
            logger.debug('Found by server param: %s' % element_name[8:-1])
            return element_name[8:-1]

    def  __parse_server_name_from_log_file_path(self, element, path_util):
        '''
        Parse server name from log-file param
        @types: jee.CmdLineElement, file_topology.Path -> str?
        '''
        element_name = element.getName()
        if element_name == 'org.jboss.boot.log.file':
            log_file_path = element.getValue()
            if path_util.isAbsolute(log_file_path):
                log_dir = path_util.dirName(log_file_path)
                server_dir = path_util.dirName(log_dir)
                logger.debug('Found by log-file %s' % path_util.baseName(server_dir))
                return path_util.baseName(server_dir)

    def parse_server_name(self, element, path_util):
        return (self.__parse_server_name_from_server_option(element) or
               self.__parse_server_name_from_log_file_path(element, path_util))

    def __is_java_option(self, element):
        return element.getType() == jee.CmdLineElement.Type.JAVA_OPTION

    def process(self, context):
        shell = context.client
        fs = file_system.createFileSystem(shell)
        path_util = file_system.getPath(fs)
        application = context.application
        osh = application.getOsh()
        process = application.getMainProcesses()[0]
        cmd_line = process.commandLine
        jvm_cmd_line_descriptor = jee.JvmCommandLineDescriptor(cmd_line)
        cmd_line_elements = jvm_cmd_line_descriptor.parseElements()
        java_options = filter(self.__is_java_option, cmd_line_elements)
        parse_fn = partiallyApply(self.parse_server_name, fptools._, path_util)
        server_name = first(keep(parse_fn, java_options))
        logger.debug('server name: %s' % server_name)
        if server_name is not None:
            osh.setAttribute('j2eeserver_servername', server_name)
            #TODO: replace to jee.ServerTopologyBuilder._composeFullName
            osh.setAttribute('j2eeserver_fullname', server_name)
        modeling.setAppServerType(osh)

        self.reportEndpointByConfigFile(context, application, cmd_line, fs)

    def reportEndpointByConfigFile(self,context, application, cmd_line, fs):
        logger.debug("reporting endpoints for jboss7 ManagedServer configfile")
        endpointOSHV = ObjectStateHolderVector()
        ip = application.getConnectionIp()
        server_runtime = jboss_discoverer.createServerRuntime(cmd_line, ip)
        home_dir = server_runtime.findHomeDirPath()
        config = server_runtime.extractOptionValue('--server-config')
        layout = jboss_discoverer.DomainModeLayout(fs, home_dir, config)
        loadDtd = 0
        server_config_parser = jboss_discoverer.ServerConfigParserV7(loadDtd)
        host_ConfigPath = layout.getHostConfigPath()
        host_ConfigPath_file = layout.getFileContent(host_ConfigPath)
        host_ControllerConfigWithExpressions = server_config_parser.parseHostControllerConfig(host_ConfigPath_file.content)
        # Host-Controller System Properties propagated from Domain System Properties and can be defined at host-controller config
        host_ControllerProperties = jboss_discoverer.SystemProperties()
        # update system properties from host-controller config-file:
        host_ControllerProperties.update(host_ControllerConfigWithExpressions.getSystemProperties())
        # now we are ready to resolve host-controller config-expressions to values
        host_ControllerConfig = server_config_parser.resolveHostControllerConfig(host_ControllerConfigWithExpressions, host_ControllerProperties)
        managementBindings = host_ControllerConfig.getManagementBindings()
        for managementBinding in managementBindings:
            port = managementBinding.getPort()
            if port:
                if context.application.getApplicationIp():
                    ip = context.application.getApplicationIp()
                endpoint = netutils.Endpoint(port, netutils.ProtocolType.TCP_PROTOCOL, ip)
                endpointOSH = modeling.createIpServerOSH(endpoint)
                hostosh = modeling.createHostOSH(ip)
                endpointOSH.setContainer(hostosh)
                linkOsh = modeling.createLinkOSH("usage", context.application.getOsh(), endpointOSH)
                endpointOSHV.add(endpointOSH)
                endpointOSHV.add(linkOsh)
                logger.debug('Get ip using configfile domain.xml:',ip)
                logger.debug('Get port using configfile domain.xml:', port)
        if endpointOSHV:
            context.resultsVector.addAll(endpointOSHV)
