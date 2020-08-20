import re

from appilog.common.system.types.vectors import ObjectStateHolderVector
from com.hp.ucmdb.discovery.library.communication.downloader.cfgfiles import GeneralSettingsConfigFile
from java.lang import Exception as JException

import asm_dependency_resolver
import asm_weblogic_discoverer
import file_system
import fptools
import jee
import jee_connection
import jee_discoverer
import jms
import logger
import netutils
import process as processModule
import process_discoverer
import weblogic
import weblogic_discoverer
from fptools import applyMapping, curry, _, safeFunc, each
from weblogic_by_shell import isWeblogicAsProcess, getDomainRootDirPath, debugGroups


def discover(Framework, shell, client, applicationResult, OSHVResult, hostOsh):
    logger.debug('Start to discover WebLogic application by SCP')

    scpType = Framework.getDestinationAttribute('service_connection_type')
    scpContext = Framework.getDestinationAttribute('service_context')

    Framework = jee_connection.EnhancedFramework(Framework)
    platform = jee.Platform.WEBLOGIC
    try:
        # prepare components for topology discoverer
        fs = file_system.createFileSystem(shell)
        processDiscoverer = process_discoverer.getDiscovererByShell(shell)
    except (Exception, JException), exc:
        logger.warnException(str(exc))
        jee_connection.reportError(Framework, str(exc), platform.getName())
    else:
        # #discover tnsnames.ora file
        # logger.debug("try to find tnsnames.ora file")
        # hostId = Framework.getDestinationAttribute('hostId')
        # Framework.sendObjects(jee_discoverer.discoverTnsnamesOra(hostId, client))

        # discover all processes
        runningProcesses = processDiscoverer.discoverAllProcesses()
        # make filtering for Weblogic processes
        serverProcesses = filter(isWeblogicAsProcess, runningProcesses)
        # obvious to terminate discovery job without weblogic processes running
        if not serverProcesses:
            logger.reportWarning("No Weblogic processes currently running")

        # create catalog of processes by PID
        processByPid = applyMapping(processModule.Process.getPid, runningProcesses)

        # for each weblogic server process get root directory path
        getRootDirectoryPath = curry(getDomainRootDirPath,
                                     shell, fs, _, processByPid.get
        )
        rootDirectoryPaths = map(safeFunc(getRootDirectoryPath), serverProcesses)
        # group server processes by root directory path
        processesByRootDirPath = {}
        for process, path in zip(serverProcesses, rootDirectoryPaths):
            processesByRootDirPath.setdefault(path, []).append(process)
        debugGroups(processesByRootDirPath)

        # NEXT CASE IS NOT SUPPORTED (as property weblogic.ConfigFile
        #     removed starting from version 9
        # important to understand that some processes may specify alternative
        # path to the configuration
        #     -Dweblogic.ConfigFile=c:\mydir\myfile.xml
        # OR
        # Instead, use the following arguments:
        #     -Dweblogic.RootDirectory=c:\mydir
        #     -Dweblogic.ConfigFile=myfile.xml

        # prepare DNS resolver used in every discoverer.
        # Should be weblogic-specific resolver to resolve ${host.name} or "*"
        destinationIp = client.getIpAddress()
        dnsResolver = jee_discoverer.DnsResolverDecorator(
                            netutils.createDnsResolverByShell(shell), destinationIp
        )
        # create reporters and builders for weblogic topology
        globalSettings = GeneralSettingsConfigFile.getInstance()
        loadExternalDtd = globalSettings.getPropertyBooleanValue('loadExternalDtd', 0)
        reporterCreator = jee_discoverer.createTopologyReporterFactory(
                                                    weblogic.ServerTopologyBuilder(),
                                                    dnsResolver)

        for rootDirPath, processes in processesByRootDirPath.items():
            if not rootDirPath:
                continue
            try:
                try:
                    logger.debug("Discover topology for the domain in %s" % rootDirPath)
                    domainLayout = weblogic_discoverer.createDomainLayout(fs, rootDirPath)
                    # at this point we have layout of version corresponding to version of platform
                    parser = weblogic_discoverer.createDomainConfigParserByLayout(domainLayout, loadExternalDtd)
                except (Exception, JException):
                    logger.warnException("Failed to process domain in %s" % rootDirPath)
                    continue
                try:
                    domainDescriptorFile = domainLayout.getFileContent(
                                domainLayout.getDomainConfigFilePath()
                    )
                    domainDescriptor = parser.parseConfiguration(domainDescriptorFile.content)
                except (Exception, JException):
                    logger.warnException("Failed to process config.xml")
                    continue

                # get version of the platform
                # this covers descriptor XML structure for two major versions
                versionInfo = domainDescriptor.versionInfo
                logger.info("Platform version is %s" % versionInfo)

                # create corresponding platform trait, based on version information
                platformTrait = jee_discoverer.getPlatformTrait(versionInfo, platform, fallbackVersion = 9)

                # create runtime instances for each process
                runtimes = map(curry(weblogic_discoverer.createServerRuntimeByProcess,
                                     _, destinationIp
                               ),
                               processes
                )
                # create catalog of runtimes by server names
                # it will be used to get more data overridden by command line
                runtimeByServerName = applyMapping(weblogic_discoverer.ServerRuntime.findServerName, runtimes)

                # processes of managed servers may contain URI to the administrative
                # server, gathering such information may help in further server
                # address resovling

                # get unique endpoints of the administrative server for current domain
                adminServerEndpointByName = applyMapping(str,
                    filter(None, map(weblogic_discoverer.ServerRuntime.findAdminServerEndpoint, runtimes))
                )
                # find unique endpoints

                if len(adminServerEndpointByName) > 1:
                    logger.info("""After analyze of running processes in single domain \
                    found servers pointing to different administrative servers
                    """)
                    logger.debug(adminServerEndpointByName)
                adminServerEndpoint = (len(adminServerEndpointByName) == 1 and
                                        adminServerEndpointByName.values()[0])

                domain = jee.Domain(domainDescriptor.getName())
                # add known clusters to the domain
                clusters = domainDescriptor.getClusters()
                each(domain.addCluster, clusters)
                # add config.xml to the domain
                domain.addConfigFiles(jee.createXmlConfigFile(domainDescriptorFile))

                deploymentTargetByName = applyMapping(jee.Cluster.getName, clusters)

                machineByName = applyMapping(
                    weblogic_discoverer.DomainConfigurationDescriptor.Machine.getName,
                    domainDescriptor.getMachines()
                )

                # find corresponding process for each discovered server to
                # provide additional information that can be overridden
                resolvedServers = []
                servers = domainDescriptor.getServers()
                # for 7-8 versions config.xml hasn't info about admin-server at all
                # try to get 1st server created by config wizard to set is as admin:
                if platformTrait.majorVersion.value() < 9:
                    try:
                        startScriptXmlFile = domainLayout.getFileContent(domainLayout.getStartupScriptPath())
                        adminServerName = parser.parseAdminServerNameInStartupScriptContent(startScriptXmlFile.content)
                    except (Exception, JException):
                        logger.warnException("Failed to process startscript.xml to get admin-server")
                    else:
                        adminServer = fptools.findFirst((lambda x: x.getName() == adminServerName), servers)
                        adminServer and adminServer.addRole(jee.AdminServerRole())
                # if domain has just one server it is stand-alone server,
                # it has admin-console and is administrative server for itself
                if len(servers) == 1 and not servers[0].hasRole(jee.AdminServerRole):
                    servers[0].addRole(jee.AdminServerRole())
                for server in servers:
                    logger.debug("Server %s" % server)
                    # find server with name present between running processes
                    runtime = runtimeByServerName.get(server.getName())
                    serverRole = server.getRole(weblogic.ServerRole)
                    if runtime:
                        # such data can be overridden
                        # - LISTEN ADDRESS
                        #     - -Dweblogic.ListenAddress=host
                        server.address = server.address or destinationIp
                        server.ip.set(destinationIp)
                        logger.debug("Found server runtime update address with destination IP")
                        # - LISTEN PORT
                        #     - -Dweblogic.ListenPort=8001
                        # serverRole.setPort(runtime.findListenPort())
                        # - SSL LISTEN PORT
                        #     - -Dweblogic.ssl.ListenPort=\d+
                        # - CLUSTER MULTICAST ADDRESS
                        #     - -Dweblogic.cluster.multicastAddress

                    elif (adminServerEndpoint
                          # target server is administrative
                          and server.hasRole(jee.AdminServerRole)
                          # listen address is unknown
                          and not server.address
                           # port has to be the same
                          and serverRole.getPort() == adminServerEndpoint.getPort()):
                        # case when administrative server resides on other host
                        # we can get its end point from running processes
                        logger.debug("Administrative server resides at %s" % adminServerEndpoint.getAddress())
                        server.address = adminServerEndpoint.getAddress()
                        server.getRole(jee.AdminServerRole).setPort(
                                adminServerEndpoint.getPort()
                        )

                    # find out address from the machine
                    if not server.address and machineByName.has_key(server.nodeName):
                        logger.debug("Address updated from node %s" % machineByName.get(server.nodeName).address)
                        server.address = machineByName.get(server.nodeName).address

                    # resolve server address
                    if server.address:
                        ips = safeFunc(dnsResolver.resolveIpsByHostname)(server.address)
                        ips and server.ip.set(ips[0])

                    # if server is administrative - set domain IP and add config.xml to server CI
                    if server.hasRole(jee.AdminServerRole):
                        server.addConfigFiles(jee.createXmlConfigFile(domainDescriptorFile))
                        if server.ip.value():
                            logger.debug("Domain updated with administrative IP")
                            domain.setIp(server.ip.value())

                    if server.ip.value():
                        logger.debug('Server is resolved')
                        resolvedServers.append(server)
                    else:
                        logger.debug('Server is not resolved')
                    logger.debug('-' * 30)

                deploymentTargetByName.update(applyMapping(jee.Server.getName, resolvedServers))
                # create reporters
                domainTopologyReporter = reporterCreator.getDomainReporter()
                jdbcDsTopologyReporter = reporterCreator.getJdbcDsReporter()
                jmsTopologyReporter = reporterCreator.getJmsDsReporter()
                applicationReporter = reporterCreator.getApplicationReporter()

                if platformTrait.majorVersion.value() < 9:
                    # Discover for versions up to 8

                    # if administrative server is not reachable for managed server
                    # managed will be started and working in MANAGED SERVER
                    # INDEPENDENCE mode, so its config is valid

                    node = jee.Node(domainDescriptor.getName())
                    each(node.addServer, resolvedServers)

                    domainVector = domainTopologyReporter.reportNodesInDomain(domain, node)
                    domainVector.addAll(domainTopologyReporter.reportClusters(domain, *domain.getClusters()))
                    sendVector = curry(doNothing, Framework, domainVector, _)

                    # process datasources and corresponding deployment targets
                    logger.info("Report JDBC datasources")
                    for datasourceConfiguration in domainDescriptor.getDatasourceConfigurations():
                        datasource = datasourceConfiguration.object
                        try:
                            for targetName in datasourceConfiguration.getTargetNames():
                                deploymentScope = deploymentTargetByName.get(targetName)
                                if deploymentScope:
                                    if not deploymentScope.getOsh():
                                        logger.warn("Failed to find built OSH of deployment deploymentScope for the %s by name %s" % (datasource, targetName))
                                    else:
                                        sendVector(jdbcDsTopologyReporter.reportDatasourcesWithDeployer(domain, deploymentScope, datasource))
                        except (Exception, JException), exc:
                            logger.warnException("Failed to determine targets by name %s" % datasource)

                    logger.info("Report JMS servers and related resources")
                    for jmsServerConfiguration in domainDescriptor.getJmsServerConfigurations():
                        jmsServer = jmsServerConfiguration.object
                        try:
                            for targetName in jmsServerConfiguration.getTargetNames():
                                deploymentScope = deploymentTargetByName.get(targetName)
                                if deploymentScope:
                                    if not deploymentScope.getOsh():
                                        logger.warn("Failed to find built OSH of deployment deploymentScope for the %s by name %s" % (datasource, targetName))
                                    else:
                                        sendVector(jmsTopologyReporter.reportJmsServer(domain, deploymentScope, jmsServer))
                        except (Exception, JException), exc:
                            logger.warnException("Failed to determine targets by name %s" % jmsServer)

                    logger.info("Report Applications")
                    # process applications and corresponding deployment targets
                    for applicationConfiguration in domainDescriptor.getApplicationConfigurations():
                        application = applicationConfiguration.object
                        try:
                            for targetName in applicationConfiguration.getTargetNames():
                                deploymentScope = deploymentTargetByName.get(targetName)
                                if deploymentScope:
                                    if not deploymentScope.getOsh():
                                        logger.warn("Failed to find built OSH of deployment deploymentScope for the %s by name %s" % (application, targetName))
                                    else:
                                        sendVector(applicationReporter.reportApplications(domain, deploymentScope, application))
                        except (Exception, JException), exc:
                            logger.warnException("Failed to determine targets for the %s" % application)
                else:
                    # make discovery for Weblogic starting from version 9
                    # Domain descriptor "config.xml" and related configurations always in updated state
                    # so we can use it to build whole domain topology process config.xml
                    # each server is a potential deployment deploymentScope
                    # if machines are not configured - use default node with domain name
                    nodeByName = {domain.getName() : jee.Node(domain.getName())}
                    for server in resolvedServers:
                        if server.nodeName:
                            # add server to its node
                            node = nodeByName.setdefault(server.nodeName, jee.Node(server.nodeName))
                            logger.debug("Found %s for the %s " % (server, node))
                            node.addServer(server)
                        else:
                            nodeByName.get(domain.getName()).addServer(server)

                    domainVector = ObjectStateHolderVector()
                    domainVector.addAll(domainTopologyReporter.reportNodesInDomain(domain, *nodeByName.values()))
                    domainVector.addAll(domainTopologyReporter.reportClusters(domain, *domain.getClusters()))
                    sendVector = curry(doNothing, Framework, domainVector, _)
                    # -- JDBC
                    datasourceByName = {}
                    for configuration in domainDescriptor.getJdbcSystemResourceConfigurations():
                        jdbcSystemResource = configuration.object
                        # find file by specified path, usually it is a relative path in configuration folder
                        descriptorFilePath  = jdbcSystemResource.descriptorFilePath
                        if descriptorFilePath is None:
                            logger.warn("JdbcSystemResourceConfiguration file path was None - ignoring")
                            continue
                        if not domainLayout.path().isAbsolute( descriptorFilePath ):
                            descriptorFilePath = domainLayout.path().join(domainLayout.getDomainConfigDirPath(), descriptorFilePath)
                        try:
                            logger.debug('Parsing JDBC descriptor file for %s' % jdbcSystemResource)
                            descriptorFile = domainLayout.getFileContent(descriptorFilePath)
                            # add domain scope jdbc config files to domain
                            domain.addConfigFiles(jee.createXmlConfigFile(descriptorFile))
                            datasource = parser.parseJdbcResourceDescriptor(descriptorFile.content)
                        except (Exception, JException):
                            logger.warnException("Failed to process JDBC descriptor file for %s" % jdbcSystemResource)
                        else:
                            # process deployment targets
                            try:
                                for targetName in configuration.getTargetNames():
                                    deploymentScope = deploymentTargetByName.get(targetName)
                                    if deploymentScope:
                                        if not deploymentScope.getOsh():
                                            logger.warn("Failed to find built OSH of deployment deploymentScope for the %s by name %s" % (datasource, targetName))
                                        else:
                                            sendVector(jdbcDsTopologyReporter.reportDatasourcesWithDeployer(domain, deploymentScope, datasource))
                                            datasourceByName[datasource.getName()] = datasource
                            except (Exception, JException), exc:
                                logger.warnException("Failed to determine targets by name %s" % jdbcSystemResource)
                    # -- JMS servers
                    jmsServerByName = {}
                    for configuration in domainDescriptor.getJmsServerConfigurations():
                        jmsServer = configuration.object
                        jmsServerByName[jmsServer.getName()] = jmsServer
                        vector = jmsTopologyReporter.reportJmsServer(domain, None, jmsServer)
                        # depend link between jdbc store and datasource
                        store = jmsServer.store
                        if store and isinstance(store, jms.JdbcStore) and store.datasourceName:
                            datasource = datasourceByName.get(store.datasourceName)
                            if datasource and datasource.getOsh():
                                vector.addAll(jmsTopologyReporter.reportStoreDependencyOnDatasource(domain, jmsServer.store, datasource))
                        # process deployment targets
                        try:
                            for targetName in configuration.getTargetNames():
                                deploymentScope = deploymentTargetByName.get(targetName)
                                if deploymentScope:
                                    if not deploymentScope.getOsh():
                                        logger.warn("Failed to find built OSH of deployment deployment scope for the %s by name %s" % (jmsServer, targetName))
                                    else:
                                        vector.addAll(jmsTopologyReporter.reportJmsServer(domain, deploymentScope, jmsServer))
                        except (Exception, JException), exc:
                            logger.warnException("Failed to determine targets by name %s" % jdbcSystemResource)
                        sendVector(vector)

                    # -- JMS destinations
                    for configuration in domainDescriptor.getJmsSystemResourceConfigurations():
                        jmsSystemResource = configuration.object
                        # find file by specified path, usually it is a relative path in configuration folder
                        descriptorFilePath  = jmsSystemResource.descriptorFilePath
                        if not domainLayout.path().isAbsolute( descriptorFilePath ):
                            descriptorFilePath = domainLayout.path().join(domainLayout.getDomainConfigDirPath(), descriptorFilePath)
                        try:
                            logger.debug('Parsing JMS descriptor file for %s' % jmsSystemResource)
                            descriptorFile = domainLayout.getFileContent(descriptorFilePath)
                            # add domain scope jms config files to domain
                            domain.addConfigFiles(jee.createXmlConfigFile(descriptorFile))
                            destinationConfigurations = parser.parseJmsResourceDescriptor(descriptorFile.content)
                        except (Exception, JException):
                            logger.warnException("Failed to process JMS descriptor file for %s" % jmsSystemResource)
                        else:
                            destinations = []
                            for jmsDestinationConfiguration in destinationConfigurations:
                                destination = jmsDestinationConfiguration.object
                                targetNames = jmsDestinationConfiguration.getTargetNames()
                                if targetNames:
                                    # there is only one deploymentScope name - one JMS server
                                    destination.server = jmsServerByName.get(targetNames[0])
                                destinations.append(destination)
                            # process deployment targets
                            vector = ObjectStateHolderVector()
                            try:
                                for targetName in configuration.getTargetNames():
                                    deploymentScope = deploymentTargetByName.get(targetName)
                                    if deploymentScope:
                                        if not deploymentScope.getOsh():
                                            logger.warn("Failed to find built OSH of deployment deploymentScope for the %s by name %s" % (jmsSystemResource, targetName))
                                        else:
                                            vector.addAll(jmsTopologyReporter.reportResources(domain, deploymentScope, *destinations))
                            except (Exception, JException):
                                logger.warnException("Failed to determine targets by name %s" % jdbcSystemResource)
                            sendVector(vector)
                    # report jdbc & jms configuration document
                    sendVector(domainTopologyReporter.reportDomain(domain))

                    # -- Applications
                    # deployment plan defined in domain config.xml
                    deploymentPlanParser = asm_weblogic_discoverer.WeblogicApplicationDeploymentPlanParser(loadExternalDtd)
                    deploymentPlanDescriptor = deploymentPlanParser.parseApplicationDeploymentPlanParser(domainDescriptorFile.content)
                    # parse application.xml
                    descriptorParser = asm_weblogic_discoverer.WeblogicApplicationDescriptorParser(loadExternalDtd)
                    appLayout = jee_discoverer.ApplicationLayout(fs)
                    applicationDiscoverer = asm_weblogic_discoverer.WeblogicApplicationDiscovererByShell(
                        shell, appLayout, descriptorParser)
                    for configuration in domainDescriptor.getApplicationConfigurations():
                        application = configuration.object
                        applicationWithModules = None

                        if isinstance(application, jee.EarApplication):
                            applicationWithModules = applicationDiscoverer.discoverEarApplication(
                                                  application.getName(), application.fullPath)
                        elif isinstance(application, jee.WarApplication):
                            applicationWithModules = applicationDiscoverer.discoverWarApplication(
                                                  application.getName(), application.fullPath)
                        if applicationWithModules:
                            application = applicationWithModules
                        # add config deployment plan xml
                        deploymentPlan = deploymentPlanDescriptor.get(application.getName())
                        if deploymentPlan:
                            logger.debug('Parsing deployment plan content %s for %s' % (deploymentPlan, application))
                            deploymentPlan = domainLayout.path().normalizePath(deploymentPlan)
                            try:
                                deploymentPlanContent = domainLayout.getFileContent(deploymentPlan)
                                deploymentPlanFile = jee.createXmlConfigFile(deploymentPlanContent)
                                if not application.getConfigFile(deploymentPlanFile.getName()):
                                    application.addConfigFiles(deploymentPlanFile)
                                    asm_weblogic_discoverer.WebLogicDeploymentPlanImplementer(application, deploymentPlanFile).implement()
                            except (Exception, JException):
                                logger.warnException(
                                    "Failed to load deployment plan content for %s descriptor: %s" % (
                                    application, deploymentPlan))
                        try:
                            for targetName in configuration.getTargetNames():
                                deploymentScope = deploymentTargetByName.get(targetName)
                                if isinstance(deploymentScope, jee.Cluster):
                                    logger.debug("The target is cluster, create relationship between application and servers.")
                                    for server in resolvedServers:
                                        clusterMemberRoles = server.getRolesByBase(jee.ClusterMemberServerRole)
                                        if clusterMemberRoles and clusterMemberRoles[0].clusterName:
                                            if clusterMemberRoles[0].clusterName == targetName:
                                                sendVector(applicationReporter.reportApplications(domain, server, application))

                                if deploymentScope:
                                    if not deploymentScope.getOsh():
                                        logger.warn("Failed to find built OSH of deployment deploymentScope for the %s by name %s" % (application, targetName))
                                    else:
                                        sendVector(applicationReporter.reportApplications(domain, deploymentScope, application))
                        except (Exception, JException):
                            logger.warnException("Failed to determine targets by name %s" % application)

                        # -- App Scoped JMS destinations
                        files = filter(lambda f: re.match('.*-jms\\.xml$', f.getName(), re.IGNORECASE), application.getConfigFiles())
                        if files:
                            try:
                                logger.debug('Parsing App Scoped JMS descriptor file %s for application %s' % (files[0].name, application))
                                appScopedJmsConfigParser = asm_weblogic_discoverer.AppScopedJmsConfigParser(loadExternalDtd)
                                appScopedJmsConfigs = appScopedJmsConfigParser.parseJmsResourceDescriptor(files[0].content)
                            except:
                                logger.warnException(
                                    "Failed to process App Scoped JMS descriptor file %s for application %s" % (files[0].name, application))
                            else:
                                destinations = []
                                for jmsDestinationConfig in appScopedJmsConfigs:
                                    destination = jmsDestinationConfig.object
                                    targetNames = jmsDestinationConfig.getTargetNames()
                                    if targetNames:
                                        # there is only one deploymentScope name - one JMS server
                                        destination.server = jmsServerByName.get(targetNames[0])
                                    destinations.append(destination)
                                # process deployment targets
                                vector = ObjectStateHolderVector()
                                try:
                                    for targetName in configuration.getTargetNames():
                                        deploymentScope = deploymentTargetByName.get(targetName)
                                        if deploymentScope:
                                            if not deploymentScope.getOsh():
                                                logger.warn("Failed to find built OSH of deployment deploymentScope for application %s by name %s" % (application, targetName))
                                            else:
                                                vector.addAll(jmsTopologyReporter.reportResources(domain, deploymentScope, *destinations))
                                except (Exception, JException):
                                    logger.warnException("Failed to determine targets by name %s" % targetName)
                                sendVector(vector)

                asm_dependency_resolver.resolveDependency(scpType, scpContext, reporterCreator, applicationResult, OSHVResult)
            except (Exception, JException), exc:
                logger.warnException("Failed to discover server topology")
                jee_connection.reportError(Framework, str(exc), platform.getName())

def doNothing(Framework, domainVector, vector):
    pass