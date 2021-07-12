from appilog.common.system.types.vectors import ObjectStateHolderVector
from com.hp.ucmdb.discovery.library.communication.downloader.cfgfiles import GeneralSettingsConfigFile
from java.lang import Exception as JException

import asm_dependency_resolver
import file_system
import fptools
import jee
import jee_connection
import jee_discoverer
import logger
import netutils
import websphere
import websphere_by_shell
import websphere_discoverer


def discover(Framework, shell, client, applicationResult, OSHVResult, hostOsh):
    logger.debug('Start to discover WebSphere application by SCP')

    scpType = Framework.getDestinationAttribute('service_connection_type')
    scpContext = Framework.getDestinationAttribute('service_context')

    try:
        fs = websphere_discoverer._createFileSystemRecursiveSearchEnabled(file_system.createFileSystem(shell))
        pathUtil = file_system.getPath(fs)
    except(Exception, JException), exc:
        logger.warnException(str(exc))
        jee_connection.reportError(Framework, str(exc), jee.Platform.WEBSPHERE.getName())
    else:
        globalSettings = GeneralSettingsConfigFile.getInstance()
        loadExternalDtd = globalSettings.getPropertyBooleanValue('loadExternalDtd', 0)
        parser = websphere_discoverer.DescriptorParser(loadExternalDtd)

        # For the DNS resolving Java resolver will be used
        dnsResolver = jee_discoverer.DnsResolverDecorator(
            netutils.JavaDnsResolver(), client.getIpAddress()
        )

        # To abstract from reporting topologies (old/enhanced) reporter creator is used
        reporterCreator = jee_discoverer.createTopologyReporterFactory(
            websphere.ServerTopologyBuilder(),
            dnsResolver
        )

        processes = applicationResult.application.getProcesses()
        runtimes = map(websphere_by_shell.createRuntime, processes)

        # find out platform version for each runtime where several runtimes
        # may use the same binary installation placed in so called 'install root directory'
        # so to reduce FS calls for the same root directory we will group
        # runtimes by this path
        installRootDirPaths = fptools.applySet(
            websphere_discoverer.ServerRuntime.findInstallRootDirPath,
            runtimes
        )
        # for install root directory get platform version
        productInfoParser = websphere_discoverer.ProductInfoParser(loadExternalDtd)
        productInfoByInstallDirPath = fptools.applyReverseMapping(
            fptools.curry(websphere_by_shell.determineVersion, fptools._, productInfoParser, fs),
            installRootDirPaths)

        # group runtimes of processes by configuration directory path
        runtimesByConfigDirPath = fptools.groupby(
            websphere_discoverer.ServerRuntime.getConfigDirPath,
            runtimes
        )
        websphere_by_shell.debugGroupping(runtimesByConfigDirPath)

        for configDirPath, runtimes in runtimesByConfigDirPath.items():
            if not configDirPath:
                continue

            logger.info("Group runtimes of processes by cell name")
            runtimesByCellName = fptools.groupby(websphere_discoverer.ServerRuntime.getCellName, runtimes)
            websphere_by_shell.debugGroupping(runtimesByCellName)

            if len(runtimesByCellName) > 1:
                logger.warn("Configuration where more than one cell in one profile is not supported")
                continue

            # parse cell configuration and get deployment type
            profileHomeDirPath = pathUtil.dirName(configDirPath)
            profileLayout = websphere_discoverer.ProfileLayout(profileHomeDirPath, fs)
            cellName = runtimesByCellName.keys()[0]
            cellLayout = websphere_discoverer.CellLayout(profileLayout.composeCellHomePath(cellName), fs)
            cellConfigFile = cellLayout.getFileContent(cellLayout.getConfigFilePath())
            cell = parser.parseCellConfig(cellConfigFile.content)

            NameBindingContent = None
            try:
                # NameBindingConfigFile = cellLayout.getFile(cellLayout.getNameBindingConfigFile())
                # cell.addConfigFile(jee.createXmlConfigFile(NameBindingConfigFile))
                NameBindingContent = cellLayout.getFileContent(cellLayout.getNameBindingConfigFile())
            except:
                logger.debug('Cannot find namebindings.xml for cell: ', cell)

            logger.info("Group runtimes of processes by node name")
            runtimesByNodeName = fptools.groupby(websphere_discoverer.ServerRuntime.getNodeName, runtimes)
            websphere_by_shell.debugGroupping(runtimesByNodeName)

            for nodeName, nodeRuntimes in runtimesByNodeName.items():
                logger.info("Discover node: %s" % nodeName)
                nodeLayout = websphere_discoverer.NodeLayout(cellLayout.composeNodeHomePath(nodeName), fs)
                node = websphere_discoverer.discoverNode(nodeLayout, pathUtil)
                cell.addNode(node)

                logger.info("Discover servers")
                servers = parser.parseServersInServerIndex(
                    nodeLayout.getFileContent(nodeLayout.getServerIndexPath()).content
                )
                nodeRuntimesByServerName = fptools.applyMapping(
                    websphere_discoverer.ServerRuntime.getServerName,
                    nodeRuntimes
                )

                # add to the node only running application servers that match their runtime
                for server in servers:
                    serverRuntime = nodeRuntimesByServerName.get(server.getName())
                    if serverRuntime and server.hasRole(jee.ApplicationServerRole):
                        logger.info("Resolved running application server %s" % server)
                        server.nodeName = nodeName
                        # assign platform version
                        productInfo = productInfoByInstallDirPath.get(serverRuntime.findInstallRootDirPath())
                        server.version = productInfo and productInfo.version
                        server.versionDescription = productInfo and ', '.join((productInfo.name, productInfo.version))
                        node.addServer(server)

            # make discovery of clusters
            serverByFullName = websphere_discoverer.groupServersByFullNameInCell(cell)
            for cluster, members in websphere_discoverer.discoverClusters(cellLayout, fs, parser):
                logger.info("Discovered %s" % cluster)
                cell.addCluster(cluster)
                # process cluster members
                clusterName = cluster.getName()
                for member in filter(None,
                                     map(fptools.curry(websphere_discoverer.getClusterMemberFromRuntimeGroup, fptools._, serverByFullName),
                                         members
                                         )
                                     ):
                    logger.info("\tServer(fullName = %s) is cluster member" % member.getFullName())
                    member.addRole(jee.ClusterMemberServerRole(clusterName))

            domainTopologyReporter = reporterCreator.getDomainReporter()
            domainVector = domainTopologyReporter.reportNodesInDomainDnsEnabled(cell, dnsResolver, *cell.getNodes())
            domainVector.addAll(domainTopologyReporter.reportClusters(cell, *cell.getClusters()))

            # doNotSendVector will NOT send vector at once, because we want to send the matched result later
            doNotSendVector = fptools.curry(doNothing, Framework, fptools._, domainVector)

            # discover resources only when SCP type is 'jms'; otherwise, discover applications
            if scpType == 'jms':
                websphere_discoverer.discoverResourcesInDomain(
                    cell, cellLayout, fs, parser,
                    reporterCreator, doNotSendVector
                )
            else:
                jndiNamedResourceManager = websphere_discoverer.JndiNamedResourceManager()
                # discover applications
                websphere_discoverer.discoverApplicationsInDomain(cell, cellLayout, fs, shell, parser,
                                             reporterCreator, jndiNamedResourceManager,
                                             doNotSendVector, NameBindingContent)

            # resolve dependencies
            asm_dependency_resolver.resolveDependency(scpType, scpContext, reporterCreator, applicationResult, OSHVResult)


def doNothing(Framework, vector, domainVector):
    pass
