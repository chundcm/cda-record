#coding=utf-8
#=== Websphere discovery by Shell based on running processes ===

# Main idea of this discovery is to find Websphere running processes related
# domain topology and resources/applications with corresponding linkage.


import logger
from appilog.common.system.types.vectors import ObjectStateHolderVector
from java.lang import Exception as JException
import jee_connection
import jee
import shellutils
import process_discoverer
import websphere_discoverer
from com.hp.ucmdb.discovery.library.communication.downloader.cfgfiles import GeneralSettingsConfigFile
import jee_discoverer
import file_system
import file_topology
import websphere
import netutils
import re
import process as process_module

from fptools import groupby, applyMapping, applySet, findFirst,\
                    curry, _, asIs, applyReverseMapping

__all__ = ['DiscoveryMain']


def DiscoveryMain(Framework):
    Framework = jee_connection.EnhancedFramework(Framework)
    platform = jee.Platform.WEBSPHERE
    domainName = Framework.getDestinationAttribute('domainName')
    shell = None
    try:
        try:
            # ======================= Establish connection =====================
            client = Framework.createClient()
            shell = shellutils.ShellFactory().createShell(client)
            # create FS
            fs = websphere_discoverer._createFileSystemRecursiveSearchEnabled(file_system.createFileSystem(shell))
            pathUtil = file_system.getPath(fs)
        except (Exception, JException), exc:
            logger.warnException(str(exc))
            jee_connection.reportError(Framework, str(exc), platform.getName())
        else:
            loadExternalDtd = websphere_discoverer.isLoadExternalDtdEnabled()
            # Parser used for configuration files parsing
            parser = websphere_discoverer.DescriptorParser(loadExternalDtd)
            # For the DNS resolving Java resolver will be used
            dnsResolver = jee_discoverer.DnsResolverDecorator(
                                netutils.JavaDnsResolver(), client.getIpAddress(),domainName
            )

            # To abstract from reporting topologies (old/enhanced) reporter creator is used
            reporterCreator = jee_discoverer.createTopologyReporterFactory(
                                  websphere.ServerTopologyBuilder(),
                                  dnsResolver
            )
            r'''
            Discovery plan
            1) group processes by profile path, specified as first parameter to java class
            1.1) find platform version using -Dinstall.root.dir obligatory system parameter for each runtime
            2) For each profile we have to determine deployment Type
                2.0) go through runtime nodes and discover running servers
                    a) every running server has jvm discovered
                2.2) If deployment type is Distributed
                    2.3) determine administrative server
            '''
            r'''0) '''
            # First step is to determine running server by finding
            # launched processes. Websphere processes are identified by substring
            # in command line 'com.ibm.ws.runtime.WsServer'.
            # Server class which has 4 parameters:
            # * <CONFIG_DIR> path to the profile configuration
            # * <CELL_NAME>  name of the Cell to which running server belongs
            # * <NODE_NAME>  name of the Node to which running server belongs
            # * <SERVER_NAME> name of the server
            processDiscoverer = process_discoverer.getDiscovererByShell(shell)
            argumentPattern = 'com.ibm.ws.runtime.WsServer'
            processes = processDiscoverer.discoverProcessesByCommandLinePattern(argumentPattern)

            # On HP-UX, the result of ps command may be truncated, so if we get nothing here, we need to do more.
            if not len(processes) and shell.getOsType() == 'HP-UX' and isCaliperAllowed():
                logger.info("Found no matched result with argument pattern on HP-UX. The argument might be truncated, try command path pattern." )
                candidateProcesses = processDiscoverer.discoverProcessesByCommandLinePattern(r'IBM/WebSphere/AppServer/java/bin')

                if len(candidateProcesses):
                    logger.info("Found %s candidate processes. Use caliper to get full commandline." % len(candidateProcesses))
                    for candidateProcess in candidateProcesses:
                        try:
                            enrichedProcess = enrichProcessByCaliper(shell, candidateProcess)
                        except (Exception, JException):
                            logger.warnException("Failed to run caliper on process %s to get full commandline." % candidateProcess.getPid())
                            continue
                        if enrichedProcess and str(enrichedProcess.commandLine).find(argumentPattern) != -1:
                            processes.append(enrichedProcess)

            # On Linux, the result of ps command may be truncated, so those truncated command which does not meet given regex will be filtered
            if len(processes) and shell.getOsType() == "Linux":
                filteredProcessCount = 0
                verificationPattern = 'com\.ibm\.ws\.runtime\.WsServer\s+"?([^"|^\s]*)"?\s+([^\s]*)\s+([^\s]*)\s+([^\s]*)\s*'
                for process in processes:
                    cmdLine = process and str(process.commandLine)
                    m = re.search(verificationPattern, cmdLine)
                    if not m:
                        processes.remove(process)
                        filteredProcessCount += 1
                if filteredProcessCount > 0:
                    logger.warn("There are %s Websphere process filtered due to incomplete/truncated ps ooutput" % filteredProcessCount)

            logger.info("Found %s Websphere processes" % len(processes))

            #discover tnsnames.ora file
            logger.debug("try to find tnsnames.ora file")
            hostId = Framework.getDestinationAttribute('hostId')
            Framework.sendObjects(jee_discoverer.discoverTnsnamesOra(hostId, client))

            # In case if there is not processes found - discovery stops with
            # warning message to the UI
            if not processes:
                logger.reportWarning("No Websphere processes currently running")
                return ObjectStateHolderVector()
            r'''1)'''
            runtimes = map(createRuntime, processes)
            # group runtimes of processes by configuration directory path
            runtimesByConfigDirPath = groupby(
                        websphere_discoverer.ServerRuntime.getConfigDirPath,
                        runtimes
            )
            debugGroupping(runtimesByConfigDirPath)

            # find out platform version for each runtime where several runtimes
            # may use the same binary installation placed in so called 'isntall root directory'
            # so to reduce FS calls for the same root directory we will group
            # runtimes by this path
            installRootDirPaths = applySet(
                    websphere_discoverer.ServerRuntime.findInstallRootDirPath,
                    runtimes
            )
            # for install root directory get platform version
            productInfoParser = websphere_discoverer.ProductInfoParser(loadExternalDtd)
            productInfoByInstallDirPath = applyReverseMapping(
                                   curry(determineVersion, _, productInfoParser, fs),
                                   installRootDirPaths)
            r'''2)'''
            for configDirPath, runtimes in sorted(runtimesByConfigDirPath.items()):
                logger.info("=" * 30)
                logger.info("Profile %s"  % configDirPath )
                logger.info("Determine cell type (standalone|distributed)")
                # expected to see the same cell name for all runtimes in scope
                # of configDirPath
                runtimesByCellName = groupby(websphere_discoverer.ServerRuntime.getCellName,
                                             runtimes)
                debugGroupping(runtimesByCellName)
                if len(runtimesByCellName) > 1:
                    logger.warn("Configuration where more than one cell in one profile is not supported")
                    continue
                # parse cell configuration and get deployment type
                profileHomeDirPath = pathUtil.dirName(configDirPath)
                profileLayout = websphere_discoverer.ProfileLayout(
                                   profileHomeDirPath, fs)
                cellName = runtimesByCellName.keys()[0]
                cellLayout = websphere_discoverer.CellLayout(
                                    profileLayout.composeCellHomePath(cellName), fs)
                cellConfigFile = cellLayout.getFileContent(cellLayout.getConfigFilePath() )
                cell = parser.parseCellConfig(cellConfigFile.content)

                securityConfigFile = cellLayout.getFile(cellLayout.getSecurityConfigFilePath())
                cell.addConfigFile(jee.createXmlConfigFile(securityConfigFile))

                cellConfigFileToReport = cellLayout.getFile(cellLayout.getConfigFilePath())
                cell.addConfigFile(jee.createXmlConfigFile(cellConfigFileToReport))

                cellResourceConfigFile = cellLayout.getFile(cellLayout.getResourcesConfigFilePath())
                cell.addConfigFile(jee.createXmlConfigFile(cellResourceConfigFile))

                NameBindingContent = None
                try:
                    NameBindingConfigFile = cellLayout.getFile(cellLayout.getNameBindingConfigFile())
                    cell.addConfigFile(jee.createXmlConfigFile(NameBindingConfigFile))
                    NameBindingContent = cellLayout.getFileContent(cellLayout.getNameBindingConfigFile())
                except:
                    logger.debug('Cannot find namebindings.xml for cell: ', cell)

                logger.info("Found %s deployment" %
                            (cell.isDistributed() and 'DISTRIBUTED'
                             or 'STANDALONE')
                )

                r'''2.0) Discover information about nodes where servers are in runtime'''
                logger.info("Group running servers by node name")
                runtimesByNodeName = groupby(websphere_discoverer.ServerRuntime.getNodeName,
                                             runtimes)
                debugGroupping(runtimesByNodeName)

                # remember administrative server if found
                administrativeServer = None
                for nodeName, nodeRuntimes in runtimesByNodeName.items():
                    logger.info("Discover node: %s" % nodeName)
                    nodeLayout = websphere_discoverer.NodeLayout(
                                    cellLayout.composeNodeHomePath(nodeName), fs)
                    node = websphere_discoverer.discoverNode(nodeLayout, pathUtil)
                    cell.addNode(node)
                    logger.info("Discover servers")
                    servers = parser.parseServersInServerIndex(
                        nodeLayout.getFileContent(nodeLayout.getServerIndexPath()).content
                    )
                    nodeRuntimesByServerName = applyMapping(
                            websphere_discoverer.ServerRuntime.getServerName,
                            nodeRuntimes
                    )
                    # add to the node only running servers that match their runtime
                    for server in servers:
                        serverRuntime = nodeRuntimesByServerName.get(server.getName())
                        if serverRuntime or server.hasRole(jee.AdminServerRole):
                            logger.info("\tResolved running %s" % server)
                            server.nodeName = nodeName
                            node.addServer(server)
                            if server.hasRole(jee.AdminServerRole):
                                administrativeServer = server
                                logger.info("\tAdministrative server found")
                            if serverRuntime:
                                # assign platform version
                                productInfo = productInfoByInstallDirPath.get(
                                                serverRuntime.findInstallRootDirPath())
                                server.version = productInfo and productInfo.version
                                server.versionDescription = productInfo and ', '.join((productInfo.name, productInfo.version))
                                # make JVM discovery if runtime present
                                server.jvm = (websphere_discoverer.JvmDiscovererByShell(shell, None).
                                  discoverJvmByServerRuntime(serverRuntime)
                                )

                r'''3)'''
                # for distributed type of deployment we have to know the administrative address
                # as Cell (domain) is spread among profiles on different destinations
                # so for further merge administrative server has to be discovered
                if cell.isDistributed() and not administrativeServer:
                    # go through nodes which are not represented by some runtime
                    # and find administrative server

                    # very rare case when administrative server cannot be found
                    logger.info("Find administrative server in non-visited nodes")
                    nodes = discoverNodes(cellLayout, pathUtil)
                    nodes = filter(curry(isNodeNotInRuntimeGroup, _, runtimesByNodeName),
                                   nodes)
                    # first of all process nodes where 'manager' substring appears
                    # often DMGR nodes has name like 'blahCellManager01'
                    nodes.sort(lambda x, y: x.getName().lower().find('manager') < y.getName().lower().find('manager'))
                    # sort nodes by 'manager' substring presence
                    for node in nodes:
                        logger.info("Visit %s" % node)
                        # find administrative servers only
                        nodeLayout = websphere_discoverer.NodeLayout(
                                        cellLayout.composeNodeHomePath(node.getName()),
                                        fs
                        )
                        adminServers = filter(lambda s: s.hasRole(jee.AdminServerRole),
                                parser.parseServersInServerIndex(
                                   nodeLayout.getFileContent(nodeLayout.getServerIndexPath()).content
                                )
                        )
                        if adminServers:
                            logger.info("Found administrative %s" % adminServers)
                            administrativeServer = adminServers[0]
                            node.addServer(administrativeServer)
                            cell.addNode(node)
                            break
                        else:
                            logger.info("Administrative server not found")

                    if not administrativeServer:
                        logger.warn("Failed to find administrative server for the domain. Domain topology wont'be reported")
                        continue

                # make discovery of clusters
                serverByFullName = websphere_discoverer.groupServersByFullNameInCell(cell)
                for cluster, members in websphere_discoverer.discoverClusters(cellLayout, fs, parser):
                    logger.info("Discovered %s" % cluster)
                    cell.addCluster(cluster)
                    # process cluster members
                    clusterName = cluster.getName()
                    for member in filter(None,
                        map(curry(websphere_discoverer.getClusterMemberFromRuntimeGroup, _, serverByFullName),
                            members
                        )
                    ):
                        logger.info("\tServer(fullName = %s) is cluster member" % member.getFullName())
                        member.addRole(jee.ClusterMemberServerRole(clusterName))

                # report domain topology (servers, clusters, nodes, servers)
                domainTopologyReporter = reporterCreator.getDomainReporter()
                domainVector = domainTopologyReporter.reportNodesInDomainDnsEnabled(cell, dnsResolver, *cell.getNodes())
                domainVector.addAll(domainTopologyReporter.reportClusters(cell, *cell.getClusters()))

                # determine whether we have at least one server with resolved IP address. Stop report if we haven't.
                if not findFirst(lambda srvr: srvr.ip.value(), serverByFullName.values()):
                    logger.warn("%s and related topology won't be reported \
as there is no at least one server with resolved IP address" % cell)
                    continue
                _domainVector = domainVector.deepClone()
                websphere_discoverer._sendVectorImmediately(Framework, domainVector, forceVectorClean = 0)

                sendVectorWithDomain = curry(websphere_discoverer.sendTopologyWithDomainVector,
                                            Framework, _, _domainVector
                )
                # discover resources
                jndiNamedResourceManager = websphere_discoverer.discoverResourcesInDomain(
                                cell, cellLayout, fs, parser,
                                reporterCreator, sendVectorWithDomain
                )

                # discover applications
                websphere_discoverer.discoverApplicationsInDomain(cell, cellLayout, fs, shell, parser,
                        reporterCreator, jndiNamedResourceManager,
                        sendVectorWithDomain, NameBindingContent)

        if not Framework.getSentObjectsCount():
            logger.reportWarning("%s: No data collected" % platform.getName())
    finally:
        try:
            shell and shell.closeClient()
        except:
            logger.debugException('')
            logger.error('Unable to close shell')
    return ObjectStateHolderVector()

def isNodeNotInRuntimeGroup(node, group):
    r''' Determine whether node is present in group where its name set as a key
    @types: jee.Node, dict[str, ?] -> bool
    '''
    return not group.has_key(node.getName())

# def getClusterMemberFromRuntimeGroup(member, serverByFullName):
#     r''' Determine whether member is present in group where its full name set as a key
#     @types: jee.Server, dict[str, jee.Server] -> jee.Server
#     '''
#     return serverByFullName.get(member.getFullName())

# def discoverClusters(cellLayout, fs, parser):
#     r''' Discover Clusters in specified <cell>
#     recursive - list node.xml in cellHomePath/nodes/*/
#     @types: CellLayout, file_system.FileSystem, websphere_discoverer.DescriptorParser -> list[Tuple[Cluster, list[jee.Server]]]
#     '''
#     clusterInfoList = []
#     for clusterRootPath in cellLayout.findClusterRootPaths():
#         try:
#             # recursive - lsit cluster.xml in cellHomePath/[clusters/*/]
#             clusterLayout = websphere_discoverer.ClusterLayout(clusterRootPath, fs)
#             clusterConfig = parser.parseClusterConfig(
#                                 clusterLayout.getFileContent(
#                                     clusterLayout.getConfigFilePath()
#                                 ).content
#                             )
#             clusterInfoList.append((clusterConfig.cluster, clusterConfig.getMembers()))
#         except Exception:
#             logger.warnException("Failed to process cluster configuration")
#     return clusterInfoList

def discoverNodes(cellLayout, pathUtil):
    r''' Discover Nodes in specified <cellLayout>
    recursive - list node.xml in cellHomePath/nodes/*/
    @types: websphere_discoverer.CellLayout, file_topology.Path -> list[jee.Node]
    '''
    nodes = []
    for nodePath in cellLayout.findNodeRootPaths():
        nodes.append(jee.Node(pathUtil.baseName(nodePath)))
    return nodes


# =========== File system wrapper
class FileFilterByPattern(file_system.FileFilter):
    def __init__(self, pattern, acceptFunction):
        r'''@types: str, callable(file)
        @raise ValueError: File pattern is not specified
        @raise ValueError: Accept function for the file filter is not specified
        '''
        if not pattern:
            raise ValueError("File pattern is not specified")
        if not callable(acceptFunction):
            raise ValueError("Accept function for the file filter is not specified")
        self.filePattern = pattern
        self.accept = acceptFunction


def determineVersion(installRootDirPath, parser, fs):
    r'''@types: str, websphere_discoverer.ProductInfoParser, file_system.FileSystem -> websphere_discoverer.ProductInformation or None
    @resource-file:ND.product
    @resource-file:BASE.product
    @resource-file:WAS.product

    First check this xml files
        <installRootDirPath>/properties\version\ND.product - for Network Deployment
        <installRootDirPath>/properties\version\BASE.product - for Base stand-alone
        <installRootDirPath>/properties\version\BASE.product + ND.product - for Base federated to ND
        <installRootDirPath>/properties\version\WAS.product

    '''
    pathUtil = file_system.getPath(fs)
    if installRootDirPath:
        propertiesDirPath = pathUtil.join(installRootDirPath, 'properties', 'version')
    productInformation = None
    try:
        files = fs.getFiles(propertiesDirPath, filters = [file_system.ExtensionsFilter(['product'])],
                                           fileAttrs = [file_topology.FileAttrs.NAME,
                                                        file_topology.FileAttrs.PATH])
    except (Exception, JException):
        logger.warnException("Failed to determine platform version as failed to get product files in specified root path")
    else:
        for productFile in files:
            fileName = str(productFile.name).lower()
            # filter the file like ibmjava*.product and java*.product to avoid reporting Java product version as WAS version
            if re.search('java', fileName):
                continue
            try:
                file = fs.getFile(productFile.path, [file_topology.FileAttrs.NAME,
                                                    file_topology.FileAttrs.PATH,
                                                    file_topology.FileAttrs.CONTENT
                                                     ])
            except (Exception, JException):
                logger.warnException("Failed to get product file")
            else:
                productInformation = parser.parseProductConfig(file.content)
                break
    # if version is not determined, so we will guess it based on the presence
    # of 'profiles' directory in the installation root directory
    if productInformation is None:
        pathUtil = file_system.getPath(fs)
        productName = 'IBM WebSphere Application Server'
        try:
            pathUtil.join(installRootDirPath, 'profiles')
        except (Exception, JException):
            logger.warnException('Failed to find profiles directory in the install root directory.')
            logger.info("Profiles directory appeared starting from the 6th version, so we make an attempt to discover WebSphere 5th")
            productInformation = websphere_discoverer.ProductInformation(productName, version = '5.x')
        else:
            logger.info("Profiles directory appeared starting from the 6th version, so we make an attempt to discovery WebSphere 6th")
            productInformation = websphere_discoverer.ProductInformation(productName, version = '6.x')
    return productInformation

def getInstallRootDirFromProfilePath(runtime, pathUtil):
    r'@types: websphere_discoverer.ServerRuntime, file_topology.Path -> str'
    configDirPath = runtime.getConfigDirPath()
    rootInstallDirPath = pathUtil.baseDir(pathUtil.baseDir(pathUtil.baseDir(configDirPath)))
    logger.debug("Used profile path (%s) to get root install directory path (%s)"
                 % (configDirPath, rootInstallDirPath))
    return rootInstallDirPath

def createRuntime(process):
    r'@types: process.Process -> websphere_discoverer.ServerRuntime'
    return websphere_discoverer.ServerRuntime(process.commandLine)

def debugGroupping(groups):
    r'@types: dict[obj, list[obj]]'
    logger.debug('-' * 30)
    for k, v in groups.items():
        logger.debug(str(k))
        for i in v:
            logger.debug('\t' + str(i))
    logger.debug('-' * 30)

def enrichProcessByCaliper(shell, process):
    pid = process.getPid()
    fullCommandline = None

    if pid:
        fullCommandline = shell.execCmd('/opt/caliper/bin/caliper fprof --process=root --attach %s --duration 1 | grep Invocation:' % pid)
        if fullCommandline:
            matcher = re.match(r'Invocation:\s+(.*)', fullCommandline)
            if matcher:
                fullCommandline = matcher.group(1)
            else:
                logger.info("Caliper's result does not match expected pattern on process %s." % pid)
                return None
        else:
            logger.info("Caliper returns nothing on process %s." % pid)
            return None
    else:
        logger.info("Failed to get pid on the given process.")
        return None

    if not fullCommandline:
        logger.info("Matched commandline is empty")
        return None

    argumentsLine = None

    tokens = re.split(r"\s+", fullCommandline, 1)
    fullCommand = tokens[0]
    if len(tokens) > 1:
        argumentsLine = tokens[1]

    commandName = fullCommand
    commandPath = None

    if not re.match(r"\[", fullCommand):
        matcher = re.match(r"(.*/)([^/]+)$", fullCommand)
        if matcher:
            commandPath = fullCommand
            commandName = matcher.group(2)

    enrichedProcess = process_module.Process(commandName, pid, fullCommandline)
    enrichedProcess.owner = process.owner
    enrichedProcess.argumentLine = argumentsLine
    enrichedProcess.executablePath = commandPath

    return enrichedProcess

def isCaliperAllowed():
    globalSettings = GeneralSettingsConfigFile.getInstance()
    return globalSettings.getPropertyBooleanValue('allowCaliperOnHPUX', False)