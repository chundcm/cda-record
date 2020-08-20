#coding=utf-8
import sys
import re

import logger
import shellutils
import errormessages
import file_system
import netutils

from appilog.common.system.types.vectors import ObjectStateHolderVector
from java.lang import Exception as JException
from fptools import curry, _, findFirst

import websphere_discoverer
import jee
import jee_discoverer
import websphere

WIN_BIT_DEFAULT_PATH_1 = "C:\\Program Files (x86)\\IBM\\WebSphere"
WIN_BIT_DEFAULT_PATH_2 = "C:\\Program Files\\IBM\\WebSphere"
AIX_DEFAULT_PATH = "/usr/IBM/WebSphere"
HPUX_DEFAULT_PATH = "/opt/IBM/WebSphere"
LINUX_DEFAULT_PATH = "/opt/IBM/WebSphere"
SOLARIS_DEFAULT_PATH = "/opt/IBM/WebSphere"

def getWebSphereHomePath(shell, paraUnixInstallPath, paraWinInstallPath):
    winHomePathList = []
    unixHomePathList = []
    try:
        osType = shell.getOsType()
        logger.debug('OS Type: ', osType)
    except:
        raise ValueError('Can not get OS type.')
    if osType == 'Microsoft Windows':
        winHomePathList.append(WIN_BIT_DEFAULT_PATH_1)
        winHomePathList.append(WIN_BIT_DEFAULT_PATH_2)
    elif osType == 'AIX':
        unixHomePathList.append(AIX_DEFAULT_PATH)
    elif osType == 'Solaris':
        unixHomePathList.append(SOLARIS_DEFAULT_PATH)
    elif osType == 'HP-UX':
        unixHomePathList.append(HPUX_DEFAULT_PATH)
    elif osType == 'Linux':
        unixHomePathList.append(LINUX_DEFAULT_PATH)
    else:
        raise ValueError('Unsupported OS type.')
    if paraWinInstallPath:
        winHomePathList = winHomePathList + paraWinInstallPath.strip().split(',')
    if paraUnixInstallPath:
        unixHomePathList = unixHomePathList + paraUnixInstallPath.strip().split(',')
    return osType, winHomePathList, unixHomePathList


class ConfigLayout(jee_discoverer.Layout):
    def __init__(self, fs):
        '@types: file_system.FileSystem, str'
        jee_discoverer.Layout.__init__(self, fs)

    def composePath(self, *directory):
        '''@types: -> str'''
        return self.path().join(*directory)


class WebSphereManualDiscoverer:
    def __init__(self, shell, websphereHomePath, osType):
        self._shell = shell
        self._websphereHomePath = websphereHomePath
        self._osType = osType

    def execCmdByShell(self, cmd):
        try:
            return self._shell.execCmd(cmd, 240000)
        except:
            logger.warn('Can not exec the command: %s' % cmd)

    def getElementName(self, path):
        if self._osType == 'Microsoft Windows':
            cmd = 'dir /b "%s"' % path
        else:
            cmd = 'ls -1 %s' % path
        res = self.execCmdByShell(cmd)
        if res:
            return res.strip().split('\n')
        else:
            return None

    def getFileContent(self, fileFullPath):
        if self._osType == 'Microsoft Windows':
            cmd = 'type "%s"' % fileFullPath
        else:
            cmd = 'cat %s' % fileFullPath
        res = self.execCmdByShell(cmd)
        if res:
            return res.strip()
        else:
            return None

    def getJavaPath(self, path):
        if self._osType == 'Microsoft Windows':
            cmd = 'dir "%s" /s /b | find "java.exe"' % path
        else:
            cmd = 'find %s -type f -name "java"' % path
        output = self.execCmdByShell(cmd)
        if output:
            return output.split('\n')[0].strip('\r')
        else:
            return None

    def getJVMInfo(self, configLayout):
        path = configLayout.composePath(self._websphereHomePath, 'java')
        javaPath = self.getJavaPath(path)
        if javaPath:
            if self._osType == 'Microsoft Windows':
                cmd = '"%s" -version' % javaPath
            else:
                cmd = '%s -version' % javaPath
            output = self.execCmdByShell(cmd)
            if output:
                vendor = None
                javaVersion, name = self.parseJavaVersion(output)
                if name.lower().find('ibm') != -1:
                    vendor = 'IBM Corporation'
                elif name.lower().find('openjdk') != -1:
                    vendor = 'OpenJDK'
                else:
                    vendor = 'Oracle'
                jvm = jee.Jvm(name or 'jvm')
                jvm.javaVersion = javaVersion
                jvm.javaVendor = vendor
                return jvm
            else:
                return None
        else:
            return None

    def parseJavaVersion(self, content):
        rawOutputLines = content.splitlines()
        # stip each line
        getStrippedLine = lambda x: x.strip()
        strippedLines = map(getStrippedLine, rawOutputLines)
        # filter empty lines
        isLineEmpty = lambda x: len(x)
        lines = filter(isLineEmpty, strippedLines)
        if len(lines) < 3:
            raise Exception("Failed to parse java -version output")
        else:
            version = None
            name = None
            matchObj = re.search('java version \"(.+?)\"', lines[0])
            if matchObj:
                version = matchObj.group(1)
            name = lines[2]
            return version, name

    def discover(self, framework, vector, client, fs, parser, dnsResolver, reporterCreator, pathUtil, hostId):
        configLayout = ConfigLayout(fs)
        websphereList = self.getElementName(self._websphereHomePath)
        for websphere in websphereList:
            self._websphereHomePath = configLayout.composePath(self._websphereHomePath, websphere)
            # get profile name
            profilesPath = configLayout.composePath(self._websphereHomePath, 'profiles')
            profileNameList = self.getElementName(profilesPath)
            if profileNameList:
                for profileName in profileNameList:
                    configDirPath = configLayout.composePath(profilesPath, profileName, 'config')
                    profileHomeDirPath = pathUtil.dirName(configDirPath)
                    profileLayout = websphere_discoverer.ProfileLayout(profileHomeDirPath, fs)

                    cellsPath = configLayout.composePath(self._websphereHomePath, 'profiles', profileName, 'config', 'cells')
                    cellNameList = self.getElementName(cellsPath)
                    if cellNameList and len(cellNameList) == 1:
                        cellName = cellNameList[0].strip()
                        cellLayout = websphere_discoverer.CellLayout(profileLayout.composeCellHomePath(cellName), fs)
                        cellConfigFile = cellLayout.getFileContent(cellLayout.getConfigFilePath())
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

                        # get node name and server name
                        nodesPath = configLayout.composePath(configDirPath, 'cells', cellName, 'nodes')
                        nodeNameList = self.getElementName(nodesPath)
                        if nodeNameList:
                            for nodeName in nodeNameList:
                                nodeLayout = websphere_discoverer.NodeLayout(cellLayout.composeNodeHomePath(nodeName), fs)
                                node = websphere_discoverer.discoverNode(nodeLayout, pathUtil)
                                cell.addNode(node)

                                servers = parser.parseServersInServerIndex(
                                    nodeLayout.getFileContent(nodeLayout.getServerIndexPath()).content)
                                for server in servers:
                                    server.nodeName = node.getName()
                                    node.addServer(server)
                                    server.jvm = self.getJVMInfo(configLayout)

                        # make discovery of clusters
                        serverByFullName = websphere_discoverer.groupServersByFullNameInCell(cell)
                        clustersPath = configLayout.composePath(configDirPath, 'cells', cellName, 'clusters')
                        res = self.getElementName(clustersPath)
                        if res and 'cluster.xml' in res:
                            clusterXmlPath = configLayout.composePath(clustersPath, 'cluster.xml')
                            content = self.getFileContent(clusterXmlPath)
                            if content:
                                clusterConfig = parser.parseClusterConfig(content).content
                                cluster = clusterConfig.cluster
                                cell.addCluster(cluster)
                                clusterName = cluster.getName()
                                for member in filter(None,
                                                     map(curry(websphere_discoverer.getClusterMemberFromRuntimeGroup,
                                                               _, serverByFullName), clusterConfig.getMembers()
                                                         )
                                                     ):
                                    logger.info("\tServer(fullName = %s) is cluster member" % member.getFullName())
                                    member.addRole(jee.ClusterMemberServerRole(clusterName))

                        domainTopologyReporter = reporterCreator.getDomainReporter()
                        domainVector = domainTopologyReporter.reportNodesInDomainDnsEnabled(cell, dnsResolver, *cell.getNodes())
                        domainVector.addAll(domainTopologyReporter.reportClusters(cell, *cell.getClusters()))

                        if not findFirst(lambda srvr: srvr.ip.value(), serverByFullName.values()):
                            logger.warn("%s and related topology won't be reported as there is no at least one server with resolved IP address" % cell)
                            continue

                        _domainVector = domainVector.deepClone()
                        websphere_discoverer._sendVectorImmediately(framework, domainVector, forceVectorClean=0)

                        sendVectorWithDomain = curry(websphere_discoverer.sendTopologyWithDomainVector,
                                                     framework, _, _domainVector
                                                     )
                        # discover resources
                        jndiNamedResourceManager = websphere_discoverer.discoverResourcesInDomain(
                            cell, cellLayout, fs, parser,
                            reporterCreator, sendVectorWithDomain
                        )

                        # discover applications
                        websphere_discoverer.discoverApplicationsInDomain(cell, cellLayout, fs, self._shell, parser,
                                                                          reporterCreator, jndiNamedResourceManager,
                                                                          sendVectorWithDomain, NameBindingContent)
                        vector.addAll(domainVector)
                    else:
                        logger.info("Configuration where no cell or more than one cell in one profile is not supported")
                        continue

def DiscoveryMain(Framework):
    # this job discover inactive websphere according to the install path
    vector = ObjectStateHolderVector()
    hostId = Framework.getDestinationAttribute('hostId')
    protocol = Framework.getDestinationAttribute('Protocol')
    shell = None
    errorMessage = None
    try:
        client = Framework.createClient()
        try:
            shell = shellutils.ShellFactory().createShell(client)
            # create FS
            fs = websphere_discoverer._createFileSystemRecursiveSearchEnabled(file_system.createFileSystem(shell))
            pathUtil = file_system.getPath(fs)
            # Parser used for configuration files parsing
            loadExternalDtd = websphere_discoverer.isLoadExternalDtdEnabled()
            parser = websphere_discoverer.DescriptorParser(loadExternalDtd)
            dnsResolver = jee_discoverer.DnsResolverDecorator(netutils.JavaDnsResolver(), client.getIpAddress())
            reporterCreator = jee_discoverer.createTopologyReporterFactory(websphere.ServerTopologyBuilder(), dnsResolver)

            # was: websphere application server
            osType, winHomePathList, unixHomePathList = getWebSphereHomePath(shell,
                                                       Framework.getParameter('unixInstallPath'),
                                                       Framework.getParameter('winInstallPath'))
            homePathList = []
            if osType == 'Microsoft Windows':
                homePathList = winHomePathList
            else:
                homePathList = unixHomePathList
            if homePathList:
                Framework.sendObjects(jee_discoverer.discoverTnsnamesOra(hostId, client))
                for homePath in homePathList:
                    try:
                        discoverer = WebSphereManualDiscoverer(shell, homePath, osType)
                        if discoverer:
                            discoverer.discover(Framework, vector, client, fs, parser, dnsResolver,
                                            reporterCreator, pathUtil, hostId)
                    except:
                        continue
        finally:
            try:
                shell and shell.closeClient()
            except:
                logger.debugException('')
                logger.error('Unable to close shell')

    except JException, ex:
        errorMessage = ex.getMessage()
    except:
        errorObject = sys.exc_info()[1]
        if errorObject:
            errorMessage = str(errorObject)
        else:
            errorMessage = logger.prepareFullStackTrace('')
    if errorMessage:
        logger.debugException(errorMessage)
        errormessages.resolveAndReport(errorMessage, protocol, Framework)

    if vector.size() == 0:
        logger.debug('No data discovered from destination.')
        logger.reportWarning('No data discovered from destination. Plesae change the value of the parameter unixInstallPath or winInstallPath.')
    return vector
