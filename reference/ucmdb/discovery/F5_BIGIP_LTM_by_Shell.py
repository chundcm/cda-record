# coding=utf-8
# === F5 BIG-IP LTM discovery by Shell based on configuration document ===

# Main idea of this discovery is to find F5 related
# domain topology and configuration documents with corresponding linkage.


import logger
import errorcodes
import errorobject
import modeling
import shellutils
import file_system
from file_topology import FileAttrs
import re
from netutils import isValidIp

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector
from java.lang import Exception as JException
from com.hp.ucmdb.discovery.library.scope import DomainScopeManager
from shellutils import Shell, ShellUtils
from F5_BIGIP_LTM_by_SNMP import F5Discoverer
from com.hp.ucmdb.discovery.library.common import CollectorsParameters
from ip_addr import IPAddress
from java.lang import Boolean, Integer

F5_CONFIG_DIR = '/config/'
F5_CONFIG_NAMES = ['bigip.conf', 'bigip_local.conf']
SEPERATE_LINE = '-' * 20
F5_TIME_OUT = 30000

class NoF5Exception():
    pass

class ServicePort:
    def __init__(self, port_name, port_number, port_type):
        self.port_name = port_name
        self.port_number = port_number
        self.port_type = port_type
        
class ServicesFile:
    def __init__(self, file_path):
        if not file_path:
            raise ValueError('Failed to get services file content due to missing path. Path to services file has to specified.')
        self.file_path = file_path
        self.ports_content = []
        self._read_file()
    
    def _parse(self, file_content):
        for row in file_content:
            line = row and row.strip()
            if not line:
                continue
            
            if line.find('#') != -1:
                line = (line and line[:line.find("#")]).strip()
                
            elems = line.split()
            if len(elems) < 2:
                continue
            
            port_name = elems[0]
            port_number, port_type = elems[1].split('/')
            port_aliases = elems[2:]
            
            for name in port_aliases + [port_name]:
                self.ports_content.append(ServicePort(name, port_number, port_type)) 
                
    def _read_file(self):
        f = open(self.file_path, 'r')
        lines = f.readlines()
        f.close()
        self._parse(lines)

    def getPortsByName(self, port_name, ip, discoverable):
        return [x.port_number for x in self.ports_content if x.port_name == port_name]
        
def DiscoveryMain(Framework):
    ipAddress = Framework.getDestinationAttribute('ip_address')
    useServicesFile = Boolean.parseBoolean(Framework.getParameter('useServicesFile'))
    f5CommandTimeOut = Integer.parseInt(Framework.getParameter('f5CommandTimeOut'))
    global F5_TIME_OUT 
    F5_TIME_OUT = f5CommandTimeOut
    shell = None
    try:
        client = Framework.createClient()
        shell = shellutils.ShellFactory().createShell(client)
        knownPortsConfigFile = None
        if not useServicesFile:
            knownPortsConfigFile = Framework.getConfigFile(CollectorsParameters.KEY_COLLECTORS_SERVERDATA_PORTNUMBERTOPORTNAME)
        else:
            file_path = CollectorsParameters.BASE_PROBE_MGR_DIR + CollectorsParameters.getDiscoveryResourceFolder() + \
                    CollectorsParameters.FILE_SEPARATOR + 'f5_services.conf'
            knownPortsConfigFile = ServicesFile(file_path)
        f5Discoverer = createF5Discoverer(shell, ipAddress, knownPortsConfigFile)
        f5Discoverer.discover()
        return f5Discoverer.getTopology()

    except NoF5Exception:
        logger.reportWarning("No F5 LTM found on the remote machine")
    except:
        errorMsg = 'Failed to get general information'
        errobj = errorobject.createError(errorcodes.FAILED_GETTING_INFORMATION, ['shell', 'general information'], errorMsg)
        logger.debugException(errorMsg)
        logger.reportWarningObject(errobj)
    finally:
        try:
            shell and shell.closeClient()
        except:
            logger.debugException('')
            logger.error('Unable to close shell')

def getCommandOutput(shell, command, timeout=F5_TIME_OUT):
    """
    Execute given command and return the output
    Returns None is the execution fails or the output is empty
    """
    if command:
        try:
            return shell.execCmd(command, timeout, 1, useCache=1)#wait for timeout
        except:
            logger.debugException('')
    else:
        logger.warn('Commands is empty')

def createF5Discoverer(shell, ipAddress, knownPortsConfigFile):
    if isinstance(shell, shellutils.F5Shell):
        output = getCommandOutput(shell, 'show /sys version')
        if output:
            logger.debug('F5 LTM shell utility is available.')
            f5info = F5VersionInfo(output, 'bigpipe')
            return F5TmshDiscoverer(shell, ipAddress, f5info, knownPortsConfigFile)
    else:
        findCommands = [('tmsh','tmsh show /sys version'), ('bigpipe','b version')]
        for commandType, command in findCommands:
            output = shell.execCmd(command)
            if shell.getLastCmdReturnCode() == 0:
                logger.debug('F5 LTM shell utility is available.')
                f5info = F5VersionInfo(output, commandType)
                return F5ShellDiscoverer(shell, ipAddress, f5info, knownPortsConfigFile)

    raise NoF5Exception


class F5VersionInfo:
    def __init__(self, versionCmdOutput, commandType):
        self.version = None
        self.build = None
        self.edition = None
        self.date = None
        self.parseVersion(versionCmdOutput, commandType)


    def parseVersion(self, versionCmdOutput, commandType):
        # For v10 or v11, the output of the command "tmsh show /sys version":
        # Sys::Version
        # Main Package
        #   Product  BIG-IP
        #   Version  11.3.0
        #   Build    39.0
        #   Edition  VE Trial 11.3.0-HF1 (based on BIGIP 11.3.0HF6)
        #   Date     Mon Mar 24 14:01:16 PDT 2014
        #
        # For v9, the output of the command "b version":
        # BIG-IP Version 9.4.5 1049.10
        if commandType == 'bigpipe':
            regexStr = 's*BIG-IP\s+Version\s+([\d\.\-]+)\s+([\d\.\-]+)'
        else:
            regexStr = '\s*Version\s+([\d\.\-]+)'

        for line in versionCmdOutput.strip().split('\n'):
            matcher = re.search(regexStr, line)
            if matcher:
                self.version = matcher.group(1)
                break
        logger.debug('BIG-IP version : ', self.version)

class IpPort:
    def __init__(self, ip, port):
        self.__ip = ip
        self.__port = port

    def getIp(self):
        return self.__ip

    def getPort(self):
        return self.__port

class Node:
    def __init__(self, name, ip):
        self.__name = name
        self.__ip = ip

    def getName(self):
        return self.__name

    def getIP(self):
        return self.__ip


def buildIpServiceEndPointOsh(osh, ipPort):
    """
    @type ipPort: IpPort
    @return: ObjectStateHolder
    """
    ipPortOSH = modeling.createServiceAddressOsh(osh, ipPort.getIp(), ipPort.getPort(), modeling.SERVICEADDRESS_TYPE_TCP)

    return ipPortOSH


class VirtualHost(IpPort):
    def __init__(self, name, ip, port):
        IpPort.__init__(self, ip, port)
        self.__name = name

    def getName(self):
        return self.__name


def buildVirtualHostOsh(oshv, f5, virtualHost):
    """
    @type oshv:         ObjectStateHolderVector
    @type f5:           ObjectStateHolder
    @type virtualHost:  VirtualHost
    @rtype: ObjectStateHolder
    """
    domainName = DomainScopeManager.getDomainByIp(virtualHost.getIp().strip())
    name = '%s:%s %s' % (virtualHost.getIp().strip(), str(virtualHost.getPort()).strip(), str(domainName).strip())
    virtualhost_name = virtualHost.getName().strip()
    if virtualhost_name and virtualhost_name[0] != '/':
        virtualhost_name = '/' + virtualhost_name
    virtualHostOsh = modeling.createCompleteHostOSH('cluster_resource_group', name, None, virtualhost_name)
    #virtualHostOsh.setAttribute('name', virtualHost.getName())
    ipOsh = modeling.createIpOSH(virtualHost.getIp())
    oshv.add(modeling.createLinkOSH('containment', virtualHostOsh, ipOsh))

    ipPortOSH = buildIpServiceEndPointOsh(virtualHostOsh, virtualHost)
    # ipPortOSH.setContainer(virtualHostOsh)
    oshv.add(virtualHostOsh)
    oshv.add(ipOsh)
    oshv.add(ipPortOSH)
    oshv.add(modeling.createLinkOSH('owner', f5, virtualHostOsh))
    return virtualHostOsh


class Cluster:
    def __init__(self, name):
        self.__name = name
        self.__ipPorts = []
        self.__virtualHosts = []

    def getVirtualHosts(self):
        return self.__virtualHosts

    def addVirtualHost(self, name, ip, port):
        self.__virtualHosts.append(VirtualHost(name, ip, port))

    def getIpPorts(self):
        return self.__ipPorts[:]

    def addIpPort(self, ip, port):
        self.__ipPorts.append(IpPort(ip, port))

    def getName(self):
        return self.__name


def buildClusterOsh(oshv, f5, cluster):
    """
    @param oshv:ObjectStateHost
    @param f5:  ObjectStateHost
    """
    clusterOsh = ObjectStateHolder('loadbalancecluster')
    cluster_name = cluster.getName().strip()
    if cluster_name and cluster_name[0] != '/':
        cluster_name = '/' + cluster_name
    clusterOsh.setAttribute('data_name', cluster_name.strip())
    oshv.add(modeling.createLinkOSH('membership', clusterOsh, f5))
    oshv.add(clusterOsh)

    virtualHosts = cluster.getVirtualHosts()
    for virtualHost in virtualHosts:
        if isValidIp(virtualHost.getIp().strip()):
            virtualHostOsh = buildVirtualHostOsh(oshv, f5, virtualHost)
            oshv.add(modeling.createLinkOSH('containment', clusterOsh, virtualHostOsh))

    for ipPort in cluster.getIpPorts():
        if isValidIp(ipPort.getIp()):
            clusterMemberOsh = modeling.createHostOSH(ipPort.getIp(), 'host')
            ipPortOsh = buildIpServiceEndPointOsh(clusterMemberOsh, ipPort)
            oshv.add(clusterMemberOsh)
            oshv.add(ipPortOsh)
            oshv.add(modeling.createLinkOSH('membership', clusterOsh, ipPortOsh))


class F5ShellDiscoverer():
    def __init__(self, shell, hostIp, versionInfo, knownPortsConfigFile):
        self.shell = shell
        self.hostIp = hostIp
        self.version = versionInfo.version
        self.configFiles = []
        self.clusters = []
        self.nodes = []
        self.knownPortsConfigFile = knownPortsConfigFile


    def discover(self):
        fs = file_system.createFileSystem(self.shell)
        self.configFiles = fs.getFiles(F5_CONFIG_DIR, False, [ConfigFileFilter()],
                                       [FileAttrs.NAME, FileAttrs.PATH, FileAttrs.CONTENT, FileAttrs.PERMS, FileAttrs.LAST_MODIFICATION_TIME,
                                        FileAttrs.OWNER])
        for configFileName in F5_CONFIG_NAMES:
            for configFile in self.configFiles:
                if configFileName == configFile.name:
                    # get all defined nodes
                    self.discoverNodes(configFile.path)
                    self.discoverPools(configFile.path)
                    self.discoverVirtualServers(configFile.path)

        for cluster in self.clusters:
            logger.debug("--" * 20)
            logger.debug("cluster name = ", cluster.getName())
            ipPorts = cluster.getIpPorts()
            for ipPort in ipPorts:
                logger.debug("ipPort (%s, %s)" % (ipPort.getIp(), ipPort.getPort()))
            virtuals = cluster.getVirtualHosts()
            for virtual in virtuals:
                logger.debug("virtual name = ", virtual.getName())
                logger.debug("virtual ipPort(%s, %s)" % (virtual.getIp(), virtual.getPort()))
            logger.debug("--" * 20)

    def discoverSnatPools(self, configFilePath):
        poolStartRegex = "ltm\s+snatpool\s+(\S+)\s*{"
        membersRegex = "members\s+{.+?}"
        
        poolContent = self.findConfigFileContent(configFilePath, 'snatpool')
        if poolContent:
            for snatPool in re.split(SEPERATE_LINE, poolContent):
                membersContent = re.search(membersRegex, snatPool, re.DOTALL)
                matcher = re.search(poolStartRegex, poolContent)
                if matcher:
                    cluster = Cluster(matcher.group(1).strip())
                    self.clusters.append(cluster)
                    m = re.search()
                
    def discoverPools(self, configFilePath):
        poolStartRegex = "ltm\s+pool\s+(\S+)\s*{"
        memberRegex = "(\S+):([\d\.]+|[\w\-]+)"
        poolStartRegexWithoutLtm = "pool\s+(\S+)\s*{"
        memberRegexWithoutLtm = "([\d\.]+):(\S+)"

        poolContent = self.findConfigFileContent(configFilePath, 'pool')
        poolContentWithoutLtm = self.findConfigFileContentWithoutLtm(configFilePath, 'pool')
#        print "poolContent"
#        print poolContent
        if poolContent:
            for line in poolContent.strip().split('\n'):
                # match pool
                matcher = re.search(poolStartRegex, line)
                if matcher:
                    cluster = Cluster(matcher.group(1).strip())
                    self.clusters.append(cluster)
                else:
                    # match members
                    matcher = re.search(memberRegex, line)
                    if matcher:
                        nodeName = matcher.group(1).strip()
                        port_raw = matcher.group(2).strip()
                        port = None
                        if port_raw.isdigit():
                            port = port_raw
                        elif port_raw == 'any':
                            port = -1
                        else:
                            port_numbers = self.knownPortsConfigFile.getPortsByName(port_raw, str(self.hostIp), False)
                            if port_numbers:
                                port = port_numbers[0]
                        if not port:
                            continue
                        ip = None
                        #logger.warn('Working on node with name %s' % nodeName)
                        for node in self.nodes:
                            if nodeName == node.getName():
                                ip = node.getIP()
                                #logger.warn('Adding IPPort %s, %s' % (ip, port))
                                cluster.addIpPort(ip, port)
                                break

        elif poolContentWithoutLtm:
            for line in poolContentWithoutLtm.strip().split('\n'):
                # match pool
                ip = None
                matcher = re.search(poolStartRegexWithoutLtm, line)
                if matcher:
                    cluster = Cluster(matcher.group(1).strip())
                    self.clusters.append(cluster)
                else:
                    # match members
                    matcher = re.search(memberRegexWithoutLtm, line)
                    if matcher:
                        ip = matcher.group(1).strip()
                        portRaw = matcher.group(2).strip()
                        port = None
                        if portRaw.isdigit():
                            port = portRaw
                        elif portRaw == 'any':
                            port = -1
                        else:
                            portNumbers = self.knownPortsConfigFile.getPortsByName(portRaw, str(self.hostIp), False)
                            if portNumbers:
                                port = portNumbers[0]
                        if not port:
                            continue

                        for node in self.nodes:
                            if ip == node.getIP():
                                cluster.addIpPort(ip, port)
                                break

    def discoverVirtualServers(self, configFilePath):
        virtualRegex = "ltm\s+virtual\s+(\S+)\s*{"
        virtualIPRegex = "destination\s+/*.*/([\d\.]+)\:([\d]+|[\w\-]+)"
        poolRegex = "pool\s+(\S+)"
        virtualRegexWithoutLtm = "virtual\s+(\S+)\s*{"
        virtualIPRegexWithoutLtm = "destination\s+([\d\.]+)\:(\w+)"
        poolRegexWithoutLtm = "pool\s+(\S+)"

        virtualContent = self.findConfigFileContent(configFilePath, 'virtual')
        virtualContentWithoutLtm = self.findConfigFileContentWithoutLtm(configFilePath, 'virtual')
#        print "Virtual Content"
#        print virtualContent
        if virtualContent:
            pools = []
            pool = None
            ip = None
            port = None
            for line in virtualContent.strip().split('\n'):
                line = line.strip()
                matcher = re.search(virtualRegex, line)
                if matcher:
                    name = matcher.group(1).strip()
                    #logger.warn('Virtual Host name is %s' % name)
                else:
                    matcher = re.search(virtualIPRegex, line)
                    if matcher:
                        ip = matcher.group(1).strip()
                        port_raw = matcher.group(2).strip()
                        port = None
                        if port_raw.isdigit():
                            port = port_raw
                        elif port_raw == 'any':
                            port = -1
                        else:
                            port_numbers = self.knownPortsConfigFile.getPortsByName(port_raw, str(self.hostIp), False)
                            if port_numbers:
                                port = port_numbers[0]

                if line.startswith('pool'):
                    #logger.warn(line)
                    matcher = re.search(poolRegex, line)
                    if matcher:
                        pool = matcher.group(1).strip()
                        #logger.debug('Pool %s' % pool)
                        pools.append(pool)

                if line == SEPERATE_LINE:
                    if name and name == "$":
                        continue
                    if not (name and ip and port):
                        logger.warn('Skipping virtual host %s due to failed to parse ip-port value' % name)
                        continue
                    #virtualHost = VirtualHost(name, ip, port)
                    for cluster in self.clusters:
                        for pool in pools:
                            if pool and cluster.getName() == pool:
                                #logger.warn('Adding member with name %s to cluster %s' % (name, pool))
                                cluster.addVirtualHost(name, ip, port)
                    pool = None
                    pools = []
                    ip = None
                    port = None

        elif virtualContentWithoutLtm:
            pools = []
            pool = None
            ip = None
            port = None
            name = None
            for line in virtualContentWithoutLtm.strip().split('\n'):
                line = line.strip()
                matcher = re.search(virtualRegexWithoutLtm, line)
                if matcher:
                    name = matcher.group(1).strip()
                else:
                    matcher = re.search(virtualIPRegexWithoutLtm, line)
                    if matcher:
                        ip = matcher.group(1).strip()
                        portRaw = matcher.group(2).strip()
                        port = None
                        if portRaw.isdigit():
                            port = portRaw
                        elif portRaw == 'any':
                            port = -1
                        else:
                            portNumbers = self.knownPortsConfigFile.getPortsByName(portRaw, str(self.hostIp), False)
                            if portNumbers:
                                port = portNumbers[0]
                if line.startswith('pool'):
                    matcher = re.search(poolRegexWithoutLtm, line)
                    if matcher:
                        pool = matcher.group(1).strip()
                        pools.append(pool)
                if line == SEPERATE_LINE:
                    if name and name == "$":
                        continue
                    if not (name and ip and port):
                        logger.warn('Skipping virtual host %s due to failed to parse ip-port value' % name)
                        continue
                    for cluster in self.clusters:
                        for pool in pools:
                            if pool and cluster.getName() == pool:
                                cluster.addVirtualHost(name, ip, port)
                    pool = None
                    pools = []
                    ip = None
                    port = None
                    name = None

    def discoverNodes(self, configFilePath):
        nodeRegex = "ltm\s+node\s+(\S+)\s*\{(.*)"
        addressRegex = "address\s+([\d\.]+)\s*\}*"
        nodeRegexWithoutLtm = "node\s([\d\.]+)\s*\{(.*)"
        nameRegex = "screen\s+(\S+)\s*"

        nodeContent = self.findConfigFileContent(configFilePath, 'node')
        nodeContentWithoutLtm = self.findConfigFileContentWithoutLtm(configFilePath, 'node')
        if nodeContent:
            for line in nodeContent.strip().split('\n'):
                line = line.strip()
                # match node name
                matcher = re.search(nodeRegex, line)
                if matcher:
                    nodeName = matcher.group(1).strip()

                    if matcher.group(2):
                        # if the node element in the same line is "ltm node /Common/IIS { address 192.168.10.11 }",
                        # need to parse the address immediately
                        addressLine = matcher.group(2).strip()
                        addressMatcher = re.search(addressRegex,addressLine)
                        if addressMatcher:
                            ip = addressMatcher.group(2).strip()


                else:
                    # match the address
                    # for example: address 192.168.10.11
                    matcher = re.search(addressRegex, line)
                    if matcher:
                        ip = matcher.group(1).strip()

                if line == SEPERATE_LINE:
                    node = Node(nodeName, ip)
                    self.nodes.append(node)
        #print "Discovered Nodes %s" % self.nodes

        elif nodeContentWithoutLtm:
            nodeName = None
            ip = None
            for line in nodeContentWithoutLtm.strip().split('\n'):
                line = line.strip()
                matcher = re.search(nodeRegexWithoutLtm, line)
                if matcher:
                    ip = matcher.group(1).strip()
                    if matcher.group(2):
                        addressLine = matcher.group(2).strip()
                        addressMatcher = re.search(nameRegex, addressLine)
                        if addressMatcher:
                            nodeName = addressMatcher.group(2).strip()
                else:
                    matcher = re.search(nameRegex, line)
                    if matcher:
                        nodeName = matcher.group(1).strip()
                if line == SEPERATE_LINE:
                    node = Node(nodeName, ip)
                    self.nodes.append(node)

    def findConfigFileContent(self, configFile, blockName):
        fileContent = self.shell.execCmd('cat ' + configFile  +
                                         ' | awk \'BEGIN {RS=\"\\n}\";FS=RS} /ltm ' +
                                         blockName + ' / {print $1\"\\n}\\n' + SEPERATE_LINE +'\";} \' ')

        if fileContent and self.shell.getLastCmdReturnCode() == 0:
            return fileContent
        else :
            return None

    def findConfigFileContentWithoutLtm(self, configFile, blockName):
        fileContent = self.shell.execCmd('cat ' + configFile +
                                         ' | awk \'BEGIN {RS=\"\\n}\";FS=RS} /' +
                                         blockName + ' / {print $1\"\\n}\\n' + SEPERATE_LINE +'\";} \' ')
        if fileContent and self.shell.getLastCmdReturnCode() == 0:
            return fileContent
        else:
            return None

    def getTopology(self):
        oshv = ObjectStateHolderVector()
        lb = modeling.createHostOSH(self.hostIp, 'lb')
        f5 = modeling.createApplicationOSH('f5_ltm', 'F5 BIG-IP LTM', lb)
        f5.setAttribute('application_version', self.version)
        oshv.add(lb)
        oshv.add(f5)
        for configFile in self.configFiles:
            if isinstance(configFile, basestring):
                oshv.add(modeling.createConfigurationDocumentOSH('complete', None, configFile, f5,  modeling.MIME_TEXT_PLAIN))
            else:
                oshv.add(modeling.createConfigurationDocumentOshByFile(configFile, f5, modeling.MIME_TEXT_PLAIN))

        for cluster in self.clusters:
            buildClusterOsh(oshv, f5, cluster)

        return oshv

class F5TmshDiscoverer(F5ShellDiscoverer):
    def __init__(self, shell, hostIp, versionInfo, knownPortsConfigFile):
        F5ShellDiscoverer.__init__(self, shell, hostIp, versionInfo, knownPortsConfigFile)
        
    def getConfigFiles(self):
        getCommandOutput(self.shell, 'cd /')
        getCommandOutput(self.shell, 'modify cli preference pager disabled')
        return [getCommandOutput(self.shell, 'show running-config recursive\ny\n')]

    def findConfigFileContent(self, configFile, blockName):
        
        #m = re.findall('(ltm\s+%s\s.+?})' % blockName, configFile, re.DOTALL)
        #m = re.findall('(ltm\s+%s\s.+?})\s*ltm' % blockName, configFile, re.DOTALL)
        m = re.split('(ltm\s+%s\s.+?{)' % blockName, configFile)
        elems = []
        for x in xrange(len(m) - 1):
            if re.search('(ltm\s+%s)' % blockName, m[x]):
                elems.append(m[x] + m[x+1])
        if elems:
            return ('\n%s\n' % SEPERATE_LINE).join(elems)

    def discover(self):
        self.configFiles = self.getConfigFiles()
        for configFile in self.configFiles:
            # get all defined nodes
            self.discoverNodes(configFile)
            self.discoverPools(configFile)
            self.discoverVirtualServers(configFile)

        for cluster in self.clusters:
            logger.debug("--" * 20)
            logger.debug("cluster name = ", cluster.getName())
            ipPorts = cluster.getIpPorts()
            for ipPort in ipPorts:
                logger.debug("ipPort (%s, %s)" % (ipPort.getIp(), ipPort.getPort()))
            virtuals = cluster.getVirtualHosts()
            for virtual in virtuals:
                logger.debug("virtual name = ", virtual.getName())
                logger.debug("virtual ipPort(%s, %s)" % (virtual.getIp(), virtual.getPort()))
            logger.debug("--" * 20)


class ConfigFileFilter(file_system.FileFilter):
    def accept(self, file_):
        return file_.path.endswith(".conf")

