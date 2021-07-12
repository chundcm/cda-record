#coding=utf-8

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector

from ibm_lpar_or_vio_by_shell import *
from ibm_lpar_or_vio_by_shell import doDiscovery as doBasicDiscovery
from vendors import PlatformVendors

SYSTEM = 's'
APPLICATION = 'a'

class _HasOsh:
    ''' Class that extends other classes with ability to have OSH built from them '''
    def __init__(self):
        self.__osh = None

    def setOsh(self, osh):
        if osh is None: raise ValueError("OSH is None")
        self.__osh = osh

    def getOsh(self):
        return self.__osh

class WPAR(_HasOsh):
    titleToAttr = {
        'name': 'name',
        'state': 'wpar_state',
        'rootvgwpar': 'is_wpar_rootvg',
        'routing': 'is_wpar_specific_routing',
        'vipwpar': 'wpar_virtual_ip',
        'directory': 'wpar_directory',
        'owner': 'wpar_owner',
        'auto': 'is_wpar_auto_started',
        'privateusr': 'is_wpar_private_usr',
        'checkpointable': 'is_wpar_checkpointable',
        'xwparipc': 'is_wpar_cross_ipc',
        'architecture': 'wpar_architecture',
        'active': 'is_wpar_active',
        'rset': 'wpar_resource_set',
        'shares_CPU': 'wpar_cpu_shares',
        'CPU': 'wpar_cpu_limits',
        'shares_memory': 'wpar_memory_shares',
        'memory': 'wpar_memory_limits',
        'procVirtMem': 'wpar_proc_memory_limit',
        'totalVirtMem': 'wpar_total_mem_limit',
        'totalProcesses': 'wpar_total_processes',
        'totalThreads': 'wpar_total_threads',
        'totalPTYs': 'wpar_total_ptys',
        'totalLargePages': 'wpar_total_large_pages',
        'pct_shmIDs': 'wpar_max_shares_memory_ids',
        'pct_pinMem': 'wpar_max_pinned_memory'
    }

    def __init__(self):
        _HasOsh.__init__(self)
        self.vector = ObjectStateHolderVector()
        self.isSystem = True

    def report(self, Framework):
        if not hasattr(self, 'name'):
            logger.debug('WPAR name is empty.')
            return
        if hasattr(self, 'type'):
            wparType = getattr(self, 'type')
            if wparType.lower() == SYSTEM:
                self.isSystem = True
                reportHostName = Framework.getParameter('reportWparNameAsHostName')
                if not (hasattr(self, 'address') and getattr(self, 'address') or
                        (reportHostName and reportHostName.lower().strip() == 'true')):
                    return
                if hasattr(self, 'address'):
                    ip = getattr(self, 'address')
                    ipMask = getattr(self, 'mask_prefix')
                    hostOSH = modeling.createHostOSH(ip, 'unix')
                    ipOSH = modeling.createIpOSH(ip)
                    link = modeling.createLinkOSH('containment', hostOSH, ipOSH)
                    self.vector.add(hostOSH)
                    self.vector.add(ipOSH)
                    self.vector.add(link)
                    networkOsh = modeling.createNetworkOSH(ip, ipMask)
                    self.vector.add(networkOsh)
                    self.vector.add(modeling.createLinkOSH('member', networkOsh, ipOSH))
                    self.vector.add(modeling.createLinkOSH('member', networkOsh, hostOSH))
                    if hasattr(self, 'interface'):
                        interface = getattr(self, 'interface')
                        interfaceOSH = ObjectStateHolder('interface')
                        interfaceOSH.setStringAttribute('interface_name', interface)
                        interfaceOSH.setContainer(hostOSH)
                        self.vector.add(interfaceOSH)
                        self.vector.add(modeling.createLinkOSH('containment', interfaceOSH, ipOSH))
                else:
                    hostOSH = ObjectStateHolder('unix')
                    self.vector.add(hostOSH)
                if reportHostName and reportHostName.lower().strip() == 'true':
                    hostOSH.setStringAttribute("name", getattr(self, 'name'))
                hostOSH.setStringAttribute('platform_vendor', PlatformVendors.IBM)
                hostOSH.setStringAttribute("os_family", 'unix')
                hostOSH.setBoolAttribute('host_iscomplete', True)
                hostOSH.setBoolAttribute('host_isvirtual', True)
                hostOSH.setListAttribute('node_role', ['virtualized_system'])
                self.setOsh(hostOSH)
                configOSH = ObjectStateHolder('ibm_wpar_profile')
                for name, value in vars(self).items():
                    if self.titleToAttr.has_key(name):
                        if self.titleToAttr[name].find('is_') != -1:
                            if value.lower() == 'yes':
                                configOSH.setBoolAttribute(self.titleToAttr[name], True)
                            else:
                                configOSH.setBoolAttribute(self.titleToAttr[name], False)
                        else:
                            configOSH.setStringAttribute(self.titleToAttr[name], value)
                configOSH.setContainer(hostOSH)
                self.vector.add(configOSH)
                return self.vector
            elif wparType.lower() == APPLICATION:
                self.isSystem = False
            else:
                logger.debug('WPAR type is not recognized: ', wparType)


class WparDiscoverer:
    GET_WPAR_GENERAL_INFO = "lswpar -G -d UCMDB"
    GET_WPAR_NETWORK_INFO = "lswpar -N -d UCMDB"
    GET_WPAR_CPU_MEMORY_INFO = "lswpar -R -d UCMDB"
    def __init__(self, shell, wparHostOSH, Framework):
        self._shell = shell
        self._wparDict = {}
        self.wparHostOSH = wparHostOSH
        self.framework = Framework

    def getWpars(self):
        self.execCommand(self.GET_WPAR_GENERAL_INFO)
        self.execCommand(self.GET_WPAR_NETWORK_INFO)
        self.execCommand(self.GET_WPAR_CPU_MEMORY_INFO)
        vector = ObjectStateHolderVector()

        hypervisorId = self.framework.getDestinationAttribute('hypervisorId')
        hypervisorOSH = modeling.createOshByCmdbId('hypervisor', hypervisorId)
        vector.add(hypervisorOSH)

        for (wparName, wpar) in self._wparDict.items():
            vector.addAll(wpar.report(self.framework))
            if wpar.isSystem and wpar.getOsh():
                linkOSH = modeling.createLinkOSH('execution_environment', hypervisorOSH, wpar.getOsh())
                vector.add(linkOSH)
            if not wpar.isSystem:
                appOSH = modeling.createApplicationOSH('running_software', wpar.name, self.wparHostOSH)
                vector.add(appOSH)
                linkOSH = modeling.createLinkOSH('dependency', appOSH, hypervisorOSH)
                vector.add(linkOSH)
        return vector

    def execCommand(self, cmd):
        output = self._shell.execCmd(cmd)
        if output and self._shell.getLastCmdReturnCode() == 0:
            self.parseGetWparOutput(output)

    def parseGetWparOutput(self, output):
        wparDict = {}
        lines = (line.strip() for line in output.splitlines()
                 if line and line.strip())
        headers = None
        for line in lines:
            if not headers:
                if line and line.find('#') != -1:
                    if line and line.find('UCMDB') != -1:
                        try:
                            headers = line.split('#')[1].split('UCMDB')
                            continue
                        except:
                            logger.warn('Failed parsing in string %s' % line)
            else:
                if line and line.find('UCMDB') != -1:
                    try:
                        tokens = line.split('UCMDB')
                        wpar = None
                        for index in range(len(tokens)):
                            header = headers[index]
                            token = tokens[index]
                            if header == 'name':
                                if self._wparDict.has_key(token):
                                    wpar = self._wparDict[token]
                                else:
                                    wpar = WPAR()
                            if wpar and not hasattr(wpar, header):
                                setattr(wpar, header, token)
                        if wpar:
                            self._wparDict.setdefault(wpar.name, wpar)
                    except:
                        logger.debugException('Failed parsing in string %s' % line)
        return wparDict

def doWparDiscovery(shell, hostOSH, Framework):
    wparDiscoverer = WparDiscoverer(shell, hostOSH, Framework)
    return wparDiscoverer.getWpars()


##############################################
########      MAIN                  ##########
##############################################
def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()

    protocol = Framework.getDestinationAttribute('Protocol')
    hostCmdbId = Framework.getDestinationAttribute('hostId')
    osType = Framework.getDestinationAttribute('osType')

    shell = None
    hostOSH = modeling.createOshByCmdbId('host', hostCmdbId)
    OSHVResult.add(hostOSH)

    try:
        client = Framework.createClient()
        shell = shellutils.ShellUtils(client)
        OSHVResult.addAll(doBasicDiscovery(shell, hostOSH, hostOSH, Framework, osType))
        OSHVResult.addAll(doWparDiscovery(shell, hostOSH, Framework))
    except Exception, ex:
        exInfo = ex.getMessage()
        errormessages.resolveAndReport(exInfo, protocol, Framework)
    except:
        exInfo = logger.prepareJythonStackTrace('')
        errormessages.resolveAndReport(exInfo, protocol, Framework)
    try:
        shell and shell.closeClient()
    except:
        logger.debugException("")
        logger.error('Unable to close shell')
    return OSHVResult
