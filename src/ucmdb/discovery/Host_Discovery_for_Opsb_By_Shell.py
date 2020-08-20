# coding=utf-8
from java.util import Properties
from java.util import Hashtable
from java.lang import Exception as JException
from java.io import IOException
from java.io import UnsupportedEncodingException
from java.nio.charset import UnsupportedCharsetException
import re

import TTY_Connection_Utils
import NTCMD_Connection_Utils
import process_discoverer
import shell_interpreter
import clientdiscoveryutils
import sys
import process
import process_to_process
import Dis_TCP
import applications
import ConnectedOSCredentialFinder
import ip_addr
import os
import wmiutils

import NTCMD_HR_REG_Service_Lib
import HR_Dis_Driver_Lib
import TTY_HR_CPU_Lib
import TTY_HR_Disk_Lib
import TTY_HR_Memory_Lib
import TTY_HR_Share_Lib
import TTY_HR_User_Lib
import errorcodes
import errormessages
import errorobject
import logger
import modeling
import netutils
import shellutils
import TTY_HR_Main
from host_win_shell import HostDiscovererByShell
from networking_win_shell import DnsServersDiscoverer, WinsServerDicoverer, DhcpServerDiscoverer, \
    IpConfigInterfaceDiscoverer
from host_win_wmi import WmiHostDiscoverer
from networking_win_wmi import WmiDnsServersDiscoverer, WmiWinsServersDiscoverer, WmiDhcpServersDiscoverer, \
    WmiInterfaceDiscoverer
from networking_win import TopologyBuilder

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector
from com.hp.ucmdb.discovery.library.clients import ClientsConsts
from com.hp.ucmdb.discovery.library.clients.agents import BaseAgent
from com.hp.ucmdb.discovery.library.clients.ddmagent import AgentSessionException
from com.hp.ucmdb.discovery.library.credentials.dictionary import ProtocolManager
from com.hp.ucmdb.discovery.common import CollectorsConstants
from com.hp.ucmdb.discovery.library.common import CollectorsParameters
from com.hp.ucmdb.discovery.library.communication.downloader.cfgfiles import GeneralSettingsConfigFile
from com.hp.ucmdb.discovery.common import TopologyConstants
from dns_resolver import SocketDnsResolver, ResolveException

from org.jdom.input import SAXBuilder

def _getSupportedShellProtocols(Framework):
    '''Returns names of protocols that will be used in the connection flow
    depending on the order in the list
    @types: Framework -> list[str]
    '''
    # ORDER IS IMPORTANT

    protocolOrder = GeneralSettingsConfigFile.getInstance().getPropertyStringValue('protocolConnectionOrder', "")
    if protocolOrder:
        supportedProtocols = []
        for protocol in protocolOrder.split(','):
            if protocol.strip().lower() == ClientsConsts.SSH_PROTOCOL_NAME.lower():
                supportedProtocols.append(ClientsConsts.SSH_PROTOCOL_NAME)
            elif protocol.strip().lower() == ClientsConsts.NTCMD_PROTOCOL_NAME.lower():
                supportedProtocols.append(ClientsConsts.NTCMD_PROTOCOL_NAME)
            else:
                logger.debug("Unknown protocol name in globalSetting:", protocol)
    else:
        supportedProtocols = [ClientsConsts.SSH_PROTOCOL_NAME,
                              ClientsConsts.NTCMD_PROTOCOL_NAME]

    return supportedProtocols


def DiscoveryMain(Framework):
    SHELL_CLIENT_PROTOCOLS = _getSupportedShellProtocols(Framework)

    ip = Framework.getDestinationAttribute('ip_address')
    domain = Framework.getDestinationAttribute('ip_domain')
    codepage = Framework.getCodePage()
    useLastState = Framework.getParameter('useLastSuccessConnection')

    vector = ObjectStateHolderVector()
    warningsList = []
    errorsList = []

    # preparing empty dictionary for storing credentials later
    credentialsByType = {}

    # take the latest used credentials if any
    lastState = None
    if useLastState and useLastState.lower() == 'true':
        lastState = Framework.loadState()

    if lastState:
        credentialsByType[None] = [lastState]

    # try to get ip address by mac address from ARP Cache
    macAddress = Framework.getDestinationAttribute('ip_mac_address')
    foundIp = clientdiscoveryutils.getIPAddressOnlyFromMacAddress(macAddress)
    if foundIp:
        ip = foundIp

    # Gather credentials for protocols
    for clientType in SHELL_CLIENT_PROTOCOLS:
        # getting an ordered list of credentials for the given client type and storing them in the credentials dictionary
        protocols = netutils.getAvailableProtocols(Framework, clientType, ip, domain)
        if protocols:
            credentialsByType[clientType] = protocols

    client = None
    if credentialsByType:
        if lastState:
            client = createClientFromLastState(Framework, lastState, warningsList, errorsList)
            if not client:
                logger.debug(
                    'Failed to create client using last state properties. Will try to connect using other credentials.')
        if not client:
            for clientType in SHELL_CLIENT_PROTOCOLS:
                credentials = credentialsByType.get(clientType)
                if credentials:
                    client = createClient(Framework, clientType, credentials, ip, codepage, warningsList, errorsList)
                    if client:
                        warningsList = []
                        errorsList = []
                        # save credentials id for further reuse
                        Framework.saveState(client.getCredentialId())
                        break
    else:
        for shellType in SHELL_CLIENT_PROTOCOLS:
            msg = errormessages.makeErrorMessage(shellType, pattern=errormessages.ERROR_NO_CREDENTIALS)
            errobj = errorobject.createError(errorcodes.NO_CREDENTIALS_FOR_TRIGGERED_IP, [shellType], msg)
            warningsList.append(errobj)

    if not client:
        Framework.clearState()
    else:
        # successfully connected, do discovery
        shell = None
        clientType = client.getClientType()

        connectedOSCredentialID = None
        try:
            try:
                shellFactory = shellutils.ShellFactory()
                shell = shellFactory.createShell(client, clientType)

                connectedOSCredentialID = ConnectedOSCredentialFinder.findCredential(Framework, shell, client,
                                                                                     errorsList, warningsList)

                # If we got a default value, we just pass None later
                # Else - we need to signal the existing client which can be only UDA by now that it has a credential
                # to take sudo password from, if it needs it
                if (not connectedOSCredentialID
                    or connectedOSCredentialID == ConnectedOSCredentialFinder.NO_CONNECTED_CRED_ID):
                    connectedOSCredentialID = None
                else:
                    try:
                        client.setConnectedShellCredentialID(connectedOSCredentialID)
                    except:
                        logger.warn('Failed to setConnectedShellCredentialID, sudo commands may not work in this run')

                vector.addAll(doDiscovery(Framework, shell, client, ip, codepage, connectedOSCredentialID))
            except (Exception, JException), jex:
                msg = str(jex)
                logger.debugException(msg)
                errormessages.resolveAndAddToObjectsCollections(msg,
                                                                clientType, warningsList, errorsList)
        finally:
            if shell:
                try:
                    shell.closeClient()
                except:
                    errobj = errorobject.createError(errorcodes.CLIENT_NOT_CLOSED_PROPERLY, None,
                                                     "Client was not closed properly")
                    warningsList.append(errobj)
                    logger.warnException('')
            # close client anyway
            if client and client.close(): pass
            # create shell OSH if connection established but discovery failed
            if not vector.size():
                logger.warn('Discovery failed, though shell object will be created')
                hostOsh = modeling.createHostOSH(ip, filter_client_ip=True)
                if hostOsh:
                    languageName = None
                    langBund = Framework.getEnvironmentInformation().getBundle('langNetwork', languageName)
                    shellOsh = createShellObj(client, client, ip, langBund, languageName, codepage,
                                              connectedShellCredId=connectedOSCredentialID)
                    shellOsh.setContainer(hostOsh)

                    vector.add(shellOsh)
                else:
                    logger.warn(
                        'Failed to create node and shell since IP is of a Client range type, not enough data for reconciliation.')

    for errobj in warningsList:
        logger.reportWarningObject(errobj)
    for errobj in errorsList:
        logger.reportErrorObject(errobj)

    return vector


def createClient(Framework, shellName, credentials, ip, codepage, warningsList, errorsList):
    'Framework, str, list(str), str, str, list(ErrorObject), list(ErrorObject) -> BaseClient or None'
    # this list will contain ports to which we tried to connect, but failed
    # this is done for not connecting to same ports with different credentials
    # and failing because of some IOException...
    failedPorts = []
    client = None
    # check which credential is good for the shell
    str = lambda x: u'%s' % x
    for credentialId in credentials:
        try:
            port = None

            if shellName and shellName != ClientsConsts.NTCMD_PROTOCOL_NAME and shellName != ClientsConsts.DDM_AGENT_PROTOCOL_NAME:
                # get port details - this is for not failing to connect to same port
                # by different credentials
                port = Framework.getProtocolProperty(credentialId, CollectorsConstants.PROTOCOL_ATTRIBUTE_PORT)
                # do not try to connect to same port if we already failed:
                if port in failedPorts:
                    continue
            props = Properties()
            props.setProperty(CollectorsConstants.DESTINATION_DATA_IP_ADDRESS, ip)
            props.setProperty(BaseAgent.ENCODING, codepage)
            props.setProperty(CollectorsConstants.ATTR_CREDENTIALS_ID, credentialId)
            return Framework.createClient(props)
        except IOException, ioEx:
            strException = str(ioEx.getMessage())
            shouldStop = errormessages.resolveAndAddToObjectsCollections(strException, shellName, warningsList,
                                                                         errorsList)
            if client:
                client.close()
            # we failed to connect - add the problematic port to failedPorts list
            if port and shouldStop:
                failedPorts.append(port)
        except (UnsupportedEncodingException, UnsupportedCharsetException), enEx:
            strException = str(enEx.getClass().getName())
            shouldStop = errormessages.resolveAndAddToObjectsCollections(strException, shellName, warningsList,
                                                                         errorsList)
            if client:
                client.close()
            if shouldStop:
                if port:
                    failedPorts.append(port)
                else:
                    return None
        except AgentSessionException, ex:
            logger.debug('Got AgentSessionException')

            # We don't want to stop for UDA connection exception
            strException = str(ex.getMessage())
            errormessages.resolveAndAddToObjectsCollections(strException, shellName, warningsList, errorsList)
            if client:
                client.close()
        except JException, ex:
            strException = str(ex.getMessage())
            shouldStop = errormessages.resolveAndAddToObjectsCollections(strException, shellName, warningsList,
                                                                         errorsList)
            if client:
                client.close()
            if shouldStop:
                return None
        except:
            if client:
                client.close()
            excInfo = str(sys.exc_info()[1])
            errormessages.resolveAndAddToObjectsCollections(excInfo, shellName, warningsList, errorsList)
    return None


def getShellName(protocolName):
    protocolSuffix = ProtocolManager.PROTOCOL
    if protocolName and protocolName.endswith(protocolSuffix):
        return protocolName[:-len(protocolSuffix)]
    return None


def getShellNameOfLastState(Framework, lastState):
    protocolName = Framework.getProtocolProperty(lastState, CollectorsConstants.PROTOCOL_ATTRIBUTE_TYPE)
    return getShellName(protocolName)


def getNatIPFromConfigurationFile():
    """
    Read IP or IP range from configuration file.
    @return: A list contains IPAddress objects and IPNetwork objects
    """
    NATIPConfigurationFileFolder = os.path.join(CollectorsParameters.BASE_PROBE_MGR_DIR,
                                                CollectorsParameters.getDiscoveryConfigFolder())
    NATIPConfigurationFile = os.path.join(NATIPConfigurationFileFolder, 'NATIpAddress.xml')

    if not os.path.exists(NATIPConfigurationFile):
        logger.info("There is no NAT IP address defined.")
        return

    # Read tags from xml file
    builder = SAXBuilder()
    configDoc = builder.build(NATIPConfigurationFile)
    rootElement = configDoc.getRootElement()
    ipElements = rootElement.getChildren('Ip')
    ipRangeElements = rootElement.getChildren('IpRange')

    NAT_IPs = []

    # Read IPAddress, add valid one to NAT_IPs list
    if ipElements:
        for ipElement in ipElements:
            ip = ipElement.getText()
            if ip_addr.isValidIpAddress(ip):
                ipObj = ip_addr.IPAddress(ip)
                NAT_IPs.append(ipObj)

    # Read IP Ranges, create IPNetwork and add to NAT_IPs list
    if ipRangeElements:
        for ipRangeElement in ipRangeElements:
            ip_range_raw = ipRangeElement.getText()
            ips = ip_range_raw.split('-')
            ip_start = ips[0]
            ip_end = ips[1]

            if ip_addr.isValidIpAddress(ip_start) and ip_addr.isValidIpAddress(ip_end):
                ip_start = ip_addr.IPAddress(ip_start)
                ip_end = ip_addr.IPAddress(ip_end)
                ips = ip_addr.summarize_address_range(ip_start, ip_end)
                logger.debug(ips)
                NAT_IPs.extend(ips)
            else:
                logger.warn("IP Range should start and end with valid IP address")

    return NAT_IPs


def createClientFromLastState(Framework, lastState, warningsList, errorsList):
    client = None
    shellName = None
    str = lambda x: u'%s' % x
    try:
        shellName = getShellNameOfLastState(Framework, lastState)
        if not shellName:
            logger.debug('No shellname found for credential id ' + lastState + '.')
            return None
        return Framework.createClient(lastState)
    except (UnsupportedEncodingException, UnsupportedCharsetException), enEx:
        strException = str(enEx.getClass().getName())
        errormessages.resolveAndAddToObjectsCollections(strException, shellName, warningsList, errorsList)
        if client:
            client.close()
    except (Exception, JException), jex:
        strException = str(jex.getMessage())
        errormessages.resolveAndAddToObjectsCollections(strException, shellName, warningsList, errorsList)
        if client:
            client.close()
    except:
        if client:
            client.close()
        excInfo = str(sys.exc_info()[1])
        errormessages.resolveAndAddToObjectsCollections(excInfo, shellName, warningsList, errorsList)
    return None


def createShellObj(shell, client, ip, langBund, language, codePage, arpMac=None, connectedShellCredId=None):
    'Shell, str, langBundle, str, str -> osh'
    # make sure that 'ip' is an ip and not a dns name
    # the reason is to make application_ip attribute hold an ip and not a dns name,
    # hence, when the application will be a trigger it will find the probe
    clientType = shell.getClientType()
    if clientType == ClientsConsts.NTCMD_PROTOCOL_NAME:
        clientType = "ntcmd"
    logger.debug('creating object for obj_name=%s' % clientType)

    ipObj = ip
    if ip_addr.isValidIpAddress(ip):
        ipObj = ip_addr.IPAddress(ip)
    else:
        # maybe it's a hostname?
        hostname = ip
        try:
            ips = SocketDnsResolver().resolve_ips(hostname)
            ipObj = ips[0]
        except ResolveException:
            logger.reportWarning('Could not resolve hostname' + hostname)
            ipObj = ip

    shellOsh = ObjectStateHolder(clientType)

    shellOsh.setAttribute('application_ip', str(ipObj))
    shellOsh.setAttribute('data_name', clientType)

    if clientType != "ntcmd":
        shellOsh.setAttribute('application_port', shell.getPort())
        shellOsh.setContainer(modeling.createHostOSH(str(ipObj)))

    if (language):
        shellOsh.setAttribute('language', language)
    if (codePage):
        shellOsh.setAttribute('codepage', codePage)

    shellOsh.setAttribute('credentials_id', shell.getCredentialId())

    if arpMac:
        shellOsh.setAttribute(TopologyConstants.ATTR_APPLICATION_ARP_MAC, arpMac)

    if connectedShellCredId:
        shellOsh.setAttribute(TopologyConstants.ATTR_CONN_OS_CRED_ID, connectedShellCredId)

    return shellOsh


def isClientTypeIP(ip):
    from com.hp.ucmdb.discovery.library.scope import DomainScopeManager
    from appilog.common.utils import RangeType

    tag = DomainScopeManager.getRangeTypeByIp(ip)
    return RangeType.CLIENT == tag


def isStampEnabled(Framework, ip):
    from java.lang import Boolean

    enableStampingParameter = Framework.getParameter('enableStamping')
    onlyStampingClientParameter = Framework.getParameter('onlyStampingClient')
    logger.debug("Parameter for enableStamping:", enableStampingParameter)
    logger.debug("Parameter for onlyStampingClient:", onlyStampingClientParameter)
    enableStamping = Boolean.parseBoolean(enableStampingParameter)
    onlyStampingClient = Boolean.parseBoolean(onlyStampingClientParameter)
    isClientIP = isClientTypeIP(ip)

    return enableStamping and (not onlyStampingClient or isClientIP)


def getUduid(client, stampIfNotExist=0):
    OPTION_UD_UNIQUE_ID = "UD_UNIQUE_ID"
    try:
        uduid = None
        try:
            clientOptions = client.getOptionsMap()
            uduid = clientOptions.get(OPTION_UD_UNIQUE_ID)
            logger.debug("Get uduid from client:", uduid)
        except:
            logger.debug("Can't get uduid from client")
            pass

        if not uduid and stampIfNotExist:
            from java.util import UUID
            uduid = UUID.randomUUID()
            logger.debug("Generated uduid:", uduid)

            from java.util import HashMap
            options = HashMap()
            options.put(OPTION_UD_UNIQUE_ID, str(uduid))
            client.setOptionsMap(options)
            clientOptions = client.getOptionsMap()
            # Get the value again to make sure the new value was set to client
            uduid = clientOptions.get(OPTION_UD_UNIQUE_ID)

        logger.debug("Final value of uduid:", uduid)
        return uduid
    except:
        return None


def hcOpsbDiscovery(client, ntcmd_obj, ip_address, langBund, Framework, host_cmdbid=None, host_key=None, host_macs=None,
                    uduid=None, nat_ip=None):
    'Shell, osh, str, Properties, Framework, .. -> oshVector'
    resultVector = ObjectStateHolderVector()

    ipAddress = ip_addr.IPAddress(ip_address)
    wmiProvider = wmiutils.WmicProvider(client)

    hostDiscoverer = WmiHostDiscoverer(wmiProvider)
    hostDo = hostDiscoverer.discover()

    hostDiscoverer = HostDiscovererByShell(client, langBund, Framework, hostDo)
    hostDiscoverer.discover()
    hostDo = hostDiscoverer.getResults()

    winsWmiServersDiscoverer = WmiWinsServersDiscoverer(wmiProvider, ipAddress)
    winsWmiServersDiscoverer.discover()
    winsServersIpList = winsWmiServersDiscoverer.getResults()
    if not winsServersIpList:
        winsServerDiscoverer = WinsServerDicoverer(client, ipAddress, langBund, Framework)
        winsServerDiscoverer.discover()
        winsServersIpList = winsServerDiscoverer.getResults()

    interfaceDiscoverer = WmiInterfaceDiscoverer(wmiProvider, ipAddress)
    try:
        interfaceDiscoverer.discover()
        logger.debug('Interfaces successfully discovered via wmic.')
        if not interfaceDiscoverer.getResults():
            try:
                shellIfaceDiscoverer = IpConfigInterfaceDiscoverer(client, ipAddress, Framework, langBund)
                shellIfaceDiscoverer.discover()
                ifaces = shellIfaceDiscoverer.getResults()
                interfaceDiscoverer.interfacesList.extend(ifaces)
            except:
                logger.debugException('')
    except:
        msg = logger.prepareFullStackTrace('')
        logger.debugException(msg)
        logger.warn('Failed getting interfaces information via wmic. Falling back to ipconfig.')
        interfaceDiscoverer = IpConfigInterfaceDiscoverer(client, ipAddress, Framework, langBund)
        interfaceDiscoverer.discover()

    hostDo.ipIsVirtual = interfaceDiscoverer.isIpVirtual()
    hostDo.ipIsNATed = interfaceDiscoverer.isIpNATed(nat_ip)
    interfacesList = interfaceDiscoverer.getResults()

    ucmdbversion = modeling.CmdbClassModel().version()
    dnsServersIpList = []
    dhcpServersIpList = []
    topoBuilder = TopologyBuilder(interfacesList, hostDo, ipAddress, ntcmd_obj, dnsServersIpList, dhcpServersIpList,
                                  winsServersIpList, host_cmdbid, host_key, host_macs, ucmdbversion)
    topoBuilder.build()
    # access built host OSH to update UD UID attribute
    if topoBuilder.hostOsh and uduid:
        NTCMD_Connection_Utils._updateHostUniversalDiscoveryUid(topoBuilder.hostOsh, uduid)

    topoBuilder.addResultsToVector(resultVector)

    return resultVector, topoBuilder.hostOsh


def doDiscovery(Framework, shell, client, ip, codepage, connectedShellCredId=None):
    '''Framework, Shell, BaseClient, str, str -> ObjectStateHolderVectorgetLanguage
    @raise JException: discovery failed
    '''
    vector = ObjectStateHolderVector()
    languageName = shell.osLanguage.bundlePostfix
    host_cmdbid = Framework.getDestinationAttribute('host_cmdbid')
    host_key = Framework.getDestinationAttribute('host_key')
    mac_address = Framework.getDestinationAttribute('ip_mac_address')
    if not mac_address or len(mac_address) == 0 or mac_address == 'NA':
        mac_address = None

    langBund = Framework.getEnvironmentInformation().getBundle('langNetwork', languageName)

    shellObj = createShellObj(shell, client, ip, langBund, languageName, codepage, mac_address, connectedShellCredId)
    hostID = Framework.getDestinationAttribute('host_cmdbid')
    if hostID and hostID == 'NA':
        hostID = None
    natIPs = getNatIPFromConfigurationFile()
    uname = None
    hostOsh = None
    uduid = None
    if not isinstance(shell, shellutils.NexusShell):
        uduid = getUduid(client, isStampEnabled(Framework, ip))
        logger.debug("Get UD_UNIQUE_ID:", uduid)
    if (shell.isWinOs()):
        uname = 'Win'
        try:
            wmicPath = Framework.getParameter('wmicPath')
            logger.debug("wmicPath:" + wmicPath)
            if wmicPath:
                client.execCmd('set PATH=%PATH%;' + wmicPath)
        except:
            logger.debug('Failed to add default wmic location to the PATH variable.')

        try:
            environment = shell_interpreter.Factory().create(shell).getEnvironment()
            environment.appendPath('PATH', '%WINDIR%\\system32\\wbem\\')
        except:
            logger.debug('Failed to add default wmic path.')
        _vecotr, hostOsh = hcOpsbDiscovery(shell, shellObj, ip, langBund, Framework, host_cmdbid,
                                           host_key, None, uduid, natIPs)
        vector.addAll(_vecotr)
    else:
        _vecotr = TTY_Connection_Utils.getOSandStuff(shell, shellObj, Framework, langBund, uduid, natIPs)
        vector.addAll(_vecotr)
        if shell.getClientType() == 'ssh':
            uname = netutils.getOSName(client, 'uname -a')
            if host_cmdbid and host_cmdbid != 'NA':
                hostOsh = modeling.createOshByCmdbIdString('host', host_cmdbid)
            else:
                hostOsh = modeling.createHostOSH(Framework.getDestinationAttribute('ip_address'))
        else:
            uname = netutils.getOSName(client, 'uname')

    # add HRA part disocvery for Opsb job:
    if not uname:
        logger.reportWarning('Unrecognized OS!')

    servicesByCmd = Hashtable()
    platformTrait = None
    processes = []
    connectivityEndPoints = []

    # opsb Services
    # no services discovery in non-windows machines
    if uname:
        if shell.isWinOs():
            try:
                srvcOSHs = NTCMD_HR_REG_Service_Lib.doService(shell, hostOsh, servicesByCmd, langBund, Framework)
                vector.addAll(srvcOSHs)
            except:
                errorMessage = 'Failed to discover services'
                logger.warn(errorMessage)

        # opsb Users
        try:
            if shell.isWinOs():
                vector.addAll(TTY_HR_User_Lib.disWinOs(hostOsh, shell))
            else:
                host_cmdbid and vector.addAll(TTY_HR_User_Lib.disGenericUNIX(host_cmdbid, shell))
        except:
            errorMessage = 'Failed to discover users by shell'
            logger.warn(errorMessage)

        # opsb CPUs
        if uname != 'MacOs':
            try:
                host_is_virtualt_str = Framework.getDestinationAttribute('is_virtual')
                host_is_virtual = False
                if host_is_virtualt_str and host_is_virtualt_str != 'NA' and host_is_virtualt_str.lower() in ['1',
                                                                                                              'true']:
                    host_is_virtual = True
                hrRoutine = TTY_HR_Main.getHRRoutine(uname, TTY_HR_CPU_Lib)
                vector.addAll(hrRoutine(host_cmdbid, shell, Framework, langBund, host_is_virtual))
            except:
                errorMessage = 'Failed to discover cpus by shell'
                logger.warn(errorMessage)

        # opsb Drivers

        if shell.isWinOs():
            try:
                HR_Dis_Driver_Lib.discoverDriverByWmi(client, vector, hostOsh)
            except:
                errorMessage = 'Failed to discover windows device driver by shell'
                logger.warn(errorMessage)

        # opsb iSCSIInfo

        if uname != 'MacOs':
            try:
                vector.addAll(TTY_HR_Disk_Lib.disWinOSiSCSIInfo(hostOsh, shell))
            except:
                errorMessage = 'Failed to discover iSCSI by shell'
                logger.warn(errorMessage)

        # opsb Memory

        if uname != 'MacOs':
            try:
                hrRoutine = TTY_HR_Main.getHRRoutine(uname, TTY_HR_Memory_Lib)
                vector.addAll(hrRoutine(None, shell, Framework, langBund, hostOsh=hostOsh))
            except:
                errorMessage = 'Failed to discover memory by shell'
                logger.warn(errorMessage)

        # opsb Shares

        if shell.isWinOs():
            try:
                TTY_HR_Share_Lib.discoverSharedResourcesByWmic(client, hostOsh, vector)
            except:
                errorMessage = 'Failed to discover shared resources by shell'
                logger.warn(errorMessage)

        # opsb Disks
        if uname != 'MacOs':
            try:
                hrRoutine = TTY_HR_Main.getHRRoutine(uname, TTY_HR_Disk_Lib)
                vector.addAll(hrRoutine(hostOsh, shell))
            except:
                errorMessage = 'Failed to discover disks by shell'
                logger.warn(errorMessage)

        # opsb Processes
        # get platform details

        try:
            platformTrait = process_discoverer.getPlatformTrait(shell)
            if platformTrait is None:
                raise ValueError()
            # discover processes
            if uname != 'VMkernel':
                try:
                    discoverer = process_discoverer.getDiscovererByShell(shell, platformTrait)
                    processes = discoverer.discoverAllProcesses()
                    if not processes:
                        raise ValueError()
                except:
                    errorMessage = 'Failed to discover processes by shell'
                    logger.warn(errorMessage)
            if processes and hostID:
                # save processes to DB
                process_discoverer.saveProcessesToProbeDb(processes, hostID, Framework)
                # discover packages info
                try:
                    packagesDiscoverer = process_discoverer.getPackagesDiscovererByShell(shell, platformTrait)
                    packageToExecutablePath = packagesDiscoverer.getPackagesByProcesses(processes)
                except:
                    logger.warn("Failed to get package names by processes path")

            # report processes
            processReporter = process.Reporter()
            for processObject in processes:
                processesVector = processReporter.reportProcess(hostOsh, processObject)
                vector.addAll(processesVector)
        except:
            logger.warnException("Failed to determine platform")

        # opsb RunningSoftware

        # No tcp and p2p discovery for vmkernel
        if uname != 'VMkernel':
            try:
                tcpDiscoverer = Dis_TCP.getDiscovererByShell(client, Framework, shell)
                if tcpDiscoverer is not None:
                    tcpDiscoverer.discoverTCP()
                    connectivityEndPoints = tcpDiscoverer.getProcessEndPoints()
            except:
                errorMessage = 'Failed to run tcp discovery by shell'
                logger.warn(errorMessage)

            appSign = applications.createApplicationSignature(Framework, client, shell, hostOsh=hostOsh)
            if processes:
                appSign.setProcessesManager(applications.ProcessesManager(processes, connectivityEndPoints))

            servicesInfo = applications.ServicesInfo(servicesByCmd)
            appSign.setServicesInfo(servicesInfo)

            if hostID:
                appSign.getApplicationsTopology(hostID)
            else:
                appSign.getApplicationsTopology(hostOsh)

            # opsb P2P:
            try:
                if hostID:
                    p2p = process_to_process.ProcessToProcess(Framework)
                    p2p.getProcessesToProcess()
                else:
                    logger.warn('cannot get process_to_process information, need rerun this job')
            except:
                errorMessage = 'Failed to run p2p discovery'
                logger.warn(errorMessage)

    return vector
