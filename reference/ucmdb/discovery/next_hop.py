import errormessages
import logger
import netutils
import ip_addr
import errorcodes
import errorobject
import modeling

import process
import asm_signature_loader
import asm_signature_processor
import scp

from xml.dom import minidom
from appilog.common.system.types.vectors import ObjectStateHolderVector
from com.hp.ucmdb.discovery.library.common import CollectorsParameters
from com.hp.ucmdb.discovery.library.communication.downloader.cfgfiles import PortType


def getConnectionsBySrcPort(port, connections):
    result = []
    for connection in connections:
        if connection.srcPort == port:
            result.append(connection)

    return result

#get all the ports with portProtocol="tcp" from portNumbertoPortName.xml
def getPortsFromPortNumberToPortNamexml(Framework):
    fileContent = Framework.getConfigFile(CollectorsParameters.KEY_COLLECTORS_SERVERDATA_PORTNUMBERTOPORTNAME).getText()
    dom = minidom.parseString(fileContent)
    PortsList = [element.attributes['portNumber'].value for element in dom.getElementsByTagName('portInfo') \
                        if element.attributes['portProtocol'].value.lower() == 'tcp']
    # resolve port range with delimiter ","
    newPortsList = []
    for portRange in PortsList:
        newPortsList.extend([item.strip() for item in portRange.split(",")])
    return newPortsList

def filterEndpointsByPorts(Framework, endpoints, connections):
    restrictPortNumber = Framework.getParameter('restrictPortNumber')
    restrictPortNumber = not restrictPortNumber or restrictPortNumber.lower() == 'true' or restrictPortNumber.lower() == 'on'

    logger.debug('Use %s to filter port: ' % CollectorsParameters.KEY_COLLECTORS_SERVERDATA_PORTNUMBERTOPORTNAME,
                 restrictPortNumber and 'yes' or 'no')

    result = []
    filteredresult = []
    ports = []

    if restrictPortNumber:
        unResolvedPortsList = getPortsFromPortNumberToPortNamexml(Framework)
        cfg_file = Framework.getConfigFile(CollectorsParameters.KEY_COLLECTORS_SERVERDATA_PORTNUMBERTOPORTNAME)
        unfiltered_ports = [port.getPortNumber() for port in cfg_file.getPorts(PortType.TCP)]
        #filter ports while ports in portNumbertoPortName.xml with xx.eg,ports in 5xx01,5xx04,444xx will be filtered
        ports = [port for port in unfiltered_ports if str(port) in unResolvedPortsList]
        ports = list(set(ports))
        logger.debug('The filtered ports are:',ports)


    filtered_connections = connections

    for endpoint in endpoints:
        local_endpoints = endpoint.getEndpoints()
        filtered = []
        befiltered = []
        for endp in local_endpoints:
            connections = getConnectionsBySrcPort(endp.getPort(), filtered_connections)
            for connection in connections:
                if connection.dstPort in ports:
                    filtered.append(netutils.Endpoint(connection.dstPort, endp.getProtocolType(), connection.dstAddr, 1,
                                                      endp.getPortType()))
                else:
                    befiltered.append(
                        netutils.Endpoint(connection.dstPort, endp.getProtocolType(), connection.dstAddr, 1,
                                          endp.getPortType()))

        if filtered:
            result.append(netutils.ConnectivityEndpoint(endpoint.getKey(), filtered))
            logger.debug('Results for pid %s is %s ' % (endpoint.getKey(), filtered))
        if befiltered:
            filteredresult.append(netutils.ConnectivityEndpoint(endpoint.getKey(), befiltered))
            logger.debug('Results to be filtered for pid %s is %s ' % (endpoint.getKey(), befiltered))

    return result, filteredresult


def reportProcessToPort(processes, endpoints, filteredendpoints, scpOsh, shell, localip, warninglist, bizOsh):
    OSHVResult = ObjectStateHolderVector()
    if endpoints:
        for process in processes:
            remotes = []
            for endpoint in endpoints:
                if endpoint.getKey() == process.getPid():
                    remotes.extend(endpoint.getEndpoints())
            if remotes:
                for remote in remotes:
                    address = remote.getAddress()
                    if not isinstance(address, (ip_addr.IPv4Address, ip_addr.IPv6Address)):
                        address = ip_addr.IPAddress(address)
                    port = remote.getPort()
                    if isinstance(address, ip_addr.IPv6Address):
                        logger.debug("ignore ipv6 address:", address)
                        continue
                    logger.debug("reporting remote address:", address)
                    logger.debug("reporting remote port:", port)
                    scpOshv = scp.createScpOSHV(bizOsh, None, scp.TCP_TYPE, str(address), port, None, shell, localip)
                    OSHVResult.addAll(scpOshv)

    if filteredendpoints:
        portlistbefiltered = []
        for process in processes:
            filteredremotes = []
            for endpoint in filteredendpoints:
                if endpoint.getKey() == process.getPid():
                    filteredremotes.extend(endpoint.getEndpoints())
            if filteredremotes:
                for filteredremote in filteredremotes:
                    portlistbefiltered.append(int(filteredremote.getPort()))

        if portlistbefiltered:
            portlistbefiltered = sorted(list(set(portlistbefiltered)))
            portlistbefiltered = [str(x) for x in portlistbefiltered]
            portlistbefiltered = ', '.join(portlistbefiltered)
            logger.debug("The following outgoing ports are filtered because they are not in %s: %s" %
                         (CollectorsParameters.KEY_COLLECTORS_SERVERDATA_PORTNUMBERTOPORTNAME, portlistbefiltered))
            errobj = errorobject.createError(errorcodes.PORT_NOT_IN_CONFIGFILE,
                                             [CollectorsParameters.KEY_COLLECTORS_SERVERDATA_PORTNUMBERTOPORTNAME,
                                              portlistbefiltered],
                                             "The following outgoing ports are filtered because they are not in %s: %s"
                                             % (CollectorsParameters.KEY_COLLECTORS_SERVERDATA_PORTNUMBERTOPORTNAME,
                                                portlistbefiltered))
            warninglist.append(errobj)

    return OSHVResult


def findConfigFileNextHop(Framework, hostIPs, shell, processMap, applicationResult, signatureLoader, scpOsh, bizOsh):
    OSHVResult = ObjectStateHolderVector()
    errorList = []

    application = applicationResult.application
    signature = signatureLoader.load(cit=application.getOsh().getObjectClass(),
                                     productName=application.getDiscoveredProductName() or application.getOsh().getAttributeValue(
                                         'data_name'),
                                     name=application.getName())
    APPLICATION_RESOURCE = "application_resource"
    TCP_ONLY = "tcp_only"
    RUNNING_SOFTWARE = "running_software"

    service_context = Framework.getDestinationAttribute('service_context')
    numberOfScp = 0
    if signature:
        logger.debug("found signature: name = ", signature.name)
        signatureMap = {}
        for scopeElement in signature.children:
            if not scopeElement.type in signatureMap.keys():
                signatureMap[scopeElement.type] = []
            signatureMap[scopeElement.type].append(scopeElement)

        if service_context and service_context != "" and APPLICATION_RESOURCE in signatureMap:
            logger.debug("have context, using application_resource scope")
            for signatureContent in signatureMap[APPLICATION_RESOURCE]:
                oshv = asm_signature_processor.process(Framework, signatureContent, applicationResult, shell, processMap, hostIPs, scpOsh,
                                               bizOsh)
                numberOfScp = numberOfScp + getNumOfSCP(oshv)
                OSHVResult.addAll(oshv)

            logger.debug("found %s scp for application: %s via config file for scope %s" % (numberOfScp, application.getName(), APPLICATION_RESOURCE))

        if numberOfScp == 0 and RUNNING_SOFTWARE in signatureMap:
            logger.debug("cannot find anything with application_resource scope, using running_software scope")
            for signatureContent in signatureMap[RUNNING_SOFTWARE]:
                oshv = asm_signature_processor.process(Framework, signatureContent, applicationResult, shell, processMap, hostIPs, scpOsh,
                                               bizOsh)
                numberOfScp = numberOfScp + getNumOfSCP(oshv)
                OSHVResult.addAll(oshv)

            logger.debug("found %s scp for application: %s via config file for scope %s" % (numberOfScp, application.getName(), RUNNING_SOFTWARE))

        if (not service_context or service_context == "") and TCP_ONLY in signatureMap:
            logger.debug("context is empty, using tcp_only scope")
            for signatureContent in signatureMap[TCP_ONLY]:
                oshv = asm_signature_processor.process(Framework, signatureContent, applicationResult, shell, processMap, hostIPs, scpOsh,
                                               bizOsh)
                numberOfScp = numberOfScp + getNumOfSCP(oshv)
                OSHVResult.addAll(oshv)

            logger.debug("found %s scp for application: %s via config file for scope %s" % (numberOfScp, application.getName(), TCP_ONLY))
        if int(numberOfScp) == 0:
            # report no scp reported in configfile error
            msg = errormessages.makeErrorMessage(None, message=signature.name,
                                                 pattern=errormessages.ERROR_NO_SCP_REPORTED_IN_CONFIGFILE)
            errorList.append(errorobject.createError(errorcodes.NO_SCP_REPORTED_IN_CONFIGFILE, [signature.name], msg))
    else:
        logger.debug('Not found signature for application name = ', application.getName())
        enableNoSignatureMatchError = (Framework.getParameter('enableNoSignatureMatchError') == "true")
        if enableNoSignatureMatchError:
            # report no signature matched in configfile error
            msg = errormessages.makeErrorMessage(None, message=application.getName(),
                                                 pattern=errormessages.ERROR_NO_SIGNATURE_MATCHED_IN_CONFIGFILE)
            errorList.append(errorobject.createError(errorcodes.NO_SIGNATURE_MATCHED_IN_CONFIGFILE, [application.getName()], msg))
    return OSHVResult, numberOfScp, errorList


def findTCPNextHop(Framework, hostIPs, shell, processes, connectivityEndPoints, connections, application, hostOsh,
                   scpOsh, bizOsh):
    warninglist = []
    OSHVResult = ObjectStateHolderVector()
    logger.debug("Start to do tcp discovery for application:", application.getName())

    pids = [x.getPid() for x in application.getProcesses() if x]
    logger.debug("getting pids:", pids)

    endpoints_for_pids = [x for x in connectivityEndPoints if x and x.getKey() in pids]
    processedendpoints, filteredendpoints = filterEndpointsByPorts(Framework, endpoints_for_pids, connections, )

    results = reportProcessToPort(application.getProcesses(), processedendpoints, filteredendpoints,
                                  scpOsh, shell, hostIPs, warninglist, bizOsh)
    logger.debug("found %s scp for application: %s via TCP connection" % (getNumOfSCP(results), application.getName()))
    OSHVResult.addAll(results)

    processReporter = process.Reporter()
    for processObject in processes:
        processesVector = processReporter.reportProcess(hostOsh, processObject)
        OSHVResult.addAll(processesVector)

    return OSHVResult, getNumOfSCP(results), warninglist


def getNumOfSCP(oshv):
    count = 0
    for osh in oshv or []:
        if osh.getObjectClass() == scp.SCP_TYPE:
            count += 1
    return count

    #check if the application in the disableTcp_application_list.xml
def check_if_application_disableTcp(Framework,ci_type=None,name=None,app_id=None):
    disablelist_filename = 'disableTcp_application_list.xml'
    application_disableTcp = False
    fileContent = Framework.getConfigFile(disablelist_filename).getText()
    dom = minidom.parseString(fileContent)
    for element in dom.getElementsByTagName('applicationInfo'):
        if ( (element.hasAttribute('name') and element.attributes['name'].value == name) or
                    (element.hasAttribute('app_id') and element.attributes['app_id'].value == app_id) ) \
                and (element.hasAttribute('ci_type') and element.attributes['ci_type'].value == ci_type):
            application_disableTcp = True
            logger.debug("Found the application %s in the disableTcp_application_list.xml,will not find next hop via TCP" % name)
            break
    return application_disableTcp

def doNextHop(Framework, hostIPs, OSHVResult, shell, applicationResults, processes, connectivityEndPoints, connections,
              hostOsh):
    for process in processes:
        logger.debug('the commandline is:',process.commandLine)

    portToDiscover = Framework.getDestinationAttribute("PORT")
    scp_id = Framework.getDestinationAttribute("SCP_ID")
    biz_id = Framework.getDestinationAttribute("BIZ_ID")
    enableTCP = Framework.getDestinationAttribute("ENABLE_TCP")
    scpOsh = scp.createOshById('scp', scp_id)
    bizOsh = scp.createOshById('business_element', biz_id)
    signatureLoader = asm_signature_loader.SignatureLoader(Framework)
    processMap = buildProcessMap(processes)
    numberOfSCP = 0
    errorlist = []
    for applicationResult in applicationResults:
        #todo: will change the logic here
        application = applicationResult.application
        configFileResults, configFileScp, configFileErrorList = findConfigFileNextHop(Framework, hostIPs, shell,
            processMap, applicationResult, signatureLoader, scpOsh, bizOsh)
        errorlist.extend(configFileErrorList)
        filterSCP(configFileResults, None, hostIPs, portToDiscover, Framework)
        OSHVResult.addAll(configFileResults)
        nonTcpScpSet = buildSCPSet(configFileResults)

        #check if the application is in the disableTcp_application_list
        application_disableTcp = check_if_application_disableTcp(Framework, ci_type=application.getOsh().getObjectClass(),
                                                    name=application.getOsh().getAttributeValue('data_name'),
                                                    app_id=application.getName())
        # Try to find next hop via TCP only when finding nothing from config files.
        tcpResults, tcpScp, warninglist = findTCPNextHop(Framework, hostIPs, shell, processes, connectivityEndPoints,
                                                      connections, application, hostOsh, scpOsh, bizOsh) \
            if int(configFileScp) == 0 and enableTCP == 'true' and not application_disableTcp else (ObjectStateHolderVector(), 0, [])

        errorlist.extend(warninglist)

        filterSCP(tcpResults, nonTcpScpSet, hostIPs, portToDiscover, Framework)
        OSHVResult.addAll(tcpResults)

        numberOfSCP = numberOfSCP + int(configFileScp) + int(tcpScp)
        logger.debug("number of SCP:", numberOfSCP)

    CompareScpAndMarkStatus(Framework, OSHVResult)

    if numberOfSCP == 0:
        logger.debug("reporting error")
        for warning in errorlist:
            logger.reportErrorObject(warning)

    return OSHVResult


def buildProcessMap(processes):
    processMap = {}
    for process in processes:
        processMap[process.getPid()] = process
    return processMap


# Collect all ip:port from SCPs found by configuration file
def buildSCPSet(oshv):
    scps = set()
    if oshv:
        for osh in oshv:
            if osh.getObjectClass() == scp.SCP_TYPE:
                ip = osh.getAttributeValue(scp.ATTR_SERVICE_IP_ADDRESS)
                port = osh.getAttributeValue(scp.ATTR_SERVICE_PORT)
                scps.add('%s:%s' % (ip, port))
    return scps


# Remove SCPs discovered from config files if it will point to itself.
# Also remove SCPs discovered by TCP connections if it was already discovered from config files or
# it has the same ip and port with the running software itself.
def filterSCP(oshv, scps, filter_ip=None, filter_port=0, Framework = None):
    if Framework:
        ip = Framework.getDestinationAttribute('ip_address')
        port = Framework.getDestinationAttribute("PORT")
        type = Framework.getDestinationAttribute('service_connection_type')
        context = Framework.getDestinationAttribute('service_context')
        logger.debug("filter scp to it self")
        scposhv = [osh for osh in oshv if
               osh.getObjectClass() == scp.SCP_TYPE]
        for scposh in scposhv:
            if str(type) == str(scposh.getAttributeValue('service_connection_type')) and str(
                    ip) == str(scposh.getAttributeValue('service_ip_address')) and str(
                    port) == str(scposh.getAttributeValue('service_port')) and str(
                    context) == str(scposh.getAttributeValue('service_context')):
                logger.debug("found scp to it self, remove it")
                oshv.remove(scposh)

    if not oshv or not scps:
        return

    iter = oshv.iterator()
    while iter.hasNext():
        osh = iter.next()
        if osh.getObjectClass() == scp.SCP_TYPE and osh.getAttributeValue(scp.ATTR_SERVICE_TYPE) == scp.TCP_TYPE:
            ip = osh.getAttributeValue(scp.ATTR_SERVICE_IP_ADDRESS)
            port = osh.getAttributeValue(scp.ATTR_SERVICE_PORT)
            key = '%s:%s' % (ip, port)
            if key in scps:
                logger.debug('Ignore duplicated TCP connection:%s:%s' % (ip, port))
                iter.remove()
            if ip == filter_ip and port == filter_port:
                logger.debug('Ignore TCP connection to it self:%s:%s' % (ip, port))
                iter.remove()
            else:
                scps.add(key)


def CompareScpAndMarkStatus(Framework, OSHVResult):
    logger.debug('Try to mark status for scp')
    discovered_scp_id = Framework.getTriggerCIDataAsList('discovered_scp_id')
    discovered_scp_type = Framework.getTriggerCIDataAsList('discovered_scp_type')
    discovered_scp_ip = Framework.getTriggerCIDataAsList('discovered_scp_ip')
    discovered_scp_port = Framework.getTriggerCIDataAsList('discovered_scp_port')
    discovered_scp_context = Framework.getTriggerCIDataAsList('discovered_scp_context')
    discovered_scp_status = Framework.getTriggerCIDataAsList('discovered_scp_status')

    scp_id = Framework.getDestinationAttribute("SCP_ID")
    scpOsh = scp.createOshById('scp', scp_id)

    scposhv = [osh for osh in OSHVResult if
               osh.getObjectClass() == scp.SCP_TYPE]
    discovery_status_map = {}
    for scpitem in scposhv:
        discovery_status_map[scpitem] = scp.DISCOVERY_STATUS_NEW

    for i in range(0, len(discovered_scp_id)):
        if not discovered_scp_id[i]:
            continue
        if str(discovered_scp_status[i]) == str(scp.DISCOVERY_STATUS_MANUALLY_ADDED):
            logger.debug('scp %s is manually added, will not change its status')
            continue
        discovered_this_time = False
        for osh in scposhv:
            if discovered_scp_context[i] == 'NA':
                discovered_scp_context[i] = None
            if str(discovered_scp_type[i]) == str(osh.getAttributeValue('service_connection_type')) and str(
                    discovered_scp_ip[i]) == str(osh.getAttributeValue('service_ip_address')) and str(
                    discovered_scp_port[i]) == str(osh.getAttributeValue('service_port')) and str(
                    discovered_scp_context[i]) == str(osh.getAttributeValue('service_context')):
                logger.debug('SCP already discovered last time, mark the relation active:', osh.toXmlString())
                discovered_this_time = True
                discovery_status_map[osh] = scp.DISCOVERY_STATUS_ACTIVE
                break
        if not discovered_this_time:
            logger.debug('SCP not discovered this time, mark the relation de_active:', discovered_scp_id[i])
            discovered_scp_osh = scp.createOshById('scp', discovered_scp_id[i])
            cplinkOsh = modeling.createLinkOSH('consumer_provider', scpOsh, discovered_scp_osh)
            cplinkOsh.setEnumAttribute('discovery_status', scp.DISCOVERY_STATUS_DEACTIVE)
            OSHVResult.add(discovered_scp_osh)
            OSHVResult.add(cplinkOsh)

    for scpitem in scposhv:
        logger.debug('-----------', scpitem.getAttributeValue('service_ip_address'))
        if scpitem.getAttributeValue('service_ip_address') and scpitem.getAttributeValue('service_ip_address') != 'None':
            cplinkOsh = modeling.createLinkOSH('consumer_provider', scpOsh, scpitem)
            cplinkOsh.setEnumAttribute('discovery_status', discovery_status_map[scpitem])
            OSHVResult.add(cplinkOsh)
