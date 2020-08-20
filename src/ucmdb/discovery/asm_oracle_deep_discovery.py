import logger
import re
import netutils
import dbconnect_oracle
import dbconnect_utils
import scp
import modeling
import dbconnect_win_shellutils
import dbconnect_unix_shellutils
import oracle_shell_utils

from appilog.common.system.types.vectors import ObjectStateHolderVector
from appilog.common.system.types import ObjectStateHolder


def discover(Framework, shell, client, runningApplication, OSHVResult, hostOsh):
    logger.debug("check if there are tns listener in the results")
    found = False
    for osh in OSHVResult:
        if osh.getObjectClass() == 'oracle_listener':
            found = True
            break

    if not found:
        logger.debug("no tns listener in the results, will skip this turn")
        return

    context = Framework.getDestinationAttribute('service_context').lower()
    ip = Framework.getDestinationAttribute('ip_address')
    scp_id = Framework.getDestinationAttribute("SCP_ID")
    portToDiscover = Framework.getDestinationAttribute("PORT")
    databaseDict = {}
    serviceNameToSIDMap = {}
    isWindows = 'false'
    osName = None
    if shell.isWinOs():
        osName = 'Windows'
        isWindows = 'true'
    else:
        if shell.getClientType() == 'ssh':
            osName = netutils.getOSName(client, 'uname -a')
        else:
            osName = netutils.getOSName(client, 'uname')

    if (client and osName):

        osNameLower = osName.lower()

        if re.search("windows", osNameLower):
            processToPortDict = dbconnect_win_shellutils.getProcToPortDictOnWindows(client, Framework)
        elif re.search("aix", osNameLower):
            processToPortDict = dbconnect_unix_shellutils.getProcToPortDictOnAIX(client, True, True)
        elif re.search("linux", osNameLower):
            processToPortDict = dbconnect_unix_shellutils.getProcToPortDictOnLinux(client, True, True)
        elif re.search("sun", osNameLower):
            processToPortDict = dbconnect_unix_shellutils.getProcToPortDictOnSolaris(client, True, True)
        elif re.search("hp-ux", osNameLower):
            processToPortDict = dbconnect_unix_shellutils.getProcToPortDictOnHPUX(client, True, True)
        else:
            dbconnect_utils.debugPrint('Unknown operating system')

    findDatabases(shell, client, processToPortDict, databaseDict, isWindows, serviceNameToSIDMap)
    logger.debug(databaseDict)
    if databaseDict != None and len(databaseDict) > 0:
        databaseOSHV = ObjectStateHolderVector()
        if not context or context == '':
            logger.debug('The context is empty, will report all the oracle databases')
            for db in databaseDict.keys():
                logger.debug('reporting database:', db)
                reportDatabase(db.upper(), hostOsh, ip, portToDiscover, databaseOSHV, scp_id)

        logger.debug('try to use instance name to filter:', context)
        if context in databaseDict.keys():
            logger.debug('Found <%s> instance <%s> (%s) with listener port <%s:%s> and installed in <%s>' % (
                (databaseDict[context])[dbconnect_utils.DBTYPE_INDEX], context,
                (databaseDict[context])[dbconnect_utils.STATUS_INDEX],
                (databaseDict[context])[dbconnect_utils.IP_INDEX],
                (databaseDict[context])[dbconnect_utils.PORT_INDEX],
                (databaseDict[context])[dbconnect_utils.PATH_INDEX]))

            reportDatabase(context.upper(), hostOsh, ip, portToDiscover, databaseOSHV, scp_id)

        logger.debug('try to use service name to filter:', context)
        if context in serviceNameToSIDMap.keys():
            for instance in serviceNameToSIDMap[context]:
                logger.debug('Found <%s> instance <%s> (%s) with listener port <%s:%s> and installed in <%s>' % (
                    (databaseDict[instance])[dbconnect_utils.DBTYPE_INDEX], instance,
                    (databaseDict[instance])[dbconnect_utils.STATUS_INDEX],
                    (databaseDict[instance])[dbconnect_utils.IP_INDEX],
                    (databaseDict[instance])[dbconnect_utils.PORT_INDEX],
                    (databaseDict[instance])[dbconnect_utils.PATH_INDEX]))
                reportDatabase(instance.upper(), hostOsh, ip, portToDiscover, databaseOSHV, scp_id)

        if not databaseOSHV.isEmpty():
            logger.debug('remove all tns listener')
            removelist = []
            for osh in OSHVResult:
                if osh.getObjectClass() == 'oracle_listener':
                    logger.debug("remove tns listener osh:", osh)
                    removelist.append(osh)
                if osh.getObjectClass() == 'ownership' and osh.getAttributeValue(
                        'link_end1').getObjectClass() == 'oracle_listener':
                    logger.debug("remove ownership of tns listener osh:", osh)
                    removelist.append(osh)

            for osh in removelist:
                OSHVResult.remove(osh)

            OSHVResult.addAll(databaseOSHV)


def reportDatabase(databaseName, container, ip, port, OSHVResult, scp_id):
    dbServerOSH = ObjectStateHolder('oracle')
    dbServerOSH.setStringAttribute('data_name', 'Oracle DB')
    dbServerOSH.setStringAttribute('vendor', 'oracle_corp')
    dbServerOSH.setStringAttribute('product_name', 'oracle_database')
    dbServerOSH.setStringAttribute('database_dbtype', 'oracle')
    dbServerOSH.setStringAttribute('application_ip', ip)
    dbServerOSH.setStringAttribute('database_dbsid', databaseName)
    dbServerOSH.setStringAttribute('application_category', 'Database')
    dbServerOSH.setContainer(container)
    OSHVResult.add(dbServerOSH)
    logger.debug("create ip service endpoint (%s:%s) for db instance:%s" % (ip, port, databaseName))
    ipServerOSH = modeling.createServiceAddressOsh(container, ip, port,
                                                   modeling.SERVICEADDRESS_TYPE_TCP, 'oracle')
    OSHVResult.add(ipServerOSH)
    OSHVResult.add(modeling.createLinkOSH('usage', dbServerOSH, ipServerOSH))
    OSHVResult.addAll(scp.createOwnerShip(scp_id, dbServerOSH))


def findDatabases(shell, localClient, procToPortDict, dbInstanceDict, isWindows='true', serviceNameToSIDMap={}):
    try:
        ## Extract information from process to port dictionary first
        dbconnect_oracle.processProcToPortDict(localClient, procToPortDict, dbInstanceDict)

        ## Search for tnsnames.ora if we have a shell connection
        if localClient.getClientType() != 'wmi' and localClient.getClientType() != 'snmp':
            if not getInformationFromListeners(shell, localClient, procToPortDict, dbInstanceDict, serviceNameToSIDMap):
                install_locs = dbconnect_oracle.parseEtcFiles(localClient, procToPortDict, dbInstanceDict, isWindows)
                dbconnect_oracle.findTnsnamesOra(localClient, procToPortDict, dbInstanceDict, isWindows, install_locs)
    except:
        excInfo = logger.prepareJythonStackTrace('')
        logger.debug('findDatabases Exception: <%s>' % excInfo)
        pass


def getInformationFromListeners(shell, client, procToPortDict, dbInstanceDict, serviceNameToSIDMap):
    env = oracle_shell_utils.getEnvConfigurator(shell)
    is_fully_discovered = 1
    for pid in procToPortDict.keys():
        processName = (procToPortDict[pid])[dbconnect_utils.PROCESSNAME_INDEX].lower()
        processPath = (procToPortDict[pid])[dbconnect_utils.PATH_INDEX]
        if re.search('tnslsnr', processName) or re.search('tnslistener', processName):
            logger.debug('Found listener with path "%s"' % processPath)
            env.setOracleHomeEnvVar(processPath)
            m = re.match(r"(.+)[\\\/]+tns.*", processPath)
            if m:
                output = shell.execCmd('%s/lsnrctl status' % m.group(1))
                if not (output and shell.getLastCmdReturnCode() == 0):
                    is_fully_discovered = 0
                    #dbDict[sidFound] = ['oracle', tnslsnrPort, ipAddress, installPath, version, statusFlag]
                ip_port, service_instance, version = parseListenerOutput(output)
                for service, instances in service_instance:
                    serviceNameToSIDMap[service.lower()] = instances
                    for instance in instances:
                        ip = None
                        port = None
                        if ip_port:
                            ip, port = ip_port[0]
                        details = dbInstanceDict.get(instance, [])
                        if details:
                            #instance already found previously
                            if details[1] == dbconnect_utils.UNKNOWN:
                                details[1] = port
                            if details[2] == dbconnect_utils.UNKNOWN:
                                details[2] = ip
                            details[4] = version
                            dbInstanceDict[instance] = details
                        else:
                            dbInstanceDict[instance] = ['oracle', port, ip, m.group(1), version,
                                                        dbconnect_utils.UNKNOWN]
    return is_fully_discovered


def parseListenerOutput(output):
    if output:
        ip_port = []
        service_instance = []
        version = None
        match = re.findall('HOST=([\w\.\-]+).*PORT=(\d+)', output)
        if match:
            ip_port = match
        match = re.findall(r'Service "([\w\-\+\.]+)" has\s*(\S+)\s*.*[\r\n]+((.*Instance.*[\r\n]+)*)', output)
        if match:
            for service, num, instancecontent, nouse in match:
                if service.lower().endswith('xdb') or service.lower().find('extrpc') != -1:
                    continue

                instances = re.findall(r'Instance\s*"([\w\-\+\.]+)",\s*status', instancecontent)
                service_instance.append([service, instances])
        match = re.search('Version\s+([\d\.]+)', output)
        if match:
            version = match.group(1)
        return ip_port, service_instance, version
