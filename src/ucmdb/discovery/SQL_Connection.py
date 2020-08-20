#coding=utf-8
from java.lang import Exception as JavaException
from java.util import Properties, HashSet
from dbutils import DbTypes
from com.hp.ucmdb.discovery.common import CollectorsConstants
import db
import db_builder
import re
import errormessages
import string
import logger
import modeling
import netutils
import protocol

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector
from com.hp.ucmdb.discovery.library.clients import ClientsConsts, \
    MissingJarsException
from file_ver_lib import resolveMSSQLVersion, resolveDB2Version, resolveOracleVersion
from appilog.common.utils import Protocol
from java.lang import Boolean

from SQL_Oracle_Additionals import *


#depending on dbType provides DB version resolver and CMDB CI type
dbMetaParams = {DbTypes.Oracle: (resolveOracleVersion, 'oracle'),
                DbTypes.MsSqlServer: (resolveMSSQLVersion, 'sqlserver'),
                DbTypes.MsSqlServerNtlm: (resolveMSSQLVersion, 'sqlserver'),
                DbTypes.MsSqlServerNtlmV2: (resolveMSSQLVersion, 'sqlserver'),
                DbTypes.Db2: (resolveDB2Version, 'db2'),
                DbTypes.MySql: (str, 'mysql'),
                DbTypes.Sybase: (str, 'sybase'),
                DbTypes.PostgreSQL: (str, 'postgresql'),
                DbTypes.MaxDB: (str, 'maxdb'),
                DbTypes.HanaDB: (str, 'hana_instance')}

def createDatabaseOSH(hostOSH, client, sid, dbVersion, appVersion, buildNumber=None, edition=None):
    protType = client.getProtocolDbType().lower()
    (versionResolver, databaseType) = dbMetaParams[protType]
    applicationVersionNumber = versionResolver(dbVersion)
    return modeling.createDatabaseOSH(databaseType,
                                      sid,
                                      str(client.getPort()),
                                      client.getIpAddress(),
                                      hostOSH,
                                      client.getCredentialId(),
                                      client.getUserName(),
                                      client.getTimeout(),
                                      dbVersion,
                                      appVersion,
                                      applicationVersionNumber,
                                      buildNumber,
                                      edition)

class OracleClientWrapper:
    IS_RAC_QUERY = ("select sum(clustered) clustered "
                    "from ( "
                    "SELECT decode(VALUE, null,0, 1) clustered "
                    "from V$SPPARAMETER "
                    "WHERE NAME = 'cluster_database' "
                    "union "
                    "SELECT decode(VALUE, 'FALSE', 0, 1) clustered "
                    "from V$PARAMETER "
                    "WHERE NAME = 'cluster_database')")

    def __init__(self, client):
        self._client = client
        self.__dbIpAddress = None

    def getOracleServerIP(self):
        try:
            host_address_result = self._client.executeQuery("select UTL_INADDR.get_host_address from dual")
            while host_address_result.next():
                ip = host_address_result.getString(1)
                if netutils.isValidIp(ip) and not netutils.isLoopbackIp(ip):
                    return ip
        except:
            logger.debugException('')

    def getOracleServerName(self):
        try:
            host_name_result = self._client.executeQuery("select UTL_INADDR.get_host_name(UTL_INADDR.get_host_address) from dual")
            while host_name_result.next():
                return host_name_result.getString(1)
        except:
            logger.debugException('')

    def getOracleServerNameByInstance(self):
        try:
            resultHost = self._client.executeQuery("select HOST_NAME from V$INSTANCE where upper(INSTANCE_NAME) = '%s'" % self._client.getSid().upper())
            while resultHost.next():
                return resultHost.getString(1)
        except:
            logger.debugException('')

    def isRacInstance(self):
        try:
            resultSet = self._client.executeQuery(OracleClientWrapper.IS_RAC_QUERY)
            if resultSet.next():
                return int(resultSet.getString(1))
        except:
            logger.debugException('')
            logger.warn('Failed to identify if it is a RAC instance. Assuming stand alone system.')
            return 0

    def getListeningIpAddress(self):
        if self.__dbIpAddress:
            return self.__dbIpAddress
        try:
            if not self.isRacInstance():
                self.__dbIpAddress = self._client.getIpAddress()
                return self.__dbIpAddress

            direct_ip = self.getOracleServerIP()
            server_name = self.getOracleServerName() or self.getOracleServerNameByInstance()
            probe_side_ip = None
            try:
                raw_probe_side_ip = netutils.getHostAddress(server_name)
                if netutils.isValidIp(raw_probe_side_ip) and not netutils.isLoopbackIp(raw_probe_side_ip):
                    probe_side_ip = raw_probe_side_ip
            except:
                logger.debugException('')


            if direct_ip and not probe_side_ip:
                self.__dbIpAddress = direct_ip

            if not direct_ip and probe_side_ip:
                self.__dbIpAddress = probe_side_ip

            if direct_ip and probe_side_ip:
                self.__dbIpAddress = probe_side_ip

            if self.__dbIpAddress:
                return self.__dbIpAddress

            raise ValueError('Server ip appeared to be incorrect')
        except:
            logger.debugException('')
            logger.reportWarning('Failed to queue oracle server ip. Will report ip used for connection.')
            self.__dbIpAddress = self._client.getIpAddress()
            return self._client.getIpAddress()

    def __getattr__(self, name):
        if name == 'getIpAddress':
            return self.getListeningIpAddress
        return getattr(self._client, name)


################################################################################
###                 query database service ip                                ###
################################################################################
### SQL> select utl_inaddr.get_host_address from dual;
###
### GET_HOST_ADDRESS
### --------------------------------------------------------------------------------
### 10.194.10.77
################################################################################

# def getDBServiceIP(dbClient, framework=None):
#     logger.debug('Query database service ip for protocol type:', DbTypes.Oracle)
#     hostAddress = ''
#     try:
#         res = dbClient.executeQuery('select utl_inaddr.get_host_address from dual')
#         while res.next():
#             hostAddress = res.getString(1)
#             logger.debug('host Address:', hostAddress)
#         res.close()
#     except:
#         logger.debugException('')
#     return hostAddress

#################################################################################

################################################################################
###                 query database node number  （节点号）                    ###
################################################################################
# SQL> select instance_number from v$instance;
#
# INSTANCE_NUMBER
# ---------------
# 1
################################################################################
def getOracleNodeNumber(dbClient, framework=None):
    logger.debug('Query database instance number for protocol type:', DbTypes.Oracle)
    instanceNumber = ''
    try:
        res = dbClient.executeQuery("select instance_number from V$INSTANCE")
        while res.next():
            instanceNumber = res.getString(1)
            logger.debug('oracle instance number:', instanceNumber)
        res.close()
    except:
        logger.debugException('')
    return instanceNumber

#################################################################################


################################################################################
###                 query database max connection  （最大连接数）             ###
################################################################################
# SQL> select t.LIMIT_VALUE from V$RESOURCE_LIMIT T  where resource_name = 'sessions';
#
# LIMIT_VALUE
# --------------------------------------------------------------------------------
# 3776
################################################################################
def getOracleMaxConnection(dbClient, framework=None):
    logger.debug('Query database max connection for protocol type:', DbTypes.Oracle)
    maxConnection = ''
    try:
        res = dbClient.executeQuery("select LIMIT_VALUE from V$RESOURCE_LIMIT where resource_name = 'sessions'")
        while res.next():
            maxConnection = res.getString(1)
            logger.debug('oracle max connection:', maxConnection)
        res.close()
    except:
        logger.debugException('')
    return maxConnection

#################################################################################

def getDbSid(dbClient, framework=None):
    protType = dbClient.getProtocolDbType().lower()
    logger.debug('Getting sid for protocol type ', protType)
    instanceName = None
    if protType == DbTypes.Sybase:
        #have to be verified; if the query returns the same as client, code have to be removed
        res = dbClient.executeQuery("select srvnetname from master..sysservers where srvid = 0")#@@CMD_PERMISION sql protocol execution
        if res.next():
            instanceName = string.strip(res.getString(1))
    elif protType == DbTypes.HanaDB:
        instanceName = getHanaDBSID(dbClient, framework)
    elif protType in (DbTypes.Oracle, DbTypes.MsSqlServer, DbTypes.MsSqlServerNtlm, DbTypes.MsSqlServerNtlmV2):
        instanceName = dbClient.getSid()
    elif protType in DbTypes.AllList:
        instanceName = dbClient.getDatabaseName()
    else:
        errorMessage = 'Database type ' + str(protType) + 'not supported'
        raise ValueError, errorMessage
    logger.debug('sid received: %s' % instanceName)
    return instanceName

def getHanaDBSID(dbClient, framework):
    if framework:
        host_name = framework.getTriggerCIData("host_name")
        if host_name:
            databaseSID = getHanaDBSIDByHost(dbClient, host_name)
            if not databaseSID:
                databaseSID = getHanaDBSIDByVirtualHost(dbClient, host_name)
            return databaseSID

def getHanaDBSIDByHost(dbClient, host_name):
    res = dbClient.executeQuery("select value from sys.m_host_information where key='sid' and host=\'" + host_name + "\'")#@@CMD_PERMISION sql protocol execution
    if res.next():
        return res.getString(1).strip()


def getHanaDBSIDByVirtualHost(dbClient, host_name):
    res = dbClient.executeQuery("select host from sys.m_host_information where key='net_hostnames' and value like \'%" + host_name + "%\'")#@@CMD_PERMISION sql protocol execution
    if res.next():
        return getHanaDBSIDByHost(dbClient, res.getString(1).strip())


def getBuildNumber(dbClient):
    buildNumber = None
    protType = dbClient.getProtocolDbType().lower()
    logger.debug('Query build number for protocol type:', protType)
    if protType in (DbTypes.MsSqlServer, DbTypes.MsSqlServerNtlm, DbTypes.MsSqlServerNtlmV2):
        try:
            res = dbClient.executeQuery("SELECT SERVERPROPERTY('ProductVersion')")
            if res.next():
                result = res.getString(1)
                if result:
                    buildNumber = result.strip()
            res.close()
        except:
            logger.debugException('')

    logger.debug("Build number is:", buildNumber)
    return buildNumber

def getEdition(dbClient):
    edition = None
    protType = dbClient.getProtocolDbType().lower()
    logger.debug('Query edition for protocol type:', protType)
    if protType in (DbTypes.MsSqlServer, DbTypes.MsSqlServerNtlm, DbTypes.MsSqlServerNtlmV2):
        try:
            res = dbClient.executeQuery("SELECT SERVERPROPERTY('Edition')")
            if res.next():
                result = res.getString(1)
                if result:
                    edition = result.strip()
            res.close()
        except:
            logger.debugException('')

    logger.debug("Edition is:", edition)
    return edition

def getServices(dbClient):
    services = []
    logger.debug('Query services for protocol type:', DbTypes.Oracle)
    try:
        res = dbClient.executeQuery("SELECT NAME, PDB from V$SERVICES WHERE NETWORK_NAME IS NOT NULL")
        while res.next():
            name = res.getString(1)
            pdb = res.getString(2).strip() != '' and res.getString(2) != 'CDB$ROOT'

            service = db.OracleServiceName(name)
            service.setPdb(pdb)
            services.append(service)
        res.close()
    except:
        logger.debugException('')
        try:
            res = dbClient.executeQuery("SELECT NAME from V$SERVICES WHERE NETWORK_NAME IS NOT NULL")
            while res.next():
                name = res.getString(1)
                service = db.OracleServiceName(name)
                services.append(service)
            res.close()
        except:
            logger.debugException('')
    return services


def getMsSqlServerSidePort(client):
    query= '''SELECT distinct local_tcp_Port
            FROM sys.dm_exec_connections
            WHERE local_tcp_port is not null and session_id = @@SPID'''
    listen_ports = []
    res = None
    try:
        try:
            res = client.executeQuery(query)
            while res.next():
                listen_ports.append(res.getString(1))
        except:
            logger.warn('Failed to get listen port from MS SQL database')
            logger.debugException('')
    finally:
        if res:
            res.close()
    return listen_ports

def isMsSqlConnectionPortValid(Framework, client):
    (_, db_type) = dbMetaParams[client.getProtocolDbType().lower()]

    if db_type == 'sqlserver':
        try:
            filterForwardedPorts = Boolean.parseBoolean(Framework.getParameter('handleSQLBrowserMappings'))
        except:
            logger.debugException('')
            filterForwardedPorts = False
        if filterForwardedPorts:
            listening_ports = getMsSqlServerSidePort(client)
            if listening_ports and not (str(client.getPort()) in listening_ports ):
                logger.warn('Encountered a situation when connection port is not among listening ports of database. Skipping.')
                return False
    return True


def querySingleRecordFromDB(dbClient, sql, *indexes):
    logger.debug('Query value from db:', sql)
    res = None
    final_result = {}
    try:
        res = dbClient.executeQuery(sql)
        if res.next():
            for index in indexes:
                result = res.getString(index)
                final_result[index] = result
    except:
        logger.debugException('')
    finally:
        if res:
            res.close()

    logger.debug("Result is:", final_result)
    return final_result


def addExtraInformationToDB(dbOsh, dbClient):
    protType = dbClient.getProtocolDbType().lower()
    if protType == DbTypes.Sybase:
        logger.debug('Query emergency_bug_fix from sybase')
        result = querySingleRecordFromDB(dbClient, 'SELECT @@version', 1)
        if result:
            ebfStr = result[1]
            if ebfStr:
                m = re.search(r'EBF\s*(\d+)', ebfStr)
                if m:
                    ebf = m.group(1)
                    logger.debug('emergency_bug_fix of sybase is:', ebf)
                    dbOsh.setStringAttribute('emergency_bug_fix', ebf)


def discoverDB(Framework, client, OSHVResult, reportedSids):

    if not isMsSqlConnectionPortValid(Framework, client):
        return

    '''
    2018/12/19  修改  修复oracle RAC数据库和主机关系
    原因:
    1) 由于oracle RAC 服务IP是绑定在当前活动的节点上，它会切换。
    2) oracle 连接会尝试所有的IP, 通过服务IP连接成功，会用服务IP创建HostOSH
    3) RAC 服务IP 切换以后，会造成oracle 和服务器的关系错位，造成oracle instance 重复
    修复:
    通过获取当前trigger node 的global ID, 唯一确认node 和oracle 的关系
    
    '''    
    host_id = Framework.getTriggerCIData("hostId")
    logger.debug('host_id ==', host_id)
    hostOSH = ObjectStateHolder('node')
    hostOSH.setStringAttribute("global_id", host_id)
    # hostOSH = modeling.createHostOSH(client.getIpAddress(), 'node')
    endpoint_builder = netutils.ServiceEndpointBuilder()
    endpoint_reporter = netutils.EndpointReporter(endpoint_builder)

    oracle_builder = db_builder.Oracle()
    reporter = db.OracleTopologyReporter(oracle_builder, endpoint_reporter)

    serviceName = client.getProperty(Protocol.SQL_PROTOCOL_ATTRIBUTE_DBSID)
    sid = getDbSid(client, Framework)
    #    if sid:
    #        sid = sid.upper()
    uniqueSID = (sid, client.getPort())
    if uniqueSID in reportedSids:
        logger.info('SID %s on port %s already reported' % (sid, client.getPort()))
        if client.getProtocolDbType().lower() == DbTypes.Oracle:
            logger.debug('report service:', serviceName)
            listener = db.OracleListener(client.getIpAddress(), client.getPort())
            OSHVResult.addAll(reporter.reportTnsListener(listener, hostOSH))
            oracleServices = []
            service = db.OracleServiceName(serviceName)
            service.setCredentialId(client.getCredentialId())
            oracleServices.append(service)
            OSHVResult.addAll(reporter.reportServiceNameTopology(oracleServices, listener.getOsh()))
            return
    reportedSids.append(uniqueSID)
    buildNumber = getBuildNumber(client)
    edition = getEdition(client)

    #补充信息
    oracleAdditionals = SQL_Oracle_Additionals.oracleDiscAddtional()
    oracle_service_name = oracleAdditionals.getOracleServiceName(client)
    oracle_character_set = oracleAdditionals.getOracleCharacterset(client)
    oracle_patch_version = oracleAdditionals.getOraclePatchVersion(client)
    oracle_is_rac = oracleAdditionals.getOracleIsRac(client)
    oracle_sessions = oracleAdditionals.getOracleSessions(client)
    oracle_processes = oracleAdditionals.getOracleProcesses(client)
    oracle_open_cursors = oracleAdditionals.getOracleOpernCursors(client)
    oracle_audit_trails = oracleAdditionals.getOracleAuditTrails(client)
    oracle_sga_size = oracleAdditionals.getOracleSgaSize(client)
    oracle_pga_size = oracleAdditionals.getOraclePgaSize(client)
    oracle_shared_pool = oracleAdditionals.getOracleSharedPool(client)
    oracle_log_mode = oracleAdditionals.getOracleLogMode(client)
    oracle_open_mode = oracleAdditionals.getOracleOpenMode(client)
    oracle_db_role = oracleAdditionals.getOracleDbRole(client)
    oracle_force_logging = oracleAdditionals.getOracleForceLogging(client)
    oracle_users = oracleAdditionals.getOracleUsers(client)
    oracle_created_date = oracleAdditionals.getOracleCreatedDate(client)

    oracle_db_id = oracleAdditionals.getOracleDbId(client)  # added 2018/08/16
    oracle_memory_target = oracleAdditionals.getOracleMemoryTarget(client)    # added 2018/08/16

    logger.debug('=================== sid ==', sid)
    databaseServer = createDatabaseOSH(hostOSH, client, sid, client.getDbVersion(), client.getAppVersion(), buildNumber, edition)

    #填充属性到database Server
    databaseServer.setAttribute('z_services',oracle_service_name)
    databaseServer.setAttribute('z_characterset',oracle_character_set)
    databaseServer.setAttribute('z_dbpatch',oracle_patch_version)
    databaseServer.setAttribute('z_israc',oracle_is_rac)
    databaseServer.setAttribute('z_sessions',oracle_sessions)
    databaseServer.setAttribute('z_processes',oracle_processes)
    databaseServer.setAttribute('z_open_cursors',oracle_open_cursors)
    databaseServer.setAttribute('z_audit_trail',oracle_audit_trails)
    databaseServer.setAttribute('z_sga_size',oracle_sga_size)
    databaseServer.setAttribute('z_pga_size',oracle_pga_size)
    databaseServer.setAttribute('z_sharedpool_size',oracle_shared_pool)
    databaseServer.setAttribute('z_log_mode',oracle_log_mode)
    databaseServer.setAttribute('z_open_mode',oracle_open_mode)
    databaseServer.setAttribute('z_dbrole',oracle_db_role)
    databaseServer.setAttribute('z_force_logging',oracle_force_logging)
    databaseServer.setAttribute('z_users',oracle_users)
    databaseServer.setDateAttribute('z_createdate', oracle_created_date)
    databaseServer.setAttribute('z_dbid', oracle_db_id)     #added 2018/08/16
    databaseServer.setAttribute('z_memory_target', oracle_memory_target)    #added 2018/08/16

    addExtraInformationToDB(databaseServer, client)
    ipCommunicationEndpoint = modeling.createServiceAddressOsh(hostOSH, client.getIpAddress(), str(client.getPort()), modeling.SERVICEADDRESS_TYPE_TCP)
    usageLink = modeling.createLinkOSH('usage', databaseServer, ipCommunicationEndpoint)

    ### set database service ip ###
    ### set database instance number ###
    ### set database max connection ###
    #logger.debug('start to query database service ip')
    #    logger.debug('start to query database instance number')
    #    logger.debug('start to query database max connection')
    #    #db_serviceip = getDBServiceIP(client)
    #    oracle_nodenumber = getOracleNodeNumber(client)
    #    oracle_maxconnection = getOracleMaxConnection(client)
    #    # if db_serviceip:
    #    #     databaseServer.setAttribute('service_ip', db_serviceip)
    #    if oracle_nodenumber:
    #        databaseServer.setAttribute('oracle_nodenumber', oracle_nodenumber)
    #    if oracle_maxconnection:
    #        databaseServer.setAttribute('oracle_maxconnection', oracle_maxconnection)

    ###############################

    OSHVResult.add(databaseServer)
    OSHVResult.add(ipCommunicationEndpoint)
    OSHVResult.add(usageLink)

    if client.getProtocolDbType().lower() == DbTypes.Oracle:
        services = getServices(client)
        if services:
            oracleServices = []
            listener = db.OracleListener(client.getIpAddress(), client.getPort())
            OSHVResult.addAll(reporter.reportTnsListener(listener, hostOSH))

            for service in services:
                oracleServices.append(service)
            OSHVResult.addAll(reporter.reportServiceNameTopology(oracleServices, listener.getOsh(), databaseServer))

def getValidOracleClient(ipAddress, client, props, protocol, Framework):
    '''reported ip from Oracle differs from the IP used for connection
    need to check if there's a connectivity via reported IP, if no IP used for connection will be reported
    @return: client or wrapped client
    '''
    wrappedClient = OracleClientWrapper(client)
    hostIp = wrappedClient.getIpAddress()
    if ipAddress != hostIp:
        props.setProperty(CollectorsConstants.DESTINATION_DATA_IP_ADDRESS, hostIp)
        try:
            testClient = Framework.createClient(protocol, props)
            if testClient:
                testClient.close()
                return wrappedClient

        except:
            logger.warn('Failed to connect using IP reported by oracle. Will report connection ip.')
    return client

def getServiceNameCredentialId(Framework, serviceList, protocol, oracleListenerOSH, ipAddressList, destinationPortList, OSHVResult):
    for service in serviceList:
        client = None
        serviceNameList = []
        if service.getCredentialId() is None:
            try:
                for ipAddress in ipAddressList:
                    if netutils.DOMAIN_SCOPE_MANAGER.isIpOutOfScope(ipAddress):
                        continue
                    for destinationPort in destinationPortList:
                        try:
                            props = Properties()
                            props.setProperty(CollectorsConstants.DESTINATION_DATA_IP_ADDRESS, ipAddress)
                            props.setProperty(CollectorsConstants.PROTOCOL_ATTRIBUTE_PORT, destinationPort)
                            props.setProperty(Protocol.SQL_PROTOCOL_ATTRIBUTE_DBSID, service.getName())
                            client = Framework.createClient(protocol, props)
                        except:
                            continue
            except:
                logger.info('Failed to connect using service name: %s with protocol %s.' % (service.getName(), protocol))
            if client:
                serviceNameList.append(service)
                service.setCredentialId(client.getCredentialId())
                endpoint_builder = netutils.ServiceEndpointBuilder()
                endpoint_reporter = netutils.EndpointReporter(endpoint_builder)
                oracle_builder = db_builder.Oracle()
                reporter = db.OracleTopologyReporter(oracle_builder, endpoint_reporter)
                OSHVResult.addAll(reporter.reportServiceNameTopology(serviceNameList, oracleListenerOSH))
                client.close()

def connectByProtocol(Framework, protocol, ipAddressList, destinationPortList, connectionSourceList, protocolType, reportedSids, errorsList, connectedTargetList):
    OSHVResult = ObjectStateHolderVector()

    from java.lang import Boolean
    if Framework.getParameter('tryAllCredentials'):
        skipConnectedDb = not Boolean.parseBoolean(Framework.getParameter('tryAllCredentials'))
    else:
        skipConnectedDb = False

    ipAddressList = list(set(ipAddressList))
    destinationPortList = list(set(destinationPortList))
    connectionSourceList = list(set(connectionSourceList))

    for ipAddress in ipAddressList:
        if netutils.DOMAIN_SCOPE_MANAGER.isIpOutOfScope(ipAddress):
            continue
        for destinationPort in destinationPortList:
            for item in connectionSourceList:
                # If this DB instance (ip:port@sid combination) is connected already, skip for trying other credential
                ipPortSid = str(ipAddress) + ':' + str(destinationPort) + '@' + str(item)
                if skipConnectedDb and ipPortSid in connectedTargetList:
                    logger.debug('Skip %s for credential %s' % (ipPortSid, protocol))
                    continue
                client = None
                logger.debug('Connecting to %s:%s@%s' % (ipAddress, destinationPort, item))
                try:
                    props = Properties()
                    props.setProperty(CollectorsConstants.DESTINATION_DATA_IP_ADDRESS, ipAddress)
                    props.setProperty(CollectorsConstants.PROTOCOL_ATTRIBUTE_PORT, destinationPort)
                    if item and protocolType != 'mysql':
                        props.setProperty(Protocol.SQL_PROTOCOL_ATTRIBUTE_DBSID, item)
                        if protocolType.lower() not in (DbTypes.MsSqlServer, DbTypes.MsSqlServerNtlm, DbTypes.MsSqlServerNtlmV2):
                            props.setProperty(Protocol.SQL_PROTOCOL_ATTRIBUTE_DBNAME, item)
                    client = Framework.createClient(protocol, props)
                    if client:
                        logger.debug('Connection by protocol %s success!' % protocol)
                        #ORacle Rac client ip address definition hook
                        protType = client.getProtocolDbType().lower()
                        if protType == DbTypes.Oracle:
                            client = getValidOracleClient(ipAddress, client, props, protocol, Framework)
                        discoverDB(Framework, client, OSHVResult, reportedSids)
                        connectedTargetList.append(ipPortSid)

                except (MissingJarsException, JavaException), ex:
                    strException = ex.getMessage()
                    logger.debug(strException)
                    logger.debugException('')
                    errorsList.append(strException)
                except Exception, ex:
                    strException = str(ex)
                    logger.debug(strException)
                    logger.debugException('')
                    errorsList.append(strException)
                if client:
                    client.close()
    return OSHVResult

NA = "NA"
def add(collection, value):
    if (not collection is None) and (value != NA):
        collection.add(value)

def getConnectionData(Framework, protocolType, acceptedProtocols):
    ips = HashSet()
    ports = HashSet()
    sids = HashSet()

    #fill IPs, ports and SIDs from DB
    destIps = Framework.getTriggerCIDataAsList('application_ip')
    destPort = Framework.getTriggerCIDataAsList('application_port')
    destSids = Framework.getTriggerCIDataAsList('sid')
    logger.debug('======== destSids ==', destSids)
    for i in range(0, len(destIps)):
        #make sure to skip adding corrupted data to the list of connections - e.g. when SID contains whitespace characters
        if protocolType.lower() in (DbTypes.MsSqlServer, DbTypes.MsSqlServerNtlm, DbTypes.Oracle, DbTypes.Db2) and re.search('\s', destSids[i]):
            continue
        #named MSSQL has SID: hostName\instanceName or clusterName\instanceName
        sidName = destSids[i]
        if sidName and sidName.find('\\') > 0 and protocolType.lower() in (DbTypes.MsSqlServer, DbTypes.MsSqlServerNtlm, DbTypes.MsSqlServerNtlmV2):
            logger.debug('====== sidName go to find \\')
            destSids[i] = sidName[sidName.find('\\')+1:]
        add(ips, destIps[i])
        add(ports, destPort[i])
        if protocolType == DbTypes.PostgreSQL:
            add(sids, destSids[i])
        else:
            add(sids, destSids[i].upper())

    #fill IPs
    destIps = Framework.getTriggerCIDataAsList('ip_address')
    if destIps != None:
        for i in range(0, len(destIps)):
            add(ips, destIps[i])

    #fill IPs and port from service address
    destIps = Framework.getTriggerCIDataAsList('sa_ip')
    destPort = Framework.getTriggerCIDataAsList('sa_port')
    for i in range(0, len(destIps)):
        add(ips, destIps[i])
        add(ports, destPort[i])

    # fill IPs and port from oracle listener
    destIps = Framework.getTriggerCIDataAsList('listener_ip')
    destPort = Framework.getTriggerCIDataAsList('listener_port')
    if destIps and destPort:
        for i in range(0, len(destIps)):
            add(ips, destIps[i])
            add(ports, destPort[i])

    #ensure that DBs with non-relevant SID will be present in the list
    if not protocolType in (DbTypes.Oracle, DbTypes.Db2):
        sids.add(None)

    connectionData = []
    for protocolId in acceptedProtocols:
        logger.debug('Collecting data  to connect with protocol %s' % protocolId)
        ipsToConnect = []
        portsToConnect = []
        sidsToConnect = []
        #only those IPs will be used which are in protocol range
        ipIter = ips.iterator()
        protocolObj = protocol.MANAGER_INSTANCE.getProtocolById(protocolId)
        while ipIter.hasNext():
            ip = ipIter.next()
            if protocolObj.isInScope(ip):
                ipsToConnect.append(ip)
        if len(ipsToConnect) == 0:
            logger.warn('No suitable IP address found for protocol range')
            continue

        #add protocol port if exists
        protocolPort = Framework.getProtocolProperty(protocolId, CollectorsConstants.PROTOCOL_ATTRIBUTE_PORT, NA)
        if protocolPort != NA:
            portsToConnect.append(protocolPort)
        else: #all collected ports will be used for connect
            portIter = ports.iterator()
            while portIter.hasNext():
                portsToConnect.append(portIter.next())
        if len(portsToConnect) == 0:
            logger.warn('No port collected for protocol')
            continue

        #add protocol SID if exists, otherwise add all from triggered CI data
        dbName = Framework.getProtocolProperty(protocolId, CollectorsConstants.SQL_PROTOCOL_ATTRIBUTE_DBSID, NA)
        if dbName == NA:
            dbName = Framework.getProtocolProperty(protocolId, CollectorsConstants.SQL_PROTOCOL_ATTRIBUTE_DBNAME, NA)
        if dbName != NA:
            sidsToConnect.append(dbName)
        else:
            sidIter = sids.iterator()
            while sidIter.hasNext():
                sidsToConnect.append(sidIter.next())
        if len(sidsToConnect) == 0:
            logger.warn('Database type %s requires instance name to connect' % protocolType)

        connectionDataItem = (protocolId, ipsToConnect, portsToConnect, sidsToConnect)
        connectionData.append(connectionDataItem)
    return connectionData

########################
#                      #
# MAIN ENTRY POINT     #
#                      #
########################

def reportWarnings(errorList):
    if errorList:
        for error in errorList:
            logger.reportWarningObject(error)

def reportErrors(errorList):
    if errorList:
        for error in errorList:
            logger.reportErrorObject(error)

def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    protocolType = Framework.getParameter('protocolType')
    if protocolType is None:
        raise Exception, 'Mandatory parameter protocolType is not set'

    #find protocols for desired DB type
    acceptedProtocols = []
    ip = Framework.getDestinationAttribute('ip_address')
    protocols = Framework.getAvailableProtocols(ip, ClientsConsts.SQL_PROTOCOL_NAME)
    logger.debug("Available db protocols [", protocols, "] for ip: ", ip)

    serviceNameList = []
    try:
        serviceNameList = Framework.getTriggerCIDataAsList('service_name')
        serviceNameList = list(set(serviceNameList))
    except:
        logger.debug('No Oracle Service Name in the trigger.')

    for protocol in protocols:
        protocolDbType = Framework.getProtocolProperty(protocol, CollectorsConstants.SQL_PROTOCOL_ATTRIBUTE_DBTYPE, NA)
        if re.match(protocolType, protocolDbType, re.IGNORECASE):
            acceptedProtocols.append(protocol)

    logger.debug("Valid db protocols [", acceptedProtocols, "]")

    if len(acceptedProtocols) == 0:
        Framework.reportWarning('Protocol not defined or IP out of protocol network range')
        logger.error('Protocol not defined or IP out of protocol network range')
        return OSHVResult

    connectionData = getConnectionData(Framework, protocolType, acceptedProtocols)
    reportedSids = []
    warningsList = []
    errorsList = []
    connectedTargetList = []
    for connectionDataItem in connectionData:
        protocolId, ipAddressList, destinationPortList, sidList = connectionDataItem
        logger.debug('=========== sidList ==', sidList)
        #        if serviceNameList:
        #            for serviceName in serviceNameList:
        #                sidList.append(serviceName)
        connectionSourceList = sidList
        logger.debug('============ connectionSourceList ==', connectionSourceList)

        logger.debug('Connecting by protocol %s' % protocolId)
        errList = []
        oshVector = connectByProtocol(Framework, protocolId, ipAddressList, destinationPortList, connectionSourceList, protocolType, reportedSids, errList, connectedTargetList)
        if oshVector.size() > 0:
            OSHVResult.addAll(oshVector)
        for error in errList:
            if errormessages.resolveAndAddToObjectsCollections(error, ClientsConsts.SQL_PROTOCOL_NAME, warningsList, errorsList):
                break
    reportError = OSHVResult.size() == 0
    if reportError:
        Framework.reportError('Failed to connect using all protocols')
        logger.error('Failed to connect using all protocols')
        reportErrors(errorsList)
        reportErrors(warningsList)
    #    else:
    #        reportWarnings(errorsList)
    #        reportWarnings(warningsList)
    return OSHVResult