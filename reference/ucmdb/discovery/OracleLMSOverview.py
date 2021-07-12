#coding=utf-8
import re
import logger
import modeling
import OracleLMSDataModel
import OracleLMSDBUtils
import OracleLMSUtils

from java.lang import Exception
from java.lang import String
from java.sql import Date
from javax.sql.rowset.serial import SerialBlob
from java.util import ArrayList
from java.util import Hashtable


def collectLMSOverviewFromOracle(Framework, oracleClient, machine_id, db_name, db_role, install_date, db_version, db_edition, dba_users_count, sessions_highwater, customTable, discoveryId):
    lmsOverviewList = getLMSOverview(Framework, oracleClient, machine_id, db_name, db_role, install_date, db_version, db_edition, dba_users_count, sessions_highwater, customTable, discoveryId)
    if lmsOverviewList != None and lmsOverviewList.size() > 0:
        lmsOverviewCount = saveOverviewToProbe(Framework, lmsOverviewList)
        lmsOverviewList.clear()
        return lmsOverviewCount
    return 0



def getLMSOverview(Framework, oracleClient, machine_id, db_name, db_role, install_date, db_version, db_edition, dba_users_count, sessions_highwater, customTable, discoveryId):
    discovered_vendor = OracleLMSUtils.getDiscoveredVendor(Framework)
    discovered_model = OracleLMSUtils.getDiscoveredModel(Framework)
    discovered_os_name = OracleLMSUtils.getDiscoveredOSName(Framework)
    is_virtual = OracleLMSUtils.isVirtualHost(Framework)
    (cpu_number, core_number) = OracleLMSUtils.getCpuNumber(Framework)
    cpu_name = OracleLMSUtils.getCpuName(Framework)
    cpu_clock_speed = OracleLMSUtils.getCpuClockSpeed(Framework)
    logical_cpu_count = OracleLMSUtils.getLogicalCpuCount(Framework)
    partition_method = OracleLMSUtils.getPartitioningMethod(Framework)

    if cpu_number == 0:
        logger.reportWarning('SocketsPopulatedPhys is not discovered. You should run an Inventory Activity to discover this data. For details, see Inventory Activity in the HP Universal CMDB Discovery and Integration Content Guide.')
    if core_number == 0:
        logger.reportWarning('TotalPhysicalCores is not discovered. You should run an Inventory Activity to discover this data. For details, see Inventory Activity in the HP Universal CMDB Discovery and Integration Content Guide.')
    if not cpu_name:
        logger.reportWarning('ProcessorIdentifier is not discovered. You should run an Inventory Activity to discover this data. For details, see Inventory Activity in the HP Universal CMDB Discovery and Integration Content Guide.')

    try:
        (sessions, logon_time) =  OracleLMSUtils.getSession(oracleClient)

        lmsOverviewList = ArrayList()
        obj =  OracleLMSDataModel.LMSOverviewObject()
        obj.group = OracleLMSUtils.encodeString(customTable.get(OracleLMSUtils.CUSTOM_GROUP))
        obj.aggregationLevel = OracleLMSUtils.encodeString(customTable.get(OracleLMSUtils.CUSTOM_AGGREGATION_LEVEL))
        obj.oracleCsi = OracleLMSUtils.encodeString(customTable.get(OracleLMSUtils.CUSTOM_ORACLE_CSI))
        obj.oracleProductCategory = 'Database'
        obj.machineId = OracleLMSUtils.encodeString(machine_id)
        if (is_virtual == 'true'):
            obj.vmachineId = OracleLMSUtils.encodeString(machine_id)
            if partition_method == '':
                logger.reportWarning('PartitioningMethod is not discovered. You should run a Virtualization Activity to discover this data. For details, see Virtualization Activity in the HP Universal CMDB Discovery and Integration Content Guide.')
        else:
            obj.vmachineId = ''
        obj.dbEdition = OracleLMSUtils.encodeString(db_edition)
        obj.dbName = OracleLMSUtils.encodeString(db_name)
        obj.version = OracleLMSUtils.encodeString(db_version)
        obj.optionsInstalled = checkOptionsInstalled(oracleClient)
        obj.optionsInUse = checkOptionsInUse(oracleClient)
        packsTable = getPacksTable(oracleClient)
        obj.packsGranted = checkPacksGranted(packsTable)
        obj.packsAgreed = checkPacksAgreed(packsTable)
        obj.applicationName = OracleLMSUtils.encodeString(customTable.get(OracleLMSUtils.CUSTOM_APPLICATION_NAME))
        obj.applicationStatus = OracleLMSUtils.encodeString(customTable.get(OracleLMSUtils.CUSTOM_APPLICATION_STATUS))
        obj.userCountDbaUsers = int(dba_users_count)
        obj.userCountApplication = int(customTable.get(OracleLMSUtils.CUSTOM_USER_COUNT_APPLICATION))
        if (discovered_vendor != 'NA'):
            obj.serverManufacturer = OracleLMSUtils.encodeString(discovered_vendor)
        if (discovered_model != 'NA'):
            obj.serverModel = OracleLMSUtils.encodeString(discovered_model)
        if (discovered_os_name != 'NA'):
            obj.operatingSystem = OracleLMSUtils.encodeString(discovered_os_name)
        obj.socketsPopulatedPhys = cpu_number
        obj.totalPhysicalCores = core_number
        if cpu_name:
            obj.processorIdentifier = cpu_name
        if (cpu_clock_speed != 'NA'):
            obj.processorSpeed = cpu_clock_speed
        obj.socketCapacityPhysical = cpu_number
        obj.totalLogicalCores = logical_cpu_count
        if (partition_method != 'NA'):
            obj.partitioningMethod = OracleLMSUtils.encodeString(partition_method)
        obj.dbRole = OracleLMSUtils.encodeString(db_role)
        obj.serverNameInTheCluster = OracleLMSUtils.encodeString(customTable.get(OracleLMSUtils.CUSTOM_SERVER_NAME_IN_THE_CLUSTER))
        if logon_time != None:
            obj.topConcurrencyTimestamp = Date(logon_time.getTime())
        obj.sessions = sessions
        obj.instanceSessionHighwater = sessions_highwater
        if install_date != None:
            obj.installDate = Date(install_date.getTime())
        measurmentComment = String(OracleLMSUtils.encodeString(customTable.get(OracleLMSUtils.CUSTOM_MEASUREMENT_COMMENT)))
        if measurmentComment.length() < 1:
            measurmentComment = String(' ')
        obj.measurementComment = SerialBlob(measurmentComment.getBytes('UTF-8'))
        obj.discoveryId = discoveryId
        lmsOverviewList.add(obj)

    except Exception, ex:
        strException = ex.getMessage()
        logger.error('Failed to get records for "OVERVIEW". ', strException)
        return
    return lmsOverviewList

def saveOverviewToProbe(Framework, lmsOverviewList):
    return OracleLMSDBUtils.OracleLMSDataServiceDAO(Framework).batchSaveLMSOverview(lmsOverviewList)

def clearOverviewByDiscoveryId(Framework, discovery_id):
    return OracleLMSDBUtils.OracleLMSDataServiceDAO(Framework).deleteLMSOverviewByDiscoveryId(discovery_id)

def getOverviewFromProbe(Framework, discovery_id):
    return OracleLMSDBUtils.OracleLMSDataServiceDAO(Framework).getLMSOverviewByDiscoveryId(discovery_id)

def getOverviewColumns():
    return OracleLMSDBUtils.COLUMNS_LMS_OVERVIEW

def getOptionsInstalled(oracleClient):
    sql = "SELECT PARAMETER, VALUE FROM V$OPTION where value = 'TRUE'"
    installedList = ArrayList()
    try:
        resultSet = oracleClient.executeQuery(sql)
        while (resultSet.next()):
            installed = resultSet.getString(1)
            installedList.add(installed)
    except Exception, ex:
        strException = ex.getMessage()
        logger.error('Failed to get installed options. ', strException)
        return
    return installedList

def checkOptionsInstalled(oracleClient):
    shortCutsList = []
    installedList = getOptionsInstalled(oracleClient)
    if (installedList != None):
        for install in installedList:
            shortcut = getShortcutForOption(install)
            if shortcut:
                shortCutsList.append('%s ' % shortcut)
    return ''.join([u'%s' % x for x in set(shortCutsList) if shortCutsList])


def getOptionsInUse(oracleClient):
    sql = "select name from dba_feature_usage_statistics where currently_used = 'TRUE'"
    inUseList = ArrayList()
    try:
        resultSet = oracleClient.executeQuery(sql)
        while (resultSet.next()):
            inUse = resultSet.getString(1)
            inUseList.add(inUse)
    except Exception, ex:
        strException = ex.getMessage()
        logger.error('Failed to get using options. ', strException)
        return
    return inUseList

def checkOptionsInUse(oracleClient):
    shortCutsList = []
    inUsedList = getOptionsInUse(oracleClient)
    if (inUsedList != None):
        for install in inUsedList:
            shortcut = getShortcutForOption(install)
            if shortcut:
                shortCutsList.append('%s ' % shortcut)
    return ''.join([u'%s' % x for x in set(shortCutsList) if shortCutsList])

def getPacksTable(oracleClient):
    owner = OracleLMSUtils.getOEMOwner(oracleClient)
    sql = "SELECT distinct a.pack_display_label, c.PACK_ACCESS_AGREED "\
          "FROM " + owner + ".MGMT_LICENSE_DEFINITIONS a, "\
          "(SELECT DECODE(COUNT(*), 0, 'NO','YES') AS PACK_ACCESS_AGREED "\
          " FROM " + owner + ".MGMT_LICENSES "\
          " WHERE upper(I_AGREE)='YES') c"
    packsTable = Hashtable()
    try:
        resultSet = oracleClient.executeQuery(sql)
        while (resultSet.next()):
            packDisplayLabel = resultSet.getString(1)
            packAccessAgreed = resultSet.getString(2)
            packsTable.put(packDisplayLabel, packAccessAgreed)
    except Exception, ex:
        strException = ex.getMessage()
        logger.error('Failed to get packs. ', strException)
        return
    return packsTable

def checkPacksGranted(packsTable):
    shortCutsList = []
    if (packsTable != None):
        for packDisplayLabel in packsTable.keySet():
            shortcut = getShortcutForOption(packDisplayLabel)
            if shortcut:
                shortCutsList.append('%s ' % shortcut)
    return ''.join(shortCutsList).strip(' ')


def checkPacksAgreed(packsTable):
    shortCutsList = []
    if (packsTable != None):
        for packDisplayLabel in packsTable.keySet():
            if (packsTable.get(packDisplayLabel) == 'YES'):
                shortcut = getShortcutForOption(packDisplayLabel)
                if shortcut:
                    shortCutsList.append('%s ' % shortcut)
    return ''.join(shortCutsList).strip(' ')


def getShortcutForOption(option):
    for key in DATABASE_OPTIONS_NAMES_SHORTCUT:
        if key.lower() == option.lower():
            return DATABASE_OPTIONS_NAMES_SHORTCUT[key]
    return None

DATABASE_OPTIONS_NAMES_SHORTCUT = {'Active Data Guard': 'ADG',
                                   'Advanced Analytics': 'AA',
                                   'Advanced Compression': 'AC',
                                   'Advanced Security': 'AS',
                                   'Audit Vault': 'AV',
                                   'Database Vault': 'DV',
                                   'In-Memory Database': 'IMD',
                                   'In-Memory Database Cache': 'IMDC',
                                   'Label Security': 'LS',
                                   'Multitenant': 'MT',
                                   'OLAP': 'OL',
                                   'Partitioning (User)': 'P',
                                   'Real Application Clusters': 'RAC',
                                   'Real Application Testing': 'RAT',
                                   'Retail Data Model': 'RDM',
                                   'Spatial & Graph': 'SP',
                                   'Warehouse Builder Data Quality': 'WBDQ',
                                   'Warehouse Builder Enterprise ETL': 'WBEE',
                                   'Cloud Management Pack': 'CMP',
                                   'Data Masking Pack': 'DDM',
                                   'Diagnostic pack': 'DD',
                                   'Tuning Pack': 'DT',
                                   'Diagnostics Pack for Oracle Middleware': 'ADP',
                                   'Management Pack for Oracle Coherence': 'AMC',
                                   'Management Pack for Oracle GoldenGate': 'AMG',
                                   'Management Pack for WebLogic Server': 'AMW',
                                   'Management Pack Plus for SOA': 'AMP',
                                   'Business Intelligence Management Pack': 'BIM',
                                   'Management Pack for WebCenter Suite': 'MWS',
                                   'Management Pack for Identity Management': 'MIM',
                                   'Configuration Management Pack for Applications': 'OCA',
                                   'Diagnostics Pack for Non-Oracle Middleware': 'ODM',
                                   'Management Connectors': 'OMC',
                                   'Oracle VM Management Pack': 'OVM',
                                   'Provisioning and Patch Automation Pack': 'OPP',
                                   'System Monitoring Plug-in for Hosts': 'OSH',
                                   'System Monitoring Plug-in for Network Devices': 'OSPN',
                                   'System Monitoring Plug-in for Non Oracle Databases': 'OSND',
                                   'System Monitoring Plug-in for Non Oracle Middleware': 'OSNM',
                                   'System Monitoring Plug-in for Storage': 'OSPS',
                                   'Service Level Management Pack': 'SLP',
                                   'Application Server Configuration Pack': 'ACP',
                                   'Content Database Suite': 'CDS',
                                   'Change Management Pack': 'DCM',
                                   'Configuration Management Pack': 'DCO',
                                   'Configuration Management Pack for Non-Oracle Systems': 'OCN',
                                   'Configuration Management Pack for Oracle Middleware': 'OCM',
                                   'Data Mining': 'DM',
                                   'Database Provisioning and Patch Automation Pack': 'DPP',
                                   'Data Profiling and Quality': 'DPQ',
                                   'Data Watch and Repair Connector': 'DWRC',
                                   'Linux Management Pack': 'OLM',
                                   'Provisioning Pack': 'PP',
                                   'Records Database': 'RDB',
                                   'Standalone Provisioning and Patch Automation Pack': 'SPP',
                                   'Total Recall': 'TR',
                                   'ASO native encryption and checksumming': 'AS',
                                   'Transparent Data Encryption': 'AS',
                                   'Encrypted Tablespaces': 'AS',
                                   'Backup Encryption': 'AS',
                                   'SecureFile Encryption': 'AS',
                                   'Oracle Database Vault': 'DV',
                                   'Oracle Label Security': 'LS',
                                   'Partitioning': 'P',
                                   'Real Application Cluster': 'RAC',
                                   'Database Configuration Management Pack': 'DCO',
                                   'Database Diagnostics Pack': 'DD',
                                   'Database Tuning Pack': 'DT',
                                   'HeapCompression': 'AC',
                                   'Backup BZIP2 Compression': 'AC',
                                   'Backup DEFAULT Compression': 'AC',
                                   'Backup HIGH Compression': 'AC',
                                   'Backup LOW Compression': 'AC',
                                   'Backup MEDIUM Compression': 'AC',
                                   'Backup ZLIB, Compression': 'AC',
                                   'SecureFile Compression (user)': 'AC',
                                   'SecureFile Deduplication (user)': 'AC',
                                   'Data Guard': 'AC',
                                   'Oracle Utility Datapump (Export)': 'AC',
                                   'Oracle Utility Datapump (Import)': 'AC',
                                   'SecureFile Encryption (user)': 'AS',
                                   'OLAP - Analytic Workspaces': 'OL',
                                   'Real Application Clusters (RAC)': 'RAC',
                                   'Database Replay: Workload Capture': 'RAT',
                                   'Database Replay: Workload Replay': 'RAT',
                                   'SQL Performance Analyzer': 'RAT',
                                   'Flashback Data Archive': 'TR',
                                   'ADDM': 'DD',
                                   'AWR Baseline': 'DD',
                                   'AWR Baseline Template': 'DD',
                                   'AWR Report': 'DD',
                                   'Baseline Adaptive Thresholds': 'DD',
                                   'Baseline Static Computations': 'DD',
                                   'Real-Time SQL Monitoring': 'DT',
                                   'SQL Tuning Advisor': 'DT',
                                   'SQL Access Advisor': 'DT',
                                   'SQL Profile': 'DT',
                                   'Automatic SQL Tuning Advisor': 'DT',
                                   'AS Provisioning and Patch Automation': 'OPP',
                                   'Config Management Pack': 'DCO',
                                   'Database Provisioning and Patch Automation': 'DPP'}
