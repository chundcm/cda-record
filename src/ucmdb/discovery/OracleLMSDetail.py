#coding=utf-8
import logger
import OracleLMSDataModel
import OracleLMSDBUtils
import OracleLMSUtils

from java.lang import Exception
from java.sql import Date

from java.util import ArrayList


def collectLMSDetailFromOracle(Framework, oracleClient, machine_id, db_name, banner, dba_users_count, db_role, install_date, discoveryId):
    lmsDetailList = getLMSDetail(Framework, oracleClient, machine_id, db_name, banner, dba_users_count, db_role, install_date, discoveryId)
    if lmsDetailList != None and lmsDetailList.size() > 0:
        lmsDetailCount = saveDetailToProbe(Framework, lmsDetailList)
        lmsDetailList.clear()
        return lmsDetailCount
    return 0

def getLMSDetail(Framework, oracleClient, machine_id, db_name, banner, dba_users_count, db_role, install_date, discoveryId):
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
        logger.reportWarning('SocketsPopulatedPhys is not discovered. You should run an Inventory Activity to discover this data. For details, see Inventory Activity in the HP Universal CMDB Discovery and Integration Content Guide..')
    if core_number == 0:
        logger.reportWarning('TotalPhysicalCores is not discovered.  You should run an Inventory Activity to discover this data. For details, see Inventory Activity in the HP Universal CMDB Discovery and Integration Content Guide.')
    if not cpu_name:
        logger.reportWarning('ProcessorIdentifier is not discovered. You should run an Inventory Activity to discover this data. For details, see Inventory Activity in the HP Universal CMDB Discovery and Integration Content Guide.')

    try:
        lmsDetailList = ArrayList()
        obj =  OracleLMSDataModel.LMSDetailObject()
        obj.rlScriptVersion = '17.2'
        obj.machineId = OracleLMSUtils.encodeString(machine_id)
        if (is_virtual == 'true'):
            obj.vmachineId = OracleLMSUtils.encodeString(machine_id)
            if partition_method == '':
                logger.reportWarning('PartitioningMethod is not discovered. You should run a Virtualization Activity to discover this data. For details, see Virtualization Activity in the HP Universal CMDB Discovery and Integration Content Guide.')
        else:
            obj.vmachineId = ''
        obj.banner = banner
        obj.dbName = OracleLMSUtils.encodeString(db_name)
        obj.userCount = int(dba_users_count)
        if (discovered_vendor != 'NA'):
            obj.serverManufacturer = OracleLMSUtils.encodeString(discovered_vendor)
        if (discovered_model != 'NA'):
            obj.serverModel = OracleLMSUtils.encodeString(discovered_model)
        if (discovered_os_name != 'NA'):
            obj.operatingSystem = OracleLMSUtils.encodeString(discovered_os_name)
        obj.socketsPopulatedPhys = cpu_number
        obj.totalPhysicalCores = core_number
        if cpu_name:
            obj.processorIdentifier = OracleLMSUtils.encodeString(cpu_name)
        if (cpu_clock_speed != 'NA'):
            obj.processorSpeed = OracleLMSUtils.encodeString(cpu_clock_speed)
        obj.totalLogicalCores = logical_cpu_count
        if (partition_method != 'NA'):
            obj.partitioningMethod = OracleLMSUtils.encodeString(partition_method)
        obj.dbRole = OracleLMSUtils.encodeString(db_role)
        if install_date != None:
            obj.installDate = Date(install_date.getTime())
        obj.discoveryId = discoveryId
        lmsDetailList.add(obj)

    except Exception, ex:
        strException = ex.getMessage()
        logger.error('Failed to get records from Oracle "DETAIL" table. ', strException)
        return
    return lmsDetailList

def saveDetailToProbe(Framework, lmsDetailList):
    return OracleLMSDBUtils.OracleLMSDataServiceDAO(Framework).batchSaveLMSDetail(lmsDetailList)

def clearDetailByDiscoveryId(Framework, discovery_id):
    return OracleLMSDBUtils.OracleLMSDataServiceDAO(Framework).deleteLMSDetailByDiscoveryId(discovery_id)

def getDetailFromProbe(Framework, discovery_id):
    return OracleLMSDBUtils.OracleLMSDataServiceDAO(Framework).getLMSDetailByDiscoveryId(discovery_id)

def getDetailColumns():
    return OracleLMSDBUtils.COLUMNS_LMS_DETAIL