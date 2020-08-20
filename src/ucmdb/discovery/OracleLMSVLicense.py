#coding=utf-8
import logger
import modeling
import OracleLMSDataModel
import OracleLMSDBUtils
import OracleLMSUtils

from java.lang import Exception
from java.util import ArrayList

SQL_VLICENCE = "SELECT SESSIONS_MAX, SESSIONS_WARNING, SESSIONS_CURRENT, SESSIONS_HIGHWATER, USERS_MAX, CPU_COUNT_CURRENT, CPU_COUNT_HIGHWATER FROM V$LICENSE"
SQL_VLICENCE_9i = "SELECT SESSIONS_MAX, SESSIONS_WARNING, SESSIONS_CURRENT, SESSIONS_HIGHWATER, USERS_MAX FROM V$LICENSE"

def collectLMSVLicenseFromOracle(Framework, oracleClient, machine_id, db_name, db_version, discoveryId):
    lmsVLicenseList = getLMSVLicense(Framework, oracleClient, machine_id, db_name, db_version, discoveryId)
    if lmsVLicenseList != None and lmsVLicenseList.size() > 0:
        lmsVLicenseCount = saveVLicenseToProbe(Framework, lmsVLicenseList)
        lmsVLicenseList.clear()
        return lmsVLicenseCount
    return 0

def getLMSVLicense(Framework, oracleClient, machine_id, db_name, db_version, discoveryId):
    numbers = db_version.split('.')
    versionNumber = int(numbers[0]) * 10 + int(numbers[1])
    resultSet = None
    logical_cpu_count = 0

    try:
        if versionNumber < 100:
            resultSet = oracleClient.executeQuery(SQL_VLICENCE_9i)
            logical_cpu_count = OracleLMSUtils.getLogicalCpuCount(Framework)
        else:
            resultSet = oracleClient.executeQuery(SQL_VLICENCE)
        if resultSet == None:
            logger.error('Oracle "V$LICENSE" table is empty.')
            return
        else:
            lmsVLicenseList = ArrayList()
            while resultSet.next():
                obj =  OracleLMSDataModel.LMSVLicenseObject()
                obj.sessionsMax = resultSet.getInt(1)
                obj.sessionsWarning = resultSet.getInt(2)
                obj.sessionsCurrent = resultSet.getInt(3)
                obj.sessionsHighwater = resultSet.getInt(4)
                obj.usersMax = resultSet.getInt(5)
                if versionNumber < 100:
                    obj.cpuCountCurrent = logical_cpu_count
                    obj.cpuCountHighwater = logical_cpu_count
                else:
                    obj.cpuCountCurrent = resultSet.getInt(6)
                    obj.cpuCountHighwater = resultSet.getInt(7)
                obj.machineId = machine_id
                obj.dbName =  db_name
                obj.discoveryId = discoveryId
                lmsVLicenseList.add(obj)
            resultSet.close()
    except Exception, ex:
        strException = ex.getMessage()
        logger.error( 'Failed to get records from Oracle "V$LICENSE" table. ', strException)
        return
    return lmsVLicenseList

def saveVLicenseToProbe(Framework, lmsVLicenseList):
    return OracleLMSDBUtils.OracleLMSDataServiceDAO(Framework).batchSaveLMSVLicense(lmsVLicenseList)

def clearVLicenseByDiscoveryId(Framework, discovery_id):
    return OracleLMSDBUtils.OracleLMSDataServiceDAO(Framework).deleteLMSVLicenseByDiscoveryId(discovery_id)

def getVLicenseFromProbe(Framework, discovery_id):
    return OracleLMSDBUtils.OracleLMSDataServiceDAO(Framework).getLMSVLicenseByDiscoveryId(discovery_id)

def getVLicenseColumns():
    return OracleLMSDBUtils.COLUMNS_LMS_V_LICENSE