#coding=utf-8
import logger
import OracleLMSDataModel
import OracleLMSDBUtils
import OracleLMSUtils

from java.lang import Exception
from java.util import ArrayList

SQL_VSESSION = "SELECT SADDR, SID, PADDR, USER#, USERNAME, COMMAND, STATUS, SERVER, SCHEMANAME, OSUSER, PROCESS, MACHINE, TERMINAL, PROGRAM, TYPE, LAST_CALL_ET, LOGON_TIME FROM V$SESSION"

def collectLMSVSessionFromOracle(Framework, oracleClient, machine_id, db_name, discoveryId):
    lmsVSessionList = getLMSVSession(oracleClient, machine_id, db_name, discoveryId)
    if lmsVSessionList != None and lmsVSessionList.size() > 0:
        lmsVSessionCount = saveVSessionToProbe(Framework, lmsVSessionList)
        lmsVSessionList.clear()
        return lmsVSessionCount
    return 0

def getLMSVSession(oracleClient, machine_id, db_name, discoveryId):
    try:
        resultSet = oracleClient.executeQuery(SQL_VSESSION)#@@CMD_PERMISION sql protocol execution
        if resultSet == None:
            logger.error('Oracle "V$SESSION" table is empty.')
            return
        else:
            lmsVSessionList = ArrayList()

            while resultSet.next():
                obj =  OracleLMSDataModel.LMSVSessionObject()
                obj.saddr = resultSet.getString(1)
                obj.sid = resultSet.getInt(2)
                obj.paddr = resultSet.getString(3)
                obj.userNo = resultSet.getInt(4)
                obj.userName = OracleLMSUtils.encodeString(resultSet.getString(5))
                obj.command = resultSet.getInt(6)
                obj.status = OracleLMSUtils.encodeString(resultSet.getString(7))
                obj.server = OracleLMSUtils.encodeString(resultSet.getString(8))
                obj.schemaName = OracleLMSUtils.encodeString(resultSet.getString(9))
                obj.osUser = OracleLMSUtils.encodeString(resultSet.getString(10))
                obj.process = OracleLMSUtils.encodeString(resultSet.getString(11))
                obj.machine = OracleLMSUtils.encodeString(resultSet.getString(12))
                obj.terminal = OracleLMSUtils.encodeString(resultSet.getString(13))
                obj.program = OracleLMSUtils.encodeString(resultSet.getString(14))
                obj.type = OracleLMSUtils.encodeString(resultSet.getString(15))
                obj.lastCallEt = resultSet.getInt(16)
                obj.logonTime = resultSet.getTimestamp(17)
                obj.machineId = machine_id
                obj.dbName =  db_name
                obj.discoveryId = discoveryId
                lmsVSessionList.add(obj)
            resultSet.close()
    except Exception, ex:
        strException = ex.getMessage()
        logger.error('Failed to get records from Oracle "V$SESSION" table. ', strException)
        return
    return lmsVSessionList

def saveVSessionToProbe(Framework, lmsVSessionList):
    return OracleLMSDBUtils.OracleLMSDataServiceDAO(Framework).batchSaveLMSVSession(lmsVSessionList)

def clearVSessionByDiscoveryId(Framework, discovery_id):
    return OracleLMSDBUtils.OracleLMSDataServiceDAO(Framework).deleteLMSVSessionByDiscoveryId(discovery_id)

def getVSessionFromProbe(Framework, discovery_id):
    return OracleLMSDBUtils.OracleLMSDataServiceDAO(Framework).getLMSVSessionByDiscoveryId(discovery_id)

def getVSessionColumns():
    return OracleLMSDBUtils.COLUMNS_LMS_V_SESSION