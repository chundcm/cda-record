#coding=utf-8
import string
import re
import logger
import OracleLMSDataModel
import OracleLMSDBUtils
import OracleLMSUtils

from java.lang import Exception
from java.sql import Date

from java.util import ArrayList

SQL_DBA_USERS = "SELECT USERNAME, USER_ID, DEFAULT_TABLESPACE, TEMPORARY_TABLESPACE, CREATED, PROFILE, EXPIRY_DATE FROM DBA_USERS"


def collectLMSDbaUsersFromOracle(Framework, oracleClient, machine_id, db_name, discoveryId):
    lmsDbaUsersList = getLMSDbaUsers(oracleClient, machine_id, db_name, discoveryId)
    if lmsDbaUsersList != None and lmsDbaUsersList.size() > 0:
        lmsDbaUsersCount = saveDbaUsersToProbe(Framework, lmsDbaUsersList)
        lmsDbaUsersList.clear()
        return lmsDbaUsersCount
    return 0



def getLMSDbaUsers(oracleClient, machine_id, db_name, discoveryId):
    try:
        resultSet = oracleClient.executeQuery(SQL_DBA_USERS)
        if resultSet == None:
            logger.error('Oracle "DBA_USERS" table is empty.')
            return
        else:
            lmsDbaUsersList = ArrayList()
            while resultSet.next():
                obj = OracleLMSDataModel.LMSDbaUsersObject()
                obj.username = OracleLMSUtils.encodeString(resultSet.getString(1))
                obj.userId = resultSet.getInt(2)
                obj.defaultTablespace = OracleLMSUtils.encodeString(resultSet.getString(3))
                obj.temporaryTablespace = OracleLMSUtils.encodeString(resultSet.getString(4))
                created_date = resultSet.getDate(5)
                if (created_date) != None and (created_date != ''):
                    obj.created = Date(created_date.getTime())
                obj.profile = OracleLMSUtils.encodeString(resultSet.getString(6))
                expiry_date = resultSet.getDate(7)
                if (expiry_date != None) and (expiry_date != ''):
                    obj.expiryDate = Date(expiry_date.getTime())
                obj.machineId = machine_id
                obj.dbName =  db_name
                obj.discoveryId = discoveryId
                lmsDbaUsersList.add(obj)
            resultSet.close()
    except Exception, ex:
        strException = ex.getMessage()
        logger.error('Failed to get records from Oracle "DBA_USERS" table. ', strException)
        return
    return lmsDbaUsersList

def saveDbaUsersToProbe(Framework, lmsDbaUsersList):
    return OracleLMSDBUtils.OracleLMSDataServiceDAO(Framework).batchSaveLMSDbaUsers(lmsDbaUsersList)

def clearDbaUsersByDiscoveryId(Framework, discovery_id):
    return OracleLMSDBUtils.OracleLMSDataServiceDAO(Framework).deleteLMSDbaUsersByDiscoveryId(discovery_id)

def getDbaUsersFromProbe(Framework, discovery_id):
    return OracleLMSDBUtils.OracleLMSDataServiceDAO(Framework).getLMSDbaUsersByDiscoveryId(discovery_id)

def getDbaUsersColumns():
    return OracleLMSDBUtils.COLUMNS_LMS_DBA_USERS