#coding=utf-8
import sys
import re

import errormessages
import logger
import modeling
import OracleLMSUtils
import OracleLMSVLicense
import OracleLMSVSession
import OracleLMSOverview
import OracleLMSDetail
import OracleLMSDbaUsers
import OracleLMSOptions

from distutils.version import LooseVersion
from java.lang import Exception,Object
from java.sql import SQLException
from java.util import Properties
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector
from appilog.common.utils import Protocol
from com.hp.ucmdb.discovery.common import CollectorsConstants
from com.hp.ucmdb.discovery.library.clients import ClientsConsts

SQL_PROTOCOL_NAME = 'SQL'
FILE_DESCRIPTION = 'This document was created by querying Oracle for Oracle LMS Data Collection. It represents Oracle configuration.'

##############################################
########      MAIN                  ##########
##############################################
def DiscoveryMain(Framework):

    OSHVResult = ObjectStateHolderVector()
    oracleId = Framework.getDestinationAttribute('id')
    historyInputContent = Framework.getTriggerCIData('document_content')
    machine_id = Framework.getTriggerCIData('discovered_host_name')
    group = Framework.getParameter('group')
    aggregationLevel = Framework.getParameter('aggregationLevel')
    oracleCSI = Framework.getParameter('oracleCSI')
    applicationName = Framework.getParameter('applicationName')
    applicationStatus = Framework.getParameter('applicationStatus')
    userCountForApplication = Framework.getParameter('userCountForApplication')
    serverNameInTheCluster = Framework.getParameter('serverNameInTheCluster')
    measurementComment = Framework.getParameter('measurementComment')
    size = Framework.getParameter('size')
    service_credentials = Framework.getTriggerCIDataAsList('pdb_credentials')
    service_names = Framework.getTriggerCIDataAsList('pdb_service_names')

    oracleClient = None

    if (size !=  '' and OracleLMSUtils.isInteger(size) == False):
         logger.reportWarning("The size parameter should be an integer.", size)
         size = 2097152

    if not OracleLMSUtils.createOracleLMSTables(Framework):
        logger.reportWarning("Can not create Oracle LMS tables.")
        return OSHVResult

    if not OracleLMSUtils.upgradeDatabase(Framework):
        logger.reportWarning("Failed to upgrade database.")

    if (machine_id == 'NA'):
        machine_id = ''
        logger.reportWarning('Machine ID is not discovered. You should run an Inventory Activity to discover this data. For details, see Inventory Activity in the HP Universal CMDB Discovery and Integration Content Guide.')


    customTable = None
    inputContent = None
    if historyInputContent and historyInputContent != 'NA':
        inputContent = OracleLMSUtils.getunZippedContent(historyInputContent)
        customTable = OracleLMSUtils.mergeCustomData(inputContent)
    else:
        (customTable, inputContent) = OracleLMSUtils.createCustomDataAndContent(group, aggregationLevel, oracleCSI, applicationName, applicationStatus, userCountForApplication, serverNameInTheCluster, measurementComment)

    if (customTable != None):
        userCount = customTable.get(OracleLMSUtils.CUSTOM_USER_COUNT_APPLICATION)
        if (userCount !=  '' and OracleLMSUtils.isInteger(userCount) == False):
            customTable.put(OracleLMSUtils.CUSTOM_USER_COUNT_APPLICATION, 0)
            errMsg = OracleLMSUtils.CUSTOM_USER_COUNT_APPLICATION + ' should be an integer.'
            logger.reportWarning(errMsg)

    result = True
    instance_name = None
    try:
        #oracle CDB
        try:
            try:
                oracleClient = Framework.createClient()
            except Exception, clientEx:
                credentialsId = Framework.getDestinationAttribute('credentialsId')
                protocols = Framework.getAvailableProtocols(ClientsConsts.SQL_PROTOCOL_NAME)
                for protocol in protocols:
                    if Framework.getProtocolProperty(protocol, Protocol.PROTOCOL_CM_CREDENTIAL_ID_ATTRIBUTE) == credentialsId:
                        sid = Framework.getProtocolProperty(protocol, CollectorsConstants.SQL_PROTOCOL_ATTRIBUTE_DBSID)
                        if sid:
                            props = Properties()
                            props.setProperty(Protocol.SQL_PROTOCOL_ATTRIBUTE_DBSID, sid)
                            oracleClient = Framework.createClient(ClientsConsts.SQL_PROTOCOL_NAME, props)
                        else:
                            raise clientEx

            instance_name = OracleLMSUtils.getInstanceName(oracleClient)
            result = result and collectLMSDataFromOracle(Framework, oracleClient,  machine_id, instance_name, customTable)
        except Exception, message:
            strException = message.getMessage()
            messageString = str(coerce_(message).encode('utf-8'))
            if(messageString.find("ORA-00904") != -1):
                logger.reportWarning("table or view does not exist")
            else:
                errormessages.resolveAndReport(strException, SQL_PROTOCOL_NAME, Framework)
            logger.debug(logger.prepareFullStackTrace(''))
        finally:
            if oracleClient != None:
                oracleClient.close()

        #all the PDBs
        serviceClient = None
        if len(service_credentials):
            for i in range(0, len(service_credentials)):
                credential = service_credentials[i]
                if credential:
                    protocolDbSid = Framework.getProtocolProperty(credential, CollectorsConstants.SQL_PROTOCOL_ATTRIBUTE_DBSID, "")
                    try:
                        try:
                            props = Properties()
                            props.setProperty(Protocol.SQL_PROTOCOL_ATTRIBUTE_DBSID, protocolDbSid)
                            serviceClient = Framework.createClient(credential, props)
                        except:
                            props = Properties()
                            props.setProperty(Protocol.SQL_PROTOCOL_ATTRIBUTE_DBSID, service_names[i])
                            serviceClient = Framework.createClient(credential, props)

                        if serviceClient:
                            result = result and collectLMSDataFromOracle(Framework, serviceClient,  machine_id, instance_name, customTable)
                        else:
                            logger.debug("Failed to connect to pdb credential: " + credential)
                    except:
                        excInfo = "Discovery Failed on PDB " + service_names[i] + ".\r\n" + str(sys.exc_info()[1])
                        errormessages.resolveAndReport(excInfo, SQL_PROTOCOL_NAME, Framework)
                        logger.debug(logger.prepareFullStackTrace(''))
                    finally:
                        if serviceClient != None:
                            serviceClient.close()

        logger.debug("Oracle LMS discovery result:", result)
        if result:
            discoveryId = machine_id + instance_name
            configFileContent = getConfigFileContent(Framework, discoveryId)
            if (clearOracleLMSInProbe(Framework, discoveryId) == 0):
                logger.reportWarning("Can not clear Oracle LMS data in Probe.")
                return None
            #convert null as empty string ''
            configFileContent = re.sub(',null', ',', configFileContent)
            configFileContent = re.sub('null,', ',', configFileContent)
        else:
            errormessages.resolveAndReport('Failed to collect data from Oracle server.', SQL_PROTOCOL_NAME, Framework)
            return None

        oracleOsh = modeling.createOshByCmdbIdString('oracle', oracleId)
        discoveryFileOsh = OracleLMSUtils.createAuditDocumentOSH('Oracle LMS Audit Discovery Data', size, configFileContent, oracleOsh, FILE_DESCRIPTION)
        customerFileOsh = OracleLMSUtils.createAuditDocumentOSH('Oracle LMS Audit Customer Data', size, inputContent, oracleOsh, FILE_DESCRIPTION)
        OSHVResult.add(discoveryFileOsh)
        OSHVResult.add(customerFileOsh)
    except SQLException, sqlex:
        strException = sqlex.getMessage()
        logger.error('Failed to execute SQL query: ', strException)
        logger.reportError("Failed to execute database query")
    except Exception, ex:
        strException = ex.getMessage()
        logger.error('Discovery failed: ', strException)
        logger.reportError('Discovery failed: %s' % strException)
    except:
        logger.debugException('')
        strException = str(sys.exc_info()[1])
        errormessages.resolveAndReport(strException, SQL_PROTOCOL_NAME, Framework)
    return OSHVResult

def coerce_(x):
    if isinstance(x, basestring):
        return x
    elif isinstance(x, Object):
        return x.toString()
    elif isinstance(x, Exception):
        return unicode(x.message)
    return repr(x)

def collectLMSDataFromOracle(Framework, oracleClient, machine_id, instance_name, customTable):
    db_name = instance_name
    (db_role, install_date) = OracleLMSUtils.getDatabaseInfo(oracleClient)
    (banner, db_version, db_edition) = OracleLMSUtils.getDbVersion(oracleClient)
    versionNumber = LooseVersion(db_version)
    if versionNumber >= LooseVersion("12.0"):
        db_name = OracleLMSUtils.getDBName(oracleClient, instance_name)
    if not db_name:
        db_name = instance_name
    dba_users_count = OracleLMSUtils.getDBAUsersCount(oracleClient)
    sessions_highwater =  OracleLMSUtils.getLicense(oracleClient)
    discoveryId = machine_id + instance_name
    if OracleLMSVLicense.collectLMSVLicenseFromOracle(Framework, oracleClient, machine_id, db_name, db_version, discoveryId) == 0:
        errorMsg = 'Failed to collect V$LICENSE data from Oracle server.'
        logger.error(errorMsg)
        return False
    elif OracleLMSVSession.collectLMSVSessionFromOracle(Framework, oracleClient, machine_id, db_name, discoveryId) == 0:
        errorMsg = 'Failed to collect V$SESSION data from Oracle server.'
        logger.error(errorMsg)
        return False
    elif OracleLMSDbaUsers.collectLMSDbaUsersFromOracle(Framework, oracleClient, machine_id, db_name, discoveryId) == 0:
        errorMsg = 'Failed to collect DBA_USERS data from Oracle server.'
        logger.error(errorMsg)
        return False
    elif OracleLMSOptions.collectLMSOptionsFromOracle(Framework, oracleClient, machine_id, instance_name, db_name, db_version) == 0:
        errorMsg = 'Failed to collect OPTIONS data from Oracle server.'
        logger.error(errorMsg)
        return False
    elif OracleLMSDetail.collectLMSDetailFromOracle(Framework, oracleClient, machine_id, db_name, banner, dba_users_count, db_role, install_date, discoveryId) == 0:
        errorMsg = 'Failed to collect DETAIL data from Oracle server.'
        logger.error(errorMsg)
        return False
    elif OracleLMSOverview.collectLMSOverviewFromOracle(Framework, oracleClient, machine_id, instance_name, db_role, install_date, db_version, db_edition, dba_users_count, sessions_highwater, customTable, discoveryId) == 0:
        errorMsg = 'Failed to collect OVERVIEW data from Oracle server.'
        logger.error(errorMsg)
        return False
    return True


def getConfigFileContent(Framework, discovery_id):
    fileContentList = []

    lmsOverviewList = OracleLMSOverview.getOverviewFromProbe(Framework, discovery_id)
    if lmsOverviewList.size() > 0:
        lmsOverviewColunms = OracleLMSOverview.getOverviewColumns()
        fileContentList = getConfigFileContentList(fileContentList, 'LMS_OVERVIEW', lmsOverviewColunms, lmsOverviewList)
    lmsOverviewList.clear()

    lmsDetailList = OracleLMSDetail.getDetailFromProbe(Framework, discovery_id)
    if lmsDetailList.size() > 0:
        lmsDetailColumns = OracleLMSDetail.getDetailColumns()
        fileContentList = getConfigFileContentList(fileContentList, 'LMS_DETAIL', lmsDetailColumns, lmsDetailList)
    lmsDetailList.clear()

    lmsVLicenseList = OracleLMSVLicense.getVLicenseFromProbe(Framework, discovery_id)
    if lmsVLicenseList.size() > 0:
        lmsVLicenseColumns = OracleLMSVLicense.getVLicenseColumns()
        fileContentList = getConfigFileContentList(fileContentList, 'LMS_V$LICENSE', lmsVLicenseColumns, lmsVLicenseList)
    lmsVLicenseList.clear()

    lmsVSessionList = OracleLMSVSession.getVSessionFromProbe(Framework, discovery_id)
    if lmsVSessionList.size() > 0:
        lmsVSessionColumns = OracleLMSVSession.getVSessionColumns()
        fileContentList = getConfigFileContentList(fileContentList, 'LMS_V$SESSION', lmsVSessionColumns, lmsVSessionList)
    lmsVSessionList.clear()

    lmsDbaUsersList = OracleLMSDbaUsers.getDbaUsersFromProbe(Framework, discovery_id)
    if lmsDbaUsersList.size() > 0:
        lmsDbaUsersColumns = OracleLMSDbaUsers.getDbaUsersColumns()
        fileContentList = getConfigFileContentList(fileContentList, 'LMS_DBA_USERS', lmsDbaUsersColumns, lmsDbaUsersList)
    lmsDbaUsersList.clear()

    lmsOptionsList = OracleLMSOptions.getOptionsFromProbe(Framework, discovery_id)
    if lmsOptionsList.size() > 0:
        lmsOptionsColumns = OracleLMSOptions.getOptionsColumns()
        fileContentList = getConfigFileContentList(fileContentList, 'LMS_OPTIONS', lmsOptionsColumns, lmsOptionsList)
    lmsOptionsList.clear()
    return ''.join(fileContentList)


def getConfigFileContentList(fileContentList, tableName, tableColunms, objList):
    fileContentList.append(tableName)
    fileContentList.append('\n')
    fileContentList.append(tableColunms)
    fileContentList.append('\n')
    for obj in objList:
        fileContentList.append(repr(obj))
        fileContentList.append('\n')
    fileContentList.append('\n')
    return fileContentList


def clearOracleLMSInProbe(Framework, discovery_id):
    result = 1
    if not OracleLMSOptions.clearOptionsByDiscoveryId(Framework, discovery_id):
        result = 0
    if not OracleLMSDbaUsers.clearDbaUsersByDiscoveryId(Framework, discovery_id):
        result = 0
    if not OracleLMSDetail.clearDetailByDiscoveryId(Framework, discovery_id):
        result = 0
    if not OracleLMSOverview.clearOverviewByDiscoveryId(Framework, discovery_id):
        result = 0
    if not OracleLMSVSession.clearVSessionByDiscoveryId(Framework, discovery_id):
        result = 0
    if not OracleLMSVLicense.clearVLicenseByDiscoveryId(Framework, discovery_id):
        result = 0
    return result