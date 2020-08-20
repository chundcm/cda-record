#coding=utf-8
import logger
import modeling
import errormessages
import sys

from java.lang import Exception as JException
from java.util import Properties
from appilog.common.utils import Protocol
from com.hp.ucmdb.discovery.common import CollectorsConstants

from appilog.common.system.types.vectors import ObjectStateHolderVector

import Oracle_SQL_Discoverer

protocolName = "SQL"

def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()

    hostId = Framework.getDestinationAttribute('hostId')
    oracle_id = Framework.getDestinationAttribute('id')
    root_id = Framework.getDestinationAttribute('root_id')
    isOracle = True
    if root_id and root_id != 'NA':
        isOracle = False
        discoveredHostOSH = modeling.createOshByCmdbIdString('oracle_db_service', root_id)
    else:
        discoveredHostOSH = modeling.createOshByCmdbIdString('host', hostId)
    oracleOSH = modeling.createOshByCmdbIdString('oracle', oracle_id)
    oracleClient = None
    
    credentialsId = Framework.getDestinationAttribute('credentialsId')

    instanceName = Framework.getDestinationAttribute('sid')
    protocolDbSid = Framework.getProtocolProperty(credentialsId, CollectorsConstants.SQL_PROTOCOL_ATTRIBUTE_DBSID, 'NA')
    try:
        #in some cases sid does not coinside to the instance name, so real sid should be used
        #e.g. when sid is written down in a world unique identifiing string format <instance name>.fulldomainname
        oracleClient = None
        if protocolDbSid and protocolDbSid != 'NA' and protocolDbSid != instanceName:
            try:
                props = Properties()
                props.setProperty(Protocol.SQL_PROTOCOL_ATTRIBUTE_DBSID, protocolDbSid)
                oracleClient = Framework.createClient(props)
            except:
                logger.debug('Failed to connect using sid defined in creds. Will try instance name as sid.')
                oracleClient = None
        if not oracleClient:
            props = Properties()
            props.setProperty(Protocol.SQL_PROTOCOL_ATTRIBUTE_DBSID, instanceName)
            oracleClient = Framework.createClient(props)

        # method "discoverDBTopology" is for getting and reporting Listener, IpServiceEndpoint, Service Name
        Oracle_SQL_Discoverer.discoverDBTopology(OSHVResult, oracleClient, oracleOSH,isOracle,discoveredHostOSH)
        Oracle_SQL_Discoverer.discoverOracle(oracleClient, oracleOSH, discoveredHostOSH, Framework)
    except JException, ex:
        strException = str(ex.getMessage())
        errormessages.resolveAndReport(strException, protocolName, Framework)
        logger.debug(logger.prepareFullStackTrace(strException))
    except:
        excInfo = str(sys.exc_info()[1])
        errormessages.resolveAndReport(excInfo, protocolName, Framework)
        logger.debug(logger.prepareFullStackTrace(''))

    if (oracleClient is not None):
        oracleClient.close()
    return OSHVResult
