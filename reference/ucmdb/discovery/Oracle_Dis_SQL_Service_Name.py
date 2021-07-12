#coding=utf-8

import sys
import logger
import errormessages
import modeling

from com.hp.ucmdb.discovery.common import CollectorsConstants
from java.lang import Exception as JException
from appilog.common.system.types.vectors import ObjectStateHolderVector
from java.util import Properties
from appilog.common.utils import Protocol

import Oracle_SQL_Discoverer

protocolName = "SQL"

def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    oracleClient = None

    hostId = Framework.getDestinationAttribute('hostId')
    hostOSH = modeling.createOshByCmdbIdString('host', hostId)

    oracle_id = Framework.getDestinationAttribute('oracleId')
    oracleOSH = modeling.createOshByCmdbIdString('oracle', oracle_id)

    serviceName = Framework.getDestinationAttribute('serviceName')

    port = Framework.getDestinationAttribute('port')
    ipAddress = Framework.getDestinationAttribute('ip_address')

    try:
        props = Properties()
        props.setProperty(Protocol.SQL_PROTOCOL_ATTRIBUTE_DBSID, serviceName)
        props.setProperty(CollectorsConstants.DESTINATION_DATA_IP_ADDRESS, ipAddress)
        props.setProperty(CollectorsConstants.PROTOCOL_ATTRIBUTE_PORT, port)
        oracleClient = Framework.createClient(props)

        Oracle_SQL_Discoverer.discoverDBTopology(OSHVResult, oracleClient, oracleOSH)
        Oracle_SQL_Discoverer.discoverOracle(oracleClient, oracleOSH, hostOSH, Framework)
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
