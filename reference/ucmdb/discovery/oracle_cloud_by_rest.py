# coding=utf-8
import sys
import logger
import oracle_cloud_discoverer

from appilog.common.system.types.vectors import ObjectStateHolderVector
from com.hp.ucmdb.discovery.common import CollectorsConstants


def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    dataflowprobe = Framework.getDestinationAttribute('probeName')
    protocols = Framework.getAvailableProtocols(None, 'http')

    if len(protocols) == 0:
        msg = 'Protocol not defined or IP out of protocol network range'
        logger.reportWarning(msg)
        return OSHVResult

    for protocol in protocols:
        try:
            logger.debug('connect with protocol:', protocol)
            username = Framework.getProtocolProperty(protocol, CollectorsConstants.PROTOCOL_ATTRIBUTE_USERNAME, '')
            list = username.split('/')
            if len(list) < 3:
                raise Exception('Failed to connect with protocol %s, wrong format. It should be Oracle two-part user name.' % protocol)

            oracle_cloud_discoverer.init_regions()
            for region in oracle_cloud_discoverer.REGIONS:
                try:
                    logger.debug('Connecting to Oracle Cloud region: %s' % region)
                    client = oracle_cloud_discoverer.OracleCloudClient(Framework, protocol, oracle_cloud_discoverer.REGIONS[region])
                    discoverer = oracle_cloud_discoverer.OracleCloudDiscoverer(client)
                    discoverer.discover()
                    OSHVResult.addAll(oracle_cloud_discoverer.OracleCloudReporter(discoverer).report())
                except:
                    strException = str(sys.exc_info()[1])
                    excInfo = logger.prepareJythonStackTrace('')
                    logger.debug(strException)
                    logger.debug(excInfo)
        except:
            strException = str(sys.exc_info()[1])
            excInfo = logger.prepareJythonStackTrace('')
            logger.debug(strException)
            logger.debug(excInfo)
            pass

    reportError = OSHVResult.size() == 0
    if reportError:
        msg = 'Failed to connect using all protocols'
        logger.reportError(msg)
        logger.error(msg)
    return OSHVResult
