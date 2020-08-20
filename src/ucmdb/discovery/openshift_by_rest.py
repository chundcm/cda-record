# coding=utf-8
import sys
import logger
import openshift_discoverer
import kubernetes_discoverer

from appilog.common.system.types.vectors import ObjectStateHolderVector


def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    ip = Framework.getDestinationAttribute('ip')
    protocols = Framework.getAvailableProtocols(ip, 'http')

    if len(protocols) == 0:
        msg = 'Protocol not defined or IP out of protocol network range'
        logger.reportWarning(msg)
        return OSHVResult

    for protocol in protocols:
        try:
            logger.debug('connect with protocol:', protocol)
            client = openshift_discoverer.OpenShiftClient(Framework, protocol)
            if client:
                openShiftDiscoverer = openshift_discoverer.OpenShiftDiscoverer(client)
                openShiftDiscoverer.discover()
                OSHVResult.addAll(openShiftDiscoverer.report())
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
