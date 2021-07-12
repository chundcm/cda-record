# coding=utf-8
import sys
import logger
import emc_ecs_discoverer

from appilog.common.system.types.vectors import ObjectStateHolderVector


def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    ip = Framework.getDestinationAttribute('ip')
    protocols = Framework.getAvailableProtocols(ip, 'http')

    if len(protocols) == 0:
        msg = 'Protocol not defined or IP out of protocol network range'
        logger.reportWarning(msg)
        return OSHVResult
    proxies = {}

    for protocol in protocols:
        try:
            logger.debug('connect with protocol:', protocol)
            endpoint = Framework.getDestinationAttribute('endpoint')
            http_proxy = Framework.getProtocolProperty(protocol, "proxy", "")

            if http_proxy:
                logger.debug("proxy:", http_proxy)
                proxies['http'] = http_proxy
                proxies['https'] = http_proxy
            client = emc_ecs_discoverer.ECSClient(Framework, protocol, endpoint, proxies)
            if client:
                logger.debug("connect to ECS successfully!")
                OSHVResult.addAll(emc_ecs_discoverer.Discover(client))
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
