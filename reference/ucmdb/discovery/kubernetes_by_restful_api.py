# coding=utf-8
import re
import sys
import logger
import kubernetes_discoverer
import kubernetes_restful_client

from appilog.common.system.types.vectors import ObjectStateHolderVector
from com.hp.ucmdb.discovery.common import CollectorsConstants

def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()

    ip = Framework.getDestinationAttribute('ip_address')
    endpoint = Framework.getDestinationAttribute('endpoint')
    credentials = Framework.getAvailableProtocols(ip, 'http')

    if re.search('/$', endpoint):
        endpoint = endpoint[0:-1]

    logger.debug('Endpoint: ', endpoint)

    usedHttpProtocol = None
    k8sClient = None
    if re.match('https', endpoint):
        useProtocolType = 'https'
    else:
        useProtocolType = 'http'

    if useProtocolType == 'https':
        if not credentials:
            msg = 'Protocol not defined or IP out of protocol network range'
            logger.reportWarning(msg)
            logger.error(msg)
            return OSHVResult

        for credential in credentials:
            try:
                protocolType = Framework.getProtocolProperty(credential, CollectorsConstants.PROTOCOL)
                if protocolType != useProtocolType:
                    logger.debug('Protocol type does not match, try next one! ')
                    continue
                keyStorePath = ''
                keyStorePass = ''
                keyPass = ''

                try:
                    keyStorePath = Framework.getProtocolProperty(credential, 'keyStorePath')
                    keyStorePass = Framework.getProtocolProperty(credential, 'keyStorePass')
                    keyPass = Framework.getProtocolProperty(credential, 'keyPass')
                except:
                    pass

                useCredential = kubernetes_restful_client.K8sCredential(keyStorePath, keyStorePass, keyPass)

                k8sClient = getK8sClient(endpoint, useCredential)
                if k8sClient:
                    usedHttpProtocol = credential
                    break
            except:
                strException = str(sys.exc_info()[1])
                excInfo = logger.prepareJythonStackTrace('')
                logger.debug(strException)
                logger.debug(excInfo)
                logger.debug("Kubernetes can not be connected, try next credential!")
                pass
    else:
        k8sClient = getK8sClient(endpoint, None)

    if k8sClient:
        k8sDiscoverer = kubernetes_discoverer.K8sDiscoverer(k8sClient, endpoint, ip, Framework)
        k8sDiscoverer.discover()
        OSHVResult.addAll(k8sDiscoverer.report())

    reportWarning = OSHVResult.size() == 0
    if reportWarning:
        msg = 'Failed to Discover Kubernetes Topology!'
        logger.reportWarning(msg)
        logger.debug(msg)
    return OSHVResult


def getK8sClient(endpoint, useCredential):
    client = kubernetes_restful_client.K8sClient(endpoint, useCredential)
    # Connect to Kubernetes
    nodesJson = client.listNodes()
    if nodesJson:
        return client
    return None
