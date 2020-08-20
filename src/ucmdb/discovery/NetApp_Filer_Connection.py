#coding=utf-8
import logger
import modeling
import netapp_webservice_utils

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector
from com.hp.ucmdb.discovery.common import CollectorsConstants

from netapp.manage import NaElement
from netapp.manage import NaServer


def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()

    protocolName = 'netapp'
    ip = Framework.getDestinationAttribute('ip_address')

    connection_success = False
    try:
        credentials = Framework.getAvailableProtocols(ip, protocolName)
        if credentials:
            for credentialId in credentials:
                wsConnection = None
                try:
                    ontapiVersion = '0.0'
                    logger.debug('Try credential:', credentialId)
                    userName = Framework.getProtocolProperty(credentialId, CollectorsConstants.PROTOCOL_ATTRIBUTE_USERNAME)
                    password = Framework.getProtocolProperty(credentialId, CollectorsConstants.PROTOCOL_ATTRIBUTE_PASSWORD)
                    port = Framework.getProtocolProperty(credentialId, CollectorsConstants.PROTOCOL_ATTRIBUTE_PORT) or '443'
                    protocol = Framework.getProtocolProperty(credentialId, "netappprotocol_protocol") or 'https'
                    wsConnection = netapp_webservice_utils.connect(protocol, ip, port, userName, password, NaServer.SERVER_TYPE_FILER)
                    if wsConnection:## Get soap version
                        aboutRequestElem = NaElement('system-get-version')
                        aboutResponseElem = netapp_webservice_utils.wsInvoke(wsConnection, aboutRequestElem)
                        is_clustered = aboutResponseElem.getChildContent('is-clustered')
                        version = aboutResponseElem.getChildContent('version')
                        if version:
                            nodeOSH = modeling.createHostOSH(ip)
                            OSHVResult.add(nodeOSH)
                            netAppRsOSH = ObjectStateHolder('running_software')
                            if is_clustered == 'true':
                                netAppRsOSH.setStringAttribute('name', 'NetApp Cluster')
                                netAppRsOSH.setStringAttribute('discovered_product_name', 'NetApp Cluster')

                            else:
                                netAppRsOSH.setStringAttribute('name', 'NetApp Filer')
                                netAppRsOSH.setStringAttribute('discovered_product_name', 'NetApp Filer')
                            netAppRsOSH.setStringAttribute('application_ip', ip)
                            netAppRsOSH.setStringAttribute('credentials_id', credentialId)
                            netAppRsOSH.setContainer(nodeOSH)
                            OSHVResult.add(netAppRsOSH)
                            connection_success = True
                            break
                except:
                    logger.debugException('')
                finally:
                    if wsConnection:
                        try:
                            wsConnection.close()
                        except:
                            pass
        else:
            logger.warn('No netapp credential found for ip:', ip)
    except:
        logger.debugException('')

    if connection_success:
        logger.debug('Connection Success')
    else:
        logger.debug('No NetApp Filer detected on: ', ip)
        Framework.reportWarning('No NetApp Filer detected')

    return OSHVResult