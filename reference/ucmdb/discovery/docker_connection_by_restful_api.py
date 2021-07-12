# coding=utf-8
import sys
import logger
import modeling
import docker_restful_client

from appilog.common.system.types.vectors import ObjectStateHolderVector
from appilog.common.system.types import ObjectStateHolder
from com.hp.ucmdb.discovery.common import CollectorsConstants

def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    connectSuccess = False

    ip = Framework.getDestinationAttribute('ip_address')
    credentials = Framework.getAvailableProtocols(ip, 'http')
    probe_name= Framework.getDestinationAttribute('probe_name')

    if len(credentials) == 0:
        msg = 'Protocol not defined or IP out of protocol network range'
        logger.reportWarning(msg)
        logger.error(msg)
        return OSHVResult

    for credential in credentials:
        try:
            port = Framework.getProtocolProperty(credential, CollectorsConstants.PROTOCOL_ATTRIBUTE_PORT)
            protocolType = Framework.getProtocolProperty(credential, CollectorsConstants.PROTOCOL)
            keyStorePath = ''
            keyStorePass = ''
            keyPass = ''
            try:
                keyStorePath = Framework.getProtocolProperty(credential, 'keyStorePath')
                keyStorePass = Framework.getProtocolProperty(credential, 'keyStorePass')
                keyPass = Framework.getProtocolProperty(credential, 'keyPass')
            except:
                pass
            endpoint = protocolType + '://' + ip + ':' + port + '/'
            logger.debug('Endpoint: ', endpoint)
            if protocolType == 'https':
                useCredential = docker_restful_client.DockerCredential(keyStorePath, keyStorePass, keyPass)
            else:
                useCredential = None

            client = docker_restful_client.DockerClient(endpoint, useCredential)
            # Connect to Docker Daemon
            versionJson = client.dockerVersion()
            if versionJson and versionJson.has_key('ApiVersion') and versionJson['ApiVersion']:
                if versionJson['ApiVersion'] > '1.18':
                    infoJson = client.dockerInfo()
                    if infoJson and infoJson.has_key('DockerRootDir'):
                        uriEndpointOsh = ObjectStateHolder('uri_endpoint')
                        uriEndpointOsh.setAttribute('uri', endpoint)
                        uriEndpointOsh.setAttribute('type', 'docker')
                        uriEndpointOsh.setAttribute('credentials_id', credential)
                        uriEndpointOsh.setAttribute("uri_probename", probe_name)
                        OSHVResult.add(uriEndpointOsh)

                        ipOsh = modeling.createIpOSH(ip)
                        OSHVResult.add(ipOsh)
                        OSHVResult.add(modeling.createLinkOSH('dependency', uriEndpointOsh, ipOsh))
                        connectSuccess = True
                        break
            else:
                logger.debug("Docker daemon can not be connected, try next credential!")
                continue
        except:
            strException = str(sys.exc_info()[1])
            excInfo = logger.prepareJythonStackTrace('')
            logger.debug(strException)
            logger.debug(excInfo)
            pass

    if not connectSuccess:
        msg = 'Fail to connect to Docker Daemon on: ' + ip
        logger.reportWarning(msg)
        logger.debug(msg)

    return OSHVResult

