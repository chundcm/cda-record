# coding=utf-8
import re
import sys
import logger
import docker_swarm_discovery_by_restful_api
import docker_restful_client
import docker_swarm_reporter

from appilog.common.system.types.vectors import ObjectStateHolderVector
from com.hp.ucmdb.discovery.common import CollectorsConstants

def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()

    ip = Framework.getDestinationAttribute('ip_address')
    endpoint = Framework.getDestinationAttribute('endpoint')
    uriId = Framework.getDestinationAttribute('endpoint_id')
    credentials = Framework.getAvailableProtocols(ip, 'http')

    if re.search('/$', endpoint):
        endpoint = endpoint[0:-1]

    logger.debug('Endpoint: ', endpoint)

    usedHttpProtocol = None
    dockerClient = None
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

                useCredential = docker_restful_client.DockerCredential(keyStorePath, keyStorePass, keyPass)

                dockerClient = getDockerClient(endpoint, useCredential)
                if dockerClient:
                    usedHttpProtocol = credential
                    break
            except:
                strException = str(sys.exc_info()[1])
                excInfo = logger.prepareJythonStackTrace('')
                logger.debug(strException)
                logger.debug(excInfo)
                logger.debug("Docker Swarm daemon can not be connected, try next credential!")
                pass
    else:
        dockerClient = getDockerClient(endpoint, None)

    if dockerClient:
        # Discover Docker Swarm
        dockerSwarmDaemon, dockerImages, dockerContainers, dockerVolumes = discoverDockerTopology(dockerClient, endpoint, ip, Framework, usedHttpProtocol, uriId)
        # Report Docker Swarm Topology
        dockerReporter = docker_swarm_reporter.DockerSwarmReporter(dockerClient, Framework, dockerSwarmDaemon, dockerImages, dockerContainers, dockerVolumes)
        OSHVResult.addAll(dockerReporter.reportDockerTopology())
    else:
        logger.debug("Fail to connect to Docker Swarm using all credentials!")

    reportWarning = OSHVResult.size() == 0
    if reportWarning:
        msg = 'Failed to Discover Docker Topology!'
        logger.reportWarning(msg)
        logger.debug(msg)
    return OSHVResult

def getDockerClient(endpoint, useCredential):
    client = docker_restful_client.DockerClient(endpoint, useCredential)
    # Connect to Docker Swarm Daemon
    versionJson = client.dockerVersion()
    if versionJson and versionJson.has_key('ApiVersion') and versionJson['ApiVersion']:
        if versionJson['Version'].find('swarm') != -1 and versionJson['ApiVersion'] > '1.18':
            return client
    return None

def discoverDockerTopology(client, endpoint, ip, Framework, usedHttpProtocol, uriId):
    dockerDiscoverer = docker_swarm_discovery_by_restful_api.DockerSwarmDiscoverer(client, endpoint, ip, Framework, usedHttpProtocol, uriId)
    dockerSwarmDaemon = dockerDiscoverer.discoverDocker()
    dockerImages = dockerDiscoverer.discoverImage()
    dockerContainers, dockerVolumes = dockerDiscoverer.discoverContainer()

    if not dockerSwarmDaemon:
        logger.warn('Failed to Discover Docker Daemon!')
        logger.reportWarning( 'Failed to Discover Docker Daemon!')
    if not dockerImages:
        logger.warn('Failed to Discover Docker Images!')
        logger.reportWarning('Failed to Discover Docker Images!')
    if not dockerContainers:
        logger.warn('Failed to Discover Docker Containers!')
        logger.reportWarning( 'Failed to Discover Docker Containers!')

    return dockerSwarmDaemon, dockerImages, dockerContainers, dockerVolumes


