# coding=utf-8
import sys
import logger
import docker_discovery_by_restful_api
import docker_restful_client
import docker_reporter

from appilog.common.system.types.vectors import ObjectStateHolderVector
from com.hp.ucmdb.discovery.common import CollectorsConstants

def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()

    ip = Framework.getDestinationAttribute('ip_address')
    credentialId = Framework.getDestinationAttribute('credentialId')
    endpoint = Framework.getDestinationAttribute('endpoint')
    logger.debug('endpoint: ', endpoint)
    try:
        protocolType = Framework.getProtocolProperty(credentialId, CollectorsConstants.PROTOCOL)
        if protocolType == 'https':
            keyStorePath = Framework.getProtocolProperty(credentialId, 'keyStorePath')
            keyStorePass = Framework.getProtocolProperty(credentialId, 'keyStorePass')
            keyPass = Framework.getProtocolProperty(credentialId, 'keyPass')
            useCredential = docker_restful_client.DockerCredential(keyStorePath, keyStorePass, keyPass)
        else:
            useCredential = None
        client = docker_restful_client.DockerClient(endpoint, useCredential)
        # Discover Docker
        dockerDaemon, dockerImages, dockerContainers, dockerVolumes = discoverDockerTopology(client, endpoint, ip, Framework)
        # Report Docker Topology
        dockerReporter = docker_reporter.DockerReporter(client, Framework, dockerDaemon, dockerImages, dockerContainers, dockerVolumes)
        OSHVResult.addAll(dockerReporter.reportDockerTopology())
    except:
        strException = str(sys.exc_info()[1])
        excInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException)
        logger.debug(excInfo)
        pass

    reportWarning = OSHVResult.size() == 0
    if reportWarning:
        msg = 'Failed to Discover Docker Topology!'
        logger.reportWarning(msg)
        logger.debug(msg)
    return OSHVResult

def discoverDockerTopology(client, endpoint, ip, Framework):
    dockerDiscoverer = docker_discovery_by_restful_api.DockerDiscoverer(client, endpoint, ip, Framework)
    dockerDaemon = dockerDiscoverer.discoverDocker()
    dockerImages = dockerDiscoverer.discoverImage()
    dockerContainers, dockerVolumes = dockerDiscoverer.discoverContainer()

    if not dockerDaemon:
        logger.warn( 'Failed to Discover Docker Daemon!')

    return dockerDaemon, dockerImages, dockerContainers, dockerVolumes


