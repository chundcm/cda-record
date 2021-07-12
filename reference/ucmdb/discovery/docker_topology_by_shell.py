# coding=utf-8
import logger
import shellutils
import docker_discovery_by_shell
import docker_shell_client
import docker_reporter

from appilog.common.system.types.vectors import ObjectStateHolderVector
from com.hp.ucmdb.discovery.common import CollectorsConstants

def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    client = Framework.createClient()
    shell = shellutils.ShellFactory().createShell(client)


    dockerShellClient = docker_shell_client.DockerClient(shell)
    # Discover Docker
    dockerDaemon, dockerImages, dockerContainers, dockerVolumes = discoverDockerTopology(dockerShellClient, Framework)
    # Report Docker Topology
    if dockerDaemon:
        dockerReporter = docker_reporter.DockerReporter(client, Framework, dockerDaemon, dockerImages, dockerContainers, dockerVolumes)
        OSHVResult.addAll(dockerReporter.reportDockerTopology())

    reportWarning = OSHVResult.size() == 0
    if reportWarning:
        msg = 'Failed to Discover Docker Topology!'
        logger.reportWarning(msg)
        logger.debug(msg)
    return OSHVResult

def discoverDockerTopology(client, Framework):
    dockerDiscoverer = docker_discovery_by_shell.DockerDiscovererShell(client, Framework)
    dockerDaemon = dockerDiscoverer.discoverDocker()
    if dockerDaemon:
        dockerImages = dockerDiscoverer.discoverImage()
        dockerContainers, dockerVolumes = dockerDiscoverer.discoverContainer()
        return dockerDaemon, dockerImages, dockerContainers, dockerVolumes
    else:
        logger.error('Failed to Discover Docker Daemon!')
        logger.reportError('Failed to Discover Docker Daemon!')
        return None, None, None, None


