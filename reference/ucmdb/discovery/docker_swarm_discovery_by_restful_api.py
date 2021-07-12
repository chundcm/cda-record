# coding=utf-8
import re
import logger
import netutils
import docker
from docker_discovery_by_restful_api import DockerDiscoverer


class DockerSwarmDiscoverer(DockerDiscoverer):
    def __init__(self, client, endpoint, ip, Framework, usedHttpProtocol, uriId):
        DockerDiscoverer.__init__(self, client, endpoint, ip, Framework)
        self.swarmClusterObj = None
        self.imageDictOnNode = {}
        self.usedHttpProtocol = usedHttpProtocol
        self.uriId = uriId


    def discoverDocker(self):
        labelList = []
        versionJson = self.client.dockerVersion()
        if versionJson:
            node = docker.Node(self.ip)
            # Swarm Daemon
            dockerSwarmDaemon = docker.DockerSwarmDaemon(self.endpoint)
            dockerSwarmDaemon.version = versionJson['Version']
            dockerSwarmDaemon.dockerNodeObj = node
            dockerSwarmDaemon.usedHttpProtocol = self.usedHttpProtocol
            dockerSwarmDaemon.uriId = self.uriId
            # Swarm Cluster
            dockerSwarmCluster = docker.DockerSwarmCluster(self.endpoint)
            dockerSwarmCluster.version = versionJson['Version']
            dockerSwarmCluster.dockerSwarmDaemonObj = dockerSwarmDaemon
            self.swarmClusterObj = dockerSwarmCluster

            dockerSwarmDaemon.dockerSwarmClusterObj = dockerSwarmCluster

            infoJson = self.client.dockerInfo()
            if infoJson:
                if infoJson['Labels']:
                    for labelKey in infoJson['Labels']:
                        labelString = labelKey + infoJson['Labels'][labelKey]
                        labelList.append(labelString)
                    dockerSwarmDaemon.labelList = labelList
                totalNum = -1
                nextNode = False
                # Here the response from swarm API is ugly:
                # ["\bNodes","2"],
                # ["Node1","Endpoint1"],
                # [" └ Status","Healthy"],
                # [" └ Containers","3"],
                # [" └ Reserved CPUs","0 / 4"],
                # [" └ Reserved Memory","0 B / 8.187 GiB"],
                # [" └ Labels","executiondriver=native-0.2, kernelversion=3.19.0-31-generic, operatingsystem=Ubuntu 15.04, storagedriver=aufs"],
                # [" └ Error","(none)"],
                # [" └ UpdatedAt","2016-06-07T02:51:42Z"],
                # ["Node2","Endpoint2"],
                # [" └ Status","Healthy"],
                # [" └ Containers","3"],
                # [" └ Reserved CPUs","0 / 2"],
                # [" └ Reserved Memory","0 B / 4.054 GiB"],
                # [" └ Labels","executiondriver=native-0.2, kernelversion=3.13.0-74-generic, operatingsystem=Ubuntu 14.04 LTS, storagedriver=aufs"],
                # [" └ Error","(none)"],
                # [" └ UpdatedAt","2016-06-07T02:52:01Z"]
                # It is an array, have to check according to the index.
                for infoArray in infoJson['DriverStatus']:
                    totalNum += 1
                    if infoArray[0].find('Nodes') != -1:
                        nextNode = True
                        totalNum = 0
                        continue
                    if nextNode and (totalNum - 1) % 8 == 0:
                        logger.debug('Node is: ', infoArray[0])
                        nodeName = infoArray[0]
                        dockerDaemonEndpoint = infoArray[1]
                        nodeIp = dockerDaemonEndpoint.split(':')[0]
                        self._getDockerDaemon(dockerSwarmCluster, nodeIp, dockerDaemonEndpoint, nodeName)
                        self.imageDictOnNode[nodeName] = {}

            return dockerSwarmDaemon


    def _getDockerDaemon(self, dockerSwarmCluster, nodeIp, dockerDaemonEndpoint, nodeName=None):

        node = docker.Node(nodeIp)
        node.setName(nodeName)
        dockerObj = docker.Docker()
        dockerObj.dockerNodeObj = node
        dockerDaemon = docker.DockerDaemon('Docker Daemon')
        dockerDaemon.dockerObj = dockerObj
        dockerDaemon.dockerNodeObj = node

        dockerSwarmCluster.dockerDaemonObjs[dockerDaemonEndpoint] = dockerDaemon
        self.dockerDaemonObj = dockerDaemon
        self.imageNodeObj = node

    def discoverImage(self):
        Images = []
        for (daemon, dockerDaemonObj) in self.swarmClusterObj.dockerDaemonObjs.items():
            nodeName = dockerDaemonObj.dockerNodeObj.getName()
            for imagesJson in self.client.dockerImagesOnNode(nodeName):
                discoveredImages = self._processImageInfo(imagesJson, dockerDaemonObj.dockerNodeObj, self.imageDictOnNode[nodeName])
                Images.extend(discoveredImages)
        return Images

    def discoverContainer(self):
        Containers = []
        Volumes = []
        for containersJson in self.client.dockerPs():
            containerJson = self.client.dockerInspectContainer(containersJson['Id'])
            if containerJson.has_key('Node') and containerJson['Node']:
                nodeName = containerJson['Node']['Name']
                dockerDaemonEndpoint = containerJson['Node']['Addr']
                dockerDaemonObj = self.swarmClusterObj.dockerDaemonObjs[dockerDaemonEndpoint]
                (discoveredContainers, discoveredVolumes) = self._getContainerInfo(dockerDaemonObj, containerJson, self.imageDictOnNode[nodeName])
                Containers.extend(discoveredContainers)
                Volumes.extend(discoveredVolumes)
            else:
                logger.debug('Can not get node data from Swarm!')

        self._linkContainers()

        return Containers, Volumes


