# coding=utf-8
import re
import logger
import docker
import modeling
import process as process_module
from docker_discovery_by_restful_api import DockerDiscoverer
import rest_json as json



class DockerDiscovererShell(DockerDiscoverer):
    def __init__(self, client, Framework):
        DockerDiscoverer.__init__(self, client, None, None, Framework)
        self.filesystemDict = {}
        self._linkDockerVolume = None

    def discoverDocker(self):
        self._linkDockerVolume = self._getFilesystem()
        version = self.client.dockerVersion()
        if version:
            nodeId = self.Framework.getTriggerCIData('hostId')
            nodeOSH = modeling.createOshByCmdbIdString("node", nodeId)
            node = docker.Node(None)
            node.setOsh(nodeOSH)
            dockerObj = docker.Docker()
            dockerObj.dockerNodeObj = node
            dockerDaemon = docker.DockerDaemon('Docker Daemon')
            dockerDaemon.version = version
            dockerDaemon.dockerObj = dockerObj
            dockerDaemon.dockerNodeObj = node
            dockerDaemonId = self.Framework.getTriggerCIData("triggerId")
            dockerDaemonOSH = modeling.createOshByCmdbIdString("docker_daemon", dockerDaemonId)
            dockerDaemon.setOsh(dockerDaemonOSH)
            self.dockerDaemonObj = dockerDaemon
            self.imageNodeObj = node

            infoLines = self.client.dockerInfo()
            if infoLines:
                labels = False
                labelList = []
                for infoLine in infoLines:
                    infoLine = infoLine.strip()
                    if not infoLine:
                        continue
                    logger.debug('docker info: ', infoLine)

                    if infoLine.find("Name:") != -1:
                        hostName = infoLine.split()[1].strip()
                        dockerDaemon.hostName = hostName
                        continue
                    if infoLine.find("Logging Driver:") != -1:
                        loggingDriver = infoLine.split("Logging Driver:")[1].strip()
                        dockerDaemon.loggingDriver = loggingDriver
                        continue
                    if infoLine.find("Labels:") != -1:
                        labels = True
                        continue
                    if labels:
                        if infoLine.find("=") != -1:
                            labelList.append(infoLine.strip())
                        else:
                            labels = False
                dockerDaemon.labelList = labelList
                return dockerDaemon

    def discoverImage(self):
        Images = []
        imagesLines = self.client.dockerImages()
        if imagesLines:
            for i in range(1, len(imagesLines)):
                imageLine = imagesLines[i].strip()
                imageInfo = imageLine.split()
                if not imageInfo:
                    continue
                logger.debug('docker image: ', imageInfo[0])
                imageName = imageInfo[0]
                imageTag = imageInfo[1]
                imageInspectOutput = self.client.dockerInspect(imageInfo[2])

                jsonOutput = json.loads(imageInspectOutput)
                imagesJson = jsonOutput[0]
                imageObj = self._recordImageInfo(imagesJson, self.imageNodeObj, self.imageDict, imageName, imageTag)
                Images.append(imageObj)
            return Images

    def discoverContainer(self):
        Containers = []
        Volumes = []
        containersLines = self.client.dockerPs()
        if containersLines:
            for i in range(1, len(containersLines)):
                containerLine = containersLines[i].strip()
                containerInfo = containerLine.split()
                if not containerInfo:
                    continue
                logger.debug('docker container: ', containerInfo[0])
                containerInspectOutput = self.client.dockerInspect(containerInfo[0])
                if containerInspectOutput:
                    jsonOutput = json.loads(containerInspectOutput)
                    containersJson = jsonOutput[0]
                    (discoveredContainers, discoveredVolumes) = self._getContainerInfo(self.dockerDaemonObj, containersJson, self.imageDict)
                    Containers.extend(discoveredContainers)
                    Volumes.extend(discoveredVolumes)

            self._linkContainers()

        return Containers, Volumes

    def _getCntainerLinks(self, containerJson):
        containerId = containerJson['Id']
        if containerJson['HostConfig'].has_key('Links') and containerJson['HostConfig']['Links']:
            self.containerLinks[containerId] = []
            for link in containerJson['HostConfig']['Links']:
                linkedContainer = link.split(':')[0].split('/')[1]
                linkedContainer = self.client.dockerInspectFormated(linkedContainer)
                if linkedContainer:
                    linkedContainerId = linkedContainer.strip()
                    self.containerLinks[containerId].append(linkedContainerId)
                else:
                    logger.warn(('Failed in getting linked container <%s>.' % linkedContainer))

    def _discoverProcesses(self, containerId):
        processList = []
        topLines = self.client.dockerTop(containerId)
        for i in range(1, len(topLines)):
            topLine = topLines[i].strip()
            if not topLine:
                continue
            matcher = re.match(r'\s*(\w+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(.+)\s+(.+)\s+(\d+):(\d+):(\d+)\s+(.+)', topLine)
            if matcher:
                owner = matcher.group(1)
                pid = matcher.group(2)
                commandLine = matcher.group(10)
                fullCommand = None
                argumentsLine = None

                if commandLine:
                    tokens = re.split(r"\s+", commandLine, 1)
                    fullCommand = tokens[0]
                    if len(tokens) > 1:
                        argumentsLine = tokens[1]

                commandName = fullCommand
                commandPath = None
                matcher = re.match(r"(.*/)([^/]+)$", fullCommand)
                if matcher:
                    commandName = matcher.group(2)
                    commandPath = fullCommand

                process = process_module.Process(commandName, pid, commandLine)
                logger.debug('process generated: ', process)
                process.argumentLine = argumentsLine
                process.owner = owner
                process.executablePath = commandPath
                processList.append(process)
        return processList

    def _getDockerVolume(self, containerJson, dockerContainer):
        Volumes = []
        # get container volumes
        if containerJson.has_key('Volumes') and containerJson['Volumes']:
            volumResults = containerJson['Volumes']
            for (dst, src) in volumResults.items():
                dockerVolume = docker.DockerVolume()
                dockerVolume.volumeNodeObj = self.imageNodeObj
                dockerVolume.volumeSrc = src
                dockerVolume.volumeDst = dst
                accessType = containerJson['VolumesRW'][dst]
                if accessType or accessType == 'true' or accessType == 'True':
                    dockerVolume.accessType = 'RW'
                else:
                    dockerVolume.accessType = 'R'
                Volumes.append(dockerVolume)
                logger.debug(dockerVolume.volumeSrc + '->' + dockerVolume.volumeDst)
                if self._linkDockerVolume:
                    self._linkDockerVolumeToLv(src, self.filesystemDict, dockerVolume)
        elif containerJson.has_key('Mounts') and containerJson['Mounts']:
            mountResults = containerJson['Mounts']
            for mountStr in mountResults:
                mount = mountStr
                dockerVolume = docker.DockerVolume()
                dockerVolume.volumeNodeObj = self.imageNodeObj
                dockerVolume.volumeSrc = mount['Source']
                dockerVolume.volumeDst = mount['Destination']
                accessType = mount['RW']
                if accessType or accessType == 'true' or accessType == 'True':
                    dockerVolume.accessType = 'RW'
                else:
                    dockerVolume.accessType = 'R'
                Volumes.append(dockerVolume)
                logger.debug(dockerVolume.volumeSrc + '->' + dockerVolume.volumeDst)
                if self._linkDockerVolume:
                    self._linkDockerVolumeToLv(mount['Source'], self.filesystemDict, dockerVolume)
        dockerContainer.usedVolumeObjs = Volumes
        return Volumes

    def _linkDockerVolumeToLv(self, volumeSource, filesystemDict, dockerVolumeObj):
        for mountPoint in filesystemDict.keys():
            if re.match(mountPoint, volumeSource):
                logicalVolume = docker.LogicalVolume(filesystemDict[mountPoint])
                dockerVolumeObj.logicalVolumeObj = logicalVolume
                break

    def _getFilesystem(self):
        linkDockerVolume = False
        dfLines = self.client.df()
        if not dfLines:
            logger.debug('Fail to get logicol volume infomation.')
        else:
            for i in range(1, len(dfLines)):
                dfLine = dfLines[i].strip()
                fileSystemInfo = dfLine.split()
                if not fileSystemInfo:
                    continue
                logger.debug('Filesystem: ', fileSystemInfo)
                fileSystemName = fileSystemInfo[0]
                fileSystemMount = fileSystemInfo[-1]
                self.filesystemDict[fileSystemMount] = fileSystemName
                linkDockerVolume = True
            return linkDockerVolume

