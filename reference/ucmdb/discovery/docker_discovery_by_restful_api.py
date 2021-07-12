# coding=utf-8
import re
import logger
import docker
import netutils
import process as process_module


class DockerDiscoverer:
    def __init__(self, client, endpoint, ip, Framework):
        if not client:
            raise ValueError('No client passed.')
        self.client = client
        self.ip = ip
        self.Framework = Framework
        self.imageNodeObj = None
        self.dockerDaemonObj = None
        self.imageDict = {}
        self.containerDict = {}
        self.containerLinks = {}
        self.endpoint = endpoint
        if endpoint and re.search("https?://", endpoint):
            prefix = re.compile("https?://")
            self.endpoint = prefix.split(endpoint)[1]
        if self.endpoint and re.search("/$", self.endpoint):
            postfix = re.compile("/$")
            self.endpoint = postfix.split(self.endpoint)[0]
        self.daemonEndpoint = self.endpoint
        if self.endpoint and self.daemonEndpoint.find(':') != -1:
            self.daemonPort = self.daemonEndpoint.split(':')[1]


    def discoverDocker(self):
        labelList = []

        versionJson = self.client.dockerVersion()
        if versionJson:
            node = docker.Node(self.ip)
            dockerObj = docker.Docker()
            dockerObj.dockerNodeObj = node
            dockerDaemon = docker.DockerDaemon('Docker Daemon')
            dockerDaemon.version = versionJson['Version']
            dockerDaemon.dockerObj = dockerObj
            dockerDaemon.dockerNodeObj = node
            self.dockerDaemonObj = dockerDaemon
            self.imageNodeObj = node

            infoJson = self.client.dockerInfo()
            if infoJson:
                hostName = infoJson['Name']
                loggingDriver = infoJson['LoggingDriver']
                dockerDaemon.hostName = hostName
                dockerDaemon.loggingDriver = loggingDriver
                if infoJson['Labels']:
                    for labelString in infoJson['Labels']:
                        labelList.append(labelString)
                    dockerDaemon.labelList = labelList

            return dockerDaemon

    def discoverImage(self):
        Images = []
        for imagesJson in self.client.dockerImages():
            discoveredImages = self._processImageInfo(imagesJson, self.imageNodeObj, self.imageDict)
            Images.extend(discoveredImages)
        return Images

    def _linkImageContainer(self, dockerContainer, imageDict):
        imageName = dockerContainer.imageName
        imageId = dockerContainer.imageId
        containerId = dockerContainer.containerId
        if imageName.count(':') == 1:
            if imageName.find('/') == -1:
                imageRepository = imageName.split(':')[0]
                imageTag = imageName.split(':')[1]
            else:
                if imageName.find('/') > imageName.find(':'):
                    imageRepository = imageName
                    imageTag = 'latest'
                else:
                    imageRepository = imageName.split(':')[0]
                    imageTag = imageName.split(':')[1]
        elif imageName.count(':') == 2:
            colonIndex = imageName.rfind(':')
            imageRepository = imageName[0:colonIndex]
            imageTag = imageName[colonIndex+1:]
        else:
            imageRepository = imageName
            imageTag = 'latest'

        if imageDict.has_key(imageId) and \
                imageDict[imageId].has_key(imageRepository) and \
                imageDict[imageId][imageRepository].has_key(imageTag):
            imageObj = imageDict[imageId][imageRepository][imageTag]
            logger.debug(repr(imageObj))
            logger.debug(repr(dockerContainer))
            dockerContainer.imageObj = imageObj
        else:
            logger.debug('Container %s does not have its image. ' % containerId)

    def _linkContainers(self):
        for containerId in self.containerLinks.keys():
            linkedObjs = []
            for linkedContainerId in self.containerLinks[containerId]:
                linkedObjs.append(self.containerDict[linkedContainerId])
            self.containerDict[containerId].linkToContainerObjs.extend(linkedObjs)

    def discoverContainer(self):
        Containers = []
        Volumes = []
        for containersJson in self.client.dockerPs():
            containerJson = self.client.dockerInspectContainer(containersJson['Id'])
            (discoveredContainers, discoveredVolumes) = self._getContainerInfo(self.dockerDaemonObj, containerJson, self.imageDict)
            Containers.extend(discoveredContainers)
            Volumes.extend(discoveredVolumes)

        self._linkContainers()

        return Containers, Volumes

    def _getContainerInfo(self, dockerDaemonObj, containerJson, imageDict):
        Containers = []
        Volumes = []
        # get container related image
        containerName = containerJson['Name']
        # Remove the heading slash in the container name
        if containerName[0] == '/':
            containerName = containerName[1:]
        # Add default tag latest if it does not specify
        imageName = containerJson['Config']['Image']
        if imageName.count(':') == 1:
            if imageName.find('/') != -1 and imageName.find('/') > imageName.find(':'):
                imageName = imageName + ':latest'
        elif imageName.count(':') == 2:
            pass
        else:
            imageName = imageName + ':latest'

        containerId = containerJson['Id']
        imageId = containerJson['Image']
        if imageId.find('sha256:') != -1:
            imageId = imageId.split('sha256:')[1].strip()
        dockerContainer = docker.DockerContainer()
        dockerContainer.setName(containerName)
        dockerContainer.containerId = containerId
        dockerContainer.imageId = imageId
        dockerContainer.imageName = imageName
        dockerContainer.daemonObj = dockerDaemonObj
        self.containerDict[containerId] = dockerContainer
        Containers.append(dockerContainer)

        # get volumes
        volumesUsed = self._getDockerVolume(containerJson, dockerContainer)
        Volumes.extend(volumesUsed)

        # link image and container
        self._linkImageContainer(dockerContainer, imageDict)

        # get running software in container
        discoverRSinDockerContainer = self.Framework.getParameter('discoverRunningSW')
        if discoverRSinDockerContainer == 'true':
            dockerContainer.processList = self._discoverProcesses(containerId)

        # get ports in container
        portsArray = []
        ports = containerJson['NetworkSettings']['Ports']
        if ports:
            port_keys = ports.keys()
            port_keys.sort()
            for port in port_keys:
                if ports[port]:
                    if ports[port][0]['HostIp'] and ports[port][0]['HostPort']:
                        portsArray.append(ports[port][0]['HostIp'] + ':' + ports[port][0]['HostPort'] + ' -> ' + port)
                else:
                    portsArray.append(port)
            containerPorts = ', '.join(portsArray)
            dockerContainer.containerPorts = containerPorts

        if containerJson.has_key('HostConfig') and containerJson['HostConfig']:
            # get container links
            self._getCntainerLinks(containerJson)

            # get container restart policy
            if containerJson['HostConfig'].has_key('RestartPolicy') and containerJson['HostConfig']['RestartPolicy']:
                if containerJson['HostConfig']['RestartPolicy'].has_key('Name') and containerJson['HostConfig']['RestartPolicy'].has_key('MaximumRetryCount'):
                    restartPolicy = containerJson['HostConfig']['RestartPolicy']['Name']
                    dockerContainer.restartPolicy = restartPolicy
                    restartMaxCountStr = containerJson['HostConfig']['RestartPolicy']['MaximumRetryCount']
                    if restartMaxCountStr:
                        restartMaxCount = int(restartMaxCountStr.strip())
                        dockerContainer.restartMaxCount = restartMaxCount

            # get container logging driver
            if containerJson['HostConfig'].has_key('LogConfig') and containerJson['HostConfig']['LogConfig']:
                if containerJson['HostConfig']['LogConfig'].has_key('Type') and containerJson['HostConfig']['LogConfig']['Type']:
                    loggingDriver = containerJson['HostConfig']['LogConfig']['Type']
                    dockerContainer.loggingDriver = loggingDriver

            # get container memory limit
            if containerJson['HostConfig'].has_key('Memory') and containerJson['HostConfig']['Memory'] != None:
                memoryLimitStr = containerJson['HostConfig']['Memory']
                memoryLimit = int(memoryLimitStr)
                # convert to MB
                memoryLimitInMB = memoryLimit/(1024*1024)
                dockerContainer.memoryLimitInMB = memoryLimitInMB

        if containerJson.has_key('Config') and containerJson['Config']:
            # get container commands
            if containerJson['Config'].has_key('Cmd') and containerJson['Config']['Cmd']:
                cmds = containerJson['Config']['Cmd']
                cmdList = []
                for cmd in cmds:
                    cmdList.append(cmd)
                dockerContainer.cmdList = cmdList

            # get container labels
            if containerJson['Config'].has_key('Labels') and containerJson['Config']['Labels']:
                labels = containerJson['Config']['Labels']
                labelKeys = labels.keys()
                labelKeys.sort()
                labelList = []
                for label in labelKeys:
                    labelStr = label + '=' + labels[label]
                    if len(labelStr) > 2000:
                        logger.debug('Label string of container %s is too long to store. '
                                     'Only 1995 characters will be stored. '
                                     'The original string is: %s' % (dockerContainer.containerId, labelStr))
                        labelStrCut = labelStr[0:1995] + '...'
                        labelList.append(labelStrCut)
                    else:
                        labelList.append(labelStr)
                dockerContainer.labelList = labelList

        return Containers, Volumes

    def _getCntainerLinks(self, containerJson):
        containerId = containerJson['Id']
        if containerJson['HostConfig'].has_key('Links') and containerJson['HostConfig']['Links']:
            self.containerLinks[containerId] = []
            for link in containerJson['HostConfig']['Links']:
                linkedContainer = link.split(':')[0].split('/')[1]
                linkedContainerJson = self.client.dockerInspectContainer(linkedContainer)
                if linkedContainerJson:
                    linkedContainerId = linkedContainerJson['Id']
                    self.containerLinks[containerId].append(linkedContainerId)
                else:
                    logger.warn(('Failed in getting linked container <%s>.' % linkedContainer))

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
                if containerJson['VolumesRW'][dst] == 'true':
                    dockerVolume.accessType = 'RW'
                else:
                    dockerVolume.accessType = 'R'
                Volumes.append(dockerVolume)
                logger.debug(dockerVolume.volumeSrc + '->' + dockerVolume.volumeDst)
        dockerContainer.usedVolumeObjs = Volumes
        return Volumes

    def _discoverProcesses(self, containerId):
        topJson = self.client.dockerTop(containerId)
        processList = []
        if topJson:
            for processInfo in topJson['Processes']:
                owner = processInfo[0]
                pid = processInfo[1]
                commandLine = processInfo[7]
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

    def _processImageInfo(self, imagesJson, imageNodeObj, imageDict):
        Images = []
        repoTags = imagesJson['RepoTags']
        imageInspectJson = self.client.dockerInspectImage(imagesJson['Id'])
        for repoTag in repoTags:
            if repoTag.count(':') == 1:
                imageName = repoTag.split(':')[0]
                imageTag = repoTag.split(':')[1]
            else:
                imageName = repoTag.split(':')[0] + ':' + repoTag.split(':')[1]
                imageTag = repoTag.split(':')[2]

            imageObj = self._recordImageInfo(imageInspectJson, imageNodeObj, imageDict, imageName, imageTag)
            Images.append(imageObj)
        return Images

    def _recordImageInfo(self, imageInspectJson, imageNodeObj, imageDict, imageName, imageTag):
        virtualSize = str(imageInspectJson['VirtualSize'])
        imageId = imageInspectJson['Id']
        if imageId.find('sha256:') != -1:
            imageId = imageId.split('sha256:')[1].strip()

        if imageName == "\u003cnone\u003e" or imageName == "<none>":
            imageName = 'Docker Image'

        dockerImageTemplate = docker.DockerImageTemplate()
        dockerImageTemplate.setName(imageId)
        dockerImageTemplate.imageId = imageId

        dockerImage = docker.DockerImage()
        dockerImage.setName(imageName)
        dockerImage.imageId = imageId
        dockerImage.imageTag = imageTag
        dockerImage.virtualSize = virtualSize
        dockerImage.imageNodeObj = imageNodeObj
        dockerImage.imageTemplateObj = dockerImageTemplate

        if imageInspectJson['Config'].has_key('Entrypoint') and imageInspectJson['Config']['Entrypoint']:
            entryPoints = imageInspectJson['Config']['Entrypoint']
            entryPoints.sort()
            entryPointList = []
            for entryPoint in entryPoints:
                entryPointList.append(entryPoint)
            dockerImage.entryPointList = entryPointList
        if imageInspectJson['Config'].has_key('Volumes') and imageInspectJson['Config']['Volumes']:
            volumes = imageInspectJson['Config']['Volumes']
            volumeKeys = volumes.keys()
            volumeKeys.sort()
            volumeList = []
            for volume in volumeKeys:
                volumeList.append(volume)
            dockerImage.volumeList = volumeList
        if imageInspectJson['Config'].has_key('Cmd') and imageInspectJson['Config']['Cmd']:
            cmds = imageInspectJson['Config']['Cmd']
            cmdList = []
            for cmd in cmds:
                cmdList.append(cmd)
            dockerImage.cmdList = cmdList
        if imageInspectJson['Config'].has_key('Labels') and imageInspectJson['Config']['Labels']:
            labels = imageInspectJson['Config']['Labels']
            labelKeys = labels.keys()
            labelKeys.sort()
            labelList = []
            for label in labelKeys:
                labelStr = label + '=' + labels[label]
                labelList.append(labelStr)
            dockerImage.labelList = labelList
        if imageInspectJson['Config'].has_key('ExposedPorts') and imageInspectJson['Config']['ExposedPorts']:
            exposedPorts = imageInspectJson['Config']['ExposedPorts']
            portKeys = exposedPorts.keys()
            portKeys.sort()
            portlList = []
            for port in portKeys:
                portlList.append(port)
            dockerImage.portlList = portlList

        if not imageDict.has_key(imageId):
            imageDict[imageId] ={}
            imageDict[imageId][imageName] ={}
            imageDict[imageId][imageName][imageTag] = dockerImage
        else:
            if not imageDict[imageId].has_key(imageName):
                imageDict[imageId][imageName] ={}
                imageDict[imageId][imageName][imageTag] = dockerImage
            else:
                if not imageDict[imageId][imageName].has_key(imageTag):
                    imageDict[imageId][imageName][imageTag] = dockerImage
        return dockerImage



