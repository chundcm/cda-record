#coding=utf-8
import logger
import modeling
import netutils
import applications

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector


class DockerReporter:
    def __init__(self, client, Framework, dockerDaemon, dockerImages, dockerContainers, dockerVolumes):
        self.Images = dockerImages
        self.Containers = dockerContainers
        self.Volumes = dockerVolumes
        self.dockerDaemon = dockerDaemon
        self.Framework = Framework
        self.client = client


    def reportDocker(self, vector, dockerDaemon):
        # Node
        if not dockerDaemon.dockerNodeObj.getOsh() and dockerDaemon.dockerNodeObj.ip:
            nodeOSH = modeling.createHostOSH(dockerDaemon.dockerNodeObj.ip)
            nodeOSH.setStringAttribute('name', dockerDaemon.dockerNodeObj.getName())
            dockerDaemon.dockerNodeObj.setOsh(nodeOSH)
        else:
            nodeOSH = dockerDaemon.dockerNodeObj.getOsh()
        # Docker Daemon
        if not dockerDaemon.getOsh():
            dockerDaemonOSH = ObjectStateHolder('docker_daemon')
            dockerDaemonOSH.setStringAttribute('name', dockerDaemon.getName())
            dockerDaemonOSH.setStringAttribute('version', dockerDaemon.version)
            dockerDaemonOSH.setStringAttribute('discovered_product_name', dockerDaemon.discovered_product_name)
            dockerDaemonOSH.setStringAttribute('docker_host', dockerDaemon.hostName)
            dockerDaemonOSH.setStringAttribute('docker_logging_driver', dockerDaemon.loggingDriver)
            dockerDaemonOSH.setListAttribute('docker_labels', dockerDaemon.labelList)
            dockerDaemon.setOsh(dockerDaemonOSH)
        else:
            dockerDaemonOSH = dockerDaemon.getOsh()
            dockerDaemonOSH.setStringAttribute('version', dockerDaemon.version)
            dockerDaemonOSH.setStringAttribute('discovered_product_name', dockerDaemon.discovered_product_name)
            dockerDaemonOSH.setStringAttribute('docker_host', dockerDaemon.hostName)
            dockerDaemonOSH.setStringAttribute('docker_logging_driver', dockerDaemon.loggingDriver)
            dockerDaemonOSH.setListAttribute('docker_labels', dockerDaemon.labelList)
        # Docker
        dockerOSH = ObjectStateHolder('docker')
        dockerOSH.setStringAttribute('name', dockerDaemon.dockerObj.getName())
        dockerDaemon.dockerObj.setOsh(dockerOSH)
        vector.add(nodeOSH)
        vector.add(dockerOSH)
        vector.add(dockerDaemonOSH)

        # Relations
        dockerDaemonOSH.setContainer(nodeOSH)
        dockerOSH.setContainer(nodeOSH)
        dockerDaemonLink = modeling.createLinkOSH('membership', dockerOSH, dockerDaemonOSH)
        vector.add(dockerDaemonLink)


    def reportDockerImage(self, vector, Images):
        ImageTemplateDict = {}
        for imageObj in Images:
            imageOSH = ObjectStateHolder('docker_image')
            imageOSH.setStringAttribute('name', imageObj.getName())
            imageOSH.setStringAttribute('docker_image_id', imageObj.imageId)
            imageOSH.setStringAttribute('repository', imageObj.getName())
            imageOSH.setStringAttribute('tag', imageObj.imageTag)
            imageOSH.setStringAttribute('virtual_size', imageObj.virtualSize)
            imageOSH.setListAttribute('docker_entry_points', imageObj.entryPointList)
            imageOSH.setListAttribute('docker_image_volumes', imageObj.volumeList)
            imageOSH.setListAttribute('docker_image_commands', imageObj.cmdList)
            imageOSH.setListAttribute('docker_image_labels', imageObj.labelList)
            imageOSH.setListAttribute('docker_image_ports', imageObj.portlList)
            imageObj.setOsh(imageOSH)

            imageTmeplateObj = imageObj.imageTemplateObj
            if ImageTemplateDict.has_key(imageTmeplateObj.imageId):
                imageTemplateOSH = ImageTemplateDict[imageTmeplateObj.imageId]
            else:
                imageTemplateOSH = ObjectStateHolder('docker_image_template')
                imageTemplateOSH.setStringAttribute('docker_image_id', imageTmeplateObj.imageId)
                imageTemplateOSH.setStringAttribute('name', imageTmeplateObj.getName())
                ImageTemplateDict[imageTmeplateObj.imageId] = imageTemplateOSH
                vector.add(imageTemplateOSH)

            vector.add(imageOSH)

            imageNodeOSH = imageObj.imageNodeObj.getOsh()
            imageOSH.setContainer(imageNodeOSH)

            dockerImageTemplateLink = modeling.createLinkOSH('resource', imageTemplateOSH, imageOSH )
            vector.add(dockerImageTemplateLink)


    def reportDockerVolume(self, vector, Volumes):
        for volumeObj in Volumes:
            dockerVolumeOSH = ObjectStateHolder('docker_volume')
            dockerVolumeOSH.setStringAttribute('name', volumeObj.getName())
            dockerVolumeOSH.setStringAttribute('dockervolume_source', volumeObj.volumeSrc)
            dockerVolumeOSH.setStringAttribute('dockervolume_destination', volumeObj.volumeDst)
            dockerVolumeOSH.setStringAttribute('logicalvolume_accesstype',volumeObj.accessType)
            vector.add(dockerVolumeOSH)
            volumeObj.setOsh(dockerVolumeOSH)

            if volumeObj.logicalVolumeObj:
                logicalVolumeOSH = ObjectStateHolder('logical_volume')
                logicalVolumeOSH.setStringAttribute('name', volumeObj.logicalVolumeObj.getName())
                logicalVolumeOSH.setContainer(volumeObj.volumeNodeObj.getOsh())
                vector.add(logicalVolumeOSH)

                lvDockerVolumeLink = modeling.createLinkOSH('dependency', dockerVolumeOSH, logicalVolumeOSH)
                vector.add(lvDockerVolumeLink)

            dockerVolumeOSH.setContainer(volumeObj.volumeNodeObj.getOsh())


    def reportDockerContaienr(self, vector, Contaienrs):
        for containerObj in Contaienrs:
            containerOSH = ObjectStateHolder('docker_container')
            containerOSH.setStringAttribute('name', containerObj.getName())
            containerOSH.setStringAttribute('docker_container_id', containerObj.containerId)
            containerOSH.setStringAttribute('docker_image_id', containerObj.imageId)
            containerOSH.setStringAttribute('docker_image', containerObj.imageName)
            containerOSH.setStringAttribute('docker_container_ports', containerObj.containerPorts)
            containerOSH.setStringAttribute('docker_container_restart_policy', containerObj.restartPolicy)
            containerOSH.setIntegerAttribute('docker_container_restart_max_count', containerObj.restartMaxCount)
            containerOSH.setStringAttribute('docker_container_logging_driver', containerObj.loggingDriver)
            containerOSH.setIntegerAttribute('docker_container_memory_limit', containerObj.memoryLimitInMB)
            containerOSH.setListAttribute('docker_container_commands', containerObj.cmdList)
            containerOSH.setListAttribute('docker_container_labels', containerObj.labelList)
            containerOSH.setContainer(containerObj.daemonObj.dockerNodeObj.getOsh())
            containerObj.setOsh(containerOSH)

            vector.add(containerOSH)

            # Report running software in Docker Container
            if containerObj.processList:
                appSign = applications.createApplicationSignature(self.Framework, None)
                appSign.setProcessesManager(applications.ProcessesManager(containerObj.processList, None))
                appSign.getApplicationsTopology(containerOSH)

            if containerObj.imageObj and containerObj.imageObj.getOsh():
                imageContainerLink = modeling.createLinkOSH('realization', containerObj.imageObj.getOsh(), containerOSH)
                vector.add(imageContainerLink)
            daemonContainerLink = modeling.createLinkOSH('manage', containerObj.daemonObj.getOsh(), containerOSH)
            vector.add(daemonContainerLink)

            # Docker Container <-> Docker Volume Link
            for usedVolumeObj in containerObj.usedVolumeObjs:
                volumeContainerLink = modeling.createLinkOSH('usage', containerOSH, usedVolumeObj.getOsh())
                vector.add(volumeContainerLink)

        # Docker Container <-> Docker Contaienr Link
        for containerObj in Contaienrs:
            for linkedContainerObj in containerObj.linkToContainerObjs:
                containersLink = modeling.createLinkOSH('usage', containerObj.getOsh(), linkedContainerObj.getOsh())
                vector.add(containersLink)



    def reportDockerTopology(self):
        vector = ObjectStateHolderVector()
        self.reportDocker(vector, self.dockerDaemon)
        self.reportDockerImage(vector, self.Images)
        self.reportDockerVolume(vector, self.Volumes)
        self.reportDockerContaienr(vector, self.Containers)

        return vector
