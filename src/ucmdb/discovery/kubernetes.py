#coding=utf-8
import entity
import logger
import modeling
import netutils
import wwn
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector


class HasId:
    def __init__(self, id):
        self.__id = None
        if id is not None:
            self.setId(id)

    def setId(self, id):
        if not (id and id.strip()):
            raise ValueError("id is empty")
        self.__id = id
        return self

    def getId(self):
        return self.__id


class HasRepr(entity.HasName, entity.HasOsh, HasId):
    def __init__(self, name):
        entity.HasName.__init__(self, name)
        entity.HasOsh.__init__(self)
        HasId.__init__(self, id=None)

    def __repr__(self):
        return "%s (name: %s, id: %s)" % (self.__class__.__name__, self.getName(), self.getId())


class Cluster(HasRepr):
    def __init__(self, id, name):
        HasRepr.__init__(self, name)
        self.setId(id)

    def acceptVisitor(self, visitor):
        return visitor.visitCluster(self)


class Node(HasRepr):
    def __init__(self, id, name, ip):
        HasRepr.__init__(self, name)
        self.setId(id)
        self.ip = ip
        self.Images = []
        self.Cluster = None
        self.__config = None
        self.cpuCapacity = None
        self.memoryCapacity = None
        self.podsCapacity = None
        self.cpuAllocatable = None
        self.memoryAllocatable = None
        self.podsAllocatable = None
        self.nodeOS = None

    def setNodeConfigOsh(self, cfg):
        self.__config = cfg

    def getNodeConfigOsh(self):
        return self.__config

    def acceptVisitor(self, visitor):
        return visitor.visitNode(self)


class Namespace(HasRepr):
    def __init__(self, id, name):
        HasRepr.__init__(self, name)
        self.setId(id)
        self.Cluster = None

    def acceptVisitor(self, visitor):
        return visitor.visitNamespace(self)


class Service(HasRepr):
    def __init__(self, id, name):
        HasRepr.__init__(self, name)
        self.setId(id)
        self.Namespace = None
        self.Selector = None
        self.clusterIp = None
        self.type = None
        self.Pods = []

    def acceptVisitor(self, visitor):
        return visitor.visitService(self)


class Pod(HasRepr):
    def __init__(self, id, name):
        HasRepr.__init__(self, name)
        self.setId(id)
        self.Volumes = {}
        self.Containers = {}
        self.Controller = None
        self.Namespace = None
        self.Labels = None
        self.annotations = None

    def acceptVisitor(self, visitor):
        return visitor.visitPod(self)


class ResourceQuota(HasRepr):
    def __init__(self, id, name):
        HasRepr.__init__(self, name)
        self.setId(id)
        self.name = name
        self.Namespace = None
        self.hardCpu = None
        self.hardMemory = None
        self.hardPods = None
        self.usedCpu = None
        self.usedMemory = None
        self.usedPods = None

    def acceptVisitor(self, visitor):
        return visitor.visitResourcequota(self)


class Controller(HasRepr):
    def __init__(self, id, name):
        HasRepr.__init__(self, name)
        self.setId(id)
        self.ContainerConfigs = []
        self.Controller = None
        self.Namespace = None
        self.replicas = None
        self.type = None

    def acceptVisitor(self, visitor):
        return visitor.visitController(self)

    def __repr__(self):
        return "%s (name: %s, type: %s, id: %s)" % (self.__class__.__name__, self.getName(), self.type, self.getId())


class ContainerConfig(HasRepr):
    def __init__(self, name, controller):
        HasRepr.__init__(self, name)
        self.Controller = controller
        self.env = []
        self.commands = []
        self.args = []
        self.ports = []

    def acceptVisitor(self, visitor):
        return visitor.visitContainerConfig(self)


class PersistentVolume(HasRepr):
    def __init__(self, id, name):
        HasRepr.__init__(self, name)
        self.setId(id)
        self.Cluster = None
        self.StorageClass = None
        self.nfs_server = None
        self.nfs_path = None
        self.fc_wwns = None
        self.iscsi_iqn = None
        self.iscsi_target = None
        self.aws_ebs = None

    def acceptVisitor(self, visitor):
        return visitor.visitPersistentVolume(self)


class PersistentVolumeClaim(HasRepr):
    def __init__(self, id, name):
        HasRepr.__init__(self, name)
        self.setId(id)
        self.Namespace = None
        self.PersistentVolume = None

    def acceptVisitor(self, visitor):
        return visitor.visitPersistentVolumeClaim(self)


class StorageClass(HasRepr):
    def __init__(self, id, name, provisioner, reclaimPolicy):
        HasRepr.__init__(self, name)
        self.setId(id)
        self.provisioner = provisioner
        self.reclaimPolicy = reclaimPolicy
        self.Cluster = None

    def acceptVisitor(self, visitor):
        return visitor.visitStorageClass(self)


class Docker(HasRepr):
    def __init__(self):
        HasRepr.__init__(self, 'Docker')

    def acceptVisitor(self, visitor):
        return visitor.visitDocker(self)


class DockerDaemon(HasRepr):
    def __init__(self, node, docker, version):
        HasRepr.__init__(self, 'Docker Daemon')
        self.Node = node
        self.Docker = docker
        self.ipServiceEndpoint = []

        self.version = version
        self.discovered_product_name = 'Docker Daemon'
        self.hostName = node.getName()
        self.loggingDriver = None
        self.labelList = []

    def acceptVisitor(self, visitor):
        return visitor.visitDockerDaemon(self)


class DockerVolume(HasRepr):
    def __init__(self, name):
        HasRepr.__init__(self, name)
        self.Node = None
        self.LogicalVolume = None
        self.Source = None

        self.volumeSrc = None
        self.volumeDst = None
        self.accessType = None

    def acceptVisitor(self, visitor):
        return visitor.visitDockerVolume(self)


class DockerImage(HasRepr):
    def __init__(self, id, name, node, imageTemplate, tag='latest'):
        HasRepr.__init__(self, name)
        self.setId(id)
        self.Node = node
        self.ImageTemplate = imageTemplate

        self.tag = tag
        self.virtualSize = None
        self.entryPointList = []
        self.volumeList = []
        self.cmdList = []
        self.labelList = []
        self.portlList = []

    def acceptVisitor(self, visitor):
        return visitor.visitDockerImage(self)

    def __repr__(self):
        imageName = self.getName() + ':' + self.tag
        return "%s (name = %s)" % (self.__class__.__name__, imageName)


class DockerImageTemplate(HasRepr):
    def __init__(self, id):
        HasRepr.__init__(self, id)
        self.setId(id)

    def acceptVisitor(self, visitor):
        return visitor.visitDockerImageTemplate(self)

    def __repr__(self):
        return "%s (name = %s)" % (self.__class__.__name__, self.getName())


class DockerContainer(HasRepr):
    def __init__(self, id, name, pod, image):
        HasRepr.__init__(self, name)
        self.setId(id)
        self.Daemon = pod.Node.Daemon
        self.Node = pod.Node
        self.Pod = pod
        self.Image = image
        self.Volumes = []
        self.processList = []

        self.imageId = image.getId()
        self.imageName = image.getName()

        self.containerPorts = ''
        self.restartPolicy = ''
        self.restartMaxCount = 0
        self.loggingDriver = ''
        self.memoryLimitInMB = 0
        self.cmdList = []
        self.labelList = []

    def acceptVisitor(self, visitor):
        return visitor.visitDockerContainer(self)


class LogicalVolume(HasRepr):
    def __init__(self, name):
        HasRepr.__init__(self, name)

    def acceptVisitor(self, visitor):
        return visitor.visitLogicalVolume(self)


class VolumeTypes:
    HostPath = 'HostPath'
    PersistentVolumeClaim = 'PersistentVolumeClaim'


class Builder:
    def visitCluster(self, cluster):
        osh = ObjectStateHolder('k8s_cluster')
        osh.setStringAttribute('name', cluster.getName())
        osh.setStringAttribute('uid', cluster.getId())
        return osh

    def visitNamespace(self, namespace):
        osh = ObjectStateHolder('k8s_namespace')
        osh.setStringAttribute('name', namespace.getName())
        osh.setStringAttribute('uid', namespace.getId())
        return osh

    def visitNode(self, node):
        def buildNodeConfig(node):
            osh = ObjectStateHolder('k8s_node_config')
            osh.setStringAttribute('name', node.getName())
            if node.cpuCapacity:
                osh.setIntegerAttribute('cpu_capacity', int(node.cpuCapacity))
            if node.memoryCapacity:
                osh.setIntegerAttribute('memory_capacity', node.memoryCapacity)
            if node.podsCapacity:
                osh.setIntegerAttribute('pod_capacity', int(node.podsCapacity))
            if node.cpuAllocatable:
                osh.setIntegerAttribute('cpu_available', int(node.cpuAllocatable))
            if node.memoryAllocatable:
                osh.setIntegerAttribute('memory_available', node.memoryAllocatable)
            if node.podsAllocatable:
                osh.setIntegerAttribute('pod_available', int(node.podsAllocatable))
            osh.setBoolAttribute('out_of_disk', node.outOfDisk)
            osh.setBoolAttribute('ready', node.ready)
            node.setNodeConfigOsh(osh)

        if node.ip:
            # kubernetes support linux os only
            CIType = 'unix'
            # in case expansion for windows platform
            if node.nodeOS == 'windows':
               CIType = 'nt'
            osh = modeling.createHostOSH(node.ip,hostClassName= CIType)
        else:
            osh = ObjectStateHolder('node')
        osh.setStringAttribute("name", node.getName())
        buildNodeConfig(node)
        return osh

    def visitPersistentVolume(self, pv):
        osh = ObjectStateHolder('k8s_persistent_volume')
        osh.setStringAttribute('name', pv.getName())
        osh.setStringAttribute('uid', pv.getId())
        osh.setStringAttribute('status', pv.status)
        osh.setListAttribute('access_modes', pv.accessModes)
        osh.setStringAttribute('reclaim_policy', pv.reclaimPolicy)
        if pv.capacity:
            osh.setIntegerAttribute('capacity', pv.capacity)
        return osh

    def visitPersistentVolumeClaim(self, pvc):
        osh = ObjectStateHolder('k8s_persistent_volume_claim')
        osh.setStringAttribute('name', pvc.getName())
        osh.setStringAttribute('uid', pvc.getId())
        osh.setStringAttribute('status', pvc.status)
        osh.setListAttribute('access_modes', pvc.accessModes)
        return osh

    def visitStorageClass(self, storageClass):
        osh = ObjectStateHolder('k8s_storage_class')
        osh.setStringAttribute('name', storageClass.getName())
        osh.setStringAttribute('uid', storageClass.getId())
        osh.setStringAttribute('provisioner', storageClass.provisioner)
        osh.setStringAttribute('reclaim_policy', storageClass.reclaimPolicy)
        return osh

    def visitResourcequota(self, resourcequota):
        osh = ObjectStateHolder('k8s_resource_quota')
        osh.setStringAttribute('name', resourcequota.getName())
        osh.setStringAttribute('uid', resourcequota.getId())
        if resourcequota.hardCpu:
            osh.setFloatAttribute('hard_cpu', resourcequota.hardCpu)
        if resourcequota.hardMemory:
            osh.setIntegerAttribute('hard_memory', resourcequota.hardMemory)
        if resourcequota.hardPods:
            osh.setIntegerAttribute('hard_pods', resourcequota.hardPods)
        if resourcequota.usedCpu:
            osh.setFloatAttribute('used_cpu', resourcequota.usedCpu)
        if resourcequota.usedMemory:
            osh.setIntegerAttribute('used_memory', resourcequota.usedMemory)
        if resourcequota.usedPods:
            osh.setIntegerAttribute('used_pods', resourcequota.usedPods)
        return osh

    def visitController(self, controller):
        osh = ObjectStateHolder('k8s_controller')
        osh.setStringAttribute('name', controller.getName())
        osh.setStringAttribute('uid', controller.getId())
        osh.setStringAttribute('type', controller.type)
        if controller.replicas:
            osh.setIntegerAttribute('desired_replicas', controller.replicas)
        return osh

    def visitContainerConfig(self, config):
        osh = ObjectStateHolder('k8s_container_config')
        osh.setStringAttribute('name', config.getName())
        osh.setStringAttribute('image', config.image)
        if config.args:
            osh.setListAttribute('args', config.args)
        return osh

    def visitPod(self, pod):
        osh = ObjectStateHolder('k8s_pod')
        osh.setStringAttribute('name', pod.getName())
        osh.setStringAttribute('uid', pod.getId())
        osh.setStringAttribute('status', pod.status)
        return osh

    def visitService(self, service):
        osh = ObjectStateHolder('k8s_service')
        osh.setStringAttribute('name', service.getName())
        osh.setStringAttribute('uid', service.getId())
        if service.clusterIp:
            osh.setStringAttribute('cluster_ip', service.clusterIp)
        if service.type:
            osh.setStringAttribute('type', service.type)
        return osh

    def visitDocker(self, docker):
        osh = ObjectStateHolder('docker')
        osh.setStringAttribute('name', docker.getName())
        return osh

    def visitDockerDaemon(self, daemon):
        osh = ObjectStateHolder('docker_daemon')
        osh.setStringAttribute('name', daemon.getName())
        if daemon.version:
            osh.setStringAttribute('version', daemon.version)
        osh.setStringAttribute('discovered_product_name', daemon.discovered_product_name)
        osh.setStringAttribute('docker_host', daemon.hostName)
        return osh

    def visitDockerContainer(self, container):
        osh = ObjectStateHolder('docker_container')
        osh.setStringAttribute('name', container.getName())
        osh.setStringAttribute('docker_container_id', container.getId())
        osh.setStringAttribute('docker_image_id', container.imageId)
        osh.setStringAttribute('docker_image', container.imageName)
        return osh

    def visitDockerImage(self, image):
        osh = ObjectStateHolder('docker_image')
        osh.setStringAttribute('name', image.getName())
        osh.setStringAttribute('docker_image_id', image.getId())
        osh.setStringAttribute('repository', image.getName())
        osh.setStringAttribute('tag', image.tag)
        return osh

    def visitDockerImageTemplate(self, imageTemplate):
        osh = ObjectStateHolder('docker_image_template')
        osh.setStringAttribute('docker_image_id', imageTemplate.getId())
        osh.setStringAttribute('name', imageTemplate.getName())
        return osh

    def visitDockerVolume(self, volume):
        osh = ObjectStateHolder('docker_volume')
        osh.setStringAttribute('name', volume.getName())
        osh.setStringAttribute('dockervolume_source', volume.volumeSrc)
        osh.setStringAttribute('dockervolume_destination', volume.volumeDst)
        return osh


class Reporter:
    def __init__(self, builder):
        self.__builder = builder

    def reportCluster(self, cluster):
        if not cluster:
            raise ValueError("Cluster is not specified")
        clusterOSH = cluster.build(self.__builder)
        return clusterOSH

    def reportNamespace(self, namespace):
        if not namespace:
            raise ValueError("Namespace is not specified")
        if not (namespace.Cluster and namespace.Cluster.getOsh()):
            raise ValueError("Cluster is not specified or not built for %s" % namespace)
        vector = ObjectStateHolderVector()
        namespaceOSH = namespace.build(self.__builder)
        vector.add(namespaceOSH)
        link = modeling.createLinkOSH('membership', namespace.Cluster.getOsh(), namespaceOSH)
        vector.add(link)
        return vector

    def reportNode(self, node):
        if not node:
            raise ValueError("Node is not specified")
        if not (node.Cluster and node.Cluster.getOsh()):
            raise ValueError("Cluster is not specified or not built for %s" % node)
        vector = ObjectStateHolderVector()
        nodeOSH = node.build(self.__builder)
        vector.add(nodeOSH)
        vector.add(modeling.createLinkOSH('membership', node.Cluster.getOsh(), nodeOSH))
        if not node.getNodeConfigOsh():
            raise ValueError("Node Config is not specified")
        nodeConfigOSH = node.getNodeConfigOsh()
        nodeConfigOSH.setContainer(nodeOSH)
        vector.add(nodeConfigOSH)
        dockerDaemonOSH = node.Daemon.build(self.__builder)
        dockerDaemonOSH.setContainer(nodeOSH)
        vector.add(dockerDaemonOSH)
        dockerOSH = node.Daemon.Docker.build(self.__builder)
        dockerOSH.setContainer(nodeOSH)
        vector.add(dockerOSH)
        vector.add(modeling.createLinkOSH('membership', dockerOSH, dockerDaemonOSH))
        if node.ip:
            ipOSH = modeling.createIpOSH(node.ip)
            vector.add(ipOSH)
            vector.add(modeling.createLinkOSH('containment', nodeOSH, ipOSH))
        for image in node.Images:
            if not (image.ImageTemplate and image.ImageTemplate.getOsh()):
                logger.debug("Image Template is not specified or not built for %s" % image)
            imageOSH = image.build(self.__builder)
            imageOSH.setContainer(nodeOSH)
            vector.add(imageOSH)
            vector.add(modeling.createLinkOSH('resource', image.ImageTemplate.getOsh(), imageOSH))
        return vector

    def reportPersistentVolume(self, pv):
        if not pv:
            raise ValueError("Persistent Volume is not specified")
        if not (pv.Cluster and pv.Cluster.getOsh()):
            raise ValueError("Cluster is not specified or not built for %s" % pv)
        vector = ObjectStateHolderVector()
        persistentVolumeOSH = pv.build(self.__builder)
        vector.add(persistentVolumeOSH)
        vector.add(modeling.createLinkOSH('membership', pv.Cluster.getOsh(), persistentVolumeOSH))
        if pv.StorageClass and pv.StorageClass.getOsh():
            vector.add(modeling.createLinkOSH('usage', persistentVolumeOSH, pv.StorageClass.getOsh()))
        if pv.nfs_server and pv.nfs_path:
            if netutils.isValidIp(pv.nfs_server):
                nodeOSH = modeling.createHostOSH(pv.nfs_server)
            else:
                nodeOSH = ObjectStateHolder('node')
                nodeOSH.setStringAttribute('name', pv.nfs_server)
            file_export_osh = ObjectStateHolder('file_system_export')
            file_export_osh.setStringAttribute('file_system_path',  pv.nfs_path)
            file_export_osh.setContainer(nodeOSH)
            vector.add(nodeOSH)
            vector.add(file_export_osh)
            vector.add(modeling.createLinkOSH('dependency', persistentVolumeOSH, file_export_osh))
        if pv.fc_wwns:
            for fc_wwn in pv.fc_wwns:
                formated_wwn = wwn.parse_from_str(fc_wwn)
                fcp_osh = ObjectStateHolder('fcport')
                fcp_osh.setStringAttribute('fcport_wwn', str(formated_wwn))
                vector.add(fcp_osh)
                vector.add(modeling.createLinkOSH('dependency', persistentVolumeOSH, fcp_osh))

        if pv.iscsi_iqn:
            iscsi_adapter_osh = ObjectStateHolder('iscsi_adapter')
            iscsi_adapter_osh.setStringAttribute("iqn",pv.iscsi_iqn)
            vector.add(iscsi_adapter_osh)
            vector.add(modeling.createLinkOSH('dependency', persistentVolumeOSH, iscsi_adapter_osh))
            if pv.iscsi_target:
                if netutils.isValidIp(pv.iscsi_target):
                    target_host_osh = modeling.createHostOSH(pv.iscsi_target)
                else:
                    target_host_osh = ObjectStateHolder('node')
                    target_host_osh.setStringAttribute('name', pv.iscsi_target)
                vector.add(target_host_osh)
                vector.add(modeling.createLinkOSH('composition', target_host_osh, iscsi_adapter_osh))

        if pv.aws_ebs:
            ebs_osh = ObjectStateHolder('amazon_ebs')
            ebs_osh.setAttribute('volume_id', pv.aws_ebs)
            vector.add(ebs_osh)
            vector.add(modeling.createLinkOSH('dependency', persistentVolumeOSH, ebs_osh))

        return vector

    def reportStorageClass(self, sc):
        if not sc:
            raise ValueError("Storage Class is not specified")
        if not (sc.Cluster and sc.Cluster.getOsh()):
            raise ValueError("Cluster is not specified or not built for %s" % sc)
        vector = ObjectStateHolderVector()
        storageClassOSH = sc.build(self.__builder)
        vector.add(storageClassOSH)
        vector.add(modeling.createLinkOSH('membership', sc.Cluster.getOsh(), storageClassOSH))
        return vector

    def reportPersistentVolumeClaim(self, pvc):
        if not pvc:
            raise ValueError("Persistent Volume Claim is not specified")
        if not (pvc.Namespace and pvc.Namespace.getOsh()):
            raise ValueError("Namespace is not specified or not built for %s" % pvc)
        vector = ObjectStateHolderVector()
        persistentVolumeClaimOSH = pvc.build(self.__builder)
        vector.add(persistentVolumeClaimOSH)
        vector.add(modeling.createLinkOSH('membership', pvc.Namespace.getOsh(), persistentVolumeClaimOSH))
        if pvc.PersistentVolume and pvc.PersistentVolume.getOsh():
            vector.add(modeling.createLinkOSH('usage', persistentVolumeClaimOSH, pvc.PersistentVolume.getOsh()))
        return vector

    def reportController(self, controller):
        if not controller:
            raise ValueError("Controller is not specified")
        if not (controller.Namespace and controller.Namespace.getOsh()):
            raise ValueError("Namespace is not specified or not built for %s" % controller)
        vector = ObjectStateHolderVector()
        controllerOSH = controller.build(self.__builder)
        vector.add(controllerOSH)
        vector.add(modeling.createLinkOSH('membership', controller.Namespace.getOsh(), controllerOSH))
        for containerConfig in controller.ContainerConfigs:
            configOSH = containerConfig.build(self.__builder)
            configOSH.setContainer(controllerOSH)
            vector.add(configOSH)
        return vector

    def linkControllers(self, controller):
        if controller.Controller:
            vector = ObjectStateHolderVector()
            vector.add(modeling.createLinkOSH('manage', controller.Controller.getOsh(), controller.getOsh()))
            return vector


    def reportPod(self, pod):
        if not pod:
            raise ValueError("Pod is not specified")
        if not (pod.Namespace and pod.Namespace.getOsh()):
            raise ValueError("Namespace is not specified or not built for %s" % pod)
        vector = ObjectStateHolderVector()
        podOSH = pod.build(self.__builder)
        vector.add(podOSH)
        vector.add(modeling.createLinkOSH('membership', pod.Namespace.getOsh(), podOSH))
        if pod.Controller:
            vector.add(modeling.createLinkOSH('manage', pod.Controller.getOsh(), podOSH))
        return vector

    def reportDockerContainer(self, container):
        if not container:
            raise ValueError("Container is not specified")
        if not (container.Node and container.Node.getOsh()):
            raise ValueError("Node is not specified or not built for %s" % container)
        if not (container.Daemon and container.Daemon.getOsh()):
            raise ValueError("Docker Daemon is not specified or not built for %s" % container)
        if not (container.Pod and container.Pod.getOsh()):
            raise ValueError("Pod is not specified or not built for %s" % container)
        if not (container.Image and container.Image.getOsh()):
            raise ValueError("Image is not specified or not built for %s" % container)
        vector = ObjectStateHolderVector()
        containerOSH = container.build(self.__builder)
        containerOSH.setContainer(container.Node.getOsh())
        vector.add(containerOSH)
        vector.add(modeling.createLinkOSH('membership', container.Pod.getOsh(), containerOSH))
        vector.add(modeling.createLinkOSH('manage', container.Daemon.getOsh(), containerOSH))
        vector.add(modeling.createLinkOSH('realization', container.Image.getOsh(), containerOSH))
        for volume in container.Volumes:
            volumeOSH = volume.build(self.__builder)
            volumeOSH.setContainer(container.Node.getOsh())
            vector.add(volumeOSH)
            vector.add(modeling.createLinkOSH('usage', containerOSH, volumeOSH))
            if volume.Source and volume.Source.getOsh():
                if volume.type == VolumeTypes.PersistentVolumeClaim:
                    vector.add(modeling.createLinkOSH('usage', volumeOSH, volume.Source.getOsh()))
                else:
                    logger.debug('Unrecognized source type (%s) for %s' % (volume.type, volume))
        return vector

    def reportImageTemplate(self, imageTemplate):
        if not imageTemplate:
            raise ValueError("ImageTemplate is not specified")
        vector = ObjectStateHolderVector()
        imageTemplateOSH = imageTemplate.build(self.__builder)
        vector.add(imageTemplateOSH)
        return vector

    def reportService(self, service):
        if not service:
            raise ValueError("Service is not specified")
        if not (service.Namespace and service.Namespace.getOsh()):
            raise ValueError("Namespace is not specified or not built for %s" % service)
        vector = ObjectStateHolderVector()
        serviceOSH = service.build(self.__builder)
        vector.add(serviceOSH)
        vector.add(modeling.createLinkOSH('membership', service.Namespace.getOsh(), serviceOSH))
        if service.Pods:
            for pod in service.Pods:
                vector.add(modeling.createLinkOSH('dependency', serviceOSH, pod.getOsh()))
        return vector

    def reportResourcequota(self, resourcequota):
        if not resourcequota:
            raise ValueError("Resourcequota is not specified")
        vector = ObjectStateHolderVector()
        resourcequotaOSH = resourcequota.build(self.__builder)
        vector.add(resourcequotaOSH)
        vector.add(modeling.createLinkOSH('membership', resourcequota.Namespace.getOsh(), resourcequotaOSH))
        return vector