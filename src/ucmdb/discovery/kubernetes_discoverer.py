# coding=utf-8
import re
import logger
import kubernetes
from kubernetes import VolumeTypes
from appilog.common.system.types.vectors import ObjectStateHolderVector
from java.lang import Boolean


class K8sDiscoverer:
    def __init__(self, client, endpoint, ip, Framework):
        if not client:
            raise ValueError('No client passed.')
        self.client = client
        self.Cluster = None
        self.NodesByName = {}
        self.NamespacesByName = {}
        self.ServicesById = {}
        self.ServicesByNameSpaceAndName = {}
        self.ControllersById = {}
        self.ImageTemplatesById = {}
        self.ContainersById = {}
        self.PodsById = {}
        self.PersistentVolumesByName = {}
        self.StorageClassesByName = {}
        self.PersistentVolumeClaimsByName = {}
        self.ResourcequotasById = {}

    def safeReport(self, item, fn):
        try:
            return fn(item)
        except:
            logger.debugException('Failed to report %s.' % item)

    def report(self):
        vector = ObjectStateHolderVector()
        reporter = kubernetes.Reporter(kubernetes.Builder())
        vector.add(self.safeReport(self.Cluster, reporter.reportCluster))
        for item in self.ImageTemplatesById:
            object = self.ImageTemplatesById[item]
            vector.addAll(self.safeReport(object, reporter.reportImageTemplate))
        for item in self.NodesByName:
            object = self.NodesByName[item]
            vector.addAll(self.safeReport(object, reporter.reportNode))
        for item in self.NamespacesByName:
            object = self.NamespacesByName[item]
            vector.addAll(self.safeReport(object, reporter.reportNamespace))
        for item in self.StorageClassesByName:
            object = self.StorageClassesByName[item]
            vector.addAll(self.safeReport(object, reporter.reportStorageClass))
        for item in self.PersistentVolumesByName:
            object = self.PersistentVolumesByName[item]
            vector.addAll(self.safeReport(object, reporter.reportPersistentVolume))
        for item in self.PersistentVolumeClaimsByName:
            for pvc in self.PersistentVolumeClaimsByName[item]:
                object = self.PersistentVolumeClaimsByName[item][pvc]
                vector.addAll(self.safeReport(object, reporter.reportPersistentVolumeClaim))
        for item in self.ControllersById:
            object = self.ControllersById[item]
            vector.addAll(self.safeReport(object, reporter.reportController))

        for item in self.ControllersById:
            object = self.ControllersById[item]
            vector.addAll(self.safeReport(object, reporter.linkControllers))

        for item in self.PodsById:
            object = self.PodsById[item]
            vector.addAll(self.safeReport(object, reporter.reportPod))
        for item in self.ContainersById:
            object = self.ContainersById[item]
            vector.addAll(self.safeReport(object, reporter.reportDockerContainer))
        for item in self.ServicesById:
            object = self.ServicesById[item]
            vector.addAll(self.safeReport(object, reporter.reportService))
        for item in self.ResourcequotasById:
            object = self.ResourcequotasById[item]
            vector.addAll(self.safeReport(object, reporter.reportResourcequota))
        return vector

    def discover(self):
        self.discoverComponents()

    def discoverComponents(self):
        self.parseNodes(self.client.listNodes())
        self.parseNamespaces(self.client.listNamespaces())

        self.parseStorageClasses(self.client.listStorageClasses())
        self.parsePersistentVolumes(self.client.listPersistentVolumes())
        self.parsePersistentVolumeClaims(self.client.listPersistentVolumeClaims())

        self.parseJobs(self.client.listJobs())
        self.parseDeployments(self.client.listDeployments())
        self.parseDaemonSets(self.client.listDaemonSets())
        self.parseStatefulSets(self.client.listStatefulSets())
        self.parseReplicationControllers(self.client.listReplicationControllers())
        self.parseReplicaSets(self.client.listReplicaSets())
        self.parsePods(self.client.listPods())
        self.parseServices(self.client.listServices())
        self.parseResourcequotas(self.client.listResourcequotas())

    def parsePersistentVolumes(self, pvs):
        for ns, _ in self.NamespacesByName.items():
            self.PersistentVolumeClaimsByName[ns] = {}

        items = getItems(pvs)
        for item in items:
            id = item['metadata']['uid']
            name = item['metadata']['name']
            status = item['status']['phase']
            capacity = parseValue(item['spec']['capacity']['storage'])
            accessModes = item['spec']['accessModes']
            reclaimPolicy = item['spec']['persistentVolumeReclaimPolicy']

            pv = kubernetes.PersistentVolume(id, name)
            pv.Cluster = self.Cluster
            pv.status = status
            pv.capacity = capacity
            pv.accessModes = accessModes
            pv.reclaimPolicy = reclaimPolicy

            if item['spec'].get('storageClassName', None):
                storageClass = item['spec']['storageClassName']
                pv.StorageClass = self.StorageClassesByName.get(storageClass)

            if item['spec'].get('nfs', None):
                nfs_server = item['spec']['nfs']['server']
                nfs_path = item['spec']['nfs']['path']
                pv.nfs_server = nfs_server
                pv.nfs_path = nfs_path

            if item['spec'].get('fc', None):
                pv.fc_wwns = item['spec']['fc']['targetWWNs']

            if item['spec'].get('iscsi', None):
                pv.iscsi_iqn = item['spec']['iscsi']['iqn']
                pv.iscsi_target = item['spec']['iscsi']['targetPortal']

            if item['spec'].get('awsElasticBlockStore', None):
                pv.aws_ebs = item['spec']['awsElasticBlockStore']['volumeID']

            self.PersistentVolumesByName[name] = pv

    def parsePersistentVolumeClaims(self, pvcs):
        items = getItems(pvcs)
        for item in items:
            id = item['metadata']['uid']
            name = item['metadata']['name']
            namespace = item['metadata']['namespace']
            status = item['status']['phase']
            accessModes = item['spec']['accessModes']
            pvc = kubernetes.PersistentVolumeClaim(id, name)
            pvc.Namespace = self.NamespacesByName.get(namespace)
            if 'volumeName' in item['spec']:
                pvName = item['spec']['volumeName']
                pvc.PersistentVolume = self.PersistentVolumesByName.get(pvName)
            pvc.status = status
            pvc.accessModes = accessModes
            self.PersistentVolumeClaimsByName[namespace][name] = pvc

    def parseStorageClasses(self, storageClasses):
        items = getItems(storageClasses)
        for item in items:
            id = item['metadata']['uid']
            name = item['metadata']['name']
            provisioner = item['provisioner']
            reclaimPolicy = None
            if item.get('reclaimPolicy', None):
                reclaimPolicy = item['reclaimPolicy']
            storageClass = kubernetes.StorageClass(id, name, provisioner, reclaimPolicy)
            storageClass.Cluster = self.Cluster
            self.StorageClassesByName[name] = storageClass

    def parseNodes(self, nodes):
        items = getItems(nodes)
        id = None
        for item in items:
            ip = None
            id = item['metadata']['uid']
            name = item['metadata']['name']
            if name == 'kube-master':
                self.Cluster = kubernetes.Cluster(id, 'Kubernetes Cluster')

            if item['status'].get('addresses'):
                for address in item['status']['addresses']:
                    if address['type'] == 'InternalIP':
                        ip = address['address']
                    elif address['type'] == 'Hostname':
                        name = address['address']
            node = kubernetes.Node(id, name, ip)

            dockerVersion = None
            dockerRuntimeVersions = item['status']['nodeInfo']['containerRuntimeVersion'].split('docker://')
            if len(dockerRuntimeVersions) > 1:
                dockerVersion = dockerRuntimeVersions[1]

            # get Node OS
            if item['metadata'].get('labels'):
                node.nodeOS = item['metadata']['labels'].get('beta.kubernetes.io/os')

            if item['status'].get('capacity', None):
                node.cpuCapacity = item['status']['capacity']['cpu']
                memoryCapacity = item['status']['capacity']['memory']
                node.podsCapacity = item['status']['capacity']['pods']
                node.memoryCapacity = parseValue(memoryCapacity)

            if item['status'].get('allocatable', None):
                node.cpuAllocatable = item['status']['allocatable']['cpu']
                memoryAllocatable = item['status']['allocatable']['memory']
                node.podsAllocatable = item['status']['allocatable']['pods']
                node.memoryAllocatable = parseValue(memoryAllocatable)

            conditions = item['status']['conditions']
            outOfDisk = False
            ready = False
            for condition in conditions:
                if condition['type'] == 'OutOfDisk':
                    outOfDisk = Boolean.parseBoolean(condition['status'])
                elif condition['type'] == 'Ready':
                    ready = Boolean.parseBoolean(condition['status'])

            node.outOfDisk = outOfDisk
            node.ready = ready

            docker = kubernetes.Docker()
            daemon = kubernetes.DockerDaemon(node, docker, dockerVersion)
            node.Daemon = daemon
            self.NodesByName[name] = node
        if not self.Cluster:
            self.Cluster = kubernetes.Cluster(id, 'Kubernetes Cluster')
        for _, node in self.NodesByName.items():
            node.Cluster = self.Cluster

    def parseNamespaces(self, namespaces):
        items = getItems(namespaces)
        for item in items:
            name = item['metadata']['name']
            id = item['metadata']['uid']
            namespace = kubernetes.Namespace(id, name)
            namespace.Cluster = self.Cluster
            self.NamespacesByName[name] = namespace

    def parseServices(self, services):
        items = getItems(services)
        for item in items:
            name = item['metadata']['name']
            id = item['metadata']['uid']
            namespace = item['metadata']['namespace']

            service = kubernetes.Service(id, name)
            service.Namespace = self.NamespacesByName.get(namespace)
            if item['spec'].get('clusterIP'):
                service.clusterIp = item['spec']['clusterIP']
            if item['spec'].get('type'):
                service.type = item['spec']['type']
            if item['spec'].get('selector'):
                service.Selector = item['spec']['selector']
                for item in self.PodsById:
                    pod = self.PodsById[item]
                    if not namespace == pod.Namespace.getName():
                        continue
                    if not (pod.Labels and service.Selector):
                        continue
                    select = True
                    for key in service.Selector:
                        if not service.Selector[key] == pod.Labels.get(key):
                            select = False
                    if select:
                        service.Pods.append(pod)
            self.ServicesById[id] = service
            self.ServicesByNameSpaceAndName[namespace + '-' + name] = service

    def parsePods(self, pods):
        items = getItems(pods)
        for item in items:
            node = None
            controller = None
            name = item['metadata']['name']
            id = item['metadata']['uid']
            namespace = item['metadata']['namespace']
            if item['metadata'].get('ownerReferences'):
                for owner in item['metadata']['ownerReferences']:
                    if owner.get('controller', None):
                        controller = owner['uid']
            status = item['status']['phase']
            pod = kubernetes.Pod(id, name)
            pod.Namespace = self.NamespacesByName.get(namespace)
            if item['metadata'].get('labels'):
                pod.Labels = item['metadata']['labels']
            if item['metadata'].get('annotations', None):
                pod.annotations = item['metadata']['annotations']
            if item['spec'].get('nodeName'):
                node = item['spec']['nodeName']
                pod.Node = self.NodesByName[node]
            if controller:
                pod.Controller = self.ControllersById.get(controller)
            pod.status = status
            if item['spec'].get('volumes'):
                for volume in item['spec']['volumes']:
                    volumeName = volume['name']
                    if 'hostPath' in volume:
                        hostPath = volume['hostPath']['path']
                        dockerVolume = kubernetes.DockerVolume(volumeName)
                        dockerVolume.volumeSrc = hostPath
                        dockerVolume.type = VolumeTypes.HostPath
                        pod.Volumes[volumeName] = dockerVolume
                    elif 'persistentVolumeClaim' in volume:
                        claimName = volume['persistentVolumeClaim']['claimName']
                        dockerVolume = kubernetes.DockerVolume(volumeName)
                        dockerVolume.volumeSrc = claimName
                        dockerVolume.type = VolumeTypes.PersistentVolumeClaim
                        pod.Volumes[volumeName] = dockerVolume
                        pvcs = self.PersistentVolumeClaimsByName.get(namespace)
                        if pvcs:
                            pvc = pvcs.get(claimName)
                            if pvc:
                                dockerVolume.Source = pvc
            self.PodsById[id] = pod

            if node:
                if item['status'].get('containerStatuses'):
                    for cStatus in item['status']['containerStatuses']:
                        containerName = cStatus['name']
                        state = cStatus['state'].keys()[0]
                        if cStatus['imageID']:
                            imageId = cStatus['imageID'].split('sha256:')[1]
                            imageFullName = cStatus['image']
                            imageName = imageFullName.split(':')[0]
                            imageTag = imageFullName.split(':')[1]
                            if not self.ImageTemplatesById.get(imageId):
                                imageTemplate = kubernetes.DockerImageTemplate(imageId)
                                self.ImageTemplatesById[imageId] = imageTemplate
                            else:
                                imageTemplate = self.ImageTemplatesById[imageId]
                            image = kubernetes.DockerImage(imageId, imageName, node, imageTemplate, imageTag)
                            pod.Node.Images.append(image)
                            if state == 'running':
                                containerId = cStatus['containerID'].split('docker://')[1]
                                container = kubernetes.DockerContainer(containerId, containerName, pod, image)
                                self.ContainersById[containerId] = container
                                pod.Containers[containerName] = container
                if item['spec'].get('containers'):
                    for container in item['spec']['containers']:
                        containerObj = pod.Containers.get(container['name'])
                        if not containerObj:
                            logger.debug('Container %s is not in running status' % container['name'])
                            continue
                        if container.get('volumeMounts'):
                            for mount in container['volumeMounts']:
                                vName = mount['name']
                                mountPath = mount['mountPath']
                                dockerVolume = pod.Volumes.get(vName)
                                if dockerVolume:
                                    dockerVolume.volumeDst = mountPath
                                    containerObj.Volumes.append(dockerVolume)
                else:
                    logger.debug('No container found for: ', pod)

    def parseJobs(self, jobs):
        self.parseControllers(jobs, 'Job')

    def parseReplicaSets(self, replicaSets):
        self.parseControllers(replicaSets, 'Replica Set')

    def parseDeployments(self, deployments):
        self.parseControllers(deployments, 'Deployment')

    def parseReplicationControllers(self, replicationControllers):
        self.parseControllers(replicationControllers, 'Replication Controller')

    def parseDaemonSets(self, daemonSets):
        self.parseControllers(daemonSets, 'Daemon Set')

    def parseStatefulSets(self, statefulSets):
        self.parseControllers(statefulSets, 'Stateful Set')

    def parseControllers(self, controllers, type):
        items = getItems(controllers)
        for item in items:
            controller = self.getController(item)
            controller.type = type
            self.ControllersById[controller.getId()] = controller

    def getController(self, item):
        name = item['metadata']['name']
        id = item['metadata']['uid']
        namespace = item['metadata']['namespace']

        controller = kubernetes.Controller(id, name)
        controller.Namespace = self.NamespacesByName.get(namespace)
        if item['spec'].get('replicas'):
            replicas = item['spec']['replicas']
            controller.replicas = int(replicas)
        containers = item['spec']['template']['spec']['containers']

        if item['metadata'].get('ownerReferences'):
            for owner in item['metadata']['ownerReferences']:
                if owner['controller']:
                    id = owner['uid']
                    controller.Controller = self.ControllersById.get(id)

        for container in containers:
            containerName = container['name']
            image = container['image']
            containerConfig = kubernetes.ContainerConfig(containerName, controller)
            containerConfig.image = image
            if container.get('args'):
                args = container['args']
                containerConfig.args = args
            controller.ContainerConfigs.append(containerConfig)
        return controller

    def parseResourcequotas(self, resourcequotas):
        items = getItems(resourcequotas)
        for item in items:
            name = item['metadata']['name']
            id = item['metadata']['uid']
            namespace = item['metadata']['namespace']
            resourcequota = kubernetes.ResourceQuota(id, name)
            resourcequota.Namespace = self.NamespacesByName.get(namespace)
            if item['spec'].get('hard'):
                hard = item['spec']['hard']
                # parseCpuValue need None check
                if hard.get('cpu'):
                    resourcequota.hardCpu = parse_cpu_value(hard.get('cpu'))
                # parseValue need None check
                if hard.get('memory'):
                    resourcequota.hardMemory = parseValue(hard.get('memory'))
                resourcequota.hardPods = hard.get('pods')
            if item['status'].get('used'):
                used = item['status']['used']
                if used.get('cpu'):
                    resourcequota.usedCpu = parse_cpu_value(used.get('cpu'))
                if used.get('memory'):
                    resourcequota.usedMemory = parseValue(used.get('memory'))
                resourcequota.usedPods = used.get('pods')
            self.ResourcequotasById[id] = resourcequota

def getItems(json):
    if json and json.get('items'):
        return json['items']
    return []


# Get value in Mega bytes
def parseValue(value):
    if value == '0' or value == 0:
        return value
    if value.find('Ki') != -1:
        valueParsed = int(value.split('Ki')[0])/1024
    elif value.find('Mi') != -1:
        valueParsed = int(value.split('Mi')[0])
    elif value.find('Gi') != -1:
        valueParsed = int(value.split('Gi')[0])*1024
    elif value.find('k') != -1:
        valueParsed = int(value.split('k')[0])/1024
    elif value.find('m') != -1:
        valueParsed = int(value.split('m')[0])
    elif value.find('g') != -1:
        valueParsed = int(value.split('g')[0])*1024
    else:
        logger.debug('Unrecognized capacity size: %s' % value)
        valueParsed = None
    return valueParsed

# normalize cpu value
def parse_cpu_value(value):
    if value.find('m') != -1:
        valueParsed = float(value.split('m')[0])/1000
    elif value.find('k') != -1:
        valueParsed = int(value.split('k')[0])*1000
    elif value.isdigit():
        valueParsed = value
    else:
        logger.debug('Unrecognized capacity size: %s' % value)
        valueParsed = None
    return valueParsed
