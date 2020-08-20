#coding=utf-8
import logger
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector

import ecs

def discoverEcsTopology(framework, service, account, credential_id, resourceDict):
    class EcsDiscoverer:
        def __init__(self, service, account, region):
            self._service = service
            self._account = account
            self._region = region
            self.TaskDefinitions = {}
            self.Tasks = {}
            self.Containers = {}
            self.Clusters = {}
            self.ContainerDefinitions = {}
            self.Services = {}
            self.DockerDaemons = []

        def getTopology(self):
            self.getTaskDef()
            self.getCluster()
            resourceDict['EcsContainers'].update(self.Containers)
            return self.reportTopology()

        def getTaskDef(self):
            from com.amazonaws.services.ecs.model import DescribeTaskDefinitionRequest
            try:
                taskDefArnList = self._service.listTaskDefinitions().getTaskDefinitionArns() or ()
                for taskDefArn in taskDefArnList:
                    request = DescribeTaskDefinitionRequest().withTaskDefinition(taskDefArn)
                    taskDef = self._service.describeTaskDefinition(request).getTaskDefinition()
                    taskDefinition = buildTaskDef(taskDef, self._account, self._region)
                    self.TaskDefinitions[taskDefArn] = taskDefinition
            except:
                logger.warnException('Fail to get task definition!')

        def getCluster(self):
            from com.amazonaws.services.ecs.model import DescribeClustersRequest
            from com.amazonaws.services.ecs.model import DescribeContainerInstancesRequest
            from com.amazonaws.services.ecs.model import ListContainerInstancesRequest
            clusterArnList = self._service.listClusters().getClusterArns()
            clusterList = []
            block = 99
            for i in range(0, len(clusterArnList), block):
                try:
                    desClusterrequest = DescribeClustersRequest().withClusters(clusterArnList[i:i+block])
                    result = self._service.describeClusters(desClusterrequest).getClusters()
                    clusterList.extend(result)
                except:
                    logger.warnException('Fail to get cluster!')
            for clusterItem in clusterList:
                cluster = buildCluster(clusterItem, self._account, self._region)
                self.Clusters[cluster.getArn()] = cluster
                self.getTask(cluster.getArn())
                self.getService(cluster.getArn())
                try:
                    # Get Container Instance in Cluster
                    listRequest = ListContainerInstancesRequest().withCluster(cluster.getArn())
                    containerInstanceArnList = self._service.listContainerInstances(listRequest).getContainerInstanceArns()

                    if containerInstanceArnList:
                        request = DescribeContainerInstancesRequest().withContainerInstances(containerInstanceArnList).withCluster(cluster.getArn())
                        containerInstanceList = self._service.describeContainerInstances(request).getContainerInstances()

                        for containerInstance in containerInstanceList:
                            ec2InstanceId = containerInstance.getEc2InstanceId()
                            if resourceDict['Ec2Intances'].has_key(ec2InstanceId):
                                cluster.Ec2Instances.append(resourceDict['Ec2Intances'][ec2InstanceId])
                except:
                    logger.warnException('Fail to get container instance!')

        def getTask(self, clusterArn):
            from com.amazonaws.services.ecs.model import ListTasksRequest
            from com.amazonaws.services.ecs.model import DescribeTasksRequest
            try:
                listRequest = ListTasksRequest().withCluster(clusterArn)
                taskArnList = self._service.listTasks(listRequest).getTaskArns() or ()

                if taskArnList:
                    describeRequest = DescribeTasksRequest().withTasks(taskArnList).withCluster(clusterArn)
                    tasks = self._service.describeTasks(describeRequest).getTasks() or ()
                    for taskItem in tasks:
                        dockerDaemon = self.getEc2Intance(taskItem)
                        if dockerDaemon:
                            self.DockerDaemons.append(dockerDaemon)
                            task = buildTask(taskItem, self.TaskDefinitions, self.Clusters, dockerDaemon)
                            self.Tasks[task.getArn()] = task
                            self.getContainer(taskItem, task, dockerDaemon)
            except:
                logger.warnException('Fail to get task!')

        def getContainer(self, taskItem, task, dockerDaemon):
            containers = taskItem.getContainers()
            for containerItem in containers:
                container = buildContainer(containerItem, task, dockerDaemon)
                self.Containers[container.getArn()] = container

        def getService(self, clusterArn):
            from com.amazonaws.services.ecs.model import ListServicesRequest
            from com.amazonaws.services.ecs.model import DescribeServicesRequest
            try:
                listRequest = ListServicesRequest().withCluster(clusterArn)
                serviceArnList = self._service.listServices(listRequest).getServiceArns() or ()
                if serviceArnList:
                    describeRequest = DescribeServicesRequest().withServices(serviceArnList).withCluster(clusterArn)
                    serviceList = self._service.describeServices(describeRequest).getServices()
                    for serviceItem in serviceList:
                        service = buildService(serviceItem, self.Clusters, self.TaskDefinitions)
                        self.Services[service.getArn()] = service
            except:
                logger.warnException('Fail to get service!')

        def getEc2Intance(self, taskItem):
            from com.amazonaws.services.ecs.model import DescribeContainerInstancesRequest
            containerInstanceArn = taskItem.getContainerInstanceArn()
            request = DescribeContainerInstancesRequest().withContainerInstances(containerInstanceArn).withCluster(taskItem.getClusterArn())
            containerInstance = self._service.describeContainerInstances(request).getContainerInstances()[0]
            ec2Id = containerInstance.getEc2InstanceId()
            logger.debug('ec2Id: ', ec2Id)
            if resourceDict['Ec2Intances'].has_key(ec2Id):
                ec2Instance = resourceDict['Ec2Intances'][ec2Id]
                dockerDaemon = buildDockerDaemon(containerInstance.getVersionInfo(), ec2Instance)
                return dockerDaemon
            return None

        def reportTopology(self):
            vector = ObjectStateHolderVector()
            for componentDict in (self.Clusters, self.TaskDefinitions, self.Tasks, self.Containers, self.Services):
                for _, item in componentDict.items():
                    vector.addAll(item.report())
            for daemon in self.DockerDaemons:
                vector.addAll(daemon.report())
            return vector

    logger.info('ECS TOPOLOGY DISCOVERY')
    vector = ObjectStateHolderVector()
    if not resourceDict['Regions']:
        raise Exception('No region found for ECS discovery.')
    for region in resourceDict['Regions']:
        try:
            service.setEndpoint(region.getEndpointHostName().replace('ec2', 'ecs'))
            ecsDiscoverer = EcsDiscoverer(service, account, region)
            vector.addAll(ecsDiscoverer.getTopology())
        except:
            logger.warnException('Fail in region:', region)
    return vector

def buildTaskDef(taskDef, account, region):
    taskDefinition = ecs.TaskDefinition(account, region)
    taskDefinition.setArn(taskDef.getTaskDefinitionArn())
    taskDefinition.family = taskDef.getFamily()
    taskDefinition.revision = taskDef.getRevision()
    taskDefinition.name = taskDefinition.family + ':' + str(taskDefinition.revision)
    taskDefinition.status = taskDef.getStatus()
    taskDefinition.attributes = []
    for attribute in taskDef.getRequiresAttributes():
        if attribute and attribute.getName() and attribute.getValue():
            attributeString = attribute.getName() + ':' + attribute.getValue()
            taskDefinition.attributes.append(attributeString)
    osh = ObjectStateHolder('amazon_ecs_task_definition')
    osh.setStringAttribute('amazon_resource_name', taskDefinition.getArn())
    osh.setStringAttribute('name', taskDefinition.name)
    osh.setStringAttribute('family', taskDefinition.family)
    osh.setIntegerAttribute('revision', taskDefinition.revision)
    osh.setStringAttribute('status', taskDefinition.status)
    osh.setListAttribute('attributes', taskDefinition.attributes)
    taskDefinition.setOsh(osh)
    taskDefinition.containerDefs = {}
    for containerDef in taskDef.getContainerDefinitions():
        containerName = containerDef.getName()
        taskDefinition.containerDefs[containerName] = {}
        taskDefinition.containerDefs[containerName]['commands'] = containerDef.getCommand()
        taskDefinition.containerDefs[containerName]['cpu'] = containerDef.getCpu()
        taskDefinition.containerDefs[containerName]['labels'] = containerDef.getDockerLabels()
        taskDefinition.containerDefs[containerName]['entrypoint'] = containerDef.getEntryPoint()
        taskDefinition.containerDefs[containerName]['environment'] = containerDef.getEnvironment()
        taskDefinition.containerDefs[containerName]['link'] = containerDef.getLinks()
        taskDefinition.containerDefs[containerName]['memory'] = containerDef.getMemory()
        taskDefinition.containerDefs[containerName]['mount'] = containerDef.getMountPoints()
        taskDefinition.containerDefs[containerName]['ports'] = containerDef.getPortMappings()
        taskDefinition.containerDefs[containerName]['volumes'] = containerDef.getVolumesFrom()
        taskDefinition.containerDefs[containerName]['image'] = containerDef.getImage()
    logger.debug('Discovered : ', taskDefinition)
    return taskDefinition

def buildTask(taskItem, taskDefs, clusters, dockerDaemon):
    node = dockerDaemon.DockerNode
    taskDef = taskDefs[taskItem.getTaskDefinitionArn()]
    cluster = clusters[taskItem.getClusterArn()]
    task = ecs.Task(node, taskDef, cluster)
    task.setArn(taskItem.getTaskArn())
    task.startedBy = taskItem.getStartedBy()
    task.createdAt = taskItem.getCreatedAt()
    task.lastStatus = taskItem.getLastStatus()
    osh = ObjectStateHolder('amazon_ecs_task')
    osh.setStringAttribute('amazon_resource_name', task.getArn())
    osh.setStringAttribute('name', task.getArn())
    osh.setStringAttribute('started_by', task.startedBy)
    osh.setStringAttribute('status', task.lastStatus)
    task.setOsh(osh)
    logger.debug('Discovered : ', task)
    return task


def buildDockerDaemon(versionInfo, ec2Instance):
    daemon = ecs.DockerDaemon(ec2Instance)
    daemon.version = versionInfo.getDockerVersion()
    osh = ObjectStateHolder('docker_daemon')
    osh.setStringAttribute('name', daemon.name)
    osh.setStringAttribute('version', daemon.version)
    osh.setStringAttribute('discovered_product_name', daemon.productName)
    daemon.setOsh(osh)
    logger.debug('Discovered : ', daemon)
    return daemon


def buildContainer(ctItem, task, dockerDaemon):
    container = ecs.DockerContainer(dockerDaemon, task)
    container.setArn(ctItem.getContainerArn())
    container.name = ctItem.getName()
    container.status = ctItem.getLastStatus()
    if task.TaskDefinition.containerDefs.has_key(container.name):
        container.imageName = task.TaskDefinition.containerDefs[container.name]['image']
        commands = task.TaskDefinition.containerDefs[container.name]['commands']
        if commands:
            for cmd in commands:
                container.commands.append(cmd)
        labels = task.TaskDefinition.containerDefs[container.name]['labels']
        if labels:
            for key, value in labels.items():
                label = key + ':' + value
                container.labels.append(label)

    osh = ObjectStateHolder('docker_container')
    osh.setStringAttribute('docker_container_id', container.getArn())
    osh.setStringAttribute('name', container.name)
    osh.setListAttribute('docker_container_commands', container.commands)
    osh.setListAttribute('docker_container_labels', container.labels)
    container.setOsh(osh)
    logger.debug('Discovered : ', container)
    return container

def buildCluster(clItem, account, region):
    cluster = ecs.Cluster(account, region)
    cluster.arn = clItem.getClusterArn()
    cluster.setArn(cluster.arn)
    cluster.name = clItem.getClusterName()
    cluster.status = clItem.getStatus()
    cluster.containerInsCount = clItem.getRegisteredContainerInstancesCount()
    osh = ObjectStateHolder('amazon_ecs_cluster')
    osh.setStringAttribute('amazon_resource_name', cluster.arn)
    osh.setStringAttribute('name', cluster.name)
    osh.setStringAttribute('status', cluster.status)
    osh.setIntegerAttribute('registered_container_instance_count', cluster.containerInsCount)
    cluster.setOsh(osh)
    logger.debug('Discovered : ', cluster)
    return cluster

def buildService(svItem, clusters, taskDefs):
    cluster = clusters[svItem.getClusterArn()]
    taskDef = taskDefs[svItem.getTaskDefinition()]
    service = ecs.Service(cluster, taskDef)
    service.setArn(svItem.getServiceArn())
    service.name = svItem.getServiceName()
    service.status = svItem.getStatus()
    service.desiredCount = svItem.getDesiredCount()
    service.runningCount = svItem.getRunningCount()
    service.maximumPercent = svItem.getDeploymentConfiguration().getMaximumPercent()
    service.minimumHealthyPercent = svItem.getDeploymentConfiguration().getMinimumHealthyPercent()
    osh = ObjectStateHolder('amazon_ecs_service')
    osh.setStringAttribute('amazon_resource_name', service.getArn())
    osh.setStringAttribute('name', service.name)
    osh.setStringAttribute('status', service.status)
    osh.setIntegerAttribute('desired_count', service.desiredCount)
    osh.setIntegerAttribute('running_count', service.runningCount)
    osh.setIntegerAttribute('max_percent', service.maximumPercent)
    osh.setIntegerAttribute('min_percent', service.minimumHealthyPercent)
    service.setOsh(osh)
    logger.debug('Discovered : ', service)
    return service

