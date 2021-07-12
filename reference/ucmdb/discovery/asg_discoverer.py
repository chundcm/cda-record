import logger
import modeling

import aws
import asg

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector

def discoverASGTopology(framework, service, account, credential_id, resourceDict):
    logger.info('ASG TOPOLOGY DISCOVERY')
    vASG = ObjectStateHolderVector()

    if not resourceDict['Regions']:
        logger.warnException('No region found for Auto Scaling Group discovery.')
        return vASG

    for region in resourceDict['Regions']:
        try:
            asgList = []
            endpoint = region.getEndpointHostName().replace('ec2', 'autoscaling')
            service.setEndpoint(endpoint)
            asgDiscoverer = ASGDiscoverer(service)
            findAsgs = asgDiscoverer.discover(endpoint)
            for findAsg in findAsgs:
                asgList.append(findAsg)
            vASG.addAll(reportASGTopology(account, region, asgList, resourceDict))
        except:
            logger.warnException('Fail in region:', region)
    return vASG


def reportASGTopology(account, region, asgList, resourceDict):
    vector = ObjectStateHolderVector()
    accountOsh = aws.Reporter(aws.Builder()).reportAccount(account)
    vRegionOsh = aws.Reporter(aws.Builder()).reportRegion(region)
    vector.add(accountOsh)
    vector.add(vRegionOsh.get(0))
    for asg in asgList:
        asgOsh = ASGBuilder().build(asg)
        asgOsh.setContainer(accountOsh)
        vector.add(asgOsh)
        createRelationShipWithRegion(asgOsh, vRegionOsh, vector)
        createRelationShipWithEc2Instance(asg, asgOsh, resourceDict, vector)
    return vector

def createRelationShipWithRegion(asgOsh, vRegionOsh, vector):
    vector.add(modeling.createLinkOSH('containment', vRegionOsh.get(0), asgOsh))

def createRelationShipWithEc2Instance(asg, asgOsh, resourceDict, vector):
    try:
        ec2instances = asg.groupaws.getInstances()
        for ec2instance in ec2instances:
            ec2Osh = resourceDict['Ec2Intances'][ec2instance.getInstanceId()]
            if ec2Osh:
                vector.add(modeling.createLinkOSH('membership', asgOsh, ec2Osh))
        return
    except:
        return

class ASGDiscoverer:
    def __init__(self, service):
        self._service = service
        self.ASGs = []

    def getASG(self, endpoint):
        asgList = []
        items = self._service.describeAutoScalingGroups().getAutoScalingGroups()
        for item in items:
            instance = asg.ASG(item.getAutoScalingGroupName())
            instance.creation_time = item.getCreatedTime()
            instance.desired_instance_number = item.getDesiredCapacity()
            instance.max_instance_number = item.getMaxSize()
            instance.min_instance_number = item.getMinSize()
            instance.health_check_type = item.getHealthCheckType()
            instance.arn = item.getAutoScalingGroupARN()
            instance.groupaws = item
            asgList.append(instance)
        return asgList

    def discover(self, endpoint):
        asgList = self.getASG(endpoint)
        return asgList

class ASGBuilder:
    def build(self, asg):
        if asg is None: raise ValueError("ASG is None!")

        asgOsh = ObjectStateHolder('amazon_asg')
        asgOsh.setStringAttribute('name', asg.getName())
        if asg.creation_time:
            asgOsh.setDateAttribute('creation_time', asg.creation_time)
        if asg.desired_instance_number:
            asgOsh.setIntegerAttribute('desired_instance_number', asg.desired_instance_number)
        if asg.max_instance_number:
            asgOsh.setIntegerAttribute('max_instance_number', asg.max_instance_number)
        if asg.min_instance_number:
            asgOsh.setIntegerAttribute('min_instance_number', asg.min_instance_number)
        if asg.health_check_type:
            asgOsh.setStringAttribute('health_check_type', asg.health_check_type)
        if asg.arn:
            asgOsh.setStringAttribute('amazon_resource_name', asg.arn)
        return asgOsh
