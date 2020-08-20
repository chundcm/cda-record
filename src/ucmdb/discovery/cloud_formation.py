# coding=utf-8

import entity
import logger
import modeling
import aws
from appilog.common.system.types.vectors import ObjectStateHolderVector
from appilog.common.system.types import ObjectStateHolder

STACK_REOURSE_TYPE_AND_CIT_MAP = {
    'AWS::EC2::Instance': 'host_node',
    'AWS::AutoScaling::AutoScalingGroup': 'amazon_asg',
    'AWS::EC2::Subnet': 'ip_subnet',
    'AWS::EC2::VPC': 'aws_vpc',
    'AWS::S3::Bucket':'amazon_s3_bucket',
    'AWS::ECS::Cluster':'amazon_ecs_cluster',
    'AWS::ECS::Service':'amazon_ecs_service',
    'AWS::ECS::TaskDefinition':'amazon_ecs_task_definition',
}


CIT_AND_KEY_ATTRIBUTE_MAP = {
    'host_node': 'cloud_instance_id',
    'amazon_asg': 'amazon_resource_name',
    'ip_subnet': 'cloud_resource_identifier',
    'aws_vpc': 'vpc_id',
    'amazon_s3_bucket':'amazon_resource_name',
    'amazon_ecs_cluster':'amazon_resource_name',
    'amazon_ecs_service':'amazon_resource_name',
    'amazon_ecs_task_definition':'amazon_resource_name',
}


def discoverCloudFormationTopology(framework, service, account, credential_id, resourceDict):
    r'@types: Framework, AmazonCloudFormationService, aws.Account'
    vector = ObjectStateHolderVector()

    from com.amazonaws.regions import RegionUtils
    regions = RegionUtils.getRegionsForService(service.ENDPOINT_PREFIX)
    logger.debug("cloud formation regions:", regions)
    for region in regions:
        endpoint = region.getServiceEndpoint(service.ENDPOINT_PREFIX)
        service.setEndpoint(endpoint)
        aws_region = aws.Region(region.getName())

        try:
            stacks = _discoverStack(service)
            reporter = Reporter(Builder())
            for stack in stacks:
                vector.addAll(reporter.reportStack(stack, account, aws_region))

        except:
            logger.warnException('Fail to discover cloud formation in region:', region)

    return vector


def _discoverStack(service):
    discoverer = CloudFormationDiscoverer(service)
    try:
        stacks = discoverer.getStacks()
        logger.debug("stacks:", stacks)
        for stack in stacks:
            try:
                stack.setResources(discoverer.getStackResources(stack.getName()))
            except Exception, ex:
                logger.warnException("Failed to discover stack %s resources: %s" % (stack.getName(), ex))
        return stacks

    except Exception, ex:
        logger.warnException("Failed to discover stacks: %s" % ex)


class CloudFormationDiscoverer:
    def __init__(self, service):
        self._service = service

    def getStacks(self):
        results = self._service.describeStacks().getStacks()
        return map(self._convertToStacks, results)

    def getStackResources(self, stack_name):
        from com.amazonaws.services.cloudformation.model import DescribeStackResourcesRequest
        rq = DescribeStackResourcesRequest().withStackName(stack_name)
        results = self._service.describeStackResources(rq).getStackResources()
        return map(self._convertToStackResources, results)

    def _convertToStacks(self, item):
        r'@types: com.amazonaws.services.cloudformation.model.StackSummary -> aws.Stack'
        return Stack(item.getStackId(),
                     item.getStackName(),
                     item.getStackStatus(),
                     item.getDescription(),
                     item.getCreationTime())

    def _convertToStackResources(self, item):
        r'@types: com.amazonaws.services.cloudformation.model.StackResourceSummary -> aws.StackResource'
        return StackResource(item.getLogicalResourceId(),
                             item.getPhysicalResourceId(),
                             item.getResourceType())


class Stack(entity.HasName, entity.HasOsh):
    def __init__(self, id, name, status, description, creation_time=None):

        entity.HasName.__init__(self)
        entity.HasOsh.__init__(self)
        self.setName(name)
        if not id:
            raise ValueError("stack id is empty")
        if not name:
            raise ValueError("stack name is empty")
        self.__id = id
        self.__status = status
        self.__template_desc = description
        self.__resources = []
        self.__creation_time = creation_time

    def getId(self):
        r'@types: -> Stack.stackId'
        return self.__id

    def getStatus(self):
        r'@types: -> Stack.stackStatus'
        return self.__status

    def getTemplateDescription(self):
        r'@types: -> Stack.templateDescription'
        return self.__template_desc

    def setResources(self, resources):
        self.__resources = resources

    def getResources(self):
        return self.__resources

    def getCreationTime(self):
        return self.__creation_time

    def acceptVisitor(self, visitor):
        r'''@types: CanVisitAwsStack -> object
        Introduce interface for visitor expected here
        '''
        return visitor.visitAwsStack(self)

    def __repr__(self):
        return 'cloud_formation.Stack("%s", "%s", "%s")' % (
            self.getId(), self.getName(), self.getStatus())


class StackResource:
    def __init__(self, logical_id, physical_id, type):

        if not logical_id:
            raise ValueError("resource logical id is empty")
        if not physical_id:
            raise ValueError("resource physical id is empty")
        if not type:
            raise ValueError("resource type is empty")
        self.__logical_id = logical_id
        self.__physical_id = physical_id
        self.__type = type

    def getLogicalId(self):
        r'@types: -> StackResource.logicalResourceId'
        return self.__logical_id

    def getPhysicalId(self):
        r'@types: -> StackResource.physicalResourceId'
        return self.__physical_id

    def getType(self):
        r'@types: -> StackResource.resourceType'
        return self.__type

    def __repr__(self):
        return 'cloud_formation.StackResource("%s", "%s", "%s")' % (
            self.getLogicalId(), self.getPhysicalId(), self.getType())


class Builder:
    def buildStackOsh(self, stack):
        osh = ObjectStateHolder('aws_stack')
        osh.setAttribute('stack_id', stack.getId())
        osh.setAttribute('name', stack.getName())
        osh.setAttribute('stack_status', stack.getStatus())
        osh.setAttribute('description', stack.getTemplateDescription())
        osh.setAttribute('stack_creation_time', stack.getCreationTime())
        return osh

    def visitAwsStack(self, stack):
        return self.buildStackOsh(stack)


class Reporter:
    def __init__(self, locationBuilder):
        r'@types: cloud_formation.Builder'
        self.__builder = locationBuilder

    def reportStack(self, stack, account, region):
        if not (account and account.getOsh()):
            raise ValueError("AWS Account is not specified or not built")
        if not stack:
            raise ValueError("AWS stack is not specified")
        vector = ObjectStateHolderVector()
        region_osh = aws.Reporter(aws.Builder()).reportRegion(region)
        stack_osh = stack._build(self.__builder)
        stack_osh.setContainer(account.getOsh())
        stack_osh.setAttribute('amazon_resource_name', 'arn:aws:cloudformation:' + region.getName() + ':' + account.getId() + 'stack/' + stack.getName() + '/' + stack.getId())
        vector.add(stack_osh)
        vector.add(modeling.createLinkOSH('membership', region_osh.get(0), stack_osh))

        for resource in stack.getResources():
            cit = STACK_REOURSE_TYPE_AND_CIT_MAP.get(resource.getType())
            if cit:
                resource_osh = ObjectStateHolder(cit)
                resource_osh.setAttribute(CIT_AND_KEY_ATTRIBUTE_MAP.get(cit), resource.getPhysicalId())
                if cit == 'amazon_asg':
                    resource_osh.setContainer(account.getOsh())
                    vector.add(modeling.createLinkOSH('containment', region_osh.get(0), resource_osh))

                vector.add(resource_osh)
                vector.add(modeling.createLinkOSH('containment', stack_osh, resource_osh))

        return vector
