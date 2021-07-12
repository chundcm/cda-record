#coding=utf-8
# Discovery of Amazon Cloud using Web Services
# ============================================
"""
Created on Aug 18, 2011
@author: Vladimir Vitvitskiy

Discovery can be configured to cover several AWS services, for instance

  * EC2 (Elastic Cloud Service)
  * RDS (Relational Database Service)
"""

from java.util import ArrayList
from java.util import Properties
from java.lang import Exception as JException, Boolean
import re
import json
import logger
import aws
import ec2
from appilog.common.system.types.vectors import ObjectStateHolderVector
import errormessages
import errorobject
import errorcodes
import aws_store
import iteratortools
import aws_rds
import db
import netutils
import db_platform
import db_builder
import ecs_discoverer
import apigateway_discoverer
import ecr_discoverer
import s3_discoverer
import asg_discoverer
import cloud_formation
import lambda_function
import sns
import sqs
from com.hp.ucmdb.discovery.library.clients import MissingSdkJarException

resourceDict = {}
resourceDict['Ec2Intances'] = {}
resourceDict['EcsContainers'] = {}
resourceDict['Regions'] = []

# Module entry point
# ------------------
def DiscoveryMain(framework):
    # === Declaration ===

    # Discovery infrastructure has predefined protocol and client for the AWS,
    # called _awsprotocol_ which is used for different discoveries.
    protocolName = 'awsprotocol'
    # As module serves for discoveries using multiple services we need a single
    # point where we can handle discovery input date, result and errors in a
    # single way.

    # Configuration of discoveries per Amazon Service allows us to declare
    # possible flows:
    CONNECTIONS = (
                # * Perform **EC2** discovery in case if job parameter
                # _ec2DiscoveryEnabled_ is set to "true". Here we have
                # specific method to connect service and list of discoveries
                # where this connection will be used.
               _Connection(_connectToEc2Service,
                          (_Discovery('ec2DiscoveryEnabled',
                                      _discoverEc2Topology,
                                      'EC2 Discovery'
                                      ),
                          )
               ),
               _Connection(_connectToLoadBalancingService,
                          (_Discovery('loadBalancingDiscoveryEnabled',
                                      _discoverLoadBalancingTopology,
                                      'LoadBalancing Discovery'
                                      ),
                          )
               ),
               _Connection(_connectToLoadBalancingV2Service,
                          (_Discovery('loadBalancingDiscoveryEnabled',
                                      _discoverLoadBalancingV2Topology,
                                      'LoadBalancing V2 Discovery'
                                      ),
                          )
               ),
                # * Perform **RDS** discovery in case if job parameter
                # _rdsDiscoveryEnabled_ is set to "true"
               _Connection(_connectToRdsService,
                          (_Discovery('rdsDiscoveryEnabled',
                                      _discoverRdsTopology,
                                      'RDS Discovery'),
                          )
               ),
               # * Perform **ECS** discovery in case if job parameter
               # _ecsDiscoveryEnabled_ is set to "true"
               _Connection(_connectToEcsService,
                           (_Discovery('ecsDiscoveryEnabled',
                                       ecs_discoverer.discoverEcsTopology,
                                       'ECS Discovery'),
                            )
               ),
               # * Perform **ECR** discovery in case if job parameter
               # _ecsDiscoveryEnabled_ is set to "true"
               _Connection(_connectToEcrService,
                           (_Discovery('ecrDiscoveryEnabled',
                                       ecr_discoverer.discoverEcrTopology,
                                       'ECR Discovery'),
                            )
               ),
               # * Perform **S3** discovery in case if job parameter
               # _s3DiscoveryEnabled_ is set to "true"
               _Connection(_connectToS3Service,
                           (_Discovery('s3DiscoveryEnabled',
                                       s3_discoverer.discoverS3Topology,
                                       'S3 Discovery'),
                            )
               ),
               # * Perform **ASG** discovery in case if job parameter
               # _asgDiscoveryEnabled_ is set to "true"
               _Connection(_connectToASGService,
                           (_Discovery('asgDiscoveryEnabled',
                                       asg_discoverer.discoverASGTopology,
                                       'ASG Discovery'),
                            )
               ),
               # * Perform **EC2 installed SW** discovery in case if job parameter
               # _ec2DiscoveryEnabled_ is set to "true". Here we connect to ssm
               # service.
               _Connection(_ConnectToSSMService,
                           (_Discovery('ec2DiscoveryEnabled',
                                       _discoverEC2InstalledSW,
                                       'EC2 Installed SW Discovery'),
                           )
               ),
               _Connection(_connectToApi_GatewayService,
                           (_Discovery('apigatewayDiscoveryEnabled',
                                       apigateway_discoverer.discoverApi_GatewayTopology,
                                       'API Gateway Discovery'),
                            )
                           ),
               _Connection(_connectToCloudFormationService,
                           (_Discovery('cloudFormationDiscoveryEnabled',
                                       cloud_formation.discoverCloudFormationTopology,
                                       'CloudFormation Discovery'),
                            )
               ),
               _Connection(_connectToLambdaService,
                          (_Discovery('lambdaDiscoveryEnabled',
                                      lambda_function.discoverLambdaTopology,
                                      'Lambda Discovery'),
                          )
                ),
               _Connection(_connect_to_sns_service,
                          (_Discovery('snsDiscoveryEnabled',
                                      sns.discover_sns_topology,
                                      'SNS Discovery'),
                          )
                ),
               _Connection(_connect_to_sqs_service,
                           (_Discovery('snsDiscoveryEnabled',
                                       sqs.discover_sqs_topology,
                                       'SQS Discovery'),
                            )
                           )
    )
    # Having declared flows we have a mechanism to determine which one should
    # be run. Framework can provide information about job parameters and
    # discovery object has corresponding configuration

    def isEnabledDiscovery(discovery, framework=framework):
        if discovery.jobParameter in ('ec2DiscoveryEnabled',
                                      'rdsDiscoveryEnabled',
                                      'loadBalancingDiscoveryEnabled',
                                      'ecsDiscoveryEnabled',
                                      'ecrDiscoveryEnabled',
                                      's3DiscoveryEnabled',
                                      'asgDiscoveryEnabled',
                                      'apigatewayDiscoveryEnabled',
                                      'cloudFormationDiscoveryEnabled',
                                      'lambdaDiscoveryEnabled',
                                      'snsDiscoveryEnabled'):
            return 1
        jobParameter = framework.getParameter(discovery.jobParameter)
        return Boolean.parseBoolean(jobParameter)

    # Containers for the errors and warnings per discovery life cycle
    discoveryErrors = []
    discoveryWarnings = []
    connectionWarnings = []
    # For the whole Job run we need to know whether there is one successful
    # discovery
    oneDiscoveryHappened = 0
    # Store successful discovery and failed discovery
    discoveredComponent = []
    failedComponent = []

    # === Connection attempt ===

    # We have to find all registered credentials for different accounts
    # to iterate over them for further discovery
    credentials = framework.getAvailableProtocols(None, protocolName)
    for credentialsId in credentials:
        # Establish connection to the Security Token Service and get account ID
        logger.info('Establish connection to Security Token Service')
        try:
            sts = _connectToSTS(framework, credentialsId)
            discoverer = STSDiscoverer(sts)
            accountId, userName = discoverer.getAccount()
        except MissingSdkJarException, e:
            # Missed AWS SDK jars no need to continue discovery report error
            discoveryErrors.append(errorobject.createError(
                                        errorcodes.MISSING_JARS_ERROR,
                                        ['AWS SDK jars are missed. '
                                         'Refer documentation for details'],
                                                           str(e))
                                   )
            break
        except:
            try:
                logger.info('Establish connection to IAM Service')
                iamService = _connectToIamService(framework, credentialsId)
                discoverer = IamDiscoverer(iamService)
                accountId, userName = discoverer.getAccount()
            except (JException, Exception), e:
                # In case if connection failed we try another credentials, maybe for
                # the same account
                msg = str(e)
                logger.warnException(msg)
                if ((msg.find('Status Code: 401') != -1)
                    or re.match('.*?Attribute.*?has no value', msg.strip(), re.I)):
                    warning = errorobject.createError(errorcodes.INVALID_USERNAME_PASSWORD, [protocolName], str(e))
                elif msg.find('Unable to execute HTTP request') != -1:
                    warning = errorobject.createError(errorcodes.DESTINATION_IS_UNREACHABLE, [protocolName], str(e))
                else:
                    warning = errorobject.createError(errorcodes.CONNECTION_FAILED, [protocolName], msg)
                connectionWarnings.append(warning)
                break
        if accountId:
            try:
                # Build and report discovered account for further usage
                iamReporter = aws.Reporter(aws.Builder())
                iamVector = ObjectStateHolderVector()
                account = aws.Account(accountId)
                accountOsh = iamReporter.reportAccount(account)
                iamVector.add(accountOsh)

                user = aws.User(userName)
                userOsh = iamReporter.reportUser(user, accountOsh)
                userOsh.setAttribute('amazon_resource_name', 'arn:aws:iam::' + account.getId() + ':user' + '/' + userName)
                iamVector.add(userOsh)
                framework.sendObjects(iamVector)

            except Exception:
                # As account is a application system failed discovery of such
                # cannot be continued we try other credentials
                logger.debugException("Failed to create account")
                warning = 'Failed to get information about Amazon account'
                discoveryWarnings.append(warning)
                # Every fail has to be remembered for reporting to the UI
            else:
                # Now we have account time to use it in discovery
                for connection in CONNECTIONS:
                    # Apply filtering mechanism to find out what discoveries
                    # are enabled
                    discoveries = filter(isEnabledDiscovery, connection.discoveries)
                    # Possible none of them are enabled so we have to check this
                    # case too
                    if discoveries:
                        # If we have enabled - we perform connection to the
                        # service only once for, possible, multiple discovery flows
                        # that are based on the same service.
                        try:
                            service = connection(framework, credentialsId)
                        except (JException, Exception), e:
                            logger.debugException(str(e))
                        else:
                            # === Run enabled discoveries ===

                            # Each discovery has one attemp to execute its flow
                            # successfully return some topology as ObjectStateHolderVector
                            for discovery in discoveries:
                                try:
                                    # Opened question whether we have to return some
                                    # object that will describe various interesting
                                    # information for client (reported to the UI)
                                    # In any case now it is a vector and it will
                                    # be sent immediately
                                    vector = discovery(framework, service, account, credentialsId, resourceDict)
                                    # === Sending data to the UCMDB
                                    framework.sendObjects(vector)
                                    # At this point we have one successful discovery
                                    oneDiscoveryHappened = 1
                                    discoveredComponent.append(str(discovery))
                                except (JException, Exception), e:
                                    logger.debugException(str(e))
                                    warning = 'Failed to execute %s. Please check detail in communication log.' % discovery.description
                                    discoveryWarnings.append(warning)
                                    failedComponent.append(str(discovery))
    # === Error cases handling ===

    # Discovery finished and we have to show reasons in the UI:
    #
    # * No credentials found for _aws_ protocol
    if not credentials:
        msg = errormessages.makeErrorMessage(protocolName, pattern=errormessages.ERROR_NO_CREDENTIALS)
        errobj = errorobject.createError(errorcodes.NO_CREDENTIALS_FOR_TRIGGERED_IP, [ protocolName ], msg)
        logger.reportErrorObject(errobj)
    # * Among enabled flows no successful discovery
    elif not oneDiscoveryHappened:
        errobj = errorobject.createError(errorcodes.FAILED_RUNNING_DISCOVERY, ['Amazon Cloud'], 'Failed to make discovery')
        logger.reportErrorObject(errobj)
        map(logger.reportWarningObject, connectionWarnings)
    if discoveredComponent:
        successStr = '. '.join(discoveredComponent)
        logger.debug('Discovered component: ', successStr)
    if failedComponent:
        failedStr = '. '.join(failedComponent)
        logger.debug('Failed component: ', failedStr)

    # * Other, like connection troubles or failed discovery
    map(logger.reportErrorObject, discoveryErrors)
    map(logger.reportWarning, discoveryWarnings)
    return ObjectStateHolderVector()

# Discovery details
# -----------------


def _discoverLoadBalancingV2Topology(framework, loadBalancingV2Service, account, credential_id, resourceDict):
    logger.info("Discover elastic application load balancers")
    awsReporter = aws.Reporter(aws.Builder())
    vector = ObjectStateHolderVector()
    if not resourceDict['Regions']:
        raise Exception('No region found for elastic application load balancer discovery.')
    for region in resourceDict['Regions']:
        try:
            loadBalancingV2Service.setEndpoint(region.getEndpointHostName().replace('ec2', 'elasticloadbalancing'))
            items = ElasticLoadBalancingV2Discover(loadBalancingV2Service).getAppLoadBalancers()
            for item in items:
                zoneOshs = []
                lb = item[0]
                for zone in lb.getAvailabilityZones():
                    zoneOsh = awsReporter.reportAvailabilityZoneByName(zone.getZoneName())
                    zoneOshs.append(zoneOsh)
                vpc_osh = vpcById[lb.getVpcId()].getOsh()
                # report  security group
                sg_osh_list = []
                # if lb is network load balance, it may not have security group
                if lb.getSecurityGroups():
                    sg_osh_list = [security_group_dict[i] for i in lb.getSecurityGroups()]
                loadbalancer = ElasticLoadBalancingV2Discover(loadBalancingV2Service).convertToLoadBalancer(item)
                ec2Reporter = ec2.Reporter(ec2.Builder())
                vector.addAll(ec2Reporter.reportLoadBalancing(awsReporter.reportAccount(account), loadbalancer, instanceIdOshDict, zoneOshs, vpc_osh, sg_osh_list))
        except (JException, Exception), e:
                logger.warnException("Failed to do load balancer discovery, error message is:%s"%str(e))
    return vector

def _discoverLoadBalancingTopology(framework, loadBalancingService, account, credential_id, resourceDict):
    logger.info("Discover elastic classic load balancers")
    awsReporter = aws.Reporter(aws.Builder())
    vector = ObjectStateHolderVector()
    if not resourceDict['Regions']:
        raise Exception('No region found for elastic classic load balancer discovery.')
    for region in resourceDict['Regions']:
        try:
            loadBalancingService.setEndpoint(region.getEndpointHostName().replace('ec2', 'elasticloadbalancing'))
            items = ElasticLoadBalancingDiscover(loadBalancingService).getLoadBalancers()
            for item in items:
                # link to zone
                zoneOshs = []
                for zoneName in item.getAvailabilityZones():
                    zoneOsh = awsReporter.reportAvailabilityZoneByName(zoneName)
                    zoneOshs.append(zoneOsh)

                vpc_osh = vpcById[item.getVPCId()].getOsh()
                sg_osh_list = []
                if item.getSecurityGroups():
                    sg_osh_list = [security_group_dict[i] for i in item.getSecurityGroups()]
                loadbalancer = ElasticLoadBalancingDiscover(loadBalancingService).convertToLoadBalancer(item)
                ec2Reporter = ec2.Reporter(ec2.Builder())
                vector.addAll(ec2Reporter.reportLoadBalancing(awsReporter.reportAccount(account), loadbalancer, instanceIdOshDict, zoneOshs, vpc_osh, sg_osh_list))
        except (JException, Exception), e:
                logger.warnException("Failed to do load balancer discovery, error message is:%s"%str(e))
    return vector

instanceIdOshDict = {}
vpcById = {}
security_group_dict = {}

def _discoverEc2Topology(framework, ec2Service, account, credential_id, resourceDict):
    r'@types: Framework, Ec2Service, aws.Account'

    logger.info('Discover REGIONS')
    # first of all get information about available Regions and Availability Zones
    regions = []
    if credential_id:
        endpoints = framework.getProtocolProperty(credential_id, "ec2_endpoint", "")
        if endpoints:
            endpointList = endpoints.split(',')
            for endpoint in endpointList:
                if endpoint:
                    pattern = "ec2\.(.*)\.amazonaws\.com"
                    match = re.search(pattern, endpoint)
                    if match:
                        name = match.group(1)
                        region = aws.Region(name, endpoint)
                        regions.append(region)
                    else:
                        logger.warn("Can not get region in ec2Endpoint: ", endpoint)
        else:
            logger.debug("No proper ec2_endpoint defined in protocol:", credential_id)

    if not regions:
        regions = _discoverRegions(ec2Service)

    logger.debug("regions:", regions)
    resourceDict['Regions'].extend(regions)

    # get information about running instances in our account
    vector = ObjectStateHolderVector()
    for region in regions:
        try:
            # Discovery collects instances information from each available region.
            # So we establish connection to
            # each of them.
            ec2Service.setEndpoint(region.getEndpointHostName())
            _discoverZonesByRegion(ec2Service, region)
            vpcs = _discoverVpc(ec2Service)
            sgs = _discovery_vpc_sg(ec2Service)
            subnets = _discoverSubnet(ec2Service)
            instances = _discoverRunningEc2AmiInstances(ec2Service)

            # every instance has mapped devices (currently we are interested in EBS)
            # to get more detailed information about volumes we have to gather all
            # their uniq IDs
            def getInstanceEbsIds(instance):
                r'@types: ec2.Ami.Instance -> list[str]'
                return map(lambda e: e.getVolume().getId(), instance.getMappedDevices())

            ids = _applySet(_toItself, iteratortools.flatten(map(getInstanceEbsIds, instances)))

            # get all EBS volumes
            ebsItems = ec2Service.describeVolumes().getVolumes() or ()
            ec2discover = Ec2Discoverer(ec2Service)
            volumes = map(ec2discover.convertToEc2Ebs, ebsItems)

            logger.debug(str(volumes))
            # having volumes on hands we can get information about corresponding

            # snapshots by IDs so again - gather unique IDs
            ids = filter(None, _applySet(aws_store.Ebs.getSnapshotId, volumes))
            snapshots = ids and map(_partialFunc(_discoverVolumeSnapshotById, ec2Service, _), ids) or ()
            snapshots = filter(None, snapshots)
            logger.debug(str(snapshots))
            logger.info("Discovered %s snapshots" % len(snapshots))

            # Get images for the running instances by IDs
            # gather unique IDs
            ids = _applySet(ec2.Ami.Instance.getImageId, instances)

            # discover AMIs by IDs
            amis = ids and _discoverEc2AmisByIds(ec2Service, ids) or ()
            # for further lookup create mapping of AMI to ID
            amiById = _applyMapping(ec2.Ami.getId, amis)
            instancesByAmiId = _groupBy(ec2.Ami.Instance.getImageId, instances)
            # Discover available elastic IPs and group them by instance id to which they
            # belong. Before grouping performing filtering by non-empty instance ID
            ec2Discoverer = Ec2Discoverer(ec2Service)
            elasticIpsByInstanceId = _groupBy(ec2.ElasticIp.getInstanceId,
                                      filter(ec2.ElasticIp.getInstanceId,
                                        warnException(ec2Discoverer.getElasticIps,
                                            (), message="Failed to get elastic IPs")()))

            logger.info('REPORT DATA')

            try:
                # First of all we have to prepare reporters for each domain
                awsReporter = aws.Reporter(aws.Builder())
                ec2Reporter = ec2.Reporter(ec2.Builder())
                storeReporter = aws_store.Reporter(aws_store.Builder())
                # report Account information
                vector.add(awsReporter.reportAccount(account))
                # report vpc
                for vpc in vpcs:
                    try:
                        vector.addAll(awsReporter.reportVpc(vpc, account))
                        vpcById[vpc.getId()] = vpc
                    except Exception:
                        logger.warnException("Failed to report %s" % vpc)
                # mapping of built availability zone to its name
                zoneByName = {}
                # Regions and corresponding availability zones
                try:
                    vector.addAll(awsReporter.reportRegion(region, account))
                except Exception:
                    logger.warnException("Failed to report %s" % region)
                else:
                    for zone in region.getZones():
                        try:
                            vector.addAll(awsReporter.reportAvailabilityZoneInRegion(region, zone))
                            zoneByName[zone.getName()] = zone
                        except Exception:
                            logger.warnException("Failed to report %s" % zone)

                volumeById = _applyMapping(aws_store.Ebs.getId, volumes)
                # group snapshots by volume ID
                snapshotsById = _groupBy(aws_store.Ebs.Snapshot.getId, snapshots)
                # report running Instances with mapped devices, IPs and configurations
                # report instances and try to get related AMI detailed information if available
                for ebs in volumes:
                    ebs.arn = 'arn:aws:ec2:' + region.getName() + ':' + account.getId() + ':' + 'volume/' + ebs.getId()
                    ebsOsh = storeReporter.reportEbs(ebs)
                    vector.add(ebsOsh)
                    # report link between availability zone and EBS
                    zoneName = ebs.getAvailabilityZoneName()
                    if zoneByName.has_key(zoneName):
                        zoneOsh = zoneByName[zoneName].getOsh()
                        vector.add(storeReporter.linkMappedVolumeToAvailabilityZone(ebsOsh, zoneOsh))

                logger.debug('Starting build Security Group:', region)
                for sg in sgs:
                    if vpcById.get(sg.vpc_id):
                        vpc_osh = vpcById[sg.vpc_id].getOsh()
                        sg_osh = awsReporter.report_sg(sg, vpc_osh)
                        vector.add(sg_osh)
                        security_group_dict[sg.group_id] = sg_osh

                subnetsById = {}
                for subnet in subnets:
                    subnet_osh = None
                    if vpcById.has_key(subnet.getVpcId()):
                        vpcOsh = vpcById[subnet.getVpcId()].getOsh()
                        vpcOsh.setAttribute('amazon_resource_name', 'arn:aws:ec2:' + region.getName() + ':' + account.getId() + ':vpc/' + subnet.getVpcId())
                        subnet_osh = ec2Reporter.reportSubnet(subnet, vpcOsh)
                        # subnet_osh.setAttribute('amazon_resource_name', 'arn:aws:ec2:' + region.getName() + ':' + account.getId() + ':subnet/' + subnet.getId())
                        vector.add(subnet_osh)
                    zoneName = subnet.getAvailabilityZone()
                    if zoneByName.has_key(zoneName):
                        zoneOsh = zoneByName[zoneName].getOsh()
                        vector.add(ec2Reporter.linkSubnetToAvailabilityZone(subnet_osh, zoneOsh))
                    subnetsById[subnet.getId()] = subnet

                for amiId in instancesByAmiId.keys():
                    for instance in instancesByAmiId.get(amiId):
                        try:
                            # report AMI instances
                            ami = amiById.get(amiId)
                            if ami:
                                ami.region = region
                            instanceVector, nodeOsh = ec2Reporter.reportAmiInstance(account, ami, instance)
                            instanceIdOshDict[instance.getId()] = nodeOsh
                            vector.addAll(instanceVector)
                            # report link to the availability zone
                            zone = zoneByName.get(instance.availabilityZoneName)
                            if zone and zone.getOsh():
                                vector.addAll(ec2Reporter.linkAmiInstanceToAvailabilityZone(instance, zone.getOsh(), ami))
                            else:
                                logger.warn("Failed to find zone %s for %s" % (
                                    instance.availabilityZoneName,
                                    instance))
                            # report mapped devices
                            devices = instance.getMappedDevices()
                            logger.info("Report mapped devices (%s) for instance %s" %
                                        (len(devices), instance.getId()))
                            containerOsh = ec2Reporter.buildInstanceNode(instance, ami)
                            resourceDict['Ec2Intances'][instance.getId()] = containerOsh

                            # report elastic IP as usual public IP address in AWS account
                            for elasticIp in elasticIpsByInstanceId.get(instance.getId(), ()):
                                vector.addAll(ec2Reporter.reportPublicIpAddress(account, elasticIp.getIp(), containerOsh))

                            for interface in instance.getInterfaces():
                                interface = ec2.NetworkInterface(interface.getNetworkInterfaceId(), interface.getMacAddress(), interface.getDescription())
                                interface_osh = ec2Reporter.reportNetworkInterface(interface, nodeOsh)
                                vector.add(interface_osh)

                            for mappedVolume in devices:
                                volume = volumeById.get(mappedVolume.getVolume().getId())
                                if volume:
                                    mappedVolume = aws_store.MappedVolume(mappedVolume.getName(), volume)
                                volumeOsh = storeReporter.reportMappedVolume(mappedVolume, containerOsh)
                                vector.add(volumeOsh)
                                # report link between EBS and logical volume
                                vector.add(storeReporter.linkEbsToMappedVolume(mappedVolume.getVolume().getOsh(), volumeOsh))
                                # report link between availability zone and EBS(logical volume)
                                zoneName = mappedVolume.getVolume().getAvailabilityZoneName()
                                if zoneByName.has_key(zoneName):
                                    zoneOsh = zoneByName[zoneName].getOsh()
                                    vector.add(storeReporter.linkMappedVolumeToAvailabilityZone(volumeOsh, zoneOsh))
                                # report related snapshots if exist
                                volumeSnapshots = snapshotsById.get(volume.getSnapshotId()) or ()
                                logger.info("Report %s snapshots for the mapped volume %s" %
                                            (len(volumeSnapshots), volume.getId()))
                                for snapshot in volumeSnapshots:
                                    snapshotOsh = storeReporter.reportSnapshot(snapshot, account.getOsh())
                                    vector.add(snapshotOsh)
                                    vector.add(storeReporter.linkSnapshotAndMappedVolume(snapshotOsh, volumeOsh))
                            vpc = vpcById.get(instance.getVpcId())
                            vector.add(ec2Reporter.linkVpcToAmiInstance(vpc.getOsh(), containerOsh))
                            subnet = subnetsById.get(instance.getSubnetId())
                            vector.add(ec2Reporter.linkSubnetToAmiInstance(subnet.getOsh(), containerOsh))

                            sg_osh_list = [security_group_dict[i] for i in instance.security_group]
                            vector.addAll(ec2Reporter.link_sg_instance(sg_osh_list, containerOsh))

                        except Exception:
                            logger.warnException("Failed to report %s" % instance)

            except (JException, Exception):
                logger.warnException("Failed to report topology")
        except (JException, Exception), e:
            logger.warnException("Failed to do ec2 discovery from region %s: %s" % (region, str(e)))
    return vector

# === Connect methods ===

# First of all each service has different connect mechanism that is extracted
# to specific method.


def _connectToRdsService(framework, credentialsId):
    """Connection method for the **Relational Database Service**
     @types: Framework, str -> RdsClient
    """
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import ServiceType
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import Client

    properties = Properties()
    properties.setProperty(Client.AWS_SERVICE_TYPE_PROPERTY, ServiceType.RDS.name()) #@UndefinedVariable
    properties.setProperty('credentialsId', credentialsId)
    return framework.createClient(properties).getService()

def _connectToLoadBalancingService(framework, credentialsId):
    """Connection method for the **Elastic Cloud Service**
     @types: Framework, str -> Ec2Client
    """
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import ServiceType
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import Client

    properties = Properties()
    properties.setProperty(Client.AWS_SERVICE_TYPE_PROPERTY, ServiceType.ELASTIC_LOAD_BALANCING.name()) #@UndefinedVariable
    properties.setProperty('credentialsId', credentialsId)
    return framework.createClient(properties).getService()

def _connectToLoadBalancingV2Service(framework, credentialsId):
    """Connection method for the **Elastic Cloud Service**
     @types: Framework, str -> Ec2Client
    """
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import ServiceType
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import Client

    properties = Properties()
    properties.setProperty(Client.AWS_SERVICE_TYPE_PROPERTY, ServiceType.ELASTIC_APP_LOAD_BALANCING.name()) #@UndefinedVariable
    properties.setProperty('credentialsId', credentialsId)
    return framework.createClient(properties).getService()

def _connectToEc2Service(framework, credentialsId):
    """Connection method for the **Elastic Cloud Service**
     @types: Framework, str -> Ec2Client
    """
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import ServiceType
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import Client

    properties = Properties()
    properties.setProperty(Client.AWS_SERVICE_TYPE_PROPERTY, ServiceType.EC2.name()) #@UndefinedVariable
    properties.setProperty('credentialsId', credentialsId)
    return framework.createClient(properties).getService()

def _connectToSTS(framework, credentialsId):
    """Connection method for the **Security Token**
     @types: Framework, str -> SecurityTokenClient
    """
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import ServiceType
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import Client

    properties = Properties()
    properties.setProperty('credentialsId', credentialsId)
    properties.setProperty(Client.AWS_SERVICE_TYPE_PROPERTY, ServiceType.STS.name()) #@UndefinedVariable
    return framework.createClient(properties).getService()

def _ConnectToSSMService(framework, credentialsId):
    """Connection metho for the SMS service
     @types: Framework, str -> SMS client"
    """
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import ServiceType
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import Client

    properties = Properties()
    properties.setProperty('credentialsId', credentialsId)
    properties.setProperty(Client.AWS_SERVICE_TYPE_PROPERTY, ServiceType.SSM.name()) #@UndefinedVariable
    return framework.createClient(properties).getService()

def _connectToIamService(framework, credentialsId):
    """Connection method for the **Identity and Access Management**
     @types: Framework, str -> IamClient
    """
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import ServiceType
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import Client

    properties = Properties()
    properties.setProperty('credentialsId', credentialsId)
    properties.setProperty(Client.AWS_SERVICE_TYPE_PROPERTY, ServiceType.IAM.name()) #@UndefinedVariable
    return framework.createClient(properties).getService()

def _connectToEcsService(framework, credentialsId):
    """Connection method for the **EC2 Container Service**
     @types: Framework, str -> EcsClient
    """
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import ServiceType
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import Client

    properties = Properties()
    properties.setProperty('credentialsId', credentialsId)
    properties.setProperty(Client.AWS_SERVICE_TYPE_PROPERTY, ServiceType.ECS.name()) #@UndefinedVariable
    return framework.createClient(properties).getService()

def _connectToEcrService(framework, credentialsId):
    """Connection method for the **EC2 Container Service**
     @types: Framework, str -> EcrClient
    """
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import ServiceType
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import Client

    properties = Properties()
    properties.setProperty('credentialsId', credentialsId)
    properties.setProperty(Client.AWS_SERVICE_TYPE_PROPERTY, ServiceType.ECR.name()) #@UndefinedVariable
    return framework.createClient(properties).getService()

def _connectToS3Service(framework, credentialsId):
    """Connection method for the **S3 Service**
        @types: Framework, str -> S3Client
    """
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import ServiceType
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import Client

    properties = Properties()
    properties.setProperty('credentialsId', credentialsId)
    properties.setProperty(Client.AWS_SERVICE_TYPE_PROPERTY, ServiceType.S3.name())  # @UndefinedVariable
    return framework.createClient(properties).getService()

def _connectToASGService(framework, credentialsId):
    """Connection method for the **ASG Service**
        @types: Framework, str -> ASGClient
    """
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import ServiceType
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import Client

    properties = Properties()
    properties.setProperty('credentialsId', credentialsId)
    properties.setProperty(Client.AWS_SERVICE_TYPE_PROPERTY, ServiceType.ASG.name())  # @UndefinedVariable
    return framework.createClient(properties).getService()


# === Gateway API Service Discoverer ===
def _connectToApi_GatewayService(framework, credentialId):
    """ connection methos for API Gateway service """

    from com.hp.ucmdb.discovery.library.clients.cloud.aws import ServiceType
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import Client
    properties = Properties()
    properties.setProperty('credentialsId', credentialId)
    properties.setProperty(Client.AWS_SERVICE_TYPE_PROPERTY, ServiceType.API_GATEWAY.name())
    return framework.createClient(properties).getService()


def _connectToCloudFormationService(framework, credentialsId):
    """Connection method for the **CloudFormation Service**
        @types: Framework, str -> CloudFormationClient
    """
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import ServiceType
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import Client

    properties = Properties()
    properties.setProperty('credentialsId', credentialsId)
    properties.setProperty(Client.AWS_SERVICE_TYPE_PROPERTY, ServiceType.CLOUD_FORMATION.name())  # @UndefinedVariable
    return framework.createClient(properties).getService()


def _connectToLambdaService(framework, credentialsId):
    """Connection method for the **Lambda Service**
        @types: Framework, str -> LambdaClient
    """
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import ServiceType
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import Client

    properties = Properties()
    properties.setProperty('credentialsId', credentialsId)
    properties.setProperty(Client.AWS_SERVICE_TYPE_PROPERTY, ServiceType.LAMBDA.name())  # @UndefinedVariable
    return framework.createClient(properties).getService()

def _connect_to_sns_service(framework, credentials_id):
    """Connection method for the **SNS Service**
        @types: Framework, str -> SNSClient
    """
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import ServiceType
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import Client

    properties = Properties()
    properties.setProperty('credentialsId', credentials_id)
    properties.setProperty(Client.AWS_SERVICE_TYPE_PROPERTY, ServiceType.SNS.name())  # @UndefinedVariable
    return framework.createClient(properties).getService()

def _connect_to_sqs_service(framework, credentials_id):
    """Connection method for the **SNS Service**
        @types: Framework, str -> SNSClient
    """
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import ServiceType
    from com.hp.ucmdb.discovery.library.clients.cloud.aws import Client

    properties = Properties()
    properties.setProperty('credentialsId', credentials_id)
    properties.setProperty(Client.AWS_SERVICE_TYPE_PROPERTY, ServiceType.SQS.name())  # @UndefinedVariable
    return framework.createClient(properties).getService()

# === Security Token Service Discoverer ===
class STSDiscoverer:
    """ Discoverer that play role of single point of access to the Security Token Service"""

    def __init__(self, service):
        self._service = service

    def getAccount(self):
        from com.amazonaws.services.securitytoken.model import GetCallerIdentityRequest
        request = GetCallerIdentityRequest()
        callerIdentity = self._service.getCallerIdentity(request)
        # get Amazon Resource Name (ARN)
        # arn:aws:iam::846188145964:user/root
        user_name = None
        arn = callerIdentity.getArn()
        tokens = arn.split(':')

        if len(tokens) > 5:
            user = tokens[5].split("/")
            if len(user) > 1:
                user_name = user[1]
        return str(callerIdentity.getAccount()), str(user_name)

# === Aim Discoverer ===

class IamDiscoverer:
    """ Discoverer that play role of single point of access to the Identity and
     Accessibility Management
    """

    def __init__(self, service):
        self._service = service

    def getAccount(self):
        """ Get account ID using ARN
        You specify the resource using the following Amazon Resource Name (ARN)
        format: arn:aws:<vendor>:<region>:<namespace>:<relative-id>

        * **vendor** identifies the AWS product (e.g., sns)
        * **region** is the AWS Region the resource resides in (e.g., us-east-1), if any
        * **namespace** is the AWS account ID with no hyphens (e.g., 123456789012)
        * **relative-id** is the service specific portion that identifies the specific resource

        @types: AimService -> str
        @raise ValueError: Wrong ARN format
        """
        result = self._service.getUser()
        # get Amazon Resource Name (ARN)
        # arn:aws:iam::846188145964:root
        arn = result.getUser().getArn()
        tokens = arn.split(':')
        # 4th token is ID
        if len(tokens) > 3:
            return str(tokens[4]), str(result.getUser().getUserName())
        raise ValueError("Wrong ARN format")


# === Classic Load balancer Discoverer ===
class ElasticLoadBalancingV2Discover:
    def __init__(self, service):
        self._service = service

    def getAppLoadBalancers(self):
        from com.amazonaws.services.elasticloadbalancingv2.model import DescribeLoadBalancersRequest
        request = DescribeLoadBalancersRequest()
        lbs = self._service.describeLoadBalancers(request).getLoadBalancers()
        lbDetails = []
        for lb in lbs:
            item = []
            loadBalancerArn = lb.getLoadBalancerArn()
            listeners = self.getListeners(loadBalancerArn)
            targetGroup = self.getTargetGroup(loadBalancerArn)
            lbTargetGroupArn = targetGroup.getTargetGroupArn()
            logger.debug('lbTargetGroup', lbTargetGroupArn)
            instances = self.getTargets(lbTargetGroupArn)
            item.append(lb)
            item.append(listeners)
            item.append(targetGroup)
            item.append(instances)
            lbDetails.append(item)

            from com.amazonaws.services.elasticloadbalancingv2.model import DescribeTagsRequest
            tagRequest = DescribeTagsRequest().withResourceArns(loadBalancerArn)
            tagsString = _getTagsAsString(self._service, tagRequest)
            item.append(tagsString)
        return lbDetails

    def getTargetGroup(self, loadBalancerArn):
        from com.amazonaws.services.elasticloadbalancingv2.model import DescribeTargetGroupsRequest
        request = DescribeTargetGroupsRequest().withLoadBalancerArn(loadBalancerArn)
        items = self._service.describeTargetGroups(request).getTargetGroups()
        return items[0]

    def getTargets(self,targetGroupArn):
        targets = set()
        from com.amazonaws.services.elasticloadbalancingv2.model import DescribeTargetHealthRequest
        request = DescribeTargetHealthRequest().withTargetGroupArn(targetGroupArn)
        targetHealthDescriptions = self._service.describeTargetHealth(request).getTargetHealthDescriptions()
        for targetHealthDescription in targetHealthDescriptions:
            target = targetHealthDescription.getTarget().getId()
            targets.add(target)
        return targets

    def getListeners(self, loadBalancerArn):
        from com.amazonaws.services.elasticloadbalancingv2.model import DescribeListenersRequest
        request = DescribeListenersRequest().withLoadBalancerArn(loadBalancerArn)
        items = self._service.describeListeners(request).getListeners()
        return items

    def getSslPolicy(self, listener):
        return listener.getSslPolicy()

    def convertToLoadBalancer(self, lbDetail):
        listeners = []
        if isinstance(lbDetail, list) and len(lbDetail) == 5:
            lb = lbDetail[0]
            appListeners = lbDetail[1]
            targetGroup = lbDetail[2]
            instances = lbDetail[3]
            tags = lbDetail[4]
            for appListener in appListeners:
                loadBalancerProtocol = appListener.getProtocol()
                loadBalancerPort = appListener.getPort()
                sslPolicy = appListener.getSslPolicy()
                instanceProtocol = targetGroup.getProtocol()
                instancePort = targetGroup.getPort()
                listeners.append(ec2.Listener(loadBalancerProtocol, loadBalancerPort, instanceProtocol, instancePort, sslPolicy))
            return ec2.LoadBalancer(lb.getLoadBalancerName(), lb.getDNSName(), instances, listeners, tags)
        else:
            raise ValueError("Load balancer detailed information not provided")

# === Application Load balancer Discoverer ===
class ElasticLoadBalancingDiscover:
    def __init__(self, service):
        self._service = service

    def getLoadBalancers(self):
        items = self._service.describeLoadBalancers().getLoadBalancerDescriptions()
        return items

    def convertToLoadBalancer(self, item):
        listenerDescriptions = item.getListenerDescriptions()
        listeners = []
        for listenerDescription in listenerDescriptions:
            listener = listenerDescription.getListener()
            loadBalancerProtocol = listener.getProtocol()
            loadBalancerPort = listener.getLoadBalancerPort()
            instanceProtocol = listener.getInstanceProtocol()
            instancePort = listener.getInstancePort()
            listeners.append(ec2.Listener(loadBalancerProtocol, loadBalancerPort, instanceProtocol, instancePort))

        from com.amazonaws.services.elasticloadbalancing.model import DescribeTagsRequest
        tagRequest = DescribeTagsRequest().withLoadBalancerNames(item.getLoadBalancerName())
        tagsString = _getTagsAsString(self._service, tagRequest)
        return ec2.LoadBalancer(item.getLoadBalancerName(), item.getDNSName(), item.getInstances(), listeners, tagsString)

# === EC2 Discoverer ===

class Ec2Discoverer:
    def __init__(self, service):
        self._service = service

    def _convertToRegion(self, item):
        r'@types: com.amazonaws.services.ec2.model.Region -> aws.Region'
        return aws.Region(item.getRegionName(), item.getEndpoint())

    def getRegions(self):
        r'@types: -> list[aws.Region]'
        results = self._service.describeRegions().getRegions()
        return map(self._convertToRegion, results)

    def _convertToAvailabilityZone(self, item):
        r'@types: com.amazonaws.services.ec2.model.AvailabilityZone -> aws.AvailabilityZone'
        return aws.AvailabilityZone(item.getZoneName(),
                             item.getRegionName(),
                             item.getState())

    def getAvailabilityZones(self):
        r'@types: -> list[aws.AvailabilityZone]'
        results = self._service.describeAvailabilityZones().getAvailabilityZones()
        return map(self._convertToAvailabilityZone, results)

    def _convertToVpcs(self, item):
        r'@types: com.amazonaws.services.ec2.model.DescribeVpcsResult -> aws.Vpc'
        vpc = aws.Vpc(item.getVpcId(),
                      item.getIsDefault(),
                      item.getState(),
                      item.getCidrBlock())
        try:
            for ipv4_set in item.getCidrBlockAssociationSet():
                list = []
                ipv4_cidr = ipv4_set.getCidrBlock()
                if ipv4_cidr:
                    list.append(ipv4_cidr)
                    vpc.setIpv4CidrBlock(list)
        except Exception, ex:
            logger.debug("Failed to getCidrBlockAssociationSet: ", ex)

        for ipv6_set in item.getIpv6CidrBlockAssociationSet():
            list = []
            ipv6_cidr = ipv6_set.getIpv6CidrBlock()
            if ipv6_cidr:
                list.append(ipv6_cidr)
                vpc.setIpv6CidrBlock(list)

        if item.getTags():
            for tag in item.getTags():
                if tag.getKey() == 'Name':
                    vpc.setName(tag.getValue())

        return vpc

    # convert to security group object
    def _convertToSGs(self, item):
        security_group = aws.SecurityGroup(item.getVpcId(), item.getGroupId(), item.getGroupName(), item.getIpPermissions(), item.getIpPermissionsEgress())
        return security_group

    def _convertToSubnets(self, item):
        r'@types: com.amazonaws.services.ec2.model.DescribeSubnetsResult -> ec2.Subnet'
        subnet = ec2.Subnet(item.getSubnetId(),
                            item.getVpcId(),
                            item.getAvailabilityZone(),
                            item.getAvailableIpAddressCount(),
                            item.getCidrBlock())

        for ipv6_set in item.getIpv6CidrBlockAssociationSet():
            list = []
            ipv6_cidr = ipv6_set.getIpv6CidrBlock()
            if ipv6_cidr:
                list.append(ipv6_cidr)
                subnet.setIpv6CidrBlock(list)

        if item.getTags():
            for tag in item.getTags():
                if tag.getKey() == 'Name':
                    subnet.setName(tag.getValue())

        return subnet

    def _convertToNetworkInterfaces(self, item):
        r'@types: com.amazonaws.services.ec2.model.DescribeNetworkInterfacesResult -> ec2.NetworkInterface'
        return ec2.NetworkInterface(item.getNetworkInterfaceId(),
                            item.getMacAddress(),
                            item.getDescription())

    def getVpcs(self):
        results = self._service.describeVpcs().getVpcs()
        return map(self._convertToVpcs, results)

    # get security group
    def get_security_groups(self):
        results = self._service.describeSecurityGroups().getSecurityGroups()
        return map(self._convertToSGs, results)

    def getSubnet(self):
        results = self._service.describeSubnets().getSubnets()
        return map(self._convertToSubnets, results)


    def convertToEc2Ami(self, item):
        r'@types: com.amazonaws.services.ec2.model.Image -> ec2.Ami'
        ami = ec2.Ami(item.getName(), item.getImageId(),
                      description=item.getDescription(),
                      createDate=item.getCreationDate(),
                      virtualizationType=item.getVirtualizationType(),
                      imageStatus=item.getState(),
                      imageType=item.getImageType(),
                      rootDeviceType=item.getRootDeviceType(),
                      platform=item.getPlatform())
        return ami.withVisibility(item.getPublic())

    def getAmisByIds(self, ids):
        r'@types: list[str] -> ec2.Ami'
        from com.amazonaws.services.ec2.model import DescribeImagesRequest
        request = DescribeImagesRequest().withImageIds(_toArrayList(ids))
        items = self._service.describeImages(request).getImages() or ()
        return map(self.convertToEc2Ami, items)

    def _convertToAwsEbs(self, item):
        r'@types: com.amazonaws.services.ec2.model.Volume -> aws_store.Ebs'
        return aws_store.Ebs(item.getVolumeId())

    def _convertToMappedVolume(self, item):
        r'@types: InstanceBlockDeviceMapping -> aws_store.MappedVolume'
        return aws_store.MappedVolume(
                                  item.getDeviceName(),
                                  self._convertToAwsEbs(item.getEbs())
                )

    def _convertToEc2AmiInstance(self, item):
        r'@types: com.amazonaws.services.ec2.model.Instance -> ec2.Ami.Instance'
        logger.debug("Convert Instance ( %s ) to DO" % item.getInstanceId())
        placement = item.getPlacement()
        availabilityZoneName = placement and placement.getAvailabilityZone()
        publicAddress = privateAddress = None
        # public address
        if item.getPublicDnsName() and item.getPublicIpAddress():
            publicAddress = ec2.Address(item.getPublicDnsName(),
                                        item.getPublicIpAddress())
        # private address
        if item.getPrivateDnsName() and item.getPrivateIpAddress():
            privateAddress = ec2.Address(item.getPrivateDnsName(),
                                        item.getPrivateIpAddress())
        tagList = item.getTags()
        tagsString = _buildTagString(tagList)
        instance = ec2.Ami.Instance(item.getInstanceId(),
                                    item.getImageId(),
                                    item.getInstanceType(),
                                    tagsString,
                                    publicAddress,
                                    privateAddress,
                                    launchIndex=item.getAmiLaunchIndex(),
                                    keyPairName=item.getKeyName(),
                                    availabilityZoneName=availabilityZoneName,
                                    vpc_id=item.getVpcId(),
                                    subnet_id=item.getSubnetId(),
                                    interfaces=item.getNetworkInterfaces(),
                                    security_group=item.getSecurityGroups())
        # process mapped devices, if root device is EBS it will be
        # in list of mapped devices, otherwise - it is instance-store
        # it does not have name so is useless for discovery
        _apply(instance.addMappedDevice, map(self._convertToMappedVolume,
                            item.getBlockDeviceMappings()))
        return instance

    def _getInstancesByFilters(self, filters):
        r'''@types: list[com.amazonaws.services.ec2.model.Filter] -> list[ec2.Ami.Instance]'''
        # get only running instances
        from com.amazonaws.services.ec2.model import DescribeInstancesRequest
        filters = _toArrayList(filters)
        request = DescribeInstancesRequest().withFilters(filters)
        result = self._service.describeInstances(request)
        reservations = result.getReservations()
        # each reservation has list of instances we want to process
        instances = []
        for r in reservations:
            instances.extend(filter(None, map(self._convertToEc2AmiInstance, r.getInstances())))
        return instances

    def getInstancesByStatus(self, status):
        r'''@types: str -> list[ec2.Ami.Instance]'''
        from com.amazonaws.services.ec2.model import Filter
        values = _toArrayList([status])
        return self._getInstancesByFilters([Filter('instance-state-name').
                                            withValues(values)])

    def getRunningInstances(self):
        r'''@types: -> list[ec2.Ami.Instance]'''
        return self.getInstancesByStatus("running")

    def convertToEc2Ebs(self, item):
        r'@types: com.amazonaws.services.ec2.model.Volume -> aws_store.Ebs'
        sizeInGb = item.getSize()
        sizeInMb = sizeInGb and str(sizeInGb).isnumeric() and int(sizeInGb) * 1024
        return aws_store.Ebs(item.getVolumeId(),
                         volumeType=item.getVolumeType(),
                         createTime=item.getCreateTime(),
                         encrypted=item.getEncrypted(),
                         iops=item.getIops(),
                         sizeInMb=sizeInMb,
                         snapshotId=item.getSnapshotId(),
                         state=item.getState(),
                         availabilityZoneName=item.getAvailabilityZone()
                         )

    def convertToVolumeSnapshot(self, item):
        r'@types: com.amazonaws.services.ec2.model.Snapshot -> aws_store.Ebs.Snapshot'
        ebsVolume = None
        if item.getVolumeId():
            sizeInGb = item.getVolumeSize()
            sizeInMb = sizeInGb and str(sizeInGb).isnumeric() and int(sizeInGb) * 1024
            ebsVolume = aws_store.Ebs(item.getVolumeId(), sizeInMb)
        return aws_store.Ebs.Snapshot(item.getSnapshotId(),
                              volume=ebsVolume,
                              description=item.getDescription(),
                              startTime=item.getStartTime())

    def convertToEc2ElasticIp(self, item):
        r'@types: com.amazonaws.services.ec2.model.Address -> ec2.ElasticIp'
        return ec2.ElasticIp(item.getPublicIp(),
                             instanceId=item.getInstanceId())

    def getVolumeSnapshotById(self, id):
        r'''@types: str -> aws_store.Ebs.Snapshot
        @raise ValueError: No IDs specified to find corresponding snapshot
        '''
        if not id:
            raise ValueError("No ID specified to find corresponding snapshot")
        from com.amazonaws.services.ec2.model import DescribeSnapshotsRequest
        request = DescribeSnapshotsRequest().withSnapshotIds(_toArrayList([id]))
        resultItems = self._service.describeSnapshots(request).getSnapshots() or ()
        return map(self.convertToVolumeSnapshot, resultItems)[0]

    def getVolumesByIds(self, ids):
        r'''@types: list[str] -> list[aws_store.Ebs]
        @raise ValueError: No IDs specified to find corresponding volumes
        '''
        if not ids:
            raise ValueError("No IDs specified to find corresponding volumes")
        from com.amazonaws.services.ec2.model import DescribeVolumesRequest
        request = DescribeVolumesRequest().withVolumeIds(_toArrayList(ids))
        resultItems = self._service.describeVolumes(request).getVolumes() or ()
        return map(self.convertToEc2Ebs, resultItems)

    def getElasticIps(self):
        r'''@types: -> list[ec2.ElasticIp]'''
        return map(self.convertToEc2ElasticIp, self._service.describeAddresses().getAddresses() or ())

# === EC2 Installed SW Discoverer ===
class SSMDiscoverer:
    """
    Discoverer that play role of single point of access to the Identify and
    Accessibility Management
    """

    def __init__(self, service):
        self._service = service

    def getInventory(self):
        from com.amazonaws.services.simplesystemsmanagement.model import ListInventoryEntriesRequest
        request = ListInventoryEntriesRequest()

        EC2InstalledSWDict = {}

        for instanceId in instanceIdOshDict.keys():
            logger.debug("ssm instance:", instanceId)
            request.withInstanceId(instanceId)
            request.withTypeName("AWS:Application")

            ListInventoryEntries = self._service.listInventoryEntries(request)
            entries = ListInventoryEntries.getEntries()
            while ListInventoryEntries.getNextToken():
                request.withNextToken(ListInventoryEntries.getNextToken())
                ListInventoryEntries = self._service.listInventoryEntries(request)
                entries.extend(ListInventoryEntries.getEntries())
            EC2InstalledSWDict[instanceId] = entries
        return EC2InstalledSWDict

def _discoverEC2InstalledSW(framework, service, account, credential_id, resourceDict):
    discoverInstalledSW = Boolean.parseBoolean(framework.getParameter('discoverInstalledSoftware'))
    if discoverInstalledSW:
        logger.debug("Discover EC2 Installed Software")
        vector = ObjectStateHolderVector()
        if not resourceDict['Regions']:
            raise Exception('No region found for EC2 installed software discovery.')
        for region in resourceDict['Regions']:
            try:
                service.setEndpoint(region.getEndpointHostName().replace('ec2', 'ssm'))
                ssmDiscover = SSMDiscoverer(service)
                EC2InstalledSWDict = ssmDiscover.getInventory()
                for instanceId, entries in EC2InstalledSWDict.items():
                    instanceIdOsh = instanceIdOshDict[instanceId]
                    ec2Reporter = ec2.Reporter(ec2.Builder())
                    vector.addAll(ec2Reporter.reportInstalledSW(instanceIdOsh, entries))
            except (JException, Exception), e:
                logger.warnException("Failed to do EC2 Installed Software discovery, error message is:%s" % str(e))
        return vector
    else:
        logger.debug("The 'discoverInstalledSoftware' not set to true.")

def _discoverRegions(service):
    r'@types: AmazonEC2 -> list(aws.Region)'
    logger.info('Discover REGIONS')
    discoverer = Ec2Discoverer(service)
    try:
        return discoverer.getRegions()
    except JException, je:
        logger.warnException("Failed to discover regions: %s" % je )

def _discoverZonesByRegion(service, region):
    logger.info('Discover ZONES')
    discoverer = Ec2Discoverer(service)
    try:
        # map availability zones to corresponding regions
        zones = discoverer.getAvailabilityZones()
        for zone in zones:
            region.addZone(zone)
        logger.info("Discovered %s zones" % len(zones))
    except JException:
        logger.warnException("Failed to discover zones")


def _discoverVpc(service):
    logger.debug('Discover VPCS')
    discoverer = Ec2Discoverer(service)
    try:
        vpcs = discoverer.getVpcs()
        logger.info("Discovered %s vpcs" % len(vpcs))
        return vpcs
    except JException:
        logger.warnException("Failed to discover vpcs")


def _discovery_vpc_sg(service):
    logger.debug('Discover Security Group')
    discoverer = Ec2Discoverer(service)
    try:
        sgs = discoverer.get_security_groups()
        return sgs
    except Exception, e:
        logger.debug("Failed to discover security group:", str(e))


def _discoverSubnet(service):
    logger.debug('Discover Subnet')
    discoverer = Ec2Discoverer(service)
    try:
        subnets = discoverer.getSubnet()
        logger.info("Discovered %s subnets" % len(subnets))
        return subnets
    except JException:
        logger.warnException("Failed to discover subnets")

def _discoverEc2AmisByIds(service, ids):
    r'''List and describe registered AMIs and AMIs you have launch permissions for.
    The AMI parameters, if specified, are the AMIs to describe.
    The  result  set  of  AMIs  described are the intersection of the AMIs specified,
    AMIs owned by the owners specified and AMIs with launch permissions as specified
    by the executable by options.

    @types: AmazonEC2, list[str] -> list[ec2.Ami]
    '''
    logger.info('Discover AMIs by such IDs %s' % ids)
    discoverer = Ec2Discoverer(service)
    images = warnException(discoverer.getAmisByIds, [])(ids)
    logger.info("Discovered AMIs: %s" % len(images))
    return images


def _apply(fn, iterable):
    r'Combinator similar to map but ignoring fn result'
    for i in iterable:
        fn(i)


def warnException(fn, defaultValue, ex=(Exception, JException), message=None):
    r'''
    @types: callable[I, O], O, tuple, str  -> Option[O]
    '''
    def wrapped(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except ex, e:
            logger.warnException(message or str(e))
        return defaultValue
    return wrapped


def _discoverRunningEc2AmiInstances(service):
    r'''
    @types: AmazonEC2  -> list(ec2.Ami.Instance)
    '''
    logger.info("Discover running instances")
    discoverer = Ec2Discoverer(service)
    instances = warnException(discoverer.getRunningInstances, [],
                              "Failed to discover instances")()
    # filter instances with public and private addresses
    instances = filter(lambda i: i.publicAddress or i.privateAddress, instances)
    logger.info("Discovered %s instances" % len(instances))
    return instances


class RdsDiscoverer:
    def __init__(self, service):
        self._service = service

    def _convertToTcpEndpoint(self, item):
        r'@types: com.amazonaws.services.rds.model.Endpoint -> netutils.Endpoint'
        return netutils.Endpoint(item.getPort(),
                                 netutils.ProtocolType.TCP_PROTOCOL,
                                 item.getAddress())

    def _convertToParameterGroupStatus(self, item):
        r'''@types: com.amazonaws.services.rds.model.DBParameterGroupStatus -> aws_rds.ParameterGroupStatus
        '''
        return aws_rds.ParameterGroup(item.getDBParameterGroupName(),
                                            status = item.getParameterApplyStatus())

    def _convertToSecurityGroupMembership(self, item):
        r'''@types: com.amazonaws.services.rds.model.DBSecurityGroupMembership -> aws_rds.SecurityGroupMembership
        '''
        return item.getVpcSecurityGroupId()

    def _converttoDbInstance(self, item):
        r'''@types: com.amazonaws.services.rds.model.DBInstance -> aws_rds.Instance

        getDBParameterGroups()#rovides the list of DB Parameter Groups applied to this DB Instance.
            com.amazonaws.services.rds.model.DBParameterGroupStatus
            getDBParameterGroupName() # The name of the DP Parameter Group.
            getParameterApplyStatus() # The status of parameter updates.
        getDBSecurityGroups() # Provides List of DB Security Group elements containing only DBSecurityGroup.Name and DBSecurityGroup.Status subelements.
            com.amazonaws.services.rds.model.DBSecurityGroupMembership
            getDBSecurityGroupName()
            getStatus()
        '''

        dbName = item.getDBName()
        platform = db_platform.findPlatformBySignature(item.getEngine())
        vendor = platform and platform.vendor
        databases = dbName and [db.Database(dbName)]
        endpoint = self._convertToTcpEndpoint(item.getEndpoint())
        server = db.DatabaseServer(endpoint.getAddress(),
                                   endpoint.getPort(),
                                   databases=databases,
                                   vendor=vendor,
                                   version=item.getEngineVersion(),
                                   platform=platform)

        sizeInGb = item.getAllocatedStorage()
        sizeInMb = ((sizeInGb and str(sizeInGb).isnumeric)
                        and sizeInGb * 1024
                        or None)
        return aws_rds.Instance(item.getDBInstanceIdentifier(),
                                server,
                                type=item.getDBInstanceClass(),
                                status=item.getDBInstanceStatus(),
                                licenseModel=item.getLicenseModel(),
                                sizeInMb=sizeInMb,
                                availabilityZoneName=item.getAvailabilityZone(),
                                creationTime=item.getInstanceCreateTime(),
                                engineName=item.getEngine(),
                                parameterGroups=map(self._convertToParameterGroupStatus,
                                                      item.getDBParameterGroups()),
                                securityGroups=map(self._convertToSecurityGroupMembership,
                                                     item.getVpcSecurityGroups()),
                                dbName=dbName,
                                arn=item.getDBInstanceArn()
                                )

    def _convertToDbEngine(self, item):
        r'@types: com.amazonaws.services.rds.model.DBEngineVersion -> aws_rds.Engine'
        return aws_rds.Engine(item.getEngine(),
                              version=item.getEngineVersion(),
                              versionDescription=item.getDBEngineVersionDescription(),
                              description=item.getDBEngineDescription())

    def _convertToParameterGroup(self, item):
        r'@types: com.amazonaws.services.rds.model.DBParameterGroup -> aws_rds.ParameterGroup'
        return aws_rds.ParameterGroup(item.getDBParameterGroupName(),
                                      description=item.getDescription(),
                                      family=item.getDBParameterGroupFamily())

    def _convertToDbSnapshot(self, item):
        r'@types: com.amazonaws.services.rds.model.DBSnapshot -> aws_rds.Snapshot'
        sizeInGb = item.getAllocatedStorage()
        sizeInMb = ((sizeInGb and str(sizeInGb).isnumeric)
                        and sizeInGb * 1024
                        or None)
        # create database server
        platform = db_platform.findPlatformBySignature(item.getEngine())
        vendor = platform and platform.vendor
        server = db.DatabaseServer(port=item.getPort(), vendor=vendor,
                                   versionDescription=item.getEngineVersion())
        # create DB Instance based on server
        instance = aws_rds.Instance(item.getDBInstanceIdentifier(), server,
                         licenseModel=item.getLicenseModel(),
                         sizeInMb=sizeInMb,
                         availabilityZoneName=item.getAvailabilityZone(),
                         creationTime=item.getInstanceCreateTime())
        # create DB Snapshot based on instance
        return aws_rds.Snapshot(item.getDBSnapshotIdentifier(), instance,
                                creationTime=item.getSnapshotCreateTime(),
                                status=item.getStatus()
                                )

    def getDbInstances(self):
        r'@types: -> list[aws_rds.Instance]'
        # logger.debug('DBClusters:',self._service.describeDBClusters())
        try:
            DBlist = self._service.describeDBInstances().getDBInstances()
            logger.info('Get DBinstances:', str(DBlist))
            return map(self._converttoDbInstance, DBlist)
        except (JException, Exception), e:
            logger.warnException('Cannot found DBinstance:', str(e))
            return map(self._converttoDbInstance, ())

    def getEngines(self):
        r'@types: -> list[aws_rds.Engine]'
        return map(self._convertToDbEngine, self._service.describeDBEngineVersions().getDBEngineVersions() or ())

    def getParameterGroups(self):
        r'@types: -> list[aws_rds.ParameterGroup]'
        return map(self._convertToParameterGroup, self._service.describeDBParameterGroups().getDBParameterGroups() or ())

    def getSnapshots(self):
        r'@types: -> list[db.Snapshot]'
        return map(self._convertToDbSnapshot, self._service.describeDBSnapshots().getDBSnapshots() or ())


def _discoverRdsTopology(framework, service, account, credential_id, resourceDict):
    r''' Discover topology of Amazon Relational Database Service
    @types: Framework, AwsRdsService, aws.Account
    @param service:  Client for accessing AmazonRDS. All service calls made using
    this client are blocking, and will not return until the service call completes
    '''
    vector = ObjectStateHolderVector()
    endpoints = []
    dataflowprobe = framework.getDestinationAttribute('probeName')
    if credential_id:
        endpoint = framework.getProtocolProperty(credential_id, "rds_endpoint", "")
        if endpoint:
            pattern = "(.*)\.amazonaws\.com"
            match = re.search(pattern, endpoint)
            if match:
                endpoint = match.group(0)
                endpoints.append(endpoint)
            else:
                logger.debug('RDS Endpiont is invalid')
    if not endpoints:
        from com.amazonaws.regions import RegionUtils
        regions = RegionUtils.getRegionsForService(service.ENDPOINT_PREFIX)
        for region in regions:
            endpoint = region.getServiceEndpoint(service.ENDPOINT_PREFIX)
            endpoints.append(endpoint)
    for endpoint in endpoints:
        service.setEndpoint(endpoint)
        discoverer = RdsDiscoverer(service)
        try:
            instances = discoverer.getDbInstances()
            if len(instances) == 0:
                continue

            logger.info("Get DB engine information")
            engines = warnException(discoverer.getEngines, [],
                                    message="Failed to get DB engine versions")()
            logger.debug("Got %s engines" % len(engines))
            # To enrich information about DB instance parameter groups where only name and
            # status are available we make additional request to get all db parameters
            # in this account
            logger.info("Get parameter groups")
            parameterGroups = warnException(discoverer.getParameterGroups, [],
                                            message="Failed to get parameter groups")()
            # Grouping DB parameter groups by name for further reporting needs
            paramGroupByName = _applyMapping(aws_rds.ParameterGroup.getName, parameterGroups)
            logger.debug("Got %s parameter groups" % len(parameterGroups))

            logger.info("Get DB snapshots")
            # User can make snapshots of existing instances. Get information about snapshots
            snapshots = warnException(discoverer.getSnapshots, [],
                                      message="Failed to get DB snapshots")()
            logger.debug("Got %s DB snapshots" % len(snapshots))
            # group snapshots by instance ID
            snapshotsByInstanceId = _groupBy(aws_rds.Snapshot.getInstanceId, snapshots)
            logger.info("RDS TOPOLOGY REPORTING")
            awsReporter = aws.Reporter(aws.Builder())
            endpointReporter = netutils.EndpointReporter(netutils.UriEndpointBuilder())
            # Report account OSH
            accountOsh = awsReporter.reportAccount(account)
            vector.add(accountOsh)
            # more functional approach can be ...
            # _reportDbInstance = _partialFunc(reportDbInstance, _, awsReporter, accountOsh)
            # map(vector.addAll, map(_reportDbInstance, instances))
            for instance in instances:
                dbServer = instance.getServer()
                # get more information about server using engine information
                # we get engine by its name and version
                for engine in engines:
                    if (engine.getName() == instance.getEngineName()
                            and engine.getVersion() == dbServer.getVersion()):
                        # recreate server with additional information
                        dbServer = db.DatabaseServer(dbServer.address,
                                                     dbServer.getPort(),
                                                     dbServer.instance,
                                                     dbServer.getDatabases(),
                                                     dbServer.vendor,
                                                     engine.getVersionDescription(),
                                                     dbServer.getPlatform(),
                                                     dbServer.getVersion(),
                                                     engine.getDescription())
                        break

                platform = (dbServer.getPlatform()
                            or db_platform.findPlatformBySignature(dbServer.vendor))
                dbReporter = db.TopologyReporter(db_builder.getBuilderByPlatform(platform))
                rdsReporter = aws_rds.Reporter(aws_rds.Builder())
                instanceOsh = rdsReporter.reportDbService(instance)
                instanceOsh.setAttribute('service_probename', dataflowprobe)
                vector.add(instanceOsh)
                vector.add(rdsReporter.linkAccountWithInstanceNode(accountOsh, instanceOsh))
                # membership link between instance node + availability zone
                zoneName = instance.getAvailabilityZoneName()
                if zoneName:
                    zoneOsh = awsReporter.reportAvailabilityZoneByName(zoneName)
                    vector.add(zoneOsh)
                    vector.add(rdsReporter.linkZoneWithInstanceNode(zoneOsh, instanceOsh))

                # report database with security group
                if instance.getSecurityGroups():
                    for sg_id in instance.getSecurityGroups():
                        vector.add(rdsReporter.link_instance_sg(instanceOsh, security_group_dict[sg_id]))
                # report endpoint
                endpoint = netutils.Endpoint(dbServer.getPort(),
                                             netutils.ProtocolType.TCP_PROTOCOL,
                                             dbServer.address)

                # reporting of parameter and security groups
                # link with parameter groups
                for group in instance.getParameterGroups():
                    if paramGroupByName.has_key(group.getName()):
                        group = paramGroupByName.get(group.getName())
                    else:
                        logger.warn("Failed to find %s for %s" % (group, instance))
                    configOsh = rdsReporter.reportParameterGroupConfig(group, accountOsh)
                    vector.add(configOsh)
                    vector.add(rdsReporter.linkInstanceWithGroupConfig(instanceOsh, configOsh))

                # report DB snapshot
                for snapshot in snapshotsByInstanceId.get(instance.getId(), ()):
                    dbSnapshot = db.Snapshot(snapshot.getId(), ownerName=account.getId())
                    vector.add(dbReporter.reportSnapshot(dbSnapshot, instanceOsh))
        except:
            logger.debug("Failed to discover DBinstance in endpoint: %s" % endpoint)
    return vector


def _discoverVolumeSnapshotById(service, id_):
    r'''@types: Ec2Service, str -> Maybe[aws_store.Ebs.Snapshot]
    @raise ValueError: No IDs specified to find corresponding snapshot
    '''
    logger.info("Discover Snapshot by ID: %s" % id_)
    snapshot = None
    try:
        snapshot = Ec2Discoverer(service).getVolumeSnapshotById(id_)
    except JException:
        logger.warnCompactException("Failed to discover snapshot")
    return snapshot


def _discoverVolumesByIds(service, ids):
    r'''@types: Ec2Service, list[str] -> list[aws_store.Ebs]
    @raise ValueError: No IDs specified to find corresponding volumes
    '''
    if not ids:
        raise ValueError("No IDs specified to find corresponding volumes")
    logger.info("Discover Volumes by IDs: %s" % ids)
    volumes = []
    try:
        volumes = Ec2Discoverer(service).getVolumesByIds(ids)
    except JException:
        logger.warnException("Failed to discover volumes")
    logger.info("Discovered %s volumes" % len(volumes))
    return volumes


def _toItself(obj):
    return obj


def _applySet(fn, items):
    r'@types: callable[A, K](A) -> K, list[A] -> list[A]'
    itemToKey = {}
    for item in items:
        itemToKey[fn(item)] = 1
    return itemToKey.keys()


def _applyMapping(fn, items):
    r'@types: callable[A, K](A) -> K, list[A] -> dict[K, list[A]]'
    itemToKey = {}
    for item in items:
        itemToKey.setdefault(fn(item), item)
    return itemToKey


def _groupBy(fn, items):
    r'@types: callable[A, K](A) -> K, list[A] -> dict[K, list[A]]'
    itemToKey = {}
    for item in items:
        itemToKey.setdefault(fn(item), []).append(item)
    return itemToKey


class MissedParam:
    pass

_ = MissedParam()


def _partialFunc(func, *args):
    r'''Creates partially applied function

    For instance we have function

    def sum(a, b, c): return a + b + c

    At some moment you know partially arguments for this function (a and c)
    fn = _partialFunc(sum, a, _, c)
    [(a + b + c), (a + b1 + c), (a + b2 + c)] = map(fn, [b, b1, b2])
    '''
    class PartialFunc:
        def __init__(self, func, args):
            self.func = func
            self.args = args

        def __call__(self, *args):
            # _, 2, 3
            args = list(args)
            finalArgs = []
            for arg in self.args:
                finalArgs += ((arg == _) and (args.pop(),) or (arg,))
            return self.func(*finalArgs)
    return PartialFunc(func, args)


def _toArrayList(items):
    values = ArrayList(len(items))
    _apply(values.add, items)
    return values


def _getTagsAsString(service, tagRequest):
    tagDiscList = service.describeTags(tagRequest).getTagDescriptions()
    if tagDiscList:
        tagList = tagDiscList[0].getTags()
        tagsString = _buildTagString(tagList)
        return tagsString


def _buildTagString(tagList):
    tags = {}
    for tagItem in tagList:
        key = str(tagItem.getKey()).strip()
        value = str(tagItem.getValue()).strip()
        tags.setdefault(key, value)
    if tags:
        tagsString = json.dumps(tags)
        return tagsString
    else:
        return None


class _Connection:
    'Connection configuration and discoveryFunc performed in scope of it'
    def __init__(self, connectionFunc, discoveries):
        self.connectionFunc = connectionFunc
        self.discoveries = discoveries

    def __call__(self, framework, credentialsId):
        r'@types: Framework, str -> AwsService'
        return self.connectionFunc(framework, credentialsId)


class _Discovery:
    'Discovery configuration'
    def __init__(self, jobParameter, discoveryFunc, description):
        self.jobParameter = jobParameter
        self.discoveryFunc = discoveryFunc
        self.description = description

    def __call__(self, framework, service, account, credential_id, resourceDict):
        r'@types: Framework, AwsService, aws.Account -> OSHV'
        return self.discoveryFunc(framework, service, account, credential_id, resourceDict)

    def __repr__(self):
        return self.description