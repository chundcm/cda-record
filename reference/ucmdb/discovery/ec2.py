'''
Created on Aug 26, 2011

@author: vvitvitskiy
'''
import entity
import logger
from appilog.common.system.types.vectors import ObjectStateHolderVector
import modeling
import netutils
from appilog.common.system.types import ObjectStateHolder
import aws
from java.lang import Boolean
import aws_store
from vendors import PlatformVendors


class Address:
    def __init__(self, hostname, ipAddress):
        r'''@types: str, str
        @raise ValueError: hostname is empty
        @raise ValueError: Invalid IP address
        '''
        if not hostname:
            raise ValueError("hostname is empty")
        if not (ipAddress and netutils.isValidIp(ipAddress)):
            raise ValueError("Invalid IP address")
        self.__hostname = hostname
        self.__ipAddress = ipAddress

    def getHostname(self):
        r'@types: -> str'
        return str(self.__hostname)

    def getIpAddress(self):
        r'@types: -> str'
        return str(self.__ipAddress)

    def __repr__(self):
        return r'ec2.Address("%s", "%s")' % (self.__hostname, self.__ipAddress)


class Image(entity.HasName, aws.HasId):
    r'''Abstract class for different types of Amazon Image
    Image is identified with name and ID
    '''
    class Type:
        MACHINE = 'machine'
        KERNEL = 'kernel'
        RAMDISK = 'ramdisk'

    class VisiblityType:
        PUBLIC = 'public'
        PRIVATE = 'private'

    def __init__(self, name, id_):
        r'''@types: str, str
        @raise ValueError: Id is empty
        @raise ValueError: Name is empty
        '''
        entity.HasName.__init__(self, name)
        # image id of format ami-3e3ecd57
        aws.HasId.__init__(self, id_)
        self.description = None
        # restrict format
        self.__architecture = None

        # Image.VisiblityType
        self.__visibility = None

    def withVisibility(self, isPublic):
        r'@types: str'
        self.__visibility = (Boolean.valueOf(isPublic)
                             and Image.VisiblityType.PUBLIC
                             or Image.VisiblityType.PRIVATE)
        return self

    def getVisibility(self):
        r'@types: -> Image.VisiblityType'
        return self.__visibility


class Instance(aws.HasId):
    r'''
    Image instance
    '''

    def __init__(self, id_, imageId):
        r'''@types: str, str
        @raise ValueError: Instance ID is empty
        @raise ValueError: Image ID is empty
        '''
        aws.HasId.__init__(self, id_)
        #  AMI id the instance was launched with
        if not imageId:
            raise ValueError("Image ID is empty")
        self.__imageId = imageId

    def getImageId(self):
        r'@types: -> str'
        return str(self.__imageId)


class Ami(Image):
    r''' Amazon Machine Image
    '''
    def __init__(self, name, id_, isPublic=None, description=None, createDate=None, virtualizationType=None,
                 imageStatus=None, imageType=None, rootDeviceType=None, region = None, platform = None):
        r'''@types: str, str, bool
        @raise ValueError: Id is empty
        @raise ValueError: Name is empty
        '''
        Image.__init__(self, name, id_)
        self.withVisibility(isPublic)
        self.description = description
        self.createDate = createDate
        self.virtualizationType = virtualizationType
        self.imageStatus = imageStatus
        self.imageType = imageType
        self.rootDeviceType = rootDeviceType
        self.region = region
        self.platform = platform

    def __repr__(self):
        return 'ec2.Ami("%s", "%s")' % (self.getName(), self.getId())

    class Instance(Instance, entity.HasOsh):
        class Type:
            '''t1.micro, m1.small, m1.large, m1.xlarge, m2.xlarge, m2.2xlarge,
             m2.4xlarge, c1.medium, c1.xlarge, cc1.4xlarge, cg1.4xlarge'''

        def __init__(self, id_, imageId, type=None, tags=None, publicAddress=None,
                     privateAddress=None,
                     launchIndex=None,
                     keyPairName=None,
                     availabilityZoneName=None,
                     vpc_id = None,
                     subnet_id = None,
                     interfaces = None,
                     security_group=[]):
            r'@types: str, str, ec2.Address, ec2.Address, Number, str, str'
            Instance.__init__(self, id_, imageId)
            entity.HasOsh.__init__(self)
            self.publicAddress = publicAddress
            self.privateAddress = privateAddress
            self.tags = tags
            self.type = type
            self.launchIndex = entity.WeakNumeric(int)
            if launchIndex is not None:
                self.launchIndex.set(launchIndex)
            self.__keyPairName = keyPairName
            self.__mappedDevices = []
            self.availabilityZoneName = availabilityZoneName
            self.vpc_id = vpc_id
            self.subnet_id = subnet_id
            self.interfaces = interfaces
            if security_group:
                self.security_group = [sg.getGroupId() for sg in security_group]

        def addMappedDevice(self, device):
            r'''@types: aws_store.MappedVolume
            @raise ValueError: Invalid mapped device
            '''
            if not (device and isinstance(device, aws_store.MappedVolume)):
                raise ValueError("Invalid mapped device")
            self.__mappedDevices.append(device)

        def getMappedDevices(self):
            r'@types: -> list(aws_store.MappedVolume)'
            return self.__mappedDevices[:]

        def getKeyPairName(self):
            r'@types: -> str or None'
            return self.__keyPairName and str(self.__keyPairName)

        def getVpcId(self):
            return self.vpc_id

        def getSubnetId(self):
            return self.subnet_id

        def getInterfaces(self):
            return self.interfaces

        def getInstanceType(self):
            return self.type

        def acceptVisitor(self, visitor):
            return visitor.visitEc2AmiInstance(self)

        def __repr__(self):
            return 'ec2.Ami.Instance("%s", "%s", "%s")' % (self.getId(),
                                                           self.getImageId(),
                                                           self.type)


class ElasticIp:
    r'''
    Elastic IP addresses are static IP addresses designed for dynamic cloud
    computing. An Elastic IP address is associated with your account not
    a particular instance, and you control that address until you choose to
    explicitly release it. Unlike traditional static IP addresses, however,
    Elastic IP addresses allow you to mask instance or Availability Zone
    failures by programmatically remapping your public IP addresses
    to any instance in your account.
    '''
    def __init__(self, publicIp, instanceId=None):
        r'''@types: str, str
        @raise ValueError: Invalid IP
        '''
        if not (publicIp and netutils.isValidIp(publicIp)):
            raise ValueError("Invalid IP")
        self.__publicIp = publicIp
        self.__instanceId = instanceId

    def getIp(self):
        r'@types: -> str'
        return self.__publicIp

    def getInstanceId(self):
        r'@types: -> str or None'
        return self.__instanceId

    def __repr__(self):
        return "%s(%s, %s)" % (self.__class__, self.__publicIp,
                               self.__instanceId)


class LoadBalancer(entity.HasName):
    r''' Amazon Machine Image
    '''
    def __init__(self, name, DNSName = None, instances = None, listeners = None, tags=None):
        r'''@types: str, str, bool
        @raise ValueError: Id is empty
        @raise ValueError: Name is empty
        '''
        entity.HasName.__init__(self, name)
        self.DNSName = DNSName
        self.instances = instances
        self.listeners = listeners
        self.tags = tags
        self.name = name


class Listener:
    def __init__(self, loadBalancerProtocol, loadBalancerPort, instanceProtocol, instancePort, sslPolicy = None):
        self.loadBalancerProtocol = loadBalancerProtocol
        self.loadBalancerPort = loadBalancerPort
        self.instanceProtocol = instanceProtocol
        self.instancePort = instancePort
        self.sslPolicy = sslPolicy


class Subnet(aws.HasId, entity.HasOsh):
    def __init__(self, id, vpc_id, avaiable_zone, ip_count, cidr_block):
        entity.HasOsh.__init__(self)
        aws.HasId.__init__(self, id)
        if not vpc_id:
            raise ValueError("vpc id is empty")
        if not avaiable_zone:
            raise ValueError("Available zone id is empty")

        self.__vpc_id = vpc_id
        self.__avaiable_zone = avaiable_zone
        self.__ip_count = ip_count
        self.__cidr = cidr_block
        self.__ipv6_cidr = None
        self.__name = None
        self.arn = None

    def getVpcId(self):
        r'@types: -> Subnet.vpc_id'
        return self.__vpc_id

    def getAvailabilityZone(self):
        r'@types: -> Subnet.availabilityZone'
        return self.__avaiable_zone

    def getCIDR(self):
        return self.__cidr

    def getAvailableIpAddressCount(self):
        return self.__ip_count

    def setIpv6CidrBlock(self, cidr):
        self.__ipv6_cidr = cidr

    def getIpv6CidrBlock(self):
        return self.__ipv6_cidr

    def setName(self, name):
        self.__name = name

    def getName(self):
        return self.__name

    def acceptVisitor(self, visitor):
        r'''@types: CanVisitSubnet -> object
        Introduce interface for visitor expected here
        '''
        return visitor.visitEc2Subnet(self)

    def __repr__(self):
        return 'ec2.Subnet("%s", "%s", "%s")' % (
                            self.getId(), self.getAvailabilityZone(), self.getVpcId())


class NetworkInterface(aws.HasId, entity.HasOsh):
    def __init__(self, id, mac_address, description):
        entity.HasOsh.__init__(self)
        aws.HasId.__init__(self, id)
        self.id = id
        self.mac_address = mac_address
        self.description = description

    def get_macaddress(self):
        return self.mac_address

    def get_description(self):
        return self.mac_address

    def acceptVisitor(self, visitor):
        return visitor.visitNetworkInterface(self)


class Builder:

    class Ec2InstanceNode(entity.HasOsh):
        r'PDO intended to build node'
        def __init__(self, amiInstance, ami):
            r'@types: ec2.Ami.Instance'
            self.amiInstance = amiInstance
            self.ami = ami
            entity.HasOsh.__init__(self)

        def acceptVisitor(self, visitor):
            return visitor.visitEc2AmiInstanceNode(self)

    class Ec2AmiInstanceConfig(entity.HasOsh):
        def __init__(self, ami, instance):
            r'@types: ec2.Ami'
            self.ami = ami
            self.instance = instance
            entity.HasOsh.__init__(self)

        def acceptVisitor(self, visitor):
            return visitor.visitEc2AmiInstanceConfig(self)

    class Ec2Ami(entity.HasOsh):
        def __init__(self, ami):
            self.ami = ami
            entity.HasOsh.__init__(self)

        def acceptVisitor(self, visitor):
            return visitor.visitEc2Ami(self)

    class Ec2LoadBalancer(entity.HasOsh):
        def __init__(self, loadbalancer):
            self.loadbalancer = loadbalancer
            entity.HasOsh.__init__(self)

        def acceptVisitor(self, visitor):
            return visitor.visitEc2LoadBalancer(self)

        def buildClusterSoftware(self):
            osh = ObjectStateHolder('lb_software')
            osh.setAttribute("discovered_product_name", "AWS Elastic Load Balancer")
            return osh

    class Ec2LoadBalancingCluster(entity.HasOsh):
        r'PDO intended to build virtual host resource'
        def __init__(self, loadbalancer, port):
            self.loadbalancer = loadbalancer
            self.port = port
            entity.HasOsh.__init__(self)

        def acceptVisitor(self, visitor):
            return visitor.visitEc2LoadBalancingCluster(self)

        def buildCRG(self):
            osh = ObjectStateHolder('cluster_resource_group')
            osh.setAttribute("name", self.loadbalancer.DNSName + ":" + str(self.port))
            return osh

        def buildUriEndpoint(self, port):
            osh = ObjectStateHolder('uri_endpoint')
            osh.setAttribute("uri", self.loadbalancer.DNSName + ":" + str(port))
            return osh

        def buildSslPolicy(self, sslPolicy):
            osh = ObjectStateHolder('amazon_security_policy')
            osh.setAttribute("name", sslPolicy)
            return osh

    def visitEc2LoadBalancingCluster(self, ec2LoadBalancingCluster):
        loadBalancer = ec2LoadBalancingCluster.loadbalancer
        port = ec2LoadBalancingCluster.port
        osh = ObjectStateHolder('loadbalancecluster')
        osh.setAttribute("name", loadBalancer.DNSName + ":" + str(port))
        return osh

    def visitEc2Subnet(self, subnet):
        cidr = None
        if subnet.getCIDR():
            cidr = subnet.getCIDR()
        elif subnet.getIpv6CidrBlock():
            cidr = subnet.getIpv6CidrBlock()[0]
        ip, netmask = netutils.obtainDotDecimalTuple(cidr)
        osh = modeling.createNetworkOSH(ip, netmask)
        osh.setAttribute("cloud_resource_identifier", subnet.getId())
        osh.setAttribute('network_count', subnet.getAvailableIpAddressCount())
        osh.setAttribute('name', subnet.getId())
        return osh

    def visitNetworkInterface(self, interface):
        id = interface.getId()
        mac_address = interface.get_macaddress()
        description = interface.get_description()
        osh = modeling.createInterfaceOSH(mac=mac_address, description= description)
        osh.setAttribute('cloud_resource_identifier', id)
        return osh

    def visitEc2LoadBalancer(self, ec2LoadBalancer):
        loadBalancer = ec2LoadBalancer.loadbalancer
        osh = ObjectStateHolder('lb')
        osh.setAttribute("name", loadBalancer.name)
        osh.setAttribute("primary_dns_name", loadBalancer.DNSName)
        return osh

    def visitEc2AmiInstanceConfig(self, ec2AmiInstanceConfig):
        r'@types: ec2.Ami.Instance -> ObjectStateHolder'
        ami = ec2AmiInstanceConfig.ami
        instance = ec2AmiInstanceConfig.instance
        osh = ObjectStateHolder('amazon_ec2_config')
        osh.setStringAttribute('name', ami.getName())
        osh.setStringAttribute('ami_visibility', str(ami.getVisibility()))
        osh.setStringAttribute('description', ami.description)
        osh.setStringAttribute('type', instance.type)
        osh.setStringAttribute('ami_id', ami.getId())
        index = instance.launchIndex
        if index and index.value() is not None:
            osh.setAttribute('ami_launch_index', index.value())
        if instance.getKeyPairName():
            osh.setAttribute('key_pair_name', instance.getKeyPairName())
        return osh

    def visitEc2Ami(self, amiInstance):
        ami = amiInstance.ami
        osh = ObjectStateHolder("aws_ami")
        osh.setAttribute("ami_id", ami.getId())
        osh.setAttribute('amazon_resource_name', 'arn:aws:ec2:' + ami.region.getName() + '::image/' + ami.getId())
        osh.setStringAttribute('name', ami.getName())
        osh.setStringAttribute('ami_visibility', str(ami.getVisibility()))
        osh.setStringAttribute('description', ami.description)
        try:
            from java.text import SimpleDateFormat
            from datetime import datetime
            date = datetime.strptime(ami.createDate,'%Y-%m-%dT%H:%M:%S.%fZ')
            dateFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss z")
            dateTime = dateFormat.parse(dateFormat.format(date))
        except:
            dateTime = None
            logger.debugException('Failed to convert create time to Date.')
        if dateTime:
            osh.setDateAttribute('image_creation_time', dateTime)
        osh.setStringAttribute('virtualization_type', ami.virtualizationType)
        osh.setStringAttribute('image_status', ami.imageStatus)
        osh.setStringAttribute('image_type', ami.imageType)
        osh.setStringAttribute('root_device_type', ami.rootDeviceType)
        return osh

    def visitEc2AmiInstanceNode(self, ec2InstanceNode):
        r'''@types: ec2.Builder.Ec2InstanceNode -> ObjectStateHolder
        @raise ValueError: Public address is not specified
        '''
        address = ec2InstanceNode.amiInstance.publicAddress
        if not address:
            logger.warn("Public address is not specified, try private address instead")
            address = ec2InstanceNode.amiInstance.privateAddress
            if not address:
                raise ValueError("Both public address and private address are not specified")
        ec2_os_type = 'host_node'
        ami = ec2InstanceNode.ami
        if ami:
            amiPlatform = ami.platform
            if amiPlatform:
                if 'windows' in amiPlatform.lower():
                    ec2_os_type = 'nt'
            else:
                ec2_os_type = 'unix'
        osh = modeling.createHostOSH(address.getIpAddress(), hostClassName=ec2_os_type)
        # description set with instance name value
        # TODO
        # osh.setAttribute('description', "Instance name: %" % ec2InstanceNode.amiInstance.getName())
        # is complete
        osh.setBoolAttribute('host_iscomplete', 1)
        osh.setAttribute("host_key", str(ec2InstanceNode.amiInstance.getId()))
        # Host Platform Vendor
        osh.setStringAttribute('platform_vendor', PlatformVendors.AWS)
        # PrimaryDnsName
        osh.setAttribute("primary_dns_name", address.getHostname())
        external_id = ec2InstanceNode.amiInstance.getId()
        if external_id:
            osh.setAttribute("cloud_instance_id", external_id)
        # NodeRole | Is Virtual
        builder = modeling.HostBuilder(osh)
        builder.setAsVirtual(1)
        if ec2InstanceNode.amiInstance.getInstanceType():
            osh.setAttribute('node_model', ec2InstanceNode.amiInstance.getInstanceType())
        return builder.build()


class Reporter:

    def __init__(self, builder):
        r'@types: ec2.Builder'
        self.__builder = builder

    def _createOshVector(self):
        return ObjectStateHolderVector()

    def reportPublicIpAddress(self, account, ipAddress, hostOsh):
        r'@types: aws.Account, str, OSH -> OSHV'
        vector = self._createOshVector()
        ipOsh = modeling.createIpOSH(ipAddress)
        vector.add(ipOsh)
        vector.add(modeling.createLinkOSH('containment', hostOsh, ipOsh))
        return vector

    def reportPrivateIpAddress(self, ipAddress, hostOsh):
        r'@types: str, OSH -> OSHV'
        vector = self._createOshVector()
        ipOsh = modeling.createIpOSH(ipAddress)
        vector.add(ipOsh)
        vector.add(modeling.createLinkOSH('containment', hostOsh, ipOsh))
        return vector

    def reportSubnet(self, subnet, vpcOsh):
        if not subnet:
            raise ValueError("IP Subnet is not specified")
        if not vpcOsh:
            raise ValueError("AWS vpc is not specified")

        subnet_osh = subnet.build(self.__builder)
        subnet_osh.setContainer(vpcOsh)
        return subnet_osh

    def reportNetworkInterface(self, interface, node_osh):
        logger.debug('to build interface...')
        interface_osh = interface.build(self.__builder)
        interface_osh.setContainer(node_osh)
        return interface_osh

    def buildInstanceNode(self, instance, ami):
        r'@types: ec2.Ami.Instance -> ObjectStateHolder'
        nodePdo = self.__builder.Ec2InstanceNode(instance, ami)
        return nodePdo.build(self.__builder)

    def reportAmiInstance(self, account, ami, instance):
        r'''@types: aws.Account, ec2.Ami, ec2.Ami.Instance -> ObjectStateHolderVector
        @raise ValueError: AWS Account is not specified or not built
        '''
        if not (account and account.getOsh()):
            raise ValueError("AWS Account is not specified or not built")
        if not instance:
            raise ValueError("AMI instance is not specified")
        vector = self._createOshVector()
        # use synthetic PDO to build node (Account as container),
        # container for the Instance
        nodeOsh = self.buildInstanceNode(instance, ami)
        vector.add(nodeOsh)
        vector.add(modeling.createLinkOSH('containment', account.getOsh(),
                                          nodeOsh))
        # report ec2 config
        if ami:
            configPdo = self.__builder.Ec2AmiInstanceConfig(ami, instance)
            vector.add(configPdo.build(self.__builder))
            configPdo.getOsh().setContainer(nodeOsh)

            amiPdo = self.__builder.Ec2Ami(ami)
            vector.add(amiPdo.build(self.__builder))
            vector.add(modeling.createLinkOSH("dependency", nodeOsh, amiPdo.getOsh()))

        # report IPs
        address = instance.publicAddress
        if address:
            vector.addAll(self.reportPublicIpAddress(account,
                                             address.getIpAddress(), nodeOsh))
        address = instance.privateAddress
        if address:
            vector.addAll(self.reportPrivateIpAddress(address.getIpAddress(),
                                                      nodeOsh))
        if instance.tags:
            vector.add(_reportTagsAsConfigFile(instance.tags, nodeOsh))
        return vector,nodeOsh

    def linkAmiInstanceToAvailabilityZone(self, instance, zoneOsh, ami):
        r'@types: ec2.Ami.Instance, aws.AvailabilityZone -> ObjectStateHolderVector'
        if not instance:
            raise ValueError("AMI instance is not specified")
        if not zoneOsh:
            raise ValueError("Availability zone OSH is not specified")
        vector = self._createOshVector()
        nodePdo = self.__builder.Ec2InstanceNode(instance, ami)
        vector.add(nodePdo.build(self.__builder))
        vector.add(zoneOsh)
        vector.add(modeling.createLinkOSH('membership', zoneOsh,
                                          nodePdo.getOsh()))
        return vector

    def reportLoadBalancing(self, accountOsh,  loadbalancer, instanceIdOshDict, zoneOshs, vpc_osh=None, sg_osh_list=[]):
        # report loadbalancer and cluster software
        vector = self._createOshVector()
        lb = self.__builder.Ec2LoadBalancer(loadbalancer)
        lbOsh = lb.build(self.__builder)
        # link to availability zones
        for zoneOsh in zoneOshs:
            vector.add(zoneOsh)
            vector.add(modeling.createLinkOSH('membership', zoneOsh, lbOsh))

        if sg_osh_list:
            for sg_osh in sg_osh_list:
                vector.add(modeling.createLinkOSH('usage', lbOsh, sg_osh))

        vector.add(lbOsh)
        # Report LoadBalance relationship with VPC
        vector.add(modeling.createLinkOSH('membership', vpc_osh, lbOsh))
        # link to account osh
        vector.add(modeling.createLinkOSH('containment', accountOsh, lbOsh))
        if loadbalancer.tags:
            vector.add(_reportTagsAsConfigFile(loadbalancer.tags, lbOsh))

        for listener in loadbalancer.listeners:
            for instance in loadbalancer.instances:
                if isinstance(instance, basestring):
                    instanceId = instance
                else:
                    instanceId = instance.getInstanceId()
                if instanceIdOshDict.has_key(instanceId):
                    instanceOsh = instanceIdOshDict[instanceId]
                    vector.add(modeling.createLinkOSH('membership', lbOsh, instanceOsh))
        return vector

    def linkSubnetToAvailabilityZone(self, subnet_osh, zone_osh):
        r'@types: ObjectStateHolder, ObjectStateHolder -> ObjectStateHolder'
        if not subnet_osh:
            raise ValueError("Subnet is not specified")
        if not zone_osh:
            raise ValueError("Availability Zone is not specified")
        return modeling.createLinkOSH('membership', zone_osh, subnet_osh)

    def linkVpcToAmiInstance(self, vpc_osh, instance_osh):
        if not vpc_osh:
            raise ValueError("Vpc is not specified")
        if not instance_osh:
            raise ValueError("AMI Instacne is not specified")
        return modeling.createLinkOSH('membership', vpc_osh, instance_osh)

    def link_sg_instance(self, sg_osh_list, instance_osh):
        if sg_osh_list:
            vector = self._createOshVector()
            for sg_osh in sg_osh_list:
                vector.add(modeling.createLinkOSH('usage', instance_osh, sg_osh))
            return vector

    def linkSubnetToAmiInstance(self, subnet_osh, instance_osh):
        if not subnet_osh:
            raise ValueError("Subnet is not specified")
        if not instance_osh:
            raise ValueError("AMI Instacne is not specified")
        return modeling.createLinkOSH('membership', subnet_osh, instance_osh)

    def reportInstalledSW(self, instanceIdOsh, entires):
        vector = ObjectStateHolderVector()
        for entry in entires:
            SWName = entry['Name']
            SWOsh = modeling.createApplicationOSH('installed_software',SWName, instanceIdOsh)
            SWOsh.setStringAttribute('version', entry['Version'])
            vector.add(SWOsh)
        return vector



def _reportTagsAsConfigFile(tags, nodeOsh):
    fileOsh = modeling.createConfigurationDocumentOSH(
                'AWS Tags',
                '<AWS Tags>',
                tags,
                contentType=modeling.MIME_TEXT_PLAIN,
                description='AWS Tags')
    fileOsh.setContainer(nodeOsh)
    return fileOsh
