#coding=utf-8
'''
Created on Aug 12, 2011

@author: vvitvitskiy
'''
import entity
import modeling
from appilog.common.system.types import ObjectStateHolder, AttributeStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector, StringVector


class HasId:
    def __init__(self, id_):
        if id_ is None:
            raise ValueError("Id is empty")
        self.__id = id_

    def getId(self):
        r'@types: -> obj'
        return self.__id


class State:
    AVAILABLE = 'available'
    NOT_AVAILABE = 'notavailable'


class Account(HasId, entity.HasOsh):
    r''' Amazon account'''
    def __init__(self, id_):
        r'''@types: str
        @param id: Amazon Account ID
        '''
        HasId.__init__(self, id_)
        entity.HasOsh.__init__(self)
        self.userName = None

    def acceptVisitor(self, visitor):
        r'@types: CanVisitAwsAccount -> ObjectStateHolder'
        return visitor.visitAwsAccount(self)

class User(entity.HasName, entity.HasOsh):
    r''' Amazon account user'''
    def __init__(self, name):
        r'''@types: str
        @param id: Amazon Account User
        '''
        entity.HasName.__init__(self)
        entity.HasOsh.__init__(self)
        self.setName(name)

    def acceptVisitor(self, visitor):
        r'@types: CanVisitAwsAccount -> ObjectStateHolder'
        return visitor.visitAwsUser(self)


class Region(entity.HasName, entity.HasOsh):
    r''' Amazon EC2 region. EC2 regions are completely isolated from each other
    '''
    def __init__(self, name, endpointHostName=None):
        r'''Region is identified by name so endpoint hostname is optional
        @types: str, str
        @param endpointHostName: Service endpoint hostname
        '''
        entity.HasName.__init__(self)
        entity.HasOsh.__init__(self)
        self.setName(name)
        self.__endpointHostName = endpointHostName
        self.__availabilityZones = []

    def addZone(self, zone):
        r'''@types: aws.AvailabilityZone
        @raise ValueError: Zone is not specified
        '''
        if not zone:
            raise ValueError("Zone is not specified")
        self.__availabilityZones.append(zone)

    def getZones(self):
        r'@types: -> list(aws.AvailabilityZone)'
        return self.__availabilityZones[:]

    def getEndpointHostName(self):
        r'@types: -> str or None'
        return str(self.__endpointHostName)

    def acceptVisitor(self, visitor):
        return visitor.visitAwsRegion(self)

    def __repr__(self):
        return 'aws.Region("%s", "%s")' % (self.getName(),
                                           self.getEndpointHostName())


class AvailabilityZone(entity.HasName, entity.HasOsh):
    r'''An EC2 availability zone, separate and fault tolerant from other
     availability zones'''

    def __init__(self, name, regionName, state):
        r'''@types: str, str, aws.State
        @raise ValueError: Name is empty
        @raise ValueError: State is empty
        @raise ValueError: Region name is empty
        '''
        entity.HasName.__init__(self)
        entity.HasOsh.__init__(self)
        self.setName(name)
        if not state:
            raise ValueError("State is empty")
        if not regionName:
            raise ValueError("Region name is empty")
        self.__state = state
        self.__regionName = regionName

    def getState(self):
        r'@types: -> AvailabilityZone.State'
        return self.__state

    def getRegionName(self):
        r'@types: -> str'
        return str(self.__regionName)

    def acceptVisitor(self, visitor):
        r'''@types: CanVisitAwsAvailabilityZone -> object
        Introduce interface for visitor expected here
        '''
        return visitor.visitAwsAvailabilityZone(self)

    def __repr__(self):
        return 'aws.AvailabilityZone("%s", "%s", "%s")' % (
                            self.getName(), self.__regionName, self.getState())


class Vpc(entity.HasOsh):
    r'''An Virtual Private Cloud(VPC)'''

    def __init__(self, id, is_default, state, cidr_block):
        r'''@types: str, str, aws.State
        @raise ValueError: Id is empty
        @raise ValueError: IsDefault is empty
        @raise ValueError: State name is empty
        '''
        entity.HasOsh.__init__(self)
        if not id:
            raise ValueError("vpc id is empty")
        if not state:
            raise ValueError("State is empty")
        self.__id = id
        self.__state = state
        self.__is_default = is_default
        self.__cidr = cidr_block
        self.__ipv6_cidr = None
        self.__name = None
        self.__ipv4_cidr = None

    def getId(self):
        r'@types: -> Vpc.id'
        return self.__id

    def getState(self):
        r'@types: -> Vpc.state'
        return self.__state

    def getIsDefault(self):
        r'@types: -> Vpc.isDefault'
        return self.__is_default

    def getCIDR(self):
        return self.__cidr

    def setIpv4CidrBlock(self, cidr):
        self.__ipv4_cidr = cidr

    def getIpv4CidrBlock(self):
        return self.__ipv4_cidr

    def setIpv6CidrBlock(self, cidr):
        self.__ipv6_cidr = cidr

    def getIpv6CidrBlock(self):
        return self.__ipv6_cidr

    def setName(self, name):
        self.__name = name

    def getName(self):
        return self.__name

    def acceptVisitor(self, visitor):
        r'''@types: CanVisitAwsVpc -> object
        Introduce interface for visitor expected here
        '''
        return visitor.visitAwsVpc(self)

    def __repr__(self):
        return 'aws.Vpc("%s", "%s", "%s")' % (
            self.getId(), self.__is_default, self.getState())


class SecurityGroup(entity.HasOsh):
    def __init__(self, vpc_id, group_id, group_name, inbound, outbound):
        entity.HasOsh.__init__(self)
        self.group_id = group_id
        self.group_name = group_name
        self.vpc_id = vpc_id
        self.inbound = self.get_bound(inbound)
        self.outbound = self.get_bound(outbound)

    def get_bound(self, bounds):
        bound_list = []
        if len(bounds) != 0:
            for bound in bounds:
                ip_protocol = bound.getIpProtocol()
                if ip_protocol == '-1':
                    ip_protocol = 'All traffic'
                bound_str = 'protocol:' + str(ip_protocol)
                from_port = bound.getFromPort()
                to_port = bound.getToPort()
                port_range = self.get_port_range(from_port, to_port)
                bound_str = bound_str + ' || ' + 'port_range:' + str(port_range)
                ipv4_ranges = bound.getIpv4Ranges()
                ipv4range = self.get_ipv4(ipv4_ranges)

                ipv6_ranges = bound.getIpv6Ranges()
                ipv6range = self.get_ipv6(ipv6_ranges)

                if len(ipv4range) != 0:
                    bound_str = bound_str + ' || ' + 'ipv4_cidr:' + '-'.join(ipv4range)

                if len(ipv6range) != 0:
                    bound_str = bound_str + ' || ' + 'ipv6_cidr:' + '-'.join(ipv6range)

                bound_list.append(bound_str)
        return bound_list

    def get_port_range(self, from_port, to_port):
        if from_port and from_port == to_port:
            port_range = str(from_port)
            if from_port == -1:
                port_range = 'N/A'

        elif from_port and to_port:
            port_range = str(from_port) + '~' + str(to_port)
        else:
            port_range = 'All'
        return port_range

    def get_ipv4(self, ipranges):
        ipv4range = []
        for iprange in ipranges:
            ipv4range.append(iprange.getCidrIp())
        return ipv4range

    def get_ipv6(self, ipranges):
        ipv6range = []
        for iprange in ipranges:
            ipv6range.append(iprange.getCidrIpv6())
        return ipv6range

    def acceptVisitor(self, visitor):
        return visitor.visit_security_group(self)


class Builder:

    def __buildLocationOsh(self, name, locationType, typeStr):
        r'@types: str, str, str -> ObjectStateHolder'
        osh = ObjectStateHolder('location')
        osh.setAttribute('name', name)
        osh.setAttribute('data_note', typeStr)
        osh.setAttribute('location_type', locationType)
        return osh

    def buildUndefinedLocationOsh(self, name, typeStr):
        return self.__buildLocationOsh(name, 'undefined', typeStr)

    def buildVpcOsh(self, vpc):
        osh = ObjectStateHolder('aws_vpc')
        osh.setAttribute('vpc_id', vpc.getId())
        osh.setAttribute('vpc_state', vpc.getState())
        osh.setAttribute('is_default', vpc.getIsDefault())
        if vpc.getIpv4CidrBlock():
            for item in vpc.getIpv4CidrBlock():
                osh.addAttributeToList('ipv4_cidr', item)
        elif vpc.getCIDR():
            osh.addAttributeToList('ipv4_cidr', vpc.getCIDR())
        if vpc.getIpv6CidrBlock():
            for item in vpc.getIpv6CidrBlock():
                osh.addAttributeToList('ipv6_cidr', item)
        osh.setAttribute('name', vpc.getName())
        return osh

    def build_sg_osh(self, sg):
        osh = ObjectStateHolder('aws_sg')
        osh.setAttribute('sg_id', sg.group_id)
        osh.setAttribute('name', sg.group_name)
        if sg.inbound:
            inbound = tuple(sg.inbound)
            tup = StringVector(inbound)
            bound_attr = AttributeStateHolder('sg_inbound', tup)
            osh.addAttributeToList(bound_attr)
        if sg.outbound:
            outbound = tuple(sg.outbound)
            tup = StringVector(outbound)
            bound_attr = AttributeStateHolder('sg_outbound', tup)
            osh.addAttributeToList(bound_attr)
        return osh

    def visitAwsAccount(self, account):
        r'@types: aws.Account -> ObjectStateHolder'
        osh = ObjectStateHolder('amazon_account')
        osh.setAttribute('account_id', account.getId())
        osh.setAttribute('name', account.getId())
        return osh

    def visitAwsUser(self, user):
        osh = ObjectStateHolder('aws_user')
        osh.setAttribute('name', user.getName())
        return osh

    def visitAwsAvailabilityZone(self, availabilityZone):
        r'@types: aws.AvailabilityZone -> ObjectStateHolder'
        return self.buildUndefinedLocationOsh(availabilityZone.getName(),
                                              'Availability Zone')

    def visitAwsRegion(self, region):
        r'@types: aws.Region -> ObjectStateHolder'
        return self.buildUndefinedLocationOsh(region.getName(), 'Region')

    def visitAwsVpc(self, vpc):
        return self.buildVpcOsh(vpc)

    def visit_security_group(self, sg):
        return self.build_sg_osh(sg)


class Reporter:
    def __init__(self, locationBuilder):
        r'@types: aws.Builder'
        self.__builder = locationBuilder

    def _createOshVector(self):
        return ObjectStateHolderVector()

    def reportAccount(self, account):
        r'''@types: aws.Account -> ObjectStateHolder
        @raise ValueError: AWS Account is not specified
        '''
        if not account:
            raise ValueError("AWS Account is not specified")
        return account.build(self.__builder)

    def reportUser(self, user, accountOsh):
        r'''@types: aws.User -> ObjectStateHolder
        @raise ValueError: AWS Account user is not specified
        '''
        if not user:
            raise ValueError("AWS User is not specified")
        userOsh = user.build(self.__builder)
        userOsh.setContainer(accountOsh)
        return userOsh

    def reportRegion(self, region, account=None):
        r'''@types: aws.Region -> ObjectStateHolderVector
        @raise ValueError: Region is not specified
        '''
        if not region:
            raise ValueError("Region is not specified")
        vector = ObjectStateHolderVector()
        region_osh = region.build(self.__builder)
        vector.add(region_osh)
        if account and account.getOsh():
            vector.add(modeling.createLinkOSH("membership", account.getOsh(), region_osh))
        return vector

    def reportAvailabilityZoneByName(self, name):
        r'''@types: str -> ObjectStateHolder
        @raise ValueError: Zone name is not specified
        '''
        return self.__builder.buildUndefinedLocationOsh(name,
                                                        'Availability Zone')

    def reportAvailabilityZone(self, zone):
        r'''@types: aws.AvailabilityZone -> ObjectStateHolder
        @raise ValueError: Zone is not specified
        '''
        if not zone:
            raise ValueError("Zone is not specified")
        return zone.build(self.__builder)

    def reportAvailabilityZoneInRegion(self, region, zone):
        r'''@types: aws.Regtion, aws.AvailabilityZone -> ObjectStateHolderVector
        @raise ValueError: Region is not specified or not built
        @raise ValueError: Zone is not specified
        '''
        if not (region and region.getOsh()):
            raise ValueError("Region is not specified or not built")
        if not zone:
            raise ValueError("Zone is not specified")
        vector = self._createOshVector()
        regionOsh = region.getOsh()
        vector.add(regionOsh)
        vector.add(zone.build(self.__builder))
        vector.add(modeling.createLinkOSH('containment', regionOsh,
                                          zone.getOsh()))
        return vector

    def reportVpc(self, vpc, account):
        if not (account and account.getOsh()):
            raise ValueError("AWS Account is not specified or not built")
        if not vpc:
            raise ValueError("AWS vpc is not specified")
        vector = self._createOshVector()
        vpc_osh = vpc.build(self.__builder)
        vpc_osh.setContainer(account.getOsh())

        vector.add(vpc_osh)
        return vector

    def report_sg(self, sg, vpc_osh):
        sg_osh = sg.build(self.__builder)
        sg_osh.setContainer(vpc_osh)
        return sg_osh
