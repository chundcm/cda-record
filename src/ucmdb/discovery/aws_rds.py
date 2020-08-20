#coding=utf-8
'''
Created on Sep 1, 2011

@author: vvitvitskiy
'''
import entity
import aws
from java.util import Date
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector
import modeling
import dns_resolver
import netutils
import logger
import db_platform
from vendors import PlatformVendors

class Instance(aws.HasId, entity.Visitable):
    r'Relational database instance, where database software of particular vendor is running'
    def __init__(self, id_, server, type=None, status=None, licenseModel=None,
                 sizeInMb=None,
                 availabilityZoneName=None,
                 creationTime=None,
                 engineName=None,
                 parameterGroups=None,
                 securityGroups=[],
                 dbName=None,
                 arn=None):
        r'''@types: str, db.DatabaseServer, str, str, str, str, long, str, java.util.Date, str
        @param id_: The DB Instance is a name for DB Instance that is unique for account in a Region
        @param type: DB Instance Class, like 'db.m1.small'
        @param licenseModel: ex, 'general-public-license'
        @param __sizeInMb: How much storage in Mb initially allocated

        @raise ValueError: Instance server is not specified
        @raise ValueError: Invalid creation time
        '''
        aws.HasId.__init__(self, id_)
        self.__type = type
        if not server:
            raise ValueError("Instance server is not specified")
        self.__server = server
        self.__status = status
        self.__engineName = engineName
        self.__licenseModel = licenseModel
        self.__sizeInMb = entity.Numeric(long)
        if sizeInMb is not None:
            self.__sizeInMb.set(sizeInMb)
        self.__availabilityZoneName = availabilityZoneName
        if not (creationTime and isinstance(creationTime, Date)):
            raise ValueError("Invalid creation time")
        self.__creationTime = creationTime
        self.__securityGroups = securityGroups
        self.__parameterGroups = []
        parameterGroups and self.__parameterGroups.extend(parameterGroups)
        self.__dbName = dbName
        self.__arn = arn

    def getEngineName(self):
        r'@types: -> str or None'
        return self.__engineName

    def getServer(self):
        r'@types: -> db.DatabaseServer'
        return self.__server

    def getAvailabilityZoneName(self):
        r'@types: -> str or None'
        return self.__availabilityZoneName

    def getParameterGroups(self):
        r'@types: -> list[aws_rds.ParameterGroup]'
        return self.__parameterGroups[:]

    def getSecurityGroups(self):
        r'@types: -> list[aws_rds.SecurityGroup]'
        return self.__securityGroups[:]

    def getDbName(self):
        r'@types: -> str or None'
        return self.__dbName

    def getDbInstanceArn(self):
        r'@types: -> str or None'
        return self.__arn

    def acceptVisitor(self, visitor):
        return visitor.visitRdsDbInstance(self)

    def __repr__(self):
        return "aws_rds.Instance(%s, %s, %s)" % (self.getId(), self.__type,
                                                 self.__server)


class _Group(entity.HasName):
    def __init__(self, name, status=None, description=None):
        r'@types: str, str, str'
        entity.HasName.__init__(self)
        self.setName(name)
        self.__status = status
        self.__description = description

    def __repr__(self):
        return "%s(%s, %s)" % (self.__class__, self.getName(), self.__status)


class ParameterGroup(_Group):
    def __init__(self, name, status=None, description=None, family=None):
        r'@types: str, str, str, str'
        _Group.__init__(self, name, status, description)
        self.__family = family


class Engine(entity.HasName):
    def __init__(self, name, version=None, versionDescription=None,
                 description=None):
        r'@types: str, str, str, str'
        entity.HasName.__init__(self)
        self.setName(name)
        self.__version = version
        self.__versionDescription = versionDescription
        self.__description = description

    def getVersion(self):
        r'@types: -> str or None'
        return self.__version

    def getVersionDescription(self):
        r'@types: -> str or None'
        return self.__versionDescription

    def getDescription(self):
        r'@types: -> str or None'
        return self.__description

    def __repr__(self):
        return '%s(%s, %s, %s)' % (self.__class__,
                                   self.getName(), self.__version,
                                   self.__description)


class Snapshot(aws.HasId):

    def __init__(self, id_, instance, creationTime=None, status=None):
        r'@types: str, aws_rds.Instance, java.util.Date, str'
        aws.HasId.__init__(self, id_)
        if not instance:
            raise ValueError("Snapshot Instance is not specified")
        self.__instance = instance
        if creationTime and not isinstance(creationTime, Date):
            raise ValueError("Specified creation time is invalid")
        self.__creationTime = creationTime
        self.__status = status

    def getInstanceId(self):
        r'@types: -> str'
        return self.__instance.getId()

    def __repr__(self):
        return 'aws_rds.Snapshot(%s, %s)' % (self.getId(), self.__instance)


def MixIn(obj, mixInClass):
    pyClass = obj.__class__
    if mixInClass not in pyClass.__bases__:
        pyClass.__bases__ += (mixInClass,)
    return obj


class _Immutable:
    r'Base class for immutable objects'
    def __init__(self):
        pass


class Serializable:

    def serialize(self):
        r'@types: -> str'
        return str(self)


class Builder(_Immutable):

    class InstanceNode(entity.Visitable):
        r''' Class created for instance node reporting
        '''
        def __init__(self, instance):
            r'@types: aws_rds.Instance'
            self.instance = instance

        def acceptVisitor(self, visitor):
            return visitor.visitRdsDbInstanceNode(self)

    def visitRdsDbInstanceNode(self, node):
        r''' Creates complete host where host_key value is DB server address
        @types: aws_rds.Builder.InstanceNode -> ObjectStateHolder'''
        instance = node.instance
        osh = ObjectStateHolder('node')
        address = instance.getServer().address
        if not address:
            raise ValueError("Node address is not specified")
        osh.setAttribute("host_key", '%s' % address)
        hostname = address.split('.', 1)[0]
        osh.setAttribute('name', hostname)
        osh.setBoolAttribute('host_iscomplete', 1)
        # PrimaryDnsName
        osh.setAttribute("primary_dns_name", address)
        # NodeRole | Is Virtual
        builder = modeling.HostBuilder(osh)
        builder.setAsVirtual(1)
        return builder.build()

    def visitRdsDbInstance(self, instance):
        platform = (instance.getServer().getPlatform() or db_platform.findPlatformBySignature(instance.getServer().vendor))
        osh = ObjectStateHolder('%s_db_service' % platform.getName())
        osh.setAttribute('cloud_resource_identifier', instance.getDbInstanceArn())
        osh.setAttribute('name', instance.getDbName())
        osh.setAttribute('endpoint_address', instance.getServer().address)
        osh.setAttribute('endpoint_port', instance.getServer().getPort())
        osh.setAttribute('service_vendor', PlatformVendors.AWS)
        osh.setAttribute('version', instance.getServer().getVersion())
        osh.setAttribute('vendor', instance.getServer().vendor)
        osh.setAttribute('db_instance_id', instance.getId())
        return osh

    def __buildGroupConfig(self, group, fileName, description):
        r'@types: aws_rds.Group -> ObjectStateHolder'
        group = MixIn(group, Serializable)
        return modeling.createConfigurationDocumentOSH(fileName,
                               None, group.serialize(),
                               contentType=modeling.MIME_TEXT_PLAIN,
                               description=description)

    def visitRdsDbParameterGroup(self, group):
        r'@types: aws_rds.ParameterGroup -> ObjectStateHolder'
        return self.__buildGroupConfig(group, 'parameterGroups.csv',
                       "Serialized version of AWS DB parameter group")


class Reporter(_Immutable):
    def __init__(self, builder):
        r'@types: aws_rds.Builder'
        _Immutable.__init__(self)
        self.__builder = builder

    def reportInstanceNode(self, instance):
        r'@types: aws_rds.Instance -> ObjectStateHolder'
        return Builder.InstanceNode(instance).acceptVisitor(self.__builder)

    def reportDbService(self, instance):
        return instance.acceptVisitor(self.__builder)

    def linkZoneWithInstanceNode(self, zoneOsh, nodeOsh):
        r'@types: ObjectStateHolder, ObjectStateHolder -> ObjectStateHolder'
        if not zoneOsh:
            raise ValueError("Zone OSH is not specified")
        if not nodeOsh:
            raise ValueError("Node OSH is not specified")
        return modeling.createLinkOSH('membership', zoneOsh, nodeOsh)

    def linkZoneWithDbServiceInstance(self, zoneOsh, dbServiceOsh):
        r'@types: ObjectStateHolder, ObjectStateHolder -> ObjectStateHolder'
        if not zoneOsh:
            raise ValueError("Zone OSH is not specified")
        if not dbServiceOsh:
            raise ValueError("DB Service OSH is not specified")
        return modeling.createLinkOSH('membership', zoneOsh, dbServiceOsh)

    def linkAccountWithDbServiceInstance(self, accountOsh, dbServiceOsh):
        r'@types: ObjectStateHolder, ObjectStateHolder -> ObjectStateHolder'
        if not accountOsh:
            raise ValueError("AWS Account OSH is not specified")
        if not dbServiceOsh:
            raise ValueError("DB Service OSH is not specified")
        return modeling.createLinkOSH('containment', accountOsh, dbServiceOsh)

    def linkInstanceWithGroupConfig(self, instanceOsh, configOsh):
        r'@types: ObjectStateHolder, ObjectStateHolder -> ObjectStateHolder'
        if not instanceOsh:
            raise ValueError("Instance OSH is not specified")
        if not configOsh:
            raise ValueError("Instance group config is not specified")
        return modeling.createLinkOSH('usage', instanceOsh, configOsh)

    def link_instance_sg(self, instance_osh, sg_osh):
        if sg_osh:
            return modeling.createLinkOSH('usage', instance_osh, sg_osh)

    def linkAccountWithInstanceNode(self, accountOsh, nodeOsh):
        r'@types: ObjectStateHolder, ObjectStateHolder -> ObjectStateHolder'
        if not accountOsh:
            raise ValueError("Instance OSH is not specified")
        if not nodeOsh:
            raise ValueError("Endpoint OSH is not specified")
        return modeling.createLinkOSH('containment', accountOsh, nodeOsh)

    def reportParameterGroupConfig(self, group, containerOsh):
        r'@types: aws_rds.ParameterGroup, ObjectStateHolder -> ObjectStateHolder'
        if not group:
            raise ValueError("DB Parameter group is not specified")
        if not containerOsh:
            raise ValueError("DB Parameter group container is not specified")
        osh = self.__builder.visitRdsDbParameterGroup(group)
        osh.setContainer(containerOsh)
        return osh

    def reportIpAddresses(self, hostname, hostOsh):
        r'@types: string, ObjectStateHolder -> ObjectStateHolderVector'
        vector = ObjectStateHolderVector()
        if not hostname:
            raise ValueError("DB address is not specified")
        ipAddresses = []
        resolver = dns_resolver.SocketDnsResolver()
        ipAddresses = resolver.resolve_ips(hostname)
        logger.info(ipAddresses)

        for ipAddress in ipAddresses:
            if not netutils.isValidIp(str(ipAddress)):
                continue
            ipOsh = modeling.createIpOSH(ipAddress)
            vector.add(ipOsh)
            vector.add(modeling.createLinkOSH('containment', hostOsh, ipOsh))

        return vector
