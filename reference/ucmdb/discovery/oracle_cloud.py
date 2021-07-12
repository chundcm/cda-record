# coding=utf-8
import entity
import modeling
import netutils
import db
import db_platform

from vendors import PlatformVendors as vendors
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector
import logger


class OracleCloudAccount(entity.HasOsh):
    def __init__(self, name):
        entity.HasOsh.__init__(self)
        self.name = name
        self.account_type = None

    def acceptVisitor(self, visitor):
        return visitor.visitAccount(self)


class Instance(entity.HasOsh):
    def __init__(self, name):
        entity.HasOsh.__init__(self)
        self.name = name
        self.hostname = None
        self.ip_address = None
        self.domain = None
        self.availability_domain = None
        self.label = None
        self.platform = None
        self.account = None
        self.hypervisor_mode = None
        self.ip_network = None
        self.vnic = None
        self.ip_reservations = []
        self.shape = None
        self.availability_domain = None
        self.imagelist = None
        self.storage_volumes = []
        self.db_server = None

    def acceptVisitor(self, visitor):
        return visitor.visitInstance(self)


class DBInstance(db.DatabaseServer):
    def __init__(self, db_type, db_name, ip, db_port, version=None, edition=None, databases=None):
        db.DatabaseServer.__init__(self, ip, db_port, databases=databases, version=version)
        self.setPlatform(db_platform.findPlatformBySignature(db_type))
        self.vendor = self.getPlatform() and self.getPlatform().vendor
        self.db_type = db_type
        self.db_name = db_name
        self.edition = edition
        self.container_osh = None

    def acceptVisitor(self, visitor):
        return visitor.visitDBInstance(self)


class IpNetwork(entity.HasOsh):
    def __init__(self, name, cidr):
        entity.HasOsh.__init__(self)
        self.name = name
        self.cidr = cidr
        self.description = None

    def acceptVisitor(self, visitor):
        return visitor.visitIpNetwork(self)


class Interface(entity.HasOsh):
    def __init__(self, name, mac_address):
        entity.HasOsh.__init__(self)
        self.name = name
        self.mac_address = mac_address
        self.description = None

    def acceptVisitor(self, visitor):
        return visitor.visitInterface(self)


class IpReservation(entity.HasOsh):
    def __init__(self, name, ip_address):
        entity.HasOsh.__init__(self)
        self.name = name
        self.ip_address = ip_address
        self.pool = None

    def acceptVisitor(self, visitor):
        return visitor.visitIpAddress(self)


class Shape(entity.HasOsh):
    def __init__(self, name):
        entity.HasOsh.__init__(self)
        self.name = name
        self.cpus = None
        self.gpus = None
        self.ram = None

    def acceptVisitor(self, visitor):
        return visitor.visitShape(self)


class StorageVolume(entity.HasOsh):
    def __init__(self, name):
        entity.HasOsh.__init__(self)
        self.name = name
        self.description = None
        self.imagelist = None
        self.platform = None
        self.storage_pool = None
        self.availability_domain = None
        self.properties = None
        self.account = None
        self.status = None
        self.size = None

    def acceptVisitor(self, visitor):
        return visitor.visitStorageVolume(self)


class StorageAttachment(entity.HasOsh):
    def __init__(self, name, instance, storage_volume):
        entity.HasOsh.__init__(self)
        self.name = name
        self.instance = instance
        self.storage_volume = storage_volume


class Region(entity.HasOsh):
    def __init__(self, name, location, region):
        entity.HasOsh.__init__(self)
        self.name = name
        self.location = location
        self.region = region

    def acceptVisitor(self, visitor):
        return visitor.visitRegion(self)


class AvailabilityDomain(entity.HasOsh):
    def __init__(self, name):
        entity.HasOsh.__init__(self)
        self.name = name
        self.region = None

    def acceptVisitor(self, visitor):
        return visitor.visitAvailabilityDomain(self)


class Image(entity.HasOsh):
    def __init__(self, name):
        entity.HasOsh.__init__(self)
        self.name = name

    def acceptVisitor(self, visitor):
        return visitor.visitImage(self)


class Builder:

    def visitAccount(self, account):
        account_osh = ObjectStateHolder('oracle_cloud_account')
        account_osh.setAttribute('name', account.name)
        if account.account_type:
            account_osh.setAttribute('account_type', account.account_type)
        return account_osh

    def visitInstance(self, instance):
        cit = 'node'
        if instance.platform == 'linux':
            cit = 'unix'
        elif instance.platform == 'windows':
            cit = 'nt'
        osh = modeling.createHostOSH(instance.ip_address, hostClassName=cit)
        osh.setAttribute('cloud_instance_id', instance.name)
        if instance.hostname:
            osh.setAttribute('name', instance.hostname)
        osh.setAttribute('platform_vendor', vendors.Solaris)
        return osh

    def visitIpNetwork(self, network):
        ip, netmask = netutils.obtainDotDecimalTuple(network.cidr)
        ip_subnet_osh = modeling.createNetworkOSH(ip, netmask)
        ip_subnet_osh.setAttribute('description', network.description)
        ip_subnet_osh.setAttribute('cloud_resource_identifier', network.name)
        return ip_subnet_osh

    def visitInterface(self, interface):
        return modeling.createInterfaceOSH(interface.mac_address, name=interface.name, description=interface.description)

    def visitIpAddress(self, ip_reservation):
        return modeling.createIpOSH(ip_reservation.ip_address)

    def visitShape(self, shape):
        shape_osh = ObjectStateHolder('oracle_cloud_config')
        shape_osh.setAttribute('name', shape.name)
        shape_osh.setAttribute('cpus', int(shape.cpus))
        shape_osh.setIntegerAttribute('ocpus', int(shape.cpus / 2))
        shape_osh.setIntegerAttribute('gpus', int(shape.gpus))
        shape_osh.setAttribute('memory', float(shape.ram) / 1024)
        return shape_osh

    def visitStorageVolume(self, volume):
        volume_osh = ObjectStateHolder('oracle_cloud_storage_volume')
        volume_osh.setAttribute('name', volume.name)
        volume_osh.setAttribute('description', volume.description)
        volume_osh.setAttribute('property', ",".join(volume.properties))
        volume_osh.setAttribute('volume_size', float(volume.size) / 1024 / 1024 / 1024)
        volume_osh.setAttribute('volume_status', volume.status)
        return volume_osh

    def visitRegion(self, region):
        region_osh = ObjectStateHolder('location')
        region_osh.setAttribute('name', region.name)
        region_osh.setAttribute('location_type', 'undefined')
        region_osh.setAttribute('data_note', 'Region')
        region_osh.setAttribute('region', region.region)
        region_osh.setAttribute('locality', region.location)
        return region_osh

    def visitAvailabilityDomain(self, domain):
        domain_osh = ObjectStateHolder('location')
        domain_osh.setAttribute('name', domain.name)
        domain_osh.setAttribute('location_type', 'undefined')
        domain_osh.setAttribute('data_note', 'Availability Domain')
        return domain_osh

    def visitImage(self, image):
        image_osh = ObjectStateHolder('oracle_cloud_image')
        image_osh.setAttribute('name', image.name)
        return image_osh


class Reporter:
    def __init__(self, builder):
        self.__builder = builder

    def reportAccount(self, account, region=None):
        if not account:
            raise ValueError("Oracle Cloud Account is not specified")
        vector = ObjectStateHolderVector()
        account_osh = account.build(self.__builder)
        vector.add(account_osh)

        if region:
            vector.add(modeling.createLinkOSH('membership', account_osh, region.getOsh()))
        return vector

    def reportInstance(self, instance):
        if not instance:
            raise ValueError("Instance is not specified")
        vector = ObjectStateHolderVector()
        host_osh = instance.build(self.__builder)
        host_osh.setAttribute('node_model', instance.shape.name)
        vector.add(host_osh)
        if instance.ip_network:
            vector.add(modeling.createLinkOSH('membership', instance.ip_network.getOsh(), host_osh))
        if instance.vnic:
            if instance.vnic.getOsh():
                vector.add(instance.vnic.getOsh())
                vector.add(modeling.createLinkOSH('composition', host_osh, instance.vnic.getOsh()))
        if instance.ip_reservations:
            for ip_reservation in instance.ip_reservations:
                vector.add(modeling.createLinkOSH('containment', host_osh, ip_reservation.getOsh()))
        instance.shape.getOsh().setContainer(host_osh)
        vector.add(modeling.createLinkOSH('membership', instance.availability_domain.getOsh(), host_osh))

        if instance.account and instance.account.getOsh():
            vector.add(modeling.createLinkOSH('membership', instance.account.getOsh(), instance.availability_domain.getOsh()))
            vector.add(modeling.createLinkOSH('containment', instance.account.getOsh(), host_osh))

        if instance.imagelist:
            vector.add(modeling.createLinkOSH('dependency', host_osh, instance.imagelist.getOsh()))

        for storage_volume in instance.storage_volumes:
            if storage_volume and storage_volume.imagelist:
                vector.add(modeling.createLinkOSH('dependency', host_osh, storage_volume.imagelist.getOsh()))

        if instance.db_server:
            db_osh = modeling.createDatabaseOSH(instance.db_server.db_type,
                                                instance.db_server.db_name,
                                                instance.db_server.getPort(),
                                                instance.db_server.address,
                                                host_osh,
                                                dbVersion=instance.db_server.getVersion(),
                                                edition=instance.db_server.edition)
            vector.add(db_osh)

            endpoint = netutils.Endpoint(instance.db_server.getPort(), netutils.ProtocolType.TCP_PROTOCOL,
                                         instance.db_server.address)
            endpoint_reporter = netutils.EndpointReporter(netutils.ServiceEndpointBuilder())
            endpoint_osh = endpoint_reporter.reportEndpoint(endpoint, host_osh)
            vector.add(endpoint_osh)
            vector.add(modeling.createLinkOSH('usage', db_osh, endpoint_osh))

        return vector

    def reportIpNetwork(self, network):
        if not network:
            raise ValueError("IP Network is not specified")
        vector = ObjectStateHolderVector()
        network_osh = network.build(self.__builder)
        vector.add(network_osh)
        return vector

    def reportInterface(self, interface):
        if not interface:
            raise ValueError("VNIC is not specified")
        vector = ObjectStateHolderVector()
        interface_osh = interface.build(self.__builder)
        vector.add(interface_osh)
        return vector

    def reportIpAddress(self, ip_reservation):
        if not ip_reservation:
            raise ValueError("IP reservation is not specified")
        vector = ObjectStateHolderVector()
        ip_osh = ip_reservation.build(self.__builder)
        vector.add(ip_osh)
        return vector

    def reportShape(self, shape):
        if not shape:
            raise ValueError("Shape is not specified")
        vector = ObjectStateHolderVector()
        shape_osh = shape.build(self.__builder)
        vector.add(shape_osh)
        return vector

    def reportStorageVolume(self, volume):
        if not volume:
            raise ValueError("Volume is not specified")
        vector = ObjectStateHolderVector()
        volume_osh = volume.build(self.__builder)
        vector.add(volume_osh)
        if volume.availability_domain:
            vector.add(modeling.createLinkOSH('membership', volume.availability_domain.getOsh(), volume_osh))
        if volume.account:
            vector.add(modeling.createLinkOSH('membership', volume.account.getOsh(), volume_osh))
        if volume.imagelist:
            vector.add(modeling.createLinkOSH('dependency', volume_osh, volume.imagelist.getOsh()))

        return vector

    def reportRegion(self, region):
        if not region:
            raise ValueError("Region is not specified")
        vector = ObjectStateHolderVector()
        region_osh = region.build(self.__builder)
        vector.add(region_osh)
        return vector

    def reportAvailabilityDomain(self, domain):
        if not domain:
            raise ValueError("Domain is not specified")
        vector = ObjectStateHolderVector()
        domain_osh = domain.build(self.__builder)
        vector.add(domain_osh)
        if domain.region:
            vector.add(modeling.createLinkOSH('containment', domain.region.getOsh(), domain_osh))

        return vector

    def reportStorageAttachment(self, attachement):
        if not attachement:
            raise ValueError("Storage attachement is not specified")
        vector = ObjectStateHolderVector()
        if attachement.instance and attachement.storage_volume:
            vector.add(modeling.createLinkOSH('usage', attachement.instance.getOsh(), attachement.storage_volume.getOsh()))
        return vector

    def reportImage(self, image):
        if not image:
            raise ValueError("Image is not specified")
        vector = ObjectStateHolderVector()
        image_osh = image.build(self.__builder)
        vector.add(image_osh)
        return vector
