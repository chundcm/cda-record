import modeling
import ip_addr
import netutils
import logger
import re

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector


class Tenant:
    def __init__(self, id):
        self.id = id

    def report(self, endpoint):
        vector = ObjectStateHolderVector()
        tenant_osh = ObjectStateHolder("azure_tenant")
        tenant_osh.setAttribute("tenant_id", self.id)
        vector.add(tenant_osh)

        uri_osh = ObjectStateHolder("uri_endpoint")
        uri_osh.setAttribute("uri", endpoint)
        vector.add(uri_osh)
        vector.add(modeling.createLinkOSH("usage", tenant_osh, uri_osh))
        return tenant_osh, vector


class Subscription:
    def __init__(self, id):
        self.id = id
        self.name = None

    def report(self, container):
        subscription_osh = ObjectStateHolder("azure_subscription")
        subscription_osh.setAttribute("subscription_id", self.id)
        subscription_osh.setAttribute("name", self.name)
        subscription_osh.setContainer(container)
        return subscription_osh


class ResourceGroup:
    def __init__(self, name):
        self.name = name
        self.id = None
        self.subscription_id = None
        self.location = None

    def report(self, container):
        rg_osh = ObjectStateHolder("azure_resource_group")
        rg_osh.setAttribute("name", self.name)
        rg_osh.setContainer(container)
        return rg_osh


class VM:
    def __init__(self, name):
        self.name = name
        self.id = None
        self.resource_group_id = None
        self.location = None
        self.interface_ids = []
        self.hardware_profile = None
        self.storage_uri = None
        self.vm_size = None
        self.os_disk = None
        self.data_disks = []
        self.osType=None
        self.osDescription=None
        self.discoveredDescription=None
        self.discoveredOSVersion=None
        self.location=None

    def report(self, resource_group_osh, ip_oshs, dict_storage_osh, dict_interface_osh):
        vector = ObjectStateHolderVector()

        osh_type = "host_node"
        os_family = None
        if self.osType=='Windows':
            osh_type = "nt"
            os_family = "windows"
        elif self.osType=='Linux':
            osh_type = "unix"
            os_family = "unix"

        host_osh = ObjectStateHolder(osh_type)
        host_osh.setAttribute("name", self.name)
        host_osh.setAttribute("platform_vendor","Azure")
        host_osh.setAttribute("os_description", self.osDescription)
        host_osh.setAttribute("discovered_location", self.location)
        host_osh.setAttribute("discovered_description", self.discoveredDescription)
        host_osh.setAttribute("discovered_os_version", self.discoveredOSVersion)
        if os_family:
            host_osh.setAttribute("os_family", os_family)
        if self.id:
            host_osh.setAttribute("cloud_instance_id",str(self.id))

        vector.add(host_osh)
        if resource_group_osh:
            vector.add(modeling.createLinkOSH("membership", resource_group_osh, host_osh))
        if ip_oshs:
            for ip_osh in ip_oshs:
                vector.add(modeling.createLinkOSH("containment", host_osh, ip_osh))
        if self.hardware_profile:
            profile_osh = ObjectStateHolder("azure_config")
            profile_osh.setAttribute("name", self.hardware_profile)
            profile_osh.setContainer(host_osh)
            if self.vm_size:
                logger.debug("vm_size:", self.vm_size)
                profile_osh.setAttribute("data_note", self.vm_size)
            vector.add(profile_osh)

        if self.os_disk:
            vector.addAll(self.os_disk.report(host_osh, dict_storage_osh))
        if self.data_disks:
            for data_disk in self.data_disks:
                vector.addAll(data_disk.report(host_osh, dict_storage_osh))
        self.report_interface(vector, host_osh, dict_interface_osh)
        return vector

    def report_interface(self, vector, host_osh, dict_interface_osh):
        for interface_id in self.interface_ids:
            if dict_interface_osh.has_key(interface_id):
                interface_osh = dict_interface_osh[interface_id]
                vector.add(interface_osh)
                interface_osh.setContainer(host_osh)
                vector.add(modeling.createLinkOSH("composition", host_osh, interface_osh))

class Network:
    def __init__(self, name):
        self.name = name
        self.location = None
        self.id = None
        self.resource_group_id = None
        self.address_prefixes = None
        self.subnets = []

    def report(self, container):
        network_osh = ObjectStateHolder("azure_virtual_network")
        network_osh.setAttribute("name", self.name)
        network_osh.setAttribute("address_prefixes", str(self.address_prefixes))
        network_osh.setContainer(container)
        return network_osh


class Subnet:
    def __init__(self, name):
        self.name = name
        self.id = None
        self.resource_group_id = None
        self.address_prefix = None
        self.address_prefix_length = 0
        self.ip_ids = []

    def report(self, network_osh):
        vector = ObjectStateHolderVector()
        netmask = None
        if ip_addr.isValidIpAddress(self.address_prefix):
            try:
                netmask = netutils.decodeSubnetMask(self.address_prefix_length)
            except:
                exInfo = logger.prepareJythonStackTrace('')
                logger.error("Failed to get decode subnet mask:", exInfo)

        if netmask:
            subnet_osh = modeling.createNetworkOSH(self.address_prefix, netmask)
            if self.id:
                subnet_osh.setAttribute('cloud_resource_identifier', self.id)
            vector.add(subnet_osh)
            vector.add(modeling.createLinkOSH("containment", network_osh, subnet_osh))
            return subnet_osh, vector


class Interface:
    def __init__(self, name):
        self.name = name
        self.id = None
        self.mac_address = None
        self.ip_config = None

    def report(self):
        azure_interface_osh = ObjectStateHolder("interface")
        azure_interface_osh.setAttribute("interface_name", self.name)
        ip_osh = self.ip_config.report()
        return ip_osh, azure_interface_osh


class IPAddress:
    def __init__(self, name):
        self.name = name
        self.id = id
        self.location = None
        self.ip_address = None
        self.ip_configuration_id = None

    def report(self):
        return modeling.createIpOSH(self.ip_address)


class IPConfiguration:
    def __init__(self, name):
        self.name = name
        self.id = None
        self.ip_address = None
        self.public_ip_id = None
        self.subnet_id = None

    def report(self):
        return modeling.createIpOSH(self.ip_address)


class StorageAccount:
    def __init__(self, name):
        self.name = name
        self.id = None
        self.location = None
        self.resource_group_id = None

    def report(self, container):
        storage_account_osh = ObjectStateHolder("azure_storage_account")
        storage_account_osh.setAttribute("name", self.name)
        storage_account_osh.setContainer(container)
        return storage_account_osh


    def report_endpoint(self, name, uri):
        uri_endpoint_osh = ObjectStateHolder('uri_endpoint')
        uri_endpoint_osh.setAttribute('uri', uri)
        uri_endpoint_osh.setAttribute('name', name)
        return uri_endpoint_osh


class Disk:
    def __init__(self, name):
        self.name = name
        self.vhd_uri = None

    def calculate_storage(self):
        if self.vhd_uri:
            match = re.search("https?://([\w.]+).core.windows.net/", self.vhd_uri)
            if match:
                vhd_info = match.group(1).strip().split(".")
                if vhd_info:
                    return vhd_info[0], vhd_info[1]
        return None, None

    def build(self, container, dict_storage_account_osh):
        vector = ObjectStateHolderVector()
        disk_osh = ObjectStateHolder("disk_device")
        disk_osh.setAttribute("name", self.name)
        disk_osh.setContainer(container)
        vector.add(disk_osh)

        if self.vhd_uri:
            vhd_uri_osh = ObjectStateHolder("uri_endpoint")
            vhd_uri_osh.setAttribute("uri", self.vhd_uri)
            logger.debug("vhd_uri", self.vhd_uri)
            storage_account, storage_type = self.calculate_storage()
            if storage_type:
                vhd_uri_osh.setAttribute("name", storage_type)
            vector.add(vhd_uri_osh)
            vector.add(modeling.createLinkOSH("usage", disk_osh, vhd_uri_osh))

            if storage_account:
                storage_account_osh = dict_storage_account_osh.get(storage_account, None)
                if storage_account_osh:
                    vector.add(modeling.createLinkOSH("containment", storage_account_osh, vhd_uri_osh))

        return disk_osh, vector


class OSDisk(Disk):
    def __init__(self, name):
        Disk.__init__(self, name)
        self.os_type = None

    def report(self, container, dict_storage_account_osh):
        disk_osh, vector = self.build(container, dict_storage_account_osh)
        vector.add(disk_osh)
        return vector


class DataDisk(Disk):
    def __init__(self, name):
        Disk.__init__(self, name)
        self.disk_size = None

    def report(self, container, dict_storage_account_osh):
        disk_osh, vector = self.build(container, dict_storage_account_osh)
        if self.disk_size:
            disk_osh.setAttribute("logicalvolume_size", self.disk_size)
        #    vector.add(disk_osh)
        return vector

class SqlDatabase():
    def __init__(self, name, id):
        self.name = name
        self.resource_group_id = None
        self.server_address = None
        if id:
            self.id = id
        else:
            raise ValueError('Key id can not be null')

    def report(self, framework, resource_group_osh):
        vector = ObjectStateHolderVector()
        db_osh = ObjectStateHolder('sqlserver_db_service')
        db_osh.setAttribute("name", self.name)
        db_osh.setAttribute("cloud_resource_identifier", self.id)
        db_osh.setAttribute("service_vendor", 'azure')
        db_osh.setAttribute("service_probename", framework.getDestinationAttribute('probeName'))
        sqlserver_address = self.server_address + '.database.windows.net'
        logger.debug("server address:", self.server_address + '.database.windows.net')
        db_osh.setAttribute('endpoint_address', sqlserver_address)
        db_osh.setIntegerAttribute('endpoint_port', 1433)
        vector.add(db_osh)
        vector.add(modeling.createLinkOSH("membership", resource_group_osh, db_osh))
        return vector



