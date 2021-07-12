# coding=utf-8

import logger
from java.lang import Boolean
from piezo.discovery.credentials import get_credential
from piezo.discovery.credentials import get_credentials
from piezo.discovery.credentials.types import TYPES
from piezo.exceptions import CredentialError
from piezo.services import get_framework
from piezo.stateholders.osh import ObjStateHolder


class Parameters(object):
    """Class which returns adapter parameters. Parameter names are
    predefined.
    """

    CHUNKSIZE = 'chunkSize'
    VROPS_PAGESIZE = 'vropsPageSize'
    DISCOVER_VCENTER_ONLY = 'discoverVCenterOnly'

    def __init__(self):
        self.framework = get_framework()

    def get(self, name):
        """Return adapter parameter with given name or None if parameter is not
        defined.
        """
        return self.framework.getParameter(name)


class Trigger(object):
    """Class which returns trigger data. Data names are predefined."""

    VROPS_ID = 'source_vrops_id'
    GLOBAL_ID = 'source_global_id'
    IP_ADDRESS = 'ip_address'
    VROPS_CREDENTIALSID = 'vrops_credentialsid'
    HOSTNAME = 'host_name'

    def __init__(self):
        self.framework = get_framework()

    def get(self, name):
        """Return trigger data with given name or None if data is not
        defined.
        """
        return self.framework.getTriggerCIData(name)


class OSHVHandler(object):
    """Class which adds objects to the object state holder vector and sends
    the vector in chunks to the UCMDB."""

    def __init__(self, chunk_size, add_oshvector):
        self.chunk_size = chunk_size
        self.add_oshvector = add_oshvector
        self.framework = get_framework()

    def flush(self):
        """Check whether chunk size has been reached and send chunk of data
        accorcingly."""
        if len(self.add_oshvector) >= self.chunk_size:
            logger.debug(len(self.add_oshvector))
            self.framework.sendObjects(self.add_oshvector.oshv)
            self.framework.flushObjects()
            self.add_oshvector.clear()

    def add_osh(self, osh):
        """Add single ObjectStateHolder to the vector and send vector if
        chunk size has been reached."""
        self.add_oshvector.append(osh)
        self.flush()

    def add_oshv(self, oshv):
        """Add all objects of the given list to the vector and send vector if
        chunk size has been reached."""
        for osh in oshv:
            self.add_oshvector.append(osh)
            self.flush()

    def add_oshv_with_dependency(self, oshv):
        """Add all objects of the given vector without chunking in between
        to ensure all ObjectStateHolders are sent in the same chunk to prevent
        reconciliation errors."""
        for osh in oshv:
            self.add_oshvector.append(osh)
        self.flush()


class VMwareHypervisor(object):
    """Hypervisor OSH"""
    HYPERVISOR_CIT = "virtualization_layer"
    HYPERVISOR_DISCOVERED_NAME = "Virtualization Layer Software"

    @classmethod
    def create_osh(cls, esx_osh, resource_id):
        hypervisor_osh = ObjStateHolder(cls.HYPERVISOR_CIT)
        hypervisor_attr = {
            "data_externalid": resource_id,
            "root_container": esx_osh,
            "discovered_product_name": cls.HYPERVISOR_DISCOVERED_NAME,
        }
        hypervisor_osh.update(hypervisor_attr.iteritems())
        return hypervisor_osh


class VMwareESX(object):
    """ESX OSH"""
    ESX_CIT = "vmware_esx_server"

    @classmethod
    def create_osh(cls, resource_name):
        esx_osh = ObjStateHolder(cls.ESX_CIT)
        esx_attr = {
            "name": resource_name,
        }
        esx_osh.update(esx_attr.iteritems())
        return esx_osh


class VMwareCluster(object):
    """Cluster OSH"""
    CLUSTER_CIT = "vmware_cluster"

    @classmethod
    def create_osh(cls, input_osh, resource_id, resource_name):
        cluster_osh = ObjStateHolder(cls.CLUSTER_CIT)
        attr = {
            "data_externalid": resource_id,
            "name": resource_name,
            "root_container": input_osh,
        }
        cluster_osh.update(attr.iteritems())
        return cluster_osh


class VMwareDatacenter(object):
    """Datacenter OSH"""
    DATACENTER_CIT = 'vmware_datacenter'

    @classmethod
    def create_osh(cls, datacenter_id, datacenter_name):
        datacenter_osh = ObjStateHolder(cls.DATACENTER_CIT)
        attr = {
            "data_externalid": datacenter_id,
            "name": datacenter_name,
        }
        datacenter_osh.update(attr.iteritems())
        return datacenter_osh


class Node(object):
    """Node OSH"""
    NODE_CIT = "node"
    WINDOWS_CIT = 'nt'
    UNIX_CIT = 'unix'

    @classmethod
    def create_osh(cls, name, properties=None, node_id=None):
        CIT = cls.NODE_CIT
        vm_os_type = None
        if properties:
            vm_properties = properties['property']

            for vm_property in vm_properties:
                if vm_property['name'] == 'summary|guest|fullName':
                    vm_os_type = vm_property['value']
            if vm_os_type:
                if 'Windows' in vm_os_type:
                    CIT = cls.WINDOWS_CIT
                elif 'Linux' in vm_os_type:
                    CIT = cls.UNIX_CIT
        node_osh = ObjStateHolder(CIT)
        node_attr = {
            "name": name,
            "os_description": vm_os_type
        }
        node_osh.update(node_attr.iteritems())
        if node_id is not None:
            node_osh["data_externalid"] = node_id
        return node_osh

class IPAddress(object):
    """IPADDRESS OSH"""
    IPADDRESS_CIT = "ip_address"

    @classmethod
    def create_osh(cls, name):
        IPAddress_osh = ObjStateHolder(cls.IPADDRESS_CIT)
        ipaddress_attr = {
            "name": name,
        }
        IPAddress_osh.update(ipaddress_attr.iteritems())
        return IPAddress_osh

class HostResource(object):
    """VMWARE HOST RESOURCE OSH"""
    VMWARE_HOST_RESOURCE_CIT = "vmware_host_resource"

    @classmethod
    def create_osh(cls, name, vm_num_cpus, vm_memory_size, vm_cpu_reservation, vm_cpu_limit, vm_memory_reservation, vm_memory_limit):
        HostResource_osh = ObjStateHolder(cls.VMWARE_HOST_RESOURCE_CIT)
        hostresource_attr = {
            "name": name,
        }
        HostResource_osh.update(hostresource_attr.iteritems())
        if vm_num_cpus:
            HostResource_osh.setIntegerAttribute('vm_num_cpus', int(vm_num_cpus.split('.')[0]))
        if vm_memory_size:
            vm_memory_size_mb = int(vm_memory_size.split('.')[0])/1024
            HostResource_osh.setIntegerAttribute('vm_memory_size', vm_memory_size_mb)
        if vm_cpu_reservation:
            HostResource_osh.setLongAttribute('vm_cpu_reservation', long(vm_cpu_reservation.split('.')[0]))
        if vm_cpu_limit:
            HostResource_osh.setLongAttribute('vm_cpu_limit', long(vm_cpu_limit.split('.')[0]))
        if vm_memory_reservation:
            HostResource_osh.setLongAttribute('vm_memory_reservation', long(vm_memory_reservation.split('.')[0]))
        if vm_memory_limit:
            HostResource_osh.setLongAttribute('vm_memory_limit', long(vm_memory_limit.split('.')[0]))
        return HostResource_osh

class VMwareVCenter(object):
    """vCenter OSH"""
    VCENTER_CIT = "vmware_virtual_center"
    VCENTER_PRODUCT_NAME = "vmware_virtual_center"
    VCENTER_DISCOVERED_NAME = "VMware VirtualCenter"

    @classmethod
    def create_osh(cls, node_osh, vcenter_id):
        vcenter_osh = ObjStateHolder(cls.VCENTER_CIT)
        vcenter_attr = {
            "data_externalid": vcenter_id,
            "root_container": node_osh,
            "discovered_product_name": cls.VCENTER_DISCOVERED_NAME,
            "product_name": cls.VCENTER_PRODUCT_NAME,
        }
        vcenter_osh.update(vcenter_attr.iteritems())
        return vcenter_osh


class IPServiceEndpoint(object):
    """IP Service Endpoint OSH"""
    IPSERVICEENDPOINT_CIT = "ip_service_endpoint"

    @classmethod
    def create_osh(cls, container, port, ip_address):
        ip_service_endpoint_osh = ObjStateHolder(cls.IPSERVICEENDPOINT_CIT)
        ip_service_endpoint_attr = {
            "bound_to_ip_address": ip_address,
            "root_container": container,
            "network_port_number": port,
            "port_type": "tcp",
            "ipserver_address": "{}:{}".format(ip_address, port)
        }
        ip_service_endpoint_osh.update(ip_service_endpoint_attr.iteritems())
        return ip_service_endpoint_osh


class VMwareVROps(object):
    """vROps OSH"""
    VROPS_CIT = "vmware_vrealize_operations"
    VROPS_DISCOVERED_NAME = "VMware vRealize Operations"

    @classmethod
    def create_osh(cls, node_osh, base_url, application_ip, credentials_id):
        vrops_osh = ObjStateHolder(cls.VROPS_CIT)
        vrops_attr = {
            "root_container": node_osh,
            "connection_url": base_url,
            "application_ip": application_ip,
            "credentials_id": credentials_id,
            "discovered_product_name": cls.VROPS_DISCOVERED_NAME,
        }
        vrops_osh.update(vrops_attr.iteritems())
        return vrops_osh


def is_relevant_resource(resource, vrops_key):
    """Filter output CIs from related CIs of API request."""
    if resource['resourceKey']['resourceKindKey'] == vrops_key:
        return True
    else:
        return False


def request_related_resources(client, trigger_id):
    """Request related resources of the input vCenter."""
    page_size = client.page_size
    api_request_page = 0
    while True:
        response = client.get_related_resources(trigger_id, api_request_page)
        number_resources = response['pageInfo']['totalCount']
        resources = response['resourceList']
        for resource in resources:
            yield resource
        api_request_page += 1
        if number_resources <= (page_size * api_request_page):
            break

def request_properties(client, resource_id):
    """Request properties of resource"""
    response = client.get_resource_properties(resource_id)
    if response:
        return response

def request_adapters(client, adapter_kind=None):
    """Request all adapters or adapters from a specific kind by client."""
    response = client.get_adapters(adapter_kind)
    adapters = response['adapterInstancesInfoDto']
    for adapter in adapters:
        yield adapter


def cast_to_int(param_name, string, default):
    """Format adapter parameter to integer."""
    try:
        result_int = int(string)
    except ValueError:
        logger.warn('Adapter parameter {} is not defined or not an '
                    'integer.'.format(param_name))
        result_int = default
    return result_int


def cast_to_bool(string):
    """Format adapter parameter to boolean."""
    return Boolean.parseBoolean(string)


def get_http_credentials(credentials_id=None):
    """Load credentials of http protocol."""
    if credentials_id is not None:
        try:
            yield get_credential(credentials_id)
        except CredentialError:
            pass
    for credential in get_credentials(TYPES['HTTP']):
        if credential.credential_id != credentials_id:
            yield credential


def create_baseurl_from_credential(credential, hostname):
    """Return baseurl of vrops for testing the credentials."""
    port = credential.port
    if credential.secure:
        protocol = 'https'
    else:
        protocol = 'http'
    if port is None:
        base_url = '{}://{}/suite-api/api'.format(protocol, hostname)
    else:
        base_url = '{}://{}:{}/suite-api/api'.format(protocol, hostname, port)
    return base_url


def format_credential_port(port, secure):
    """Format port to integer"""
    if port is None:
        if secure:
            port = 443
        else:
            port = 80
    else:
        port = int(port)
    return port
