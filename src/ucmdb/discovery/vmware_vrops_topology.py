# coding=utf-8

import logger
import modeling
from piezo.discovery import Discovery
from piezo.exceptions import TopologyError
from piezo.ports.requests import RequestException
from piezo.stateholders.oshv import ObjStateHolderVector
from vmware_vrops_rest import VROpsClient
from vmware_vrops_utils import create_baseurl_from_credential
from vmware_vrops_utils import format_credential_port
from vmware_vrops_utils import cast_to_bool
from vmware_vrops_utils import cast_to_int
from vmware_vrops_utils import get_http_credentials
from vmware_vrops_utils import IPServiceEndpoint
from vmware_vrops_utils import is_relevant_resource
from vmware_vrops_utils import Node
from vmware_vrops_utils import OSHVHandler
from vmware_vrops_utils import Parameters
from vmware_vrops_utils import request_adapters
from vmware_vrops_utils import request_related_resources
from vmware_vrops_utils import request_properties
from vmware_vrops_utils import Trigger
from vmware_vrops_utils import VMwareCluster
from vmware_vrops_utils import VMwareDatacenter
from vmware_vrops_utils import VMwareESX
from vmware_vrops_utils import VMwareHypervisor
from vmware_vrops_utils import VMwareVCenter
from vmware_vrops_utils import VMwareVROps
from vmware_vrops_utils import IPAddress
from vmware_vrops_utils import HostResource

VROPS_DEFAULT_PAGESIZE = 100
UCMDB_DEFAULT_CHUNKSIZE = 50
UCMDB_VCENTER_CIT = "vmware_virtual_center"
UCMDB_DATACENTER_CIT = "vmware_datacenter"
UCMDB_CLUSTER_CIT = "vmware_cluster"
UCMDB_HYPERVISOR_CIT = "virtualization_layer"
VROPS_VCENTER_ADAPTERKIND = "VMWARE"
VROPS_VCENTER_KEY = "VCENTER"
VROPS_DATACENTER_KEY = "Datacenter"
VROPS_CLUSTER_KEY = "ClusterComputeResource"
VROPS_ESX_KEY = "HostSystem"
VROPS_VM_KEY = "VirtualMachine"
USAGE = "usage"
MANAGE = "manage"
MEMBERSHIP = "membership"
CONTAINMENT = "containment"
EXECUTION_ENVIRONMENT = "execution_environment"


def create_vm_osh(oshv_handler, input_hypervisor_osh, virtual_machine, virtual_machine_properties):
    """Create Virtual Machine as Node OSH connected to parent Hypervisor."""
    tmp_add_oshv = ObjStateHolderVector()
    tmp_add_oshv.append(input_hypervisor_osh)
    try:
        resource_id = virtual_machine['identifier']
        vm_name = virtual_machine['resourceKey']['name']
    except KeyError as err:
        logger.error(err)
        msg = "Response does not meet expected format to create VirtualMachine."
        raise KeyError(msg)
    virtual_machine_osh = Node.create_osh(vm_name, virtual_machine_properties, resource_id)
    tmp_add_oshv.append(virtual_machine_osh)
    tmp_add_oshv.append(modeling.createLinkOSH(EXECUTION_ENVIRONMENT,
                                               input_hypervisor_osh.get_osh(),
                                               virtual_machine_osh.get_osh()))
    host_resource_properties = {'vm_num_cpus':None, 'vm_memory_size_kb':None, 'vm_cpu_reservation':None, 'vm_cpu_limit':None, 'vm_memory_reservation':None, 'vm_memory_limit':None}
    vm_properties = virtual_machine_properties['property']
    for vm_property in vm_properties:
        if vm_property['name'] == 'summary|guest|ipAddress':
            ip_address = vm_property['value']
            if ip_address:
                ip_osh = IPAddress.create_osh(ip_address)
                tmp_add_oshv.append(ip_osh)
                tmp_add_oshv.append(modeling.createLinkOSH(CONTAINMENT,
                                                           virtual_machine_osh.get_osh(),
                                                           ip_osh.get_osh()))
        elif vm_property['name'] == 'config|hardware|numCpu':
            host_resource_properties.update({"vm_num_cpus" :vm_property['value']})
        elif vm_property['name'] == 'config|hardware|memoryKB':
            host_resource_properties.update({"vm_memory_size_kb": vm_property['value']})
        elif vm_property['name'] == 'cpu|reservation':
            host_resource_properties.update({"vm_cpu_reservation": vm_property['value']})
        elif vm_property['name'] == 'cpu|limit':
            host_resource_properties.update({"vm_cpu_limit": vm_property['value']})
        elif vm_property['name'] == 'mem|host_reservation':
            host_resource_properties.update({"vm_memory_reservation": vm_property['value']})
        elif vm_property['name'] == 'mem|host_limit':
            host_resource_properties.update({"vm_memory_limit": vm_property['value']})
    host_resource_osh = HostResource.create_osh(vm_name, host_resource_properties['vm_num_cpus'], host_resource_properties['vm_memory_size_kb'], host_resource_properties['vm_cpu_reservation'], host_resource_properties['vm_cpu_limit'], host_resource_properties['vm_memory_reservation'], host_resource_properties['vm_memory_limit'])
    host_resource_osh.setContainer(virtual_machine_osh.get_osh())
    tmp_add_oshv.append(host_resource_osh)
    oshv_handler.add_oshv_with_dependency(tmp_add_oshv)


def create_hypervisor_osh(oshv_handler, input_osh, esx_server):
    """Create Hypervisor OSH with ESX and link to parent cluster or
    datacenter."""
    tmp_add_oshv = ObjStateHolderVector()
    tmp_add_oshv.append(input_osh)
    try:
        hypervisor_id = esx_server['identifier']
        esx_name = esx_server['resourceKey']['name']
    except KeyError as err:
        logger.error(err)
        msg = "Response does not meet expected format to create Hypervisor/ESX."
        raise KeyError(msg)
    esx_osh = VMwareESX.create_osh(esx_name)
    tmp_add_oshv.append(esx_osh)
    hypervisor_osh = VMwareHypervisor.create_osh(esx_osh, hypervisor_id)
    tmp_add_oshv.append(hypervisor_osh)
    if input_osh.getObjectClass() == UCMDB_DATACENTER_CIT:
        link = CONTAINMENT
    else:
        link = MEMBERSHIP
    tmp_add_oshv.append(modeling.createLinkOSH(link, input_osh.get_osh(),
                                               hypervisor_osh.get_osh()))
    oshv_handler.add_oshv_with_dependency(tmp_add_oshv)
    return hypervisor_osh


def create_cluster_osh(oshv_handler, input_osh, cluster):
    """Create cluster OSHs with link to parent datacenter."""
    tmp_add_oshv = ObjStateHolderVector()
    tmp_add_oshv.append(input_osh)
    try:
        cluster_id = cluster['identifier']
        cluster_name = cluster['resourceKey']['name']
    except KeyError as err:
        logger.error(err)
        msg = "Response does not meet expected format to create Cluster."
        raise KeyError(msg)
    cluster_osh = VMwareCluster.create_osh(input_osh, cluster_id, cluster_name)
    tmp_add_oshv.append(cluster_osh)
    oshv_handler.add_oshv_with_dependency(tmp_add_oshv)
    return cluster_osh


def create_datacenter_osh(oshv_handler, input_vcenter_osh, datacenter):
    """Create datacenter OSHs with link to parent vCenter."""
    tmp_add_oshv = ObjStateHolderVector()
    tmp_add_oshv.append(input_vcenter_osh)
    try:
        datacenter_id = datacenter['identifier']
        datacenter_name = datacenter['resourceKey']['name']
    except KeyError as err:
        logger.error(err)
        msg = "Response does not meet expected format to create Datacenter."
        raise KeyError(msg)
    datacenter_osh = VMwareDatacenter.create_osh(datacenter_id, datacenter_name)
    tmp_add_oshv.append(datacenter_osh)
    tmp_add_oshv.append(modeling.createLinkOSH(MANAGE,
                                               input_vcenter_osh.get_osh(),
                                               datacenter_osh.get_osh()))
    oshv_handler.add_oshv_with_dependency(tmp_add_oshv)
    return datacenter_osh


def create_vcenter_osh(oshv_handler, input_vrops_osh, adapter):
    """Create VMware VirtualCenter OSH with host node and connect to parent
    vROps."""
    tmp_add_oshv = ObjStateHolderVector()
    tmp_add_oshv.append(input_vrops_osh)
    try:
        vcenter_id = adapter['id']
        host_name = adapter['resourceKey']['name']
    except KeyError as err:
        logger.error(err)
        msg = "Response does not meet expected format to create vCenter."
        raise KeyError(msg)
    node_osh = Node.create_osh(host_name)
    tmp_add_oshv.append(node_osh)

    vcenter_osh = VMwareVCenter.create_osh(node_osh, vcenter_id)
    tmp_add_oshv.append(vcenter_osh)
    tmp_add_oshv.append(modeling.createLinkOSH(MANAGE,
                                               input_vrops_osh.get_osh(),
                                               vcenter_osh.get_osh()))
    oshv_handler.add_oshv_with_dependency(tmp_add_oshv)
    return vcenter_osh


def create_vcenter_topology(oshv_handler, client, input_osh):
    """Create vCenter topology: Load related resources of given CI from vROps
    API and process related resources. Call function recursively for each of
    those processed resources to create whole vCenter topology."""
    input_osh_vropsid = input_osh.getAttributeValue("data_externalid")
    input_osh_name = input_osh.getAttributeValue("name") or ""
    input_type = input_osh.getObjectClass()

    logger.info("Requesting related resources from {} with id {} and name <{}>"
                .format(input_type, input_osh_vropsid, input_osh_name))

    related_resources = request_related_resources(client, input_osh_vropsid)
    for resource in related_resources:
        logger.debug("resource:", resource)
        new_osh = None
        if input_type == UCMDB_VCENTER_CIT:
            if is_relevant_resource(resource, VROPS_DATACENTER_KEY):
                new_osh = create_datacenter_osh(oshv_handler, input_osh,
                                                resource)
        elif input_type == UCMDB_DATACENTER_CIT:
            if is_relevant_resource(resource, VROPS_CLUSTER_KEY):
                new_osh = create_cluster_osh(oshv_handler, input_osh, resource)
            elif is_relevant_resource(resource, VROPS_ESX_KEY):
                new_osh = create_hypervisor_osh(oshv_handler, input_osh,
                                                resource)
        elif input_type == UCMDB_CLUSTER_CIT:
            if is_relevant_resource(resource, VROPS_ESX_KEY):
                new_osh = create_hypervisor_osh(oshv_handler, input_osh,
                                                resource)
        elif input_type == UCMDB_HYPERVISOR_CIT:
            if is_relevant_resource(resource, VROPS_VM_KEY):
                resource_id = resource['identifier']
                vm_properties = request_properties(client, resource_id)
                logger.debug("vm_properties:", vm_properties)
                create_vm_osh(oshv_handler, input_osh, resource, vm_properties)
        if new_osh is not None:
            create_vcenter_topology(oshv_handler, client, new_osh)


def update_vrops_ci(oshv_handler, ip_address, port, credentials_id, base_url,
                    node_name):
    """Update input CIs of discovery job to keep its data up-to-date"""
    tmp_add_oshv = ObjStateHolderVector()
    node_osh = Node.create_osh(node_name)
    tmp_add_oshv.append(node_osh)

    ip_service_endpoint_osh = IPServiceEndpoint.create_osh(node_osh, port,
                                                           ip_address)
    tmp_add_oshv.append(ip_service_endpoint_osh)
    vrops_osh = VMwareVROps.create_osh(node_osh, base_url, ip_address,
                                       credentials_id)
    tmp_add_oshv.append(vrops_osh)
    link = modeling.createLinkOSH(USAGE, vrops_osh.get_osh(),
                                  ip_service_endpoint_osh.get_osh())
    tmp_add_oshv.append(link)
    oshv_handler.add_oshv_with_dependency(tmp_add_oshv)
    return vrops_osh


def discovery(add_oshv, _):
    """Start discovering the vROps topology. Load trigger and parameter data
    as well as http credentials to authenticate at vROps API and request
    vCenter data."""
    parameters = Parameters()
    trigger = Trigger()
    application_ip = trigger.get(trigger.IP_ADDRESS)
    host_name = trigger.get(trigger.HOSTNAME)
    discover_vcenter_only = parameters.get(parameters.DISCOVER_VCENTER_ONLY)
    discover_vcenter_only = cast_to_bool(discover_vcenter_only)
    vrops_page_size = parameters.get(parameters.VROPS_PAGESIZE)
    vrops_page_size = cast_to_int(parameters.VROPS_PAGESIZE, vrops_page_size,
                                  VROPS_DEFAULT_PAGESIZE)
    ucmdb_chunk_size = parameters.get(parameters.CHUNKSIZE)
    ucmdb_chunk_size = cast_to_int(parameters.CHUNKSIZE, ucmdb_chunk_size,
                                   UCMDB_DEFAULT_CHUNKSIZE)
    oshv_handler = OSHVHandler(ucmdb_chunk_size, add_oshv)

    credentials_id = trigger.get(trigger.VROPS_CREDENTIALSID)
    if credentials_id == 'NA':
        credentials_id = None
    credentials = get_http_credentials(credentials_id)

    # Test credentials
    for credential in credentials:
        base_url = create_baseurl_from_credential(credential, application_ip)
        client = VROpsClient(base_url, vrops_page_size)
        try:
            client.authenticate(credential.username, credential.password)
        except (RequestException, ValueError) as err:
            logger.info(err.message)
        else:
            valid_credential = credential
            break
    else:
        msg = 'No valid credentials found.'
        raise TopologyError(msg)

    # Discover topology
    port = format_credential_port(valid_credential.port,
                                  valid_credential.secure)
    input_vrops_osh = update_vrops_ci(oshv_handler, application_ip,
                                      port, valid_credential.credential_id,
                                      base_url, host_name)
    logger.info("Request vCenter information")
    adapters = request_adapters(client, VROPS_VCENTER_ADAPTERKIND)
    for adapter in adapters:
        logger.info("Create vCenter {}".format(adapter['resourceKey']['name']))
        vcenter_osh = create_vcenter_osh(oshv_handler, input_vrops_osh,
                                         adapter)
        if not discover_vcenter_only:
            msg = "Create topology of vCenter {}"
            logger.info(msg.format(adapter['resourceKey']['name']))
            create_vcenter_topology(oshv_handler, client, vcenter_osh)

DiscoveryMain = Discovery(discovery)
