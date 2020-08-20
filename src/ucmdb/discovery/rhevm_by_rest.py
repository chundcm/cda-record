# coding=utf-8
import sys
import logger
import modeling
import rhevm_discoverer as rhevm_discoverer_v4
import rhevm_discoverer_v3

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector


def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    ip = Framework.getDestinationAttribute('ip')
    protocols = Framework.getAvailableProtocols(ip, 'http')

    datacenter_dict = {}
    cluster_dict = {}
    host_dict = {}
    vm_pool_dict = {}
    storage_dict = {}
    network_dict = {}
    vnic_profile_dict = {}
    disk_dict = {}

    if len(protocols) == 0:
        msg = 'Protocol not defined or IP out of protocol network range'
        logger.reportWarning(msg)
        logger.error(msg)
        return OSHVResult

    for protocol in protocols:
        try:
            logger.debug('connect with protocol:', protocol)
            rhevm_discoverer = rhevm_discoverer_v4
            client = rhevm_discoverer.RHEVMClient(Framework, protocol)
            client.authenticate()
            # if access via token is possible, then client.token is not null at this point
            rhevm_instance = rhevm_discoverer.RHEVMDiscoverer(client).discover()
            rhevm_osh = rhevm_instance.build()
            rhevm_host_osh = modeling.createHostOSH(ip)
            rhevm_osh.setContainer(rhevm_host_osh)
            logger.debug("rhevm_instance.major_version:", rhevm_instance.major_version)
            if rhevm_instance.major_version == '3':
                logger.debug("api version 3")
                rhevm_discoverer = rhevm_discoverer_v3

            OSHVResult.add(rhevm_host_osh)
            OSHVResult.add(rhevm_osh)
            OSHVResult.addAll(get_data_centers(client, rhevm_discoverer, rhevm_osh, datacenter_dict))
            OSHVResult.addAll(get_clusters(client, rhevm_discoverer, datacenter_dict, cluster_dict))
            OSHVResult.addAll(get_networks(client, rhevm_discoverer, datacenter_dict, network_dict))

            OSHVResult.addAll(get_vm_pools(client, rhevm_discoverer, cluster_dict, vm_pool_dict))
            OSHVResult.addAll(get_vnic_profiles(client, rhevm_discoverer, network_dict, vnic_profile_dict))

            OSHVResult.addAll(get_storage_domains(client, rhevm_discoverer, datacenter_dict, storage_dict))
            OSHVResult.addAll(get_disks(client, rhevm_discoverer, storage_dict, disk_dict))

            OSHVResult.addAll(get_hosts(client, rhevm_discoverer, network_dict, host_dict))
            OSHVResult.addAll(get_vms(client, rhevm_discoverer, host_dict, cluster_dict, vm_pool_dict, vnic_profile_dict, disk_dict))

        except:
            strException = str(sys.exc_info()[1])
            excInfo = logger.prepareJythonStackTrace('')
            logger.debug(strException)
            logger.debug(excInfo)
            pass

    reportError = OSHVResult.size() == 0
    if reportError:
        msg = 'Failed to connect using all protocols'
        logger.reportError(msg)
        logger.error(msg)
    return OSHVResult


def get_data_centers(client, rhevm_discoverer, rhevm_osh, datacenter_dict):
    vector = ObjectStateHolderVector()
    try:
        datacenters = rhevm_discoverer.DataCenterDiscoverer(client).discover()
        for datacenter in datacenters:
            datacenter_osh = datacenter.build()
            datacenter_dict[datacenter.id] = datacenter_osh
            vector.add(datacenter_osh)
            vector.add(modeling.createLinkOSH('manage', rhevm_osh, datacenter_osh))
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning('Failed to discover data center.')
    finally:
        return vector


def get_clusters(client, rhevm_discoverer, datacenter_dict, cluster_dict):
    vector = ObjectStateHolderVector()
    try:
        clusters = rhevm_discoverer.ClusterDiscoverer(client).discover()
        for cluster in clusters:
            cluster_osh = cluster.build()
            cluster_osh.setContainer(datacenter_dict.get(cluster.datacenter_id))
            cluster_dict[cluster.id] = cluster_osh
            vector.add(cluster_osh)
            # OSHVResult.add(modeling.createLinkOSH('manage', rhevm_osh, cluster_osh))
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning('Failed to discover cluster.')
    finally:
        return vector


def get_networks(client, rhevm_discoverer, datacenter_dict, network_dict):
    vector = ObjectStateHolderVector()
    try:
        networks = rhevm_discoverer.NetworkDiscoverer(client).discover()
        for network in networks:
            network_osh = network.build()
            network_osh.setContainer(datacenter_dict.get(network.datacenter_id))
            network_dict[network.id] = network_osh
            vector.add(network_osh)
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning('Failed to discover network.')
    finally:
        return vector


def get_vm_pools(client, rhevm_discoverer, cluster_dict, vm_pool_dict):
    vector = ObjectStateHolderVector()
    try:
        vm_pools = rhevm_discoverer.VMPoolDiscoverer(client).discover()
        for vm_pool in vm_pools:
            vm_pool_osh = vm_pool.build()
            vm_pool_osh.setContainer(cluster_dict.get(vm_pool.cluster_id))
            vm_pool_dict[vm_pool.id] = vm_pool_osh
            vector.add(vm_pool_osh)
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning('Failed to discover vm pools.')
    finally:
        return vector


def get_vnic_profiles(client, rhevm_discoverer, network_dict, vnic_profile_dict):
    vector = ObjectStateHolderVector()
    try:
        vnic_profiles = rhevm_discoverer.VNICProfileDiscoverer(client).discover()
        for vnic_profile in vnic_profiles:
            vnic_profile_osh = vnic_profile.build()
            vnic_profile_osh.setContainer(network_dict.get(vnic_profile.network_id))
            vnic_profile_dict[vnic_profile.id] = vnic_profile_osh
            vector.add(vnic_profile_osh)
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning('Failed to discover vnic profile.')
    finally:
        return vector


def get_hosts(client, rhevm_discoverer, network_dict, host_dict):
    vector = ObjectStateHolderVector()
    try:
        hosts = rhevm_discoverer.HostDiscoverer(client).discover()
        for host in hosts:
            host_osh = host.build()
            host_dict[host.id] = host_osh
            vector.add(host_osh)

            for interface in host.interfaces:
                if interface.ip_address:
                    interface_osh = interface.build(host_osh)
                    ipaddress_osh = modeling.createIpOSH(interface.ip_address, interface.ip_netmask)
                    vector.add(interface_osh)
                    vector.add(ipaddress_osh)
                    vector.add(modeling.createLinkOSH('containment', host_osh, ipaddress_osh))
                    vector.add(modeling.createLinkOSH('containment', interface_osh, ipaddress_osh))
                    network_osh = network_dict.get(interface.network_id, None)
                    if network_osh:
                        vector.add(modeling.createLinkOSH('usage', interface_osh, network_osh))
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning('Failed to discover host.')
    finally:
        return vector


def get_vms(client, rhevm_discoverer, host_dict, cluster_dict, vm_pool_dict, vnic_profile_dict, disk_dict):
    vector = ObjectStateHolderVector()
    try:
        vms = rhevm_discoverer.VirtualMachineDiscoverer(client).discover()
        for vm in vms:
            vm_osh, hr_osh = vm.build()

            cluster_osh = cluster_dict.get(vm.cluster_id)
            vector.add(vm_osh)
            vector.add(hr_osh)
            vector.add(modeling.createLinkOSH('containment', cluster_osh, vm_osh))

            if vm.host_id:
                host_osh = host_dict.get(vm.host_id)
                hv_osh = ObjectStateHolder('virtualization_layer')
                hv_osh.setStringAttribute('name', 'Kvm Hyperviso')
                hv_osh.setStringAttribute('product_name', 'kvm_hypervisor')
                hv_osh.setContainer(host_osh)
                vector.add(hv_osh)
                vector.add(modeling.createLinkOSH('execution_environment', hv_osh, vm_osh))
                vector.add(modeling.createLinkOSH('membership', cluster_osh, hv_osh))

            if vm.vm_pool_id:
                vector.add(modeling.createLinkOSH('containment', vm_pool_dict.get(vm.vm_pool_id), vm_osh))

            for interface in vm.interfaces:
                interface_osh = interface.build(vm_osh)
                profile_osh = vnic_profile_dict[interface.vnic_profile_id]

                vector.add(interface_osh)
                vector.add(modeling.createLinkOSH('usage', interface_osh, profile_osh))

            for disk_id in vm.disk_ids:
                disk_osh = disk_dict[disk_id]
                vector.add(modeling.createLinkOSH('containment', vm_osh, disk_osh))
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning('Failed to discover virtual machine.')
    finally:
        return vector


def get_storage_domains(client, rhevm_discoverer, datacenter_dict, storage_dict):
    vector = ObjectStateHolderVector()
    try:
        storages = rhevm_discoverer.StorageDomainDiscoverer(client).discover()
        for storage in storages:
            lv_osh, lv_vector = storage.build()
            lv_osh.setContainer(datacenter_dict.get(storage.datacenter_id))
            storage_dict[storage.id] = lv_osh
            vector.addAll(lv_vector)

    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning('Failed to discover storage domain.')
    finally:
        return vector


def get_disks(client, rhevm_discoverer, storage_dict, disk_dict):
    vector = ObjectStateHolderVector()
    try:
        disks = rhevm_discoverer.DiskDiscoverer(client).discover()
        for disk in disks:
            disk_osh = disk.build()
            disk_osh.setContainer(storage_dict.get(disk.storage_domain_id))
            disk_dict[disk.id] = disk_osh
            vector.add(disk_osh)
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning('Failed to discover disk.')
    finally:
        return vector
