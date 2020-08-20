import modeling
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector


class RHEVM:
    def __init__(self, name):
        self.name = name
        self.vendor = None
        self.application_version = None
        self.major_version = None
        self.minor_version = None

    def build(self):
        rhevm_osh = ObjectStateHolder('red_hat_virtualization')
        rhevm_osh.setAttribute('name', self.name)
        rhevm_osh.setAttribute('discovered_product_name', 'Red Hat Virtualization Manager')
        rhevm_osh.setAttribute('vendor', self.vendor)
        rhevm_osh.setAttribute('application_version', self.application_version)
        rhevm_osh.setAttribute('version', '.'.join((self.major_version, self.minor_version)))

        return rhevm_osh


class DataCenter:
    def __init__(self, name, id):
        self.name = name
        self.id = id
        self.status = None
        self.description = None
        self.major_version = None
        self.minor_version = None

    def build(self):
        dc_osh = ObjectStateHolder('rhevm_datacenter')
        dc_osh.setAttribute('name', self.name)
        dc_osh.setAttribute('datacenter_id', self.id)
        dc_osh.setAttribute('datacenter_status', self.status)
        dc_osh.setAttribute('description', self.description)
        dc_osh.setAttribute('compatibility_version', '.'.join((self.major_version, self.minor_version)))
        return dc_osh


class Cluster:
    def __init__(self, name, id):
        self.name = name
        self.id = id
        self.cpu_architecture = None
        self.cpu_type = None
        self.description = None
        self.major_version = None
        self.minor_version = None
        self.datacenter_id = None

    def build(self):
        cluster_osh = ObjectStateHolder('rhevm_cluster')
        cluster_osh.setAttribute('name', self.name)
        cluster_osh.setAttribute('cpu_architecture', self.cpu_architecture)
        cluster_osh.setAttribute('cpu_type', self.cpu_type)
        cluster_osh.setAttribute('compatibility_version', '.'.join((self.major_version, self.minor_version)))
        cluster_osh.setAttribute('description', self.description)
        return cluster_osh


class Network:
    def __init__(self, name, id):
        self.name = name
        self.id = id
        self.description = None
        self.datacenter_id = None

    def build(self):
        network_osh = ObjectStateHolder('rhevm_network')
        network_osh.setAttribute('name', self.name)
        network_osh.setAttribute('description', self.description)
        return network_osh


class Host:
    def __init__(self, name, id, os_type='host_node'):
        self.name = name
        self.id = id
        self.os_type = os_type
        self.description = None
        self.cluster_id = None
        self.links = {}
        self.interfaces = []

    def build(self):
        host_osh = ObjectStateHolder(self.os_type)
        host_osh.setAttribute('name', self.name)

        return host_osh


class Interface:
    def __init__(self, mac):
        self.mac = mac
        self.name = None
        self.speed = None
        self.network_id = None
        self.ip_address = None
        self.ip_netmask = None
        self.vnic_profile_id = None

    def build(self, container_osh):
        return modeling.createInterfaceOSH(self.mac, container_osh, name=self.name, speed=self.speed)


class VMPool:
    def __init__(self, name, id):
        self.name = name
        self.id = id
        self.max_user_vms = None
        self.size = None
        self.prestarted_vms = None
        self.cluster_id = None

    def build(self):
        vm_pool_osh = ObjectStateHolder('rhevm_resource_pool')
        vm_pool_osh.setAttribute('name', self.name)
        vm_pool_osh.setIntegerAttribute('max_user_vms', self.max_user_vms)
        vm_pool_osh.setIntegerAttribute('size', self.size)
        vm_pool_osh.setIntegerAttribute('prestarted_vms', self.prestarted_vms)

        return vm_pool_osh


class VNICProfile:
    def __init__(self, name, id):
        self.name = name
        self.id = id
        self.network_id = None

    def build(self):
        vnic_profile_osh = ObjectStateHolder('rhevm_vnic_profile')
        vnic_profile_osh.setAttribute('name', self.name)
        return vnic_profile_osh


class VirtualMachine:
    def __init__(self, name, id):
        self.name = name
        self.id = id
        self.host_id = None
        self.cluster_id = None
        self.cpu_architecture = None
        self.cpu_core = None
        self.cpu_socket = None
        self.cpu_thread = None
        self.memory = None
        self.vm_pool_id = None
        self.links = {}
        self.interfaces = []
        self.disk_ids = []

    def build(self):
        vm_osh = ObjectStateHolder('host_node')
        vm_osh.setAttribute('name', self.name)
        vm_osh.setAttribute('host_biosuuid', self.id.upper())

        hr_osh = ObjectStateHolder('kvm_domain_config')
        hr_osh.setAttribute('data_name', self.name)
        hr_osh.setAttribute('kvm_cpu_architecture', self.cpu_architecture)
        hr_osh.setIntegerAttribute('kvm_cores_per_socket', self.cpu_core)
        hr_osh.setIntegerAttribute('kvm_socket', self.cpu_socket)
        hr_osh.setIntegerAttribute('kvm_threads_per_core', self.cpu_thread)
        hr_osh.setLongAttribute('kvm_domain_max_memory', long(self.memory) / 1024)
        hr_osh.setIntegerAttribute('kvm_domain_vcpus', long(self.cpu_socket) * long(self.cpu_core) * long(self.cpu_thread))
        hr_osh.setContainer(vm_osh)

        return vm_osh, hr_osh


class LogicalVolume:
    def __init__(self, name, id):
        self.name = name
        self.id = id
        self.datacenter_id = None
        self.client_ip = None
        self.path = None
        self.type = None
        self.available = None
        self.used = None

    def build(self):
        vector = ObjectStateHolderVector()
        lv_osh = ObjectStateHolder('rhevm_volume')
        lv_osh.setAttribute('name', self.name)
        lv_osh.setDoubleAttribute('logicalvolume_size', long((long(self.available) + long(self.used)) / 1024 / 1024))
        vector.add(lv_osh)

        if self.client_ip and self.path:
            client_osh = modeling.createHostOSH(self.client_ip)
            file_export_osh = ObjectStateHolder('file_system_export')
            file_export_osh.setStringAttribute('file_system_path', self.path)
            file_export_osh.setContainer(client_osh)

            vector.add(client_osh)
            vector.add(file_export_osh)
            vector.add(modeling.createLinkOSH('dependency', lv_osh, file_export_osh))
        return lv_osh, vector


class Disk:
    def __init__(self, name, id):
        self.name = name
        self.id = id
        self.storage_domain_id = None
        self.actual_size = None

    def build(self):
        disk_osh = ObjectStateHolder('rhevm_disk_device')
        disk_osh.setStringAttribute('name', self.name)
        disk_osh.setStringAttribute('rhevm_disk_id', self.id)
        disk_osh.setIntegerAttribute('disk_size', long(self.actual_size) / 1024 / 1024)
        return disk_osh
