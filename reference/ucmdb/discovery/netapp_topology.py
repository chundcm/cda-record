# coding=utf-8
from collections import namedtuple
import modeling
import logger
import netutils
import netapp_webservice_utils
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector

SCRIPT_NAME = 'netapp_topology.py'


class NetAppClusterBuilder:
    CIT = "netapp_cluster"
    _Pdo = namedtuple('Pdo', ('name'))

    @staticmethod
    def create_pdo(name):
        return NetAppClusterBuilder._Pdo(name)

    def build(self, pdo):
        cluster_osh = ObjectStateHolder(self.CIT)
        cluster_osh.setAttribute("name", pdo.name)
        return cluster_osh


class VServerBuilder:
    CIT = "clusteredservice"
    _Pdo = namedtuple('Pdo', ('uuid', 'name', 'type'))

    @staticmethod
    def create_pdo(uuid, name, type=None):
        return VServerBuilder._Pdo(uuid, name, type)

    def build(self, pdo, cluster_osh):
        vector = ObjectStateHolderVector()
        vserver_osh = ObjectStateHolder(self.CIT)
        logger.debug("VServerBuilder:", pdo)
        vserver_osh.setAttribute("data_name", pdo.name)
        vserver_osh.setAttribute('host_key', '%s:%s' % ("", pdo.name))
        vserver_osh.setBoolAttribute('host_iscomplete', 1)
        vserver_osh.setAttribute('data_note', cluster_osh.getAttribute('name').getStringValue())
        vector.add(vserver_osh)
        vector.add(modeling.createLinkOSH('containment', cluster_osh, vserver_osh))

        return vserver_osh, vector

class VServerReporter:
    def __init__(self, builder=VServerBuilder()):
        self._builder = builder

    def report(self, pdo, cluster_osh, vserverOshDict):
        vector = ObjectStateHolderVector()
        vserver_osh, result_vector = self._builder.build(pdo, cluster_osh)
        vector.addAll(result_vector)
        vserverOshDict[pdo.name] = vserver_osh
        return vector

class NetAppNode:
    def __init__(self, id, name, serial_number, model, memory_size=None):
        self._id = id
        self._name = name
        self._serial_number = serial_number
        self._model = model
        self._memory_size = memory_size
        self._cpus = []

    def addCPU(self, cpu_id):
        self._cpus.append(cpu_id)


class NetAppNodeBuilder:
    CIT = "netapp_node"

    @staticmethod
    def create_pdo(id, name, serial_number, model, memory_size=None):
        return NetAppNode(id, name, serial_number, model, memory_size)

    def build(self, pdo):
        vector = ObjectStateHolderVector()
        host_osh = ObjectStateHolder('netapp_node')
        host_osh.setAttribute('host_key', pdo._id + ' (NetApp System ID)')
        #host_osh.setAttribute('data_note', 'No IP or MAC address found through NetApp API - Duplication of this CI is possible')
        host_osh.setBoolAttribute('host_iscomplete', 1)

        if netapp_webservice_utils.isValidString(pdo._name):
            host_osh.setAttribute('name', pdo._name)

        if netapp_webservice_utils.isValidString(pdo._model):
            host_osh.setAttribute('discovered_model', pdo._model)
        if netapp_webservice_utils.isValidString(pdo._serial_number):
            host_osh.setAttribute('serial_number', pdo._serial_number)
        vector.add(host_osh)

        if pdo._cpus:
            for cpu_id in pdo._cpus:
                cpu_osh = ObjectStateHolder('cpu')
                cpu_osh.setAttribute('cpu_cid', cpu_id)
                cpu_osh.setContainer(host_osh)
                vector.add(cpu_osh)

        return host_osh, vector

class NetAppClusterReporter:
    def __init__(self, builder=NetAppNodeBuilder()):
        self._builder = builder

    def report(self, cluster_name, node_pdo_list, nodeOshDict):
        vector = ObjectStateHolderVector()
        cluster_osh = ObjectStateHolder('netapp_cluster')
        cluster_osh.setAttribute('name', cluster_name)
        vector.add(cluster_osh)

        for node_pdo in node_pdo_list:
            node_osh, node_vector = self._builder.build(node_pdo)
            nodeOshDict[node_pdo._name] = node_osh
            vector.addAll(node_vector)
            vector.add(modeling.createLinkOSH("membership", cluster_osh, node_osh))
        return cluster_osh, vector

class InterfaceBuilder:
    CIT = "interface"

    _Pdo = namedtuple('Pdo', ('name', 'ip', 'netmask', 'host_name', 'role'))

    @staticmethod
    def create_pdo(name, ip, netmask, host_name, role):
        return InterfaceBuilder._Pdo(name, ip, netmask, host_name, role)

    def build(self, pdo, container_osh, ):
        return modeling.createInterfaceOSH(None, container_osh, name=pdo.name)


class InterfaceReporter:
    def __init__(self, builder=InterfaceBuilder()):
        self._builder = builder

    def report(self, pdo, container_osh):
        vector = ObjectStateHolderVector()
        interface_osh = self._builder.build(pdo, container_osh)
        vector.add(interface_osh)
        if pdo.ip and netutils.isValidIp(pdo.ip):
            ip_osh = modeling.createIpOSH(pdo.ip, pdo.netmask)
            vector.add(ip_osh)
            vector.add(modeling.createLinkOSH("containment", container_osh, ip_osh))
            vector.add(modeling.createLinkOSH("containment", interface_osh, ip_osh))
        return vector

class VolumeBuilder:
    CIT = "logicalvolume"

    _Pdo = namedtuple('Pdo', (
    'id', 'name', 'size', 'free', 'used', 'type', 'state', 'host_name', 'node_name', 'path', 'serial_number',
    'volume_name', 'aggr_name'))

    @staticmethod
    def create_pdo(id, name, size, free, used, type, state, host_name, node_name=None, path=None, serial_number=None,
                   volume_name=None, aggr_name=None):
        return VolumeBuilder._Pdo(id, name, size, free, used, type, state, host_name, node_name, path, serial_number,
                                  volume_name, aggr_name)

    def build(self, pdo, container_osh):
        if netapp_webservice_utils.isValidString(pdo.id) and netapp_webservice_utils.isValidString(pdo.name):
            ## Build LOGICALVOLUME OSH
            volume_osh = ObjectStateHolder(self.CIT)
            volume_osh.setAttribute('name', pdo.name)
            # volumeOSH.setIntegerAttribute('logicalvolume_id', volumeID)
            if netapp_webservice_utils.isValidString(pdo.size):
                volume_osh.setDoubleAttribute('logicalvolume_size', long(pdo.size) / 1024.0 / 1024.0)
            if netapp_webservice_utils.isValidString(pdo.free):
                volume_osh.setDoubleAttribute('logicalvolume_free', long(pdo.free) / 1024.0 / 1024.0)
            if netapp_webservice_utils.isValidString(pdo.used):
                volume_osh.setDoubleAttribute('logicalvolume_used', long(pdo.used) / 1024.0 / 1024.0)
            if netapp_webservice_utils.isValidString(pdo.type):
                volume_osh.setAttribute('logicalvolume_fstype', pdo.type)
            if netapp_webservice_utils.isValidString(pdo.state):
                volume_osh.setAttribute('logicalvolume_status', pdo.state)
            if netapp_webservice_utils.isValidString(pdo.serial_number):
                volume_osh.setAttribute('serial_number', pdo.serial_number)
            volume_osh.setContainer(container_osh)
            return volume_osh
        else:
            netapp_webservice_utils.debugPrint(1, '[' + SCRIPT_NAME + ':buildVolumeOsh] Insufficient information to build LOGICALVOLUME CI for volume with UUID <%s>' % pdo.id)




class VolumeReporter:
    def __init__(self, builder=VolumeBuilder()):
        self._builder = builder

    def report(self, pdo, container_osh):
        return  self._builder.build(pdo, container_osh)


class DiskBuilder:
    CIT = "disk_device"

    _Pdo = namedtuple('Pdo', ('id', 'name', 'serial_number', 'model', 'vendor', 'node_name', 'aggregate_name'))

    @staticmethod
    def create_pdo(id, name, serial_number, model, vendor, node_name, aggregate_name=None):
        return DiskBuilder._Pdo(id, name, serial_number, model, vendor, node_name, aggregate_name)

    def build(self, pdo, container_osh):
        if netapp_webservice_utils.isValidString(pdo.id) and netapp_webservice_utils.isValidString(pdo.name):
            disk_device_osh = ObjectStateHolder(self.CIT)
            disk_device_osh.setAttribute('name', pdo.name)
            disk_device_osh.setAttribute('vendor', pdo.vendor)
            disk_device_osh.setAttribute('serial_number', pdo.serial_number)
            disk_device_osh.setAttribute('model_name', pdo.model)
            disk_device_osh.setContainer(container_osh)
            return disk_device_osh
        else:
            netapp_webservice_utils.debugPrint(1, '[' + SCRIPT_NAME + ':DiskBuilder] Insufficient information to build DISK DEVICE CI for volume with UUID <%s>' % pdo.id)


class DiskReporter:
    def __init__(self, builder=DiskBuilder()):
        self._builder = builder

    def report(self, pdo, container_osh):
        return self._builder.build(pdo, container_osh)


class ISCSIAdapterBuilder:
    CIT = "iscsi_adapter"

    _Pdo = namedtuple('Pdo', ('iqn', 'name', 'host_name'))

    @staticmethod
    def create_pdo(iqn, name, host_name):
        return ISCSIAdapterBuilder._Pdo(iqn, name, host_name)

    def build(self, pdo, container_osh):
        scsi_osh = ObjectStateHolder(self.CIT)
        scsi_osh.setStringAttribute("iqn", pdo.iqn)
        if pdo.name:
            scsi_osh.setAttribute('data_name', pdo.name)
        scsi_osh.setContainer(container_osh)
        return scsi_osh


class RemoteISCSIAdapterBuilder(ISCSIAdapterBuilder):
    _Pdo = namedtuple('Pdo', ('iqn', 'name', 'ip_address', 'vserver_name', ))

    @staticmethod
    def create_remote_pdo(iqn, name, ip_address, vserver_name):
        return RemoteISCSIAdapterBuilder._Pdo(iqn, name, ip_address, vserver_name)


class ISCSIAdapterReporter:
    def __init__(self, iscsi_builder=ISCSIAdapterBuilder()):
        self._builder = iscsi_builder

    def report(self, pdo, container_osh):
        iscsi_osh = self._builder.build(pdo, container_osh)
        return iscsi_osh

    def reportRemoteISCSIAdapter(self, initiator_pdo, scsiOshDict, lunOshDict):
        vector = ObjectStateHolderVector()
        logger.debug("initiator_pdo.ip_address:", initiator_pdo.ip_address)
        if initiator_pdo.ip_address and netutils.isValidIp(initiator_pdo.ip_address):
            host_osh = modeling.createHostOSH(str(initiator_pdo.ip_address))
            ip_osh = modeling.createIpOSH(str(initiator_pdo.ip_address))
            vector.add(host_osh)
            vector.add(ip_osh)
            vector.add(modeling.createLinkOSH('containment', host_osh, ip_osh))

            remote_iscsi_osh = self._builder.build(initiator_pdo, host_osh)
            vector.add(remote_iscsi_osh)

            if initiator_pdo.vserver_name:
                scsi_osh = scsiOshDict.get(initiator_pdo.vserver_name, None)
                if scsi_osh:
                    vector.add(modeling.createLinkOSH('usage', remote_iscsi_osh, scsi_osh))

                # lunVector = getLunForInitiator(localFramework, wsConnection, name, lunOshDict)
                # for lunOsh in lunVector:
                # resultVector.add(modeling.createLinkOSH('dependency', lunOsh, osh))
        return vector


class FCAdapterReporter():
    def report(self, wwn, container_osh, hbaOshDict=None):
        fc_adpater_osh = ObjectStateHolder("fchba")
        fc_adpater_osh.setStringAttribute("fchba_wwn", wwn)
        fc_adpater_osh.setContainer(container_osh)
        if hbaOshDict is not None:
            hbaOshDict[wwn] = fc_adpater_osh
        return fc_adpater_osh


class FCPortBuilder:
    CIT = "fcport"

    _Pdo = namedtuple('Pdo', ('wwn', 'name', 'hba_wwn', 'vserver_name'))

    @staticmethod
    def create_pdo(wwn, name, hba_wwn, vserver_name):
        return FCPortBuilder._Pdo(wwn, name, hba_wwn, vserver_name)

    def build(self, pdo):
        fcport_osh = ObjectStateHolder(self.CIT)
        if pdo.name:
            fcport_osh.setAttribute('name', pdo.name)
        if pdo.wwn:
            fcport_osh.setStringAttribute("fcport_wwn", pdo.wwn)
            return fcport_osh
        return None

class FCPortReporter:
    def __init__(self, builder=FCPortBuilder()):
        self._builder = builder

    def report(self, pdo, hbaOshDict):
        vector = ObjectStateHolderVector()
        fcp_osh = self._builder.build(pdo)
        vector.add(fcp_osh)

        if pdo.hba_wwn:
            hba_osh = hbaOshDict.get(pdo.hba_wwn, None)
            if hba_osh:
                vector.add(modeling.createLinkOSH("containment", hba_osh, fcp_osh))
        return fcp_osh, vector


    def reportRemoteFCPort(self, pdo, fcpOshDict):
        vector = ObjectStateHolderVector()
        fcp_osh = self._builder.build(pdo)
        vector.add(fcp_osh)

        if pdo.hba_wwn:
            hba_osh = ObjectStateHolder("fchba")
            hba_osh.setStringAttribute("fchba_wwn", pdo.hba_wwn)
            vector.add(modeling.createLinkOSH("containment", hba_osh, fcp_osh))

        if fcpOshDict and fcpOshDict.get(pdo.wwn, None):
            vector.add(modeling.createLinkOSH("fcconnect", fcp_osh, fcpOshDict.get(pdo.wwn, None)))
        return vector

class SnapshotBuilder:
    CIT = "logicalvolume_snapshot"

    _Pdo = namedtuple('Pdo', ('name', 'busy', 'dependency', 'total_blocks', 'used_blocks', 'volume'))

    @staticmethod
    def create_pdo(name, busy, dependency, total_blocks, used_blocks, volume):
        return SnapshotBuilder._Pdo(name, busy, dependency, total_blocks, used_blocks, volume)

    def build(self, pdo, container_osh):
        if netapp_webservice_utils.isValidString(pdo.name):
            snapshot_osh = ObjectStateHolder(self.CIT)
            snapshot_osh.setAttribute("name", pdo.name)
            if netapp_webservice_utils.isValidString(pdo.busy):
                snapshot_osh.setBoolAttribute('is_busy', pdo.busy)
            if netapp_webservice_utils.isValidString(pdo.total_blocks):
                snapshot_osh.setIntegerAttribute('total_block_percentage', pdo.total_blocks)
            if netapp_webservice_utils.isValidString(pdo.used_blocks):
                snapshot_osh.setIntegerAttribute('used_block_percentage', pdo.used_blocks)
            if netapp_webservice_utils.isValidString(pdo.dependency):
                snapshot_osh.setAttribute('application_dependencies', pdo.dependency)
            snapshot_osh.setContainer(container_osh)
            return snapshot_osh


class SnapshotReporter:
    def __init__(self, builder=SnapshotBuilder()):
        self._builder = builder

    def report(self, pdo, container_osh, snapshotOshDict):
        snapshot_osh = self._builder.build(pdo, container_osh)
        snapshotOshDict[pdo.volume + ' ' + pdo.name] = snapshot_osh
        return snapshot_osh


class NetworkShareBuilder:
    CIT = "networkshare"

    _Pdo = namedtuple('Pdo', ('name', 'path', 'type', 'host_name', 'file_system'))

    @staticmethod
    def create_pdo(name, path, type, host_name, file_system=None):
        return NetworkShareBuilder._Pdo(name, path, type, host_name, file_system)

    def build(self, pdo, container_osh):
        if netapp_webservice_utils.isValidString(pdo.name):
            network_share_osh = ObjectStateHolder(self.CIT)
            network_share_osh.setAttribute("name", pdo.name)
            if netapp_webservice_utils.isValidString(pdo.path):
                network_share_osh.setAttribute('share_path', pdo.path)
            network_share_osh.setContainer(container_osh)
            return network_share_osh


class FileSystemReporter:
    def report(self, mount_point, container_osh):
        file_system_osh = ObjectStateHolder('file_system')
        file_system_osh.setAttribute('name', mount_point)
        file_system_osh.setAttribute('mount_point', mount_point)
        file_system_osh.setContainer(container_osh)
        return file_system_osh


class NetworkShareReporter:
    def __init__(self, builder=NetworkShareBuilder()):
        self._builder = builder

    def report(self, pdo, container_osh, filesystemOshDict=None):
        vector = ObjectStateHolderVector()
        network_share_osh = self._builder.build(pdo, container_osh)
        vector.add(network_share_osh)
        if filesystemOshDict and filesystemOshDict.get(pdo.file_system, None):
            file_system_osh = filesystemOshDict.get(pdo.file_system, None)
            realization_link_osh = modeling.createLinkOSH('realization', network_share_osh, file_system_osh)
            realization_link_osh.setAttribute('name', pdo.type)
            vector.add(realization_link_osh)
        return vector