#coding=utf-8
import re
import smis
import logger
import fptools
import cim
import cim_discover
import netutils
import smis_cimv2
from smis_cimv2 import BaseSmisDiscoverer
from smis_cimv2 import topologyFieldHanlder
from smis_cimv2 import stringClean

class Namespace(smis_cimv2.BaseNamespace):
    def __init__(self):
        smis_cimv2.BaseNamespace.__init__(self)
        self.__namespace = 'root/LsiArray13'

    def associateTopologyObj2Discoverers(self, topology):
        self.handlers.append(topologyFieldHanlder(topology.storage_systems,
                                                  StorageSystemDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.storage_processors,
                                                  StorageProcessorDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.end_point_links,
                                                  EndPointToVolumeDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.lun_mappings,
                                                  smis_cimv2.LunMaskingMappingViewDiscover()))

        self.handlers.append(topologyFieldHanlder(topology.ports,
                                                  FcPortDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.physical_volumes,
                                                  PhysicalVolumeDiscoverer()))

#        self.__handlers.append( topologyFieldHanlder( topology.hosts,
#                                                     HostCimv2Dicoverer() ) )

        self.handlers.append(topologyFieldHanlder(topology.logical_volumes,
                                                  LogicalVolumeDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.remote_endpoints,
                                                  RemoteEndpointDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.storage_pools,
                                                  StoragePoolDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.physcial_volumes_2_pool_links,
                                                  smis_cimv2.PhysicalVolume2StoragePoolDiscoverer()))

#        self.__handlers.append( topologyFieldHanlder( topology.file_shares,
#                                                     FileShareCimv2Discoverer() ) )

#        self.__handlers.append( topologyFieldHanlder( topology.file_systems,
#                                                     FileSystemCimv2Discoverer() ) )

class StorageProcessorDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'LSISSI_StorageProcessorSystem'
        self.controllerCanister = 'LSISSI_ControllerCanister'

    def discover(self, client):
        canisters = []
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        instances = client.getInstances(self.className)
        canisters = client.getInstances(self.controllerCanister)

        return self.parse(instances, canisters)

    def parse(self, instances, canisters):
        result = []
        canisterMap = {}
        for canister in canisters:
            canisterMap[stringClean(canister.getPropertyValue('StorageProcessorSystem_Name'))] = canister

        for instance in instances:
            name = instance.getPropertyValue('ElementName')
            serial = None
            version = None
            model = None
            vendor = None
            ip = None
            id = stringClean(instance.getPropertyValue('Name'))
            if id in canisterMap:
                canister = canisterMap[id]
                model = canister.getPropertyValue('Model')
                vendor = canister.getPropertyValue('Manufacturer')

            system_path = instance.getPropertyValue('Description')
            #version = instance.getPropertyValue('FirmwareVersion')
            status = smis_cimv2.PROCESSOR_STATE_VALUE_MAP.get(stringClean(instance.getPropertyValue('HealthState')), 'Unknown')
            node_wwn = None
            ident_list = instance.getPropertyValue('IdentifyingDescriptions')
            ident_value = instance.getPropertyValue('OtherIdentifyingInfo')

            try:
                serial_index = ident_list.index('SCSI Vendor Specific Name')
                serial = ident_value[serial_index]
            except:
                logger.warn('Failed to get Controller Serial Number of storage processor')

            try:
                ip_index = ident_list.index('Ipv4 Address')
                ip = ident_value[ip_index]
            except:
                logger.warn('Failed to get ip of storage processor')

            try:
                storage_processor = smis.StorageProcessor(id, name, node_wwn, system_path, version, serial, status, model, vendor, ip)
                result.append(storage_processor)
            except:
                logger.warnException('')

        return result

class EndPointToVolumeDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)

    def discover(self, client):
        return []

class StorageSystemDiscoverer(smis_cimv2.StorageSystemDiscoverer):
    def __init__(self):
        smis_cimv2.StorageSystemDiscoverer.__init__(self)
        self.className = 'LSISSI_StorageSystem'
        self.firmwareIdentity = 'LSISSI_ControllerFirmwareIdentity'

    def discover(self, client):
        firmwares = []
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        instances = client.getInstances(self.className)
        if self.firmwareIdentity:
            firmwares = client.getInstances(self.firmwareIdentity)
        return self.parse(instances, firmwares)

    def parse(self, instances, firmwares):
        result = []
        firmwareMap = {}
        for firmware in firmwares:
            firmwareMap[stringClean(firmware.getPropertyValue('StorageSystem_Name'))] = firmware

        for instance in instances:
            osVersion = None
            vendor = None
            name = stringClean(instance.getPropertyValue('ElementName'))
            sydId = stringClean(instance.getPropertyValue('Name'))
            if sydId in firmwareMap:
                firmware = firmwareMap[sydId]
                osVersion = stringClean(firmware.getPropertyValue('VersionString'))
                vendor = stringClean(firmware.getPropertyValue('Manufacturer'))

            model = None
            #model = stringClean(firmware.getPropertyValue('StorageSystem_Name'))
            status = smis_cimv2.getOperationalStatus(instance)
            serial = stringClean(instance.getPropertyValue('NVSRAMVersion'))
            description = stringClean(instance.getPropertyValue('Description'))
            identList = instance.getPropertyValue('IdentifyingDescriptions')
            identValue = instance.getPropertyValue('OtherIdentifyingInfo')

            #ip = '10.112.21.91'
            ip = None
            hostObj = smis.Host(sydId, ip, name, sydId, description, [], [], model, serial, osVersion, vendor, status)
            result.append(hostObj)

        return result

class FcPortDiscoverer(smis_cimv2.FcPortDiscoverer):
    def __init__(self):
        smis_cimv2.FcPortDiscoverer.__init__(self)
        self.className = 'LSISSI_FCPort'

class PhysicalVolumeDiscoverer(smis_cimv2.PhysicalVolumeDiscoverer):
    def __init__(self):
        smis_cimv2.PhysicalVolumeDiscoverer.__init__(self)
        self.className = 'LSISSI_DiskExtent'

class LogicalVolumeDiscoverer(smis_cimv2.LogicalVolumeDiscoverer):
    def __init__(self):
        smis_cimv2.LogicalVolumeDiscoverer.__init__(self)
        self.className = 'LSISSI_StorageVolume'

    def parse(self, instances):
        result = []
        for instance in instances:
            name = stringClean(instance.getPropertyValue('Name'))
            managedSysName = stringClean(instance.getPropertyValue('SystemName'))
            blockSize = stringClean(instance.getPropertyValue('BlockSize'))
            blocksNumber = stringClean(instance.getPropertyValue('NumberOfBlocks'))
            blocksConsumable = stringClean(instance.getPropertyValue('ConsumableBlocks'))
            objectId = stringClean(instance.getPropertyValue('DeviceID'))
            status = instance.getPropertyValue('StatusDescriptions') and ",".join(instance.getPropertyValue('StatusDescriptions'))
            poolId = instance.getPropertyValue('PoolId')

            humanReadableName = stringClean(instance.getPropertyValue('ElementName'))

            freeSpaceInMb = None
            sizeInMb = None
            try:
                sizeInMb = float(blocksNumber) * int(blockSize)/ (1024 * 1024)
            except:
                logger.debugException('')
                logger.debug('Failed to convert sizeInMb value')
            try:
                freeSpaceInMb = float(blocksConsumable) * int(blockSize)/ (1024 * 1024)
            except:
                logger.debugException('')
                logger.debug('Failed to convert freeSpaceInMb value')
            try:
                lvObj = smis.LogicalVolume(name, managedSysName, objectId, freeSpaceInMb, sizeInMb, None, humanReadableName, status, poolId)
                result.append(lvObj)
            except:
                logger.debugException('')


        return result

class RemoteEndpointDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)

    def discover(self, client):
        return []

class StoragePoolDiscoverer(smis_cimv2.StoragePoolDiscoverer):
    def __init__(self):
        smis_cimv2.StoragePoolDiscoverer.__init__(self)
        self.className = 'LSISSI_StoragePool'

    def parse(self, instances):
        result = []
        id = 0
        for instance in instances:
            cim_id = stringClean(instance.getPropertyValue('PoolID'))

            name = stringClean(instance.getPropertyValue('Name'))
            parent = stringClean(instance.getPropertyValue('StorageSystem_Name'))
            availSpace = stringClean(instance.getPropertyValue('TotalManagedSpace'))
            freeSpace = stringClean(instance.getPropertyValue('RemainingManagedSpace'))
            type = None
            try:
                type = stringClean(instance.getPropertyValue('DiskGroupType'))
            except:
                logger.warn('Failed to get pool type')

            try:
                pool = smis.StoragePool(name = name, parentReference = parent, id = id, type = type, availableSpaceInMb = freeSpace, totalSpaceInMb = availSpace,\
                     unExportedSpaceInMb = freeSpace, dataRedund = None, lvmIds = None, cimId = cim_id)
                result.append(pool)
                id += 1
            except:
                logger.debugException('')
        return result

