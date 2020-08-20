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
        self.__namespace = 'root/eva'

    def associateTopologyObj2Discoverers(self, topology):
        self.handlers.append(topologyFieldHanlder(topology.storage_systems,
                                                  StorageSystemDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.storage_processors,
                                                  StorageProcessorEvaDiscoverer()))

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

class StorageProcessorEvaDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'HPEVA_StorageProcessorSystem'

    def parse(self, instances):
        result = []
        for instance in instances:
            name = instance.getProperty('ElementName').getValue()
            id = instance.getProperty('Name').getValue()
            system_path = instance.getProperty('Description').getValue()
            version = instance.getProperty('FirmwareVersion').getValue()
            status = smis_cimv2.PROCESSOR_STATE_VALUE_MAP.get(stringClean(instance.getProperty('HealthState').getValue()), 'Unknown')

            serial = None
            node_wwn = None

            ident_list = instance.getProperty('IdentifyingDescriptions').getValue()
            ident_value = instance.getProperty('OtherIdentifyingInfo').getValue()

            try:
                serial_index = ident_list.index('Controller Serial Number')
                serial = ident_value[serial_index]
            except:
                logger.warn('Failed to get Controller Serial Number')

            try:
                node_wwn_index = ident_list.index('Node WWN')
                node_wwn = ident_value[node_wwn_index]
            except:
                logger.warn('Failed to get wwn of storage system')

            try:
                storage_processor = smis.StorageProcessor(id, name, node_wwn, system_path, version, serial, status)
                result.append(storage_processor)
            except:
                logger.warnException('')

        return result

class EndPointToVolumeDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'HPEVA_ProtocolControllerForVolume'

    def parse(self, instances):
        result = []
        for instance in instances:
            acc = stringClean(instance.getProperty('Antecedent').getValue())
            dep = stringClean(instance.getProperty('Dependent').getValue())
            volume_id = None
            endpoint_id = None
            m = re.search(r'DeviceID=[\\"]*(\w+)[\\"]*', acc)
            if m:
                endpoint_id = m.group(1)
            m = re.search(r'DeviceID=[\\"]*(\w+)[\\"]*', dep)
            if m:
                volume_id = m.group(1)
            try:
                obj = smis.RemoteHostToLogicalVolumeLink(volume_id, endpoint_id)
                result.append(obj)
            except:
                logger.debugException('')
        return result

class StorageSystemDiscoverer(smis_cimv2.StorageSystemDiscoverer):
    def __init__(self):
        smis_cimv2.StorageSystemDiscoverer.__init__(self)
        self.className = 'HPEVA_StorageSystem'

    def parse(self, instances):
        result = []
        for instance in instances:
            name = stringClean(instance.getProperty('ElementName').getValue())
            description = stringClean(instance.getProperty('Description').getValue())

            serial = None

            osVersion = instance.getProperty('FirmwareVersion').getValue()

            model = instance.getProperty('Model').getValue()

            status = instance.getProperty('Status').getValue()
            vendor = instance.getProperty('Manufacturer').getValue()

            identList = instance.getProperty('IdentifyingDescriptions').getValue()
            identValue = instance.getProperty('OtherIdentifyingInfo').getValue()
            sydId = stringClean(instance.getProperty('Name').getValue())
            ip = None
            hostWwn = None
            try:
                ip = instance.getProperty('ManagingAddress').getValue()
                if not ip:
                    ipIndex = identList.index('Ipv4 Address')
                    ip = identValue[ipIndex]
                if not ip:
                    raise ValueError('ip is empty')
            except:
                logger.warn('Failed to get ip of storage system')

            try:
                hostWwnIndex = identList.index('Node WWN')
                hostWwn = identValue[hostWwnIndex]
            except:
                logger.warn('Failed to get wwn of storage system')


            hostObj = smis.Host(sydId, ip, name, sydId, description, [], [], model, serial, osVersion, vendor, status)
            result.append(hostObj)

        return result

class FcPortDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'HPEVA_DiskFCPort'

    def parse(self, instances):
        result = []
        for instance in instances:
            portId = None
            portName = stringClean(instance.getProperty('Name').getValue())
            portWwn = stringClean(instance.getProperty('PermanentAddress').getValue())
            portIndex = stringClean(instance.getProperty('PortNumber').getValue())
            portStatus = smis_cimv2.PORT_STATUS_VALUE_MAP.get(stringClean(instance.getProperty('OperationalStatus').getValue()), smis_cimv2.PORT_STATE_UNKNOWN)
            portState = instance.getProperty('Status').getValue()
            speedBps = stringClean(instance.getProperty('Speed').getValue())
            maxSpeedBps = stringClean(instance.getProperty('MaxSpeed').getValue())
            portType = smis_cimv2.PORT_TYPE_VALUE_MAP.get(stringClean(instance.getProperty('PortType').getValue()), smis_cimv2.PORT_TYPE_RESERVED)
            referencedTo = None
            container = stringClean(instance.getProperty('SystemName').getValue())
            systemId = None
            m = re.match('(.+)\..+$', container)
            if m:
                systemId = m.group(1)
            try:
                fcpObj = smis.FcPort(portId, portIndex, portWwn, portName, systemId, referencedTo, portStatus, portState, speedBps, container, maxSpeedBps, portType)
                result.append(fcpObj)
            except:
                logger.debugException('')
        return result

class PhysicalVolumeDiscoverer(smis_cimv2.PhysicalVolumeDiscoverer):
    def __init__(self):
        smis_cimv2.PhysicalVolumeDiscoverer.__init__(self)
        self.className = 'HPEVA_DiskExtent'

class LogicalVolumeDiscoverer(smis_cimv2.LogicalVolumeDiscoverer):
    def __init__(self):
        smis_cimv2.LogicalVolumeDiscoverer.__init__(self)
        self.className = 'HPEVA_StorageVolume'

    def parse(self, instances):
        result = []
        for instance in instances:
            name = stringClean(instance.getProperty('Name').getValue())
            managedSysName = stringClean(instance.getProperty('SystemName').getValue())
            blockSize = stringClean(instance.getProperty('BlockSize').getValue())
            blocksNumber = stringClean(instance.getProperty('NumberOfBlocks').getValue())
            blocksConsumable = stringClean(instance.getProperty('ConsumableBlocks').getValue())
            blocksProvisionable = stringClean(instance.getProperty('AllocatedBlocks').getValue())
            objectId = stringClean(instance.getProperty('DeviceID').getValue())
            status = instance.getProperty('Status').getValue()
            poolId = instance.getProperty('DiskGroupID').getValue()

            humanReadableName = stringClean(instance.getProperty('Caption').getValue()) or ''
            m = re.match(r'.*\\(.+)$', humanReadableName)
            if m:
                humanReadableName = m.group(1)

            freeSpaceInMb = None
            sizeInMb = None
            usedSpaceInMb = None
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
                usedSpaceInMb = float(blocksProvisionable) * int(blockSize)/ (1024 * 1024)
            except:
                logger.debugException('')
                logger.debug('Failed to convert blocksProvisionable value')
            try:
                lvObj = smis.LogicalVolume(name, managedSysName, objectId, freeSpaceInMb, sizeInMb, None, humanReadableName, status, poolId)
                result.append(lvObj)
            except:
                logger.debugException('')


        return result

class RemoteEndpointDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'HPEVA_ViewProtocolController'

    def parse(self, instances):
        result = []
        for instance in instances:
            hostName = None
            hostIp = None

            hostName = stringClean(instance.getProperty('ElementName').getValue())
            ipRaw = stringClean(instance.getProperty('HPHostIPAddress').getValue())

            if ipRaw:
                if netutils.isValidIp(ipRaw):
                    hostIp = ipRaw
                else:
                    hostName = ipRaw

            wwns = instance.getProperty('WWNs').getValue()

            objId = stringClean(instance.getProperty('DeviceID').getValue())
            for wwn in wwns:
                try:
                    endPoint = smis.RemoteEndPoint(wwn=wwn, name=hostName, portIndex = None, objId = objId, hostIp = hostIp)
                    result.append(endPoint)
                except:
                    logger.debugException('')
        return result

class StoragePoolDiscoverer(smis_cimv2.StoragePoolDiscoverer):
    def __init__(self):
        smis_cimv2.StoragePoolDiscoverer.__init__(self)
        self.className = 'HPEVA_StoragePool'

    def parse(self, instances):
        result = []
        id = 0
        for instance in instances:
            cim_id = stringClean(instance.getProperty('PoolID').getValue())

            name = stringClean(instance.getProperty('Name').getValue())
            availSpace = stringClean(instance.getProperty('TotalManagedSpace').getValue())
            freeSpace = stringClean(instance.getProperty('RemainingManagedSpace').getValue())
            type = None
            try:
                type = stringClean(instance.getProperty('DiskGroupType').getValue())
            except:
                logger.warn('Failed to get pool type')

            try:
                pool = smis.StoragePool(name = name, parentReference = None, id = id, type = type, availableSpaceInMb = freeSpace, totalSpaceInMb = availSpace,\
                     unExportedSpaceInMb = freeSpace, dataRedund = None, lvmIds = None, cimId = cim_id)
                result.append(pool)
                id += 1
            except:
                logger.debugException('')
        return result