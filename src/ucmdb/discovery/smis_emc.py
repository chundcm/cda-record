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
        self.__namespace = 'root/emc'


    def associateTopologyObj2Discoverers(self, topology):
        self.handlers.append(topologyFieldHanlder(topology.storage_systems,
                                                  StorageSystemDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.storage_processors,
                                                  StorageProcessorDiscoverer()))


        self.handlers.append(topologyFieldHanlder(topology.end_point_links,
                                                  EndPointToVolumeDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.lun_mappings,
                                                  LunMaskingMappingViewDiscover()))

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
                                                  PhysicalVolume2StoragePoolDiscover()))

        self.fallback_handlers[LogicalVolumeDiscoverer] = topologyFieldHanlder(topology.logical_volumes,
                                                                                LogicalVolumeEMCDiscovererFallback())
        self.fallback_handlers[PhysicalVolumeDiscoverer] = topologyFieldHanlder(topology.physical_volumes,
                                                                                PhysicalVolumeEMCDiscovererFallBack())
        self.fallback_handlers[FcPortDiscoverer] = topologyFieldHanlder(topology.ports,
                                                                        FcPortEMCDiscovererFallBack())

#        self.__handlers.append( topologyFieldHanlder( topology.file_shares,
#                                                     FileShareCimv2Discoverer() ) )

#        self.__handlers.append( topologyFieldHanlder( topology.file_systems,
#                                                     FileSystemCimv2Discoverer() ) )

class StorageProcessorDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'EMC_StorageProcessorSystem'

    def discover(self, client):
        canisters = []
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        instances = client.getInstances(self.className)
        return self.parse(instances)

    def parse(self, instances):
        result = []
        for instance in instances:
            name = instance.getPropertyValue('ElementName')
            serial = instance.getPropertyValue('EMCSerialNumber')
            version = instance.getPropertyValue('EMCPromRevision')
            model = None
            vendor = None
            ip = None
            id = stringClean(instance.getPropertyValue('Name'))
            parent = None
            plusElement = id.split('+')
            if len(plusElement) >= 2:
                parent = "+".join(plusElement[0:2])
            system_path = stringClean(instance.getPropertyValue('Caption'))
            status = instance.getPropertyValue('StatusDescriptions') and ",".join(instance.getPropertyValue('StatusDescriptions'))
            node_wwn = None
            try:
                storage_processor = smis.StorageProcessor(id, name, node_wwn, system_path, version, serial, status, model, vendor, ip, parent)
                result.append(storage_processor)
            except:
                logger.warnException('')

        return result

class LunMaskingMappingViewDiscover(smis_cimv2.LunMaskingMappingViewDiscover):
    def __init__(self):
        smis_cimv2.LunMaskingMappingViewDiscover.__init__(self)

    def discover_from_mappingview(self, client):
        result = []
        className = 'EMC_MaskingMappingView'
        logger.debug('Queuing class "%s"' % className)
        instances = []
        SHIdAndLunId2LvMap = {}
        SHIdAndLunId2LocalWWNMap = {}
        SHId2LunIdMap = {}
        SHID2SHObjMap = {}

        try:
            instances = client.getInstances(className)
        except:
            pass
        for instance in instances:
            lv = instance.getPropertyValue('LogicalDevice')
            lvId = lv and stringClean(lv.getKeyValue('DeviceID'))
            managedSysName = lv and stringClean(lv.getKeyValue('SystemName'))
            lvRef = smis.LogicalVolumeId(managedSysName, lvId)

            endPoint = instance.getPropertyValue('ProtocolEndpoint')
            localWwn  = endPoint and stringClean(endPoint.getKeyValue('Name'))

            shid = stringClean(instance.getPropertyValue('SHIDStorageID'))
            shidtype = stringClean(instance.getPropertyValue('SHIDIDType'))
            shIdObj = smis.StorageHardwareId( shid, shidtype)
            SHID2SHObjMap[shid] = shIdObj

            lun_id = stringClean(instance.getPropertyValue('PCFUDeviceNumber'))
            lunId = str(int(lun_id, 16))
            luns = []
            if SHId2LunIdMap.get(shid):
                luns = SHId2LunIdMap.get(shid)
            luns.append(lunId)
            SHId2LunIdMap[shid] = luns

            lvs = []
            if SHIdAndLunId2LvMap.get(shid+lunId):
                lvs = SHIdAndLunId2LvMap.get(shid+lunId)
            lvs.append(lvRef)
            SHIdAndLunId2LvMap[shid+lunId] = lvs

            localWWNs = []
            if SHIdAndLunId2LocalWWNMap.get(shid+lunId):
                localWWNs = SHIdAndLunId2LocalWWNMap.get(shid+lunId)
            localWWNs.append(localWwn)
            SHIdAndLunId2LocalWWNMap[shid+lunId] = localWWNs

        for shid in SHId2LunIdMap.keys():
            luns = SHId2LunIdMap.get(shid)
            for lunid in luns:
                localWwns = SHIdAndLunId2LocalWWNMap.get(shid+lunid)
                lvs = SHIdAndLunId2LvMap.get(shid+lunid)
                shIdObj = SHID2SHObjMap.get(shid)
                lun = smis.LUN(lunid)
                mappingView = smis.LunMaskingMappingView(lvs, shIdObj, localWwns, lun)
                result.append(mappingView)
        return result

class PhysicalVolume2StoragePoolDiscover(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'EMC_ConcreteComponentView'

    def parse(self, links):
        result = {}
        for link in links:
            pvs = []
            try:
                poolRef = link.getPropertyValue('GroupComponent')
                poolId = stringClean(poolRef.getKey('InstanceID').getValue())
                mappedPVs = result.get(poolId)
                if mappedPVs:
                    pvs = mappedPVs

                phyRef = link.getPropertyValue('PartComponent')
                pvId = stringClean(phyRef.getKey('DDDeviceID').getValue())
                pvContainer = stringClean(phyRef.getKey('DDSystemName').getValue())
                pvs.append(pvContainer+pvId)
            except:
                logger.debugException('cannot find the physical volume to storage pool linkages')

            result[poolId] = pvs

        return result

class EndPointToVolumeDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)

    def discover(self, client):
        return []

class StorageSystemDiscoverer(smis_cimv2.StorageSystemDiscoverer):
    def __init__(self):
        smis_cimv2.StorageSystemDiscoverer.__init__(self)
        self.className = 'EMC_StorageSystem'
        self.systemSoftwares = 'EMC_StorageSystemSoftwareIdentity'
        self.arrayChassis = 'EMC_ArrayChassis'

    def discover(self, client):
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        instances = client.getInstances(self.className)
        if self.systemSoftwares:
            softwares = client.getInstances(self.systemSoftwares)
        if self.arrayChassis:
            chassises = client.getInstances(self.arrayChassis)

        return self.parse(instances, softwares, chassises)

    def parse(self, instances, systemSoftwares, arrayChassises):
        result = []
        softwareMap = {}
        chassisMap = {}
        for software in systemSoftwares:
            softwareMap[stringClean(software.getPropertyValue('InstanceID'))] = software
        for chassis in arrayChassises:
            chassisMap[stringClean(chassis.getPropertyValue('Tag'))] = chassis

        for instance in instances:
            osVersion = None
            vendor = None
            model = None
            serial = None
            name = stringClean(instance.getPropertyValue('ElementName'))
            sydId = stringClean(instance.getPropertyValue('Name'))
            if sydId in softwareMap:
                software = softwareMap[sydId]
                osVersion = stringClean(software.getPropertyValue('VersionString'))
                vendor = stringClean(software.getPropertyValue('Manufacturer'))

            if sydId in chassisMap:
                chassis = chassisMap[sydId]
                serial = stringClean(chassis.getPropertyValue('SerialNumber'))
                model = stringClean(chassis.getPropertyValue('Model'))

            status = instance.getPropertyValue('StatusDescriptions') and ",".join(instance.getPropertyValue('StatusDescriptions'))
            description = stringClean(instance.getPropertyValue('Description'))

            ip = None
            hostObj = smis.Host(sydId, ip, name, sydId, description, [], [], model, serial, osVersion, vendor, status)
            result.append(hostObj)

        return result

class FcPortDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'Clar_FrontEndFCPort' #we only care front end for remote connection

    def parse(self, instances):
        result = []
        for instance in instances:
            portId = stringClean(instance.getPropertyValue('PortNumber'))
            portName = stringClean(instance.getPropertyValue('EMCPortName'))
            portWwn  = instance.getPropertyValue('PermanentAddress')
            deviceId = stringClean(instance.getPropertyValue('DeviceID'))
            if deviceId and portWwn is None:
                portWwn = deviceId
            portIndex = stringClean(instance.getPropertyValue('PortNumber'))
            portStatus = instance.getPropertyValue('StatusDescriptions') and ",".join(instance.getPropertyValue('StatusDescriptions'))
            portState = smis_cimv2.PORT_STATE_VALUE_MAP.get(stringClean(instance.getPropertyValue('HealthState')), smis_cimv2.PORT_STATE_UNKNOWN)
            speedBps = stringClean(instance.getPropertyValue('Speed'))
            maxSpeedBps = instance.getPropertyValue('MaxSpeed')
            portType = smis_cimv2.PORT_TYPE_VALUE_MAP.get(stringClean(instance.getPropertyValue('PortType')), smis_cimv2.PORT_TYPE_RESERVED)
            referencedTo = None
            container = stringClean(instance.getPropertyValue('SystemName'))
            systemId = stringClean(instance.getPropertyValue('ElementName'))
            try:
                fcpObj = smis.FcPort(portId, portIndex, portWwn, portName, container, referencedTo, portStatus, portState, speedBps, container, maxSpeedBps, portType)
                result.append(fcpObj)
            except:
                logger.debugException('')
        return result

class PhysicalVolumeDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'EMC_DiskDriveView'

    def parse(self, instances):
        result = []
        for instance in instances:
            name = stringClean(instance.getPropertyValue('DDName'))
            managedSysName = stringClean(instance.getPropertyValue('DDSystemName'))
            blockSize = stringClean(instance.getPropertyValue('SEBlockSize'))
            blocksNumber = stringClean(instance.getPropertyValue('SENumberOfBlocks'))
            objectId = stringClean(instance.getPropertyValue('DDDeviceID'))
            #humanReadableName = stringClean(instance.getPropertyValue('ElementName')) or ''
            sizeInMb = None
            try:
                sizeInMb = float(blocksNumber) * int(blockSize)/ (1024 * 1024)
            except:
                logger.debugException('')
                logger.debug('Failed to convert sizeInMb value')

            try:
                pvObj = smis.PhysicalVolume(name, managedSysName, objectId, sizeInMb)
                result.append(pvObj)
            except:
                logger.debugException('')

        return result

class LogicalVolumeDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'EMC_VolumeView'

    def parse(self, instances):
        result = []
        for instance in instances:
            name = stringClean(instance.getPropertyValue('SVDeviceID'))
            managedSysName = stringClean(instance.getPropertyValue('SVSystemName'))
            blockSize = stringClean(instance.getPropertyValue('SVBlockSize'))
            blocksNumber = stringClean(instance.getPropertyValue('SVNumberOfBlocks'))
            blocksConsumable = stringClean(instance.getPropertyValue('SVConsumableBlocks'))

            objectId = stringClean(instance.getPropertyValue('SVDeviceID'))
            status = smis_cimv2.getOperationalStatus(instance, property = 'SVOperationalStatus')
            poolId = stringClean(instance.getPropertyValue('SPInstanceID'))

            humanReadableName = stringClean(instance.getPropertyValue('SVElementName'))

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
        self.className = 'SE_StorageHardwareID'

    def parse(self, instances):
        result = []
        for instance in instances:
            name = stringClean(instance.getPropertyValue('ElementName'))
            hostName = stringClean(instance.getPropertyValue('EMCHostName'))
            hostIp = stringClean(instance.getPropertyValue('EMCIpAddress'))

            deviceId = stringClean(instance.getPropertyValue('StorageID'))

            portIndex = None
            try:
                if hostName or hostIp:
                    endPoint = smis.RemoteEndPoint(wwn=deviceId, name=hostName, portIndex=portIndex, hostIp=hostIp)
                    result.append(endPoint)
            except:
                logger.debugException('')
                logger.debug('Using %s, %s, %s' % (name, deviceId, portIndex))
        return result

class StoragePoolDiscoverer(smis_cimv2.StoragePoolDiscoverer):
    def __init__(self):
        smis_cimv2.StoragePoolDiscoverer.__init__(self)
        self.className = 'EMC_StoragePool'

    def discover(self, client):
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        instances = client.getInstances(self.className)
        return self.parse(instances, client)

    def parse(self, instances, client):
        result = []
        poolToLvIds = {}
        childPoolMap = {}
        smis_cimv2.discoverPoolLinks(client, childPoolMap, poolToLvIds )
        id = 0
        for instance in instances:
            name = stringClean(instance.getPropertyValue('ElementName'))
            availSpace = stringClean(instance.getPropertyValue('TotalManagedSpace'))
            freeSpace = stringClean(instance.getPropertyValue('RemainingManagedSpace'))
            type = None
            instanceId = stringClean(instance.getPropertyValue('InstanceID')) or 0
            cim_id = instanceId
            poolToChildPoolIds = childPoolMap.get(cim_id)
            parent = None
            plusElement = instanceId.split('+')
            if len(plusElement) >= 2:
                parent = "+".join(plusElement[0:2])

            try:
                pool = smis.StoragePool(name = name, parentReference = parent, id = id, type = type, availableSpaceInMb = freeSpace, totalSpaceInMb = availSpace,\
                     unExportedSpaceInMb = freeSpace, dataRedund = None, lvmIds = poolToLvIds.get(instanceId), cimId = cim_id, childPoolIds=poolToChildPoolIds )
                result.append(pool)
            except:
                logger.debugException('')
        return result


class FcPortEMCDiscovererFallBack(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'EMC_FCPort'

    def parse(self, instances):
        result = []
        for instance in instances:
            portId = stringClean(instance.getProperty('PortNumber').getValue())
            portName = stringClean(instance.getProperty('EMCPortName').getValue())
            portWwn = instance.getProperty('PermanentAddress').getValue()
            deviceId = stringClean(instance.getProperty('DeviceID').getValue())
            if deviceId and portWwn is None:
                portWwn = deviceId
            portIndex = stringClean(instance.getProperty('PortNumber').getValue())
            portStatus = ",".join(instance.getProperty('StatusDescriptions').getValue())
            portState = smis_cimv2.PORT_STATE_VALUE_MAP.get(stringClean(instance.getProperty('HealthState').getValue()),
                                                            smis_cimv2.PORT_STATE_UNKNOWN)
            speedBps = stringClean(instance.getProperty('Speed').getValue())
            maxSpeedBps = None
            property = instance.getProperty('MaxSpeed')
            if property:
                maxSpeedBps = stringClean(property.getValue())
            portType = smis_cimv2.PORT_TYPE_VALUE_MAP.get(stringClean(instance.getProperty('PortType').getValue()),
                                                          smis_cimv2.PORT_TYPE_RESERVED)
            referencedTo = None
            container = stringClean(instance.getProperty('SystemName').getValue())
            systemId = stringClean(instance.getProperty('ElementName').getValue())
            try:
                fcpObj = smis.FcPort(portId, portIndex, portWwn, portName, container, referencedTo, portStatus,
                                     portState, speedBps, container, maxSpeedBps, portType)
                result.append(fcpObj)
            except:
                logger.debugException('')
        return result


class PhysicalVolumeEMCDiscovererFallBack(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'EMC_DiskExtent'

    def parse(self, instances):
        result = []
        for instance in instances:
            name = stringClean(instance.getProperty('Name').getValue())
            managedSysName = stringClean(instance.getProperty('SystemName').getValue())
            blockSize = stringClean(instance.getProperty('BlockSize').getValue())
            blocksNumber = stringClean(instance.getProperty('NumberOfBlocks').getValue())
            objectId = stringClean(instance.getProperty('DeviceID').getValue())
            humanReadableName = stringClean(instance.getProperty('ElementName').getValue()) or ''
            sizeInMb = None
            try:
                sizeInMb = float(blocksNumber) * int(blockSize)
            except:
                logger.debugException('')
                logger.debug('Failed to convert sizeInMb value')

            try:
                pvObj = smis.PhysicalVolume(name, managedSysName, objectId, sizeInMb, humanReadableName)
                result.append(pvObj)
            except:
                logger.debugException('')

        return result


class LogicalVolumeCimv2Discoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'CIM_StorageVolume'

    def parse(self, instances):
        result = []
        for instance in instances:
            name = stringClean(instance.getProperty('Name').getValue())
            managedSysName = stringClean(instance.getProperty('SystemName').getValue())
            objectId = stringClean(instance.getProperty('DeviceID').getValue())
            blockSize = stringClean(instance.getProperty('BlockSize').getValue())
            blocksNumber = stringClean(instance.getProperty('NumberOfBlocks').getValue())
            blocksConsumable = stringClean(instance.getProperty('ConsumableBlocks').getValue())
            blocksProvisionable = stringClean(instance.getProperty('ProvisionedConsumableBlocks').getValue())
            humanReadableName = stringClean(instance.getProperty('ElementName').getValue())
            freeSpaceInMb = None
            sizeInMb = None
            usedSpaceInMb = None
            try:
                sizeInMb = float(blocksNumber) * int(blockSize)
            except:
                logger.debugException('')
                logger.debug('Failed to convert sizeInMb value')
            try:
                freeSpaceInMb = float(blocksConsumable) * int(blockSize)
            except:
                logger.debugException('')
                logger.debug('Failed to convert freeSpaceInMb value')
            try:
                usedSpaceInMb = float(blocksProvisionable) * int(blockSize)
            except:
                logger.debugException('')
                logger.debug('Failed to convert blocksProvisionable value')
            try:
                lvObj = smis.LogicalVolume(name, managedSysName, objectId, freeSpaceInMb, sizeInMb, None,
                                           humanReadableName)
                result.append(lvObj)
            except:
                logger.debugException('')
        return result


class LogicalVolumeEMCDiscovererFallback(LogicalVolumeCimv2Discoverer):
    def __init__(self):
        LogicalVolumeCimv2Discoverer.__init__(self)
        self.className = 'EMC_StorageVolume'

    def parse(self, instances):
        result = []
        for instance in instances:
            name = stringClean(instance.getProperty('Name').getValue())
            managedSysName = stringClean(instance.getProperty('SystemName').getValue())
            blockSize = stringClean(instance.getProperty('BlockSize').getValue())
            blocksNumber = stringClean(instance.getProperty('NumberOfBlocks').getValue())
            blocksConsumable = stringClean(instance.getProperty('ConsumableBlocks').getValue())
            # blocksProvisionable = stringClean(instance.getProperty('AllocatedBlocks').getValue())
            objectId = stringClean(instance.getProperty('DeviceID').getValue())
            status = ",".join(instance.getProperty('StatusDescriptions').getValue())
            poolId = None

            humanReadableName = stringClean(instance.getProperty('ElementName').getValue())

            freeSpaceInMb = None
            sizeInMb = None
            try:
                sizeInMb = float(blocksNumber) * int(blockSize)
            except:
                logger.debugException('')
                logger.debug('Failed to convert sizeInMb value')
            try:
                freeSpaceInMb = float(blocksConsumable) * int(blockSize)
            except:
                logger.debugException('')
                logger.debug('Failed to convert freeSpaceInMb value')
            try:
                lvObj = smis.LogicalVolume(name, managedSysName, objectId, freeSpaceInMb, sizeInMb, None,
                                           humanReadableName, status, poolId)
                result.append(lvObj)
            except:
                logger.debugException('')

        return result
