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
        self.__namespace = 'purestorage'

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
                                                  PhysicalVolume2StoragePoolDiscoverer()))

#        self.__handlers.append( topologyFieldHanlder( topology.file_shares,
#                                                     FileShareCimv2Discoverer() ) )
                                                  
#        self.__handlers.append( topologyFieldHanlder( topology.file_systems,
#                                                     FileSystemCimv2Discoverer() ) )


class StorageProcessorDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'PURESTORAGE_Controller'

    def discover(self, client):
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        instances = client.getInstances(self.className)
        return self.parse(instances, client)

    def parse(self, instances, client):
        processorToArrayMap = self.getParentRelationship(client)
        result = []
        for instance in instances:
            name = instance.getProperty('ElementName').getValue()
            id = instance.getProperty('Name').getValue()
            status = smis_cimv2.PROCESSOR_STATE_VALUE_MAP.get(stringClean(instance.getProperty('HealthState').getValue()), 'Unknown')
            parent = processorToArrayMap.get(id)

            try:
                storage_processor = smis.StorageProcessor(id, name, status=status, parent=parent)
                result.append(storage_processor)
            except:
                pass

        return result

    def getParentRelationship(self, client):
        processorToArrayMap = {}
        relationships = client.getInstances('PURESTORAGE_ComponentController')

        for relationship in relationships:
            parentRef = relationship.getProperty('GroupComponent').getValue()
            childRef = relationship.getProperty('PartComponent').getValue()
            if not parentRef or not childRef:
                continue
            parentId = stringClean(parentRef.getKey('Name').getValue())
            childId = stringClean(parentRef.getKey('Name').getValue())
            processorToArrayMap[childId] = parentId

        return processorToArrayMap

class PhysicalVolume2StoragePoolDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'PURESTORAGE_DisksComponentOfPrimordialStoragePool'

    def parse(self, links):
        result = {}
        for link in links:
            pvs = []
            try:
                poolRef = link.getProperty('GroupComponent').getValue()
                poolId = stringClean(poolRef.getKey('InstanceID').getValue())
                mappedPVs = result.get(poolId)
                if mappedPVs:
                    pvs = mappedPVs

                phyRef = link.getProperty('PartComponent').getValue()
                pvId = stringClean(phyRef.getKey('DeviceID').getValue())
                pvContainer = stringClean(phyRef.getKey('SystemName').getValue())
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

class StorageSystemDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'PURESTORAGE_Array'

    def parse(self, instances):
        result = []
        for instance in instances:
            name = stringClean(instance.getProperty('ElementName').getValue())
            description = None
            osVersion = None
            sydId = stringClean(instance.getProperty('Name').getValue())
            status = smis_cimv2.getOperationalStatus(instance, property = 'OperationalStatus')
            ip = None
            vendor = "Pure Storage"
            model = None
            serial = None
            identList = instance.getProperty('IdentifyingDescriptions').getValue()
            identValue = instance.getProperty('OtherIdentifyingInfo').getValue()
            try:
                index = identList.index('ipv4 address')
                fields = stringClean(identValue[index])
                others = fields.split('+')
                if len(others) == 1:
                    ip = others[0].strip()                    
            except:
                pass

            hostObj = smis.Host(sydId, ip, name, sydId, description, [], [], model, serial, osVersion, vendor, status)            
            result.append(hostObj)

        return result

class FcPortDiscoverer(smis_cimv2.FcPortDiscoverer):
    def __init__(self):
        smis_cimv2.FcPortDiscoverer.__init__(self)
        self.className = 'PURESTORAGE_FCTargetPort' #we only care front end for remote connection


class PhysicalVolumeDiscoverer(smis_cimv2.PhysicalVolumeDiscoverer):
    def __init__(self):
        smis_cimv2.PhysicalVolumeDiscoverer.__init__(self)
        self.className = 'PURESTORAGE_DiskExtent'

class LogicalVolumeDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'PURESTORAGE_StorageVolume'

    def parse(self, instances):
        result = []
        for instance in instances:
            name = stringClean(instance.getPropertyValue('Name'))
            managedSysName = stringClean(instance.getPropertyValue('SystemName'))
            objectId = stringClean(instance.getPropertyValue('DeviceID'))
            blockSize = stringClean(instance.getPropertyValue('BlockSize'))
            blocksNumber = stringClean(instance.getPropertyValue('ConsumableBlocks'))
            humanReadableName = stringClean(instance.getPropertyValue('ElementName'))
            freeSpaceInMb = None
            sizeInMb = None
            try:
                sizeInMb = float(blocksNumber) * int(blockSize)/(1024 * 1024)
            except:
                logger.debugException('')
                logger.debug('Failed to convert sizeInMb value')
            try:
                lvObj = smis.LogicalVolume(name, managedSysName, objectId, freeSpaceInMb, sizeInMb, None, humanReadableName)
                result.append(lvObj)
            except:
                logger.debugException('')
        return result

class RemoteEndpointDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'CIM_StorageHardwareID'

    def discover(self, client):
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        instances = client.getInstances(self.className)
        return self.parse(instances, client)

    def parse(self, instances, client):
        instanceToArrayMap = self.getParentRelationship(client)
        result = []
        for instance in instances:
            instanceId = stringClean(instance.getPropertyValue('InstanceID'))
            deviceId = stringClean(instance.getPropertyValue('StorageID'))

            portIndex = None
            hostIp = None
            try:
                endPoint = smis.RemoteEndPoint(wwn=deviceId, name=hostName, portIndex=portIndex, hostIp=hostIp)
                result.append(endPoint)
            except:
                pass
        return result

    def getParentRelationship(self, client):
        instanceToArrayMap = {}
        relationships = client.getInstances('PURESTORAGE_SHIDManagementServiceDependency')

        for relationship in relationships:
            parentRef = relationship.getProperty('Antecedent').getValue()
            childRef = relationship.getProperty('Dependent').getValue()
            if not parentRef or not childRef:
                continue
            parentId = stringClean(parentRef.getKey('SystemName').getValue())
            childId = stringClean(childRef.getKey('InstanceID').getValue())
            instanceToArrayMap[childId] = parentId

        return instanceToArrayMap
            
class StoragePoolDiscoverer(smis_cimv2.StoragePoolDiscoverer):
    def __init__(self):
        smis_cimv2.StoragePoolDiscoverer.__init__(self)
        self.className = 'PURESTORAGE_PrimordialStoragePool'

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
        processorToArrayMap = self.getParentRelationship(client)
        id = 0
        for instance in instances:
            #id = stringClean(instance.getProperty('PoolID').getValue())
            name = stringClean(instance.getProperty('ElementName').getValue())
            availSpace = stringClean(instance.getProperty('TotalManagedSpace').getValue())
            freeSpace = stringClean(instance.getProperty('RemainingManagedSpace').getValue())
            type = None
            instanceId = stringClean(instance.getProperty('InstanceID').getValue()) or 0
            cim_id = instanceId
            poolToChildPoolIds = childPoolMap.get(cim_id)
            parent = processorToArrayMap.get(instanceId)
            try:
                pool = smis.StoragePool(name = name, parentReference = parent, id = id, type = type, availableSpaceInMb = freeSpace, totalSpaceInMb = availSpace,\
                     unExportedSpaceInMb = freeSpace, dataRedund = None, lvmIds = poolToLvIds.get(instanceId), cimId = cim_id, childPoolIds=poolToChildPoolIds )
                result.append(pool)
            except:
                pass
        return result

    def getParentRelationship(self, client):
        processorToArrayMap = {}
        relationships = client.getInstances('PURESTORAGE_HostedPrimordialPool')

        for relationship in relationships:
            parentRef = relationship.getProperty('GroupComponent').getValue()
            childRef = relationship.getProperty('PartComponent').getValue()
            if not parentRef or not childRef:
                continue
            parentId = stringClean(parentRef.getKey('Name').getValue())
            childId = stringClean(childRef.getKey('InstanceID').getValue())
            processorToArrayMap[childId] = parentId

        return processorToArrayMap
