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
        self.__namespace = 'root/tpd'

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

#        self.__handlers.append( topologyFieldHanlder( topology.file_shares,
#                                                     FileShareCimv2Discoverer() ) )

#        self.__handlers.append( topologyFieldHanlder( topology.file_systems,
#                                                     FileSystemCimv2Discoverer() ) )

class StorageProcessorDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'TPD_NodeSystem'

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
            if instance.getClassName() != 'TPD_NodeSystem':
                continue

            name = instance.getPropertyValue('ElementName')
            id = instance.getPropertyValue('Name')
            system_path = instance.getPropertyValue('Description')
            version = instance.getPropertyValue('KernelVersion')
            status = smis_cimv2.PROCESSOR_STATE_VALUE_MAP.get(stringClean(instance.getPropertyValue('HealthState')), 'Unknown')

            serial = None
            node_wwn = None

            parent = processorToArrayMap.get(id)
            try:
                storage_processor = smis.StorageProcessor(id, name, node_wwn, system_path, version, serial, status, parent=parent)
                result.append(storage_processor)
            except:
                logger.warnException('')

        return result

    def getParentRelationship(self, client):
        processorToArrayMap = {}
        relationships = client.getInstances('TPD_NodeComponentCS')

        for relationship in relationships:
            parentRef = relationship.getPropertyValue('GroupComponent')
            childRef = relationship.getPropertyValue('PartComponent')
            if not parentRef or not childRef:
                continue
            parentId = stringClean(parentRef.getKeyValue('Name'))
            childId = stringClean(parentRef.getKeyValue('Name'))
            processorToArrayMap[childId] = parentId

        return processorToArrayMap

class EndPointToVolumeDiscoverer(smis_cimv2.EndPointToVolumeDiscoverer):
    def __init__(self):
        smis_cimv2.EndPointToVolumeDiscoverer.__init__(self)

class PhysicalVolume2StoragePoolDiscover(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'TPD_StoragePoolComponent'

    def parse(self, links):
        result = {}
        for link in links:
            pvs = []
            try:
                poolRef = link.getPropertyValue('GroupComponent')
                poolId = stringClean(poolRef.getKeyValue('InstanceID'))
                mappedPVs = result.get(poolId)
                if mappedPVs:
                    pvs = mappedPVs

                phyRef = link.getPropertyValue('PartComponent')
                pvId = stringClean(phyRef.getKeyValue('DeviceID'))
                pvContainer = stringClean(phyRef.getKeyValue('SystemName'))
                pvs.append(pvContainer+pvId)
            except:
                logger.debugException('cannot find the physical volume to storage pool linkages')

            result[poolId] = pvs

        return result

class StorageSystemDiscoverer(smis_cimv2.StorageSystemDiscoverer):
    def __init__(self):
        smis_cimv2.StorageSystemDiscoverer.__init__(self)
        self.className = 'TPD_StorageSystem'

class FcPortDiscoverer(smis_cimv2.FcPortDiscoverer):
    def __init__(self):
        smis_cimv2.FcPortDiscoverer.__init__(self)
        self.className = 'TPD_FCPort' #we only care front end for remote connection

class PhysicalVolumeDiscoverer(smis_cimv2.PhysicalVolumeDiscoverer):
    def __init__(self):
        smis_cimv2.PhysicalVolumeDiscoverer.__init__(self)
        self.className = 'TPD_DiskStorageExtent'

class LogicalVolumeDiscoverer(smis_cimv2.LogicalVolumeDiscoverer):
    def __init__(self):
        smis_cimv2.LogicalVolumeDiscoverer.__init__(self)
        self.className = 'TPD_StorageVolume'

class RemoteEndpointDiscoverer(smis_cimv2.RemoteEndpointDiscoverer):
    def __init__(self):
        smis_cimv2.RemoteEndpointDiscoverer.__init__(self)
        self.className = 'TPD_StorageHardwareID'

class LunMaskingMappingViewDiscover(smis_cimv2.LunMaskingMappingViewDiscover):
    def __init__(self):
        smis_cimv2.LunMaskingMappingViewDiscover.__init__(self)

class HostDicoverer(smis_cimv2.HostDicoverer):
    def __init__(self):
        smis_cimv2.HostDicoverer.__init__(self)
        self.className = 'TPD_NodeSystem'

class StoragePoolDiscoverer(smis_cimv2.StoragePoolDiscoverer):
    def __init__(self):
        smis_cimv2.StoragePoolDiscoverer.__init__(self)
        self.className = 'CIM_StoragePool'

    def discover(self, client):
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        instances = client.getInstances(self.className)
        return self.parse(client, instances)

    def parse(self, client, instances):
        result = []
        poolToLvIds = {}
        childPoolMap = {}
        smis_cimv2.discoverPoolLinks(client, childPoolMap, poolToLvIds )
        for instance in instances:
            name = stringClean(instance.getPropertyValue('Name'))
            availSpace = stringClean(instance.getPropertyValue('TotalManagedSpace'))
            freeSpace = stringClean(instance.getPropertyValue('RemainingManagedSpace'))
            type = stringClean(instance.getPropertyValue('ResourceType'))
            id = 0
            instanceId = stringClean(instance.getPropertyValue('InstanceID'))
            cim_id = instanceId
            poolToChildPoolIds = childPoolMap.get(cim_id)
            try:
                pool = smis.StoragePool(name = name, parentReference = None, id = id, type = type, availableSpaceInMb = freeSpace, totalSpaceInMb = availSpace,\
                     unExportedSpaceInMb = freeSpace, dataRedund = None, lvmIds = poolToLvIds.get(instanceId), cimId = cim_id, childPoolIds=poolToChildPoolIds)
                result.append(pool)
            except:
                logger.debugException('')
        return result
