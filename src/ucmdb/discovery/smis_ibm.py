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
        self.__namespace = 'root/ibm'

    def associateTopologyObj2Discoverers(self, topology):
        self.handlers.append(topologyFieldHanlder(topology.storage_systems,
                                                  StorageSystemDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.storage_processors,
                                                  StorageProcessorDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.end_point_links,
                                                  smis_cimv2.EndPointToVolumeDiscoverer()))

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
                                                  smis_cimv2.RemoteEndpointDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.storage_pools,
                                                  StoragePoolDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.physcial_volumes_2_pool_links,
                                                  PhysicalVolume2StoragePoolDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.iogroups,
                                              IOGroupDiscoverer()))

#        self.__handlers.append( topologyFieldHanlder( topology.file_shares,
#                                                     FileShareCimv2Discoverer() ) )

#        self.__handlers.append( topologyFieldHanlder( topology.file_systems,
#                                                     FileSystemCimv2Discoverer() ) )

class StorageProcessorDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'IBMTSSVC_Node'

    def discover(self, client):
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        instances = client.getInstances(self.className)
        return self.parse(instances, client)

    def parse(self, instances, client):
        processorToArrayMap = self.getParentRelationship(client)
        chassisToProcessorMap = self.getChassises(client)
        result = []
        for instance in instances:
            name = instance.getPropertyValue('ElementName')
            id = instance.getPropertyValue('Name')
            system_path = None
            status = smis_cimv2.getOperationalStatus(instance)

            chassis = chassisToProcessorMap.get(id)
            serial = None
            vendor = None
            model = None
            version = None
            if chassis:
                model = chassis.model
                serial = chassis.serialNumber
                vendor = chassis.manufacturer
                version = chassis.version

            node_wwn = None
            identList = instance.getPropertyValue('IdentifyingDescriptions')
            identValue = instance.getPropertyValue('OtherIdentifyingInfo')
            try:
                nodeWwnIndex = identList.index('FC WWNN')
                node_wwn = identValue[nodeWwnIndex]
            except:
                logger.warn('Failed to get wwn of storage processor')

            parent = processorToArrayMap.get(id)
            try:
                storage_processor = smis.StorageProcessor(id, name, node_wwn, system_path, version, serial, status,
                                                          model, vendor, parent=parent)
                result.append(storage_processor)
            except:
                pass
        return result

    def getParentRelationship(self, client):
        processorToArrayMap = {}
        relationships = client.getInstances('IBMTSSVC_NodeComponentOfCluster')

        for relationship in relationships:
            parentRef = relationship.getPropertyValue('GroupComponent')
            childRef = relationship.getPropertyValue('PartComponent')
            if not parentRef or not childRef:
                continue
            parentId = stringClean(parentRef.getKeyValue('Name'))
            childId = stringClean(childRef.getKeyValue('Name'))
            processorToArrayMap[childId] = parentId

        return processorToArrayMap

    def getChassises(self, client):
        chassisToProcessorMap = {}
        instances = client.getInstances('CIM_Chassis')  # get all chassis for huawei two types of storage
        for instance in instances:
            parentRef = stringClean(instance.getPropertyValue('Tag'))
            manufacturer = stringClean(instance.getPropertyValue('Manufacturer'))
            serialNumber = stringClean(instance.getPropertyValue('SerialNumber'))
            model = stringClean(instance.getPropertyValue('Model'))
            version = stringClean(instance.getPropertyValue('Version'))
            chassis = smis.Chassis(parentRef, manufacturer, model, serialNumber, version)
            chassisToProcessorMap[parentRef] = chassis
        return chassisToProcessorMap

class IOGroupDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'IBMTSSVC_IOGroup'

    def discover(self, client):
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        instances = client.getInstances(self.className)
        return self.parse(instances, client)

    def parse(self, instances, client):
        iogroupToArrayMap = self.getParentRelationship(client)
        result = []
        for instance in instances:
            name = instance.getPropertyValue('ElementName')
            id = instance.getPropertyValue('Name')

            nodes = []
            identList = instance.getPropertyValue('IdentifyingDescriptions')
            identValue = instance.getPropertyValue('OtherIdentifyingInfo')
            try:
                for ident,value in zip(identList, identValue):
                    if ident == 'Node Name':
                        nodes.append(value)
            except:
                logger.warn('Failed to get node  of IO group')

            parent = iogroupToArrayMap.get(id)
            try:
                iogroup = smis.IOGroup(id, name, parent, nodes)
                result.append(iogroup)
            except:
                pass
        return result

    def getParentRelationship(self, client):
        processorToArrayMap = {}
        relationships = client.getInstances('IBMTSSVC_IOGroupComponentOfCluster')

        for relationship in relationships:
            parentRef = relationship.getPropertyValue('GroupComponent')
            childRef = relationship.getPropertyValue('PartComponent')
            if not parentRef or not childRef:
                continue
            parentId = stringClean(parentRef.getKeyValue('Name'))
            childId = stringClean(childRef.getKeyValue('Name'))
            processorToArrayMap[childId] = parentId

        return processorToArrayMap

class PhysicalVolume2StoragePoolDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'IBMTSSVC_StoragePoolComponent'

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
        self.className = 'IBMTSSVC_Cluster'
        self.IBMTSSVC_Product = 'IBMTSSVC_Product'

    def discover(self, client):
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        instances = client.getInstances(self.className)
        if self.IBMTSSVC_Product:
            products = client.getInstances(self.IBMTSSVC_Product)
        return self.parse(instances, products, client)

    def parse(self, instances, products, client):
        result = []
        productMap = {}
        for product in products:
            productMap[stringClean(product.getPropertyValue('IdentifyingNumber'))] = product

        for instance in instances:
            name = stringClean(instance.getPropertyValue('Name'))
            description = stringClean(instance.getPropertyValue('ElementName'))
            serial = None
            vendor = 'IBM'
            model = None
            sydId = stringClean(instance.getPropertyValue('Name'))
            if sydId in productMap:
                product = productMap[sydId]
                model = stringClean(product.getPropertyValue('Name'))
            osVersion = instance.getPropertyValue('CodeLevel') and stringClean(instance.getPropertyValue('CodeLevel'))

            status = instance.getPropertyValue('Status')

            identList = instance.getPropertyValue('IdentifyingDescriptions')
            identValue = instance.getPropertyValue('OtherIdentifyingInfo')
            ip = None
            try:
                ipIndex = identList.index('Ipv4 Address')
                ipAddr = identValue[ipIndex]
                if netutils.isValidIp(ipAddr):
                    ip = ipAddr
            except:
                logger.warn('Failed to get ip of storage system')

            hostObj = smis.Host(sydId, ip, name, sydId, description, [], [], model, serial, osVersion, vendor, status)
            result.append(hostObj)
        return result

class FcPortDiscoverer(smis_cimv2.FcPortDiscoverer):
    def __init__(self):
        smis_cimv2.FcPortDiscoverer.__init__(self)
        self.className = 'IBMTSSVC_FCPort' #we only care front end for remote connection

class LunMaskingMappingViewDiscover(smis_cimv2.LunMaskingMappingViewDiscover):
    def __init__(self):
        smis_cimv2.LunMaskingMappingViewDiscover.__init__(self)
        self.sapavaiableforelement_classname = 'IBMTSSVC_SAPAvailableForElement'

class PhysicalVolumeDiscoverer(smis_cimv2.PhysicalVolumeDiscoverer):
    def __init__(self):
        smis_cimv2.PhysicalVolumeDiscoverer.__init__(self)
        self.className = 'IBMTSSVC_BackendVolume'

class LogicalVolumeDiscoverer(smis_cimv2.LogicalVolumeDiscoverer):
    def __init__(self):
        smis_cimv2.LogicalVolumeDiscoverer.__init__(self)
        self.className = 'IBMTSSVC_StorageVolume'

class StoragePoolDiscoverer(smis_cimv2.StoragePoolDiscoverer):
    def __init__(self):
        smis_cimv2.StoragePoolDiscoverer.__init__(self)
        self.className = 'CIM_StoragePool'

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
        processorToArrayMap = {}
        processorToArrayMap = self.getParentRelationship(client)
        id = 0
        for instance in instances:
            #id = stringClean(instance.getPropertyValue('PoolID'))
            name = stringClean(instance.getPropertyValue('ElementName'))
            availSpace = stringClean(instance.getPropertyValue('TotalManagedSpace'))
            freeSpace = stringClean(instance.getPropertyValue('RemainingManagedSpace'))
            type = None
            instanceId = stringClean(instance.getPropertyValue('InstanceID')) or 0
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
        relationships = client.getInstances('CIM_HostedStoragePool')

        for relationship in relationships:
            parentRef = relationship.getPropertyValue('GroupComponent')
            childRef = relationship.getPropertyValue('PartComponent')
            if not parentRef or not childRef:
                continue
            parentId = stringClean(parentRef.getKeyValue('Name'))
            childId = stringClean(childRef.getKeyValue('InstanceID'))
            processorToArrayMap[childId] = parentId

        return processorToArrayMap