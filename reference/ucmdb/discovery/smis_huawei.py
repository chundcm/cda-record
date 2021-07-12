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
        self.__namespace = 'root/huawei'

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

        self.handlers.append(topologyFieldHanlder(topology.file_shares,
                                                  FileShareDiscover()))

        self.handlers.append(topologyFieldHanlder(topology.file_systems,
                                                  FileSystemDiscover()))

class FileShareDiscover(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'CIM_FileShare'

    def discover(self, client):
        instances = []
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        instances = client.getInstances(self.className)
        links = client.getInstances('CIM_SharedElement')
        return self.parse(instances, links)

    def parse(self, instances, links):
        shareTofsMap = {}
        shareTomanagedSystemId = {}
        for link in links:
            fsRef = link.getPropertyValue('SystemElement')
            shareRef = link.getPropertyValue('SameElement')

            fsId = fsRef and stringClean(fsRef.getKeyValue('Name'))
            fsSystemId = fsRef and stringClean(fsRef.getKeyValue('CSName'))
            shareId = shareRef and stringClean(shareRef.getKeyValue('InstanceID'))
            shareTofsMap[shareId] = fsId
            shareTomanagedSystemId[shareId] = fsSystemId

        result = []
        for instance in instances:
            name = stringClean(instance.getPropertyValue('Name'))
            instanceId = stringClean(instance.getPropertyValue('InstanceID'))
            managedSysName = shareTomanagedSystemId.get(instanceId)
            mountPoint = stringClean(instance.getPropertyValue('ElementName'))
            fsReference = shareTofsMap.get(instanceId)
            try:
                fsObj = smis.FileShare(mountPoint, instanceId, name, managedSysName, fsReference)
                result.append(fsObj)
            except:
                logger.debugException('')
        return result

class FileSystemDiscover(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'HuaSy_LocalFileSystem'

    def discover(self, client):
        instances = []
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        instances = client.getInstances(self.className)
        links = client.getInstances('HuaSy_ResidesOnExtent')
        return self.parse(instances, links)

    def parse(self, instances, links):
        fsTolvMap = {}
        for link in links:
            lvRef = link.getPropertyValue('Antecedent')
            fsRef = link.getPropertyValue('Dependent')


            lvId = stringClean(lvRef.getKeyValue('DeviceID'))
            lvSystemId = stringClean(lvRef.getKeyValue('SystemName'))
            fsId = stringClean(fsRef.getKeyValue('Name'))
            fsTolvMap[fsId] = lvSystemId + lvId

        result = []
        for instance in instances:
            name = stringClean(instance.getPropertyValue('Name'))
            mountPoint = stringClean(instance.getPropertyValue('Root'))
            managedSysName = stringClean(instance.getPropertyValue('CSName'))
            fileSystemSize = stringClean(instance.getPropertyValue('FileSystemSize'))
            freeSpaceSize = stringClean(instance.getPropertyValue('AvailableSpace'))
            status = smis_cimv2.getOperationalStatus(instance, property = 'OperationalStatus')
            lvReference = fsTolvMap.get(name)
            freeSpaceInMb = None
            sizeInMb = None
            try:
                sizeInMb = float(fileSystemSize) /(1024*1024)
            except:
                logger.debugException('')
                logger.debug('Failed to convert sizeInMb value')
            try:
                freeSpaceInMb = float(freeSpaceSize) /(1024*1024)
            except:
                logger.debugException('')
                logger.debug('Failed to convert freeSpaceInMb value')
            try:
                fsObj = smis.FileSystem(mountPoint,name, managedSysName, freeSpaceInMb, sizeInMb, status, lvReference)
                result.append(fsObj)
            except:
                logger.debugException('')
        return result

class StorageProcessorDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'HuaSy_StorageControllerSystem'

    def discover(self, client):
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        instances = []
        nodes = []
        controllers = []
        try:
            controllers = client.getInstances(self.className)
            nodes = client.getInstances('HuaSy_Node')
        except:
            pass
        instances.extend(nodes)
        instances.extend(controllers)
        return self.parse(instances, client)

    def parse(self, instances, client):
        processorToArrayMap = self.getParentRelationship(client)

        result = []
        for instance in instances:
            name = instance.getPropertyValue('Name')
            id = instance.getPropertyValue('ElementName')
            status = smis_cimv2.PROCESSOR_STATE_VALUE_MAP.get(stringClean(instance.getPropertyValue('HealthState')), 'Unknown')
            parent = processorToArrayMap.get(id)
            try:
                storage_processor = smis.StorageProcessor(id, name,status=status, parent=parent)
                result.append(storage_processor)
            except:
                logger.warnException('')

        return result

    def getParentRelationship(self, client):
        processorToArrayMap = {}
        relationships = client.getInstances('HuaSy_ComponentCS')

        for relationship in relationships:
            parentRef = relationship.getPropertyValue('GroupComponent')
            childRef = relationship.getPropertyValue('PartComponent')
            if not parentRef or not childRef:
                continue
            parentId = stringClean(parentRef.getKeyValue('Name'))
            childId = stringClean(parentRef.getKeyValue('Name'))
            processorToArrayMap[childId] = parentId

        return processorToArrayMap


class LunMaskingMappingViewDiscover(smis_cimv2.LunMaskingMappingViewDiscover):
    def __init__(self):
        smis_cimv2.LunMaskingMappingViewDiscover.__init__(self)

    def discover_controller_2_swhid(self, client):
        className = 'CIM_AssociatedInitiatorMaskingGroup'
        logger.debug('Queuing class "%s"' % className)
        controllerToIMGMap = {}
        associations =[]
        try:
            associations = client.getInstances(className)
        except:
            pass
        for link in associations:
            antecedent = link.getPropertyValue('Antecedent')
            dependent = link.getPropertyValue('Dependent')
            controllerId = stringClean(antecedent.getKeyValue('DeviceID'))
            controllerContainer = stringClean(antecedent.getKeyValue('SystemName'))
            imgId = stringClean(dependent.getKeyValue('InstanceID'))
            imgs = []
            if controllerToIMGMap.get(controllerContainer + controllerId):
                imgs = controllerToIMGMap.get(controllerContainer + controllerId)
            imgs.append(imgId)
            controllerToIMGMap[controllerContainer+controllerId] = imgs

        className = 'Nex_MemberOfCollection_IMG_SHWID'
        logger.debug('Queuing class "%s"' % className)
        imgToHWIDsMap = {}
        associations =[]
        try:
            associations = client.getInstances(className)
        except:
            pass
        for link in associations:
            collection = link.getPropertyValue('Collection')
            member = link.getPropertyValue('Member')
            imgId = stringClean(collection.getKeyValue('InstanceID'))
            hwId = stringClean(member.getKeyValue('InstanceID'))
            hwIds = []
            if imgToHWIDsMap.get(imgId):
                hwIds = imgToHWIDsMap.get(imgId)
            hwIds.append(hwId)
            imgToHWIDsMap[imgId] = hwIds

        for controller in controllerToIMGMap.keys():
            imgIds = controllerToIMGMap.get(controller)
            for img in imgIds:
                hwIds = imgToHWIDsMap.get(img)
                if hwIds:
                    for hwId in hwIds:
                        remoteSHIds = []
                        if self.SHIdObjMap.get(hwId):
                            if self.controllerToRemoteSHIdObjMap.get(controller):
                                remoteSHIds = self.controllerToRemoteSHIdObjMap.get(controller)
                            remoteSHIds.append(self.SHIdObjMap.get(hwId))
                        self.controllerToRemoteSHIdObjMap[controller] = remoteSHIds

class PhysicalVolume2StoragePoolDiscover(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        #self.className = 'HuaSy_AssociatedComponentExtent'
        self.className = 'HuaSy_ConcreteComponent'

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
                strName = str(phyRef.getObjectName())
                if strName.find('DiskExtent') >= 0:
                    pvId = stringClean(phyRef.getKeyValue('DeviceID'))
                    pvContainer = stringClean(phyRef.getKeyValue('SystemName'))
                    pvs.append(pvContainer+pvId)
                    result[poolId] = pvs
            except:
                logger.debugException('cannot find the physical volume to storage pool linkages')

        return result

class EndPointToVolumeDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)

    def discover(self, client):
        return []

class StorageSystemDiscoverer(smis_cimv2.StorageSystemDiscoverer):
    def __init__(self):
        smis_cimv2.StorageSystemDiscoverer.__init__(self)
        self.className = 'HuaSy_StorageSystem'

    def discover(self, client):
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        instances = []
        storages = []
        clusters = []
        try:
            storages = client.getInstances(self.className)
        except:
            pass
        try:
            clusters = client.getInstances('HuaSy_Cluster')
        except:
            pass

        instances.extend(storages)
        instances.extend(clusters)
        return self.parse(instances, client)

    def parse(self, instances, client):
        result = []
        chassisToArrayMap = self.getChassises(client)
        for instance in instances:
            name = stringClean(instance.getPropertyValue('Name'))
            description = stringClean(instance.getPropertyValue('ElementName'))
            chassis = chassisToArrayMap.get(name)
            serial = None
            vendor = None
            model = None
            if chassis:
                model = chassis.model
                serial = chassis.serialNumber
                vendor = chassis.manufacturer
            sydId = stringClean(instance.getPropertyValue('Name'))
            ip = None
            osVersion = None
            status = None
            hostObj = smis.Host(sydId, ip, name, sydId, description, [], [], model, serial, osVersion, vendor, status)
            result.append(hostObj)
        return result

    def getChassises(self, client):
        chassisToArrayMap = {}
        instances = client.getInstances('CIM_Chassis') #get all chassis for huawei two types of storage
        for instance in instances:
            parentRef = stringClean(instance.getPropertyValue('Tag'))
            manufacturer = stringClean(instance.getPropertyValue('Manufacturer'))
            serialNumber = stringClean(instance.getPropertyValue('SerialNumber'))
            model = stringClean(instance.getPropertyValue('Model'))
            chassis = smis.Chassis(parentRef,manufacturer,model,serialNumber)
            chassisToArrayMap[parentRef] = chassis
        return chassisToArrayMap

class FcPortDiscoverer(smis_cimv2.FcPortDiscoverer):
    def __init__(self):
        smis_cimv2.FcPortDiscoverer.__init__(self)
        self.className = 'HuaSy_FrontEndFCPort' #we only care front end for remote connection

class PhysicalVolumeDiscoverer(smis_cimv2.PhysicalVolumeDiscoverer):
    def __init__(self):
        smis_cimv2.PhysicalVolumeDiscoverer.__init__(self)
        self.className = 'HuaSy_DiskExtent'

class LogicalVolumeDiscoverer(smis_cimv2.LogicalVolumeDiscoverer):
    def __init__(self):
        smis_cimv2.LogicalVolumeDiscoverer.__init__(self)
        self.className = 'Nex_StorageVolume'

    def discover(self, client):
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)

        instances = []
        volumes = []
        disks = []
        try:
            volumes = client.getInstances(self.className)
        except:
            pass
        try:
            disks = client.getInstances('HuaSy_LogicalDisk')
        except:
            pass

        instances.extend(volumes)
        instances.extend(disks)
        return self.parse(instances)

    def parse(self, instances):
        result = []
        for instance in instances:
            name = stringClean(instance.getPropertyValue('Name'))
            managedSysName = stringClean(instance.getPropertyValue('SystemName'))
            blockSize = stringClean(instance.getPropertyValue('BlockSize'))
            blocksNumber = stringClean(instance.getPropertyValue('NumberOfBlocks'))
            blocksConsumable = stringClean(instance.getPropertyValue('ConsumableBlocks'))
            objectId = stringClean(instance.getPropertyValue('DeviceID'))
            status = smis_cimv2.getOperationalStatus(instance, property = 'OperationalStatus')
            poolId = None

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

class RemoteEndpointDiscoverer(smis_cimv2.RemoteEndpointDiscoverer):
    def __init__(self):
        smis_cimv2.RemoteEndpointDiscoverer.__init__(self)
        self.className = 'HuaSy_StorageHardwareID'

class StoragePoolDiscoverer(smis_cimv2.StoragePoolDiscoverer):
    def __init__(self):
        smis_cimv2.StoragePoolDiscoverer.__init__(self)
        self.className = 'CIM_StoragePool'
        self.linkClass = 'HuaSy_AllocatedFromStoragePool'

    def discover(self, client):
        links = None
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        instances = client.getInstances(self.className)
        if self.linkClass:
            links = client.getInstances(self.linkClass)
        return self.parse(client, instances, links)

    def parse(self, client, instances, links):
        result = []
        poolToLvIds = {}
        childPoolMap = {}
        smis_cimv2.discoverPoolLinks(client, childPoolMap, poolToLvIds,'HuaSy_AllocatedFromStoragePool' )

        for instance in instances:
            name = stringClean(instance.getPropertyValue('ElementName'))
            availSpace = stringClean(instance.getPropertyValue('TotalManagedSpace'))
            freeSpace = stringClean(instance.getPropertyValue('RemainingManagedSpace'))
            type = None
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
