#coding=utf-8
import types
import re
import smis
import logger
import fptools
import cim
import cim_discover
import netutils

PROCESSOR_STATE_VALUE_MAP = {'0' : 'Unknown',
                        '5' : 'OK',
                        '10' : 'Degraded',
                        '15' : 'Minor failure',
                        '20' : 'Major failure',
                        '25' : 'Critical failure',
                        '30' : 'Non-recoverable error'
                       }

PORT_STATE_UNKNOWN = 'Unknown'
PORT_TYPE_RESERVED = 'Vendor Reserved'
PORT_STATE_VALUE_MAP = {'0' : 'Unknown',
                            '5' : 'OK',
                            '10' : 'Degraded',
                            '15' : 'Minor failure',
                            '20' : 'Major failure',
                            '25' : 'Critical failure',
                            '30' : 'Non-recoverable error'
                            }

PORT_STATUS_VALUE_MAP = { '0' :'Unknown',
                             '1' : 'Other',
                             '2' : 'OK',
                             '3' : 'Degraded',
                             '4' : 'Stressed',
                             '5' : 'Predictive Failure',
                             '6' : 'Error',
                             '7' : 'Non-Recoverable Error,',
                             '8' : 'Starting',
                             '9' : 'Stopping',
                             '10' : 'Stopped',
                             '11' : 'In Service',
                             '12' : 'No Contact',
                             '13' : 'Lost Communication',
                             '14' : 'Aborted',
                             '15' : 'Dormant',
                             '16' : 'Supporting Entity in Error',
                             '17' : 'Completed',
                             '18' : 'Power Mode'
                             }

PORT_TYPE_VALUE_MAP = {  '0' :'Unknown',
                             '1' : 'Other',
                             '10' : 'N',
                             '11' : 'NL',
                             '12' : 'F/NLd',
                             '13' : 'Nx',
                             '14' : 'E',
                             '15' : 'F',
                             '16' : 'FL',
                             '17' : 'B',
                             '18' : 'G'
                            }

def stringClean(value):
    '''
    Transforms a value to a string and strips out space and " symbols from it
    @param value: string convertable value
    '''
    if value is not None:
        return str(value).strip(' "\\')
    else:
        return None

def getOperationalStatus(instance, property = 'OperationalStatus' ):
    STATE_UNKNOWN = 'Unknown'
    STATUS_VALUE_MAP = { '0' : 'Unknown',
                         '1' : 'Other',
                         '2' : 'OK',
                         '3' : 'Degraded',
                         '4' : 'Stressed',
                         '5' : 'Predictive Failure',
                         '6' : 'Error',
                         '7' : 'Non-Recoverable Error,',
                         '8' : 'Starting',
                         '9' : 'Stopping',
                         '10' : 'Stopped',
                         '11' : 'In Service',
                         '12' : 'No Contact',
                         '13' : 'Lost Communication',
                         '14' : 'Aborted',
                         '15' : 'Dormant',
                         '16' : 'Supporting Entity in Error',
                         '17' : 'Completed',
                         '18' : 'Power Mode',
                         '19' : 'Relocating',
                         '32769' : 'ONLINE'
                       }
    statusValueList = []
    if instance:
        statusList = instance.getPropertyValue(property)
        for s in statusList:
            statusValueList.append(STATUS_VALUE_MAP.get(str(s), STATE_UNKNOWN))

    return ",".join(statusValueList)

def discoverPoolLinks(client, childPoolMap=[], poolToLvIds=[],linkClassName='CIM_AllocatedFromStoragePool'):
    links = []
    try:
        links = client.getInstances(linkClassName)
    except:
        pass

    for link in links:
        childPoolIds = []
        antecedent = link.getPropertyValue('Antecedent')
        dependent = link.getPropertyValue('Dependent')
        usedPool = stringClean(antecedent.getKeyValue('InstanceID'))
        usedLv = None
        strName = str(dependent.getObjectName())
        if strName.find('StorageVolume') >= 0 or strName.find('LogicalDisk') >= 0 \
                or strName.find('SnapshotVolume') >= 0:
            usedLv =  stringClean(dependent.getKeyValue('DeviceID'))
            if usedPool and usedLv:
                lvIds = poolToLvIds.get(usedPool, [])
                usedLv and lvIds.append(usedLv)
                poolToLvIds[usedPool] = lvIds
        else:
            mappedPoolIds = childPoolMap.get(usedPool)
            if mappedPoolIds:
                childPoolIds = mappedPoolIds

            childPoolId = stringClean(dependent.getKeyValue('InstanceID'))
            childPoolIds.append(childPoolId)
            childPoolMap[usedPool] = childPoolIds

def discoverControllerToRemoteWwnsLinks(client, SHIdObjMap):
    controllerToRemoteSHIdObjMap = {}
    apIdToAuthorizedPrivilegeMap = {}
    className = 'CIM_AuthorizedTarget'
    logger.debug('Queuing class "%s"' % className)
    links = []
    try:
        links = client.getInstances(className)
    except:
        pass
    for link in links:
        privilege = link.getPropertyValue('Privilege')
        target = link.getPropertyValue('TargetElement')
        controllerId = stringClean(target.getKeyValue('DeviceID'))
        controllerContainer = stringClean(target.getKeyValue('SystemName'))
        apId = stringClean(privilege.getKeyValue('InstanceID'))
        ap = smis.AuthorizedPrivilege(apId)
        if apIdToAuthorizedPrivilegeMap.get(apId):
                ap = apIdToAuthorizedPrivilegeMap.get(apId)
        ap.controllerId = controllerContainer+controllerId
        apIdToAuthorizedPrivilegeMap[apId] = ap

    className = 'CIM_AuthorizedSubject'
    logger.debug('Queuing class "%s"' % className)
    links = []
    try:
        links = client.getInstances(className)
    except:
        pass
    for link in links:
        privilege = link.getPropertyValue('Privilege')
        storageHardware = link.getPropertyValue('PrivilegedElement')
        shwId = stringClean(storageHardware.getKeyValue('InstanceID'))
        apId = stringClean(privilege.getKeyValue('InstanceID'))
        if apIdToAuthorizedPrivilegeMap.get(apId):
            ap = apIdToAuthorizedPrivilegeMap.get(apId)
            ap.storageHwId = shwId
            apIdToAuthorizedPrivilegeMap[apId] = ap

    for apId in apIdToAuthorizedPrivilegeMap.keys():
        shIdObjs = []
        ap = apIdToAuthorizedPrivilegeMap.get(apId)
        if ap.controllerId and ap.storageHwId:
            if controllerToRemoteSHIdObjMap.get(ap.controllerId):
                shIdObjs = controllerToRemoteSHIdObjMap.get(ap.controllerId)
            if SHIdObjMap.get(ap.storageHwId):
                shIdObj = SHIdObjMap.get(ap.storageHwId)
                shIdObjs.append(shIdObj)
            controllerToRemoteSHIdObjMap[ap.controllerId] = shIdObjs

    return controllerToRemoteSHIdObjMap

def getMappingFromInstances(client, className, keyProper, valueProper):
    map = {}
    instances = []
    logger.debug('Queuing class "%s"' % className)
    try:
        instances = client.getInstances(className)
    except:
        pass
    for instance in instances:
        key = instance.getPropertyValue(keyProper)
        val = instance.getPropertyValue(valueProper)
        if key:
            map[key] = val
    return map;

def getMapping2FromInstances(client, className, keyProper, valueProper):
    map = {}
    instances = []
    logger.debug('Queuing class "%s"' % className)
    try:
        instances = client.getInstances(className)
    except:
        pass
    for instance in instances:
        key = instance.getPropertyValue(keyProper)
        val = instance.getPropertyValue(valueProper)
        vals = []
        if key:
            if map.get(key):
                vals = map.get(key)
            vals.append(val)
            map[key] = vals
    return map;

def getMappingFromLinks(client, className, antName, depName, keyProper, valueProper):
    map = {}
    links = []
    logger.debug('Queuing class "%s"' % className)
    try:
        links = client.getInstances(className)
    except:
        pass
    for link in links:
        antecedent = link.getPropertyValue(antName)
        dependent = link.getPropertyValue(depName)
        key = antecedent and  stringClean(antecedent.getKeyValue(keyProper))
        val = dependent and  stringClean(dependent.getKeyValue(valueProper))
        if key:
            map[key] = val
    return map;

def getMapping2FromLinks(client, className, keyProper, valueProper,
                         antName = 'Antecedent',
                         depName = 'Dependent'):
    map = {}
    links = []
    logger.debug('Queuing class "%s"' % className)
    try:
        links = client.getInstances(className)
    except:
        pass
    for link in links:
        antecedent = link.getPropertyValue(antName)
        dependent = link.getPropertyValue(depName)
        key = antecedent and stringClean(antecedent.getKeyValue(keyProper))
        val = dependent and stringClean(dependent.getKeyValue(valueProper))
        vals = []
        if key:
            if map.get(key):
                vals = map.get(key)
            vals.append(val)
            map[key] = vals
    return map;

class topologyFieldHanlder:
    def __init__(self, field, discoverer):
        self.field = field
        self.discoverer = discoverer
    def __call__(self, client):
        if type(self.field) is list:
            self.field.extend(self.discoverer.discover(client))
        elif type(self.field) is dict:
            self.field.update(self.discoverer.discover(client))
        else:
            logger.debugException('Wrong field type:%s', type(self.field))

class BaseNamespace:
    def __init__(self):
        self.__namespace = ''
        self.handlers = []
        self.fallback_handlers = {}

    def associateTopologyObj2Discoverers(self, topology):
        pass

    def discover(self,client):
        for handler in self.handlers:
            if(str(handler.discoverer.__class__).find("smis_brocade")==-1):
                try:
                    handler(client)
                except:
                    logger.debugException('')
                if not handler.field:
                    logger.debug('Failed to get data. Will try fallback if defined for class %s' % handler.discoverer.__class__)
                    fallback_handler = self.fallback_handlers.get(handler.discoverer.__class__)
                    fallback_handler and fallback_handler(client)
            else:
                handler(client)

class Namespace(BaseNamespace):
    def __init__(self):
        BaseNamespace.__init__(self)
        self.__namespace = 'root/cimv2'


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

        self.handlers.append(topologyFieldHanlder(topology.hosts,
                                                  HostDicoverer()))

        self.handlers.append(topologyFieldHanlder(topology.logical_volumes,
                                                  LogicalVolumeDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.remote_endpoints,
                                                  RemoteEndpointDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.storage_pools,
                                                  StoragePoolDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.physcial_volumes_2_pool_links,
                                                  PhysicalVolume2StoragePoolDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.file_shares,
                                                  FileShareDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.file_systems,
                                                  FileSystemDiscoverer()))


class BaseSmisDiscoverer:
    '''
    Basic Discoverer class from which all specific discoverers should derive.
    '''
    def __init__(self):
        self.className = None

    def parse(self, instances):
        raise NotImplementedError('')

    def discover(self, client):
        instances = []
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        instances = client.getInstances(self.className)
        return self.parse(instances)

class StorageProcessorDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)

    def discover(self, client):
        return []

class EndPointToVolumeDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)

    def discover(self, client):
        return []

class PhysicalVolume2StoragePoolDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)

    def discover(self, client):
        return []

class FileShareDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)

    def discover(self, client):
        return {}

class FileSystemDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)

    def discover(self, client):
        return []

class LunMaskingMappingViewDiscover(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.controllerforunit_classname = 'CIM_ProtocolControllerForUnit'
        self.sapavaiableforelement_classname = 'CIM_SAPAvailableForElement'

        self.controllerLunIdToLvMap = {}
        self.controllerToLUNIdsMap = {}
        self.SHIdObjMap = {}
        self.controllerToRemoteSHIdObjMap = {}
        self.controllerToLocalWwnMap = {}
        self.lunIdToLunObjMap = {}

    def discover_controller_for_unit(self, client):
        links = []
        className = self.controllerforunit_classname
        logger.debug('Queuing class "%s"' % className)
        try:
            links = client.getInstances(className)
        except:
            pass
        for link in links:
            antecedent = link.getPropertyValue('Antecedent')
            dependent = link.getPropertyValue('Dependent')
            scsiController = stringClean(antecedent.getKeyValue('DeviceID'))
            scsiControllerContainer = stringClean(antecedent.getKeyValue('SystemName'))

            lunId = stringClean(link.getPropertyValue('DeviceNumber'))
            if lunId is None:
                continue
            lun_id = str(int(lunId, 16))
            lvId = stringClean(dependent.getKeyValue('DeviceID'))
            lvContainer = stringClean(dependent.getKeyValue('SystemName'))
            lvRef = smis.LogicalVolumeId(lvContainer, lvId)
            lvRefs = []
            lunSeparator = 'LUN'
            if self.controllerLunIdToLvMap.get(scsiControllerContainer + scsiController + lunSeparator + lun_id):
                lvRefs = self.controllerLunIdToLvMap.get(scsiControllerContainer + scsiController + lunSeparator + lun_id)
            lvRefs.append(lvRef)
            self.controllerLunIdToLvMap[scsiControllerContainer + scsiController + lunSeparator + lun_id] = lvRefs
            lunIds = []
            if self.controllerToLUNIdsMap.get(scsiControllerContainer + scsiController):
                lunIds = self.controllerToLUNIdsMap.get(scsiControllerContainer + scsiController)
            lunIds.append(lun_id)
            self.controllerToLUNIdsMap[scsiControllerContainer + scsiController] = lunIds

    def discover_storage_hardward_id(self, client):
        className = 'CIM_StorageHardwareID'
        logger.debug('Queuing class "%s"' % className)
        instances = []
        try:
            instances = client.getInstances(className)
        except:
            pass
        for instance in instances:
            instanceId = stringClean(instance.getPropertyValue('InstanceID'))
            id = stringClean(instance.getPropertyValue('StorageID'))
            type = stringClean(instance.getPropertyValue('IDType'))
            shIdObj = smis.StorageHardwareId( id, type)
            self.SHIdObjMap[instanceId] = shIdObj

    def discover_controller_2_swhid(self, client):
        className = 'CIM_AssociatedPrivilege'
        logger.debug('Queuing class "%s"' % className)
        associations =[]
        try:
            associations = client.getInstances(className)
        except:
            pass
        for link in associations:
            subject = link.getPropertyValue('Subject')
            target = link.getPropertyValue('Target')
            hwId = stringClean(subject.getKeyValue('InstanceID'))
            controllerId = stringClean(target.getKeyValue('DeviceID'))
            controllerContainer = stringClean(target.getKeyValue('SystemName'))
            remoteSHIds = []
            if self.SHIdObjMap.get(hwId):
                if self.controllerToRemoteSHIdObjMap.get(controllerContainer+controllerId):
                    remoteSHIds = self.controllerToRemoteSHIdObjMap.get(controllerContainer+controllerId)
                remoteSHIds.append(self.SHIdObjMap.get(hwId))
            self.controllerToRemoteSHIdObjMap[controllerContainer+controllerId] = remoteSHIds

        if len(self.controllerToRemoteSHIdObjMap) == 0:
            self.controllerToRemoteSHIdObjMap = discoverControllerToRemoteWwnsLinks(client, self.SHIdObjMap)

    def discover_controller_2_local_wwn(self, client):
        className = self.sapavaiableforelement_classname
        logger.debug('Queuing class "%s"' % className)

        linkages = []
        try:
            linkages = client.getInstances(className)
        except:
            pass
        for link in linkages:
            localWwns = []
            antecedent = link.getPropertyValue('ManagedElement')
            dependent = link.getPropertyValue('AvailableSAP')
            controllerId = stringClean(antecedent.getKeyValue('DeviceID'))
            controllerContainer = stringClean(antecedent.getKeyValue('SystemName'))
            localWwn = stringClean(dependent.getKeyValue('Name'))
            if controllerId and localWwn and localWwn != "":
                if self.controllerToLocalWwnMap.get(controllerContainer+controllerId):
                    localWwns = self.controllerToLocalWwnMap.get(controllerContainer+controllerId)
                localWwns.append(localWwn)
                self.controllerToLocalWwnMap[controllerContainer+controllerId] = localWwns

    def discover_from_mappingview(self, client):
        result = []
        return result

    def discover(self, client):
        result = self.discover_from_mappingview(client)
        if len(result):
            return result
        self.discover_controller_for_unit(client)
        self.discover_storage_hardward_id(client)
        self.discover_controller_2_swhid(client)
        self.discover_controller_2_local_wwn(client)
        return self.parse()

    def parse(self):
        result = []
        for controller in self.controllerToRemoteSHIdObjMap.keys():
            remoteSHObjs = self.controllerToRemoteSHIdObjMap.get(controller)
            localWwns = self.controllerToLocalWwnMap.get(controller)
            luns = self.controllerToLUNIdsMap.get(controller)
            lunSeparator = 'LUN'
            for shObj in remoteSHObjs:
                if luns:
                    for lunid in luns:
                        lvs = self.controllerLunIdToLvMap.get(controller + lunSeparator + lunid)
                        lun = smis.LUN(lunid)
                        mappingView = smis.LunMaskingMappingView(lvs, shObj, localWwns, lun)
                        result.append(mappingView)
        return result

class StorageSystemDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'CIM_StorageSystem'

    def parse(self, instances):
        result = []
        for instance in instances:
            name = stringClean(instance.getPropertyValue('ElementName'))
            description = stringClean(instance.getPropertyValue('Description'))
            m = re.search('Serial number:\s*(\w+)', description)
            serial = m and m.group(1)
            m = re.search('OS version:([\d\.]+)', description)
            osVersion = m and m.group(1)
            m = re.search('(.+?), ID', description)
            model = m and m.group(1)

            identList = instance.getPropertyValue('IdentifyingDescriptions')
            identValue = instance.getPropertyValue('OtherIdentifyingInfo')
            sydId = stringClean(instance.getPropertyValue('Name'))
            ip = None
            hostWwn = None
            try:
                ipIndex = identList.index('Ipv4 Address')
                ip = identValue[ipIndex]
            except:
                logger.warn('Failed to get ip of storage system')

            try:
                hostWwnIndex = identList.index('Node WWN')
                hostWwn = identValue[hostWwnIndex]
            except:
                logger.warn('Failed to get wwn of storage system')


            hostObj = smis.Host(sydId, ip, name, sydId, description, [], [], model, serial, osVersion)
            result.append(hostObj)

        return result

class FcPortDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'CIM_FCPort'

    def parse(self, instances):
        result = []
        for instance in instances:
            portId = stringClean(instance.getPropertyValue('PortNumber'))
            portName = stringClean(instance.getPropertyValue('ElementName'))
            portWwn  = instance.getPropertyValue('PermanentAddress')
            deviceId = stringClean(instance.getPropertyValue('DeviceID'))
            if deviceId and portWwn is None:
                portWwn = deviceId
            portIndex = stringClean(instance.getPropertyValue('PortNumber'))
            portStatus = instance.getPropertyValue('StatusDescriptions') and ",".join(instance.getPropertyValue('StatusDescriptions'))
            if not portStatus:
                portStatus = getOperationalStatus(instance)
            portState = PORT_STATE_VALUE_MAP.get(stringClean(instance.getPropertyValue('HealthState')), PORT_STATE_UNKNOWN)
            speedBps = stringClean(instance.getPropertyValue('Speed'))
            maxSpeedBps = instance.getPropertyValue('MaxSpeed')
            portType =PORT_TYPE_VALUE_MAP.get(stringClean(instance.getPropertyValue('PortType')), PORT_TYPE_RESERVED)
            referencedTo = instance.getPropertyValue('ConnectedTo')
            container = stringClean(instance.getPropertyValue('SystemName'))

            try:
                fcpObj = smis.FcPort(portId, portIndex, portWwn, portName, container, referencedTo, portStatus, portState, speedBps, container, maxSpeedBps, portType)
                result.append(fcpObj)
            except:
                logger.debugException('')
        return result

class PhysicalVolumeDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'CIM_DiskExtent'

    def parse(self, instances):
        result = []
        for instance in instances:
            name = stringClean(instance.getPropertyValue('DeviceID'))
            managedSysName = stringClean(instance.getPropertyValue('SystemName'))
            blockSize = stringClean(instance.getPropertyValue('BlockSize'))
            blocksNumber = stringClean(instance.getPropertyValue('NumberOfBlocks'))
            objectId = stringClean(instance.getPropertyValue('DeviceID'))

            humanReadableName = stringClean(instance.getPropertyValue('ElementName')) or ''

            sizeInMb = None
            try:
                sizeInMb = float(blocksNumber) * int(blockSize)/ (1024 * 1024)
            except:
                logger.debugException('')
                logger.debug('Failed to convert sizeInMb value')

            try:
                pvObj = smis.PhysicalVolume(name, managedSysName, objectId, sizeInMb, humanReadableName)
                result.append(pvObj)
            except:
                logger.debugException('')

        return result

class LogicalVolumeDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'CIM_StorageVolume'

    def parse(self, instances):
        result = []
        for instance in instances:
            name = stringClean(instance.getPropertyValue('Name'))
            managedSysName = stringClean(instance.getPropertyValue('SystemName'))
            objectId = stringClean(instance.getPropertyValue('DeviceID'))
            blockSize = stringClean(instance.getPropertyValue('BlockSize'))
            blocksNumber = stringClean(instance.getPropertyValue('NumberOfBlocks'))
            blocksConsumable = stringClean(instance.getPropertyValue('ConsumableBlocks'))
            humanReadableName = stringClean(instance.getPropertyValue('ElementName'))
            freeSpaceInMb = None
            sizeInMb = None
            try:
                sizeInMb = float(blocksNumber) * int(blockSize)/(1024 * 1024)
            except:
                logger.debugException('')
                logger.debug('Failed to convert sizeInMb value')
            try:
                freeSpaceInMb = float(blocksConsumable) * int(blockSize)/(1024 * 1024)
            except:
                logger.debugException('')
                logger.debug('Failed to convert freeSpaceInMb value')
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

    def parse(self, instances):
        result = []
        for instance in instances:
            name = stringClean(instance.getPropertyValue('ElementName'))
            hostIp = instance.getPropertyValue('IPAddress')
            deviceId = stringClean(instance.getPropertyValue('StorageID'))

            hostName = None
            # skipp the node if name is wwn
            if name.upper() != deviceId.upper():
                hostName = name

            portIndex = None
            try:
                endPoint = smis.RemoteEndPoint(wwn=deviceId, name=hostName, portIndex=portIndex, hostIp=hostIp)
                result.append(endPoint)
            except:
                pass
        return result

class HostDicoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'CIM_NodeSystem'

    def parse(self, instances):
        result = []
        for instance in instances:
            name = stringClean(instance.getPropertyValue('Name'))
            logger.debug('name %s' % name)
        return result

class StoragePoolDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'CIM_StoragePool'

    def parse(self, instances):
        result = []
        for instance in instances:
            name = stringClean(instance.getPropertyValue('Name'))
            availSpace = stringClean(instance.getPropertyValue('TotalManagedSpace'))
            freeSpace = stringClean(instance.getPropertyValue('RemainingManagedSpace'))
            type = stringClean(instance.getPropertyValue('ResourceType'))
            id = stringClean(instance.getPropertyValue('DiskDeviceType')) or 0
            try:
                pool = smis.StoragePool(name = name, parentReference = None, id = id, type = type, availableSpaceInMb = freeSpace, totalSpaceInMb = availSpace,\
                     unExportedSpaceInMb = freeSpace, dataRedund = None, lvmIds = None)
                result.append(pool)
            except:
                logger.debugException('')
        return result