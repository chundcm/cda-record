# coding=utf-8
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector
import wwn

import entity
import modeling
import netutils
import logger


class CimCategory:
    SMIS = "Storage"


def convertOptional(value, fn, message):
    '''
    Converts passed value by calling a function fn, in case fn fails message is written to log
    @param value: acceptable value for fn
    @param fn: callable
    @param message: string
    '''
    try:
        if value is not None:
            return fn(value)
    except:
        logger.warn(message)


class FcPort(entity.Immutable):
    '''
    DO represents Fibre Channel Adapter Port.
    '''

    def __init__(self, id, index, wwn=None, name=None, parentReference=None, remotePeer=None, \
                 status=None, state=None, speedMbps=None, storageProcessorId=None, maxSpeedMbps=None, portType=None):
        '''
        @param id: int required
        @param index: int required
        @param wwn: string
        @param name: string
        @param parentReference: id/name/ or whatever of the Node to which the fcPort belongs to
        @param remotePeer: str
        @param status: string
        @param state: string
        @param speedGbps: float
        @raise ValueError: in case id or index is not set
        '''
        self.id = id and convertOptional(id, int, 'Failed to convert FCPort speed value "%s" to int' % id)
        self.index = index and convertOptional(index, int, 'Failed to convert FCPort speed value "%s" to int' % index)
        self.wwn = wwn

        if not (self.id or self.index or self.wwn):
            raise ValueError("Id or index or port wwn must be set")

        self.name = name
        self.parentReference = parentReference
        self.remotePeer = remotePeer
        self.status = status
        self.state = state
        convertedSpeed = convertOptional(speedMbps, float,
                                         'Failed to convert FCPort speed value "%s" to float' % speedMbps)
        self.speedGbps = convertedSpeed and convertedSpeed / 1073741824  # Gbits
        convertedSpeed = convertOptional(maxSpeedMbps, float,
                                         'Failed to convert FCPort max speed value "%s" to float' % maxSpeedMbps)
        self.maxSpeedGbps = convertedSpeed and convertedSpeed / 1073741824  # Gbits
        self.storageProcessorId = storageProcessorId
        self.portType = portType

    def __repr__(self):
        return "PcPort(id='%s', index=%s, wwm='%s', name='%s', parentReference='%s', remotePeer='%s', status='%s', state='%s', speedGbps=%s, storageProcessorId = '%s')" %\
            (self.id, self.index, self.wwn, self.name, self.parentReference, self.remotePeer, self.status, self.state, self.speedGbps, self.storageProcessorId)

    def __str__(self):
        return self.__repr__()

class StorageFabric(entity.Immutable):
    '''
    DO represents logical volume identity with contatiner+id.
    '''
    def __init__(self, name, wwn):
        self.name = name
        self.wwn = wwn

    def __repr__(self):
        return "StorageFabric(name='%s', wwn='%s')" % (self.name, self.wwn)

    def __str__(self):
        return self.__repr__()

class FCSwtich(entity.Immutable):
    '''
    DO represents logical volume identity with contatiner+id.
    '''
    def __init__(self, name, wwn, roles=None, domainId=None, type=None, vfId=None):
        self.name = name
        self.wwn = wwn
        self.domainId = domainId
        self.roles = roles
        self.type = type
        self.vfId = vfId

    def __repr__(self):
        return "FCSwtich(name='%s', wwn='%s', roles='%s', domainId='%s', type='%s', vfId='%s')" \
               % (self.name, self.wwn, self.roles, self.domainId, self.type, self.vfId)

    def __str__(self):
        return self.__repr__()


class FileShare(entity.Immutable):
    '''
    DO represents Fibre Channel Host Adapter
    '''
    def __init__(self, path, instanceId, name = None, parentReference = None, fsReference = None):
        '''
        @param path: string
        @param name: string
        @param parentReference: string
        '''
        if not path:
            raise ValueError("path attributes can not be None")
        self.instanceId = instanceId
        self.path = path
        self.name = name
        self.parentReference = parentReference
        self.fsReference = fsReference

class FileShareBuilder:
    '''
    File Share builder
    '''
    def build(self, file):
        '''
        @param file: instance of FileShare
        @raise ValueError: file is not set
        @return: file export Osh
        '''
        if not file:
            raise ValueError("file is not specified")
        fileExportOsh = ObjectStateHolder("file_system_export")
        fileExportOsh.setStringAttribute('file_system_path', file.path)
        fileExportOsh.setStringAttribute('name', file.name)
        return fileExportOsh

class FileShareReporter:
    '''
    File Share reporter
    '''
    def __init__(self, builder):
        '''
        @param builder: instance of FileShareBuilder
        @raise ValueError: builder is not set
        '''
        if not builder:
            raise ValueError('Builder is not passed')
        self.builder = builder

    def report(self, file, containerOsh):
        '''
        @param file: instance of file share DO
        @param container: corresponding container osh
        @raise ValueError: container is not set
        @return: tuple(file export Osh, OSHV)
        '''
        if not containerOsh:
            raise ValueError('Container for file is not specified')
        vector = ObjectStateHolderVector()
        fileExportOsh = self.builder.build(file)
        fileExportOsh.setContainer(containerOsh)
        vector.add(fileExportOsh)
        return (fileExportOsh, vector)


class FileSystem(entity.Immutable):
    '''
    DO File System
    '''
    def __init__(self, mountPoint, name = None, parentReference = None, freeSpaceInMb = None,
                 sizeInMb = None, status = None, lvReference = None):
        '''
        @param name: mountPoint
        @param name: string
        @param parentReference: string
        @param freeSpaceInMb: float
        @param sizeInMb: float
        @raise ValueError: in case name is not set or empty
        '''
        if not mountPoint:
            raise ValueError("path attributes can not be None")
        self.mountPoint = mountPoint
        self.name = name
        self.sizeInMb = sizeInMb
        self.parentReference = parentReference
        self.freeSpaceInMb = convertOptional(freeSpaceInMb, float,
                                             'Failed to convert freeSpaceInMb value "%s" to float' % freeSpaceInMb)
        self.status = status
        self.lvReference = lvReference

class FileSystemBuilder:
    '''
    File System builder
    '''
    def build(self, file):
        '''
        @param file: instance of FileShare
        @raise ValueError: file is not set
        @return: file export Osh
        '''
        if not file:
            raise ValueError("file system is not specified")

        fileSystemOsh = ObjectStateHolder("file_system")
        fileSystemOsh.setStringAttribute("mount_point", file.mountPoint)
        if file.freeSpaceInMb is not None:
            fileSystemOsh.setAttribute("free_space", file.freeSpaceInMb)
        if file.sizeInMb is not None:
            fileSystemOsh.setAttribute("disk_size", file.sizeInMb)
        if file.name is not None:
            fileSystemOsh.setStringAttribute('name', file.name)

        return fileSystemOsh

class FileSystemReporter:
    '''
    File System reporter
    '''
    def __init__(self, builder):
        '''
        @param builder: instance of FileSystemBuilder
        @raise ValueError: builder is not set
        '''
        if not builder:
            raise ValueError('Builder is not passed')
        self.builder = builder

    def report(self, file, containerOsh):
        '''
        @param file: instance of file system DO
        @param container: corresponding container osh
        @raise ValueError: container is not set
        @return: tuple(file export Osh, OSHV)
        '''
        if not containerOsh:
            raise ValueError('Container for file is not specified')
        vector = ObjectStateHolderVector()
        fileSystemOsh = self.builder.build(file)
        fileSystemOsh.setContainer(containerOsh)
        vector.add(fileSystemOsh)
        return (fileSystemOsh, vector)


class FcHba(entity.Immutable):
    '''
    DO represents Fibre Channel Host Adapter
    '''
    def __init__(self, name, wwn, parentReference = None, targetWwn = None, localPorts = None):
        '''
        @param name: string
        @param wwn: string
        @param parentReference: string
        @param targetWwn: string
        @param localPorts: list of ports
        @raise ValueError: in case name or wwn is not set
        '''
        if not (name and wwn):
            raise ValueError("Name and wwn attributes can not be None")
        self.wwn = wwn
        self.name = name
        self.parentReference = parentReference
        self.targetWwn = targetWwn
        self.localPorts = localPorts or []

class Host(entity.Immutable):
    '''
    DO represents Node of Storage System or Node to where the Lun/LV is exported to.
    '''
    def __init__(self, id, ip = None, name = None, systemObjId = None, descr = None, localHba = None, \
                 localPorts = None, model = None, serial = None, version = None, vendor = None, status = None):
        '''
        @param id: string
        @param ip: string
        @param name: string
        @param systemObjId: id of the object in Storage System
        @param description: string
        @param localHba: list of host base adapters
        @param localPorts: list of fcPorts
        @raise ValueError: in case ip is invalid and no name, or no name and ip
        '''
        self.ip = None
        try:
            if netutils.isValidIp(ip):
                self.ip = ip
        except:
            logger.warn('IP "%s" for node name "%s" is invalid' % (ip, name))

        if not id and not (name or self.ip):
            raise ValueError("Name or ip attribute must be set along with id attribute")
        self.name = name
        self.id = id
        self.localHba = localHba or []
        self.localPorts = localPorts or []
        self.systemObjId = systemObjId
        self.description = descr
        self.model = model
        self.serial = serial
        self.version = version
        self.vendor = vendor
        self.status = status

    def __repr__(self):
        return 'Host(id="%s", ip="%s" name="%s", systemObjId="%s", description="%s")' % (
        self.id, self.ip, self.name, self.systemObjId, self.description)

    def __str__(self):
        return self.__repr__()

class StoragePool(entity.Immutable):
    '''
    DO represents Storage Pool
    '''
    def __init__(self, name, parentReference = None, id = None, type = None, availableSpaceInMb = None, totalSpaceInMb = None,\
                 unExportedSpaceInMb = None, dataRedund = None, lvmIds = None, cimId = None, childPoolIds = None):
        '''
        @param name: string
        @param parentReference:  string
        @param id: string
        @param type: string
        @param availableSpaceInMb: float
        @param totalSpaceInMb: float
        @param unExportedSpaceInMb: float
        @param dataRedund: integer
        @param lvmIds: list of volume identificatiors
        @raise ValueError: in case name is not set or empty
        @param lvmIds: list of child pool identificatiors
        '''
        if not name:
            raise ValueError("Name must be set")
        self.name = name
        self.parentReference = parentReference
        self.id = id
        self.type = type
        availableSpaceInMb = convertOptional(availableSpaceInMb, float,
                                             'Failed to convert vailableSpaceInMb value "%s" to float' % availableSpaceInMb)
        self.availableSpaceInMb = availableSpaceInMb and availableSpaceInMb / (1024 * 1024)
        totalSpaceInMb = convertOptional(totalSpaceInMb, float,
                                         'Failed to convert totalSpaceInMb value "%s" to float' % totalSpaceInMb)
        self.totalSpaceInMb = totalSpaceInMb and totalSpaceInMb / (1024 * 1024)
        unExportedSpaceInMb = convertOptional(unExportedSpaceInMb, float,
                                              'Failed to convert unExportedSpaceInMb value "%s" to float' % unExportedSpaceInMb)
        self.unExportedSpaceInMb = unExportedSpaceInMb and unExportedSpaceInMb / (1024 * 1024)
        self.dataRedund = convertOptional(dataRedund, int,
                                          'Failed to convert dataRedund value "%s" to int' % dataRedund)
        self.lvmIds = lvmIds or []
        self.cimId = cimId
        self.childPoolIds = childPoolIds or []

    def __repr__(self):
        return 'StoragePool(name="%s", parentReference="%s", id="%s", type="%s", availableSpaceInMb="%s", totalSpaceInMb="%s", unExportedSpaceInMb="%s", dataRedund="%s", lvmIds = %s)' % \
               (self.name, self.parentReference, self.id, self.type, self.availableSpaceInMb, self.totalSpaceInMb,
                self.unExportedSpaceInMb, self.dataRedund, self.lvmIds, self.childPoolIds)

    def __str__(self):
        return self.__repr__()

class LogicalVolumeId():
    '''
    DO represents logical volume identity with contatiner+id.
    '''
    def __init__(self, container, lvId):
        self.container = container
        self.lvId = lvId

    def __repr__(self):
        return "LogicalVolumeId(container='%s', lvId='%s')" % (self.container, self.lvId)

    def __str__(self):
        return self.__repr__()

class LogicalVolume(entity.Immutable):
    '''
    DO represents Logical Volume
    '''
    def __init__(self, name, parentReference = None, systemObjId = None, freeSpaceInMb = None, sizeInMb = None, usedSpaceInMb = None, humanReadableName = None, status = None, poolId= None):
        '''
        @param name: string
        @param parentReference: string
        @param systemObjId: string
        @param freeSpaceInMb: float
        @param sizeInMb: float
        @param usedSpaceInMb: float
        @raise ValueError: in case name is not set or empty
        '''
        if not name:
            raise ValueError("Name must be set")
        self.name = name
        self.parentReference = parentReference
        self.systemObjId = systemObjId
        self.freeSpaceInMb = convertOptional(freeSpaceInMb, float,
                                             'Failed to convert freeSpaceInMb value "%s" to float' % freeSpaceInMb)
        self.sizeInMb = convertOptional(sizeInMb, float, 'Failed to convert sizeInMb value "%s" to float' % sizeInMb)
        self.usedSpaceInMb = convertOptional(usedSpaceInMb, float,
                                             'Failed to convert usedSpaceInMb value "%s" to float' % usedSpaceInMb)
        self.humanReadableName = humanReadableName
        self.status = status
        self.poolId = poolId

    def __repr__(self):
        return "LogicalVolume(name='%s', parentReference='%s', systemObjId='%s', freeSpaceInMb='%s', sizeInMb='%s', usedSpaceInMb='%s')" % \
               (
               self.name, self.parentReference, self.systemObjId, self.freeSpaceInMb, self.sizeInMb, self.usedSpaceInMb)

    def __str__(self):
        return self.__repr__()

class PhysicalVolume():
    '''
    DO represents Physical Volume or Physical Disk
    '''

    def __init__(self, name, parentReference=None, systemObjId=None, sizeInMb=None, humanReadableName=None,
                 poolId=None):
        '''
        @param name: string
        @param parentReference: string
        @param systemObjId: string
        @param sizeInMb: float
        @param usedSpaceInMb: float
        @raise ValueError: in case name is not set or empty
        '''
        if not name:
            raise ValueError("Name must be set")
        self.name = name
        self.parentReference = parentReference
        self.systemObjId = systemObjId
        self.sizeInMb = convertOptional(sizeInMb, float, 'Failed to convert sizeInMb value "%s" to float' % sizeInMb)
        self.humanReadableName = humanReadableName
        self.poolId = poolId
        self.Osh = None

    def __repr__(self):
        return "PhysicalVolume(name='%s', parentReference='%s', systemObjId='%s', sizeInMb='%s')" % \
               (self.name, self.parentReference, self.systemObjId, self.sizeInMb)

    def __str__(self):
        return self.__repr__()

class Chassis():
    '''
    DO represents Physical Volume or Physical Disk
    '''

    def __init__(self, parentReference=None, manufacturer=None, model=None, serialNumber=None, version=None):
        self.parentReference = parentReference
        self.manufacturer = manufacturer
        self.model = model
        self.serialNumber = serialNumber
        self.version = version

    def __repr__(self):
        return "Chassis(manufacturer='%s', parentReference='%s', " \
               "model='%s', serialNumber='%s', version='%s')" % \
               (self.manufacturer, self.parentReference, self.model, self.serialNumber, self.version)

    def __str__(self):
        return self.__repr__()

class StorageProcessor(entity.Immutable):
    '''
    DO represents Storage Processor System
    '''

    def __init__(self, id, name, node_wwn=None, system_path=None, version=None, serial=None, status=None, model=None,
                 vendor=None, ip=None, parent=None):
        '''
        @param name: string
        @param id: string
        @param node_wwn: string
        @param system_apth: string
        @param version: string
        @param serial: string
        @param status: string
        @raise ValueError: in case name is not set or empty
        @param parent: string storage system.
        '''
        if not (name or id):
            raise ValueError("Name and id must be set")
        self.id = id
        self.name = name
        self.node_wwn = node_wwn
        self.system_path = system_path
        self.version = version
        self.serial = serial
        self.status = status
        self.model = model
        self.vendor = vendor
        self.ip = ip
        self.parent = parent


class IOGroup(entity.Immutable):
    '''
    DO represents Storage Processor System
    '''

    def __init__(self, id, name, parent=None, nodes=None):
        '''
        @param name: string
        @param id: string
        @param nodes: string composite nodes
        @param parent: string storage system.
        '''
        if not (name or id):
            raise ValueError("Name and id must be set")
        self.id = id
        self.name = name
        self.parent = parent
        if nodes:
            self.nodes = nodes
        else:
            self.nodes = []


class IOGroupBuilder:
    '''
    IO Group builder
    '''
    def build(self, iogroup):
        if not iogroup:
            raise ValueError('IO Group is not specified.')

        iogroupOsh = ObjectStateHolder('iogroup')
        iogroupOsh.setStringAttribute('name', iogroup.id)
        iogroupOsh.setStringAttribute('description', iogroup.name)
        return iogroupOsh


class IOGroupReporter:
    '''
    Storage Processor System reporter
    '''
    def __init__(self, builder):
        '''
        @param builder: instance of StorageProcessorBuilder
        @raise ValueError: builder is not set
        '''
        if not builder:
            raise ValueError('Builder is not passed')
        self.builder = builder

    def report(self, iogroup, containerOsh, nodeOshMap):
        '''
        @param iogroup: instance of IO Group
        @param containerOsh: container Osh
        @param nodeOshMap: nodeOsh map
        @raise ValueError: container is not set
        @return: tuple(IOGroup Osh, OSHV)
        '''
        vector = ObjectStateHolderVector()
        IOGroupOsh = self.builder.build(iogroup)
        if containerOsh:
            IOGroupOsh.setContainer(containerOsh)
        vector.add(IOGroupOsh)

        for node in iogroup.nodes:
            nodeOsh = nodeOshMap.get(node)
            if nodeOsh:
                linkOsh = modeling.createLinkOSH('usage', IOGroupOsh, nodeOsh)
                vector.add(linkOsh)

        return IOGroupOsh, vector


class RemoteHostToLogicalVolumeLink(entity.Immutable):
    '''
    Do represents a link between logical volume on storage array and remote host
    '''
    def __init__(self, volume_id, host_id):
        if not (volume_id and host_id):
            raise ValueError('Both values for volime id and host id must be set')
        self.volume_id = volume_id
        self.host_id = host_id

    def __repr__(self):
        return "RemoteHostToLogicalVolumeLink(volume_id='%s', host_id='%s')" %\
            (self.volume_id, self.host_id)

    def __str__(self):
        return self.__repr__()

class AuthorizedPrivilege:
    '''
    Do represents a AuthorizedPrivilege link to controller and storage hardwareid.
    '''
    def __init__(self, apid, controllerId=None, storageHwId=None):
        self.apid = apid
        self.controllerId = controllerId
        self.storageHwId = storageHwId

    def __repr__(self):
        return "AuthorizedPrivilege(apid='%s', controllerId='%s', storageHwId='%s')" % \
               (self.apid, self.controllerId, self.storageHwId)

    def __str__(self):
        return self.__repr__()

class LUN(entity.Immutable):
    '''
    DO represents LUN
    '''

    def __init__(self, lun_id):
        '''
        @param name: string
        @param id: string cim instance
        @param lun_id: integer
        @param parent: string storage system.
        '''
        if not (lun_id):
            raise ValueError("Name and id must be set")
        self.lun_id = lun_id


class LUNBuilder:
    '''
    LUN builder
    '''
    def build(self, lun):
        if not lun:
            raise ValueError('LUN is not specified.')

        LunOsh = ObjectStateHolder('LUN')
        if isinstance( lun.lun_id, basestring ):
            lun_id = int(lun.lun_id)
        else:
            lun_id = lun.lun_id

        LunOsh.setIntegerAttribute('lun_id', lun_id)
        return LunOsh

class LUNReporter:
    '''
    Reporter class for LUN.
    '''
    def __init__(self, builder):
        '''
        @param builder: instance of LUNBuilder
        @raise ValueError: builder is not set
        '''
        if not builder:
            raise ValueError('Builder is not passed')
        self.builder = builder

    def report(self, lun):
        """
        @param lun: instance of lun DO
        @return: tuple (LUN Osh, OSHV)
        @raise ValueError: Container is missing
        """
        lunOsh = self.builder.build(lun)
        vector = ObjectStateHolderVector()
        vector.add(lunOsh)
        return (lunOsh, vector)

STORAGEHARDWARE_ID_TYPE_MAP = {
    '1': 'Other',
    '2': 'PortWWN',
    '3': 'NodeWWN',
    '4': 'Hostname',
    '5': 'iSCSI Name',
    '6': 'SwitchWWN',
    '7': 'SAS Address'
}

class StorageHardwareId(entity.Immutable):
    '''
    Do represents a storage hardware id.(remote connection info)
    @param storage_id: instance of IQN or WWN
    @param id_type: type of Other, PortWWN, NodeWWN, Hostname, iSCSI Name, SwitchWWN, SAS Address
    '''
    def __init__(self, storage_id, id_type ):
        self.storage_id = storage_id
        self.id_type = id_type

    def __repr__(self):
        return "StorageHardwareId(storage_id='%s', id_type='%s')" % \
               (self.storage_id, self.id_type)

    def __str__(self):
        return self.__repr__()

class LunMaskingMappingView(entity.Immutable):
    '''
    Do represents a LUN masking mapping view.(remote endpoint --> local endpoint--> logical volume)
    '''
    def __init__(self, volumes, shIdObj, local_wwns, lun = None):
        self.volumes = volumes
        self.storage_hardware_id = shIdObj
        self.local_wwns = local_wwns
        self.lun = lun


    def __repr__(self):
        return "LunMaskingMappingView(volumes='%s', shId='%s', " \
               "local_wwns='%s', lun_id='%s')" % \
               (self.volumes, self.shIdObj and self.shIdObj.storage_id,
                self.local_wwns, self.lun and self.lun.lun_id)

    def __str__(self):
        return self.__repr__()

class RemoteEndPoint:
    '''
    class represent the remote endpoint to which the local port is connected to
    '''
    def __init__(self, name, wwn, portIndex, objId = None, hostIp = None):
        if not (((name or hostIp) and portIndex) or wwn):
            raise ValueError("One of the required fields is not set: name, wwn, portIndex")
        self.name = name
        self.hostIp = hostIp
        self.wwn = wwn
        self.portIndex = portIndex and int(portIndex)
        self.objId = objId

    def __repr__(self):
        return "RemoteEndPoint(name='%s', wwn='%s', portIndex='%s', objId='%s')" % (
        self.name, self.wwn, self.portIndex, self.objId)

    def __str__(self):
        return self.__repr__()

class RemoteEndPointReporter:
    '''
    Builds and report remote Host Osh and fcpool Osh
    '''
    def report(self, endpoint, hostOsh):
        if not endpoint:
            raise ValueError('No endpoint set')

        if hostOsh is None:
            if endpoint.hostIp:
                hostOsh = modeling.createHostOSH(endpoint.hostIp)
                if endpoint.name:
                    hostOsh.setStringAttribute('name', endpoint.name)
            elif endpoint.name:
                hostOsh = buildCompleteHost(endpoint.name)

        remoteWwnFormated = str(wwn.parse_from_str(endpoint.wwn))
        if remoteWwnFormated:
            fcPort = FcPort(endpoint.portIndex, endpoint.portIndex, endpoint.wwn)
            fcPortBuilder = FibreChanelPortBuilder()
            fcPortReporter = FibreChannelPortReporter(fcPortBuilder)
            (fcPortOsh, vector) = fcPortReporter.report(fcPort, hostOsh)

        if hostOsh:
            vector.add(hostOsh)

        return (fcPortOsh, hostOsh, vector)

class FibreChanelPortBuilder:
    '''
    Class grants an ability to build an fcport Osh from a FcPort DO
    Note: Container is not set, since it's a part of reporting.
    '''
    def build(self, fcPort):
        '''
        @param fcPort: fcPort DO
        @return: fcport OSH
        '''
        if not fcPort:
            raise ValueError("fcPort object is None")
        fcPortOsh = ObjectStateHolder("fcport")
        if fcPort.id is not None:
            fcPortOsh.setIntegerAttribute("fcport_portid", fcPort.id)
        if fcPort.index is not None:
            fcPortOsh.setIntegerAttribute("port_index", fcPort.index)

        wwnFormated = ''
        try:
            wwnFormated = str(wwn.parse_from_str(fcPort.wwn))
        except:
            logger.debug('error about fcPort.wwn: %s' % fcPort.wwn)

        if len(wwnFormated) != len('XX:XX:XX:XX:XX:XX:XX:XX'):
            return None
        fcPort.wwn and fcPortOsh.setStringAttribute("fcport_wwn", wwnFormated)
        fcPort.name and fcPortOsh.setStringAttribute("fcport_symbolicname", fcPort.name)
        fcPort.status and fcPortOsh.setStringAttribute("fcport_status", fcPort.status)
        fcPort.state and fcPortOsh.setStringAttribute("fcport_state", fcPort.state)
        fcPort.portType and fcPortOsh.setStringAttribute("fcport_type", fcPort.portType)
        if fcPort.maxSpeedGbps is not None:
            fcPortOsh.setAttribute("fcport_maxspeed", fcPort.maxSpeedGbps)
        if fcPort.speedGbps is not None:
            fcPortOsh.setAttribute("fcport_speed", fcPort.speedGbps)
        return fcPortOsh

class FibreChannelPortReporter:
    '''
    Class grants an ability to build and report fcport as OSH and OSHV
    '''
    def __init__(self, builder):
        '''
        @param builder: instance of FibreChanelPortBuilder
        @raise ValueError: builder is not set
        '''
        if not builder:
            raise ValueError('Builder is not passed')
        self.builder = builder

    def report(self, fcPort, containerOsh):
        '''
        @param fcPort: fcPort DO
        @param containerOsh: osh of corresponding container
        @return: tuple (fcport Osh, OSHV)
        @raise ValueError: Container is missing
        '''
        #        if not containerOsh:
        #            raise ValueError('Container for fcPort is not specified')
        fcPortOsh = self.builder.build(fcPort)
        vector = ObjectStateHolderVector()
        if fcPortOsh:
            if containerOsh:
                fcPortOsh.setContainer(containerOsh)
            vector.add(fcPortOsh)

        return (fcPortOsh, vector)

class LogicalVolumeBuilder:
    '''
    Builder class for logical volume.
    '''
    def build(self, logVolume):
        '''
        @param logVolume: LogicalVolume DO instance
        @raise ValueError: logVolume is not set
        '''
        if not logVolume:
            raise ValueError("logVolume is not specified")
        lvOsh = ObjectStateHolder("logical_volume")
        lvOsh.setStringAttribute("name", logVolume.name)
        if logVolume.freeSpaceInMb is not None:
            lvOsh.setAttribute("logicalvolume_free", logVolume.freeSpaceInMb)
        if logVolume.sizeInMb is not None:
            lvOsh.setAttribute("logicalvolume_size", logVolume.sizeInMb)
        if logVolume.usedSpaceInMb is not None:
            lvOsh.setAttribute("logicalvolume_used", logVolume.usedSpaceInMb)
        if logVolume.humanReadableName is not None:
            lvOsh.setStringAttribute('user_label', logVolume.humanReadableName)
        if logVolume.status is not None:
            lvOsh.setStringAttribute('logicalvolume_status', logVolume.status)
        if logVolume.systemObjId is not None:
            lvOsh.setStringAttribute('logical_volume_global_id', logVolume.systemObjId)
        return lvOsh


class LogicalVolumeReporter:
    '''
    Reporter class for Logical Volume.
    '''
    def __init__(self, builder):
        '''
        @param builder: instance of LogicalVolumeBuilder
        @raise ValueError: builder is not set
        '''
        if not builder:
            raise ValueError('Builder is not passed')
        self.builder = builder

    def report(self, logVolume, containerOsh):
        '''
        @param logVolume: instance of LogicalVolume DO
        @param containerOsh: osh of corresponding container
        @return: tuple (logcal_volume Osh, OSHV)
        @raise ValueError: Container is missing
        '''
        if not containerOsh:
            raise ValueError('Container for fcPort is not specified')
        lvOsh = self.builder.build(logVolume)
        vector = ObjectStateHolderVector()
        lvOsh.setContainer(containerOsh)
        vector.add(lvOsh)
        return (lvOsh, vector)

class PhysicalVolumeBuilder:
    '''
    Builder class for physical volume.
    '''
    def build(self, physVolume):
        '''
        @param physVolume: PhysicalVolume DO instance
        @raise ValueError: physVolume is not set
        '''
        if not physVolume:
            raise ValueError("physVolume is not specified")
        pvOsh = ObjectStateHolder("physicalvolume")
        pvOsh.setStringAttribute("name", physVolume.name)
        if physVolume.sizeInMb is not None:
            pvOsh.setAttribute("volume_size", physVolume.sizeInMb)
        if physVolume.humanReadableName is not None:
            pvOsh.setStringAttribute('user_label', physVolume.humanReadableName)
        if physVolume.systemObjId is not None:
            pvOsh.setStringAttribute('volume_id', physVolume.systemObjId)
        return pvOsh

class PhysicalVolumeReporter:
    '''
    Reporter class for Physical Volume.
    '''
    def __init__(self, builder):
        '''
        @param builder: instance of PhysicalVolumeBuilder
        @raise ValueError: builder is not set
        '''
        if not builder:
            raise ValueError('Builder is not passed')
        self.builder = builder

    def report(self, physVolume, containerOsh):
        '''
        @param physVolume: instance of PhysicalVolume DO
        @param containerOsh: osh of corresponding container
        @return: tuple (physical_volume Osh, OSHV)
        @raise ValueError: Container is missing
        '''
        if not containerOsh:
            raise ValueError('Container for fcPort is not specified')
        pvOsh = self.builder.build(physVolume)
        pvOsh.setContainer(containerOsh)
        vector = ObjectStateHolderVector()
        vector.add(pvOsh)
        return (pvOsh, vector)


class StoragePoolBuilder:
    '''
    Builder for StoragePool DO
    '''
    def build(self, storagePool):
        '''
        @param storagePool: instance of StoragePool DO
        @raise ValueError: storagePool is not set
        @return: storagepool Osh
        '''
        if not storagePool:
            raise ValueError("storagePool is not specified")
        spOsh = ObjectStateHolder("storagepool")
        spOsh.setStringAttribute("name", storagePool.name)
        if storagePool.id is not None:
            spOsh.setIntegerAttribute("storagepool_poolid", storagePool.id)
        if storagePool.cimId is not None:
            spOsh.setStringAttribute("storagepool_cimpoolid", storagePool.cimId)
        storagePool.type and spOsh.setStringAttribute("storagepool_pooltype", storagePool.type)
        if storagePool.availableSpaceInMb is not None:
            spOsh.setAttribute("storagepool_mbavailable", storagePool.availableSpaceInMb)
        if storagePool.totalSpaceInMb is not None:
            spOsh.setAttribute("storagepool_mbtotal", storagePool.totalSpaceInMb)
        if storagePool.unExportedSpaceInMb is not None:
            spOsh.setAttribute("storagepool_mbunexported", storagePool.unExportedSpaceInMb)
        if storagePool.dataRedund is not None:
            spOsh.setIntegerAttribute("storagepool_maxdataredundancy", storagePool.dataRedund)
        return spOsh


class StoragePoolReporter:
    '''
    Reporter of StoragePool DO
    '''
    def __init__(self, builder):
        '''
        @param builder: instance of StoragePoolBuilder
        @raise ValueError: builder is not set
        '''
        if not builder:
            raise ValueError('Builder is not passed')
        self.builder = builder

    def report(self, storagePool, containerOsh):
        '''
        @param storagePool: instance of StoragePool DO
        @param containerOsh: osh of corresponding container
        @return: tuple (storagepool Osh, OSHV)
        @raise ValueError: Container is missing
        '''
        if not containerOsh:
            raise ValueError('Container for Storage Pool is not specified')
        spOsh = self.builder.build(storagePool)
        spOsh.setContainer(containerOsh)
        vector = ObjectStateHolderVector()
        vector.add(spOsh)
        return (spOsh, vector)


class FcHbaBuilder:
    '''
    Fibre Channel Host Base Adapter builder
    '''
    def build(self, fcHba):
        '''
        @param fcHba: instance of FcHba DO
        @raise ValueError: fcHba is not set
        @return: fchba Osh
        '''
        if not fcHba:
            raise ValueError("fcHba is not specified")
        fcHbaOsh = ObjectStateHolder("fchba")
        fcHbaOsh.setStringAttribute('name', fcHba.name)
        wwnFormated = str(wwn.parse_from_str(fcHba.wwn))
        fcHbaOsh.setStringAttribute('fchba_wwn', wwnFormated)
        fcHbaOsh.setStringAttribute('fchba_targetportwwn', wwnFormated)
        return fcHbaOsh


class FcHbaReporter:
    '''
    Fibre Channel Host Base Adapter reporter
    '''
    def __init__(self, builder):
        '''
        @param builder: instance of FcHbaBuilder
        @raise ValueError: builder is not set
        '''
        if not builder:
            raise ValueError('Builder is not passed')
        self.builder = builder

    def report(self, fcHba, containerOsh):
        '''
        @param fcHba: instance of FcHba DO
        @param container: corresponding container osh
        @raise ValueError: container is not set
        @return: tuple(fchba Osh, OSHV)
        '''
        if not containerOsh:
            raise ValueError('Container for fcHba is not specified')
        vector = ObjectStateHolderVector()
        fcHbaOsh = self.builder.build(fcHba)
        fcHbaOsh.setContainer(containerOsh)
        vector.add(fcHbaOsh)
        return (fcHbaOsh, vector)

class StorageProcessorBuilder:
    '''
    Storage Processor System builder
    '''
    def build(self, storage_processor):
        if not storage_processor:
            raise ValueError('Storage Processor is not specified.')

        storageProcessorOsh = ObjectStateHolder('storageprocessor')
        storageProcessorOsh.setStringAttribute('name', storage_processor.name)

        if storage_processor.version:
            storageProcessorOsh.setStringAttribute('storageprocessor_version', storage_processor.version)
        if storage_processor.serial:
            storageProcessorOsh.setStringAttribute('serial_number', storage_processor.serial)
        if storage_processor.status:
            storageProcessorOsh.setStringAttribute('storageprocessor_status', storage_processor.status)
        if storage_processor.ip:
            storageProcessorOsh.setStringAttribute('storageprocessor_ip', storage_processor.ip)
        if storage_processor.model:
            storageProcessorOsh.setStringAttribute('storageprocessor_model', storage_processor.model)
        if storage_processor.vendor:
            storageProcessorOsh.setStringAttribute('storageprocessor_vendor', storage_processor.vendor)

        return storageProcessorOsh

class StorageProcessorReporter:
    '''
    Storage Processor System reporter
    '''
    def __init__(self, builder):
        '''
        @param builder: instance of StorageProcessorBuilder
        @raise ValueError: builder is not set
        '''
        if not builder:
            raise ValueError('Builder is not passed')
        self.builder = builder

    def report(self, storage_processor, containerOsh):
        '''
        @param fcHba: instance of FcHba DO
        @param container: corresponding container osh
        @raise ValueError: container is not set
        @return: tuple(fchba Osh, OSHV)
        '''
        vector = ObjectStateHolderVector()
        storageProcessorOsh = self.builder.build(storage_processor)
        storageProcessorOsh.setContainer(containerOsh)
        vector.add(storageProcessorOsh)
        return (storageProcessorOsh, vector)

class FcSwitchBuilder:
    '''
    Storage Processor System builder
    '''
    def build(self, switch):
        if not switch:
            raise ValueError('Fibre Channel Switch is not specified.')

        switchOsh = ObjectStateHolder('fcswitch')
        switchOsh.setStringAttribute('name', switch.name)

        if switch.wwn:
            switchOsh.setStringAttribute('fcswitch_wwn', switch.wwn)
        if switch.roles:
            switchOsh.setStringAttribute('fcswitch_role', switch.roles)
        if switch.domainId:
            switchOsh.setStringAttribute('fcswitch_domainid', switch.domainId)
        if switch.type and switch.type.lower().find('virtual') > -1:
            switchOsh.setBoolAttribute('host_isvirtual', 1)
        return switchOsh

class FcSwitchReporter:
    def __init__(self, builder):
        if not builder:
            raise ValueError('Builder is not passed')
        self.builder = builder

    def report(self, switch, fabricOsh):
        vector = ObjectStateHolderVector()
        switchOsh = self.builder.build(switch)
        vector.add(switchOsh)
        if fabricOsh:
            linkOsh = modeling.createLinkOSH('membership', fabricOsh, switchOsh)
            vector.add(linkOsh)
        return (switchOsh, vector)

class StorageFabricBuilder:
    def build(self, fabric):
        if not fabric:
            raise ValueError('Storage Fabric is not specified.')

        fabricOsh = ObjectStateHolder('storagefabric')
        fabricOsh.setStringAttribute('name', fabric.name)

        if fabric.wwn:
            fabricOsh.setStringAttribute('storagefabric_wwn', fabric.wwn)

        return fabricOsh

class StorageFabricReporter:
    def __init__(self, builder):
        if not builder:
            raise ValueError('Builder is not passed')
        self.builder = builder

    def report(self, fabric):
        fabricOsh = self.builder.build(fabric)
        return fabricOsh

def buildCompleteHost(name):
    '''
    @param node: host name
    @return: node Osh
    '''
    if not name:
        raise ValueError('Host name must be set')
    osh = ObjectStateHolder('node')
    osh.setStringAttribute('name', name)
    osh.setBoolAttribute('host_iscomplete', 1)
    return osh

class TopologyBuilder:
    '''
    General SMI-S topolofy builder
    '''
    def buildHost(self, node):
        '''
        @param node: instance of Host DO
        @return: node Osh
        '''
        if node.ip is not None:
            return modeling.createHostOSH(ipAddress=node.ip, machineName=node.name)
        else:
            hostOsh = modeling.ObjectStateHolder('node')
            if node.name is not None:
                hostOsh.setStringAttribute('name', node.name)
            if node.serial is not None:
                hostOsh.setStringAttribute('serial_number', node.serial)
            if node.model is not None:
                hostOsh.setStringAttribute('discovered_model', node.model)
            if node.description is not None:
                hostOsh.setStringAttribute('os_description', node.description)
            if node.vendor is not None:
                hostOsh.setStringAttribute('vendor', node.vendor)
            return hostOsh

    def buildStorageArray(self, node):
        '''
        @param node: instance of Host DO
        @return: Storage Array Osh
        '''
        if node.ip is not None:
            hostOsh = modeling.createHostOSH(hostClassName='storagearray', ipAddress=node.ip)
        else:
            hostOsh = modeling.ObjectStateHolder('storagearray')
        if node.name is not None:
            hostOsh.setStringAttribute('name', node.name)
        if node.serial is not None:
            hostOsh.setStringAttribute('serial_number', node.serial)
        if node.version is not None:
            hostOsh.setStringAttribute('hardware_version', node.version)
        if node.model is not None:
            hostOsh.setStringAttribute('discovered_model', node.model)
        if node.description is not None:
            hostOsh.setStringAttribute('os_description', node.description)
        if node.vendor is not None:
            hostOsh.setStringAttribute('vendor', node.vendor)
        if node.status is not None:
            hostOsh.setStringAttribute('storagearray_status', node.status)
        return hostOsh

    def reportFcSwitchTopolopy(self, fabrics, switches, hosts, fcPorts, switch2FabricLinks={}, pswitch2lswitchLinks = {}, portlinks={}):
        resultVector = ObjectStateHolderVector()
        fabricOshIdMap = {}
        switchOshIdMap = {}
        hostOshIdMap = {}
        portOshIdMap = {}
        for fabric in fabrics:
            fabricBuilder = StorageFabricBuilder()
            fabricReporter = StorageFabricReporter(fabricBuilder)
            fabricOsh = fabricReporter.report(fabric)
            fabricOshIdMap[fabric.name] = fabricOsh
            resultVector.add(fabricOsh)

        builder = FcSwitchBuilder()
        reporter = FcSwitchReporter(builder)
        for switch in switches:
            fabricId = switch2FabricLinks.get(switch.wwn)
            fabricOsh = fabricId and fabricOshIdMap.get(fabricId)
            (switchOsh, vector) = reporter.report(switch, fabricOsh)
            switchOshIdMap[switch.wwn] = switchOsh
            resultVector.addAll(vector)

        for host in hosts:
            hostOsh = ObjectStateHolder('node')
            if host.name:
                hostOsh.setStringAttribute('name', host.name)
                hostOshIdMap[host.name] = hostOsh
                resultVector.add(hostOsh)

        portBuilder = FibreChanelPortBuilder()
        portReporter = FibreChannelPortReporter(portBuilder)
        for fc in fcPorts:
            if fc.parentReference:
                containerOsh = fabricOshIdMap.get(fc.parentReference) or \
                               switchOshIdMap.get(fc.parentReference) or \
                               hostOshIdMap.get(fc.parentReference)
                (portOsh, vector) = portReporter.report(fc, containerOsh)
                resultVector.addAll(vector)
                if portOsh:
                    portOshIdMap[fc.wwn] = portOsh

        for link in pswitch2lswitchLinks.keys():
            lswitchOsh = switchOshIdMap.get(link)
            pswitchOsh = switchOshIdMap.get(pswitch2lswitchLinks.get(link))
            if lswitchOsh and pswitchOsh:
                linkOsh = modeling.createLinkOSH('backbone', lswitchOsh, pswitchOsh)
                resultVector.add(linkOsh)

        for link in portlinks.keys():
            end1Osh = portOshIdMap.get(link)
            end2Osh = portOshIdMap.get(portlinks.get(link))
            if end1Osh and end2Osh:
                linkOsh = modeling.createLinkOSH('fcconnect', end1Osh, end2Osh)
                resultVector.add(linkOsh)

        return resultVector

    def reportFabricTopology(self, topology):
        return self.reportFcSwitchTopolopy(topology.storage_fabrics, topology.fc_switchs, topology.hosts,
                                           topology.ports, topology.switch_2_fabric,
                                           topology.physical_switch_2_logical_switch,topology.fcport_connections)

    def reportStorageTopology(self, topology):
        return self.reportTopology(
            topology.storage_systems, topology.ports, topology.storage_pools, topology.logical_volumes,
            topology.remote_endpoints, topology.hc_hbas,
            topology.storage_processors, topology.physical_volumes, topology.file_shares, topology.file_systems,
            topology.end_points_links, topology.lun_mappings, topology.physcial_volumes_2_pool_links, topology.iogroups)

    def reportTopology(self, storageSystems, ports, pools, lvs, endPoints, fcHbas=None, storageProcessors=None,
                       pvs=None, fileShares=None, fileSystems=None, endpointLinks=None, lunMappings=None,
                       pv2poolLinks=None, iogroups=None):
        '''
        @param ports: collection of FcPort DO instances
        @param pools: collection of StoragePool DO instances
        @param lvs: collection of LogicaVolumes DO instances
        @param fcHbas: collection of FcHba DO instances
        @return: OSHV
        '''
        resultVector = ObjectStateHolderVector()
        if not storageSystems:
            raise ValueError('No storage system discovered.')
        idToHostOshMap = {}
        storageSystemIdToOshMap = {}
        for storageSystem in storageSystems:
            storageSystemOsh = self.buildStorageArray(storageSystem)
            resultVector.add(storageSystemOsh)
            storageSystemIdToOshMap[storageSystem.id] = storageSystemOsh
            resultVector.add(storageSystemOsh)
            idToHostOshMap[storageSystem.id] = storageSystemOsh

        storageSystem = storageSystems[0]
        storageSystemOsh = storageSystemIdToOshMap.get(storageSystem.id)

        storageProcessorIdToOshMap = {}
        storageProcessorNameToOshMap = {}
        if storageProcessors:
            for storageProcessor in storageProcessors:
                processorBuilder = StorageProcessorBuilder()
                processorReporter = StorageProcessorReporter(processorBuilder)
                if storageProcessor.parent:
                    parentOsh = storageSystemIdToOshMap.get(storageProcessor.parent) or \
                                storageSystemIdToOshMap.get(storageProcessor.parent[:len(storageProcessor.parent)-1])
                else:
                    parentOsh = storageSystemOsh
                (processorOsh, vector) = processorReporter.report(storageProcessor, parentOsh)
                storageProcessorIdToOshMap[storageProcessor.id] = processorOsh
                storageProcessorNameToOshMap[storageProcessor.name] = processorOsh
                resultVector.addAll(vector)

        iogroupIdToOshMap = {}
        for iogroup in iogroups:
            builder = IOGroupBuilder()
            iogroupReporter = IOGroupReporter(builder)
            if iogroup.parent:
                parentOsh = storageSystemIdToOshMap.get(iogroup.parent)
            else:
                parentOsh = storageSystemOsh
            iogroupOsh, vector = iogroupReporter.report(iogroup, parentOsh, storageProcessorNameToOshMap)
            iogroupIdToOshMap[iogroup.id] = iogroupOsh
            resultVector.addAll(vector)

        portRemotePeerToPortOshMap = {}
        portBuilder = FibreChanelPortBuilder()
        portReporter = FibreChannelPortReporter(portBuilder)

        endpointIdToHostOshMap = {}
        reporter = RemoteEndPointReporter()
        for endpoint in endPoints:
            try:
                hostOsh = storageSystemIdToOshMap.get(endpoint.name) or storageProcessorIdToOshMap.get(endpoint.name)
                (remotePortOsh, remoteNodeOsh, vector) = reporter.report(endpoint, hostOsh)
                resultVector.addAll(vector)
                if remotePortOsh:
                    portRemotePeerToPortOshMap[endpoint.name] = remotePortOsh
                endpointIdToHostOshMap[endpoint.objId] = remoteNodeOsh
            except ValueError, e:
                logger.debugException('Failed to report fc port')

        for port in ports:
            containerOsh = idToHostOshMap.get(port.parentReference, None)
            if not containerOsh:
                containerOsh = storageProcessorIdToOshMap.get(port.parentReference, None)
            if containerOsh:
                (portOsh, vector) = portReporter.report(port, containerOsh)
                resultVector.addAll(vector)
                # remotePortOsh = portRemotePeerToPortOshMap.get(port.remotePeer)
                remote_peers = [port.remotePeer]
                if port.remotePeer and port.remotePeer.find(';') != -1:
                    remote_peers.extend(port.remotePeer.split(';'))
                for remote_peer in remote_peers:
                    remotePortOsh = portRemotePeerToPortOshMap.get(remote_peer)
                    if remotePortOsh and portOsh:
                        linkOsh = modeling.createLinkOSH('fcconnect', portOsh, remotePortOsh)
                        resultVector.add(linkOsh)
                    if port.storageProcessorId:
                        processorOsh = storageProcessorIdToOshMap.get(port.storageProcessorId)
                        if processorOsh:
                            linkOsh = modeling.createLinkOSH('containment', processorOsh, portOsh)
                            resultVector.add(linkOsh)

        fcHbaBuilder = FcHbaBuilder()
        fcHbaReporter = FcHbaReporter(fcHbaBuilder)
        if fcHbas:
            for fcHba in fcHbas:
                hostOsh = idToHostOshMap.get(fcHba.parentReference)
                if hostOsh:
                    (fcHbaOsh, vector) = fcHbaReporter.report(fcHba, hostOsh)
                    resultVector.addAll(vector)

        pvIdMap = {}
        pvBuilder = PhysicalVolumeBuilder()
        pvReporter = PhysicalVolumeReporter(pvBuilder)
        for pv in pvs:
            hostOsh = idToHostOshMap.get(pv.parentReference)
            if not hostOsh:
                hostOsh = storageProcessorIdToOshMap.get(pv.parentReference)
            if hostOsh:
                (pvOsh, vector) = pvReporter.report(pv, hostOsh)
                resultVector.addAll(vector)
                pv.Osh = pvOsh
                pvIdMap[pv.parentReference + pv.systemObjId] = pv

        poolBuilder = StoragePoolBuilder()
        poolReporter = StoragePoolReporter(poolBuilder)
        poolIdToPoolOsh = {}
        for pool in pools:
            if pool.parentReference:
                parentOsh = storageSystemIdToOshMap.get(pool.parentReference) or \
                            storageSystemIdToOshMap.get(pool.parentReference[:len(pool.parentReference)-1])
            else:
                parentOsh = storageSystemOsh
            (poolOsh, vector) = poolReporter.report(pool, parentOsh)
            if pool.cimId:
                poolIdToPoolOsh[pool.cimId] = poolOsh
            resultVector.addAll(vector)

        for poolId in pv2poolLinks.keys():
            poolOsh = poolIdToPoolOsh.get(poolId)
            if poolOsh:
                pvs = pv2poolLinks[poolId]
                for pvId in pvs:
                    pv = pvIdMap.get(pvId)
                    if pv is None:
                        logger.debug('cannot find physical volume pvId:' + pvId)
                        continue
                    if pv.Osh:
                        linkOsh = modeling.createLinkOSH('usage', poolOsh, pv.Osh)
                        resultVector.add(linkOsh)

        lvmBuilder = LogicalVolumeBuilder()
        lvmReporter = LogicalVolumeReporter(lvmBuilder)
        lvmIdToLvmOshMap = {}
        lvSystemNameAndIdToLvmOshMap = {}
        for lv in lvs:
            hostOsh = idToHostOshMap.get(lv.parentReference) \
                      or iogroupIdToOshMap.get(lv.parentReference) \
                      or storageSystemIdToOshMap.get(lv.parentReference) \
                      or storageProcessorIdToOshMap.get(lv.parentReference)
            if hostOsh:
                (lvmOsh, vector) = lvmReporter.report(lv, hostOsh)
                lvmIdToLvmOshMap[lv.systemObjId] = lvmOsh
                lvSystemNameAndIdToLvmOshMap[lv.parentReference + lv.systemObjId] = lvmOsh
                resultVector.addAll(vector)
                if lv.poolId:
                    poolOsh = poolIdToPoolOsh.get(lv.poolId)
                    if poolOsh:
                        linkOsh = modeling.createLinkOSH('membership', poolOsh, lvmOsh)
                        resultVector.add(linkOsh)

        # building member links
        for pool in pools:
            (poolOsh, vector) = poolReporter.report(pool, storageSystemOsh)
            if pool.lvmIds:
                for lvmId in pool.lvmIds:
                    lvOsh = lvmIdToLvmOshMap.get(lvmId)
                    if lvOsh:
                        linkOsh = modeling.createLinkOSH('membership', poolOsh, lvOsh)
                        resultVector.add(linkOsh)
            if pool.childPoolIds:
                for poolId in pool.childPoolIds:
                    plOsh = poolIdToPoolOsh.get(poolId)
                    if plOsh:
                        linkOsh = modeling.createLinkOSH('membership', poolOsh, plOsh)
                        resultVector.add(linkOsh)

        fsId2fsOshMap = {}
        fsBuilder = FileSystemBuilder()
        fsReporter = FileSystemReporter(fsBuilder)
        for fs in fileSystems:
            hostOsh = storageSystemIdToOshMap.get(fs.parentReference) or storageProcessorIdToOshMap.get(
                fs.parentReference)
            if hostOsh:
                (fsOsh, vector) = fsReporter.report(fs, hostOsh)
                fsId2fsOshMap[fs.name] = fsOsh
                resultVector.addAll(vector)
                lvOsh = lvSystemNameAndIdToLvmOshMap.get(fs.lvReference)
                if lvOsh:
                    linkOsh = modeling.createLinkOSH('dependency', fsOsh, lvOsh)
                    resultVector.add(linkOsh)

        if fileShares:
            fshareBuilder = FileShareBuilder()
            fshareReporter = FileShareReporter(fshareBuilder)
            for fshare in fileShares:
                hostOsh = storageSystemIdToOshMap.get(fshare.parentReference) or storageProcessorIdToOshMap.get(
                    fshare.parentReference)
                if hostOsh:
                    (fshareOsh, vector) = fshareReporter.report(fshare, hostOsh)
                    resultVector.addAll(vector)
                    fsOsh = fsId2fsOshMap.get(fshare.fsReference)
                    if fsOsh:
                        linkOsh = modeling.createLinkOSH('realization', fshareOsh, fsOsh)
                        resultVector.add(linkOsh)

        if endpointLinks:
            lvmIdToLvmMap = {}
            for lv in lvs:
                lvmIdToLvmMap[lv.systemObjId] = lv
            for endpointLink in endpointLinks:
                localVolumeOsh = lvmIdToLvmOshMap.get(endpointLink.volume_id)
                remoteNodeOsh = endpointIdToHostOshMap.get(endpointLink.host_id)
                logVolume = lvmIdToLvmMap.get(endpointLink.volume_id)
                if localVolumeOsh and remoteNodeOsh and logVolume:
                    (remoteLvmOsh, vector) = lvmReporter.report(logVolume, remoteNodeOsh)
                    resultVector.addAll(vector)
                    linkOsh = modeling.createLinkOSH('dependency', remoteLvmOsh, localVolumeOsh)
                    resultVector.add(linkOsh)

        for lunMap in lunMappings:
            remoteFcPortOsh = None
            remoteiScsiAdapterOsh = None
            lunOsh = None

            if lunMap.storage_hardware_id:
                try:
                    remotePortType = STORAGEHARDWARE_ID_TYPE_MAP.get(lunMap.storage_hardware_id.id_type)
                    if remotePortType == 'PortWWN' or remotePortType == 'NodeWWN':
                        remoteWwnFormated = lunMap.storage_hardware_id.storage_id and \
                                            str(wwn.parse_from_str(lunMap.storage_hardware_id.storage_id))
                        if not remoteWwnFormated:
                            continue
                        remoteFcPortOsh = ObjectStateHolder("fcport")
                        remoteFcPortOsh.setStringAttribute("fcport_wwn", remoteWwnFormated)
                        resultVector.add(remoteFcPortOsh)
                    elif remotePortType == 'iSCSI Name':
                        remoteiScsiAdapterOsh = ObjectStateHolder("iscsi_adapter")
                        remoteiScsiAdapterOsh.setStringAttribute("iqn", lunMap.storage_hardware_id.storage_id)
                        resultVector.add(remoteiScsiAdapterOsh)

                    if lunMap.lun:
                        has_relationship = False
                        if lunMap.local_wwns:
                            for local_wwn in lunMap.local_wwns:
                                localFcPortOsh = None
                                lunBuilder = LUNBuilder()
                                lunReporter = LUNReporter(lunBuilder)
                                (lunOsh, vector) = lunReporter.report(lunMap.lun)
                                try:
                                    localWwnFormated = local_wwn and str(wwn.parse_from_str(local_wwn))
                                    if not localWwnFormated:
                                        continue
                                    localFcPortOsh = ObjectStateHolder("fcport")
                                    localFcPortOsh.setStringAttribute("fcport_wwn", localWwnFormated)
                                    resultVector.add(localFcPortOsh)
                                    if remoteiScsiAdapterOsh:
                                        link1Osh = modeling.createLinkOSH('iscsi_initiator',lunOsh, remoteiScsiAdapterOsh)
                                        link2Osh = modeling.createLinkOSH('iscsi_target',lunOsh, localFcPortOsh)
                                        str_wwn = remoteiScsiAdapterOsh.getAttributeValue("iqn")
                                        note = "LUN for initiator:" + str(str_wwn) + " target:" + localWwnFormated
                                        lunOsh.setStringAttribute('data_note', note)
                                        resultVector.add(lunOsh)
                                        resultVector.add(link1Osh)
                                        resultVector.add(link2Osh)
                                        has_relationship = True
                                    if remoteFcPortOsh:
                                        str_wwn = remoteFcPortOsh.getAttributeValue("fcport_wwn")
                                        note = "LUN for initiator:" + str(str_wwn) + " target:" + localWwnFormated
                                        lunOsh.setStringAttribute('data_note', note)
                                        link1Osh = modeling.createLinkOSH('iscsi_initiator', lunOsh, remoteFcPortOsh)
                                        link2Osh = modeling.createLinkOSH('iscsi_target', lunOsh, localFcPortOsh)
                                        resultVector.add(lunOsh)
                                        resultVector.add(link1Osh)
                                        resultVector.add(link2Osh)
                                        has_relationship = True

                                    linkOsh = modeling.createLinkOSH('fcconnect', remoteFcPortOsh, localFcPortOsh)
                                    resultVector.add(linkOsh)
                                except:
                                    logger.debug('error about lunMap.local_wwns: %s' % lunMap.local_wwns)
                                    logger.debugException('')

                                if has_relationship:
                                    for lvRef in lunMap.volumes:
                                        lvOsh = lvSystemNameAndIdToLvmOshMap.get(lvRef.container + lvRef.lvId)
                                        if lvOsh and lunOsh:
                                            linkOsh = modeling.createLinkOSH('dependency', lunOsh, lvOsh)
                                            resultVector.add(linkOsh)

                except:
                    logger.debug('error about lunMap.remote storage hardware id: %s' % lunMap.storage_hardware_id.storage_id)
                    logger.debugException('')

        return resultVector
