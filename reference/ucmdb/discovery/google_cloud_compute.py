#coding=utf-8
import entity
import logger
import re
import modeling
import netutils
import google_cloud
from vendors import PlatformVendors

from java.util import Date
from java.lang import Double
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector


def convertToDate(item):
    from datetime import datetime, timedelta, tzinfo
    class FixedOffset(tzinfo):
        """offset_str: Fixed offset in str: e.g. '-04:00'"""
        def __init__(self, offsetString):
            sign, hours, minutes = offsetString[0], offsetString[1:3], offsetString[4:]
            offset = (int(hours) * 60 + int(minutes)) * (-1 if sign == "-" else 1)
            self.__offset = timedelta(minutes=offset)
            self.hour = hours
            self.sign = sign
        def utcoffset(self, dt=None):
            return self.__offset
        def tzname(self, dt):
            return "UTC" + self.sign + self.hour
        def dst(self, dt=None):
            return timedelta(0)
        def __repr__(self):
            return 'FixedOffset(%d)' % (self.utcoffset().total_seconds() / 60)
    try:
        from java.text import SimpleDateFormat
        if item.creationTimestamp:
            dateString, tz = item.creationTimestamp[:-6], item.creationTimestamp[-6:]
            dateUtc = datetime.strptime(dateString, "%Y-%m-%dT%H:%M:%S.%f")
            dt = dateUtc.replace(tzinfo=FixedOffset(tz))
            dateFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            dateTime = dateFormat.parse(dateFormat.format(dt))
            return dateTime
    except:
        logger.debugException('Failed to convert create timestamp to Date for: ', item)
        return None


class HasSource:
    def __init__(self, sourceUrl):
        self.sourceUrl = sourceUrl
        self.__source = None

    def setSource(self, source):
        self.__source = source

    def getSource(self):
        return self.__source


class Image(google_cloud.HasId, entity.HasName, entity.HasOsh, HasSource):
    def __init__(self, id, name, description=None, creationTimestamp=None, diskSizeGb=None, archiveSizeBytes=None,
                 status=None, sourceUrl=None, projectId=None):
        r'@types: str, long, str, str, str'
        google_cloud.HasId.__init__(self, id)
        entity.HasOsh.__init__(self)
        HasSource.__init__(self, sourceUrl)
        entity.HasName.__init__(self, name)
        self.description = description
        self.creationTimestamp = creationTimestamp
        self.diskSizeGb = entity.WeakNumeric(int)
        if diskSizeGb is not None:
            self.diskSizeGb.set(diskSizeGb)
        self.archiveSizeBytes = entity.WeakNumeric(long)
        if archiveSizeBytes is not None:
            self.archiveSizeBytes.set(archiveSizeBytes)
        self.status = status
        self.projectId = projectId

    def acceptVisitor(self, visitor):
        return visitor.visitImage(self)

    def __repr__(self):
        return "Image(%s, %s)" % (self.getId(), self.getName())


class Disk(google_cloud.HasId, entity.HasName, entity.HasOsh, HasSource):
    def __init__(self, id, name, creationTimestamp=None, description=None, diskType=None, sizeGb=None, status=None, zone=None, sourceUrl=None):
        r'@types: str, long, str, str, str'
        google_cloud.HasId.__init__(self, id)
        entity.HasOsh.__init__(self)
        HasSource.__init__(self, sourceUrl)
        entity.HasName.__init__(self, name)
        self.sizeGb = entity.WeakNumeric(int)
        if sizeGb is not None:
            self.sizeGb.set(sizeGb)
        self.creationTimestamp = creationTimestamp
        self.description = description
        self.diskType = diskType
        self.status = status
        self.isBootDevice = None
        self.isAutoDelete = None
        self.mode = None
        self.interface = None
        self.__availabilityZone = zone

    def getZone(self):
        return self.__availabilityZone

    def acceptVisitor(self, visitor):
        return visitor.visitDisk(self)

    def __repr__(self):
        return "Disk(%s, %s)" % (self.getId(), self.getName())


class Snapshot(google_cloud.HasId, entity.HasName, entity.HasOsh, HasSource):
    def __init__(self, id, name=None, description=None, creationTimestamp=None, diskSizeGb=None, storageBytes=None,
                 status=None, sourceUrl=None):
        google_cloud.HasId.__init__(self, id)
        entity.HasOsh.__init__(self)
        HasSource.__init__(self, sourceUrl)
        entity.HasName.__init__(self, name)
        self.description = description
        self.creationTimestamp = creationTimestamp
        self.diskSizeGb = entity.WeakNumeric(int)
        if diskSizeGb is not None:
            self.diskSizeGb.set(diskSizeGb)
        self.storageBytes = storageBytes
        self.status = status

    def acceptVisitor(self, visitor):
        return visitor.visitSnapshot(self)

    def __repr__(self):
        return "Snapshot(%s, %s)" % (self.getId(), self.getName())


class Volume(google_cloud.HasId, entity.HasName, entity.HasOsh):
    def __init__(self, id, name, sizeGb, disk, node):
        google_cloud.HasId.__init__(self, id)
        entity.HasName.__init__(self, name)
        entity.HasOsh.__init__(self)
        self.setName(name)
        self.sizeGb = sizeGb
        self.__disk = disk
        self.__node = node

    def acceptVisitor(self, visitor):
        return visitor.visitVolume(self)

    def getDisk(self):
        return self.__disk

    def getNode(self):
        return self.__node

    def __repr__(self):
        return 'Volume(%s, %s)' % (self.getName(), self.__disk)


class ServiceAccount(entity.HasName, entity.HasOsh):
    def __init__(self, name):
        entity.HasName.__init__(self, name)
        entity.HasOsh.__init__(self)

    def acceptVisitor(self, visitor):
        return visitor.visitServiceAccount(self)

    def __repr__(self):
        return r'ServiceAccount("%s")' % self.getName()


class Instance(google_cloud.HasId, entity.HasName, entity.HasOsh):

    def __init__(self, id, name, zone, creationTimestamp=None, description=None, cpuPlatform=None, machineType=None,
                 status=None, internalIps=None, externalIps=None, os_type=None):
        google_cloud.HasId.__init__(self, id)
        entity.HasOsh.__init__(self)
        entity.HasName.__init__(self, name)
        self.ServiceAccounts = []
        self.Volumes = []
        self.creationTimestamp = creationTimestamp
        self.cpuPlatform = cpuPlatform
        self.description = description
        self.machineType = machineType
        self.status = status
        self.internalIps = internalIps
        self.externalIps = externalIps
        self.__availabilityZone = zone
        self.__config = None
        self.os_type = self.set_os_type(os_type)

    def set_os_type(self, os_type):
        if os_type:
            if re.search(r'windows', os_type.lower()):
                return 'nt'
            else:
                return 'unix'
        return None

    def setVmConfigOsh(self, cfg):
        self.__config = cfg

    def getVmConfigOsh(self):
        return self.__config

    def getZone(self):
        return self.__availabilityZone

    def acceptVisitor(self, visitor):
        return visitor.visitInstanceNode(self)

    def __repr__(self):
        return 'Instance("%s", "%s")' % (self.getId(), self.getName())


class Builder:
    def visitImage(self, image):
        osh = ObjectStateHolder("google_cloud_image")
        osh.setStringAttribute("id", image.getId())
        osh.setStringAttribute('name', image.getName())
        osh.setStringAttribute('description', image.description)

        dateTime = convertToDate(image)
        if dateTime:
            osh.setDateAttribute('creation_time', dateTime)
        if image.archiveSizeBytes and image.archiveSizeBytes.value() is not None:
            archiveSize = long(image.archiveSizeBytes.value())
            osh.setLongAttribute('archive_size', archiveSize)
        if image.diskSizeGb and image.diskSizeGb.value() is not None:
            diskSize = int(image.diskSizeGb.value())
            osh.setIntegerAttribute('disk_size', diskSize)
        if image.status:
            osh.setStringAttribute('status', image.status)
        return osh

    def visitSnapshot(self, snapshot):
        osh = ObjectStateHolder('google_cloud_snapshot')
        osh.setStringAttribute('name', snapshot.getName())
        if snapshot.description:
            osh.setStringAttribute('description', snapshot.description)
        if snapshot.diskSizeGb and snapshot.diskSizeGb.value() is not None:
            size = int(snapshot.diskSizeGb.value())
            osh.setIntegerAttribute('disk_size', size)
        if snapshot.storageBytes:
            osh.setLongAttribute('snapshot_size', snapshot.storageBytes)
        if snapshot.status:
            osh.setStringAttribute('status', snapshot.status)

        dateTime = convertToDate(snapshot)
        if dateTime:
            osh.setDateAttribute('snapshot_create_time', dateTime)

        return osh

    def visitDisk(self, disk):
        osh = ObjectStateHolder('google_cloud_disk')
        osh.setStringAttribute('id', disk.getId())
        osh.setStringAttribute('name', disk.getName())
        osh.setBoolAttribute('is_boot_disk', disk.isBootDevice)
        osh.setBoolAttribute('is_auto_delete', disk.isAutoDelete)

        dateTime = convertToDate(disk)
        if dateTime:
            osh.setDateAttribute('creation_time', dateTime)
        if disk.sizeGb and disk.sizeGb.value() is not None:
            size = int(disk.sizeGb.value())
            osh.setIntegerAttribute('size', size)
        if disk.status:
            osh.setStringAttribute('status', disk.status)
        if disk.diskType:
            osh.setStringAttribute('type', disk.diskType)
        if disk.mode:
            osh.setStringAttribute('mode', disk.mode)
        if disk.interface:
            osh.setStringAttribute('interface', disk.interface)
        if disk.description:
            osh.setStringAttribute('description', disk.description)
        return osh

    def visitVolume(self, volume):
        osh = ObjectStateHolder('logical_volume')
        osh.setStringAttribute('name', volume.getName())
        osh.setStringAttribute('logical_volume_global_id', volume.getId())
        # convert to Mb
        if volume.sizeGb and volume.sizeGb.value() is not None:
            size = Double.valueOf(volume.sizeGb.value())
            sizeInMb = size * 1000
            osh.setDoubleAttribute('logicalvolume_size', sizeInMb)
        return osh

    def visitServiceAccount(self, sa):
        osh = ObjectStateHolder('googlecloudserviceaccount')
        osh.setStringAttribute('name', sa.getName())
        return osh

    def visitInstanceNode(self, instance):

        def buildVmConfig(instance):
            osh = ObjectStateHolder('google_cloud_vm_config')
            osh.setStringAttribute('name', instance.getName())
            dateTime = convertToDate(instance)
            if dateTime:
                osh.setDateAttribute('creation_time', dateTime)
            osh.setStringAttribute('cpu_platform', instance.cpuPlatform)
            osh.setStringAttribute('description', instance.description)
            osh.setStringAttribute('type', instance.machineType)
            osh.setStringAttribute('status', instance.status)
            instance.setVmConfigOsh(osh)

        publicAddress = privateAddress = None
        # public address
        if instance.externalIps:
            publicAddress = instance.externalIps[0]
        # private address
        if instance.internalIps:
            privateAddress = instance.internalIps[0]
        address = publicAddress
        if not address:
            logger.warn("Public address is not specified, try private address instead")
            address = privateAddress
            if not address:
                raise ValueError("Both public address and private address are not specified")
        if instance.os_type:
            osh = modeling.createHostOSH(address, hostClassName=instance.os_type)
        else:
            osh = modeling.createHostOSH(address, hostClassName='host_node')
        osh.setStringAttribute('description', instance.description)
        osh.setStringAttribute("name", instance.getName())
        osh.setBoolAttribute('host_iscomplete', True)
        osh.setBoolAttribute('host_isvirtual', True)
        id = instance.getId()
        if id:
            osh.setStringAttribute("cloud_instance_id", id)
            osh.setStringAttribute("host_key", id)
        # Host Platform Vendor
        osh.setStringAttribute('platform_vendor', PlatformVendors.Google)
        buildVmConfig(instance)
        return osh


class Reporter:
    def __init__(self, builder):
        self.__builder = builder

    def getSourceOsh(self, item):
        if not item.getSource():
            logger.debug('%s has no Source.' % item)
            return None
        if not item.getSource().getOsh():
            logger.debug('OSH is missing for %s.' % item.getSource())
            return None
        return item.getSource().getOsh()

    def reportDisk(self, disk, project):
        if not disk:
            raise ValueError("Disk is not specified")
        if not disk.getZone():
            raise ValueError("Disk's Zone not specified")
        vector = ObjectStateHolderVector()
        diskOSH = disk.build(self.__builder)
        vector.add(diskOSH)
        projectLink = modeling.createLinkOSH('composition', project.getOsh(), diskOSH)
        vector.add(projectLink)
        zoneLink = modeling.createLinkOSH('membership', disk.getZone().getOsh(), diskOSH)
        vector.add(zoneLink)
        return vector

    def reportSnapshot(self, snapshot, project):
        if not snapshot:
            raise ValueError("Snapshot is not specified")
        if not project:
            raise ValueError("Project is not specified for Snapshot")
        if not snapshot.getSource():
            raise ValueError("Disk is not specified for Snapshot")
        vector = ObjectStateHolderVector()
        snapshotOSH = snapshot.build(self.__builder)
        snapshotOSH.setContainer(project.getOsh())
        vector.add(snapshotOSH)
        return vector

    def reportImage(self, image, project):
        if not image:
            raise ValueError("Image is not specified")
        if not project:
            raise ValueError("Project is not specified for Image")
        vector = ObjectStateHolderVector()
        imageOSH = image.build(self.__builder)
        vector.add(imageOSH)
        if image.projectId == project.getId():
            link = modeling.createLinkOSH('containment', project.getOsh(), imageOSH)
            vector.add(link)
        else:
            if image.projectId:
                projectObj = google_cloud.Project(image.projectId)
                projectOSH = google_cloud.Reporter(google_cloud.Builder()).reportProject(projectObj)
                link = modeling.createLinkOSH('containment',projectOSH, imageOSH)
                vector.add(projectOSH)
                vector.add(link)
        return vector

    def linkImageToSource(self, image):
        sourceOSH = self.getSourceOsh(image)
        if sourceOSH and image.getOsh():
            return modeling.createLinkOSH('usage', image.getOsh(), sourceOSH)

    def linkDiskToSource(self, disk):
        sourceOSH = self.getSourceOsh(disk)
        if sourceOSH and disk.getOsh():
            return modeling.createLinkOSH('usage', disk.getOsh(), sourceOSH)

    def linkSnapshotToSource(self, snapshot):
        sourceOSH = self.getSourceOsh(snapshot)
        if sourceOSH and snapshot.getOsh():
            return modeling.createLinkOSH('containment', snapshot.getOsh(), sourceOSH)

    def reportInstance(self, instance, project):
        if not (project and project.getOsh()):
            raise ValueError("Project is not specified or not built")
        if not instance:
            raise ValueError("Instance is not specified")
        vector = ObjectStateHolderVector()
        nodeOSH = instance.build(self.__builder)
        vector.add(nodeOSH)
        vector.add(modeling.createLinkOSH('containment', project.getOsh(), nodeOSH))
        vector.add(modeling.createLinkOSH('membership', instance.getZone().getOsh(), nodeOSH))
        if not instance.getVmConfigOsh():
            raise ValueError("Instance Config is not specified")
        vmConfigOSH = instance.getVmConfigOsh()
        vmConfigOSH.setContainer(instance.getOsh())
        vector.add(vmConfigOSH)
        for volume in instance.Volumes:
            logger.debug('link volume %s to node' % volume)
            volumeOSH = volume.build(self.__builder)
            volumeOSH.setContainer(volume.getNode().getOsh())
            vector.add(volumeOSH)
            vector.add(modeling.createLinkOSH("usage", volumeOSH, volume.getDisk().getOsh()))
            isBootDevice = volume.getDisk().isBootDevice
            if isBootDevice:
                if volume.getDisk().getSource():
                    vector.add(modeling.createLinkOSH("usage", nodeOSH, volume.getDisk().getSource().getOsh()))
                else:
                    logger.debug('Source of %s is not found.' % volume.getDisk())

        for sa in instance.ServiceAccounts:
            saOSH = sa.build(self.__builder)
            saOSH.setContainer(project.getOsh())
            vector.add(saOSH)
            vector.add(modeling.createLinkOSH("usage", saOSH, nodeOSH))
        return vector

    def linkIpsToNode(self, instance):
        # report IPs
        vector = ObjectStateHolderVector()
        for ipAddress in instance.internalIps + instance.externalIps:
            try:
                logger.debug('link ip %s to node %s' % (ipAddress, instance))
                ipOSH = modeling.createIpOSH(ipAddress)
                if instance.getOsh():
                    vector.add(ipOSH)
                    vector.add(modeling.createLinkOSH('containment', instance.getOsh(), ipOSH))
                else:
                    logger.debug('Node OSH not created for ip: ' % ipAddress)
            except:
                logger.debugException('Failed to report ip: ', ipAddress)
        return vector



