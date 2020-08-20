# coding=utf-8
import logger
import google_cloud_compute
from appilog.common.system.types.vectors import ObjectStateHolderVector


ResourceByUrl = {}

# === Compute Engine Discoverer ===
class Discoverer:
    def __init__(self, framework, gcloudClient, project):
        self.Project = project
        self.diskByUrl = {}
        self.snapshotByUrl = {}
        self.imageByUrl = {}
        self.instanceByName = {}
        self.framework = framework
        self.api = 'compute'
        self.description = 'Google Cloud Compute Engine'
        self.client = gcloudClient
        self.service = self.client.getService(self.api)

    def safeReport(self, item, fn):
        try:
            return fn(item, self.Project)
        except:
            logger.debugException('Failed to report %s.' % item)

    def report(self):
        vector = ObjectStateHolderVector()
        reporter = google_cloud_compute.Reporter(google_cloud_compute.Builder())
        for item in self.snapshotByUrl:
            object = self.snapshotByUrl[item]
            vector.addAll(self.safeReport(object, reporter.reportSnapshot))
        for item in self.imageByUrl:
            object = self.imageByUrl[item]
            vector.addAll(self.safeReport(object, reporter.reportImage))
        for item in self.diskByUrl:
            object = self.diskByUrl[item]
            vector.addAll(self.safeReport(object, reporter.reportDisk))

        for item in self.snapshotByUrl:
            vector.add(reporter.linkSnapshotToSource(self.snapshotByUrl[item]))
        for item in self.imageByUrl:
            vector.add(reporter.linkImageToSource(self.imageByUrl[item]))
        for item in self.diskByUrl:
            vector.add(reporter.linkDiskToSource(self.diskByUrl[item]))

        for item in self.instanceByName:
            object = self.instanceByName[item]
            vector.addAll(self.safeReport(object, reporter.reportInstance))
        for item in self.instanceByName:
            object = self.instanceByName[item]
            vector.addAll(reporter.linkIpsToNode(object))
        return vector

    def discover(self, GlobalResources):
        ResourceByUrl = {}
        vector = ObjectStateHolderVector()
        try:
            self.snapshotByUrl = self.getSnapshots()
        except:
            logger.debugException('Failed in list Snapshots')
        try:
            self.imageByUrl = self.getImages()
        except:
            logger.debugException('Failed in list Images')
        try:
            self.diskByUrl = self.getDisks(GlobalResources['zoneByName'])
        except:
            logger.debugException('Failed in list Disks')

        ResourceByUrl.update(self.snapshotByUrl)
        ResourceByUrl.update(self.diskByUrl)
        ResourceByUrl.update(self.imageByUrl)
        self.linkEntities(ResourceByUrl)

        try:
            self.instanceByName = self.getRunningInstances(GlobalResources['zoneByName'])
        except:
            logger.debugException('Failed in list Instances')
        return vector, None

    def linkEntities(self, ResourceByUrl):
        for url in ResourceByUrl:
            item = ResourceByUrl.get(url)
            sourceUrl = ResourceByUrl.get(url).sourceUrl
            if sourceUrl:
                if ResourceByUrl.get(sourceUrl):
                    item.setSource(ResourceByUrl.get(sourceUrl))
                else:
                    logger.debug('Source %s is not found for %s' % (sourceUrl, item))

    def getImages(self):
        imageByUrl = {}
        imageList = self.client.getListedItems(self.service, 'images')
        imageByUrl.update(self._parseImages(imageList))
        return imageByUrl

    def getImageByUrl(self, url):
        imageByUrl = {}
        imageList = [self.client.getItemByUrl(url)]
        imageByUrl.update(self._parseImages(imageList))
        return imageByUrl

    def getDisks(self, zoneByName):
        diskByUrl = {}
        for zoneName in zoneByName:
            zone = zoneByName.get(zoneName)
            diskList = self.client.getListedItems(self.service, 'disks', zone=zoneName)
            diskByUrl.update(self._parseDisks(diskList, zone))
        return diskByUrl

    def getSnapshots(self):
        snapshotByUrl = {}
        snapshotList = self.client.getListedItems(self.service, 'snapshots')
        snapshotByUrl.update(self._parseSnapshots(snapshotList))
        return snapshotByUrl

    def getRunningInstances(self, zoneByName):
        instanceByName = {}
        params = {'filter': 'status=running'}
        for zoneName in zoneByName:
            zone = zoneByName.get(zoneName)
            instanceList = self.client.getListedItems(self.service, 'instances', params=params, zone=zoneName)
            instanceByName.update(self._parseInstances(instanceList, zone))
        return instanceByName

    def _parseImages(self, imageList):
        images = {}
        for item in imageList:
            id = item.get('id')
            name = item.get('name')
            description = item.get('description')
            creationTimestamp = item.get('creationTimestamp')
            diskSizeGb = item.get('diskSizeGb')
            archiveSizeBytes = item.get('archiveSizeBytes')
            status = item.get('status')
            imageUrl = item.get('selfLink')
            sourceImageUrl = item.get('sourceImage')
            sourceDiskUrl = item.get('sourceDisk')
            selfLink = item.get('selfLink')
            try:
                projectId = selfLink.split('projects/')[1].split('/')[0]
            except:
                projectId = None
            sourceUrl = None
            if sourceImageUrl:
                sourceUrl = sourceImageUrl
            elif sourceDiskUrl:
                sourceUrl = sourceDiskUrl
            imageObj = google_cloud_compute.Image(id, name, description, creationTimestamp, diskSizeGb,
                                                  archiveSizeBytes, status, sourceUrl, projectId)
            if sourceImageUrl:
                imageObj.sourceImageUrl = sourceImageUrl
            images.setdefault(imageUrl, imageObj)
        # The source image may come from other project, so get that image into the image dict
        for image in images:
            if hasattr(image, 'sourceImageUrl'):
                if not image.sourceImageUrl in images:
                    imageByUrl = self.getImageByUrl(image.sourceImageUrl)
                    if imageByUrl:
                        images.update(imageByUrl)
        return images

    def _parseSnapshots(self, snapshotList):
        snapshotByUrl = {}
        for item in snapshotList:
            name = item.get('name')
            id = item.get('id')
            description = item.get('description')
            # sourceDiskId = item.get('sourceDiskId')
            sourceDiskUrl = item.get('sourceDisk')
            creationTimestamp = item.get('creationTimestamp')
            diskSizeGb = item.get('diskSizeGb')
            storageBytes = item.get('storageBytes')
            status = item.get('status')
            selfLink = item.get('selfLink')
            snapshotObj = google_cloud_compute.Snapshot(id, name, description, creationTimestamp, diskSizeGb,
                                                        storageBytes, status, sourceDiskUrl)
            snapshotByUrl.setdefault(selfLink, snapshotObj)
        return snapshotByUrl

    def _parseDisks(self, diskList, zone):
        diskByUrl = {}
        for item in diskList:
            id = item.get('id')
            name = item.get('name')
            creationTimestamp = item.get('creationTimestamp')
            description = item.get('description')
            sizeGb = item.get('sizeGb')
            status = item.get('status')
            diskUrl = item.get('selfLink')
            diskType = item.get('diskType')
            if diskType:
                diskType = diskType.split('/diskTypes/')[1]
            sourceImageUrl = item.get('sourceImage')
            sourceSnapshotUrl = item.get('sourceSnapshot')
            sourceUrl = None
            if sourceImageUrl:
                sourceUrl = sourceImageUrl
                if not sourceImageUrl in self.imageByUrl:
                    imageByUrl = self.getImageByUrl(sourceImageUrl)
                    if imageByUrl:
                        self.imageByUrl.update(imageByUrl)
            elif sourceSnapshotUrl:
                sourceUrl = sourceSnapshotUrl

            diskObj = google_cloud_compute.Disk(id, name, creationTimestamp, description, diskType, sizeGb, status, zone, sourceUrl)
            diskByUrl.setdefault(diskUrl, diskObj)
        return diskByUrl

    def _parseInstances(self, instanceList, zone):
        instanceByName = {}
        for item in instanceList:
            id = item.get('id')
            name = item.get('name')
            creationTimestamp = item.get('creationTimestamp')
            description = item.get('description')
            cpuPlatform = item.get('cpuPlatform')
            machineType = item.get('machineType')
            if machineType:
                machineType = machineType.split('/machineTypes/')[1]
            status = item.get('status')
            networkInterfaces = item.get('networkInterfaces')
            internalIps = []
            externalIps = []
            for netIf in networkInterfaces:
                internalIps.append(netIf.get('networkIP'))
                if netIf.get('accessConfigs'):
                    for config in netIf.get('accessConfigs'):
                        if config.get('natIP'):
                            externalIps.append(config.get('natIP'))
            internalIps.sort()
            externalIps.sort()
            instanceObj = google_cloud_compute.Instance(id, name, zone, creationTimestamp, description, cpuPlatform,
                                                        machineType, status, internalIps, externalIps)
            serviceAccounts = item.get('serviceAccounts')
            for sa in serviceAccounts:
                email = sa.get('email')
                serviceAccount = google_cloud_compute.ServiceAccount(email)
                instanceObj.ServiceAccounts.append(serviceAccount)
            disks = item.get('disks')
            for disk in disks:
                boot = disk.get('boot')
                autoDelete = disk.get('autoDelete')
                selfUrl = disk.get('source')
                mode = disk.get('mode')
                interface = disk.get('interface')
                deviceName = disk.get('deviceName')
                os_licenses = disk.get('licenses')
                os_type = ''
                if os_licenses:
                    os_type = os_licenses[0].split('/projects/')[1]
                if self.diskByUrl.get(selfUrl):
                    diskObj = self.diskByUrl.get(selfUrl)
                    diskObj.isBootDevice = boot
                    diskObj.isAutoDelete = autoDelete
                    diskObj.mode = mode
                    diskObj.interface = interface
                    volumeObj = google_cloud_compute.Volume(diskObj.getId(), diskObj.getName(), diskObj.sizeGb, diskObj, instanceObj)
                    instanceObj.Volumes.append(volumeObj)
                else:
                    logger.debug('Disk %s is not found for instance: %s' % (deviceName, instanceObj.getName()))
                instanceObj.os_type = instanceObj.set_os_type(os_type)
            instanceByName.setdefault(name, instanceObj)
        return instanceByName

