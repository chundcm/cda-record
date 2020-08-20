# coding=utf-8
import logger
import re

import modeling
from appilog.common.system.types.vectors import ObjectStateHolderVector
from appilog.common.system.types import ObjectStateHolder

from snmp_model_finder import SnmpQueryHelper

SCRIPT_NAME = "storage_snmp_dat_domain"


class DataDomainDiscover():
    def __init__(self, client):
        self.snmpQueryHelper = SnmpQueryHelper(client)
        self.disks = []
        self.nfs = []

    def discoverer(self):
        self.discover_disks()
        self.discover_nfs()

    def discover_disks(self):
        try:
            diskModel = self.snmpQueryHelper.snmpWalk(".1.3.6.1.4.1.19746.1.6.1.1.1.4")
            diskFirmwareVersion = self.snmpQueryHelper.snmpWalk(".1.3.6.1.4.1.19746.1.6.1.1.1.5")
            diskSerialNumber = self.snmpQueryHelper.snmpWalk(".1.3.6.1.4.1.19746.1.6.1.1.1.6")
            diskCapacity = self.snmpQueryHelper.snmpWalk(".1.3.6.1.4.1.19746.1.6.1.1.1.7")

            if len(diskModel) != len(diskFirmwareVersion) or len(diskModel) != len(diskSerialNumber) or len(
                    diskModel) != len(
                diskCapacity):
                logger.warn(
                    'Snmp walk error: get different size of diskModel,  diskFirmwareVersion, diskSerialNumber and diskCapacity.'
                    ' Maybe snmp query timeout, please consider increasing timeout')
                return

            for i in range(0, len(diskModel)):
                if diskModel[i]:
                    disk = Disk()
                    disk.model = diskModel[i][1]
                    disk.firmware_version = diskFirmwareVersion[i][1]
                    disk.serial_number = diskSerialNumber[i][1]
                    disk.capacity = diskCapacity[i][1]
                    index = diskModel[i][0].split(".")
                    disk.enclosureId = index[-2]
                    disk.diskNumIndex = index[- 1]
                    logger.debug("disk %s.%s: %s || %s || %s || %s " % (
                        disk.enclosureId, disk.diskNumIndex, disk.model, disk.firmware_version, disk.serial_number,
                        disk.capacity))
                    self.disks.append(disk)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':DataDomainDiscover.discover_disks] Exception: <%s>' % excInfo)

    def discover_nfs(self):
        try:
            nfsClientPath = self.snmpQueryHelper.snmpWalk(".1.3.6.1.4.1.19746.1.9.2.1.1.2")
            nfsClientClients = self.snmpQueryHelper.snmpWalk(".1.3.6.1.4.1.19746.1.9.2.1.1.3")
            if len(nfsClientPath) != len(nfsClientClients):
                logger.warn(
                    'Snmp walk error: get different size of nfsClientPath and disk nfsClientClients.'
                    ' Maybe snmp query timeout, please consider increasing timeout')
                return
            for i in range(0, len(nfsClientPath)):
                if nfsClientPath[i]:
                    nfs_path = nfsClientPath[i][1]
                    nfs_client = nfsClientClients[i][1]
                    logger.debug("nfs %s: %s || %s" % (i, nfs_path, nfs_client))
                    self.nfs.append((nfs_path, nfs_client))
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':DataDomainDiscover.discover_nfs] Exception: <%s>' % excInfo)

    def reporter(self, container_osh):
        vector = ObjectStateHolderVector()
        for disk in self.disks:
            vector.add(disk.report(container_osh))

        for path, client in self.nfs:
            file_export_osh = ObjectStateHolder("file_system_export")
            file_export_osh.setStringAttribute('file_system_path', path)
            file_export_osh.setContainer(container_osh)
            vector.add(file_export_osh)

            client_osh = ObjectStateHolder("node")
            client_osh.setStringAttribute('name', client)
            vector.add(client_osh)
            vector.add(modeling.createLinkOSH('dependency', client_osh, file_export_osh))
        return vector


class Disk:
    def __init__(self):
        self.model = None
        self.firmware_version = None
        self.serial_number = None
        self.capacity = None
        self.enclosureId = None
        self.diskNumIndex = None

    def report(self, container_osh):
        pv_osh = ObjectStateHolder("physicalvolume")
        pv_osh.setAttribute("serial_number", self.serial_number)
        pv_osh.setAttribute("name",
                            "%s-%s-%s-%s" % (self.enclosureId, self.diskNumIndex, self.model, self.firmware_version))

        if self.capacity:
            mb = self.convertCapacityToMB()
            if mb:
                pv_osh.setAttribute("volume_size", mb)

        pv_osh.setContainer(container_osh)
        return pv_osh

    def convertCapacityToMB(self):
        results = re.match("(.*)\s+TiB", self.capacity)
        if results:
            return float(results.group(1)) * 1024 * 1024
        else:
            results = re.match("(.*)\s+GiB", self.capacity)
            if results:
                return float(results.group(1)) * 1024
