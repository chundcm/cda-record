#coding=utf-8
import modeling
import string
import re

import logger

from appilog.common.system.types.vectors import ObjectStateHolderVector
from appilog.common.system.types import ObjectStateHolder

from org.jdom.input import SAXBuilder
from java.io import StringReader

import NTCMD_HR_Dis_Disk_Lib
import dns_resolver
import host_base_parser
import host_topology


def _kbToMb(size_in_kb):
    return round(float(size_in_kb) / 1024)


class DiskTopologyBuilder:
    def __init__(self, containerOsh, shell):
        self.resultVector = ObjectStateHolderVector()
        self.containerOsh = containerOsh
        self.remoteHosts = {}
        self.shell = shell
        self.mountPointToDisk = {}

    def handleDiskRow(self, fileSystem, mountedOn, size, usedSize=None, fileSystemType=None):
        '''
        @param usedSize: disk used size in 1K-blocks
        @param size: disk size in 1K-blocks
        '''
        if mountedOn in self.mountPointToDisk:
            logger.reportWarning('File system object already reported for the mount point; skipping new one')
            logger.warn("File system object already reported for the mount point '%s'; skipping new one (mount point: '%s'; file system: '%s')" % (mountedOn, mountedOn, fileSystem))
            return
        if str(size).isdigit():
            sizeInMb = _kbToMb(size)
        else:
            sizeInMb = None
        if str(usedSize).isdigit():
            usedSizeInMb = _kbToMb(usedSize)
        else:
            usedSizeInMb = None
        type_ = modeling.UNKNOWN_STORAGE_TYPE
        diskOsh = modeling.createDiskOSH(self.containerOsh, mountedOn, type_, size=sizeInMb,
                                         name=fileSystem, usedSize=usedSizeInMb, fileSystemType=fileSystemType)
        if diskOsh:
            self.mountPointToDisk[mountedOn] = diskOsh
            self.resultVector.add(diskOsh)

            host_reporter = host_topology.Reporter()
            resolver = dns_resolver.create(shell=self.shell)
            try:
                (remoteHost, remoteMountPoint) = getRemoteHostAndMountPoint(fileSystem)
                if remoteHost and remoteMountPoint:
                    if remoteHost.startswith('[') and remoteHost.endswith(']'):
                        remoteHost = remoteHost[1:-1]
                    host_osh = self.remoteHosts.get(remoteHost)
                    if not host_osh:
                        host = host_base_parser.parse_from_address(remoteHost, resolver.resolve_ips)
                        #do not report hostname as it may be alias
                        host_osh, _, oshs = host_reporter.report_host_with_ips(host.ips)
                        self.remoteHosts[remoteHost] = host_osh
                        self.resultVector.addAll(oshs)

                    remoteShareOsh = ObjectStateHolder('networkshare')
                    remoteShareOsh.setContainer(host_osh)
                    remoteShareOsh.setStringAttribute('data_name', remoteMountPoint)
                    remoteShareOsh.setStringAttribute('share_path', remoteMountPoint)
                    self.resultVector.add(remoteShareOsh)

                    self.resultVector.add(modeling.createLinkOSH('realization', remoteShareOsh, diskOsh))
            except:
                stackTrace = logger.prepareFullStackTrace('Failed to link disk to the remote share.')
                logger.warn(stackTrace)

    def getResultVector(self):
        return self.resultVector



def getRemoteHostAndMountPoint(fileSystem):
    matcher = re.match('(.*?):(/.*)', fileSystem)
    if matcher:
        remoteHost = matcher.group(1)
        remoteMountPoint = matcher.group(2)
        return (remoteHost, remoteMountPoint)
    else:
        return (None, None)


def getFilesystemData(command, client):
    output = client.execCmd(command)  # V@@CMD_PERMISION tty protocol execution
    lines = []
    if output:
        lines = output.split('\n')
        lines = [line.strip() for line in lines if __isValidLine(line)]
    else:
        logger.warn('Failed to obtain disks information')
    return lines


def __isValidLine(line):
    return line and line.strip() and line.find('/net') != 0 and line.find('/mnt') != 0 and not re.match('Filesystem\s+', line)


def disHPUX(hostOsh, client):
    lines = getFilesystemData('df -P', client)  # V@@CMD_PERMISION tty protocol execution

    fileSystem = None
    mountedOn = None
    diskSize = None

    topologyBuilder = DiskTopologyBuilder(hostOsh, client)
    for line in lines:
        token = line.split()
        if len(token) == 6 and token[4].find('%'):
            fileSystem = token[0]
            diskSize = __parseHPUXDiskSize(token[1])
            used = __parseHPUXDiskSize(token[2])
            # token[3] - available disk space
            # token[4] - use %
            mountedOn = token[5]
            fileSystem_type = getHPUXFileSysInfo(fileSystem, client)
            topologyBuilder.handleDiskRow(fileSystem, mountedOn, diskSize, usedSize=used, fileSystemType=fileSystem_type)
        elif len(token) == 5 and token[3].find('%'):
            fileSystem = fileSystem or token[4]
            mountedOn = token[4]
            diskSize = __parseHPUXDiskSize(token[0])
            fileSystem_type = getHPUXFileSysInfo(fileSystem, client)
            topologyBuilder.handleDiskRow(fileSystem, mountedOn, diskSize, fileSystemType=fileSystem_type)
        elif len(token) == 1:
            fileSystem = token[0]

    return topologyBuilder.getResultVector()


def getHPUXFileSysInfo(filesystem, client):
    fs_cmd = 'cat /etc/fstab | grep ' + filesystem + ' | awk \'{print $3}\''
    result = client.execCmd(fs_cmd)
    if result and len([x for x in result.strip().splitlines() if x]) == 1:
        return result and result.strip()
    else:
        logger.debug('Returned more than one value, looks like a name overlap')
        types = [x for x in result.strip().splitlines() if x]

        names_cmd = 'cat /etc/fstab | grep ' + filesystem + ' | awk \'{print $1}\''
        names_str = client.execCmd(names_cmd)
        names = [x.strip() for x in names_str.strip().splitlines() if x and x.strip()]
        for i in xrange(len(names)):
            if filesystem == names[i]:
                logger.debug('FS Type is detected: %s' % types[i])
                return types[i]


def __parseHPUXDiskSize(hpuxDiskSize):
    '''
    @param hpuxDiskSize: disk size in 512B blocks
    '''
    if str(hpuxDiskSize).isdigit():
        return str(string.atol(hpuxDiskSize) / 2)


def _calcUsedSize(diskSize, freeSize):
    if str(diskSize).isdigit():
        diskSize = int(diskSize)
    else:
        diskSize = None
    freeDiskSize = freeSize
    if str(freeDiskSize).isdigit():
        freeDiskSize = int(freeDiskSize)
    else:
        diskSize = None
    usedDiskSize = None
    if diskSize is not None and freeDiskSize is not None:
        usedDiskSize = diskSize - freeDiskSize
    return diskSize, usedDiskSize


def _parseDfOutput(topologyBuilder, lines, getFileSystemType=None, client=None):
    fileSystem = None
    file_system_type = None
    for line in lines:
        token = line.split()
        if re.search('Permission denied', line):
            continue
        if re.search('none', token[0]):
            continue
        if len(token) == 6:
            fileSystem = token[0]
            diskSize = token[1]
            diskSizeUsed = token[2]
            mountedOn = token[5]
            if fileSystem == 'swap':
                diskSize = '0'
            if getFileSystemType:
                file_system_type = getFileSystemType(fileSystem, client, mountedOn)
            topologyBuilder.handleDiskRow(fileSystem, mountedOn, diskSize, usedSize=diskSizeUsed, fileSystemType=file_system_type)
        # token could be split into
        elif len(token) == 1:
            # filesystem on one line
            fileSystem = token[0]
        elif len(token) == 5:
            # and the rest on the other
            fileSystem = fileSystem or token[4]
            diskSize = token[0]
            diskSizeUsed = token[1]
            mountedOn = token[4]
            if getFileSystemType:
                file_system_type = getFileSystemType(fileSystem, client, mountedOn)
            topologyBuilder.handleDiskRow(fileSystem, mountedOn, diskSize, usedSize=diskSizeUsed, fileSystemType=file_system_type)
    return topologyBuilder.getResultVector()


def disAIX(hostOsh, client):
    lines = getFilesystemData('df -k', client)  # V@@CMD_PERMISION tty protocol execution
    topologyBuilder = DiskTopologyBuilder(hostOsh, client)
    for line in lines:
        try:
            token = line.split()
            fileSystem = token[0]
            diskSize = token[1]
            freeDiskSize = token[2]
            (diskSize, usedDiskSize) = _calcUsedSize(diskSize, freeDiskSize)
            mountedOn = token[6]
            fileSystem_type = getAIXFileSysType(fileSystem, client)
            topologyBuilder.handleDiskRow(fileSystem, mountedOn, diskSize, usedSize=usedDiskSize, fileSystemType=fileSystem_type)
        except:
            logger.debugException('')
    return topologyBuilder.getResultVector()


def getAIXFileSysType(filesystem, client):
    fst_cmd = 'lsfs -l ' + filesystem + ' | grep / | awk \'{print $4}\''
    output = client.execCmd(fst_cmd)
    match = re.search('(No record matching)', output, re.IGNORECASE)
    match_permission = re.search('(The file access permissions do not allow the specified action)', output, re.IGNORECASE | re.MULTILINE)
    if re.search('(syntax error)', output, re.IGNORECASE | re.MULTILINE):
        fst_cmd = 'lsfs -l ' + filesystem + ' | grep / | awk \'{print \$4}\''
        output = client.execCmd(fst_cmd)
        match = re.search('(Cannot find or open file system)', output, re.IGNORECASE)
        match_permission = re.search('(The file access permissions do not allow the specified action)', output, re.IGNORECASE | re.MULTILINE)
    if match:
        logger.error('Failed to find file system type for %s' % filesystem)
        return None
    if match_permission:
        logger.error('Permission denied to get file system type for %s' % filesystem)
        return None
    else:
        return output


def disFreeBSD(hostOsh, client):
    topologyBuilder = DiskTopologyBuilder(hostOsh, client)
    lines = getFilesystemData('df -k', client)  # V@@CMD_PERMISION tty protocol execution
    for line in lines:
        token = line.split()
        fileSystem = token[0]
        if re.search('procfs', fileSystem):
            continue
        diskSize = token[1]
        usedSize = token[2]
        mountedOn = token[5]
        topologyBuilder.handleDiskRow(fileSystem, mountedOn, diskSize, usedSize=usedSize)
    return topologyBuilder.getResultVector()


def disLinux(hostOsh, client):
    topologyBuilder = DiskTopologyBuilder(hostOsh, client)
    lines = getFilesystemData('df -k', client)  # V@@CMD_PERMISION tty protocol execution
    return _parseDfOutput(topologyBuilder, lines, getLinuxFSType, client)


def getLinuxFSType(filesystem, client, mounton=None):
    lines = executeLinuxFSTypeCommand(filesystem, client)
    if lines and len(lines) > 1 and mounton:
        lines = executeLinuxFSTypeCommand(mounton, client)
    if lines and len(lines) > 1:
        fs_type = lines[0].strip().split()
        if len(fs_type) > 1:
            return fs_type[1]
        return lines[0].strip()


def executeLinuxFSTypeCommand(key, client):
    fst_cmd = 'df -Th | grep ' + key + ' | awk \'{print $2}\''
    output = client.execCmd(fst_cmd)
    if output:
        lines = output.split('\n')
        return [line.strip() for line in lines if (line and line.strip() and line.find('df:') != 0 and not re.match('Filesystem\s+', line))]


def disSunOS(hostOsh, client):
    topologyBuilder = DiskTopologyBuilder(hostOsh, client)
    lines = getFilesystemData('df -k | awk \'{print $1,$2,$3,$4,$5,$6}\'', client)  # V@@CMD_PERMISION tty protocol execution
    return _parseDfOutput(topologyBuilder, lines, getSUNFSType, client)


def getSUNFSType(filesystem, client, mounton=None):
    fst_cmd = 'grep -i -w ' + filesystem + ' /etc/vfstab | awk \'{print $4}\''
    output = client.execCmd(fst_cmd)
    if output:
        match = re.search('(No record matching|No record matching)', output)
        if match:
            logger.error("Failed to find file System Type for %s" % filesystem)
        else:
            return output


def disVMKernel(hostOsh, client, Framework=None, langBund=None):
    topologyBuilder = DiskTopologyBuilder(hostOsh, client)
    xml = client.execCmd('esxcfg-info -F xml | sed -n \'/<vmfs-filesystems>/,/<\/vmfs-filesystems>/p\' | sed -n \'1,/<\/vmfs-filesystems>/p\'')
    #Cleanup retrieved xml. Sometimes there is some debug info added
    xml = xml[xml.find('<'): xml.rfind('>') + 1]

    builder = SAXBuilder(0)
    document = builder.build(StringReader(xml))
    rootElement = document.getRootElement()
    vm_filesystems = rootElement.getChildren('vm-filesystem')
    for vm_filesystem in vm_filesystems:
        mountPoint = ''
        size = ''
        fileSystem = ''
        usage = None

        vmfs_values = vm_filesystem.getChildren('value')
        for value in vmfs_values:
            if value.getAttributeValue('name') == 'console-path':
                mountPoint = value.getText()
            elif value.getAttributeValue('name') == 'size':
                size = value.getText()
            elif value.getAttributeValue('name') == 'usage':
                usage = value.getText()

        dlp_values = vm_filesystem.getChild('extents').getChild('disk-lun-partition').getChildren('value')
        for value in dlp_values:
            if value.getAttributeValue('name') == 'console-device':
                fileSystem = value.getText()
        topologyBuilder.handleDiskRow(fileSystem, mountPoint, size, usage)
    return topologyBuilder.getResultVector()


def disWinOS(hostOsh, shell):
    resultVector = ObjectStateHolderVector()
    if not NTCMD_HR_Dis_Disk_Lib.discoverDiskByWmic(shell, resultVector, hostOsh):
        NTCMD_HR_Dis_Disk_Lib.discoverDisk(shell, resultVector, hostOsh)
    NTCMD_HR_Dis_Disk_Lib.discoverPhysicalDiskByWmi(shell, resultVector, hostOsh)
    return resultVector

def disWinOSiSCSIInfo(hostOsh, shell):
    resultVector = ObjectStateHolderVector()
    NTCMD_HR_Dis_Disk_Lib.discoveriSCSIInfo(shell, resultVector, hostOsh)
    return resultVector