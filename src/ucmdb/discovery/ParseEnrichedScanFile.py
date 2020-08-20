# coding=utf-8
import os
import re
import sys
import time
from stat import *

# Since the enriched xml files define the encoding UTF-8,
# need to switch from the default encoding of python ASCII to UTF-8.
reload(sys)
sys.setdefaultencoding('UTF-8')

import logger

import InventoryUtils
import inventoryerrorcodes
import netutils
import modeling
import ip_addr
import xmlutils
import md5
import ldom

# java natural
from java.io import File
from java.io import FileInputStream
from java.util.zip import GZIPInputStream
from java.lang import Boolean
from java.lang import Exception
from java.text import SimpleDateFormat
from java.util import Date
from java.net import InetAddress

# xml related

# ucmdb
from appilog.common.system.types.vectors import ObjectStateHolderVector
from appilog.common.system.types import ObjectStateHolder
from com.hp.ucmdb.discovery.probe.agents.probemgr.workflow.state import WorkflowStepStatus
from com.hp.ucmdb.discovery.probe.agents.probemgr.xmlenricher import XmlEnricherConstants
from com.hp.ucmdb.discovery.common.scanner.config import ScannerConfigurationUtil
from com.hp.ucmdb.discovery.common.mapping.config import MappingConfigurationUtil

# for scan file mapping
from javax.xml.xpath import XPathConstants
from javax.xml.xpath import XPathFactory
from appilog.common.system.types import AttributeStateHolder
from com.hp.ucmdb.discovery.library.common import CollectorsParameters
from com.hp.ucmdb.discovery.common.scanfilemapping.config import CI
from com.hp.ucmdb.discovery.common.scanfilemapping.config import MappingConfig
from appilog.common.system.types.classmodel import CITRoot
from appilog.common.system.types.classmodel import CITComposition
import host_discoverer
import process as process_module
import process_discoverer
import process_to_process
import Dis_TCP
import applications
from cmdlineutils import CmdLine
from networking_win import InterfaceRoleManager
import TTY_Connection_Utils
from vendors import PlatformVendors

RECOGNITION_LEVEL_RAW = 'raw'
RECOGNITION_LEVEL_NORMALIZED = 'normalized'
RECOGNITION_LEVEL_PARTIAL = 'partially_recognized'
RECOGNIZED_BY_SAI = 'SAI'
RECOGNIZED_BY_BDNA = 'BDNA'


class ProcessorFamily:
    X86_32 = "x86_32"
    X86_64 = "x86_64"
    IA64 = "ia64"
    SPARC = "sparc"
    PowerPC = "power_pc"
    PA_RISC = "pa_risc"


SOFTWARE_LICENSE_TYPES = {
    "Unknown": 0,
    "Free": 1,
    "Commercial": 2
}


class OsArchitecture:
    X86 = "32-bit"
    X64 = "64-bit"
    IA64 = "ia64"


class VSEdition:
    ent = "Enterprise"
    pro = "Professional"
    std = "Standard"
    pre = "Premium"
    ult = "Ultimate"
    community = "Community"
    express = "Express"


X86_PLATFORMS = ["i686", "x86", ProcessorFamily.X86_32]
X86_64_PLATFORMS = ["amd64", ProcessorFamily.X86_64]
PRIMARY_IP_ADDRESS_FLAG = "Primary"

SHORT_HOST_NAME_PATTERN = "(.+?)\\."
SHORT_OSINSTALLTYPE_PATTERN = "(.*) Edition.*"
KEY_DOMAIN = "domain"
KEY_DOMAIN_CONF = "config"
KEY_SWITCH_NIC = "switch_nic"
KEY_SWITCH = "switch"
KEY_VIRTUAL_VOLUME = "virtual_volume"


def StepMain(Framework):
    InventoryUtils.executeStep(Framework, processEnrichedScanFile, InventoryUtils.STEP_DOESNOT_REQUIRES_CONNECTION,
                               InventoryUtils.STEP_DOESNOT_REQUIRES_LOCK)


def processEnrichedScanFile(Framework):
    # get the enriched full file name
    localScanFileFolderPath = CollectorsParameters.PROBE_MGR_INVENTORY_XMLENRICHER_FILES_FOLDER + XmlEnricherConstants.SENDING_FOLDER_NAME + File.separator
    localScanFileName = InventoryUtils.generateScanFileName(Framework)
    finalScanFileNameOnLocalMachine = localScanFileFolderPath + localScanFileName
    logger.debug('full generated enriched scan file path: ' + finalScanFileNameOnLocalMachine)
    parseFile(Framework, finalScanFileNameOnLocalMachine)


def parseFile(Framework, filePath, isManual=None, reportWarning=None):
    ################### for delete afterwards - start
    # finalScanFileNameOnLocalMachine = "C:\work\DDMX\scanFile18mb.xsf"
    ################### for delete afterwards - end

    if not isEnrichedScanFileReady(filePath):
        Framework.reportError(inventoryerrorcodes.INVENTORY_DISCOVERY_ENRICHED_SCANFILE_NOTREADY, [filePath])
        Framework.setStepExecutionStatus(WorkflowStepStatus.FAILURE)
        return
    else:
        fis = None
        _input = None
        try:
            try:
                fis = FileInputStream(filePath)
                _input = GZIPInputStream(fis)
                vector = domParse(_input, Framework, isManual, reportWarning, filePath)
                if vector and vector.size() > 0:
                    # sending results
                    logger.debug("Sending objects...")
                    Framework.sendObjects(vector)
                    Framework.flushObjects()
                    logger.debug("Finished sending objects...")

                    Framework.setStepExecutionStatus(WorkflowStepStatus.SUCCESS)
                else:
                    Framework.setStepExecutionStatus(WorkflowStepStatus.FAILURE)
            except:
                # send error info to framework
                errorMessage = str(sys.exc_info()[1])
                logger.debugException(errorMessage)
                if reportWarning:
                    Framework.reportWarning(inventoryerrorcodes.INVENTORY_DISCOVERY_FAILED_EXECUTE_STEP,
                                            [Framework.getState().getCurrentStepName(), errorMessage])
                else:
                    Framework.reportError(inventoryerrorcodes.INVENTORY_DISCOVERY_FAILED_EXECUTE_STEP,
                                          [Framework.getState().getCurrentStepName(), errorMessage])
                    Framework.setStepExecutionStatus(WorkflowStepStatus.FATAL_FAILURE)
        finally:
            if fis:
                fis.close()
            if _input:
                _input.close()

        # remove xsf file from storage
        if File(filePath).delete():
            logger.debug("Downloadable scan file [" + filePath + "] was deleted successfully")
        else:
            logger.debug("Failed to delete downloadable xsf file[" + filePath + "]")


def isEnrichedScanFileReady(enrichedScanFileName):
    logger.debug('Checking for existence of enriched scan file: ' + enrichedScanFileName)
    enrichedScanFile = File(enrichedScanFileName)
    if enrichedScanFile.exists():
        return 1
    logger.debug('Can not find enriched scan file.')
    return 0


def domParse(_input, Framework, isManual, reportWarning, filePath):
    try:
        dbf = xmlutils.getXmlSecurityUtil().getDocumentBuilderFactoryInstance()
        db = dbf.newDocumentBuilder()
        doc = db.parse(_input)
        OSHVResult = ObjectStateHolderVector()
        rootNode = doc.getElementsByTagName("inventory").item(0)
        errors = getNodeValues("error", rootNode)[0]
        if len(errors):
            logger.error(errors)
            if reportWarning:
                Framework.reportWarning(inventoryerrorcodes.INVENTORY_DISCOVERY_FAILED_PARSING,
                                        [errors + ": " + filePath])
            else:
                Framework.reportError(inventoryerrorcodes.INVENTORY_DISCOVERY_FAILED_PARSING,
                                      [errors + ": " + filePath])
                Framework.setStepExecutionStatus(WorkflowStepStatus.FATAL_FAILURE)
            return OSHVResult

        mappingConfigValue = Framework.getParameter('MappingConfiguration')
        mappingConfig = MappingConfigurationUtil.loadMappingConfiguration(mappingConfigValue)
        # Node entity mapping
        # create OSH(Node)
        initScanFileMappingConfig()
        nodeOSH = createNodeOSH(Framework, OSHVResult, rootNode)
        if not isManual:
            uduid = Framework.getProperty(InventoryUtils.ATTR_UD_UNIQUE_ID)
            logger.debug("Will set uduid if not empty to node:", uduid)
            if uduid:
                nodeOSH.setStringAttribute(InventoryUtils.ATTR_UD_UNIQUE_ID, uduid)

        logger.debug("Node OSH created!")
        # create Cpu osh
        createCpuOSH(OSHVResult, rootNode, nodeOSH)
        logger.debug("Cpu OSH created!")
        createOSVM(OSHVResult, rootNode, nodeOSH, Framework)
        nicWithDeviceID = createHardwareBoardOSH(OSHVResult, rootNode, nodeOSH, Framework)
        logger.debug("HardwareBoard OSH created!")
        createDisplayMonitorOSH(OSHVResult, rootNode, nodeOSH)
        logger.debug("DisplayMonitor OSH created!")
        # create PrinterDriver osh
        createPrinterDriverOSH(OSHVResult, rootNode, nodeOSH)
        logger.debug("Printer DriverOSH created!")
        createUSBDeviceOSH(OSHVResult, rootNode, nodeOSH)
        logger.debug("USB DeviceOSH created!")
        # create iSCSI OSH
        physicalVolumeOshMap = createiSCSIOSH(OSHVResult, rootNode, nodeOSH)
        logger.debug("iSCSI OSH created!")
        # create DiskDevice osh
        createDiskOSH(OSHVResult, rootNode, nodeOSH)
        logger.debug("DiskDevice OSH created!")
        # create WindowsDeviceDriver OSH
        driverWithDeviceID = createWindiwsDeviceDriverOSH(OSHVResult, rootNode, nodeOSH)
        logger.debug("Windows Device Driver OSH created!")
        # link NIC and driver
        if driverWithDeviceID and nicWithDeviceID:
            for deviceIDKeyInDriver in driverWithDeviceID:
                for deviceIDKeyInNic in nicWithDeviceID:
                    if deviceIDKeyInDriver == deviceIDKeyInNic:
                        NicDriverLink = modeling.createLinkOSH("usage", nicWithDeviceID[deviceIDKeyInNic],
                                                               driverWithDeviceID[deviceIDKeyInDriver])
                        OSHVResult.add(NicDriverLink)
        # create FileSystem OSH
        createFsOSH(OSHVResult, rootNode, nodeOSH, physicalVolumeOshMap)
        logger.debug("FileSystem OSH created!")
        # create MemoryUnit osh
        createMMUOSH(OSHVResult, rootNode, nodeOSH)
        logger.debug("MemoryUnit osh created!")
        # create FileSystemExport osh
        createFseOSH(OSHVResult, rootNode, nodeOSH)
        logger.debug("FileSystemExport osh created!")
        if isWindows(rootNode):
            createWinUserOSH(OSHVResult, rootNode, nodeOSH)
            logger.debug("Windows User osh created!")
            if mappingConfig.services:
                createWindowsServiceOSH(OSHVResult, rootNode, nodeOSH)
                logger.debug("Windows Service osh created!")
        else:
            createOsUserOSH(OSHVResult, rootNode, nodeOSH)
            logger.debug("Os User osh created!")
            if mappingConfig.services:
                createDaemonOSH(OSHVResult, rootNode, nodeOSH)
                logger.debug("Daemon osh created!")
        # software mapping
        mapInstalledSoftware(OSHVResult, rootNode, nodeOSH, mappingConfig)
        logger.debug("InstalledSoftware OSH created!")
        mapRunningProcess(OSHVResult, rootNode, nodeOSH, Framework, isManual)
        logger.debug("Running software OSH created!")
        # inventory scanner mapping
        createScannerOSH(Framework, OSHVResult, rootNode, nodeOSH, filePath)
        logger.debug("InventoryScanner OSH created!")
        # create configuration Document
        if mappingConfig.configDocument:
            configurationStr = mapConfigurations(rootNode)
            cdOsh = modeling.createConfigurationDocumentOSH('NodeConfig.ini', 'NA', configurationStr, nodeOSH,
                                                            modeling.MIME_TEXT_PLAIN)
            OSHVResult.add(cdOsh)
            logger.debug("ConfigurationDocument OSH created!")
        createMSCluster(OSHVResult, rootNode)
        mapNewCI(OSHVResult, rootNode, nodeOSH)
        return OSHVResult
    except:
        logger.error("Failed parsing scan file...")
        errorMessage = str(sys.exc_info()[1])
        Framework.reportError(inventoryerrorcodes.INVENTORY_DISCOVERY_FAILED_PARSING, [errorMessage])
        logger.debugException(errorMessage)
    return None


# parse the value of a node
def getNodeValues(tagName, element, defaultValue=['']):
    nodeList = element.getElementsByTagName(tagName)
    values = []
    nodes = nodeListToArray(nodeList)
    for node in nodes:
        if node.getFirstChild():
            values.append(node.getFirstChild().getNodeValue().strip())
    if not len(values):
        values = defaultValue
    return values


def getNodeAttribute(node, attributeName, defaultValue=''):
    value = node.getAttribute(attributeName).strip()
    if not len(value):
        value = defaultValue
    return value


def existNodeAttribute(node, attributeName):
    return node.hasAttribute(attributeName)


# elements that are of enum type are in the structure like:
# <tagName type="attrib" value="4">enum interpretation</tagName>
def getNodeEnumAttribute(parentNode, tagName, defaultValue='0'):
    nodeList = parentNode.getElementsByTagName(tagName)
    if nodeList.getLength():
        nodeArray = nodeListToArray(nodeList)
        for node in nodeArray:
            if node.getTextContent() and node.getAttributes().getNamedItem("value").getNodeValue() is not None:
                return node.getAttributes().getNamedItem("value").getNodeValue()
    return defaultValue


def sumSwapFileSize(fileSizeArray):
    _sum = 0
    for fileSize in fileSizeArray:
        if fileSize:
            _sum += int(fileSize)
    return _sum


def createNodeOSH(Framework, oshvresults, root):
    osArchitecture = None
    if isWindows(root):
        nodeOsh = ObjectStateHolder("nt")
        modeling.setHostOsFamily(nodeOsh, osFamily=None, osTypeOrClassName="nt")
        mapStringAttribute(nodeOsh, "nt_registeredowner", "hwOSDefaultUserName", root)
        mapStringAttribute(nodeOsh, "nt_registrationorg", "hwOSDefaultOrganisationName", root)
        groupName = getNodeValues("hwWorkgroupName", root)[0]
        if groupName:
            nodeOsh.setStringAttribute("nt_workgroup", groupName)
        servicePackNo = getNodeValues("hwOSServiceLevel", root)[0]
        matcher = re.search('^Service\s+Pack\s+([\d]+)', servicePackNo, re.I)
        if matcher:
            servicePackNo = str(float(matcher.group(1)))
        nodeOsh.setStringAttribute("nt_servicepack", servicePackNo)
        osArchitecture = mapOsArchitecture(root)
    elif isUnix(root) or isMac(root):
        nodeOsh = ObjectStateHolder("unix")
        modeling.setHostOsFamily(nodeOsh, osFamily=None, osTypeOrClassName="unix")
        osArchitecture = mapOsArchitecture(root)
    else:
        nodeOsh = ObjectStateHolder("node")

    udUuid = Framework.getTriggerCIData('nodeGUID')
    if udUuid and udUuid.upper() != "NA" and udUuid.find('-') > 0:
        nodeOsh.setStringAttribute("ud_unique_id", udUuid)

    if osArchitecture:
        nodeOsh.setStringAttribute("os_architecture", osArchitecture)
    nodeOsh.setIntegerAttribute("memory_size", getNodeValues("hwMemTotalMB", root, '0')[0])
    nodeOsh.setIntegerAttribute("swap_memory_size", sumSwapFileSize(getNodeValues("hwMemSwapFileSize", root, [0])))
    osName = mapOsName(root)
    if osName:
        nodeOsh.setStringAttribute("discovered_os_name", osName)
    osVendor = mapOsVendor(root)
    if osVendor:
        nodeOsh.setStringAttribute("discovered_os_vendor", osVendor)
    osVer = mapOsVer(root)
    if osVer:
        nodeOsh.setStringAttribute("discovered_os_version", osVer)
    # for osinstalltype, 'Edition.*' should be excluded to resolve data flipping issue
    osinstalltype = mapOsType(root)
    if osinstalltype:
        ostype_rs = re.search(SHORT_OSINSTALLTYPE_PATTERN, osinstalltype)
        if ostype_rs:
            osinstalltype = ostype_rs.group(1)
        nodeOsh.setStringAttribute("host_osinstalltype", osinstalltype)
    osrelease = mapOsRelease(root)
    if osrelease:
        nodeOsh.setStringAttribute("host_osrelease", osrelease)

    biosAssetTag = getNodeValues("hwsmbiosAssetTagNumber", root)[0]
    if host_discoverer.isBiosAssetTagValid(biosAssetTag):
        biosAssetTag = biosAssetTag.strip()
        nodeOsh.setStringAttribute("bios_asset_tag", biosAssetTag)

    mapStringAttribute(nodeOsh, "bios_source", "hwBiosSource", root)

    # set bios info
    if isWindows(root):
        biosVer = getNodeValues("hwBiosVersion", root)[0]
        biosDate = getNodeValues("hwsmbiosBIOSDate", root)[0]
    else:
        biosDate = getNodeValues("hwBiosDate", root)[0]
        if not biosDate:
            biosDate = getNodeValues("hwsmbiosBIOSDate", root)[0]
        biosVer = getNodeValues("hwsmbiosBIOSVersion", root)[0]
        if not biosVer:
            # hpux, aix, solaris get the firmware info as bios. ( solaris sparc get the OBP info as bios)
            biosVer = getNodeValues("hwBiosSource", root)[0]

    if "OBP" in biosVer:
        OBPInfo = biosVer.split()
        biosVer = OBPInfo[1]
        biosDate = OBPInfo[2]
        sfDate = SimpleDateFormat('yyyy/MM/dd')
    else:
        sfDate = SimpleDateFormat('yyyy-MM-dd')

    if biosVer:
        nodeOsh.setStringAttribute("bios_version", biosVer)
    if biosDate:
        bios_date = sfDate.parse(biosDate)
        nodeOsh.setDateAttribute('bios_date', bios_date)

    # set os install date
    os_install_date = getNodeValues("hwOSInstallDate", root)[0]
    os_installdate = None
    try:
        if isWindows(root):
            sfDate = SimpleDateFormat('yyyyMMddHHmmss')
            if os_install_date:
                os_installdate = sfDate.parse(str(os_install_date))
        elif isLinux(root):
            result = re.match("\s*(\d{4}\-\d{2}-\d{2} \d{2}\:\d{2}\:\d{2}).*", os_install_date)
            if result:
                sfDate = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                date = result.group(1)
                os_installdate = sfDate.parse(str(date))
            else:
                result_list = os_install_date.split(' ')
                if len(result_list) >= 4:
                    if 'PM' in os_install_date:
                        temp = int(result_list[4].split(':')[0]) + 12
                        time = str(temp) + result_list[4][2:]
                    else:
                        time = result_list[4]
                    sfDate = SimpleDateFormat("dd MMM yyyy HH:mm:ss")
                    date_str = result_list[1] + ' ' + result_list[2] + ' ' + result_list[3] + ' ' + time
                    os_installdate = sfDate.parse(str(date_str))
        if os_installdate:
            nodeOsh.setDateAttribute("os_installed_date", os_installdate)
    except Exception, e:
        logger.warn('Cannot discover the OS Installed Date :', str(e))

    nodeOsh.setEnumAttribute("chassis_type", int(getNodeEnumAttribute(root, "hwsmbiosChassisType", '2')))
    biosUUID = getNodeValues("hwsmbiosSystemUUID", root)[0]
    if biosUUID:
        if len(biosUUID) == 32:
            convertToMicrosoftStandart = InventoryUtils.getGlobalSetting().getPropertyStringValue(
                'setBiosUuidToMicrosoftStandart', 'false')
            if convertToMicrosoftStandart.lower() == 'true':
                # convert uuid to MS standard which the first 16 bytes are encoding in little endian
                formattedUUID = biosUUID[6:8] + biosUUID[4:6] + biosUUID[2:4] + biosUUID[0:2]
                formattedUUID = formattedUUID + "-" + biosUUID[10:12] + biosUUID[8:10] + "-" + biosUUID[
                                                                                               14:16] + biosUUID[12:14]
                formattedUUID = formattedUUID + "-" + biosUUID[16:20] + "-" + biosUUID[20:]
            else:
                formattedUUID = biosUUID[0:8] + "-" + biosUUID[8:12] + "-" + biosUUID[12:16] + "-" + biosUUID[
                                                                                                     16:20] + "-" + biosUUID[
                                                                                                                    20:]
            biosUUID = formattedUUID
        nodeOsh.setStringAttribute("bios_uuid", biosUUID)
    model = getNodeValues("hwsmbiosProductName", root)[0]
    if not len(model):
        model = getNodeValues("hwBiosMachineModel", root)[0]
    model = model.strip()
    if len(model):
        modeling.setHostModelAttribute(nodeOsh, model)
    manufacturer = getNodeValues("hwsmbiosSystemManufacturer", root)[0]
    if not len(manufacturer):
        manufacturer = getNodeValues("hwBiosManufacturer", root)[0]
    manufacturer = manufacturer.strip()
    if len(manufacturer):
        modeling.setHostManufacturerAttribute(nodeOsh, manufacturer)
    if isWindows(root):
        mapStringAttribute(nodeOsh, "net_bios_name", "hwLocalMachineID", root)
        adDomainName = getNodeValues("hwDomainName", root)[0]
        if adDomainName:
            nodeOsh.setStringAttribute("nt_ad_domain_name", adDomainName)
    domainName = getNodeValues("hwIPDomain", root)[0]
    if domainName:
        nodeOsh.setStringAttribute("domain_name", domainName)
    roles = mapNodeRole(root)
    if roles is not None:
        for role in roles:
            nodeOsh.addAttributeToList("node_role", role)

    vmFlag = isVirtualMachine(root)
    if vmFlag == 0:
        if manufacturer == "Xen" and model == "HVM domU":
            logger.debug(
                "This should be an virtual machine because manufacturer is: " + manufacturer + " and model is: " + model)
            vmFlag = 1

    nodeOsh.setBoolAttribute("host_isvirtual", vmFlag)

    hostName = getNodeValues("hwIPHostName", root)[0]
    if hostName:
        result = re.search(SHORT_HOST_NAME_PATTERN, hostName)
        if result:
            hostName = result.group(1)
        nodeOsh.setStringAttribute("host_hostname", hostName)

    dnsNameList = None
    primaryDnsName = None
    primaryIPAddress = None
    ipAddresses = nodeListToArray(root.getElementsByTagName("hwNICIPAddresses_value"))
    for ipAddress in ipAddresses:
        flag = getNodeValues("hwNICIPAddressFlags", ipAddress)[0]
        if flag == PRIMARY_IP_ADDRESS_FLAG:
            dnsNameList = getNodeValues("hwNICIPAddressDNSNames", ipAddress)[0]
            primaryIPAddress = getNodeValues("hwNICIPAddress", ipAddress)[0]
            break
    if dnsNameList:
        dnsNameList = dnsNameList.split(' ')
        for dnsName in dnsNameList:
            if dnsName.split('.')[0].lower() == hostName.lower():
                primaryDnsName = dnsName
                break
        if not primaryDnsName:
            primaryDnsName = dnsNameList[0]
        if len(primaryDnsName) > 4000:
            primaryDnsName = primaryDnsName[:4000]
        if primaryDnsName:
            nodeOsh.setStringAttribute("primary_dns_name", primaryDnsName)
    if primaryIPAddress:
        nodeOsh.setStringAttribute("primary_ip_address", primaryIPAddress)

    if isAIX(root):
        sn = getNodeValues("hwBiosMachineId", root)[0]
        m = re.search('(\w+),\d{2}(\w+)', sn)
        if m:
            sn = m.group(2)
    else:
        sn = getNodeValues("hwBiosSerialNumber", root)[0]

    if not len(sn):
        sn = getNodeValues("hwsmbiosSystemSerialNumber", root)[0]

    reportPhysicalSerialNumbers = InventoryUtils.getGlobalSetting().getPropertyBooleanValue(
        'reportPhysicalSerialNumbers', False)
    logger.debug("reportPhysicalSerialNumbers:", reportPhysicalSerialNumbers)
    if reportPhysicalSerialNumbers:
        physicalSerialNumbers = getNodeValues("hwsmbiosPhysicalAttributeSerialNumber", root)[0]
        logger.debug('physicalSerialNumbers:', physicalSerialNumbers)
        if physicalSerialNumbers:
            sn = physicalSerialNumbers

    if sn:
        sn = sn.strip()
        if len(sn):
            modeling.setHostSerialNumberAttribute(nodeOsh, sn)  # ddm_id_attribute
    dnsServers = getDnsServers(root)
    if dnsServers:
        nodeOsh.setListAttribute("dns_servers", dnsServers)
    processorFamily = mapProcessorFamily(root)
    if processorFamily:
        logger.debug("Processor Family: " + processorFamily)
        nodeOsh.setStringAttribute("processor_family", processorFamily)

    # Uncomment this code to map the selected asset data fields detected by the inventory scanners to UCMDB
    # mapAssetData(root, nodeOsh)
    oshvresults.add(nodeOsh)
    mapScanFile(oshvresults, root, nodeOsh, nodeOsh)
    return nodeOsh


# Example code illustrating how to map selected asset data fields from the scan file to
# UCMDB CI attributes, for example, map the hwAssetDescription field to the Node CI 'description' attribute.
# def mapAssetData(root, hostOsh):
# mapStringAttribute(hostOsh, "description", "hwAssetDescription", root)

# Try running "netsh int ipv6 sh dns", and you'll see that any adapter with IPv6 addresses
# shows DNS servers at fec0:0:0:ffff::1, 2 and 3
# here we exclude it to align with the values in HC by Shell
dnsDefIPv6 = ["fec0:0:0:ffff::1%1", "fec0:0:0:ffff::2%1", "fec0:0:0:ffff::3%1"]


def getDnsServers(root):
    dnsServers = []
    if isWindows(root):
        dnsWithIPv6 = getNodeValues("hwNICDNSServer", root)
        for dnsIPAddr in dnsWithIPv6:
            if dnsIPAddr not in dnsDefIPv6:
                dnsServers.append(dnsIPAddr)
        dnsServers.sort()
    elif isMac(root):
        dnsServers = getNodeValues("hwNICDNSServer", root)
    else:
        dnsServers = getNodeValues("hwNetworkDNSServer", root)
    return dnsServers


# create cpu OSH
def createCpuOSH(oshvresults, root, hostOsh):
    coreCount = int(getNodeValues("hwCPUCoreCount", root, ['0'])[0])
    logicalCpuCount = int(getNodeValues("hwCPUCount", root, ['0'])[0])
    physicalCpuCount = int(getNodeValues("hwPhysicalCPUCount", root, ['0'])[0])

    # If logical CPU count or physical CPU count is zero,
    # then we can't create any CPU information.
    if not physicalCpuCount or not logicalCpuCount:
        return

    coreNoPerPhysicalCpu = coreCount / physicalCpuCount
    logicalCpuNoPerPhysicalCpu = logicalCpuCount / physicalCpuCount
    cpus = root.getElementsByTagName("hwCPUs_value")
    cpusArray = nodeListToArray(cpus)
    cpuIndex = {}
    cpuWithoutId = []
    for idx in range(logicalCpuCount):
        cpu = cpusArray[idx]
        cpuId = getNodeValues("hwCPUPhysicalId", cpu)[0]
        if not cpuId:
            cpuWithoutId.append(idx)
        elif not cpuId in cpuIndex:
            cpuIndex[cpuId] = idx

    freeIndex = 0
    while cpuWithoutId and len(cpuIndex) < physicalCpuCount:
        while str(freeIndex) in cpuIndex:
            freeIndex += 1
        cpuIndex[str(freeIndex)] = cpuWithoutId.pop()
        freeIndex += 1

    # If we are creating CPUs based on cpuIndex
    # then we need to calculate the core per physical CPU based on it too.
    coreNoPerPhysicalCpu = coreCount / len(cpuIndex)
    logicalCpuNoPerPhysicalCpu = logicalCpuCount / len(cpuIndex)

    # QCCR1H104788 CPU Mismatch in UD discovery
    # Server1 has 10 CPUs but in UCMDB discovery shows there are 3 sockets, each has 3 cores, totally = 9 CPU in UI
    nRemainCoreCount = coreCount - coreNoPerPhysicalCpu * len(cpuIndex)
    nRemainLogicalCpuCount = logicalCpuCount - logicalCpuNoPerPhysicalCpu * len(cpuIndex)

    for cpuId, idx in cpuIndex.iteritems():
        nRemainCore = 0
        nRemainLogicalCpu = 0
        if nRemainCoreCount > 0:
            nRemainCore += 1
            nRemainCoreCount -= 1

        if nRemainLogicalCpuCount > 0:
            nRemainLogicalCpu += 1
            nRemainLogicalCpuCount -= 1

        cpu = cpusArray[idx]
        cpuId = "CPU" + cpuId
        cpuSpeed = getNodeValues("hwCPUSpeed", cpu, ['0'])[0]
        cpuVendor = getNodeValues("hwCPUVendor", cpu)[0]
        cpuName = getNodeValues("hwCPUDescription", cpu)[0]
        if not cpuName:
            cpuName = getNodeValues("hwCPUType", cpu)[0]
        if cpuName.find("Power") != -1:
            cpuName = "PowerPC-" + str(cpuName).upper()
        # todo the following line works on all the platforms except solaris
        cpuOsh = modeling.createCpuOsh(cpuId, hostOsh, cpuSpeed, coreNoPerPhysicalCpu + nRemainCore, cpuVendor, None,
                                       cpuName)
        cpuOsh.setEnumAttribute("cpu_specifier", int(getNodeEnumAttribute(cpu, "hwCPUType", '0')))
        cpuOsh.setIntegerAttribute("logical_cpu_count", logicalCpuNoPerPhysicalCpu + nRemainLogicalCpu)
        oshvresults.add(cpuOsh)
        mapScanFile(oshvresults, root, hostOsh, cpuOsh, cpu, idx)


# create VM OSH
def createOSVM(oshvresults, root, hostOsh, Framework):
    def to_float(s, default_value=-1):
        try:
            if s != '-':
                return float(s)
            else:
                return float(default_value)
        except:
            return float(default_value)

    def to_boolean(s, default_value=-1):
        if s == 'true':
            return True
        else:
            return False

    def to_int(s, default_value=-1):
        try:
            if s != '-':
                return int(s)
            else:
                return int(default_value)
        except:
            return int(default_value)

    def to_long(s, default_value=-1):
        try:
            if s != '-':
                return long(s)
            else:
                return long(default_value)
        except:
            return long(default_value)

    def parseCappedMemory(value):
        resultDict = {}
        memoryType = ['physical', 'swap', 'locked']
        for item in memoryType:
            pattern = re.compile(r'%s: (\d)*G' % item)
            m = pattern.search(value)
            if m:
                resultDict[item] = m.group(1)
        return resultDict

    containerData = root.getElementsByTagName("hwOSContainers_value")
    logger.debug('ContainerData nodes:', containerData)
    containerData = nodeListToArray(containerData)
    logger.debug('node:', containerData)
    hasHypervisor = False
    LdomhypervisorOsh = None
    ldomServerOsh = None
    LdomNum = 0
    LdomDomainDict = {}
    LdomVSWDict = {}
    idx = 0
    globalZoneOsh = None
    localZoneOshList = []
    for conData in containerData:
        vmType = str(getNodeValues("hwOSContainerType", conData)[0]).upper()
        logger.debug("vmType:", vmType)
        if vmType == 'AIX LOGICAL PARTITION':
            lparDict = getPropertiesValue(conData)
            if lparDict:
                lparOsh = ObjectStateHolder('ibm_lpar_profile')
                lparOsh.setStringAttribute('name', 'Lpar Profile')

                if 'Type' in lparDict:
                    lparOsh.setStringAttribute('lpar_smt', lparDict['Type'])
                    lparType = lparDict['Type'].upper()
                    if 'SHARED' in lparType:
                        lparOsh.setStringAttribute('proc_mode', 'shared')
                    elif 'DEDICATED' in lparType:
                        lparOsh.setStringAttribute('proc_mode', 'dedicated')

                if 'Mode' in lparDict:
                    lparMode = str(lparDict['Mode'].upper())
                    if 'UNCAPPED' in lparMode:
                        lparOsh.setStringAttribute('sharing_mode', 'uncap')
                    elif 'CAPPED' in lparMode:
                        lparOsh.setStringAttribute('sharing_mode', 'cap')

                    lparOsh.setStringAttribute('lpar_mode', lparDict['Mode'])

                    # if 'DONATING' in lparMode:
                    #     lparOsh.setStringAttribute('lpar_mode', lparMode['Mode'])
                    # else:
                    #     lparOsh.setStringAttribute('lpar_mode', lparMode['Mode'])

                if 'Entitled Capacity' in lparDict:
                    lparOsh.setFloatAttribute('entitled_capacity', to_float(lparDict['Entitled Capacity']))

                if 'Online Virtual CPUs' in lparDict:
                    lparOsh.setFloatAttribute('online_virtual_cpu', to_float(lparDict['Online Virtual CPUs']))

                if 'Active Physical CPUs in system' in lparDict:
                    lparOsh.setFloatAttribute('active_physical_cpu',
                                              to_float(lparDict['Active Physical CPUs in system']))

                if 'Active CPUs in Pool' in lparDict:
                    lparOsh.setFloatAttribute('active_cpu_in_pool', to_float(lparDict['Active CPUs in Pool']))

                if 'Shared Pool ID' in lparDict:
                    lparOsh.setStringAttribute('shared_pool_id', lparDict['Shared Pool ID'])

                if 'Partition Name' in lparDict:
                    lparOsh.setStringAttribute('lpar_name', lparDict['Partition Name'])

                if 'Partition Group-ID' in lparDict:
                    lparOsh.setStringAttribute('work_group_id', lparDict['Partition Group-ID'])

                if 'Maximum Physical CPUs in system' in lparDict:
                    lparOsh.setFloatAttribute('max_proc_units', to_float(lparDict['Maximum Physical CPUs in system']))

                if 'Maximum Virtual CPUs' in lparDict:
                    lparOsh.setIntegerAttribute('max_procs', to_int(lparDict['Maximum Virtual CPUs']))

                if 'Shared Physical CPUs in system' in lparDict:
                    lparOsh.setFloatAttribute('shared_physical_cpus',
                                              to_float(lparDict['Shared Physical CPUs in system']))

                if 'Maximum Capacity of Pool' in lparDict:
                    lparOsh.setFloatAttribute('max_capacity_pool', to_float(lparDict['Maximum Capacity of Pool']))

                if 'Entitled Capacity of Pool' in lparDict:
                    lparOsh.setFloatAttribute('entitled_capacity_pool', to_float(lparDict['Entitled Capacity of Pool']))

                lparOsh.setContainer(hostOsh)
                oshvresults.add(lparOsh)
                logger.debug("ibm_lpar_profile OSH created!")
        elif vmType == 'AIX WORKLOAD PARTITION':
            discoverWPAR = Boolean.parseBoolean(Framework.getParameter('discoverWPAR'))
            if discoverWPAR:
                createWparOsh(oshvresults, root, hostOsh, hasHypervisor, conData, idx)
        elif vmType == 'SOLARIS LDOM PARTITION':
            discoverLDOM = Boolean.parseBoolean(Framework.getParameter('discoverLDOM'))
            if discoverLDOM:
                logger.debug("Begin to report Ldom topology!")
                LdomNum += 1
                if LdomNum == 1:
                    hostOsh.setBoolAttribute("lic_type_udf", 1)
                    LdomhypervisorOsh = ObjectStateHolder('hypervisor')
                    LdomhypervisorOsh.setAttribute('name', 'LDOM Hypervisor')
                    LdomhypervisorOsh.setStringAttribute('discovered_product_name', 'Oracle LDOM Hypervisor')

                    # create Ldom Server Osh
                    hostname = getNodeValues("hwIPHostName", root)[0]
                    logger.debug("hostKey=", hostname)
                    hostname = "_".join([hostname, 'hardware'])
                    ldomServerOsh = modeling.createCompleteHostOSH('ldom_server', hostname)
                    ldomServerOsh.setStringAttribute('name', hostname)
                    ldomServerOsh.setBoolAttribute('host_isvirtual', 0)
                    ldomServerOsh.setStringAttribute('discovered_vendor', 'Sun Microsystems')
                    LdomhypervisorOsh.setContainer(ldomServerOsh)
                    oshvresults.add(LdomhypervisorOsh)
                    oshvresults.add(ldomServerOsh)
                if ldomServerOsh:
                    createLdomTopology(oshvresults, hostOsh, LdomhypervisorOsh, conData, ldomServerOsh, LdomDomainDict,
                                       LdomVSWDict)

        elif vmType == 'SOLARIS ZONE':
            zoneName = str(getNodeValues("hwOSContainerName", conData)[0])
            zoneId = str(getNodeValues("hwOSContainerID", conData)[0])
            zoneStatus = str(getNodeValues("hwOSContainerStatus", conData)[0])
            logger.debug("Solaris Zone name: ", zoneName)
            if zoneName:
                unixOsh = ObjectStateHolder('unix')
                unixOsh.setStringAttribute('name', zoneName)
                if zoneName == 'global':
                    zoneName = getNodeValues("hwIPHostName", root)[0]
                    logger.debug('%s is a global Solaris Zone' % zoneName)
                    unixOsh.setStringAttribute('name', zoneName)
                    globalZoneOsh = unixOsh
                else:
                    logger.debug('%s is a local Solaris Zone' % zoneName)
                    zoneDict = getPropertiesValue(conData)
                    zoneConfigOsh = None
                    if zoneDict:
                        zoneConfigOsh = ObjectStateHolder('solaris_zone_config')
                        zoneConfigOsh.setAttribute('name', 'Solaris Zone Configuration')
                        zoneConfigOsh.setStringAttribute('zone_uuid', zoneId)
                        zoneConfigOsh.setStringAttribute('zone_status', zoneStatus)
                        if 'zonename' in zoneDict:
                            zoneConfigOsh.setStringAttribute('zone_name', zoneDict['zonename'])
                        if 'zonepath' in zoneDict:
                            zoneConfigOsh.setStringAttribute('zone_path', zoneDict['zonepath'])
                        if 'brand' in zoneDict:
                            zoneConfigOsh.setStringAttribute('zone_brand', zoneDict['brand'])
                        if 'limitpriv' in zoneDict:
                            zoneConfigOsh.setStringAttribute('limit_privileges', zoneDict['limitpriv'])
                        if 'scheduling-class' in zoneDict:
                            zoneConfigOsh.setStringAttribute('scheduling_class', zoneDict['scheduling-class'])
                        if 'limitpriv' in zoneDict:
                            zoneConfigOsh.setStringAttribute('limit_privileges', zoneDict['limitpriv'])
                        if 'autoboot' in zoneDict:
                            zoneConfigOsh.setBoolAttribute('zone_autoboot', to_boolean(zoneDict['autoboot']))
                        if 'capped-cpu' in zoneDict:
                            zoneConfigOsh.setFloatAttribute('capped_cpu_ncpus',
                                                            zoneDict['capped-cpu'].rstrip(']').split(':')[-1])
                        if 'capped-memory' in zoneDict:
                            memoryDict = parseCappedMemory(zoneDict['capped-memory'])
                            if memoryDict.get('physical'):
                                zoneConfigOsh.setLongAttribute('capped_memory_physical',
                                                               to_long(memoryDict['physical']))
                            if memoryDict.get('swap'):
                                zoneConfigOsh.setLongAttribute('capped_memory_swap', to_long(memoryDict['swap']))
                            if memoryDict.get('locked'):
                                zoneConfigOsh.setLongAttribute('capped_memory_locked', to_long(memoryDict['locked']))
                    localZoneOshList.append((unixOsh, zoneConfigOsh))
        idx += 1
    if globalZoneOsh:
        oshvresults.add(globalZoneOsh)
        if localZoneOshList:
            hypervisorOsh = ObjectStateHolder('hypervisor')
            hypervisorOsh.setStringAttribute('discovered_product_name', 'Solaris Zones Hypervisor')
            hypervisorOsh.setContainer(globalZoneOsh)
            oshvresults.add(hypervisorOsh)
            for (localZoneOsh, zoneConfigOsh) in localZoneOshList:
                if localZoneOsh and zoneConfigOsh:
                    link = modeling.createLinkOSH("execution_environment", hypervisorOsh, localZoneOsh)
                    zoneConfigOsh.setContainer(localZoneOsh)
                    oshvresults.add(link)
                    oshvresults.add(localZoneOsh)
                    oshvresults.add(zoneConfigOsh)


def getPropertiesValue(root):
    resultDict = {}
    partitionData = root.getElementsByTagName("hwOSContainerProperties_value")
    partitionData = nodeListToArray(partitionData)
    if partitionData:
        for pData in partitionData:
            nodes = pData.getChildNodes()
            nodes = nodeListToArray(nodes)
            key = None
            value = None
            for node in [node for node in nodes if node.getNodeType() == 1]:
                if node.getTagName() == 'hwOSContainerPropertyName':
                    key = node.getTextContent()
                elif node.getTagName() == 'hwOSContainerPropertyValue':
                    value = node.getTextContent()
            if key and value:
                logger.debug('The hwOSContainerPropertyName is: %s, and its value is: %s' % (key, value))
                resultDict[key] = value
    return resultDict


# create wpar osh
def createWparOsh(oshvresults, root, hostOsh, hasHypervisor, conData, idx):
    hypervisorOsh = None
    if not hasHypervisor:
        hasHypervisor = True
        hostOsh.setBoolAttribute("lic_type_udf", 1)
        hypervisorOsh = ObjectStateHolder('hypervisor')
        hypervisorOsh.setAttribute('name', 'IBM WPAR Hypervisor')
        hypervisorOsh.setStringAttribute('discovered_product_name', 'IBM WPAR Hypervisor')
        hypervisorOsh.setContainer(hostOsh)
        oshvresults.add(hypervisorOsh)
    containerName = getNodeValues("hwOSContainerName", conData)[0]
    containerID = getNodeValues("hwOSContainerID", conData)[0]
    containerStatus = getNodeValues("hwOSContainerStatus", conData)[0]
    containerDirectory = getNodeValues("hwOSContainerDirectory", conData)[0]
    containerIPAddress = getNodeValues("hwOSContainerNetworkDeviceAddress", conData)[0]
    containerSubmask = getNodeValues("hwOSContainerNetworkDeviceSubnetMask", conData)[0]

    wparpropertiesdata = conData.getElementsByTagName("hwOSContainerProperties_value")
    logger.debug("wpar properties raw nodes:", wparpropertiesdata)
    wparpropertiesdata = nodeListToArray(wparpropertiesdata)
    logger.debug("wpar properties nodes:", wparpropertiesdata)
    wparInterfaceName = None
    wparHostName = None
    wparOsh = None
    if wparpropertiesdata:
        wparDict = {}
        for wpdata in wparpropertiesdata:
            nodes = wpdata.getChildNodes()
            nodes = nodeListToArray(nodes)
            key = None
            value = None
            for node in [node for node in nodes if node.getNodeType() == 1]:
                if node.getTagName() == 'hwOSContainerPropertyName':
                    key = node.getTextContent()
                elif node.getTagName() == 'hwOSContainerPropertyValue':
                    value = node.getTextContent()
            if key and value:
                logger.debug('%s=%s' % (key, value))
                wparDict[key] = value
        if 'Network Interface Name' in wparDict:
            wparInterfaceName = wparDict['Network Interface Name']
        if 'Hostname' in wparDict:
            wparHostName = wparDict['Hostname']
        if 'Type' in wparDict:
            wparType = str(wparDict['Type'].upper())
            if 'S' in wparType:
                wparIP = getValidIPInString(containerIPAddress)
                logger.debug("wparIP:", wparIP)
                ipOsh = None
                interfaceOsh = None
                if wparIP is not None:
                    wparOsh = modeling.createHostOSH(wparIP, 'unix')
                    wparOsh.setStringAttribute('name', wparHostName)
                    wparOsh.setStringAttribute('os_family', 'unix')
                    wparOsh.setListAttribute('node_role', ['virtualized_system'])
                    wparOsh.setStringAttribute('platform_vendor', PlatformVendors.IBM)
                    wparOsh.setBoolAttribute('host_iscomplete', True)
                    wparOsh.setBoolAttribute('host_isvirtual', True)

                    ipOsh = modeling.createIpOSH(wparIP, netmask=containerSubmask)
                    oshvresults.add(ipOsh)
                    interfaceOsh = ObjectStateHolder('interface')
                    interfaceOsh.setStringAttribute('interface_name', wparInterfaceName)
                    interfaceOsh.setContainer(wparOsh)
                    oshvresults.add(interfaceOsh)
                else:
                    wparOsh = ObjectStateHolder('unix')
                    wparOsh.setStringAttribute('name', wparHostName)
                profileOsh = ObjectStateHolder('ibm_wpar_profile')
                for key, value in wparDict.items():
                    if 'Active' == key:
                        profileOsh.setBoolAttribute('is_wpar_active', 'yes' == value)
                    if 'Architecture' == key:
                        profileOsh.setStringAttribute('wpar_architecture', value)
                    if 'Auto' == key:
                        profileOsh.setBoolAttribute('is_wpar_auto_started', 'yes' == value)
                    if 'Checkpointable' == key:
                        profileOsh.setBoolAttribute('is_wpar_checkpointable', 'yes' == value)
                    if 'CPU Limits' == key:
                        profileOsh.setStringAttribute('wpar_cpu_limits', value)
                    if 'CPU Shares' == key:
                        profileOsh.setStringAttribute('wpar_cpu_shares', value)
                    if 'Cross-WPAR IPC' == key:
                        profileOsh.setBoolAttribute('is_wpar_cross_ipc', 'yes' == value)
                    if 'Max Pinned Memory' == key:
                        profileOsh.setStringAttribute('wpar_max_pinned_memory', value)
                    if 'Max Shared Memory IDs' == key:
                        profileOsh.setStringAttribute('wpar_max_shares_memory_ids', value)
                    if 'Memory Shares' == key:
                        profileOsh.setStringAttribute('wpar_memory_shares', value)
                    if 'Memory Limits' == key:
                        profileOsh.setStringAttribute('wpar_memory_limits', value)
                    if 'Owner' == key:
                        profileOsh.setStringAttribute('wpar_owner', value)
                    if 'Per-Process Virtual Memory Limit' in wparDict:
                        profileOsh.setStringAttribute('wpar_proc_memory_limit', value)
                    if 'Private /usr' == key:
                        profileOsh.setBoolAttribute('is_wpar_private_usr', 'yes' == value)
                    if 'Resource Set' == key:
                        profileOsh.setStringAttribute('wpar_resource_set', value)
                    if 'RootVG WPAR' == key:
                        profileOsh.setBoolAttribute('is_wpar_rootvg', 'yes' == value)
                    if 'Total Large Pages' == key:
                        profileOsh.setStringAttribute('wpar_total_large_pages', value)
                    if 'Total Processes' == key:
                        profileOsh.setStringAttribute('wpar_total_processes', value)
                    if 'Total PTYs' == key:
                        profileOsh.setStringAttribute('wpar_total_ptys', value)
                    if 'Total Threads' == key:
                        profileOsh.setStringAttribute('wpar_total_threads', value)
                    if 'Total Virtual Memory Limit' == key:
                        profileOsh.setStringAttribute('wpar_total_mem_limit', value)
                    if 'Virtual IP WPAR' == key:
                        profileOsh.setStringAttribute('wpar_virtual_ip', value)
                    if 'WPAR-Specific Routing' == key:
                        profileOsh.setBoolAttribute('is_wpar_specific_routing', 'yes' == value)
                profileOsh.setStringAttribute('wpar_directory', containerDirectory)
                profileOsh.setStringAttribute('name', containerName)
                profileOsh.setStringAttribute('wpar_state', containerStatus)
                oshvresults.add(wparOsh)
                oshvresults.add(profileOsh)
                profileOsh.setContainer(wparOsh)
                if hypervisorOsh:
                    oshvresults.add(modeling.createLinkOSH('execution_environment', hypervisorOsh, wparOsh))
                else:
                    logger.debug('hypervisorOsh created fail!')
                if ipOsh is not None:
                    oshvresults.add(modeling.createLinkOSH('containment', wparOsh, ipOsh))
                    oshvresults.add(modeling.createLinkOSH('containment', interfaceOsh, ipOsh))
            else:
                wparOsh = modeling.createApplicationOSH('running_software', containerName, hostOsh)
                oshvresults.add(modeling.createLinkOSH('dependency', wparOsh, hypervisorOsh))
                oshvresults.add(wparOsh)
    if wparOsh:
        mapScanFile(oshvresults, root, hostOsh, wparOsh, conData, idx)
    logger.debug('ibm_wpar_info OSH created!')


# create Solaris Ldom volume Osh
def CreateVolumeOsh(oshvresults, ldomDomainOsh, vdiskService, clientsNum, ldomDict, volList):
    if len(volList[0:clientsNum]):
        for vol in volList[0:clientsNum]:
            volumeOsh = ObjectStateHolder('ldom_virtual_volume')
            virtualvolumeName = None
            vBackVolumeOsh = None
            for key in ldomDict.keys():
                if vol + ":vol" in key:
                    virtualvolumeName = ldomDict[key]
                elif vol + ":dev" in key:
                    volumeOsh.setStringAttribute('vv_device', ldomDict[key])
                    vBackVolumeOsh = ObjectStateHolder('logical_volume')
                    vBackVolumeOsh.setStringAttribute('name', ldomDict[key])
                elif vol + ":mpgroup" in key:
                    volumeOsh.setStringAttribute('vv_mpgroup', ldomDict[key])
                elif vol + ":opts" in key:
                    volumeOsh.setStringAttribute('vv_options', ldomDict[key])
            if virtualvolumeName:
                volumeOsh.setStringAttribute('name', virtualvolumeName)
                virtualvolume = ldom.VirtualDiskVolume(virtualvolumeName)
                virtualvolume.setOsh(KEY_VIRTUAL_VOLUME, volumeOsh)
                vdiskService.volumesByName[virtualvolumeName] = virtualvolume
                vdsOsh = vdiskService.getOsh()
                oshvresults.add(volumeOsh)
                volumeOsh.setContainer(vdsOsh)
            if vBackVolumeOsh:
                oshvresults.add(vBackVolumeOsh)
                vBackVolumeOsh.setContainer(ldomDomainOsh)
                oshvresults.add(modeling.createLinkOSH('realization', volumeOsh, vBackVolumeOsh))


# create Solaris Ldom disk Osh
def createLdomDiskOsh(oshvresults, containerName, vdiskList, ldomDict, LdomDomainDict):
    for vdisk in vdiskList:
        vdiskOsh = ObjectStateHolder('ldom_virtual_disk')
        volName = None
        vdsName = None
        for key in ldomDict.keys():
            if vdisk + ":name" in key:
                vdiskOsh.setStringAttribute('name', ldomDict[key])
            elif vdisk + ":dev" in key:
                vdiskOsh.setStringAttribute('vd_device', ldomDict[key])
            elif vdisk + ":mpgroup" in key:
                vdiskOsh.setStringAttribute('vd_mpgroup', ldomDict[key])
            elif vdisk + ":timeout" in key:
                vdiskOsh.setIntegerAttribute('vd_timeout', ldomDict[key])
            elif vdisk + ":id" in key:
                vdiskOsh.setIntegerAttribute('vd_id', ldomDict[key])
            elif vdisk + ":vol" in key:
                tokens = re.split('@', ldomDict[key])
                if tokens and len(tokens) == 2:
                    volName = tokens[0]
                    vdsName = tokens[1]
        DiskDomain = LdomDomainDict[containerName]
        LdomDomainOsh = DiskDomain.getOsh(KEY_DOMAIN)
        vdiskOsh.setContainer(LdomDomainOsh)
        oshvresults.add(vdiskOsh)
        if vdsName:
            for LdomDomain in LdomDomainDict.values():
                if LdomDomain.diskServicesByName:
                    vds = LdomDomain.diskServicesByName[vdsName]
                    volume = vds.volumesByName[volName]
                    volOsh = volume.getOsh(KEY_VIRTUAL_VOLUME)
                    oshvresults.add(modeling.createLinkOSH('dependency', vdiskOsh, volOsh))
        logger.debug("createLdomDiskOsh Done!")


# create Virtual switch Md5 string
def _getMd5OfString(strValue):
    digest = md5.new()
    digest.update(strValue)
    hashStr = digest.hexdigest()
    return hashStr


# create Solaris Ldom virtual switch Osh
def createLdomVirtualSwitchOsh(oshvresults, LdomhypervisorOsh, containerName, vswList, LdomDomainDict, ldomDict):
    LdomDomain = LdomDomainDict[containerName]
    LdomDomainOsh = LdomDomain.getOsh(KEY_DOMAIN)
    for vsw in vswList:
        vswName = None
        vswVlanid = None
        vswId = None
        vswMtu = None
        vswMac = None
        for key in ldomDict.keys():
            if vsw + ":name" in key:
                vswName = ldomDict[key]
            if vsw + ":default-vlan-id" in key:
                vswVlanid = ldomDict[key]
            if vsw + ":id" in key:
                vswId = ldomDict[key]
            if vsw + ":mtu" in key:
                vswMtu = ldomDict[key]
            if vsw + ":mac-addr" in key:
                vswMac = ldomDict[key]
        if vswMac and vswName:
            switch = ldom.VirtualSwitch(vswName)
            switch.domainName = containerName
            switchInterfaceOsh = modeling.createInterfaceOSH(vswMac)
            vswKey = "_".join([str(LdomDomain._hostKey), containerName, vswName])
            vswKeyMd5 = _getMd5OfString(vswKey)
            switchOsh = modeling.createCompleteHostOSH('ldom_virtual_switch', vswKeyMd5)
            hostBuilder = modeling.HostBuilder(switchOsh)
            hostBuilder.setAsLanSwitch(1)
            hostBuilder.setAsVirtual(1)
            switchOsh = hostBuilder.build()

            switchOsh.setStringAttribute('name', vswName)
            if vswVlanid is not None:
                switchOsh.setIntegerAttribute('vsw_default_vlan_id', vswVlanid)
            if vswId is not None:
                switchOsh.setIntegerAttribute('vsw_id', vswId)
            if vswMtu is not None:
                switchOsh.setIntegerAttribute('vsw_mtu', vswMtu)
            switch.setOsh(KEY_SWITCH_NIC, switchInterfaceOsh)
            switch.setOsh(KEY_SWITCH, switchOsh)
            switch.setMac(vswMac)
            switchInterfaceOsh.setContainer(switchOsh)
            LdomDomain.switchesByName[vswName] = switch
            oshvresults.add(switchInterfaceOsh)
            oshvresults.add(switchOsh)
            oshvresults.add(modeling.createLinkOSH('execution_environment', LdomhypervisorOsh, switchOsh))
            oshvresults.add(modeling.createLinkOSH('containment', LdomDomainOsh, switchOsh))


# create Solaris Ldom Topology
def createLdomTopology(oshvresults, hostOsh, LdomhypervisorOsh, conData, ldomServerOsh, LdomDomainDict, LdomVSWDict):
    ldomDomainOsh = None
    vnetName = None
    vnetMac = None
    vinterfaceOsh = None
    vdiskList = []
    vswList = []
    vdsList = []
    volList = []
    ldomDict = {}
    containerName = getNodeValues("hwOSContainerName", conData)[0]
    containerID = getNodeValues("hwOSContainerID", conData)[0]
    containerStatus = getNodeValues("hwOSContainerStatus", conData)[0]
    containerHostID = getNodeValues("hwOSContainerHostID", conData)[0]
    containerRole = getNodeValues("hwOSContainerRole", conData)[0]

    # create Domain
    LdomDomain = ldom.Domain(containerName)

    # create ldom config Osh
    ldomConfigOsh = ObjectStateHolder('ldom_config')
    ldomConfigOsh.setAttribute('name', "LDOM Config")
    if containerName:
        ldomConfigOsh.setStringAttribute('ldom_name', containerName)
    if containerHostID:
        ldomConfigOsh.setStringAttribute('ldom_hostid', containerHostID)
    if containerID:
        ldomConfigOsh.setStringAttribute('ldom_uuid', containerID)
    if containerStatus:
        ldomConfigOsh.setStringAttribute('ldom_state', containerStatus)

    ldomPropertiesData = conData.getElementsByTagName("hwOSContainerProperties_value")
    ldomPropertiesData = nodeListToArray(ldomPropertiesData)
    if ldomPropertiesData:
        vnetList = []
        for ldomData in ldomPropertiesData:
            nodes = ldomData.getChildNodes()
            nodes = nodeListToArray(nodes)
            key = None
            value = None
            for node in [node for node in nodes if node.getNodeType() == 1]:
                if node.getTagName() == 'hwOSContainerPropertyName':
                    key = node.getTextContent()
                elif node.getTagName() == 'hwOSContainerPropertyValue':
                    value = node.getTextContent()
            if key and value:
                logger.debug('%s=%s' % (key, value))
                ldomDict[key] = value
            if 'VDS' in key:
                vdsList.append(key.split(':')[0])
            elif 'VDISK' in key:
                vdiskList.append(key.split(':')[0])
            elif 'VOLUME' in key:
                volList.append(key.split(':')[0])
            elif 'VSW' in key:
                vswList.append(key.split(':')[0])
            elif 'VNET' in key:
                vnetList.append(key.split(':')[0])
        vdsList = list(set(vdsList))
        vdiskList = list(set(vdiskList))
        volList = list(set(volList))
        vswList = list(set(vswList))
        vnetList = list(set(vnetList))
        vdsList.sort()
        vdiskList.sort()
        volList.sort()

        for vnet in vnetList:
            switchName = None
            domainName = None
            for key in ldomDict.keys():
                if vnet + ":name" in key:
                    vnetName = ldomDict[key]
                elif vnet + ":mac-addr" in key:
                    vnetMac = ldomDict[key]
                elif vnet + ":service" in key:
                    vnetService = ldomDict[key]
                    tokens = re.split('@', vnetService)
                    if tokens and len(tokens) == 2:
                        switchName = tokens[0]
                        domainName = tokens[1]
            vinterfaceOsh = modeling.createInterfaceOSH(vnetMac)
            vinterface = ldom.VirtualInterface(vnetName)
            vinterface.setOsh(vinterfaceOsh)
            vinterface.setMac(vnetMac)
            LdomDomain.virtualInterfacesByName[vnetName] = vinterface
            if domainName:
                for switch in LdomDomainDict[domainName].switchesByName.values():
                    if switchName == switch.getName():
                        switchInterfaceOsh = switch.getOsh(KEY_SWITCH_NIC)
                        macs = [switch.getMac(), vnetMac]
                        linkId = ":".join(macs)
                        linkId = str(hash(linkId))
                        layer2Osh = ObjectStateHolder('layer2_connection')
                        layer2Osh.setAttribute('layer2_connection_id', linkId)
                        oshvresults.add(layer2Osh)
                        oshvresults.add(modeling.createLinkOSH('membership', layer2Osh, switchInterfaceOsh))
                        oshvresults.add(modeling.createLinkOSH('membership', layer2Osh, vinterfaceOsh))
            oshvresults.add(vinterfaceOsh)

        if 'classis serial' in ldomDict:
            ldomServerOsh.setStringAttribute('serial_number', ldomDict['classis serial'])
        if 'HostModel' in ldomDict:
            ldomServerOsh.setStringAttribute('discovered_model', ldomDict['HostModel'])
        if 'ncpu' in ldomDict:
            ldomServerOsh.setIntegerAttribute('ncpu', ldomDict['ncpu'])
            ldomConfigOsh.setIntegerAttribute('ldom_ncpu', ldomDict['ncpu'])
        if 'server memory' in ldomDict:
            memorySize = ldomDict['server memory']
            memorySizeMegabytes = int(memorySize) / (1024 * 1024)
            ldomServerOsh.setIntegerAttribute('memory_size', memorySizeMegabytes)
        if 'mem' in ldomDict:
            memorySize = ldomDict['mem']
            memorySizeMegabytes = int(memorySize) / (1024 * 1024)
            ldomConfigOsh.setIntegerAttribute('ldom_memory_size', memorySizeMegabytes)
        if 'mac-addr' in ldomDict:
            LdomDomain._hostKey = ldomDict['mac-addr']
            ldomConfigOsh.setStringAttribute('ldom_mac', ldomDict['mac-addr'])
        if 'failure-policy' in ldomDict:
            ldomConfigOsh.setStringAttribute("ldom_failure_policy", ldomDict['failure-policy'])

    if "control" in containerRole:
        ldomConfigOsh.setBoolAttribute("ldom_is_control", 1)
        ldomDomainOsh = hostOsh
    else:
        if vnetMac:
            vnetMac = netutils.parseMac(vnetMac)
            ldomDomainOsh = modeling.createCompleteHostOSH('node', str(vnetMac))
            oshvresults.add(ldomDomainOsh)
    if "I/O" in containerRole:
        ldomConfigOsh.setBoolAttribute("ldom_is_io", 1)
    if ldomDomainOsh:
        for vds in vdsList:
            vdsName = None
            vdsOsh = ObjectStateHolder('ldom_vds')
            clientsNum = None
            for key in ldomDict.keys():
                if vds + ":name" in key:
                    vdsName = ldomDict[key]
                if vds + ":nclients" in key:
                    clientsNum = int(ldomDict[key])
            if vdsName:
                vdsOsh.setStringAttribute('name', vdsName)
                vdiskService = ldom.VirtualDiskService(vdsName)
                vdiskService.domainName = containerName
                vdiskService.setOsh(vdsOsh)
                if len(volList):
                    CreateVolumeOsh(oshvresults, ldomDomainOsh, vdiskService, clientsNum, ldomDict, volList)
                    volList = volList[clientsNum:]
                LdomDomain.diskServicesByName[vdsName] = vdiskService
        LdomDomain.setOsh(KEY_DOMAIN, ldomDomainOsh)
        LdomDomain.setOsh(KEY_DOMAIN_CONF, ldomConfigOsh)
        LdomDomainDict[containerName] = LdomDomain
        ldomConfigOsh.setContainer(ldomDomainOsh)
        for vds in LdomDomain.diskServicesByName.values():
            vdsOsh = vds.getOsh()
            vdsOsh.setContainer(ldomDomainOsh)
            oshvresults.add(vdsOsh)
        if vinterfaceOsh:
            vinterfaceOsh.setContainer(ldomDomainOsh)
        oshvresults.add(ldomConfigOsh)
        oshvresults.add(modeling.createLinkOSH('execution_environment', LdomhypervisorOsh, ldomDomainOsh))
    if len(vswList):
        createLdomVirtualSwitchOsh(oshvresults, LdomhypervisorOsh, containerName, vswList, LdomDomainDict, ldomDict)
    if len(vdiskList):
        createLdomDiskOsh(oshvresults, containerName, vdiskList, ldomDict, LdomDomainDict)


# create windows device driver OSH
def createWindiwsDeviceDriverOSH(oshvresults, root, hostOsh):
    deviceDriverWithID = {}
    deviceDrivers = root.getElementsByTagName("hwOSDeviceDriverData_value")
    deviceDriverArray = nodeListToArray(deviceDrivers)
    propertyArray = {'hwOSDeviceDriverDataCompatID': 'compat_id',
                     'hwOSDeviceDriverDataDescription': 'description',
                     'hwOSDeviceDriverDataDeviceClass': 'device_class',
                     'hwOSDeviceDriverDataDeviceID': 'device_id',
                     'hwOSDeviceDriverDataDeviceName': 'device_name',
                     'hwOSDeviceDriverDataDevLoader': 'dev_loader',
                     'hwOSDeviceDriverDataDriverDate': 'driver_date',
                     'hwOSDeviceDriverDataDriverName': 'driver_name',
                     'hwOSDeviceDriverDataDriverProviderName': 'driver_provider_name',
                     'hwOSDeviceDriverDataDriverVersion': 'driver_version',
                     'hwOSDeviceDriverDataFriendlyName': 'friendly_name',
                     'hwOSDeviceDriverDataHardWareID': 'hardware_id',
                     'hwOSDeviceDriverDataInfName': 'inf_name',
                     'hwOSDeviceDriverDataInstallDate': 'install_date',
                     'hwOSDeviceDriverDataIsSigned': 'is_signed',
                     'hwOSDeviceDriverDataLocation': 'location',
                     'hwOSDeviceDriverDataManufacturer': 'manufacturer',
                     'hwOSDeviceDriverDataName': 'name',
                     'hwOSDeviceDriverDataPDO': 'pdo',
                     'hwOSDeviceDriverDataSigner': 'signer'}
    for deviceDriver in deviceDriverArray:
        deviceDriverOsh = ObjectStateHolder("windows_device_driver")
        deviceID = None
        for propertyName in propertyArray.keys():
            driverAttribute = getNodeValues(propertyName, deviceDriver)[0]
            if len(driverAttribute):
                deviceDriverOsh.setStringAttribute(propertyArray[propertyName], driverAttribute)
                if propertyName == 'hwOSDeviceDriverDataDeviceID':
                    deviceID = driverAttribute
        deviceDriverWithID[deviceID] = deviceDriverOsh
        deviceDriverOsh.setContainer(hostOsh)
        oshvresults.add(deviceDriverOsh)
    return deviceDriverWithID


# create disk device OSH
def createDiskOSH(oshvresults, root, hostOsh):
    scsiDevices = root.getElementsByTagName("hwPhysicalDiskData_value")
    scsiDevicesArray = nodeListToArray(scsiDevices)
    idx = 0
    for device in scsiDevicesArray:
        diskOsh = ObjectStateHolder("disk_device")
        deviceName = getNodeValues("hwPhysicalDiskID", device)[0]
        if len(deviceName):
            diskOsh.setStringAttribute("name", deviceName.upper())  # id attribute
            # mapStringAttribute(diskOsh, "model_name", "hwSCSIDeviceName", device)
            # mapStringAttribute(diskOsh, "vendor", "hwSCSIDeviceVendor", device)
            mapStringAttribute(diskOsh, "serial_number", "hwPhysicalDiskSerialNumber", device)
            diskOsh.setStringAttribute("disk_type", InventoryUtils.DISK_TYPE_MAPPING.get(
                getNodeEnumAttribute(device, "hwPhysicalDiskType")))
            diskOsh.setIntegerAttribute("disk_size", int(getNodeValues("hwPhysicalDiskSize", device, ['0'])[0]))
            diskOsh.setContainer(hostOsh)
            oshvresults.add(diskOsh)
            mapScanFile(oshvresults, root, hostOsh, diskOsh, device, idx)
            idx += 1


def _toFloatOrNone(floatString):
    if floatString is not None:
        try:
            return float(floatString)
        except ValueError:
            logger.debug('Can not parse %s to float' % floatString)


def _createFsOSH(hostOsh, mountedTo, diskType,
                 labelName, filesystemDevice, filesystemType,
                 diskSize, freeSpace):
    usedSize = None
    if freeSpace and diskSize:
        usedSize = diskSize - freeSpace

    fsOsh = modeling.createFileSystemOSH(hostOsh, mountedTo, diskType,
                                         labelName=labelName, mountDevice=filesystemDevice,
                                         fileSystemType=filesystemType,
                                         size=diskSize, usedSize=usedSize, failures=None)
    return fsOsh


def createiSCSIOSH(oshvResults, root, hostOsh):
    phyVolumeOshMap = {}
    iScsiInitiatorOsh = None
    initiator = getNodeValues("hwiSCSIInitiator", root)[0]
    if len(initiator):
        iScsiInitiatorOsh = ObjectStateHolder("iscsi_adapter")
        iScsiInitiatorOsh.setStringAttribute("iqn", initiator)
        iScsiInitiatorOsh.setContainer(hostOsh)
        oshvResults.add(iScsiInitiatorOsh)
        mapScanFile(oshvResults, root, hostOsh, iScsiInitiatorOsh)

    if iScsiInitiatorOsh is None:
        return phyVolumeOshMap

    idx = 0
    targets = root.getElementsByTagName("hwiSCSITargetData_value")
    targetsArray = nodeListToArray(targets)
    for target in targetsArray:
        targetName = getNodeValues("hwiSCSIIQN", target)[0]
        iScsiTargetOsh = ObjectStateHolder("iscsi_adapter")
        iScsiTargetOsh.setStringAttribute("iqn", targetName)
        oshvResults.add(iScsiTargetOsh)
        mapScanFile(oshvResults, root, None, iScsiTargetOsh, idx)
        usageOsh = modeling.createLinkOSH('usage', iScsiInitiatorOsh, iScsiTargetOsh)
        oshvResults.add(usageOsh)

        portals = target.getElementsByTagName("hwiSCSIPortals_value")
        portalArray = nodeListToArray(portals)
        targetHostOsh = None
        for portal in portalArray:
            portalAddress = getNodeValues("hwiSCSIPortalAddress", portal)[0]
            portalAddress = portalAddress.strip(' []')
            ipAddr = getValidIP(portalAddress)
            if targetHostOsh is None and ipAddr:
                ipAddrStr = getValidIPInString(portalAddress)
                if ipAddrStr:
                    targetHostOsh = modeling.createHostOSH(ipAddrStr)
                    oshvResults.add(targetHostOsh)
                    iScsiTargetOsh.setContainer(targetHostOsh)
                    mapScanFile(oshvResults, root, hostOsh, targetHostOsh, idx)

            if targetHostOsh and ipAddr:
                ipOsh = modeling.createIpOSH(ipAddr)
                oshvResults.add(ipOsh)
                oshvResults.add(modeling.createLinkOSH('contained', targetHostOsh, ipOsh))

        devices = target.getElementsByTagName("hwiSCSIDevices_value")
        devicesArray = nodeListToArray(devices)
        for device in devicesArray:
            deviceLegacyName = getNodeValues("hwiSCSIDeviceLegacyName", device)[0]
            interfaceName = getNodeValues("hwiSCSIDeviceInterfaceName", device)[0]
            diskNumber = getNodeValues("hwBoundPhysicalDiskNumber", device)[0]
            phyVolumeOsh = ObjectStateHolder("physicalvolume")
            phyVolumeOsh.setStringAttribute("name", deviceLegacyName)
            phyVolumeOsh.setStringAttribute("volume_id", interfaceName)
            phyVolumeOsh.setContainer(hostOsh)
            oshvResults.add(phyVolumeOsh)
            mapScanFile(oshvResults, root, hostOsh, phyVolumeOsh, idx)
            phyVolumeOshMap[diskNumber] = phyVolumeOsh
            oshvResults.add(modeling.createLinkOSH('dependency', phyVolumeOsh, iScsiTargetOsh))
        idx += 1

    return phyVolumeOshMap


# create file system OSH
def createFsOSH(oshvresults, root, hostOsh, physicalVolumeOshMap={}):
    fss = root.getElementsByTagName("hwMountPoints_value")
    fssArray = nodeListToArray(fss)
    idx = 0
    for fs in fssArray:
        mountedTo = getNodeValues("hwMountPointMountedTo", fs)[0]
        if isWindows(root):
            mountedTo = mountedTo.rstrip(":\\")
        mpVolumeType = getNodeValues("hwMountPointVolumeType", fs)[0]
        mpVolumeMedia = getNodeValues("hwMountPointVolumeMedia", fs)[0]
        if len(
                mountedTo) and mpVolumeType and mpVolumeMedia and mpVolumeType != 'Unsupported' and mpVolumeMedia != 'Unknown':
            diskType = getNodeValues("hwMountPointVolumeMedia", fs)[0]
            diskSize = getNodeValues("hwMountPointVolumeTotalSize", fs, [0])[0]
            diskSize = _toFloatOrNone(diskSize)
            freeSpace = getNodeValues("hwMountPointVolumeFreeSpace", fs, [0])[0]
            freeSpace = _toFloatOrNone(freeSpace)
            labelName = getNodeValues("hwMountPointVolumeLabel", fs)[0]
            filesystemType = getNodeValues("hwMountPointVolumeType", fs)[0]
            filesystemDevice = getNodeValues("hwMountPointVolumeDevice", fs)[0]
            boundPhysicalDiskNumber = getNodeValues("hwMountPointVolumePhysicalDiskNumber", fs)[0]
            fsOsh = _createFsOSH(hostOsh, mountedTo, diskType,
                                 labelName, filesystemDevice, filesystemType,
                                 diskSize, freeSpace)
            if fsOsh:
                oshvresults.add(fsOsh)

                mpVolumeName = getNodeValues("hwMountPointVolumeName", fs)[0]
                # avoid creating logic volumes when file system is special like "pseudo" file systems
                # /dev, /proc
                if len(mpVolumeName) and not mpVolumeType == 'Unsupported':
                    logicalVolOsh = ObjectStateHolder("logical_volume")
                    logicalVolOsh.setStringAttribute("name", mpVolumeName)  # id attribute
                    mapStringAttribute(logicalVolOsh, "logicalvolume_fstype", "hwMountPointVolumeType", fs)
                    if freeSpace:
                        logicalVolOsh.setDoubleAttribute("logicalvolume_free", freeSpace)
                    if diskSize:
                        logicalVolOsh.setDoubleAttribute("logicalvolume_size", diskSize)
                    logicalVolOsh.setContainer(hostOsh)
                    oshvresults.add(logicalVolOsh)
                    oshvresults.add(modeling.createLinkOSH("dependency", fsOsh, logicalVolOsh))
                    mapScanFile(oshvresults, root, hostOsh, logicalVolOsh, fs, idx)
                    phyVolumeOsh = physicalVolumeOshMap.get(boundPhysicalDiskNumber)
                    if phyVolumeOsh:
                        oshvresults.add(modeling.createLinkOSH("usage", logicalVolOsh, phyVolumeOsh))

                mapScanFile(oshvresults, root, hostOsh, fsOsh, fs, idx)
        idx += 1


# create file system export OSH
def createFseOSH(oshvresults, root, hostOsh):
    netshareValues = root.getElementsByTagName("hwNetworkShares_value")
    netshareValuesArray = nodeListToArray(netshareValues)
    idx = 0
    for netshare in netshareValuesArray:
        networkSharePath = getNodeValues("hwNetworkSharePath", netshare)[0]
        # for $IPC, this field would be empty
        if len(networkSharePath):
            fseOsh = ObjectStateHolder("file_system_export")
            fseOsh.setStringAttribute("file_system_path", networkSharePath)  # id attribute
            fseOsh.setListAttribute("share_names", getNodeValues("hwNetworkShareName", netshare))
            fseOsh.setStringAttribute("name", networkSharePath)
            mapStringAttribute(fseOsh, "description", "hwNetworkShareRemark", netshare)
            fseOsh.setContainer(hostOsh)
            oshvresults.add(fseOsh)
            mapScanFile(oshvresults, root, hostOsh, fseOsh, netshare, idx)
            idx += 1


# create windows user OSH
def createWinUserOSH(oshvresults, root, hostOsh):
    networkLogonName = getNodeValues("hwNetworkLogonName", root)[0]
    if len(networkLogonName):
        userOsh = ObjectStateHolder("winosuser")
        userOsh.setStringAttribute("name", networkLogonName)  # id attribute
        mapStringAttribute(userOsh, "winosuser_domain", "hwNetworkLogonDomain", root)
        userInfo = root.getElementsByTagName("hwOSUserProfiles_value")
        if userInfo.getLength():
            userInfoArray = nodeListToArray(userInfo)
            for user in userInfoArray:
                userName = getNodeValues("hwOSUserProfileName", user)[0]
                if networkLogonName in userName:
                    userlogon = getNodeValues("hwOSUserProfileLastLogon", user)[0]
                    if userlogon:
                        try:
                            sfDate = SimpleDateFormat('yyyy-MM-dd HH:mm:ss')
                            user_logon = sfDate.parse(userlogon)
                        except:
                            sfDate = SimpleDateFormat('yyyy-MM-dd')
                            user_logon = sfDate.parse(userlogon)
                        userOsh.setDateAttribute("osuser_last_logon", user_logon)
                        break
        userOsh.setContainer(hostOsh)
        oshvresults.add(userOsh)
        mapScanFile(oshvresults, root, hostOsh, userOsh)


# create os user OSH
def createOsUserOSH(oshvresults, root, hostOsh):
    networkLogonName = getNodeValues("hwNetworkLogonName", root)[0]
    if len(networkLogonName):
        userOsh = ObjectStateHolder("osuser")
        userOsh.setStringAttribute("name", networkLogonName)  # id attribute
        userOsh.setContainer(hostOsh)
        oshvresults.add(userOsh)
        mapScanFile(oshvresults, root, hostOsh, userOsh)


# create memory unit OSH
def createMMUOSH(oshvresults, root, hostOsh):
    mmuValues = root.getElementsByTagName("hwsmbiosMemoryDevice_value")
    if mmuValues.getLength():
        mmuValuesArray = nodeListToArray(mmuValues)
        idx = 0
        for mmu in mmuValuesArray:
            memoryUnitOSH = ObjectStateHolder("memory_unit")
            name = getNodeValues("hwsmbiosMemoryArrayDeviceLocator", mmu)[0]
            if not len(name):
                name = getNodeValues("hwsmbiosMemoryArrayBankLocator", mmu)[0]
            memoryUnitOSH.setStringAttribute("name", name)  # id attribute
            memoryUnitOSH.setStringAttribute("serial_number", getNodeValues("hwsmbiosMemoryArraySerialNumber", mmu)[0])
            memoryUnitOSH.setIntegerAttribute("memory_unit_index", idx)
            memoryUnitOSH.setContainer(hostOsh)
            oshvresults.add(memoryUnitOSH)
            mapScanFile(oshvresults, root, hostOsh, memoryUnitOSH, mmu, idx)
            idx += 1
    else:
        mmuValues = root.getElementsByTagName("hwsmbiosMemoryModuleInformation_value")
        if mmuValues.getLength():
            mmuValuesArray = nodeListToArray(mmuValues)
            idx = 0
            for mmu in mmuValuesArray:
                memoryUnitOSH = ObjectStateHolder("memory_unit")
                memoryUnitOSH.setStringAttribute("name", getNodeValues("hwsmbiosMemoryModuleSocketDesignation", mmu)[0])
                memoryUnitOSH.setIntegerAttribute("memory_unit_index", idx)
                memoryUnitOSH.setContainer(hostOsh)
                oshvresults.add(memoryUnitOSH)
                mapScanFile(oshvresults, root, hostOsh, memoryUnitOSH, mmu, idx)
                idx += 1
        else:
            mmuValues = root.getElementsByTagName("hwMemoryConfig_value")
            if mmuValues.getLength() > 0:
                mmuValuesArray = nodeListToArray(mmuValues)
                idx = 0
                for mmu in mmuValuesArray:
                    memoryUnitOSH = ObjectStateHolder("memory_unit")
                    memoryUnitOSH.setStringAttribute("name", getNodeValues("hwMemoryBank", mmu)[0])
                    memoryUnitOSH.setIntegerAttribute("memory_unit_index", idx)
                    memoryUnitOSH.setContainer(hostOsh)
                    oshvresults.add(memoryUnitOSH)
                    mapScanFile(oshvresults, root, hostOsh, memoryUnitOSH, mmu, idx)
                    idx += 1


def mapStringAttribute(osh, ucmdbAttribute, ddmiAttribute, element):
    value = getNodeValues(ddmiAttribute, element)[0]
    if len(value):
        if ucmdbAttribute == 'vendor' and ddmiAttribute == 'hwCardVendor' and len(value) > 1000:
            value = value[:1000]
        if ucmdbAttribute == 'bios_source' and ddmiAttribute == 'hwBiosSource' and len(value) > 1000:
            value = value[:1000]
        osh.setStringAttribute(ucmdbAttribute, value)


def mapOsVer(root):
    ver = ''
    os = getNodeValues("hwOSHostOsCategory", root)[0]
    if re.search('Windows', os):
        internalVer = getNodeValues("hwOSInternalVersion", root)[0]
        buildLvl = getNodeValues("hwOSBuildLevel", root)[0]
        # remove leading 0 of minor version
        version_delimiter = '.'
        ver = None
        try:
            refined_internal_version = ''
            for field in internalVer.split(version_delimiter):
                refined_internal_version += (str(int(field)) + version_delimiter)
            ver = refined_internal_version + buildLvl
        except:  # In case the above translation does not work, follow the old way
            ver = re.sub(r'\.0', '..', internalVer) + "." + buildLvl
        return ver
    if (os == 'Unix' or os == 'Mac OS'):
        ver = getNodeValues("hwOSHostVersion", root)[0]
        return ver
    return ver


def mapOsType(root):
    _type = ''
    if isWindows(root):
        _type = getNodeValues("hwOSHostWindowsNTMode", root)[0]
        edition = getNodeValues("hwOSHostEdition", root)[0]
        if not _type:
            _type = edition
    if isLinux(root):
        _type = getNodeValues("hwOSHostLinuxType", root)[0]
    if isMac(root):
        _type = getNodeValues("hwOSHostMacOsType", root)[0]
    if isHPUX(root):
        _type = getNodeValues("hwOSHostHPUXType", root)[0]
    return _type


def mapOsRelease(root):
    if isWindows(root):
        return getNodeValues("hwOSBuildLevel", root)[0]
    os = getNodeValues("hwOSHostOsCategory", root)[0]
    if (os == 'Unix'):
        unixtype = getNodeValues("hwOSHostUnixType", root)[0]
        if (unixtype == 'Linux' or unixtype == 'XenServer'):
            return "release " + getNodeValues("hwOSHostVersion", root)[0]
        elif unixtype == 'Solaris':
            return getNodeValues('hwOSDetailedServiceLevel', root)[0]
        serviceLevel = getNodeValues("hwOSServiceLevel", root)[0]
        if unixtype == 'AIX':
            serviceLevel = serviceLevel.lstrip('0')
        return serviceLevel
    return ''


def mapOsVendor(root):
    os = getNodeValues("hwOSHostOsCategory", root)[0]
    if (os == "Microsoft Windows") or (os == "DOS"):
        return "Microsoft"
    if (os == "IBM OS/2"):
        return "IBM"
    if (os == "Mac OS"):
        return "Apple"
    if (os == "Unix"):
        return mapUxOsVendor(root)
    return os


def mapUxOsVendor(root):
    os = getNodeValues("hwOSHostUnixType", root)[0]
    if (os == "Solaris"):
        return "Oracle"
    if (os == "HP-UX"):
        return "Hewlett-Packard"
    if (os == "AIX"):
        return "IBM"
    if (os == "Mac OS X"):
        return "Apple"
    if (os == "VMware"):
        return "VMware"
    if (os == "XenServer"):
        return "Citrix"
    if (os == "Linux"):
        linuxType = getNodeValues("hwOSHostLinuxType", root)[0]
        if re.search('^Red Hat', linuxType):
            return "Red Hat"
        if (re.search('^SUSE', linuxType)) or (re.search('^Novell', linuxType)):
            return "Micro Focus"
        if re.search('^Oracle Linux', linuxType):
            return "Oracle"
        if re.search('^CentOS', linuxType):
            return "CentOS"
        if re.search('^Ubuntu', linuxType):
            return "Canonical Ltd"
    return os


# Mapping processor family from platform information
def mapProcessorFamily(root):
    # @types: scan file -> string or None
    unixType = getNodeValues("hwOSHostUnixType", root)[0]
    platform = getNodeValues("hwPlatform", root)[0]
    logger.debug("UnixType: " + unixType)
    if platform:
        logger.debug("HardwarePlatform: " + platform)
        platform = platform.lower()

    processorFamily = None
    if isMac(root) or unixType == "Mac OS X":
        if platform == "powerpc":
            processorFamily = ProcessorFamily.PowerPC
        else:
            processorFamily = getProcessorFamilyFromCPUFeatures(root)
    elif isLinux(root) or isXenServer(root):
        processorFamily = getProcessorFamilyFromCPUFeatures(root)
    elif isUnix(root):
        if unixType == "AIX":
            processorFamily = ProcessorFamily.PowerPC
        elif unixType == "HP-UX":
            if platform == "ia64":
                processorFamily = ProcessorFamily.IA64
            else:
                processorFamily = ProcessorFamily.PA_RISC
        elif unixType == "Solaris":
            if platform == "i86pc":
                processorFamily = getProcessorFamilyFromCPUFeatures(root)
            else:
                processorFamily = ProcessorFamily.SPARC
    elif isWindows(root):
        if platform == "ia64":
            processorFamily = ProcessorFamily.IA64
        elif platform == "amd64":
            processorFamily = ProcessorFamily.X86_64
        else:
            processorFamily = getProcessorFamilyFromCPUFeatures(root)
    return processorFamily


# Gets processor family from CPUFeatures
def getProcessorFamilyFromCPUFeatures(root):
    # @types: scan file -> ProcessorFamily.X86_32 or ProcessorFamily.X86_64
    cpuFeatures = getNodeValues("hwCPUIntelFeatures", root)
    for cpuFeature in cpuFeatures:
        logger.debug("CpuFeature:" + cpuFeature)
        if re.search("AMD64/EM64T", cpuFeature):
            return ProcessorFamily.X86_64
    return ProcessorFamily.X86_32


# Mapping Os Architecture from platform information
def mapOsArchitecture(root):
    # @types: scan file -> OsArchitecture.x86 or OsArchitecture.x64 or None
    osArchitecture = getNodeValues("hwOSArchitecture", root)[0].lower()
    return osArchitecture


def mapInstalledSoftware(oshvresults, root, hostOsh, mappingConfig):
    logger.debug("Reports free software:" + str(mappingConfig.reportFreeSoftware()))
    softwares = createSoftwareOSH(oshvresults, root, hostOsh, mappingConfig)
    createOsInstalledSoftware(oshvresults, root, hostOsh, mappingConfig)
    if mappingConfig.partiallyRecApp:
        softwares.update(createSoftwareOSH(oshvresults, root, hostOsh, mappingConfig, partial=1))
    createSoftwareLink(oshvresults, softwares)


# <application version="00.000" release="0" name="name" publisher="pub" language="English" os="os" versionid="1000" licensedby="8000" .../>
# <partialapp version="00.000" release="0" name="name" publisher="pub" language="English" os="os" versionid="1000" .../>
# <users>
#    <user id="0" name="ALL USERS"/>
# </users>
# <applicationusage>
#    <used versionid="1000" userid="0" .../>
# </applicationusage>
def createSoftwareOSH(oshvresults, root, hostOsh, mappingConfig, partial=None):
    dateFormatter = SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    bdnaDateFormatter = SimpleDateFormat("yyyy-MM-dd")
    usagesArray = []
    usersArray = []
    softwares = {}
    userNumberThreshold = None
    if mappingConfig.softwareUtilization:
        userNumberThreshold = mappingConfig.numberOfUser
        usages = root.getElementsByTagName("used")
        usagesArray = nodeListToArray(usages)
        users = root.getElementsByTagName("user")
        usersArray = nodeListToArray(users)
    recognitionMethod = int(getNodeEnumAttribute(root, "hwRecognitionMethod", '1'))
    if recognitionMethod == 1 or recognitionMethod == 0:
        recognitionLevelStr = RECOGNITION_LEVEL_RAW
    elif recognitionMethod == 2:
        recognitionLevelStr = RECOGNITION_LEVEL_NORMALIZED
    if partial:
        recognitionLevelStr = RECOGNITION_LEVEL_PARTIAL
        applications = root.getElementsByTagName("partialapp")
    else:
        applications = root.getElementsByTagName("application")
    applicationsArray = nodeListToArray(applications)
    for application in applicationsArray:
        name = getNodeAttribute(application, "name")
        vendor = getNodeAttribute(application, "publisher")
        version = getNodeAttribute(application, "version")
        desc = getNodeAttribute(application, "verdesc")
        licenseType = getSoftwareLicenseType(getNodeAttribute(application, "commercial"))
        if not mappingConfig.reportFreeSoftware() and licenseType == SOFTWARE_LICENSE_TYPES["Free"]:
            logger.debug(name + " ignored because its license type is free.")
            continue
        if recognitionLevelStr == RECOGNITION_LEVEL_RAW:
            softwareOsh = handleRawApplications(mappingConfig, name, vendor, version, desc)
        else:
            softwareOsh = handleNormalApplications(mappingConfig, name, vendor, version, desc)
        if softwareOsh:
            softwareOsh.setStringAttribute("file_system_path",
                                           getNodeAttribute(application, "maindir"))  # DDM ID Attribute
            if existNodeAttribute(application, "component"):
                recognitionByStr = RECOGNIZED_BY_BDNA
            else:
                recognitionByStr = RECOGNIZED_BY_SAI
            softwareOsh.setAttribute("recognized_by", recognitionByStr)
            softwareOsh.setBoolAttribute("is_suite_component", len(getNodeAttribute(application, "licencedby")) > 0)
            usageLastUsedDateStr = getNodeAttribute(application, "usagelastused")
            if len(usageLastUsedDateStr):
                usageLastUsedDate = dateFormatter.parse(usageLastUsedDateStr)
                softwareOsh.setDateAttribute("usage_last_used", usageLastUsedDate)

            inFocusUsageLastUsedDateStr = getNodeAttribute(application, "usagelastusedfoc")
            if len(inFocusUsageLastUsedDateStr):
                inFocusUsageLastUsedDate = dateFormatter.parse(inFocusUsageLastUsedDateStr)
                softwareOsh.setDateAttribute("infocus_usage_last_used", inFocusUsageLastUsedDate)
            lastUsedDateStr = getNodeAttribute(application, "lastUsed")
            if len(lastUsedDateStr):
                lastUsedDate = dateFormatter.parse(lastUsedDateStr)
                softwareOsh.setDateAttribute("last_used_date", lastUsedDate)
            eolDateStr = getNodeAttribute(application, "endoflife")
            if len(eolDateStr):
                eolDate = bdnaDateFormatter.parse(eolDateStr)
                softwareOsh.setDateAttribute("end_of_life_date", eolDate)
            obsoleteDateStr = getNodeAttribute(application, "obsolete")
            if len(obsoleteDateStr):
                obsoleteDate = bdnaDateFormatter.parse(obsoleteDateStr)
                softwareOsh.setDateAttribute("obsolete_date", obsoleteDate)
            language = getNodeAttribute(application, "language")
            if not re.search('neutral', language, re.I):
                softwareOsh.setStringAttribute("software_language", language)
            softwareOsh.setStringAttribute("version", getNodeAttribute(application, "version"))
            softwareOsh.setStringAttribute("release", getNodeAttribute(application, "release"))
            softwareOsh.setStringAttribute("description", getNodeAttribute(application, "verdesc"))
            softwareOsh.setStringAttribute("supported_operation_systems", getNodeAttribute(application, "os"))
            softwareOsh.setStringAttribute("component", getNodeAttribute(application, "component"))
            if "visual studio" in name.lower():
                version = getNodeAttribute(application, "version")
                if "pro" in version:
                    softwareOsh.setStringAttribute("edition", VSEdition.pro)
                elif "express" in version:
                    softwareOsh.setStringAttribute("edition", VSEdition.express)
                elif "pre" in version:
                    softwareOsh.setStringAttribute("edition", VSEdition.pre)
                elif "ent" in version:
                    softwareOsh.setStringAttribute("edition", VSEdition.ent)
                elif "ult" in version:
                    softwareOsh.setStringAttribute("edition", VSEdition.ult)
                elif "community" in version:
                    softwareOsh.setStringAttribute("edition", VSEdition.community)
                elif "std" in version:
                    softwareOsh.setStringAttribute("edition", VSEdition.std)
            else:
                softwareOsh.setStringAttribute("edition", getNodeAttribute(application, "edition"))
            softwareOsh.setStringAttribute("service_pack", getNodeAttribute(application, "servicepack"))
            softwareOsh.setAttribute("recognition_level", recognitionLevelStr)
            softwareOsh.setStringAttribute("software_type", getNodeAttribute(application, "type"))
            softwareOsh.setIntegerAttribute("software_category_id", int(getNodeAttribute(application, "typeid", '0')))
            softwareOsh.setIntegerAttribute("sai_version_id", int(getNodeAttribute(application, "versionid", '0')))
            softwareOsh.setEnumAttribute("software_license_type", licenseType)
            # map installation package type, like APP-v
            softwareOsh.setEnumAttribute("installation_package_type",
                                         int(getNodeAttribute(application, "applicationtype", '0')))
            softwareOsh.setIntegerAttribute("usage_days_last_month",
                                            int(getNodeAttribute(application, "usagedayslastmonth", '0')))
            softwareOsh.setIntegerAttribute("usage_days_last_quarter",
                                            int(getNodeAttribute(application, "usagedayslastquarter", '0')))
            softwareOsh.setIntegerAttribute("usage_days_last_year",
                                            int(getNodeAttribute(application, "usagedayslastyear", '0')))
            softwareOsh.setFloatAttribute("usage_hours_last_month",
                                          float(getNodeAttribute(application, "usagehourslastmonth", '0')))
            softwareOsh.setFloatAttribute("usage_hours_last_quarter",
                                          float(getNodeAttribute(application, "usagehourslastquarter", '0')))
            softwareOsh.setFloatAttribute("usage_hours_last_year",
                                          float(getNodeAttribute(application, "usagehourslastyear", '0')))
            softwareOsh.setFloatAttribute("usage_hours_last_year_daily_peak",
                                          float(getNodeAttribute(application, "usagedailypeak", '0')))
            softwareOsh.setFloatAttribute("usage_percent", float(getNodeAttribute(application, "usagepercent", '0')))
            # include in-focus software utilization information
            softwareOsh.setIntegerAttribute("infocus_usage_days_last_month",
                                            int(getNodeAttribute(application, "usagedayslastmonthfoc", '0')))
            softwareOsh.setIntegerAttribute("infocus_usage_days_last_quarter",
                                            int(getNodeAttribute(application, "usagedayslastquarterfoc", '0')))
            softwareOsh.setIntegerAttribute("infocus_usage_days_last_year",
                                            int(getNodeAttribute(application, "usagedayslastyearfoc", '0')))
            softwareOsh.setFloatAttribute("infocus_usage_hours_last_month",
                                          float(getNodeAttribute(application, "usagehourslastmonthfoc", '0')))
            softwareOsh.setFloatAttribute("infocus_usage_hours_last_quarter",
                                          float(getNodeAttribute(application, "usagehourslastquarterfoc", '0')))
            softwareOsh.setFloatAttribute("infocus_usage_hours_last_year",
                                          float(getNodeAttribute(application, "usagehourslastyearfoc", '0')))
            # softwareOsh.setFloatAttribute("infocus_usage_hours_last_year_daily_average", float(getNodeAttribute(application, "usagedailyaveragefoc", '0')))
            softwareOsh.setFloatAttribute("infocus_usage_hours_last_year_daily_peak",
                                          float(getNodeAttribute(application, "usagedailypeakfoc", '0')))
            softwareOsh.setFloatAttribute("infocus_usage_percent",
                                          float(getNodeAttribute(application, "usagepercentfoc", '0')))
            if len(getNodeAttribute(application, "usagedayslastmonth")):
                softwareOsh.setDateAttribute("utilization_update_date", Date())
            userlist = getUserList(application, usagesArray, usersArray)
            softwareOsh.setListAttribute("utilization_user_list", userlist)
            # To avoid the capacity risk, set a threshold on the number of users as
            # we have no way to tell reliably if Terminal Services or Citrix is in use
            versionid = getNodeAttribute(application, "versionid")
            if versionid:
                newSoftwareEntry = [softwareOsh]
                licencedBy = getNodeAttribute(application, "licencedby")
                if licencedBy:
                    newSoftwareEntry.append(int(licencedBy))
                oldSoftwareEntry = softwares.get(int(versionid))
                if not oldSoftwareEntry or len(oldSoftwareEntry) < len(newSoftwareEntry):
                    softwares[int(versionid)] = newSoftwareEntry
            if userNumberThreshold and len(userlist) >= userNumberThreshold:
                # versionid is connection between <applicationdata/> and <applicationusage/>
                for usage in usagesArray:
                    vid = getNodeAttribute(usage, "versionid")
                    if vid == versionid:
                        createSoftwareUtilizationOSH(oshvresults, softwareOsh, usage, usersArray)
            softwareOsh.setContainer(hostOsh)
            oshvresults.add(softwareOsh)
    return softwares


# get list of users that use the current software
def getUserList(application, usagesArray, usersArray):
    userlist = []
    appVerId = getNodeAttribute(application, "versionid")
    for usage in usagesArray:
        vid = getNodeAttribute(usage, "versionid")
        if vid == appVerId:
            username = getUserName(usersArray, getNodeAttribute(usage, "userid"))
            if not re.search('^ALL USERS', username, re.IGNORECASE):
                userlist.append(username)
    return userlist


# create per-user software utilization mapping
def createSoftwareUtilizationOSH(oshvresults, softwareOsh, usage, usersArray):
    username = getUserName(usersArray, getNodeAttribute(usage, "userid"))
    if len(username) and not re.search('^ALL USERS', username, re.IGNORECASE):
        su = ObjectStateHolder("user_software_utilization")
        su.setStringAttribute("user_name", username)
        su.setIntegerAttribute("usage_days_last_month", int(getNodeAttribute(usage, "usagedayslastmonth", '0')))
        su.setIntegerAttribute("usage_days_last_quarter", int(getNodeAttribute(usage, "usagedayslastquarter", '0')))
        su.setIntegerAttribute("usage_days_last_year", int(getNodeAttribute(usage, "usagedayslastyear", '0')))
        su.setFloatAttribute("usage_hours_last_month", float(getNodeAttribute(usage, "usagehourslastmonth", '0')))
        su.setFloatAttribute("usage_hours_last_quarter", float(getNodeAttribute(usage, "usagehourslastquarter", '0')))
        su.setFloatAttribute("usage_hours_last_year", float(getNodeAttribute(usage, "usagehourslastyear", '0')))
        su.setFloatAttribute("usage_hours_last_year_daily_peak", float(getNodeAttribute(usage, "usagedailypeak", '0')))
        su.setFloatAttribute("usage_percent", float(getNodeAttribute(usage, "usagepercent", '0')))
        dateFormatter = SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
        usageLastUsedDateStr = getNodeAttribute(usage, "usagelastused")
        if len(usageLastUsedDateStr):
            usageLastUsedDate = dateFormatter.parse(usageLastUsedDateStr)
            su.setDateAttribute("usage_last_used", usageLastUsedDate)
        inFocusUsageLastUsedDateStr = getNodeAttribute(usage, "usagelastusedfoc")
        if len(inFocusUsageLastUsedDateStr):
            inFocusUsageLastUsedDate = dateFormatter.parse(inFocusUsageLastUsedDateStr)
            su.setDateAttribute("infocus_usage_last_used", inFocusUsageLastUsedDate)
        # include in-focus information
        su.setIntegerAttribute("infocus_usage_days_last_month",
                               int(getNodeAttribute(usage, "usagedayslastmonthfoc", '0')))
        su.setIntegerAttribute("infocus_usage_days_last_quarter",
                               int(getNodeAttribute(usage, "usagedayslastquarterfoc", '0')))
        su.setIntegerAttribute("infocus_usage_days_last_year",
                               int(getNodeAttribute(usage, "usagedayslastyearfoc", '0')))
        su.setFloatAttribute("infocus_usage_hours_last_month",
                             float(getNodeAttribute(usage, "usagehourslastmonthfoc", '0')))
        su.setFloatAttribute("infocus_usage_hours_last_quarter",
                             float(getNodeAttribute(usage, "usagehourslastquarterfoc", '0')))
        su.setFloatAttribute("infocus_usage_hours_last_year",
                             float(getNodeAttribute(usage, "usagehourslastyearfoc", '0')))
        su.setFloatAttribute("infocus_usage_hours_last_year_daily_peak",
                             float(getNodeAttribute(usage, "usagedailypeakfoc", '0')))
        su.setFloatAttribute("infocus_usage_percent", float(getNodeAttribute(usage, "usagepercentfoc", '0')))
        su.setContainer(softwareOsh)
        oshvresults.add(su)


def getUserName(usersArray, userid):
    for user in usersArray:
        _id = user.getAttribute("id")
        if userid == _id:
            return user.getAttribute("name").strip()
    return ''


def createOsInstalledSoftware(oshvresults, root, hostOsh, mappingConfig):
    dateFormatter = SimpleDateFormat("yyyy-MM-dd")
    dateFormatter2 = SimpleDateFormat("EEE MMM dd hh:mm:ss z yyyy")
    dateFormatter3 = SimpleDateFormat("yyyyMMdd")
    osInstalledApps = root.getElementsByTagName("hwOSInstalledApps_value")
    osInstalledAppsArray = nodeListToArray(osInstalledApps)
    for application in osInstalledAppsArray:
        name = getNodeValues("hwOSInstalledAppName", application)[0]
        if not len(name):
            name = getNodeValues("hwOSInstalledAppDescription", application)[0]
        vendor = getNodeValues("hwOSInstalledAppPublisher", application)[0]
        version = getNodeValues("hwOSInstalledAppVersion", application)[0]
        softwareOsh = handleRawApplications(mappingConfig, name, vendor, version)
        if softwareOsh:
            mapStringAttribute(softwareOsh, "release", "hwOSInstalledAppRelease", application)
            softwareOsh.setAttribute("recognition_level", RECOGNITION_LEVEL_RAW)
            # todo use unified api to set install path to prevent duplicate installed software CIs
            mapStringAttribute(softwareOsh, "file_system_path", "hwOSInstalledAppInstallDir", application)
            # map installation source, like Mac App Store, Microsoft App Store
            mapStringAttribute(softwareOsh, "installation_source", "hwOSInstalledAppSource", application)
            # map installation package type, like APP-v
            softwareOsh.setEnumAttribute("installation_package_type",
                                         int(getNodeEnumAttribute(application, "hwOSInstalledAppPackageType")))
            productid = getNodeValues("hwOSInstalledAppProductID", application)[0]
            if len(productid):
                softwareOsh.setStringAttribute("software_productid", productid)
                appType = int(getNodeEnumAttribute(application, "hwOSInstalledAppPackageType"))
                if re.search('^KB', productid, re.IGNORECASE) and appType == 7:
                    name = name + '-' + productid
                    logger.debug('name changed to:', name)
                    softwareOsh.setStringAttribute("name", name)
            lastUsedDateStr = getNodeValues("hwOSInstalledAppLastExecuted", application)[0]
            if len(lastUsedDateStr):
                lastUsedDate = dateFormatter.parse(lastUsedDateStr)
                softwareOsh.setDateAttribute("last_used_date", lastUsedDate)

            installDateStr = getNodeValues("hwOSInstalledAppInstallDate", application)[0]
            if len(installDateStr):
                installDate = None
                try:
                    installDate = dateFormatter2.parse(installDateStr)
                except:
                    try:
                        installDate = dateFormatter3.parse(installDateStr)
                    except:
                        logger.warn('Unparseable installation date: ', installDateStr)
                        pass
                if installDate:
                    softwareOsh.setDateAttribute("installation_date", installDate)
            comments = getNodeValues("hwOSInstalledAppComments", application)[0]
            if comments:
                softwareOsh.setStringAttribute("comments", comments)

            softwareOsh.setContainer(hostOsh)
            oshvresults.add(softwareOsh)


def handleRawApplications(mappingConfig, name, vendor=None, version=None, desc=None):
    if not mappingConfig.rawApp:
        return None
    inclPatternStr = mappingConfig.includeValueForRaw
    exclPatternStr = mappingConfig.excludeValueForRaw
    # default to exclude all
    if not len(inclPatternStr) and not len(exclPatternStr):
        return None
    return processIncludeExclude(inclPatternStr, exclPatternStr, name, vendor, version, desc)


def parsePatterns(patternValues):
    namePattern = patternValues.get('name')
    versionPattern = patternValues.get('version')
    vendorPattern = patternValues.get('discovered_vendor')
    descPattern = patternValues.get('description')
    return namePattern, versionPattern, vendorPattern, descPattern


def handleNormalApplications(mappingConfig, name, vendor=None, version=None, desc=None):
    if not mappingConfig.normalApp:
        return None
    exclPatternStr = mappingConfig.excludeValueForNormal
    inclPatternStr = mappingConfig.includeValueForNormal
    return processIncludeExclude(inclPatternStr, exclPatternStr, name, vendor, version, desc)


def processIncludeExclude(inclPatternStr, exclPatternStr, name, vendor, version, desc):
    nameInclPattern, versionInclPattern, vendorInclPattern, descInclPattern = parsePatterns(inclPatternStr)
    nameExclPattern, versionExclPattern, vendorExclPattern, descExclPattern = parsePatterns(exclPatternStr)
    softwareOsh = None
    if passAndMatchPattern(nameInclPattern, name) and passAndMatchPattern(versionInclPattern,
                                                                          version) and passAndMatchPattern(
            vendorInclPattern, vendor) and passAndMatchPattern(descInclPattern, desc):
        softwareOsh = createInstalledSoftwareOsh(name, vendor, version, desc)
    if exclPatternStr and len(exclPatternStr) and passAndMatchPattern(nameExclPattern, name) and passAndMatchPattern(
            versionExclPattern, version) and passAndMatchPattern(vendorExclPattern, vendor) and passAndMatchPattern(
            descExclPattern, desc):
        softwareOsh = None
    return softwareOsh


def passAndMatchPattern(pattern, content):
    if not pattern or not len(pattern):
        return 1
    if not content:
        content = ''
    return re.search(pattern, content)


def createInstalledSoftwareOsh(name, vendor=None, version=None, desc=None):
    if not len(name):  # key attribute
        return None
    softwareOsh = ObjectStateHolder("installed_software")
    softwareOsh.setStringAttribute("name", name)
    if vendor and len(vendor):
        if vendor == 'Microsoft Corporation':
            vendor = 'Microsoft'
        softwareOsh.setStringAttribute("discovered_vendor", vendor)
    if version and len(version):
        softwareOsh.setStringAttribute("version", version)
    if desc and len(desc):
        softwareOsh.setStringAttribute("description", desc)
    return softwareOsh


def mapRunningProcess(OSHVResult, root, nodeOSH, Framework, isManual):
    processList = []
    tcpList = []
    runningProcessElements = root.getElementsByTagName('hwRunningProcess_value')
    if not runningProcessElements:
        return

    # parsing...
    runningProcessArray = nodeListToArray(runningProcessElements)
    for runningProcess in runningProcessArray:
        process, tcps = parseProcessesAndTCPs(runningProcess, root)
        if process:
            processList.append(process)
        if tcps:
            tcpList.extend(tcps)

    # report all ipserviceendpoint in status "listen" and "established"
    discoverIpServiceEndpoint = Boolean.parseBoolean(Framework.getParameter('discoverPorts'))
    if discoverIpServiceEndpoint:
        # set license type to UDF if need to report all ports
        nodeOSH.setBoolAttribute("lic_type_udf", 1)
        reportIpServiceEndpoint(Framework, OSHVResult, nodeOSH, tcpList)

    discoverProcesses = Boolean.parseBoolean(Framework.getParameter('discoverProcesses'))
    if discoverProcesses:
        # set license type to UDF if need to report all processes
        nodeOSH.setBoolAttribute("lic_type_udf", 1)
        reportAllProcesses(Framework, OSHVResult, nodeOSH, processList)
    # report...
    logger.debug('Start to report process...')
    try:
        reportProcessAndTCP(processList, tcpList, Framework, OSHVResult, root, nodeOSH, isManual)
    except:
        logger.reportWarning()
        logger.reportWarning('Failed to report running process')


class TCPConnection:
    def __init__(self, pid, processName, localIP, localPort, foreignIP, foreignPort, portStatus, protocol):
        self.pid = pid
        self.processName = processName
        self.localIP = localIP
        self.localPort = localPort
        self.foreignIP = foreignIP
        self.foreignPort = foreignPort
        self.isIPv6 = False
        self.protocol = None
        if protocol:
            protocol = protocol.lower()
            if protocol.find('tcp') != -1:
                self.protocol = modeling.TCP_PROTOCOL
                if protocol.find('tcp6') != -1:
                    self.isIPv6 = True
            elif protocol.find('udp') != - 1:
                self.protocol = modeling.UDP_PROTOCOL
                if protocol.find('udp6') != -1:
                    self.isIPv6 = True

        self.portStatus = portStatus
        self.isListening = None
        if self.protocol == modeling.TCP_PROTOCOL:
            self.isListening = portStatus and portStatus.upper() == 'LISTEN'
        elif self.protocol == modeling.UDP_PROTOCOL:
            self.isListening = True

    def isUDP(self):
        return self.protocol == modeling.UDP_PROTOCOL

    def isTCP(self):
        return self.protocol == modeling.TCP_PROTOCOL

    def __str__(self):
        return "pid:%s, processName:%s, localIP:%s, localPort:%s, foreignIP:%s, foreignPort:%s, isListening:%s" \
               % (
                   self.pid, self.processName, self.localIP, self.localPort, self.foreignIP, self.foreignPort,
                   self.isListening)


def parseProcess(runningProcess, root):
    process = None
    processName = None
    pid = getNodeValues('hwRunningProcessPID', runningProcess)[0]
    if pid:
        processName = getNodeValues('hwRunningProcessName', runningProcess)[0]
        owner = getNodeValues('hwRunningProcessUser', runningProcess)[0]
        processPath = getNodeValues('hwRunningProcessPath', runningProcess)[0]
        commandLine = getNodeValues('hwRunningProcessCmdLine', runningProcess)[0]
        startupDateStr = ''
        if isWindows(root):
            commandLine, executablePath, commandName, argumentLine = parseWindowProcess(commandLine, processPath,
                                                                                        processName)
        else:
            commandLine, executablePath, commandName, argumentLine = parseUnixProcess(commandLine, processPath,
                                                                                      processName)

        if commandName:
            process = process_module.Process(commandName, pid, commandLine)
            process.owner = owner
            process.executablePath = executablePath
            process.argumentLine = argumentLine

            if startupDateStr:
                try:
                    startupDate = modeling.getDateFromString(startupDateStr, 'MMM dd HH:mm:ss yyyy')
                    process.setStartupTime(startupDate)
                except:
                    logger.warn("Failed to parse startup time from value '%s'" % startupDateStr)
        else:
            logger.warn("Ignore pid " + pid + " because it has no process name.")

    return process, processName


def __fixMissedProcessNameInCommandLine(name, cmdLine):
    matchObj = re.match(r'(:?["\'](.*?)["\']|(.*?)\s)', cmdLine)
    if matchObj:
        firstCmdToken = matchObj.group(1).strip()
    else:
        firstCmdToken = cmdLine.strip()
        # remove quotes
    firstCmdToken = re.sub('[\'"]', '', firstCmdToken).lower()
    # token has to end with process name
    processNameLower = name.lower()
    if not firstCmdToken.endswith(processNameLower):
        extStartPos = processNameLower.rfind('.')
        if extStartPos != -1:
            pnameNoExt = processNameLower[0:extStartPos]
            if not firstCmdToken.endswith(pnameNoExt):
                cmdLine = '%s %s' % (name, cmdLine)
    return cmdLine


def parseWindowProcess(commandLine, processPath, processName):
    commandName = processName
    argumentLine = None
    if commandLine:
        commandLine = __fixMissedProcessNameInCommandLine(processName, commandLine)
        commandLine = commandLine and commandLine.strip()
        argsMatch = re.match('("[^"]+"|[^"]\S+)\s+(.+)$', commandLine)
        if argsMatch:
            argumentLine = argsMatch.group(2)
    return commandLine, processPath, commandName, argumentLine


def parseUnixProcess(commandLine, processPath, processName):
    fullCommand = processPath
    argumentsLine = None
    if commandLine:
        tokens = re.split(r"\s+", commandLine, 1)
        fullCommand = tokens[0]
        if len(tokens) > 1:
            argumentsLine = tokens[1]
    commandName = processName
    commandPath = processPath
    if not re.match(r"\[", fullCommand):
        matcher = re.match(r"(.*/)([^/]+)$", fullCommand)
        if matcher:
            commandPath = fullCommand
            commandName = matcher.group(2)
    return commandLine, commandPath, commandName, argumentsLine


def parseProcessesAndTCPs(runningProcess, root):
    process, processName = parseProcess(runningProcess, root)

    tcp_list = []
    # parsing tcp connection
    tcpElements = runningProcess.getElementsByTagName('hwTCPIPConnectivity_value')
    if tcpElements:
        tcpArray = nodeListToArray(tcpElements)
        for tcp in tcpArray:
            if tcp and tcp.getChildNodes() and tcp.getChildNodes().getLength():
                pid = getNodeValues('hwTCPIPConnectivityPID', tcp)[0]
                localIP = getNodeValues('hwTCPIPConnectivityLocalIP', tcp)[0]
                localPort = getNodeValues('hwTCPIPConnectivityLocalPort', tcp, ['0'])[0]
                foreignIP = getNodeValues('hwTCPIPConnectivityForeignIP', tcp)[0]
                foreignPort = getNodeValues('hwTCPIPConnectivityForeignPort', tcp, ['0'])[0]
                portStatus = getNodeValues('hwTCPIPConnectivityStatus', tcp)[0]
                protocol = getNodeValues('hwTCPIPConnectivityProtocol', tcp)[0]
                if pid:
                    con = TCPConnection(pid, processName, localIP, localPort, foreignIP, foreignPort, portStatus,
                                        protocol)
                    tcp_list.append(con)
    return process, tcp_list


def storeAndReportProcess(Framework, OSHVResult, hostId, nodeOSH, processes):
    processId2OSH = {}
    try:
        # save processes to DB
        if hostId:
            process_discoverer.saveProcessesToProbeDb(processes, hostId, Framework)  # table: Processes
        else:
            logger.debug("Manual inventory discovery job, hostID not available, skip process to DB.")
        # report processes
        discoverProcesses = Boolean.parseBoolean(Framework.getParameter('discoverProcesses'))
        if discoverProcesses:
            processReporter = process_module.Reporter()
            for processObject in processes:
                processesVector = processReporter.reportProcess(nodeOSH, processObject)
                OSHVResult.addAll(processesVector)
                for processOsh in processesVector:
                    processId2OSH[processObject.getPid()] = processOsh
                    break
        return processId2OSH
    except:
        logger.reportWarning()
        logger.reportWarning('Failed to report process')


def processApplicationSignature(Framework, OSHVResult, connectivityEndPoints, hostId, NodeOSH, processes, root):
    logger.debug("Begin process application...")
    try:
        appSign = applications.createApplicationSignature(Framework, None)

        if not processes:
            logger.debug("No processes reported. Exiting application recognition")
            return
        appSign.setProcessesManager(applications.ProcessesManager(processes, connectivityEndPoints))
        softNameToInstSoftOSH, servicesByCmd = getInstalledSoftwareAndServiceFromResult(OSHVResult)
        appSign.setServicesInfo(applications.ServicesInfo(servicesByCmd))
        appSign.setInstalledSoftwareInfo(applications.InstalledSoftwareInfo(None, softNameToInstSoftOSH))

        logger.debug('Starting application recognized')
        if not appSign.connectionIp:
            logger.debug("Manual inventory discovery job, set node primary ip as conntionIp")
            appSign.connectionIp = NodeOSH.getAttribute("primary_ip_address").getValue()
        appSign._applicationIpStrategy = applications.ApplicationIpSelectionStrategyByEndpoints(appSign.connectionIp)
        appSign.getApplicationsTopology(hostId)
        Framework.clearState()  # avoid to conflict with call home ip which is also stored in the state
        logger.debug('Finished application recognized')
    except:
        logger.reportWarning()
        logger.reportWarning('Failed to process by app signature')


def process2process(Framework):
    try:
        p2p = process_to_process.ProcessToProcess(Framework)  # Agg_V5 join Port_Process, Processes
        p2p.getProcessesToProcess()
    except:
        logger.reportWarning()
        logger.reportWarning('Failed to run p2p discovery')


def __filter_tcp_connection_by_processes(tcp_connections, processes):
    filtered_connections = []
    for process in processes:
        for tcp_connection in tcp_connections:
            if int(tcp_connection.pid) == process.getPid():
                filtered_connections.append(tcp_connection)
    return filtered_connections


def reportProcessAndTCP(processList, tcpList, Framework, OSHVResult, root, nodeOSH, isManual):
    hostId = Framework.getCurrentDestination().getHostId()
    connectivity_endpoints = __get_processes_conectivity_endpoints(Framework, tcpList)
    processes = applications.filter_required_processes(Framework, processList, connectivity_endpoints, nodeOSH)
    tcp_connections = __filter_tcp_connection_by_processes(tcpList, processes)

    # for manual, the hostId is actual a host node osh which just created, it is not applicable to store to db
    processIDOsh = {}
    logger.debug('Start to store and report processes...')
    processIDOsh = storeAndReportProcess(Framework, OSHVResult, hostId, nodeOSH, processes)

    logger.debug('Start to process tcp...')
    connectivityEndPoints = processTCP(Framework, tcp_connections)
    if not hostId:
        hostId = nodeOSH

    logger.debug('Start to process application signature...')
    processApplicationSignature(Framework, OSHVResult, connectivityEndPoints, hostId, nodeOSH, processes, root)

    if not isManual:
        logger.debug('Start to build relations for process...')
        process2process(Framework)


def reportAllProcesses(Framework, OSHVResult, nodeOSH, processList):
    logger.debug("report all processes")
    for process in processList:
        processOsh = modeling.createProcessOSH(process.getName(), nodeOSH, process.commandLine)
        OSHVResult.add(processOsh)


def reportIpServiceEndpoint(Framework, OSHVResult, nodeOSH, tcpList):
    logger.debug("report all listen and established ports")
    endpoints = getAllListenAndEstablishedPorts(Framework, tcpList)
    for endpoint in endpoints:
        endpointBuilder = netutils.ServiceEndpointBuilder()
        endpointReporter = netutils.EndpointReporter(endpointBuilder)
        ipServerOSH = endpointReporter.reportEndpoint(endpoint, nodeOSH)
        OSHVResult.add(ipServerOSH)


def getAllListenAndEstablishedPorts(Framework, tcpList):
    result = []
    discover = TCPDisByScanner(Framework, tcpList, True)
    if not discover.nodeIpList:
        logger.debug("Manual inventory discovery job, need to add parsed node ips from scan file.")
        map(discover.nodeIpList.append, Framework.getProperty("parsedNodeIPList"))
    for tcp in tcpList:
        if not tcp.protocol:
            logger.debug("No protocol found for connection, skip it.")
            continue
        if not tcp.protocol in [6, 17]:
            logger.debug("Unsupported protocol found for connection, skip it.", tcp.protocol)
            continue
        if tcp.protocol == 6 and (not tcp.portStatus in ["ESTABLISHED", "LISTEN"]):
            logger.debug("Useless port found, skip it.", tcp.portStatus)
            continue
        if tcp.localIP:
            allIPs = discover.getAllIPOfHost(tcp.localIP)
            for ip in allIPs:
                endpoint = netutils.Endpoint(tcp.localPort, tcp.protocol, ip, tcp.isListening)
                if endpoint:
                    result.append(endpoint)
        else:
            logger.debug("TCP local ip is invalid, skip it.")
    return result


def getInstalledSoftwareAndServiceFromResult(OSHVResult):
    softNameToInstSoftOSH = {}
    serviceNameToServiceOSH = {}

    iterator = OSHVResult.iterator()
    while iterator.hasNext():
        osh = iterator.next()
        oshClass = osh.getObjectClass()
        if oshClass == 'installed_software':
            name = osh.getAttributeValue("name")
            if name:
                softNameToInstSoftOSH[str(name)] = osh
        elif oshClass == 'windows_service':
            serviceCommandLine = osh.getAttributeValue("service_commandline")
            if serviceCommandLine:
                serviceNameToServiceOSH[CmdLine(str(serviceCommandLine).lower())] = osh
    return softNameToInstSoftOSH, serviceNameToServiceOSH


class TCPDisByScanner(Dis_TCP.TCPDiscovery):
    def __init__(self, Framework, tcpList, supportIPv6=False):
        Dis_TCP.TCPDiscovery.__init__(self, None, Framework)
        self.hostIps = None
        self.nodeIpList = Framework.getTriggerCIDataAsList('nodeIpList') or []
        self.tcpList = tcpList
        self.supportIPv6 = supportIPv6

    def isValidIPInSystem(self, ipObject):
        return not (not ipObject or ipObject.is_loopback or ipObject.is_multicast or ipObject.is_link_local) \
               and (ipObject.get_version() == 4 or (ipObject.get_version() == 6 and self.supportIPv6))

    def getValidIPObject(self, rawIP):
        if ip_addr.isValidIpAddress(rawIP):
            ipObject = ip_addr.IPAddress(rawIP)
            # filter client IP because client IP will cause reconciliation error in UCMDB(CR#89410).
            if self.isValidIPInSystem(ipObject) and not InventoryUtils.isClientTypeIP(str(ipObject)):
                return ipObject
        return None

    def getValidIPInString(self, rawIP):
        ipObject = self.getValidIPObject(rawIP)
        if ipObject:
            return str(ipObject)
        return None

    def getAllIPByVersion(self, version=4):
        allIPv6 = []
        for ip in self.nodeIpList:
            ipObject = self.getValidIPObject(ip)
            if ipObject and ipObject.get_version() == version:
                allIPv6.append(str(ipObject))
        return allIPv6

    def getAllIPv4Address(self):
        return self.getAllIPByVersion(4)

    def getAllIPv6Address(self):
        return self.getAllIPByVersion(6)

    def getAllIPOfHost(self, rawIP):
        if rawIP.find("*") >= 0:
            return self.getAllIPv4Address()
        ipObject = self.getValidIPObject(rawIP)
        if ipObject:
            # like 0.0.0.0 for ipv4 or :: for ipv6
            if ipObject.is_unspecified:
                if ipObject.get_version() == 4:
                    return self.getAllIPv4Address()
                elif ipObject.get_version() == 6:
                    return self.getAllIPv6Address()
            else:
                return [str(ipObject)]
        return []

    def get_connectivity_endpoint(self, ipaddress, port, process_pid, listen=0, protocol=modeling.TCP_PROTOCOL,
                                  ProcessName=None, listenIpPorts=None):
        port = int(port)
        ips = []
        if self.hostIps and (ipaddress.startswith('0.') or ipaddress.find("*") >= 0):
            ips.extend(self.hostIps)
        elif self.hostIps and (ipaddress == '::'):
            ips.extend(self.hostIps)  # TODO add only ipv6 addresses
        else:
            ips.append(ipaddress)

        processEndpoints = []
        for ip in ips:
            if not listenIpPorts:
                listenIpPorts = []
            listenIpPorts.append('%s:%s' % (ip, port))

            if ip and port:
                endpoint = netutils.Endpoint(port, protocol, ip, listen)
                processEndpoints.append(endpoint)
        if processEndpoints and process_pid:
            processPidInt = None
            try:
                processPidInt = int(process_pid)
            except:
                logger.warn("Failed to convert process PID to int")
            else:
                return netutils.ConnectivityEndpoint(processPidInt,
                                                     processEndpoints)

    def compose_processes_endpoints(self):
        result = []
        listenIpPorts = []
        for tcp in self.tcpList:
            logger.debug('Begin process tcp info:', tcp)
            if not tcp.protocol:
                logger.debug("No protocol found for connection, skip it.")
                continue
            if not self.supportIPv6 and tcp.isIPv6:
                logger.debug("Found ipv6 link, but not supported by setting, skip it.")
                continue
            if tcp.isListening:
                if tcp.localIP:
                    allIPs = self.getAllIPOfHost(tcp.localIP)
                    for ip in allIPs:
                        connectivity_endpoint = self.get_connectivity_endpoint(ip, tcp.localPort, tcp.pid, 1,
                                                                               tcp.protocol, tcp.processName,
                                                                               listenIpPorts)
                        if connectivity_endpoint:
                            result.append(connectivity_endpoint)
            else:
                if not tcp.portStatus or self.isUndefinedState(tcp.portStatus):
                    logger.debug('Found undefined links status:', tcp.portStatus, ', skip it.')
                elif tcp.isUDP():
                    logger.debug('Found not listen udp entry, skip it.')
                elif tcp.localPort == 0 or tcp.foreignPort == 0:
                    logger.debug('Found port:0, skip it.')
                else:
                    validLocalIP = self.getValidIPInString(tcp.localIP)
                    validForeignIP = self.getValidIPInString(tcp.foreignIP)
                    if not validLocalIP:
                        logger.debug('Local IP is invalid:', tcp.localIP)
                    elif not validForeignIP:
                        logger.debug('Remote IP is invalid:', tcp.foreignIP)
                    else:
                        localPort = int(long(tcp.localPort) & 0xffff)
                        ipPort = '%s:%d' % (validLocalIP, localPort)
                        if not ipPort in listenIpPorts:
                            connectivity_endpoint = self.get_connectivity_endpoint(validLocalIP, localPort, tcp.pid, 0,
                                                                                   modeling.TCP_PROTOCOL,
                                                                                   tcp.processName)
                            if connectivity_endpoint:
                                result.append(connectivity_endpoint)
        return result

    def _process(self):
        listenIpPorts = []
        for tcp in self.tcpList:
            logger.debug('Begin process tcp info:', tcp)
            if not tcp.protocol:
                logger.debug("No protocol found for connection, skip it.")
                continue
            if not self.supportIPv6 and tcp.isIPv6:
                logger.debug("Found ipv6 link, but not supported by setting, skip it.")
                continue
            if tcp.isListening:
                if tcp.localIP:
                    allIPs = self.getAllIPOfHost(tcp.localIP)
                    for ip in allIPs:
                        self._addTcpData(ip, tcp.localPort, tcp.pid, 1, tcp.protocol, tcp.processName, listenIpPorts)
            else:
                if not tcp.portStatus or self.isUndefinedState(tcp.portStatus):
                    logger.debug('Found undefined links status:', tcp.portStatus, ', skip it.')
                elif tcp.isUDP():
                    logger.debug('Found not listen udp entry, skip it.')
                elif tcp.localPort == 0 or tcp.foreignPort == 0:
                    logger.debug('Found port:0, skip it.')
                else:
                    validLocalIP = self.getValidIPInString(tcp.localIP)
                    validForeignIP = self.getValidIPInString(tcp.foreignIP)
                    if not validLocalIP:
                        logger.debug('Local IP is invalid:', tcp.localIP)
                    elif not validForeignIP:
                        logger.debug('Remote IP is invalid:', tcp.foreignIP)
                    else:
                        localPort = int(long(tcp.localPort) & 0xffff)
                        foreignPort = int(long(tcp.foreignPort) & 0xffff)
                        ipPort = '%s:%d' % (validLocalIP, localPort)
                        if not ipPort in listenIpPorts:
                            # Add the host:port pair if it is not added in listening mode
                            self._addTcpData(validLocalIP, localPort, tcp.pid, 0, modeling.TCP_PROTOCOL,
                                             tcp.processName)
                        self.pdu.addTcpConnection(validLocalIP, localPort, validForeignIP, foreignPort)

    def discoverTCP(self):
        try:
            self._process()
        except:
            logger.reportWarning()
        finally:
            try:
                self.pdu.flushPortToProcesses()
            except:
                logger.reportWarning()
            try:
                self.pdu.flushTcpConnection()
            except:
                logger.reportWarning()
            self.pdu.close()


def __get_processes_conectivity_endpoints(Framework, tcpList):
    tcpList.sort(key=lambda x: x.pid)
    collectIPv6Connectivity = Boolean.parseBoolean(Framework.getParameter('collectIPv6Connectivity'))
    discover = TCPDisByScanner(Framework, tcpList, collectIPv6Connectivity)
    try:
        result = discover.compose_processes_endpoints()
    finally:
        discover.pdu.close()

    return result


def processTCP(Framework, tcpList):
    tcpList.sort(key=lambda x: x.pid)
    collectIPv6Connectivity = Boolean.parseBoolean(Framework.getParameter('collectIPv6Connectivity'))
    discover = TCPDisByScanner(Framework, tcpList, collectIPv6Connectivity)
    if not discover.nodeIpList:
        logger.debug("Manual inventory discovery job, need to add parsed node ips from scan file.")
        map(discover.nodeIpList.append, Framework.getProperty("parsedNodeIPList"))
    discover.discoverTCP()
    return discover.getProcessEndPoints()


def createInterfaceOSH(oshvresults, root, hostOsh, Framework=None):
    interfaceWithMac = {}
    networkCards = root.getElementsByTagName("hwNetworkCards_value")
    networkCardsArray = nodeListToArray(networkCards)
    idx = 0
    ParsedNodeIPList = []
    for networkCard in networkCardsArray:
        interfaceName = getNodeValues("hwNICInterfaceName", networkCard)[0]
        interfaceDesc = getNodeValues("hwNICDescription", networkCard)[0]
        mac = getNodeValues("hwNICPhysicalAddress", networkCard)[0]
        interfaceType = InventoryUtils.getInterfaceType(getNodeValues("hwNICType", networkCard)[0])
        interfaceSpeed = getNodeValues("hwNICCurrentSpeed", networkCard, [0])[0]
        convertInterfaceSpeed = None
        if interfaceSpeed is not None:
            convertInterfaceSpeed = long(interfaceSpeed) * 1000 * 1000
        interfaceOsh = modeling.createInterfaceOSH(mac, hostOSH=hostOsh, description=interfaceDesc, index=idx + 1,
                                                   type=interfaceType, speed=convertInterfaceSpeed, name=interfaceName)
        if interfaceOsh:
            oshvresults.add(interfaceOsh)
            hasValidMac = netutils.isValidMac(mac)
            interface_role = []
            if (not hasValidMac) or isVirtualInterface(networkCard):
                interface_role.append("virtual_interface")
            else:
                interface_role.append("physical_interface")
            interfaceOsh.setListAttribute("interface_role", interface_role)
            gateways = getNodeValues("hwNICGateway", networkCard)
            interfaceOsh.setListAttribute("gateways", gateways)
            mapStringAttribute(interfaceOsh, "primary_wins", "hwNICPrimaryWins", networkCard)
            mapStringAttribute(interfaceOsh, "secondary_wins", "hwNICSecondaryWins", networkCard)
            ipAddresses = networkCard.getElementsByTagName("hwNICIPAddresses_value")
            ipAddressesArray = nodeListToArray(ipAddresses)
            for ipAddress in ipAddressesArray:
                try:
                    ip = getNodeValues("hwNICIPAddress", ipAddress)[0]
                    netmask = getNodeValues("hwNICSubnetMask", ipAddress)[0]
                    match = re.search('^(\d+\.\d+\.\d+\.\d+)$', ip) or re.search('([\da-fA-F\:]+)', ip)
                    if match:
                        ipaddr = getValidIP(match.group(1).strip())
                        if ipaddr:
                            ParsedNodeIPList.append(str(ipaddr))
                            ipVersion = ipaddr.version
                            netmask = getNodeValues("hwNICSubnetMask", ipAddress)[0]
                            flag = getNodeValues("hwNICIPAddressFlags", ipAddress)[0]
                            if flag == PRIMARY_IP_ADDRESS_FLAG and hasValidMac:
                                hostOsh.setStringAttribute("primary_mac_address", mac)
                            # for ipv4 we need to filter all the local ips
                            if ipVersion == 4:
                                if len(netmask) and not netutils.isLocalIp(ip):
                                    netmaskArray = netmask.split('.')
                                    if len(
                                            netmaskArray) == 1:  # in some cases, the subnet mask is represented as a number such as 8
                                        netmask = formatNetmask(netmaskArray[0])
                                ipProps = modeling.getIpAddressPropertyValue(str(ipaddr), netmask,
                                                                             dhcpEnabled=isDhcpEnabled(networkCard),
                                                                             interfaceName=interfaceName)
                                ipOsh = modeling.createIpOSH(ipaddr, netmask=netmask, ipProps=ipProps)
                            elif ipVersion == 6:
                                ipProps = None
                                if isDhcpEnabled(networkCard):
                                    ipProps = modeling.IP_ADDRESS_PROPERTY_DHCP
                                ipOsh = modeling.createIpOSH(ipaddr, ipProps=ipProps)
                            ipOsh.setAttribute("ip_address_type", mapIpAddressType(ipVersion))
                            oshvresults.add(ipOsh)
                            oshvresults.add(modeling.createLinkOSH("containment", interfaceOsh, ipOsh))
                            oshvresults.add(modeling.createLinkOSH("containment", hostOsh, ipOsh))
                except:
                    logger.debug('Failed to create IpOSH with ip: ', ipaddr,
                                 ', and net mask: ', netmask)

            mapScanFile(oshvresults, root, hostOsh, interfaceOsh, networkCard, idx)
            interfaceWithMac[mac] = interfaceOsh
        idx += 1
    if Framework:
        Framework.setProperty("parsedNodeIPList", ParsedNodeIPList)
    return interfaceWithMac


def isVirtualInterface(networkCard):
    interfaceDesc = getNodeValues("hwNICDescription", networkCard)[0]
    if interfaceDesc:
        for signature in InterfaceRoleManager._VIRTUAL_NIC_SIGNATURES:
            if re.search(signature, interfaceDesc, re.I):
                return 1
    return 0


def createScannerOSH(Framework, oshvresults, root, hostOsh, filePath):
    dateFormatter = SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    invScanner = ObjectStateHolder("inventory_scanner")
    scanfilePath = getNodeValues("processedscanfile", root)[0]
    probeName = CollectorsParameters.getValue(CollectorsParameters.KEY_COLLECTORS_PROBE_NAME)
    invScanner.setStringAttribute("processed_scan_file_path", scanfilePath)
    invScanner.setStringAttribute("processed_scan_file_probe", probeName)
    invScanner.setStringAttribute("version", getNodeValues("hwScannerVersion", root)[0])
    invScanner.setStringAttribute("scanner_command_line", getNodeValues("hwScanCmdLine", root)[0])
    invScanner.setStringAttribute("scanner_configuration", getScannerConfigFileName(Framework))
    scannerType = getNodeValues("hwCreationMethod", root)[0]
    invScanner.setEnumAttribute("scanner_type",
                                int(getNodeEnumAttribute(root, "hwCreationMethod", '4')))  # required attribute
    mapStringAttribute(invScanner, "description", "hwScannerDescription", root)
    invScanner.setStringAttribute("discovered_product_name", "Inventory Scanner")  # used in reconcilliation
    invScanner.setLongAttribute("files_total", long(getNodeValues("hwFilesTotal", root, ['0'])[0]))
    invScanner.setLongAttribute("files_processed", long(getNodeValues("hwFilesProcessed", root, ['0'])[0]))
    invScanner.setLongAttribute("files_recognized", long(getNodeValues("hwFilesRecognised", root, ['0'])[0]))
    invScanner.setIntegerAttribute("scan_duration", int(getNodeValues("hwScanDuration", root, ['0'])[0]))
    scanDateStr = getNodeValues("hwScanDate", root)[0]
    if len(scanDateStr):
        scanDate = dateFormatter.parse(scanDateStr)
        invScanner.setDateAttribute("startup_time", scanDate)
    upgradeState = Framework.getProperty(InventoryUtils.SCANNER_UPGRADE_STATE)
    if upgradeState == '1':
        upgradeDate = Framework.getProperty(InventoryUtils.SCANNER_UPGRADE_DATE)
        invScanner.setDateAttribute("upgrade_date", upgradeDate)

    scanFileLastDownloadedTime = Framework.getProperty(InventoryUtils.AGENT_OPTION_DISCOVERY_SCANFILE_DOWNLOAD_TIME)
    if scanFileLastDownloadedTime:
        invScanner.setDateAttribute('scan_file_last_downloaded_time', scanFileLastDownloadedTime)
    elif filePath:
        try:
            mTime = os.stat(filePath)[ST_MTIME]
            scanDownloadedTimeStr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(mTime))
            scanDownloadedDate = dateFormatter.parse(scanDownloadedTimeStr)
            invScanner.setDateAttribute('scan_file_last_downloaded_time', scanDownloadedDate)
        except:
            logger.warn("Failed to get the scan file last downloaded time")
    invScanner.setContainer(hostOsh)
    oshvresults.add(invScanner)


# note: for manually scan job we cannot get a valid config file name because user can use any kind of cxz files they
# generated and they could also rename it as they like when running the scanner command.
def getScannerConfigFileName(Framework):
    scannerConfigFile = ''
    scannerConfigPerPlatformSettings = Framework.getParameter('ScannerConfigurationFile')
    if scannerConfigPerPlatformSettings and len(scannerConfigPerPlatformSettings):
        scannerConfig = ScannerConfigurationUtil.getInstance().loadScannerConfigurationPerPlatformWrapper(
            scannerConfigPerPlatformSettings)
        platform = Framework.getProperty(InventoryUtils.STATE_PROPERTY_PLATFORM)
        architecture = Framework.getProperty(InventoryUtils.STATE_PROPERTY_ARCHITECTURE)
        scannerConfigFile = scannerConfig.getScannerNameForPlatform(platform, architecture)
    return scannerConfigFile


def createWindowsServiceOSH(oshvresults, root, hostOsh):
    winServices = root.getElementsByTagName("hwOSServices_value")
    winServicesArray = nodeListToArray(winServices)
    idx = 0
    for winService in winServicesArray:
        winServiceOsh = ObjectStateHolder("windows_service")
        winServiceOsh.setStringAttribute("name",
                                         getNodeValues("hwOSServiceDisplayName", winService)[0])  # key attribute
        serviceTypes = getNodeValues("hwOSServiceType", winService)[0]
        serviceTypesArray = serviceTypes.split(',')
        winServiceOsh.setListAttribute("service_type", serviceTypesArray)
        mapStringAttribute(winServiceOsh, "service_starttype", "hwOSServiceStartup", winService)
        mapStringAttribute(winServiceOsh, "service_operatingstatus", "hwOSServiceStatus", winService)
        mapStringAttribute(winServiceOsh, "service_commandline", "hwOSServiceFileName", winService)
        mapStringAttribute(winServiceOsh, "service_description", "hwOSServiceDescription", winService)
        mapStringAttribute(winServiceOsh, "service_startuser", "hwOSServiceUser", winService)
        mapStringAttribute(winServiceOsh, "service_name", "hwOSServiceName", winService)
        winServiceOsh.setContainer(hostOsh)
        oshvresults.add(winServiceOsh)
        mapScanFile(oshvresults, root, hostOsh, winServiceOsh, winService, idx)
        idx += 1


def createDaemonOSH(oshvresults, root, hostOsh):
    daemons = root.getElementsByTagName("hwOSServices_value")
    daemonsArray = nodeListToArray(daemons)
    idx = 0
    for daemon in daemonsArray:
        daemonOsh = ObjectStateHolder("daemon")
        daemonOsh.setStringAttribute("name", getNodeValues("hwOSServiceName", daemon)[0])  # key attribute
        mapStringAttribute(daemonOsh, "daemon_path", "hwOSServiceFileName", daemon)
        daemonOsh.setContainer(hostOsh)
        oshvresults.add(daemonOsh)
        mapScanFile(oshvresults, root, hostOsh, daemonOsh, daemon, idx)
        idx += 1


def createGraphicsAdapterOSH(oshvresults, root, hostOsh, idx):
    graphAdapters = root.getElementsByTagName("hwDisplayGraphicsAdapters_value")
    graphAdaptersArray = nodeListToArray(graphAdapters)
    for graphAdapter in graphAdaptersArray:
        graphAdapterOsh = ObjectStateHolder("graphics_adapter")
        graphAdapterOsh.setStringAttribute("name", getNodeValues("hwDisplayGraphicsAdapterName", graphAdapter)[
            0])  # key attribute
        resolutionX = int(getNodeValues("hwDisplayDesktopResolutionX", graphAdapter, ['0'])[0])
        if resolutionX:
            graphAdapterOsh.setIntegerAttribute("current_display_mode_resolution_x", resolutionX)
        resolutionY = int(getNodeValues("hwDisplayDesktopResolutionY", graphAdapter, ['0'])[0])
        if resolutionY:
            graphAdapterOsh.setIntegerAttribute("current_display_mode_resolution_y", resolutionY)
        colourDepth = int(getNodeValues("hwDisplayDesktopColourDepth", graphAdapter, ['0'])[0])
        if colourDepth:
            graphAdapterOsh.setIntegerAttribute("current_display_mode_colour_depth", colourDepth)
        colours = long(getNodeValues("hwDisplayDesktopColours", graphAdapter, ['0'])[0])
        if colours:
            graphAdapterOsh.setLongAttribute("current_display_mode_colours", colours)
        refreshRate = int(getNodeValues("hwDisplayDesktopRefreshRate", graphAdapter, ['0'])[0])
        if refreshRate:
            graphAdapterOsh.setIntegerAttribute("current_display_mode_refresh_rate", refreshRate)
        graphAdapterOsh.setStringAttribute("current_display_mode_resolution",
                                           getNodeValues("hwDisplayDesktopResolution", graphAdapter)[0])
        graphAdapterOsh.setStringAttribute("board_index", str(idx))
        cardMemory = int(getNodeValues("hwDisplayGraphicsAdapterMemoryMB", graphAdapter, ['0'])[0])
        if cardMemory:
            graphAdapterOsh.setIntegerAttribute("graphics_card_memory", cardMemory)
        graphAdapterOsh.setContainer(hostOsh)
        oshvresults.add(graphAdapterOsh)
        mapScanFile(oshvresults, root, hostOsh, graphAdapterOsh, graphAdapter, idx)
        idx += 1
    return idx


def createNetworkAdapterOSH(oshvresults, root, hostOsh, idx, Framework=None):
    # create interface with mac
    macInterface = createInterfaceOSH(oshvresults, root, hostOsh, Framework)
    NICWithDeviceID = {}
    networkAdapters = root.getElementsByTagName("hwNetworkCards_value")
    networkAdaptersArray = nodeListToArray(networkAdapters)
    for networkAdapter in networkAdaptersArray:
        networkAdapterOsh = ObjectStateHolder("network_adapter")
        networkAdapterOsh.setStringAttribute("name", getNodeValues("hwNICDescription", networkAdapter)[0])
        NICType = getNodeValues("hwNICType", networkAdapter)[0]
        if NICType in ["Software Loopback", "Encapsulation Interface"]:
            continue
        networkAdapterOsh.setStringAttribute("network_adapter_type", getNodeValues("hwNICType", networkAdapter)[0])
        networkAdapterOsh.setStringAttribute("board_index", str(idx))
        deviceID = getNodeValues("hwNICDeviceID", networkAdapter)[0]
        if deviceID:
            NICWithDeviceID[deviceID] = networkAdapterOsh
        networkAdapterOsh.setContainer(hostOsh)
        oshvresults.add(networkAdapterOsh)
        # link interface and NIC
        macaddress = getNodeValues("hwNICPhysicalAddress", networkAdapter)[0]
        if macaddress and macInterface:
            for mac in macInterface:
                if mac == macaddress:
                    NicInterfaceLink = modeling.createLinkOSH("realization", networkAdapterOsh, macInterface[mac])
                    oshvresults.add(NicInterfaceLink)
        mapScanFile(oshvresults, root, hostOsh, networkAdapterOsh, networkAdapter, idx)
        idx += 1
    return idx, NICWithDeviceID


def createMotherBoardOSH(oshvresults, root, hostOsh, idx):
    motherBoards = root.getElementsByTagName("hwsmbiosBaseBoardInformation_value")
    motherBoardsArray = nodeListToArray(motherBoards)
    for motherBoard in motherBoardsArray:
        motherBoardOsh = ObjectStateHolder("hardware_board")
        motherBoardOsh.setStringAttribute("name",
                                          getNodeValues("hwsmbiosBaseBoardName", motherBoard)[0])  # key attribute
        motherBoardOsh.setStringAttribute("board_index", str(idx))
        mapStringAttribute(motherBoardOsh, "serial_number", "hwsmbiosBaseBoardSerialNumber", motherBoard)
        mapStringAttribute(motherBoardOsh, "hardware_version", "hwsmbiosBaseBoardVersion", motherBoard)
        mapStringAttribute(motherBoardOsh, "vendor", "hwsmbiosBaseBoardManufacturer", motherBoard)
        motherBoardOsh.setEnumAttribute("type", 25)  # set it to 'mother board'
        motherBoardOsh.setContainer(hostOsh)
        oshvresults.add(motherBoardOsh)
        mapScanFile(oshvresults, root, hostOsh, motherBoardOsh, motherBoard, idx)
        idx += 1


def createHardwareBoardOSH(oshvresults, root, hostOsh, Framework=None):
    hardwareBoards = root.getElementsByTagName("hwCards_value")
    hardwareBoardsArray = nodeListToArray(hardwareBoards)
    idx = 0
    idx = createGraphicsAdapterOSH(oshvresults, root, hostOsh, idx)
    # create network adapter
    idx, OSHWithDeviceID = createNetworkAdapterOSH(oshvresults, root, hostOsh, idx, Framework)
    logger.debug('index networkcard is :', idx)
    for hwBoard in hardwareBoardsArray:
        cardType = int(getNodeEnumAttribute(hwBoard, "hwCardClass", '5'))
        # ignore display card and network adapter,as it has been created
        if cardType == 1 or cardType == 0:
            continue
        hwBoardOsh = ObjectStateHolder("hardware_board")
        hwBoardOsh.setStringAttribute("name", getNodeValues("hwCardName", hwBoard)[0])  # key attribute
        hwBoardOsh.setStringAttribute("board_index", str(idx))
        hwBoardOsh.setEnumAttribute("type", cardType)
        hwBoardOsh.setEnumAttribute("bus", int(getNodeEnumAttribute(hwBoard, "hwCardBus", '8')))
        mapStringAttribute(hwBoardOsh, "vendor", "hwCardVendor", hwBoard)
        mapStringAttribute(hwBoardOsh, "vendor_card_id", "hwCardID", hwBoard)
        mapStringAttribute(hwBoardOsh, "hardware_version", "hwCardRevision", hwBoard)
        hwBoardOsh.setContainer(hostOsh)
        oshvresults.add(hwBoardOsh)
        mapScanFile(oshvresults, root, hostOsh, hwBoardOsh, hwBoard, idx)
        idx += 1
    createMotherBoardOSH(oshvresults, root, hostOsh, idx)
    return OSHWithDeviceID


def createDisplayMonitorOSH(oshvresults, root, hostOsh):
    monitors = root.getElementsByTagName("hwDisplayMonitors_value")
    monitorsArray = nodeListToArray(monitors)
    idx = 0
    for monitor in monitorsArray:
        monitorName = getNodeValues("hwMonitorName", monitor)[0]
        vendorCode = getNodeValues("hwMonitorVendorCode", monitor)[0]
        if len(monitorName) and len(vendorCode):
            monitorOsh = ObjectStateHolder("display_monitor")
            monitorOsh.setStringAttribute("name", monitorName)  # key attribute
            mapStringAttribute(monitorOsh, "serial_number", "hwMonitorSerialNumber", monitor)
            monitorX = int(getNodeValues('hwMonitorSizeCmX', monitor, ['0'])[0])
            if monitorX:
                monitorOsh.setIntegerAttribute("monitor_size_x", monitorX)
            monitorY = int(getNodeValues('hwMonitorSizeCmY', monitor, ['0'])[0])
            if monitorY:
                monitorOsh.setIntegerAttribute("monitor_size_y", monitorY)
            monitorManufactYear = int(getNodeValues('hwMonitorManufactureYear', monitor, ['0'])[0])
            if monitorManufactYear:
                monitorOsh.setIntegerAttribute("monitor_manufacture_year", monitorManufactYear)
            monitorOsh.setStringAttribute("vendor", vendorCode)  # normalize it
            monitorOsh.setContainer(hostOsh)
            oshvresults.add(monitorOsh)
            mapScanFile(oshvresults, root, hostOsh, monitorOsh, monitor, idx)
            idx += 1


def createPrinterDriverOSH(oshvresults, root, hostOsh):
    printers = root.getElementsByTagName("hwPrinters_value")
    printerArray = nodeListToArray(printers)
    idx = 0
    for printer in printerArray:
        printerName = getNodeValues("hwPrinterName", printer)[0]
        if len(printerName):
            printerDriverOsh = ObjectStateHolder("printer_driver")
            printerDriverOsh.setStringAttribute("name", printerName)  # key attribtue
            printerPort = getNodeValues("hwPrinterPort", printer)[0]
            if printerPort:
                printerDriverOsh.setStringAttribute("printer_port", printerPort)
            printerDriver = getNodeValues("hwPrinterDriver", printer)[0]
            if printerDriver:
                printerDriverOsh.setStringAttribute("printer_driver", printerDriver)
            printerDriverVersion = getNodeValues("hwPrinterDriverVersion", printer)[0]
            if printerDriverVersion:
                printerDriverOsh.setStringAttribute("printer_driver_version", printerDriverVersion)
            printerDriverOsh.setContainer(hostOsh)
            oshvresults.add(printerDriverOsh)
            mapScanFile(oshvresults, root, hostOsh, printerDriverOsh, printer, idx)
            idx += 1


def createUSBDeviceOSH(oshvresults, root, hostOsh):
    usb_devices = root.getElementsByTagName("hwUSBDevices_value")
    usb_devices_array = nodeListToArray(usb_devices)
    idx = 0
    for usb_device in usb_devices_array:
        # hwUSBDeviceName in scanfile indicates the unique name of the USB device that is assigned by operating system. hwUSBDeviceDescription is the name of USB device.
        usb_name = getNodeValues("hwUSBDeviceDescription", usb_device)[0]
        usb_index = getNodeValues("hwUSBDeviceIndex", usb_device)[0]
        if len(usb_name) and len(usb_index):
            usb_device_osh = ObjectStateHolder("usb_device")
            usb_device_osh.setStringAttribute("name", usb_name)  # key attribtue
            usb_device_osh.setStringAttribute("usb_device_index", usb_index)  # key attribtue

            usb_vendor = getNodeValues("hwUSBDeviceVendor", usb_device)[0]
            if usb_vendor:
                usb_device_osh.setStringAttribute("usb_device_vendor", usb_vendor)

            usb_os_name = getNodeValues("hwUSBDeviceName", usb_device)[0]
            if usb_os_name:
                usb_device_osh.setStringAttribute("os_usb_device_name", usb_os_name)
            else:
                continue

            usb_product_id = getNodeValues("hwUSBDeviceProductId", usb_device)[0]
            if usb_product_id:
                usb_device_osh.setStringAttribute("usb_device_product_id", usb_product_id)

            usb_product_id = getNodeValues("hwUSBDeviceVersion", usb_device)[0]
            if usb_product_id:
                usb_device_osh.setStringAttribute("usb_device_version", usb_product_id)

            usb_class = getNodeValues("hwUSBDeviceClass", usb_device)[0]
            if usb_class:
                usb_device_osh.setStringAttribute("usb_device_class", usb_class)

            usb_device_osh.setContainer(hostOsh)
            oshvresults.add(usb_device_osh)
            mapScanFile(oshvresults, root, hostOsh, usb_device_osh, usb_device, idx)
            idx += 1


def mapConfigurations(root):
    configStr = []
    # create EnvrionmentVariables section
    setConfigFileSectionName(configStr, 'EnvrionmentVariables')
    evsArray = nodeListToArray(root.getElementsByTagName("hwOSEnvironment_value"))
    for ev in evsArray:
        setConfigFileProperty(configStr, getNodeValues("hwOSEnvironmentName", ev)[0],
                              getNodeValues("hwOSEnvironmentValue", ev)[0])
        # map StandardWindowsDirectories
    if isWindows(root):
        setConfigFileSectionName(configStr, 'StandardWindowsDirectories')
        setConfigFileProperty(configStr, 'ProgramFiles', getNodeValues("hwOSProgramFilesDir", root)[0])
        setConfigFileProperty(configStr, 'CurrentUserDesktop', getNodeValues("hwOSCurrentUserDesktopDir", root)[0])
        setConfigFileProperty(configStr, 'AllUsersDesktop', getNodeValues("hwOSAllUsersDesktopDir", root)[0])
        setConfigFileProperty(configStr, 'CurrentUserStartMenu', getNodeValues("hwOSCurrentUserStartMenuDir", root)[0])
        setConfigFileProperty(configStr, 'AllUsersStartMenuDir', getNodeValues("hwOSAllUsersStartMenuDir", root)[0])
        setConfigFileProperty(configStr, 'RecycleBin', getNodeValues("hwOSRecycleBin", root)[0])
        setConfigFileProperty(configStr, 'CurrentUserAdminTool', getNodeValues("hwOSAdminTools", root)[0])
        setConfigFileProperty(configStr, 'AllUsersAdminTools', getNodeValues("hwOSAllUsersAdminTools", root)[0])
        setConfigFileProperty(configStr, 'CurrentUserAppData', getNodeValues("hwOSAppData", root)[0])
        setConfigFileProperty(configStr, 'AllUsersAppData', getNodeValues("hwOSAllUsersAppData", root)[0])
        setConfigFileProperty(configStr, 'CurrentUserDocuments', getNodeValues("hwOSDocuments", root)[0])
        setConfigFileProperty(configStr, 'AllUsersDocuments', getNodeValues("hwOSAllUsersDocuments", root)[0])
        setConfigFileProperty(configStr, 'ControlPanel', getNodeValues("hwOSControlPanel", root)[0])
        setConfigFileProperty(configStr, 'Cookies', getNodeValues("hwOSCookies", root)[0])
        setConfigFileProperty(configStr, 'Fonts', getNodeValues("hwOSFonts", root)[0])
        # map StartupApps
        setConfigFileSectionName(configStr, 'StartupApps')
        startupAppsArray = nodeListToArray(root.getElementsByTagName("hwOSStartupApps_value"))
        idx = 0
        for startupApp in startupAppsArray:
            setConfigFileProperty(configStr, str(idx),
                                  getNodeValues("hwStartupAppsName", startupApp)[0] +
                                  getNodeValues("hwStartupAppsParams", startupApp)[0])
            idx += 1
            # map ScreenSaver
        setConfigFileSectionName(configStr, 'ScreenSaver')
        setConfigFileProperty(configStr, 'ScreenSaverProgram', getNodeValues("hwScreenSaverProgram", root)[0])
        setConfigFileProperty(configStr, 'ScreenSaverName', getNodeValues("hwScreenSaverName", root)[0])
        # map wallpaper
        setConfigFileSectionName(configStr, 'Wallpaper')
        setConfigFileProperty(configStr, 'Wallpaper', getNodeValues("hwWallPaperName", root)[0])
        # map web browser
        setConfigFileSectionName(configStr, 'WebBrowser')
        setConfigFileProperty(configStr, 'WebBrowser', getNodeValues("hwWebBrowser", root)[0])
        setConfigFileProperty(configStr, 'Parameters', getNodeValues("hwWebBrowserParameters", root)[0])
        setConfigFileProperty(configStr, 'Description', getNodeValues("hwWebBrowserDescription", root)[0])
        setConfigFileProperty(configStr, 'Version', getNodeValues("hwWebBrowserVersion", root)[0])
        # map shell
        setConfigFileSectionName(configStr, 'Shell')
        setConfigFileProperty(configStr, 'Shell', getNodeValues("hwActiveShell", root)[0])
        setConfigFileProperty(configStr, 'Description', getNodeValues("hwActiveShellDescription", root)[0])
        setConfigFileProperty(configStr, 'Version', getNodeValues("hwActiveShellVersion", root)[0])
        # map mail client
        setConfigFileSectionName(configStr, 'MailClient')
        setConfigFileProperty(configStr, 'MailClient', getNodeValues("hwMailClient", root)[0])
        setConfigFileProperty(configStr, 'Parameters', getNodeValues("hwMailClientParameters", root)[0])
        setConfigFileProperty(configStr, 'Description', getNodeValues("hwMailClientDescription", root)[0])
        setConfigFileProperty(configStr, 'Version', getNodeValues("hwMailClientVersion", root)[0])
    if isUnix(root):
        # map UnixSystemConfig
        setConfigFileSectionName(configStr, 'UnixSystemConfig')
        setConfigFileProperty(configStr, 'SC2CBind', getNodeValues("hwSC2CBind", root)[0])
        setConfigFileProperty(configStr, 'SC2CDev', getNodeValues("hwSC2CDev", root)[0])
        setConfigFileProperty(configStr, 'SC2CharTerm', getNodeValues("hwSC2CharTerm", root)[0])
        setConfigFileProperty(configStr, 'SC2FortDev', getNodeValues("hwSC2FortDev", root)[0])
        setConfigFileProperty(configStr, 'SC2FortRun', getNodeValues("hwSC2FortRun", root)[0])
        setConfigFileProperty(configStr, 'SC2Localedef', getNodeValues("hwSC2Localedef", root)[0])
        setConfigFileProperty(configStr, 'SC2SwDev', getNodeValues("hwSC2SwDev", root)[0])
        setConfigFileProperty(configStr, 'SC2Upe', getNodeValues("hwSC2Upe", root)[0])
        setConfigFileProperty(configStr, 'SCAsynchronousIO', getNodeValues("hwSCAsynchronousIO", root)[0])
        setConfigFileProperty(configStr, 'SCFSync', getNodeValues("hwSCFSync", root)[0])
        setConfigFileProperty(configStr, 'SCJobControl', getNodeValues("hwSCJobControl", root)[0])
        setConfigFileProperty(configStr, 'SCMappedFiles', getNodeValues("hwSCMappedFiles", root)[0])
        setConfigFileProperty(configStr, 'SCMemLock', getNodeValues("hwSCMemLock", root)[0])
        setConfigFileProperty(configStr, 'SCMemLockRange', getNodeValues("hwSCMemLockRange", root)[0])
        setConfigFileProperty(configStr, 'SCMemProtection', getNodeValues("hwSCMemProtection", root)[0])
        setConfigFileProperty(configStr, 'SCMessagePassing', getNodeValues("hwSCMessagePassing", root)[0])
        setConfigFileProperty(configStr, 'SCPrioritizedIO', getNodeValues("hwSCPrioritizedIO", root)[0])
        setConfigFileProperty(configStr, 'SCPrioritySchedul', getNodeValues("hwSCPrioritySchedul", root)[0])
        setConfigFileProperty(configStr, 'SCRealtimeSignals', getNodeValues("hwSCRealtimeSignals", root)[0])
        setConfigFileProperty(configStr, 'SCSemaphores', getNodeValues("hwSCSemaphores", root)[0])
        setConfigFileProperty(configStr, 'SCSharedMemObj', getNodeValues("hwSCSharedMemObj", root)[0])
        setConfigFileProperty(configStr, 'SCSynchronizedIO', getNodeValues("hwSCSynchronizedIO", root)[0])
        setConfigFileProperty(configStr, 'SCThrAttrStackAddr', getNodeValues("hwSCThrAttrStackAddr", root)[0])
        setConfigFileProperty(configStr, 'SCThrAttrStackSize', getNodeValues("hwSCThrAttrStackSize", root)[0])
        setConfigFileProperty(configStr, 'SCThrPrioSchedul', getNodeValues("hwSCThrPrioSchedul", root)[0])
        setConfigFileProperty(configStr, 'SCThrProcShared', getNodeValues("hwSCThrProcShared", root)[0])
        setConfigFileProperty(configStr, 'SCThrSafeFunc', getNodeValues("hwSCThrSafeFunc", root)[0])
        setConfigFileProperty(configStr, 'SCThreads', getNodeValues("hwSCThreads", root)[0])
        setConfigFileProperty(configStr, 'SCXbs5Ilp32Off32', getNodeValues("hwSCXbs5Ilp32Off32", root)[0])
        setConfigFileProperty(configStr, 'SCXbs5Ilp32OffBig', getNodeValues("hwSCXbs5Ilp32OffBig", root)[0])
        setConfigFileProperty(configStr, 'SCXbs5Ilp32Off64', getNodeValues("hwSCXbs5Ilp32Off64", root)[0])
        setConfigFileProperty(configStr, 'SCXOpenCrypt', getNodeValues("hwSCXOpenCrypt", root)[0])
        setConfigFileProperty(configStr, 'SCXOpenEnhI18n', getNodeValues("hwSCXOpenEnhI18n", root)[0])
        setConfigFileProperty(configStr, 'SCXOpenLegacy', getNodeValues("hwSCXOpenLegacy", root)[0])
        setConfigFileProperty(configStr, 'SCXOpenRealtime', getNodeValues("hwSCXOpenRealtime", root)[0])
        setConfigFileProperty(configStr, 'SCXOpenRtThreads', getNodeValues("hwSCXOpenRtThreads", root)[0])
        setConfigFileProperty(configStr, 'SCXOpenShm', getNodeValues("hwSCXOpenShm", root)[0])
        setConfigFileProperty(configStr, 'SCSysAcct', getNodeValues("hwSCSysAcct", root)[0])
        setConfigFileProperty(configStr, 'SCFileSystemDrivers', getNodeValues("hwSCFileSystemDrivers", root)[0])
        setConfigFileProperty(configStr, 'SCDrivers', getNodeValues("hwSCDrivers", root)[0])
        setConfigFileProperty(configStr, 'SCLocaleIPCFeatures', getNodeValues("hwSCLocaleIPCFeatures", root)[0])
        setConfigFileProperty(configStr, 'SCNFSFeatures', getNodeValues("hwSCNFSFeatures", root)[0])
        setConfigFileProperty(configStr, 'SCAIOLisIOMax', getNodeValues("hwSCAIOLisIOMax", root)[0])
        setConfigFileProperty(configStr, 'SCAIOMax', getNodeValues("hwSCAIOMax", root)[0])
        setConfigFileProperty(configStr, 'SCAIOPrioDelta', getNodeValues("hwSCAIOPrioDelta", root)[0])
        setConfigFileProperty(configStr, 'SCArgMax', getNodeValues("hwSCArgMax", root)[0])
        setConfigFileProperty(configStr, 'SCAtExitMax', getNodeValues("hwSCAtExitMax", root)[0])
        setConfigFileProperty(configStr, 'SCAvphysPages', getNodeValues("hwSCAvphysPages", root)[0])
        setConfigFileProperty(configStr, 'SCBcBaseMax', getNodeValues("hwSCBcBaseMax", root)[0])
        setConfigFileProperty(configStr, 'SCBcDimMAx', getNodeValues("hwSCBcDimMAx", root)[0])
        setConfigFileProperty(configStr, 'SCBcScaleMax', getNodeValues("hwSCBcScaleMax", root)[0])
        setConfigFileProperty(configStr, 'SCBcStringMax', getNodeValues("hwSCBcStringMax", root)[0])
        setConfigFileProperty(configStr, 'SCChildMax', getNodeValues("hwSCChildMax", root)[0])
        setConfigFileProperty(configStr, 'SCCollWeightsMax', getNodeValues("hwSCCollWeightsMax", root)[0])
        setConfigFileProperty(configStr, 'SCDelayTimerMax', getNodeValues("hwSCDelayTimerMax", root)[0])
        setConfigFileProperty(configStr, 'SCExprNestMax', getNodeValues("hwSCExprNestMax", root)[0])
        setConfigFileProperty(configStr, 'SCGetGrRSizeMax', getNodeValues("hwSCGetGrRSizeMax", root)[0])
        setConfigFileProperty(configStr, 'SCGetPwRSizeMax', getNodeValues("hwSCGetPwRSizeMax", root)[0])
        setConfigFileProperty(configStr, 'SCLineMax', getNodeValues("hwSCLineMax", root)[0])
        setConfigFileProperty(configStr, 'LoginNameMax', getNodeValues("hwLoginNameMax", root)[0])
        setConfigFileProperty(configStr, 'SCMqOpenMax', getNodeValues("hwSCMqOpenMax", root)[0])
        setConfigFileProperty(configStr, 'SCMqPrioMax', getNodeValues("hwSCMqPrioMax", root)[0])
        setConfigFileProperty(configStr, 'SCNGroupsMax', getNodeValues("hwSCNGroupsMax", root)[0])
        setConfigFileProperty(configStr, 'SCNProcessesConf', getNodeValues("hwSCNProcessesConf", root)[0])
        setConfigFileProperty(configStr, 'SCNProcessorsOnln', getNodeValues("hwSCNProcessorsOnln", root)[0])
        setConfigFileProperty(configStr, 'SCOpenMax', getNodeValues("hwSCOpenMax", root)[0])
        setConfigFileProperty(configStr, 'SCPageSize', getNodeValues("hwSCPageSize", root)[0])
        setConfigFileProperty(configStr, 'SCPassMax', getNodeValues("hwSCPassMax", root)[0])
        setConfigFileProperty(configStr, 'SCPhysPages', getNodeValues("hwSCPhysPages", root)[0])
        setConfigFileProperty(configStr, 'SCReDupMax', getNodeValues("hwSCReDupMax", root)[0])
        setConfigFileProperty(configStr, 'SCRTSigMax', getNodeValues("hwSCRTSigMax", root)[0])
        setConfigFileProperty(configStr, 'SCSemNSemsMax', getNodeValues("hwSCSemNSemsMax", root)[0])
        setConfigFileProperty(configStr, 'SCSemValueMax', getNodeValues("hwSCSemValueMax", root)[0])
        setConfigFileProperty(configStr, 'SCSigQueueMax', getNodeValues("hwSCSigQueueMax", root)[0])
        setConfigFileProperty(configStr, 'SCStreamMAx', getNodeValues("hwSCStreamMAx", root)[0])
        setConfigFileProperty(configStr, 'SCThreadDestruct', getNodeValues("hwSCThreadDestruct", root)[0])
        setConfigFileProperty(configStr, 'SCThreadKeysMax', getNodeValues("hwSCThreadKeysMax", root)[0])
        setConfigFileProperty(configStr, 'SCThreadStackMin', getNodeValues("hwSCThreadStackMin", root)[0])
        setConfigFileProperty(configStr, 'SCThreadStackMax', getNodeValues("hwSCThreadStackMax", root)[0])
        setConfigFileProperty(configStr, 'SCPThreadMax', getNodeValues("hwSCPThreadMax", root)[0])
        setConfigFileProperty(configStr, 'SCTimerMax', getNodeValues("hwSCTimerMax", root)[0])
        setConfigFileProperty(configStr, 'SCTtyNameMax', getNodeValues("hwSCTtyNameMax", root)[0])
        setConfigFileProperty(configStr, 'SCTZNameMax', getNodeValues("hwSCTZNameMax", root)[0])
        setConfigFileProperty(configStr, 'SCXopenVersion', getNodeValues("hwSCXopenVersion", root)[0])
        setConfigFileProperty(configStr, 'SCNBSDMax', getNodeValues("hwSCNBSDMax", root)[0])
        setConfigFileProperty(configStr, 'SCNProcessesMax', getNodeValues("hwSCNProcessesMax", root)[0])
        setConfigFileProperty(configStr, 'SCNUsersMax', getNodeValues("hwSCNUsersMax", root)[0])
        setConfigFileProperty(configStr, 'SCQuotasTableSize', getNodeValues("hwSCQuotasTableSize", root)[0])
        setConfigFileProperty(configStr, 'SCInodeTableSize', getNodeValues("hwSCInodeTableSize", root)[0])
        setConfigFileProperty(configStr, 'SCDNLookupCacheSize', getNodeValues("hwSCDNLookupCacheSize", root)[0])
        setConfigFileProperty(configStr, 'SCCalloutTableSize', getNodeValues("hwSCCalloutTableSize", root)[0])
        setConfigFileProperty(configStr, 'SCGPrioMax', getNodeValues("hwSCGPrioMax", root)[0])
        setConfigFileProperty(configStr, 'SCNSPushesMax', getNodeValues("hwSCNSPushesMax", root)[0])
        setConfigFileProperty(configStr, 'SCXOpenXcuVer', getNodeValues("hwSCXOpenXcuVer", root)[0])
        # map Display Desktop
    setConfigFileSectionName(configStr, 'DisplaySettings')
    setConfigFileProperty(configStr, 'DisplayDesktopRefreshRate', getNodeValues("hwDisplayDesktopRefreshRate", root)[0])
    setConfigFileProperty(configStr, 'DisplayDesktopColourDepth', getNodeValues("hwDisplayDesktopColourDepth", root)[0])
    setConfigFileProperty(configStr, 'DisplayDesktopResolution ', getNodeValues("hwDisplayDesktopResolution", root)[0])
    setConfigFileProperty(configStr, 'DisplayDesktopColours', getNodeValues("hwDisplayDesktopColours", root)[0])
    return ''.join(configStr)


def setConfigFileSectionName(strBuilder, sectionName):
    strBuilder.append('[' + sectionName + ']')
    strBuilder.append('\r\n')


def setConfigFileProperty(strBuilder, propertyName, propertyValue):
    if len(propertyValue):
        strBuilder.append(propertyName + '=' + propertyValue)
        strBuilder.append('\r\n')


def createMSCluster(oshvresults, root):
    clusterName = getNodeValues("hwOSClusterName", root)[0]
    if len(clusterName):
        msclusterOSH = ObjectStateHolder("mscluster")
        msclusterOSH.setStringAttribute("name", clusterName)
        modeling.setAppSystemVendor(msclusterOSH, getNodeValues("hwOSClusterVendor", root)[0])
        mapStringAttribute(msclusterOSH, "description", "hwOSClusterDescription", root)
        oshvresults.add(msclusterOSH)
        nodeNames = getNodeValues("hwOSClusterNodeName", root)
        for nodeName in nodeNames:
            nodeHostOSH = None
            try:
                nodeIps = InetAddress.getAllByName(nodeName)
                if len(nodeIps) > 0:
                    nodeHostOSH = modeling.createHostOSH(nodeIps[0].getHostAddress(), 'nt')
                    for ip in nodeIps:
                        normalized_ip = getValidIP(ip.getHostAddress())
                        if normalized_ip:
                            ipOSH = modeling.createIpOSH(normalized_ip)
                            containmentLinkOSH = modeling.createLinkOSH('containment', nodeHostOSH, ipOSH)
                            oshvresults.add(ipOSH)
                            oshvresults.add(containmentLinkOSH)
            except:
                errorMessage = str(sys.exc_info()[1])
                logger.debugException(errorMessage)

            if not nodeHostOSH:
                continue

            nodeHostOSH.setStringAttribute('os_family', 'windows')
            oshvresults.add(nodeHostOSH)
            clusterSoftware = modeling.createClusterSoftwareOSH(nodeHostOSH, 'Microsoft Cluster SW')
            oshvresults.add(clusterSoftware)
            oshvresults.add(modeling.createLinkOSH('membership', msclusterOSH, clusterSoftware))
        mapScanFile(oshvresults, root, None, msclusterOSH)


def getValidIP(ip):
    try:
        ipAddr = ip_addr.IPAddress(ip)
        if not (ipAddr.is_loopback or ipAddr.is_multicast or
                ipAddr.is_link_local or ipAddr.is_unspecified):
            return ipAddr
    except:
        pass
    return None


def getValidIPInString(rawIP):
    ipObject = getValidIP(rawIP)
    if ipObject:
        return str(ipObject)
    return None


def isVirtualMachine(root):
    if getNodeValues("hwVirtualMachineType", root)[0] == '':
        return 0
    return 1


def isWindows(root):
    os = getNodeValues("hwOSHostOsCategory", root)[0]
    if re.search('Windows', os):
        return 1
    return 0


def isUnix(root):
    os = getNodeValues("hwOSHostOsCategory", root)[0]
    if re.search('Unix', os):
        return 1
    return 0


def isLinux(root):
    os = getNodeValues("hwOSHostUnixType", root)[0]
    if re.search('Linux', os):
        return 1
    return 0


def isXenServer(root):
    os = getNodeValues("hwOSHostUnixType", root)[0]
    if re.search('XenServer', os):
        return 1
    return 0


def isMac(root):
    os = getNodeValues("hwOSHostOsCategory", root)[0]
    if re.search('Mac', os):
        return 1
    return 0


def isHPUX(root):
    os = getNodeValues("hwOSHostUnixType", root)[0]
    if re.search('HP-UX', os):
        return 1
    return 0


def isAIX(root):
    os = getNodeValues("hwOSHostUnixType", root)[0]
    if re.search('AIX', os):
        return 1
    return 0


def mapNodeRole(root):
    roles = []
    networkTcpip = root.getElementsByTagName("hwNetworkTcpip")
    routingEnabled = getNodeValues("hwIPRoutingEnabled", root)[0]
    if re.search('Yes', routingEnabled, re.IGNORECASE):
        roles.append("router")
    if isVirtualMachine(root):
        roles.append("virtualized_system")
    return roles


def mapOsName(root):
    if isWindows(root):
        return getNodeValues("hwOSHostWindowsName", root)[0]
    osName = getNodeValues("hwOSHostUnixType", root)[0]
    if (re.search('Solaris', osName)):
        return re.sub('Solaris', 'SunOS', osName)
    if (re.search('Linux', osName)):
        osDistribution = getNodeValues("hwHostOS", root)[0]
        osName = TTY_Connection_Utils.LinuxDiscoverer.mappingLinuxDistribution(osDistribution)
    return osName


def mapIpAddressType(ipAddressVersion):
    if ipAddressVersion == 4:
        return "IPv4"
    if ipAddressVersion == 6:
        return "IPv6"
    return ''


def isDhcpEnabled(networkCard):
    useDHCP = getNodeValues("hwNICUsesDHCP", networkCard)[0]
    return re.search('Yes', useDHCP, re.IGNORECASE)


def nodeListToArray(nodeList):
    nodeArray = []
    l = nodeList.getLength()
    while l > 0:
        node = nodeList.item(nodeList.getLength() - l)
        nodeArray.append(node)
        l -= 1
    return nodeArray


# format subnet mask (i.e. 8 = 1111 1111.0.0.0(255.0.0.0)
def formatNetmask(netmask):
    maskAsNumber = int(netmask)
    formattedNetmask = [0, 0, 0, 0]
    if maskAsNumber <= 32:
        masksecs = maskAsNumber / 8
        idx = 0
        while masksecs > 0:
            formattedNetmask[idx] = pow(2, 8) - 1
            idx += 1
            masksecs -= 1
        highBits = maskAsNumber % 8
        if highBits:
            lowBits = 8 - maskAsNumber % 8
            formattedNetmask[idx] = pow(2, 8) - pow(2, lowBits)
    return (str(formattedNetmask[0]) + '.' + str(formattedNetmask[1]) + '.' +
            str(formattedNetmask[2]) + '.' + str(formattedNetmask[3]))


# ===============For SCAN FILE MAPPING CONFIG================
_mappingConfig = None
_xPath = XPathFactory.newInstance().newXPath()


def __getXPathValue__(path, node):
    return _xPath.evaluate(path, node)


def getScanFileMappingConfig():
    global _mappingConfig
    mappingConfig = _mappingConfig
    if not mappingConfig:
        logger.debug("Load mapping config")
        mappingConfigFile = CollectorsParameters.PROBE_MGR_CONFIGFILES_DIR + MappingConfig.CONFIG_FILE_NAME
        logger.debug("Hardware Mapping config file:", mappingConfigFile)
        mappingConfig = MappingConfig.loadMappingConfigFromFile(mappingConfigFile)
        logger.debug("Mapping config:", mappingConfig)
        if mappingConfig:
            logger.debug("Set global mapping config")
            _mappingConfig = mappingConfig
    return mappingConfig


def initScanFileMappingConfig():
    global _mappingConfig
    _mappingConfig = None
    getScanFileMappingConfig()


def getXPath(valueContent, needIndex=1):
    parts = valueContent.split('/')
    newParts = []
    for part in parts:
        if part.endswith('[]'):
            position = part.rindex('[]')
            part = part[:position]
            newParts.append(part)
            if needIndex:
                newParts.append(part + '_value[__INDEX__]')  # xxx_value is an array
            else:
                newParts.append(part + '_value')  # xxx_value is an array
        else:
            newParts.append(part)
    scalarArray = '/'.join(newParts)
    return scalarArray


def isSuper(superClass, subClass):
    cmdbModel = modeling.CmdbClassModel().getConfigFileManager().getCmdbClassModel()
    return cmdbModel.isTypeOf(superClass, subClass)


def superCmp(superCI, subCI):
    if isSuper(superCI.name, subCI.name):
        return 1
    else:
        return -1


def getOrderedCIList(ciList, currentOsh):
    relatedCIList = []
    for ci in ciList:
        if isSuper(ci.name, currentOsh.getObjectClass()):
            relatedCIList.append(ci)
    return sorted(relatedCIList, cmp=superCmp)


def getOrderedAttributeList(ciList, currentOsh):
    orderedCIList = getOrderedCIList(ciList, currentOsh)
    orderedAttributeList = []
    for ci in orderedCIList:
        ciAttributes = ci.attributes
        for attr in ciAttributes:
            if not attributeNotExist(orderedAttributeList, attr) and canOverwrite(attr, currentOsh):
                orderedAttributeList.append(attr)
    return orderedAttributeList


def attributeNotExist(attributeList, targetAttribute):
    for attr in attributeList:
        if attr.name == targetAttribute.name:
            return 1
    return 0


def canOverwrite(attr, currentOsh):
    return attr.overwrite or not currentOsh.getAttribute(attr.name)


def evaluateXPath(exp, targetNode):
    try:
        return _xPath.evaluate(exp, targetNode)
    except:
        logger.warn('Failed to evaluate xpath for:[%s]' % exp)

    return None


def mapScanFile(OSHVResult, rootNode, nodeOSH, currentOsh, currentNode=None, index=0):
    logger.debug("Begin map data for:", currentOsh.getObjectClass())
    hardwareNode = getHardwareNode(rootNode)

    mc = getScanFileMappingConfig()
    if not mc:
        logger.warn("Mapping config is invalid.")
        return
    orderedAttributeList = getOrderedAttributeList(mc.ciList, currentOsh)

    for attr in orderedAttributeList:
        ci = attr.CI
        srcValue = None
        valueType = attr.value.type.getTypeValue()
        valueContent = attr.value.content
        if not valueContent or not valueContent.strip():
            continue
        valueContent = valueContent.strip()
        __DATA__ = {'currentOsh': currentOsh, 'hardwareNode': hardwareNode, 'currentNode': currentNode,
                    'attrName': attr.name, 'attrType': attr.type, 'index': index, 'nodeOsh': nodeOSH}
        logger.debug("__DATA__:", __DATA__)
        if valueType == 'constant':  # constant value
            srcValue = valueContent
        elif valueType == 'scalar':  # scalar value from scan file
            scalar = getXPath(valueContent, 0)
            srcValue = evaluateXPath(scalar, hardwareNode)
        elif valueType == 'pre/post':
            # pre/post value from scan file:hwAssetData/hwAssetCustomData/hwAssetCustomData_value/hwAssetCustomDataName/hwAssetCustomDataValue
            customDataName = valueContent
            customDataName = customDataName.replace('__INDEX__',
                                                    str(index + 1))  # if index=3, xx__INDEX__ will be replace to xx3
            xpath = "hwAssetData/hwAssetCustomData/hwAssetCustomData_value[hwAssetCustomDataName='%s']/hwAssetCustomDataValue" % customDataName
            srcValue = evaluateXPath(xpath, hardwareNode)
        elif valueType == 'array':  # array field from scan file
            scalarArray = getXPath(valueContent)
            if ci.kind == CI.CI_MAPPING_KIND_SINGLE:  # if signle, only get the first one
                scalarArray = scalarArray.replace('__INDEX__', str(1))
            elif ci.kind == CI.CI_MAPPING_KIND_MULTIPLE:
                scalarArray = scalarArray.replace('__INDEX__', str(index + 1))  # get the value for each
            srcValue = evaluateXPath(scalarArray, hardwareNode)
        elif valueType == 'expression':  # an expression which can be evaluated to a value
            exp = valueContent
            srcValue = str(eval(exp))
        elif valueType == 'script':  # A jython script which can have complex logic and finally return a value
            code = valueContent
            exec('def __dummy__(__DATA__):' + code)  # A dummy method which will wrap the script content
            srcValue = str(eval('__dummy__(__DATA__)'))
        currentOsh.setAttribute(AttributeStateHolder(attr.name, srcValue, attr.type))
        logger.debug("The value of %s is%s" % (attr.name, currentOsh.getAttribute(attr.name)))


def isNewCI(ci, OSHVResult):
    iterator = OSHVResult.iterator()
    while iterator.hasNext():
        osh = iterator.next()
        if ci.name == osh.getObjectClass():
            return 0
    return 1


def getHardwareNode(rootNode):
    hardwareNode = rootNode.getElementsByTagName("hardwaredata").item(0)
    return hardwareNode


def mapNewCI(OSHVResult, rootNode, nodeOSH):
    logger.debug('Begin mapping new CI...')
    hardwareNode = getHardwareNode(rootNode)
    mc = getScanFileMappingConfig()
    if not mc:
        logger.warn("Mapping config is invalid.")
        return
    for ci in mc.ciList:
        if isNewCI(ci, OSHVResult) and ci.createNewCI:
            if ci.kind == CI.CI_MAPPING_KIND_SINGLE:
                logger.debug('Create new CI:', ci.name)
                osh = ObjectStateHolder(ci.name)
                OSHVResult.add(osh)
                if ci.relationshipWithNode:
                    link = modeling.createLinkOSH(ci.relationshipWithNode, nodeOSH, osh)
                    OSHVResult.add(link)
                    if not osh.getAttribute(
                            CITRoot.ATTR_ROOT_CONTAINER) and ci.relationshipWithNode == CITComposition.CLASS_NAME:
                        osh.setContainer(nodeOSH)
                mapScanFile(OSHVResult, rootNode, nodeOSH, osh)
            elif ci.kind == CI.CI_MAPPING_KIND_MULTIPLE:
                if ci.source:
                    scalarArray = getXPath(ci.source, 0)
                    nodeList = _xPath.evaluate(scalarArray, hardwareNode, XPathConstants.NODESET)
                    _len = nodeList.getLength()
                    idx = 0
                    while idx < _len:
                        currentNode = nodeList.item(idx)
                        osh = ObjectStateHolder(ci.name)
                        OSHVResult.add(osh)
                        if ci.relationshipWithNode:
                            link = modeling.createLinkOSH(ci.relationshipWithNode, nodeOSH, osh)
                            OSHVResult.add(link)
                            if not osh.getAttribute(
                                    CITRoot.ATTR_ROOT_CONTAINER) and ci.relationshipWithNode == CITComposition.CLASS_NAME:
                                osh.setContainer(nodeOSH)
                        mapScanFile(OSHVResult, rootNode, nodeOSH, osh, currentNode, idx)
                        idx += 1


# get the license type of the software from the scan file
def getSoftwareLicenseType(commercial):
    return SOFTWARE_LICENSE_TYPES.get(commercial, 0)


# creates connection links between software and which licenced by
def createSoftwareLink(oshvresults, softwares):
    for software in softwares.itervalues():
        if len(software) == 2:
            softwareOsh = software[0]
            licencedBy = software[1]
            parentSoftwareEntry = softwares.get(licencedBy)
            if parentSoftwareEntry:
                parentSoftwareOsh = parentSoftwareEntry[0]
                link = modeling.createLinkOSH("membership", parentSoftwareOsh, softwareOsh)
                oshvresults.add(link)
