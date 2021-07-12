'''
Created on Sep 18, 2010

@author: kchhina

Changed TCPIP NETSTAT Command to use the FORMAT=Long, this is to support Mainframes that have the IPV6 Stack enabled P.Odom
Added host_iscomplete = true to the zos host. This was causing reconcilation errors in UCMDB 8.x P. Odom
'''
import re, logger, modeling, shellutils, errormessages
import eview_lib
import file_mon_utils

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector
from java.util import Properties
from com.hp.ucmdb.discovery.library.clients.agents import BaseAgent
from com.hp.ucmdb.discovery.library.clients import ClientsConsts
import errorobject
import errorcodes
from eview_lib import isNotNull, isNull
from modeling import _CMDB_CLASS_MODEL
import eview_netutils
import os
import string

''' Global Variables '''
_STR_EMPTY = ''


_CMD_EVIEW_VERSION = 'F %s,SHOW VERSION'
_CMD_D_IPLINFO = 'D IPLINFO'
_CMD_D_TCPIP = 'D TCPIP'
_CMD_D_NETSTAT_HOME = 'D TCPIP,%s,NETSTAT,HOME,FORMAT=LONG'
_CMD_D_SYMBOLS = 'D SYMBOLS'
_CMD_D_MSTOR = 'D M=STOR'
_CMD_D_M_CPU = 'D M=CPU'


''' Classes '''
class EViewConfFileNameFilter:
    def accept(self, fileName):
        return re.search('^ev390_config_.*$', fileName.lower()) is not None

''' Methods '''

def ev_getDefaultIpOfLpar(ls):
    defaultIp = None
    # Get TCP stacks -----------------------------------------------
    tcpStackList = ev3_getTcpStackNameOutput(ls)
    try:
        if len(tcpStackList) > 0:
            for tcpStack in tcpStackList:
                # Get LPAR default IP address from the first TCP stack -
                homeLists = ev4_getHomelistOutput(ls, tcpStack)

        # 07 Oct 2016 - Added by EView to set defaultIp to PRIMARY interface.
        #               If no PRIMARY found then first IP address will be set to defaultIp
                if len(homeLists) >= 1 and eview_lib.isNotNull(homeLists[0][1]):
                    for ListEntry in homeLists:
                        if ListEntry[2] == 'PRIMARY':
                           defaultIp = ListEntry[1]
                    if defaultIp is None:
        # 07 Oct 2016 - End of EView change
                       defaultIp = homeLists[0][1]
    except:
        errMsg = 'Failure in method ev_getDefaultIpOfLpar()'
        logger.error(errMsg)
        logger.reportError(errMsg)
    return defaultIp

def ev1_getEviewVersion(ls, task):
    # process EView agent information ------------------------------------------
    eviewVersion = None
    output = ls.evMvsCmd(_CMD_EVIEW_VERSION % task)
    try:
        if output.isSuccess() and len(output.cmdResponseList) > 0:
            logger.debug('Successfully connected to EView Agent')
            eviewVersionLine = output.cmdResponseList[1]
            eviewVersionList = output.getValuesFromLine("s", eviewVersionLine, "EVO607", "Copyright")
            if len(eviewVersionList) == 3:
                eviewVersion = eviewVersionList[1]
                logger.debug('Found EView Agent Version = ', eviewVersion)
        else:
            logger.reportError('Unable to get output for command - %s\nError: %s' % (_CMD_EVIEW_VERSION, output.errorDump))
    except:
        errMsg = 'Failure in method ev1_getEviewVersion()'
        logger.errorException(errMsg)
        logger.reportError("Failed to get version.")

    return eviewVersion

def ev2_getIplInfoOutput(ls):
    # process IPL Info ---------------------------------------------------------
    zOsRelease = ''
    ieasymList = ''
    ieasysList = ''
    machineBootDate = ''
    try:
        output = ls.evMvsCmd(_CMD_D_IPLINFO)
        if isNotNull(output) and output.isSuccess() and len(output.cmdResponseList) > 0:
            releaseList = output.getValuesFromLineList('s', output.cmdResponseList, 'RELEASE z/OS', 'LICENSE')
            if len(releaseList) > 0 and len(releaseList[0]) == 3:
                zOsRelease = releaseList[0][1] or ''
            ieasymList = output.getValuesFromLineList('s', output.cmdResponseList, 'IEASYM LIST =')
            if len(ieasymList) > 0 and len(ieasymList[0]) == 2:
                ieasymList = ieasymList[0][1] or ''
            ieasysList = output.getValuesFromLineList('s', output.cmdResponseList, 'IEASYS LIST =')
            if len(ieasymList) > 0 and len(ieasymList[0]) == 2:
                ieasysList = ieasysList[0][1] or ''
            bootList = output.getValuesFromLineList('s', output.cmdResponseList, 'SYSTEM IPLED AT', 'ON')
            if len(bootList) > 0 and len(bootList[0]) == 2:
                bootTime = bootList[0][1] or ''
                bootDate = bootList[0][2] or ''
                if eview_lib.isNotNull(bootDate) and eview_lib.isNotNull(bootTime):
                    machineBootDate = modeling.getDateFromString('%s %s' % (bootDate, bootTime), 'MM/dd/yyyy kk.mm.ss')
        else:
            logger.reportWarning('Unable to get output for command - %s' % _CMD_D_IPLINFO)
    except:
        errMsg = 'Failure in method ev2_getIplInfoOutput()'
        logger.error(errMsg)
        logger.reportError(errMsg)

    return (zOsRelease, ieasymList, ieasysList, machineBootDate)

def ev3_getTcpStackNameOutput(ls):
    # get the running TCP stacks -----------------------------------------
    tcpStackList = []
    output = ls.evMvsCmd(_CMD_D_TCPIP)
    try:
        if isNotNull(output) and output.isSuccess() and len(output.cmdResponseList) > 0:
            headerColumns = ['COUNT', 'TCPIP NAME', 'VERSION', 'STATUS']
            tableBeginPattern = 'EZAOP50I TCPIP STATUS REPORT'
            tableEndPattern = 'END TCPIP STATUS REPORT'
            firstColumnPaddingChar = ' '
            includeRegexPattern = ''
            ignorePatterns = ['------']
            stacks = output.getTableValues(output.cmdResponseList, headerColumns, tableBeginPattern, tableEndPattern, firstColumnPaddingChar, includeRegexPattern, ignorePatterns)
            for i in range(1, len(stacks)):
                if len(stacks[i]) == 4 and eview_lib.isNotNull(stacks[i][1]):
                    tcpStackList.append(stacks[i][1])
        else:
            logger.reportWarning('Unable to get output for command - %s' % _CMD_D_TCPIP)
    except:
        errMsg = 'Failure in method ev3_getTcpStackNameOutput()'
        logger.error(errMsg)
        logger.reportError(errMsg)

    return tcpStackList

def ev4_getHomelistOutput(ls, tcpStack):
    # process HOMELIST ---------------------------------------------------------
    homeLists = [] # [ADDRESS, LINK, FLG]
    linkList = []
    addrList = []
    flgList = []
    try:
        output = ls.evMvsCmd(_CMD_D_NETSTAT_HOME % tcpStack)
        if isNotNull(output) and output.isSuccess() and len(output.cmdResponseList) > 0:
            linkList = output.getValuesFromLineList('s', output.cmdResponseList, '(LINKNAME|INTFNAME):')
            addrList = output.getValuesFromLineList('s', output.cmdResponseList, 'ADDRESS:')
            flgList = output.getValuesFromLineList('s', output.cmdResponseList, 'FLAGS:')
            if len(linkList) > 0:
                for i in range(0, len(linkList)):
                    linkname = linkList[i][1]
                    address = addrList[i][1]
                    if address == '127.0.0.1':
                        continue
                    flag =  flgList[i][1]
                    tempList = [linkname,address,flag]
                    homeLists.append(tempList)

    except:
        errMsg = 'Failure in method ev4_getHomelistOutput()'
        logger.error(errMsg)
        logger.reportError(errMsg)

    return homeLists

def ev5_getSymlistOutput(ls):
    # process SYMLIST ----------------------------------------------------------
    symbolsMap = {} # {name:value}
    try:
        output = ls.evMvsCmd(_CMD_D_SYMBOLS)
        if output.isSuccess() and len(output.cmdResponseList) > 0:
            symbolsList = output.getValuesFromLineList('s', output.cmdResponseList, '&', '\.\s+=\s+"', '"')
            for symbols in symbolsList:
                if len(symbols) == 4:
                    symbolName = symbols[1]
                    symbolValue = symbols[2]
                    if isNotNull(symbolName) and isNotNull(symbolValue):
                        symbolsMap[symbolName] = symbolValue
    except:
        errMsg = 'Failure in method ev5_getSymlistOutput()'
        logger.error(errMsg)
        logger.reportError(errMsg)
    return symbolsMap

def ev6_getMemoryOutput(ls):
    # Spencer: Get the total memory of the lpar -------------------------------
    memory = 0
    try:
        output = ls.evMvsCmd(_CMD_D_MSTOR)
        if output.isSuccess() and len(output.cmdResponseList) > 0:
            for line in output.cmdResponseList:
                matches = re.match('^\s*(\d+)M-(\d+)M\s*$', line)
                if (matches != None):
                    memory += int(matches.group(2)) - int(matches.group(1))
    except:
        errMsg = 'Failure in method ev6_getMemoryOutput()'
        logger.error(errMsg)
        logger.reportError(errMsg)
    return memory

def ev7_getCpulistOutput(ls):
    cpuLists = [] # [CPU ID, CPU STATUS, CPU SERIAL, CPU RAW STATUS]
    cpcSi = ''
    cpcName = ''
    cpcId = ''
    lpName = ''
    lpId = ''
    output = ls.evMvsCmd(_CMD_D_M_CPU)
    if output.isSuccess() and len(output.cmdResponseList) > 0:
        # first search for CPUs ------------------------------------------------
        headerColumns = ['ID', 'CPU', 'SERIAL']
        tableBeginPattern = 'PROCESSOR STATUS'
        tableEndPattern = 'CPC ND ='
        firstColumnPaddingChar = ''
        includeRegexPattern = ''
        ignorePatterns = []
        cpuTable = output.getTableValues(output.cmdResponseList, headerColumns, tableBeginPattern, tableEndPattern, firstColumnPaddingChar, includeRegexPattern, ignorePatterns)
        
        # then search for CPC SI -----------------------------------------------
        cpcSiList = output.getValuesFromLineList('s', output.cmdResponseList, 'CPC SI =')
        if isNotNull(cpcSiList) and len(cpcSiList) > 0 and isNotNull(cpcSiList[0][1]):
            cpcSi = cpcSiList[0][1]
        
        # then search for CPC ID -----------------------------------------------
        cpcIdList = output.getValuesFromLineList('s', output.cmdResponseList, 'CPC ID =')
        if isNotNull(cpcIdList) and len(cpcIdList) > 0 and isNotNull(cpcIdList[0][1]):
            cpcId = cpcIdList[0][1]
        
        # then search for CPC Name ---------------------------------------------
        cpcNameList = output.getValuesFromLineList('s', output.cmdResponseList, 'CPC NAME =')
        if isNotNull(cpcNameList) and len(cpcNameList) > 0 and isNotNull(cpcNameList[0][1]):
            cpcName = cpcNameList[0][1]
        
    return (cpuLists, cpcSi, cpcId, cpcName)

def osh_createZOsOsh(defaultIp, zOsRelease, ieasymList, ieasysList, machineBootDate, symbolsMap, memory, cpcSi):
    # Create LPAR OSH ----------------------------------
    # version 9.0+ attributes --------------------------------------------------
    str_discovered_os_name = 'discovered_os_name'
    str_discovered_os_version = 'discovered_os_version'
    str_vendor = 'vendor'
    str_os_vendor = 'os_vendor'
    str_name = 'name'
    str_serial_number = 'serial_number'
    # version 8.0- attributes --------------------------------------------------
    if _CMDB_CLASS_MODEL.version() < 9:
        str_discovered_os_name = 'host_os'
        str_discovered_os_version = 'host_osversion'
        str_vendor = 'host_vendor'
        str_os_vendor = 'host_vendor' # duplicated
        str_name = 'data_name'
        str_serial_number = 'host_serialnumber'
    isComplete = 1
    zOsOsh = modeling.createHostOSH(defaultIp, 'zos', _STR_EMPTY, _STR_EMPTY, machineBootDate)
    zOsOsh.setAttribute(str_discovered_os_name, 'z/OS')
    zOsOsh.setBoolAttribute('host_iscomplete', isComplete)
    zOsOsh.setAttribute(str_discovered_os_version, zOsRelease)
    zOsOsh.setAttribute(str_os_vendor, 'IBM')
    zOsOsh.setAttribute(str_vendor, 'IBM')

    zOsOsh.setAttribute('ieasym_list', ieasymList)
    
    # Spencer: Set memory attribute
    zOsOsh.setAttribute('memory_size', memory)
    
    if isNotNull(cpcSi):
        cpcSiList = string.split(cpcSi, '.')
        if len(cpcSiList) == 5:
                cpcSerial = cpcSiList[4]
                zOsOsh.setAttribute(str_serial_number,cpcSerial )

    if isNotNull(symbolsMap) and len(symbolsMap) > 0:
        if symbolsMap.has_key('SYSNAME'):
            zOsOsh.setAttribute(str_name, symbolsMap['SYSNAME'])
        if symbolsMap.has_key('SYSALVL'):
            zOsOsh.setAttribute('mvs_architecture_level', symbolsMap['SYSALVL'])
        if symbolsMap.has_key('SYSCLONE'):
            zOsOsh.setAttribute('mvs_system_clone', symbolsMap['SYSCLONE'])
        if symbolsMap.has_key('SYSALVL'):
            zOsOsh.setAttribute('mvs_system_r1', symbolsMap['SYSR1'])

    return zOsOsh

def osh_createIpOsh(zOsOsh, defaultIp):
    _vector = ObjectStateHolderVector()
    # Create IP OSH ------------------------------------
    if eview_netutils._isValidIp(defaultIp):
        ipOsh = eview_netutils._buildIp(defaultIp)
        _vector.add(ipOsh)
        _vector.add(zOsOsh)
        if _CMDB_CLASS_MODEL.version() < 9:
            linkOsh = modeling.createLinkOSH('contained', zOsOsh, ipOsh)
            _vector.add(linkOsh)
        else:
            linkOsh = modeling.createLinkOSH('containment', zOsOsh, ipOsh)
            _vector.add(linkOsh)

    return _vector

def osh_createEviewOsh(localshell, zOsOsh, appPath, confFolder, file, nodeName, eviewVersion, defaultIp):
    # Create EView agent OSH ---------------------------------------------------
    logger.debug('Creating EView object')
    eviewOSH = ObjectStateHolder('eview')

    if _CMDB_CLASS_MODEL.version() >= 9:
        eviewOSH.setAttribute('name', nodeName)
        eviewOSH.setAttribute('discovered_product_name', nodeName)
        eviewOSH.setAttribute('version', eviewVersion)
    else:
        eviewOSH.setAttribute('data_name', nodeName)
        eviewOSH.setAttribute('application_version', eviewVersion)

    eviewOSH.setAttribute('application_path', appPath)
    eviewOSH.setAttribute('application_ip', defaultIp)
    eviewOSH.setAttribute('vendor', 'EView Technology Inc.')
    eviewOSH.setAttribute('eview_agent_type', 'z/OS')
    fileContents = localshell.safecat('%s%s' % (confFolder, file))
    address, port = eview_lib.getEviewAgentAddress(localshell, fileContents)
    if eview_lib.isNotNull(address):
        eviewOSH.setAttribute('application_ip', address)
    if eview_lib.isNotNull(port) and eview_lib.isnumeric(port):
        eviewOSH.setIntegerAttribute('application_port', int(port))
    eviewOSH.setContainer(zOsOsh)
    return eviewOSH

def processEviewConfFiles(Framework, localshell):
    _vector = ObjectStateHolderVector()
    fileMonitor = file_mon_utils.FileMonitor(Framework, localshell, ObjectStateHolderVector(), None, None)
    folder = Framework.getParameter('EViewInstallationFolder')
    if isNull(folder):
        logger.reportError('Job parameter - EViewInstallationFolder is empty. Set the path to the EView client installation root and rerun job.')
        return _vector

    appPath = fileMonitor.rebuildPath(folder) + "\\bin\\ev390hostcmd.exe"
    confFolder = fileMonitor.rebuildPath(folder) + "\\conf\\"
    confFiles = None
    try:
        confFiles = fileMonitor.listFilesWindows(confFolder, EViewConfFileNameFilter())
    except:
        logger.reportError('Unable to get EView configuration files from folder: %s' % confFolder)
        return _vector

    # Create zOS & EView agent objects -----------------------------------------
    if isNull(confFiles):
        logger.reportError('Unable to get EView configuration files from folder: %s' % confFolder)
        return _vector
    elif len(confFiles) < 1:
        logger.reportError('Unable to get EView configuration files from folder: %s' % confFolder)
        return _vector
    else:
        for file in confFiles:

            nodeName = file[13:len(file)]   # The name of the configuration file is ev390_config_<NODE_NAME>
            if eview_lib.isNotNull(nodeName):

                #===================================================================
                # connect to each node with configuration and only
                # create EView CI for the ones that connect
                #===================================================================
                ls = eview_lib.EvShell(Framework, nodeName, appPath)
                # Get EView agent version ------------------------------------------
                task = Framework.getParameter('EViewStartedTask')
                if isNull(task):
                    task = 'VP390'
                eviewVersion = ev1_getEviewVersion(ls, task)
                if eview_lib.isNotNull(eviewVersion):
                    logger.debug('Successfully executed command against EView agent on node: ', nodeName)

                    # Get IPL info -------------------------------------------------
                    (zOsRelease, ieasymList, ieasysList, machineBootDate) = ev2_getIplInfoOutput(ls)
                    
                    # Spencer: Get memory info -------------------------------------
                    memory = ev6_getMemoryOutput(ls)

                    # Get the default IP of the LPAR -------------------------------
                    defaultIp = ev_getDefaultIpOfLpar(ls)

                    if isNull(defaultIp):
                        logger.reportWarning('Unable to get IP Address of LPAR: %s. Continuing with next LPAR.' % nodeName)
                        continue
                    else:
                        # Get Symbols --------------------------------------------------
                        symbolsMap = ev5_getSymlistOutput(ls)   # {symbolName:symbolValue}
                        
                        # CPU List Command ---------------------------------------------------------
                        (cpuLists, cpcSi, cpcId, cpcName) = ev7_getCpulistOutput(ls)
                        
                        # Create zOS OSH ---------------------------------
                        zOsOsh = osh_createZOsOsh(defaultIp, zOsRelease, ieasymList, ieasysList, machineBootDate, symbolsMap, memory, cpcSi)
                        _vector.add(zOsOsh)

                        if isNotNull(zOsOsh):
                            # Create IP OSH and link it to zOS OSH -------------------------
                            _vector.addAll(osh_createIpOsh(zOsOsh, defaultIp))

                            # Create EView Agent OSH and link it to the zOS OSH ------------
                            eviewOSH = osh_createEviewOsh(localshell, zOsOsh, appPath, confFolder, file, nodeName, eviewVersion, defaultIp)
                            _vector.add(eviewOSH)
                else:
                    warnMsg = 'Unable to connect to: %s' % nodeName
                    logger.warn(warnMsg)
                    warnObj = errorobject.createError(errorcodes.CONNECTION_FAILED, None, warnMsg)
                    logger.reportWarningObject(warnObj)

    return _vector

######################
#        MAIN
######################
def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    logger.debug(" ###### Connecting to EView client")
    logger.info (os.getenv('COMPUTERNAME'))
    codePage = Framework.getCodePage()
    properties = Properties()
    properties.put(BaseAgent.ENCODING, codePage)

    localshell = None
    try:
        client = Framework.createClient(ClientsConsts.LOCAL_SHELL_PROTOCOL_NAME)
        localshell = shellutils.ShellUtils(client, properties, ClientsConsts.LOCAL_SHELL_PROTOCOL_NAME)
    except Exception, ex:
        exInfo = ex.getMessage()
        errormessages.resolveAndReport(exInfo, ClientsConsts.LOCAL_SHELL_PROTOCOL_NAME, Framework)
        logger.error(exInfo)
    except:
        exInfo = logger.prepareJythonStackTrace('')
        errormessages.resolveAndReport(exInfo, ClientsConsts.LOCAL_SHELL_PROTOCOL_NAME, Framework)
        logger.error(exInfo)
    else:
        OSHVResult.addAll(processEviewConfFiles(Framework, localshell))
        localshell.closeClient()

    return OSHVResult