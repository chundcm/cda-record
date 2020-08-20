import re
import os
import sys
import logger
import modeling
import shellutils
import netutils
import errormessages
import shellutils
from java.lang import Exception as JException
from appilog.common.system.types.vectors import ObjectStateHolderVector
from appilog.common.system.types import AttributeStateHolder, ObjectStateHolder
from javax.xml.xpath import XPathConstants
import jee_discoverer
import jee
import file_system
import websphere_discoverer

LCSERVER_CONFIG_FILE = 'server.xml'
LCSERVER_BOOT_CONFIG_FILE = 'bootstrap.properties'
WIN_SUFFIX_SERVER = "\\usr\\servers\\"
WIN_SUFFIX_BIN = "\\bin\\"
WIN_SEP = "\\"
UNIX_SUFFIX_SERVER = "/usr/servers/"
UNIX_SUFFIX_BIN = "/bin/"
UNIX_SEP = "/"

class ConfigurationFile:
    def __init__(self, shell, location):
        self.location = location
        self.shell = shell
        self.path = None
        self.name = None
        self.content = self.getConfigurationContent()
        self.isxml = self.isXML()
        self.getNamePath()

    def getNamePath(self):
        self.path = os.path.dirname(self.location)
        self.name = self.location[len(self.path)+1:]

    def getConfigurationContent(self):
        try:
            configFileContent = self.shell.safecat(self.location)
        except:
            configFileContent = None
            logger.debug(self.location + ' wasn\'t found')
        if configFileContent and self.shell.getLastCmdReturnCode() == 0:
                return configFileContent

    def isXML(self):
        isxml = 0
        m = re.match('.*\.xml', self.location)
        if m:
            isxml = 1
        return isxml

class LibertyCoreServer:
    def __init__(self, shell, j2eeCmdLine, maxDepth):
        self.shell = shell
        self.name = None
        self.j2eeCmdLine = j2eeCmdLine
        self.serverName = None
        self.versionNum = None
        self.versionDetails = None
        self.applicationBase = None
        self.binDir = None
        self.serverDir = None
        self.serverSuffix = None
        self.binSuffix = None
        self.sep = None
        self.paramDict = {}
        self.CONFIG_FILE_INCLUDE_MAX_DEPTH = maxDepth
        self.applicationList = []
        self.datasourceList = []
        self.configFileList = []

    def discoverLCServer(self):
        self.getDirs()
        self.getVersionDetail()
        self.getParamDict()
        serverconfig_location = self.serverDir + LCSERVER_CONFIG_FILE
        serverConfigFile = ConfigurationFile(self.shell, serverconfig_location)
        if serverConfigFile.content:
            self.discoverIncludes(serverConfigFile)
        else:
            logger.warn("Nothing exist in server.xml file, discovery aborted")

    def getParamDict(self):
        boot_location = self.serverDir + LCSERVER_BOOT_CONFIG_FILE
        bootConfig = ConfigurationFile(self.shell, boot_location)
        content = bootConfig.content
        if content:
            self.configFileList.append(bootConfig)
            contentList = content.split("\n")
            for i in contentList:
                m = re.search("(\S+)=(\S+)", i)
                if m:
                    self.paramDict[m.group(1)] = m.group(2)
        else:
            logger.info("The file bootstrap.properties is not exist or nothing exist in file content.")

    def getVersionDetail(self):
        cmd = self.binDir + 'server version'
        versionDetails = self.shell.execCmd(cmd)
        if versionDetails and self.shell.getLastCmdReturnCode() == 0:
            self.versionDetails = versionDetails
            m = re.match('WebSphere\sApplication\sServer\s(\S+)\s(.*)', versionDetails)
            if m:
                self.versionNum = m.group(1)

    def getDirs(self):
        if self.shell.isWinOs():
            self.serverSuffix = WIN_SUFFIX_SERVER
            self.binSuffix = WIN_SUFFIX_BIN
            self.sep = WIN_SEP
            m = re.match(r'(.*)-jar\s"(.*)\\bin\\tools\\ws-server.jar"\s.*\sstart\s(\S+)', self.j2eeCmdLine)
            if m:
                 self.applicationBase = m.group(2)
                 self.serverName = m.group(3)
            else:
                logger.info("Failed to parse Base Dir for server %s on Windows", self.serverName)
        else:
            self.serverSuffix = UNIX_SUFFIX_SERVER
            self.binSuffix = UNIX_SUFFIX_BIN
            self.sep = UNIX_SEP
            m = re.match('(.*)-jar\s(.*)\/bin\/tools\/ws-server.jar\s(\S+)', self.j2eeCmdLine)
            if m:
                self.applicationBase = m.group(2)
                self.serverName = m.group(3)
            else:
                logger.info("Failed to parse Base Dir for server %s on Unix", self.serverName)
        self.name = self.applicationBase + " - " + self.serverName
        self.serverDir = self.applicationBase + self.serverSuffix + self.serverName + self.sep
        self.binDir = self.applicationBase + self.binSuffix + self.sep

    def discoverIncludes(self, configFileObject, configFileList = None, includeDepth = 0):
        configFileLoadSequence = []
        # If max recursion depth is reached stop discovering deeper
        if includeDepth > self.CONFIG_FILE_INCLUDE_MAX_DEPTH:
            return
        currentIncludeDepth = includeDepth + 1
        if configFileList != None:
            configFileLoadSequence.extend(configFileList)
        configFileLoadSequence.append(configFileObject.location)
        libertyParse = LibertyParser(0,configFileObject, self.paramDict, self.sep)
        self.applicationList.extend(libertyParse.parseApplication())
        self.datasourceList.extend(libertyParse.parseDataSource())
        self.configFileList.append(configFileObject)
        includeList = libertyParse.parseIncludes()
        for include in includeList:
            includedConfigFileObject = ConfigurationFile(self.shell, include['location'])
            # Avoid circular include
            if includedConfigFileObject.location in configFileLoadSequence:
                continue
            # Avoid to discover config file with empty content
            if includedConfigFileObject.content:
                # Avoid to parse non-XML file
                if includedConfigFileObject.isxml:
                    self.discoverIncludes(includedConfigFileObject, configFileLoadSequence, currentIncludeDepth)
                else:
                    logger.info("The fie %s is not a XML file.", includedConfigFileObject.location)
                    self.configFileList.append(includedConfigFileObject)
            else:
                logger.info("The content of file %s is empty, ignore it", includedConfigFileObject.location)

def createLibertyCoreOSH(hostOSH, name, version, version_details):
    osh = modeling.createApplicationOSH('j2eeserver', 'WebSphere Liberty Core Server', hostOSH, 'J2EE', 'ibm_corp')
    osh.setAttribute('name', name)
    if version:
        osh.setAttribute('version', version)
    if version_details:
        osh.setAttribute('application_version', version_details)
    return osh

def createDomainOSH(name):
    osh = ObjectStateHolder('j2eedomain')
    osh.setAttribute('data_name', name)
    return osh

def createDataSourceOSH(domainOSH, name, jndi, driver):
    osh = ObjectStateHolder('jdbcdatasource')
    if name:
        osh.setAttribute('data_name', name)
    else:
        osh.setAttribute('data_name', jndi)
    osh.setAttribute('jdbcdatasource_jndiname', jndi)
    if driver:
        osh.setAttribute('jdbcdatasource_drivername', driver)
    osh.setContainer(domainOSH)
    return osh

def createApplicationOSH(name, location):
    appOSH = None
    if name:
        appOSH = ObjectStateHolder('j2eeapplication')
        appOSH.setAttribute('data_name', name)
        appOSH.setAttribute('resource_path', location)
    return appOSH

class LibertyParser(jee_discoverer.BaseXmlParser):
    DATASOURCE_LIST = ['id', 'jndiName', 'jdbcDriverRef']
    APPLICATION_LIST = ['name', 'location']
    INCLUDE_LIST = ['location']
    def __init__(self, loadExternalDtd, configFile, paramDict, sep):
        jee_discoverer.BaseXmlParser.__init__(self, loadExternalDtd)
        self.configFile = configFile
        self.paramDict = paramDict
        self.sep = sep
        self.document = self._buildDocumentForXpath(configFile.content, namespaceAware = 0)

    def baseParser(self, Obj, attributeList):
        resultList = []
        #resultDict = {}
        objList = self._getXpath().evaluate('//server/' + Obj, self.document, XPathConstants.NODESET)
        if objList and objList.getLength():
            for i in range(0, objList.getLength()):
                resultDict = {}
                objNode = objList.item(i)
                for j in attributeList:
                    resultDict[j] = objNode.getAttribute(j)
                resultList.append(resultDict)
        return resultList

    def parseDataSource(self):
        return self.baseParser('dataSource', LibertyParser.DATASOURCE_LIST)

    def parseApplication(self):
        appList = self.baseParser('application', LibertyParser.APPLICATION_LIST)
        webappList = self.baseParser('webApplication', LibertyParser.APPLICATION_LIST)
        entappList = self.baseParser('enterpriseApplication', LibertyParser.APPLICATION_LIST)
        if webappList:
            appList.extend(webappList)
        if entappList:
            appList.extend(entappList)
        return appList

    def parseIncludes(self):
        includeList = self.baseParser('include', LibertyParser.INCLUDE_LIST)
        for include in includeList:
            include['location'] = self.locationTransfer(include['location'])
        return includeList

    def locationTransfer(self, location):
        if location:
            s = re.search('\$\{(.*)\}/(.*)', location)
            if s:
                split_list = re.split('/', s.group(2))
                split_location = self.sep.join(split_list)
                location_trans = self.paramDict[s.group(1)] + self.sep + split_location
            else:
                m = re.match('\/', location)
                if m:
                    location_trans = location
                else:
                    location_trans = self.configFile.path + self.sep + location
        return location_trans

def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    shell = None
    hostPrimaryIP = Framework.getDestinationAttribute('ip_address')
    protocol = Framework.getDestinationAttribute('Protocol')
    libertyCmdLines = Framework.getTriggerCIDataAsList('websphere_process_cmdline')
    maxDepth = Framework.getParameter('max_include_depth')
    errorMessage = None
    try:
        client = Framework.createClient()
        try:
            shell = shellutils.ShellUtils(client)
            fs = websphere_discoverer._createFileSystemRecursiveSearchEnabled(file_system.createFileSystem(shell))
            layOut = jee_discoverer.Layout(fs)
            hostOSH = modeling.createHostOSH(hostPrimaryIP)
            OSHVResult.add(hostOSH)
            jvmtopoBuilder = jee.ServerTopologyBuilder()
            for item in libertyCmdLines:
                try:
                    ## create websphere liberty core server
                    libertyCmdLine = item
                    libertyCoreServer = LibertyCoreServer(shell, libertyCmdLine, int(maxDepth))
                    libertyCoreServer.discoverLCServer()
                    libertyOSH = createLibertyCoreOSH(hostOSH, libertyCoreServer.name, libertyCoreServer.versionNum, libertyCoreServer.versionDetails)
                    OSHVResult.add(libertyOSH)
                    ## create a virtual j2ee domain server
                    domainName = 'was_liberty_core@' + libertyCoreServer.name
                    domainOSH = createDomainOSH(domainName)
                    OSHVResult.add(domainOSH)
                    memberOSH = modeling.createLinkOSH('membership', domainOSH, libertyOSH)
                    OSHVResult.add(memberOSH)
                    ## jvm discovery
                    cmdLineDescriptor = jee.JvmCommandLineDescriptor(libertyCmdLine)
                    serverRuntime = jee_discoverer.ServerRuntime(cmdLineDescriptor, hostPrimaryIP)
                    jvmDiscover = jee_discoverer.JvmDiscovererByShell(shell, layOut)
                    jvm = jvmDiscover.discoverJvmByServerRuntime(serverRuntime)
                    jvmOSH = jvmtopoBuilder.buildJvmOsh(jvm)
                    jvmOSH.setContainer(libertyOSH)
                    OSHVResult.add(jvmOSH)
                    ## create configuration document
                    for configfile in libertyCoreServer.configFileList:
                        configFileOsh = modeling.createConfigurationDocumentOSH(configfile.name, configfile.path, configfile.content, libertyOSH)
                        OSHVResult.add(configFileOsh)
                    ## create datasources
                    for ds in libertyCoreServer.datasourceList:
                        dsOSH = createDataSourceOSH(domainOSH, ds['id'], ds['jndiName'], ds['jdbcDriverRef'])
                        OSHVResult.add(dsOSH)
                        deployOSH = modeling.createLinkOSH('deployed', libertyOSH, dsOSH)
                        OSHVResult.add(deployOSH)
                    ## create application
                    for app in libertyCoreServer.applicationList:
                        appOSH = createApplicationOSH(app['name'], app['location'])
                        if appOSH:
                            appOSH.setContainer(libertyOSH)
                            OSHVResult.add(appOSH)
                except:
                    logger.debugException('')
                    Framework.reportWarning('Failed to discover one or more websphere liberty core server configuration.')
        finally:
            try:
                shell and shell.closeClient()
            except:
                logger.debugException('')
                logger.error('Unable to close shell')
    except JException, ex:
        errorMessage = ex.getMessage()
    except:
        errorObject = sys.exc_info()[1]
        if errorObject:
            errorMessage = str(errorObject)
        else:
            errorMessage = logger.prepareFullStackTrace('')
    if errorMessage:
        logger.debugException(errorMessage)
        errormessages.resolveAndReport(errorMessage, protocol, Framework)
    return OSHVResult