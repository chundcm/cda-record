#coding=utf-8
from plugins import Plugin
from appilog.common.system.types.vectors import ObjectStateHolderVector
from appilog.common.system.types import ObjectStateHolder

import re
import logger
import shellutils
import ip_addr
import netutils
import modeling

from com.hp.ucmdb.discovery.library.clients import ClientsConsts
from xml.dom import minidom

class ApacheTomcatPlugin(Plugin):

    def __init__(self):
        Plugin.__init__(self)
        self.__client = None
        self.__applicationOsh = None
        self.version = None
        self.separator = None

    def isApplicable(self, context):
        self.__client = context.client
        self.__applicationOsh = context.application.getOsh()
        return 1

    def process(self, context):
        tomcatHomeDir = None
        if self.__client.getClientType() == ClientsConsts.SNMP_PROTOCOL_NAME:
            self.__applicationOsh.setObjectClass('application')
        else:
            if self.__client.getClientType() == ClientsConsts.WMI_PROTOCOL_NAME:
                self.separator = '\\'
            else:
                try:
                    tomcatHomeDir = None
                    osBuff = self.__client.execCmd('ver')
                    if osBuff is not None:
                        osBuff = osBuff.lower()
                        if (osBuff.lower().find('windows') > -1 or osBuff.lower().find('ms-dos') > -1):
                            self.separator = '\\'
                        else:
                            self.separator = '/'
                except:
                    logger.debugException('Failed to determine file separator, suppose Windows')
                    self.separator = '\\'
            try:
                catalinaBase = catalinaHome = None
                if self.separator is not None:
                    #there always should be only one process - java/tomcat process is main process (and the only process) in tomcat signatures
                    process = context.application.getProcesses()[0]
                    procName = process.getName()
                    cmdLine = process.commandLine
                    if cmdLine is None:
                        cmdLine = process.executablePath
                        
                    if cmdLine is not None:
                        catalinaBase, catalinaHome = self.getTomcatHomeDir(procName, cmdLine)

                if catalinaBase or catalinaHome:
                    version = self.resolveVersion(catalinaBase, catalinaHome, procName)
                    if version is not None:
                        self.__applicationOsh.setAttribute("application_version_number", version)
                    tomcatHomeDir = catalinaHome or catalinaBase
                    self.__applicationOsh.setAttribute("webserver_configfile", tomcatHomeDir + 'conf' + self.separator + 'server.xml')
                    self.__applicationOsh.setAttribute("application_path", tomcatHomeDir)
                else:
                    self.__applicationOsh.setObjectClass('application')
                    logger.debug('Failed to identify Apache Tomcat configuration file path, creating software element instead of tomcat strong type')
            except:
                logger.debugException('Failed to process Apache Tomcat info')
                self.__applicationOsh.setObjectClass('application')
        if procName.find("Agent") == -1:
            self.__applicationOsh.setAttribute("data_name", 'Apache Tomcat')
        else:
            self.__applicationOsh.setAttribute("data_name", 'TomcatAgent')
            self.__applicationOsh.setAttribute("application_category", 'Agent')
            self.__applicationOsh.setAttribute("product_name", 'tomcat_agent')

        #reportEndpointByConfigFile
        if self.__client.getClientType() != ClientsConsts.WMI_PROTOCOL_NAME and tomcatHomeDir is not None:
            self.reportEndpointByConfigFile(context,tomcatHomeDir)

    def reportEndpointByConfigFile(self,context,tomcatHomeDir):
        logger.debug("reporting endpoints for apache tomcat using configfile ")
        endpointOSHV = ObjectStateHolderVector()
        if self.separator == '\\':
            cmd = 'type "' + tomcatHomeDir + 'conf' + self.separator + 'server.xml"'
        else:
            cmd = 'cat ' + tomcatHomeDir + 'conf' + self.separator + 'server.xml'
        fileContent = self.__client.execCmd(cmd)
        lastErrorCode = self.__client.getLastCmdReturnCode
        if lastErrorCode != 0:
            logger.warn('Endpoints for apache tomcat cannot be reported due to server.xml content is not found')
            return
        logger.debug('the server.xml content is,',fileContent)
        dom = minidom.parseString(fileContent)
        ports = []
        for element in dom.getElementsByTagName('Connector'):
            try:
                portValue = int(element.attributes['port'].value)
                ports.append(portValue)
            except ValueError, ex:
                logger.warn(str(ex))
                logger.debug("The port: %s is vaild" % element.attributes['port'].value)
        for port in ports:
            if context.application.getApplicationIp():
                ip = context.application.getApplicationIp()
            endpoint = netutils.Endpoint(port, netutils.ProtocolType.TCP_PROTOCOL, ip)
            endpointOSH = modeling.createIpServerOSH(endpoint)
            hostosh = modeling.createHostOSH(ip)
            endpointOSH.setContainer(hostosh)
            linkOsh = modeling.createLinkOSH("usage", context.application.getOsh(), endpointOSH)
            endpointOSHV.add(endpointOSH)
            endpointOSHV.add(linkOsh)
            logger.debug('Get ip using configfile:', ip)
            logger.debug('Get port using configfile:', port)
        if endpointOSHV:
            context.resultsVector.addAll(endpointOSHV)

    def getTomcatHomeDir(self, procName, cmdLine):
        #try to find out home directory from java parameters
        catalinaBase = self.extractHomeDirFromCmdLine(cmdLine, '-Dcatalina.base=')
        catalinaHome = self.extractHomeDirFromCmdLine(cmdLine, '-Dcatalina.home=')
        if catalinaHome is None:
            #we try to discover if this is pure tomcat installation - tomcat<version> command placed under catalina.home\bin
            #and if so we can assume that this is tomcat process
            #for example we search if process is like c:\tomcat\bin\tomcat5.exe
            version = self.__parseVersionByName(procName)
            if version is not None:
                index = cmdLine.lower().find(self.separator + 'bin' + self.separator + procName.lower())
                if index != -1:
                    catalinaHome = cmdLine[0:index + 1]
        if catalinaBase is None:
            pattern = re.compile(r'(\w\:\\[\w\s\d-]+)[\w\s\.\d\-\\]+(tomcat(?:\d+)?)\.exe', re.I)
            m = pattern.search(cmdLine)
            if m:
                catalinaBase = m.group(1) + self.separator + 'Base' + self.separator + m.group(2)

        catalinaHome = self.normalizeDir(catalinaHome)
        catalinaBase = self.normalizeDir(catalinaBase)
        # QCCR1H117644: [CR]If 'server.xml' is not found during tomcat discovery, exceptions are logged
        catalinaHome = catalinaHome if self.pathExists(self.__client.isWinOs(), catalinaHome) else None
        catalinaBase = catalinaBase if self.pathExists(self.__client.isWinOs(), catalinaBase) else None
        logger.debug('Catalina home:', catalinaHome)
        logger.debug('Catalina base:', catalinaBase)
        return catalinaBase, catalinaHome

    def extractHomeDirFromCmdLine(self, cmdLine, param):
        procParentFolder = None
        catalinaParam = param.lower()
        index = cmdLine.lower().find(catalinaParam)
        if index != -1:
            nextIndex = cmdLine.lower().find(' -d', index + len(catalinaParam))
            if nextIndex == -1:
                nextIndex = cmdLine.lower().find('-classpath', index + len(catalinaParam))
                if nextIndex == -1:
                    nextIndex = cmdLine.lower().find(' org.apache.catalina.startup.bootstrap', index + len(catalinaParam))
            procParentFolder = cmdLine[index + len(catalinaParam):nextIndex]
        if (procParentFolder is not None) and (len(procParentFolder) == 0):
            procParentFolder = None

        procParentFolder = self.normalizeDir(procParentFolder)
        if procParentFolder is not None:
            #checking if this is absolute path
            if not ((procParentFolder[1] == ':') or (procParentFolder[0] == self.separator)):
                procParentFolder = None
        return procParentFolder

    def normalizeDir(self, dir):
        #some time windows return command line with unix slash /
        #we also want to remove quotes from command line
        if dir is not None:
            dir = dir.strip()
            if dir[0] == '"':
                dir = dir[1:]
            if dir[len(dir) - 1] == '"':
                if dir.endswith('" "'):
                    dir = dir[:-3]
                else:
                    dir = dir[0:len(dir) - 1]
            if dir[len(dir) - 1] != self.separator:
                dir = dir + self.separator
            dir = dir.replace('/', self.separator).replace('\\', self.separator)
            dir = dir.strip()
        return dir

    def pathExists(self, isWin, path):
        if not path:
            return 0
        if isWin:
            command = "if EXIST \"%s\" echo 0" % path
            output = self.__client.execCmd(command)  # @@CMD_PERMISION shell protocol execution
            # code = self.shellUtils.getLastCmdReturnCode()
            # if code == 0 and
            if output is not None:
                output = output.strip()
                if output == '0':
                    return 1
        else:
            command = "ls -d \"%s\"" % path
            self.__client.execCmd(command)
            code = self.__client.getLastCmdReturnCode()
            if code == 0:
                return 1
        return 0

    def resolveVersion(self, catalinaBase, catalinaHome, procName):
        'str, str -> str'
        version = None
        if (self.__client.getClientType() == ClientsConsts.SSH_PROTOCOL_NAME
            or self.__client.getClientType() == ClientsConsts.TELNET_PROTOCOL_NAME
            or self.__client.getClientType() == ClientsConsts.NTCMD_PROTOCOL_NAME
            or self.__client.getClientType() == ClientsConsts.OLD_NTCMD_PROTOCOL_NAME
            or self.__client.getClientType() == shellutils.PowerShell.PROTOCOL_TYPE):
            version = self.__parseVersionByConfigFiles(catalinaBase)
            if not version and catalinaHome and catalinaHome != catalinaBase:
                version = self.__parseVersionByConfigFiles(catalinaHome)
        if version is None:
            version = self.__parseVersionByName(procName)
        return version

    def __parseVersionByConfigFiles(self, tomcatHomeDir):
        if not tomcatHomeDir:
            return None

        try:
            if self.separator == '\\':
                cmd = 'type "' + tomcatHomeDir + 'webapps' + self.separator + 'ROOT' + self.separator + 'RELEASE-NOTES.txt"'
            else:
                cmd = 'cat ' + tomcatHomeDir + 'webapps' + self.separator + 'ROOT' + self.separator + 'RELEASE-NOTES.txt'
            buff = self.__client.execCmd(cmd)
            if buff is not None:
                match = re.search('Apache Tomcat Version ([\.\d]+)', buff)
                if match is not None:
                    return str(match.group(1)).strip()
        except:
            logger.debugException('Failed to resolve version from RELEASE-NOTES.txt')

        try:
            if self.separator == '\\':
                cmd = 'type "' + tomcatHomeDir + 'RELEASE-NOTES"'
            else:
                cmd = 'cat ' + tomcatHomeDir + 'RELEASE-NOTES'
            buff = self.__client.execCmd(cmd)
            if buff is not None:
                match = re.search('Apache Tomcat Version ([\.\d]+)', buff) or re.search('\* Tomcat ([\.\d]+)', buff)
                if match is not None:
                    return str(match.group(1)).strip()
        except:
            logger.debugException('Failed to resolve version from RELEASE-NOTES')

        try:
            if self.separator == '\\':
                cmd = 'type "' + tomcatHomeDir + 'RUNNING.txt"'
            else:
                cmd = 'cat ' + tomcatHomeDir + 'RUNNING.txt'
            buff = self.__client.execCmd(cmd)
            if buff is not None:
                match = re.search('Running The Tomcat ([\.\d]+)', buff)
                if match is not None:
                    return str(match.group(1)).strip()
        except:
            logger.debugException('Failed to resolve version from RUNNING.txt')

        try:
            if self.separator == '\\':
                cmd = '"' + tomcatHomeDir + 'bin\\version.bat"'
            else:
                cmd = tomcatHomeDir + 'bin/version.sh'
            buff = self.__client.execCmd(cmd)
            if buff is not None:
                match = re.search('Server version: Apache Tomcat/([\.\d]+)', buff)
                if match is not None:
                    return str(match.group(1)).strip()
        except:
            logger.debugException('Failed to resolve version from ')
        return None

    def __parseVersionByName(self, procName):
        match = re.search(r'.*tomcat(\d).*', procName.lower())
        if match:
            return match.group(1).strip()
        return None
