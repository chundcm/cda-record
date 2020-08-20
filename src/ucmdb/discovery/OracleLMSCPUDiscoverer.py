# coding=utf-8
import re
import os
import file_system

import logger

from com.hp.ucmdb.discovery.library.common import CollectorsParameters
from com.hp.ucmdb.discovery.common import CollectorsConstants


def OracleCPUUtils(framework, client, shell):
    return OracleCPUDiscovererFactory().createDiscoverer(framework, client, shell)


class OracleCPUDiscovererFactory:
    def createDiscoverer(self, framework, client, shell):
        if shell.isWinOs():
            discoverer = WindowsOracleCPUDiscoverer(framework, client, shell)
        else:
            discoverer = UnixOracleCPUDiscoverer(framework, client, shell)
        return discoverer


class OracleCPUDiscoverer:
    def __init__(self, framework, client, shell):
        self.framework = framework
        self.shell = shell
        self.client = client
        self._fs = file_system.createFileSystem(self.shell)
        try:
            self.temp_folder = self._fs.getTempFolder()
        except:
            self.temp_folder = "/tmp/"
        self.fileName = None

    def discover(self):
        if self.uploadScript() != 0:
            logger.reportError("Failed to upload script to remote server.")
            return None, None
        else:
            path = self.executeScript()
            fileName = os.path.basename(path)
            fileContent = self.getFileContent(path)
            self.clearTmpFiles()
            return fileName, fileContent

    def uploadScript(self):
        raise NotImplementedError()

    def executeScript(self):
        raise NotImplementedError()

    def getFileContent(self, path):
        return self.shell.safecat(path)

    def clearTmpFiles(self):
        raise NotImplementedError()


class WindowsOracleCPUDiscoverer(OracleCPUDiscoverer):
    SCRIPT_NAME = 'lms_cpuq.cmd'

    def __init__(self, framework, client, shell):
        OracleCPUDiscoverer.__init__(self, framework, client, shell)
        computerName = self.getComputerName()
        self.fileName = self.temp_folder + computerName + '-lms_cpuq.txt'

    def getComputerName(self):
        command = 'echo %COMPUTERNAME%'
        output = self.shell.execCmd(command)
        if not self.shell.getLastCmdReturnCode() and output:
            return output


    def uploadScript(self):
        source = CollectorsParameters.PROBE_MGR_RESOURCES_DIR + WindowsOracleCPUDiscoverer.SCRIPT_NAME
        target = self.temp_folder + WindowsOracleCPUDiscoverer.SCRIPT_NAME
        logger.debug('Copy local ', source, " to remote ", target)
        return self.client.uploadFile(source, target, 1)

    def executeScript(self):
        # remove %temp%\lms_cpuq_tmp.txt
        self.shell.deleteFile(self.temp_folder + 'lms_cpuq_tmp.txt')

        # run the script                                                                    
        fileName = self.temp_folder + WindowsOracleCPUDiscoverer.SCRIPT_NAME
        command = "cmd /c " + '"' + fileName + ' ' + self.temp_folder + '"'
        output = self.shell.execCmd(command)
        if not self.shell.getLastCmdReturnCode() and output:
            self.parseNotice(output)
            return self.fileName

    def clearTmpFiles(self):
        self.shell.deleteFile(self.temp_folder + WindowsOracleCPUDiscoverer.SCRIPT_NAME)
        self.shell.deleteFile(self.fileName)
        self.shell.deleteFile(self.temp_folder + 'lms_cpuq_tmp.txt')
        self.shell.deleteFile(self.temp_folder + 'cpu_info.err')
        self.shell.deleteFile(self.temp_folder + 'lms_cpuq_tmp.vbs')


    def parseNotice(self, output):
        m = re.search('\*\* NOTICE:\s+([\s\S]*\s+\*\*)', output)
        if m:
            result = m.group(1).replace('**', '')
            messages = result.split('\r\n')
            if not self.framework.getDestinationAttribute('host_server_id'):
                logger.reportWarning(messages[0] + "\nYou should run a Virtualization Activity to discover the host data. For details, see Virtualization Activity in the HP Universal CMDB Discovery and Integration Content Guide.")
            elif not self.framework.getDestinationAttribute('host_server_audit_document'):
                logger.reportWarning(messages[0] + "\nYou should run Oracle LMS CPU discovery on the host operating system.")


class UnixOracleCPUDiscoverer(OracleCPUDiscoverer):
    SCRIPT_NAME = 'lms_cpuq.sh'

    def __init__(self, framework, client, shell):
        OracleCPUDiscoverer.__init__(self, framework, client, shell)
        self.fileName = None

    def uploadScript(self):
        source = CollectorsParameters.PROBE_MGR_RESOURCES_DIR + UnixOracleCPUDiscoverer.SCRIPT_NAME
        target = self.temp_folder + UnixOracleCPUDiscoverer.SCRIPT_NAME
        logger.debug('Copy local ', source, " to remote ", target)
        path_permission = os.access(target, os.W_OK)
        if not path_permission:
            self.temp_folder = "/tmp/"
            target = self.temp_folder + UnixOracleCPUDiscoverer.SCRIPT_NAME
            logger.debug('Copy local ', source, " to remote ", target)
        return self.client.uploadFile(source, target, 1)


    def executeScript(self):
        fileName = self.temp_folder + UnixOracleCPUDiscoverer.SCRIPT_NAME
        command = "sh " + fileName
        output = self.shell.execCmd(command)
        if not self.shell.getLastCmdReturnCode() and output:
            self.fileName = self.parseFileName(output).strip()
            self.parseNotice(output)
            return self.fileName

    def parseFileName(self, output):
        m = re.search('Please collect the output file generated:\s+(.+)', output)
        if m:
            return m.group(1)

    def clearTmpFiles(self):
        self._fs.removeFile(self.temp_folder + UnixOracleCPUDiscoverer.SCRIPT_NAME)
        if self.fileName:
            self._fs.removeFile(self.fileName)

    def parseNotice(self, output):
        m = re.search('(Current OS user[\s\S]*?)Script', output)
        if m:
            message = m.group(1)
            if message:
                logger.reportWarning(message)