#coding=utf-8
from __future__ import with_statement
from java.lang import String
from java.io import File
from java.lang import Exception as JException
from java.util import Date
from com.hp.ucmdb.discovery.library.common import CollectorsParameters

from import_utils import DataSource

import shellutils
import logger
import os

class FileDataSource(DataSource):
    """
    This is base class for all DataSources which are based on file in the file system.
    E.g. CsvFileDataSource or PropertyFileDataSource
    """
    def __init__(self, fileName, Framework, fileEncoding=None, isBinaryMode=False):
        DataSource.__init__(self)
        self.remoteFile = fileName
        self.localTempFile = None
        self.Framework = Framework
        self.isBinaryMode = isBinaryMode
        self.encoding = fileEncoding or CollectorsParameters.getDefaultOEMEncoding()


    """
    Download target CSV file to probe
    """
    def downloadRemoteCsvFile(self, client):
        try:
            logger.info("Downloading CSV file to Probe...")
            fileSeparator = File.separator
            timeStamp = Date().getTime()
            logger.debug(timeStamp)
            self.localTempFile = CollectorsParameters.BASE_PROBE_MGR_DIR + CollectorsParameters.getDiscoveryResourceFolder() + fileSeparator + "tempCsv" + str(timeStamp) + ".csv"
            if client.downloadFile(self.localTempFile, self.remoteFile, 1):
                logger.warn("Failed to download remote CSV file, abort...")
                raise Exception("Failed to download remote CSV file")
            logger.info("Remote CSV file has been downloaded to %s" % self.localTempFile)
        except:
            logger.debugException('')


    """
    Shell client creation and file content retrieving is done here.
    Client closing is also performed here because there is no need to keep
    open connection after file content is transfered to the Probe machine
    """
    def open(self):
        client = None
        try:
            client = self.Framework.createClient()
            if self.isBinaryMode:
                self.downloadRemoteCsvFile(client)
                with open(self.localTempFile, 'r') as f:
                    fileContent = f.read().decode(self.encoding).encode('utf-8')
            else:
                shell = shellutils.ShellUtils(client)
                fileContent = self.getFileContent(shell, self.remoteFile)
            bytes = self.getBytes(String(fileContent))
            self.data = self.parseFileContent(bytes)
        finally:
            if client:
                client.close()
            # Remove temp CSV file from probe
            if self.localTempFile and os.path.isfile(self.localTempFile):
                logger.info("Delete temp CSV file from probe")
                os.remove(self.localTempFile)

            
    """
    Getting file content via remote shell
    """
    def getFileContent(self, shell, fileName):
        if shell.isWinOs() and self.encoding.lower() == 'utf-8':
            shell.setCodePage(65001)
            shell.useCharset("UTF-8")
        return shell.safecat(fileName)    
    
    """
    Most of existing file based DataSources needs file bytes to operate with
    """
    def getBytes(self, fileContent):
        return fileContent.getBytes(self.encoding)        
    
    """
    Nothing to close here, since connection is closed in "open" method
    """
    def close(self):
        pass
    
    """
    Abstract method which should be implemented by derived classes
    """
    def parseFileContent(self, bytes):
        "Each file-based DataSource should parse file content. File content if of java.lang.String type"       
        raise NotImplementedError, "parseContent"
