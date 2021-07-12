#coding=utf-8
import re
import sys
import logger
from file_ver_lib import getLinuxFileVer
import modeling

def parseVersion(buff):
    matchVersion = re.search("Ver\s+((\d+\.?)+)", buff)
    matchFullVersion = re.search("Ver\s+(.+)", buff)
    if matchVersion:
        version = matchVersion.group(1)
        fullVersion = matchFullVersion.group(1) 
        return version, fullVersion

def parseVersion2(buff):
    matchVersion = re.search("Distrib\s+((\d+\.?)+)", buff)
    matchFullVersion = re.search("Ver\s+(.+)", buff)
    if matchVersion:
        version = matchVersion.group(1)
        fullVersion = matchFullVersion.group(1)
        return version, fullVersion
 
def getWindowsVersion(path, client):
    if path:
        cmd = '"' + path.replace('\/','\\') + '"' +' --version'
        buff = client.execCmd(cmd, 60000)
        if buff and client.getLastCmdReturnCode() == 0:
            return parseVersion(buff)
        else:
            cmd = cmd.replace('mysqld', 'mysql')
            buff = client.execCmd(cmd, 60000)
            if buff and client.getLastCmdReturnCode() == 0:
                return parseVersion2(buff)
    return '', ''
    
def getUnixVersion(path, client):
    if path:
        cmd = path + ' --version'
        buff = client.execCmd(cmd, 60000)
    if buff and client.getLastCmdReturnCode() == 0:
        return parseVersion(buff)
    else:
        version = getLinuxFileVer(client, path)
        if version:
            return version, version
        else:
            cmd = cmd.replace('/sbin/mysqld', '/bin/mysql')
            buff = client.execCmd(cmd, 60000)
            if buff and client.getLastCmdReturnCode() == 0:
                return parseVersion2(buff)


def setVersion(mysqlOsh, path, client):
    try:                
        if client.isWinOs():
            version, fullVersion = getWindowsVersion(path, client)
        else:
            version, fullVersion = getUnixVersion(path, client)
        if version:
            mysqlOsh.setAttribute("application_version_number", version)

            if fullVersion and 'MariaDB' in fullVersion:
                mysqlOsh.setAttribute("data_name", "Maria DB")
                mysqlOsh.setAttribute("discovered_product_name", "Maria DB")

            mysqlOsh.setAttribute("application_version", modeling.parseMySQLVersionforApplication(fullVersion))
            modeling.setDatabaseVersion(mysqlOsh, version)
            logger.debug("MySQL version : " + version)
        if fullVersion:
            if 'MariaDB' in fullVersion:
                mysqlOsh.setAttribute("name", 'MariaDB')
                # QCCR1H112169 (cont) Enhanced Discovery for MySQL by Host Application by Shell (VW)
                mysqlOsh.setAttribute("discovered_product_name","Maria DB")
                mysqlOsh.setAttribute("vendor",None)
            else:
                mysqlOsh.setAttribute("name", 'MySQL')
                mysqlOsh.setAttribute("vendor","oracle_corp")
        else:
            logger.error('Failed getting MySQL version')
    except:
        errMsg = 'Failed getting MySQL version. Exception received: %s' % (sys.exc_info()[1])
        logger.errorException(errMsg)
        mysqlOsh.setAttribute("name", 'MySQL')
