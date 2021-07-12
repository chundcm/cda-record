#coding=utf-8
import re
import netutils
import ip_addr
import socket
import dns_resolver

from plugins import Plugin
from appilog.common.system.types.vectors import ObjectStateHolderVector
from appilog.common.system.types import ObjectStateHolder

from java.net import InetSocketAddress
import logger
import modeling
import file_system
import websphere_discoverer
from xml.dom import minidom

from applications import IgnoreApplicationException
from com.hp.ucmdb.discovery.library.clients import ClientsConsts

class WebsphereServerPluginWindows(Plugin):
    def __init__(self):
        Plugin.__init__(self)
        self.pattern = r'com\.ibm\.ws\.runtime\.WsServer\s+"?[^"]*"?\s+([^\s]*)\s+([^\s]*)\s+([^\s]*)\s*'

    def isApplicable(self, context):
        return 1

    def process(self, context):
        processWebsphere(context, 'java.exe', self.pattern)

class WebsphereServerPluginUnix(Plugin):
    def __init__(self):
        Plugin.__init__(self)
        self.pattern = r'com\.ibm\.ws\.runtime\.WsServer\s+"?[^"|\s]*"?\s+([^\s]*)\s+([^\s]*)\s+([^\s]*)\s*'

    def isApplicable(self, context):
        return 1

    def process(self, context):
        processWebsphere(context, 'java', self.pattern)

def processWebsphere(context, processName, pattern):
    wasOsh = context.application.getOsh()
    cellName = None
    serverName = None
    process = context.application.getProcess(processName)
    processOriginCmd = process.commandLine
    if processOriginCmd is not None:
        logger.debug('For process id ', process.getPid(), ' found original command line ', processOriginCmd)
        m = re.search(pattern, processOriginCmd)
        p = re.search('.*Duser.install.root=/usr/(.*?)/(.*?)/AppServer', processOriginCmd)
        q = re.search('.*DjvmName=(.*?)\s', processOriginCmd)
        s = re.search('.*Dosgi.configuration.area=/usr/(.*?)/(.*?)/(.*?)/(.*?)/(.*?)/', processOriginCmd)
        if m is not None:
            cellName = m.group(1)
            serverName = m.group(3)
            fullName = ''.join([m.group(2), '_', m.group(3)]).strip()
            logger.debug('Parsed out server name ', serverName, ' in cell ', cellName)
        elif p is not None and q is not None:
            cellName = p.group(2)
            serverName = q.group(1)
            fullName = ''.join([p.group(2), '_', q.group(1)]).strip()
            logger.debug('Parsed out server name ', serverName, ' in cell ', cellName)
        elif s is not None:
            cellName = s.group(2)
            serverName = s.group(5)
            fullName = ''.join([s.group(2), '_', s.group(5)]).strip()
            logger.debug('Parsed out server name ' + serverName + ' in cell ' + cellName)
        else:
            logger.debug('Failed to parse out cell name and server name from command line')

    if serverName is not None:
        wasOsh.setStringAttribute('j2eeserver_servername', serverName)
        if fullName:
            wasOsh.setStringAttribute('j2eeserver_fullname', fullName)
        modeling.setJ2eeServerAdminDomain(wasOsh, cellName)
        modeling.setAppServerType(wasOsh)
    else:
        raise IgnoreApplicationException('WebSphere details cannot be acquired, ignoring the application')

    if context.client.getClientType() != ClientsConsts.WMI_PROTOCOL_NAME:
        reportendPortByConfigfile(context,process)

def reportendPortByConfigfile(context,process):
    logger.debug("reporting endpoints for websphere using configfile ")
    shell = context.client
    fs = file_system.createFileSystem(shell)
    pathUtil = file_system.getPath(fs)
    endpointOSHV = ObjectStateHolderVector()
    reg = r'com\.ibm\.ws\.runtime\.WsServer\s+"?([^"]*)"?\s+([^\s]*)\s+([^\s]*)\s+([^\s]*)\s*'
    processOriginCmd = process.commandLine
    m = re.search(reg, processOriginCmd)
    if not m:
        logger.reportWarning(
            'Failed getting required information from Websphere process data. Skipping endpoints by config file reporting.')
        return

    config_root_path = m.group(1)
    cell_name = m.group(2)
    node_name = m.group(3)
    configHomeDirPath = pathUtil.dirName(config_root_path)
    node_root_Path = pathUtil.join(pathUtil.normalizePath(configHomeDirPath),'config','cells',cell_name,'nodes',node_name)
    nodeLayout = websphere_discoverer.NodeLayout(node_root_Path, fs)
    try:
        fileContent = nodeLayout.getFileContent(nodeLayout.getServerIndexPath()).content
    except:
        logger.debugException('Failed to get file content. Aborting.')
        logger.reportWarning('Failed to get Webspphere configuration file. Skipping endpoints by config file reporting.')
        return

    dom = minidom.parseString(fileContent)

    for element in dom.getElementsByTagName('endPoint'):
        ip = None
        host = element.hasAttribute('host') and element.attributes['host'].value
        port = element.hasAttribute('port') and element.attributes['port'].value
        if not host or host == '*' or host == '127.0.0.1':
            if context.application.getApplicationIp():
                ip = context.application.getApplicationIp()
        elif netutils.isValidIp(host):
            ip = host
        else:
            ip = netutils.resolveIP(shell,host)
        if ip:
            if port:
                endpoint = netutils.Endpoint(port, netutils.ProtocolType.TCP_PROTOCOL, ip)
                endpointOSH = modeling.createIpServerOSH(endpoint)
                hostosh = modeling.createHostOSH(ip)
                endpointOSH.setContainer(hostosh)
                linkOsh = modeling.createLinkOSH("usage", context.application.getOsh(), endpointOSH)
                endpointOSHV.add(endpointOSH)
                endpointOSHV.add(linkOsh)
                logger.debug('Get ip using configfile serverindex.xml:',ip)
                logger.debug('Get port using configfile serverindex.xml:', port)
    if endpointOSHV:
        context.resultsVector.addAll(endpointOSHV)