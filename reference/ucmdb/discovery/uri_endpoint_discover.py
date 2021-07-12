# coding=utf-8
import logger
import modeling
import os
import ip_addr
import dns_resolver
import sys
import re

from xml.dom import minidom
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector
from com.hp.ucmdb.discovery.library.common import CollectorsParameters
from ip_addr import IPAddress


URL_FILE_NAME = 'UriEndpointConfiguration.xml'


def reportCIs(url, type, probe=None, ips=None, ip_address=None):
    vector = ObjectStateHolderVector()
    uriEndpointOsh = ObjectStateHolder('uri_endpoint')
    uriEndpointOsh.setAttribute('uri', url)
    uriEndpointOsh.setAttribute('type', type)
    if probe:
        uriEndpointOsh.setAttribute("uri_probename", probe)
    vector.add(uriEndpointOsh)
    if ips:
        for ip in ips:
            vector.addAll(reportIpCI(ip, uriEndpointOsh))
    if ip_address:
        vector.addAll(reportIpCI(ip_address, uriEndpointOsh))

    return vector

def reportIpCI(ip, uriEndpointOsh):
    vector = ObjectStateHolderVector()
    ipOsh = modeling.createIpOSH(ip)
    vector.add(ipOsh)
    vector.add(modeling.createLinkOSH('dependency', uriEndpointOsh, ipOsh))
    return vector


##############################################
##############################################
## Main
##############################################
##############################################
def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    configFileFolder = os.path.join(CollectorsParameters.BASE_PROBE_MGR_DIR,
                                    CollectorsParameters.getDiscoveryConfigFolder())
    uriEndpointConf = os.path.join(configFileFolder, URL_FILE_NAME)

    logger.debug("uriEndpointConf:", uriEndpointConf)

    if not os.path.exists(uriEndpointConf):
        logger.reportWarning('UriEndpoint configuration file not found:%s' % uriEndpointConf)
        return None
    listFile = open(uriEndpointConf)
    try:
        url_dom = minidom.parseString(listFile.read())
        nodeList = url_dom.getElementsByTagName('dataflowprobe')
        length = nodeList.length
        while length > 0:
            node = nodeList.item(nodeList.length - length)
            probe_name = node.getAttribute('name')
            logger.debug("probe_name:", probe_name)
            OSHVResult.addAll(uri_endpoint_discover(node, probe=probe_name))
            length = length - 1
        OSHVResult.addAll(uri_endpoint_discover(url_dom))

    except:
        msg = "Failed to read file:" + str(URL_FILE_NAME)
        logger.reportWarning(msg)
        logger.error(msg, str(sys.exc_info()[1]))

    finally:
        listFile.close()
    return OSHVResult


def uri_endpoint_discover(url_dom, probe=None):
    vector = ObjectStateHolderVector()
    nodeList = url_dom.getElementsByTagName('uri-endpoint')
    length = nodeList.length
    ips = []
    while length > 0:
        node = nodeList.item(nodeList.length - length)
        if node.getElementsByTagName("url"):
            url = node.getElementsByTagName("url")[0].childNodes[0].nodeValue
            logger.debug("url:", url)
            ips = resolveIpAddress(url)
        if node.getElementsByTagName("type"):
            type = node.getElementsByTagName("type")[0].childNodes[0].nodeValue
            logger.debug("type:", type)
        if node.getElementsByTagName("ip-address"):
            ip_address = node.getElementsByTagName("ip-address")[0].childNodes[0].nodeValue
            ips.append(IPAddress(ip_address))
        if not url:
            raise Exception("Wrong URL configuration.")
        if not type:
            raise Exception("Wrong TYPE configuration.")
        if not ips:
            logger.error("Failed to resolve ipaddress for uri_endpoint:" + url)
        if url and type:
            vector.addAll(reportCIs(url.strip(), type.strip(), probe, ips))
        length = length - 1
    return vector

def resolveIpAddress(url):
    ips = []
    try:
        match = re.findall("(https?)://([\w.]+):(\d+)/", url)
        if not match:
            match = re.findall("(https?)://([\w.]+)", url)
        for parameters in match:
            if len(parameters) >= 2:
                protocol = parameters[0]
                name = parameters[1]
                logger.debug("resolveIpAddress-hostname:", name)
                if not ip_addr.isValidIpAddress(name):
                    resolver = dns_resolver.SocketDnsResolver()
                    ips = resolver.resolve_ips(name)
                else:
                    ips.append(IPAddress(name))
            else:
                msg = "wrong format of url:" + str(url)
                logger.reportWarning(msg)
                logger.error("wrong format of url:", url)
    except:
        msg = "Failed to resolve host name: " + str(url)
        logger.reportWarning(msg)
        logger.error(msg, str(sys.exc_info()[1]))
    return ips


