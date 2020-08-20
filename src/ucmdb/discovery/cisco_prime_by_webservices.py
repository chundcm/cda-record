# coding=utf-8
import sys
import logger

import modeling
import rest_requests as requests
import base64

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector
from com.hp.ucmdb.discovery.common import CollectorsConstants
from java.lang import Boolean, Integer


def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()

    endpoint = Framework.getDestinationAttribute('endpoint')
    ip = Framework.getDestinationAttribute('ip')
    protocols = Framework.getAvailableProtocols(ip, "http")
    proxies = {}

    if len(protocols) == 0:
        msg = 'Protocol not defined or IP out of protocol network range'
        logger.reportWarning(msg)
        logger.error(msg)
        return OSHVResult

    for protocol in protocols:
        try:
            logger.debug("connect with protocol:", protocol)
            username = Framework.getProtocolProperty(protocol, CollectorsConstants.PROTOCOL_ATTRIBUTE_USERNAME)
            credential = Framework.getProtocolProperty(protocol, CollectorsConstants.PROTOCOL_ATTRIBUTE_PASSWORD)
            http_proxy = Framework.getProtocolProperty(protocol, "proxy", "")

            if http_proxy:
                proxies['http'] = http_proxy
                proxies['https'] = http_proxy

            encoded = base64.encodestring(username + ":" + credential)
            basic = 'Basic %s' % encoded[:-1]

            headers = {
                'Authorization': basic
            }
            discoverAccesspoint = Boolean.parseBoolean(Framework.getParameter('discoverAccesspoint'))
            if discoverAccesspoint:
                OSHVResult.addAll(getInventory(endpoint, headers, proxies))
            discoverDevice = Boolean.parseBoolean(Framework.getParameter('discoverDevice'))
            if discoverDevice:
                OSHVResult.addAll(getAccessPoints(endpoint, headers, proxies))

        except:
            strException = str(sys.exc_info()[1])
            excInfo = logger.prepareJythonStackTrace('')
            logger.debug(strException)
            logger.debug(excInfo)
            pass

    reportError = OSHVResult.size() == 0
    if reportError:
        msg = 'Failed to connect using all protocols'
        logger.reportError(msg)
        logger.error(msg)
    return OSHVResult


def getAccessPoints(endpoint, headers, proxies):
    vector = ObjectStateHolderVector()
    start = 0
    getResult = True
    while getResult:
        try:
            rsp = requests.get(
                endpoint + "/webacs/api/v1/data/AccessPointDetails.json?.full=true&.maxResults=100&.firstResult=%s" %start,
                headers=headers, proxies=proxies, debug=False, verify=False)

            json_rsp = rsp.json()
            logger.debug('Visit AccessPoints start from result %s' %start)
            logger.debug(json_rsp)
            start += 100

            if json_rsp['queryResponse']['entity']:
                for device_json in json_rsp['queryResponse']['entity']:
                    ipAddress = device_json['accessPointDetailsDTO']['ipAddress']
                    name = device_json['accessPointDetailsDTO']['name']
                    device_id = device_json['accessPointDetailsDTO']['@id']
                    serialNumber = device_json['accessPointDetailsDTO']['serialNumber']
                    model = device_json['accessPointDetailsDTO']['model']
                    macAddress = device_json['accessPointDetailsDTO']['macAddress']
                    mapLocation = ''
                    if device_json['accessPointDetailsDTO'].has_key('mapLocation'):
                        mapLocation = device_json['accessPointDetailsDTO']['mapLocation']
                    hwtype = device_json['accessPointDetailsDTO']['apType']

                    ip_osh = modeling.createIpOSH(ipAddress)
                    mapping_device_type = 'netdevice'
                    device_type = device_json['accessPointDetailsDTO']['type']
                    # customer private patch, mapping to accesspoint CIT
                    # if device_type and device_type == 'UnifiedAp':
                    #     mapping_device_type = 'accesspoint'
                    device_osh = ObjectStateHolder(mapping_device_type)
                    device_osh.setAttribute('data_externalid', device_id)
                    device_osh.setAttribute('discovered_os_vendor', "Cisco")
                    device_osh.setAttribute('discovered_vendor', "Cisco")
                    device_osh.setAttribute('discovered_os_name', "IOS")

                    try:
                        softwareVersion = device_json['accessPointDetailsDTO']['softwareVersion']
                        softwareVersion and device_osh.setAttribute('discovered_os_version',softwareVersion) \
                        and device_osh.setAttribute('host_osrelease', softwareVersion) \
                        and device_osh.setAttribute('discovered_description',model + " " + softwareVersion)
                    except:
                        logger.debug('There is no softwareVersion on %s' % name)

                    if isinstance(mapLocation, int) or isinstance(mapLocation, long):
                        mapLocation = str(mapLocation)
                    elif isinstance(mapLocation, basestring):
                        mapLocation = mapLocation.encode('utf-8')
                    device_osh.setAttribute('discovered_location', mapLocation)
                    device_osh.setAttribute('discovered_model', model)

                    device_osh.setAttribute('extended_node_family', hwtype)
                    device_osh.setAttribute('name', name)
                    device_osh.setAttribute('os_vendor', "cisco_system_inc")
                    device_osh.setAttribute('primary_dns_name', name)
                    device_osh.setAttribute('primary_ip_address', ipAddress)
                    device_osh.setAttribute('primary_mac_address', macAddress)
                    device_osh.setAttribute('serial_number', serialNumber)

                    vector.add(ip_osh)
                    vector.add(device_osh)
                    vector.add(modeling.createLinkOSH('containment', device_osh, ip_osh))
            else:
                getResult = False
                logger.warn('no more AccessPoints')
        except Exception, ex:
            getResult = False
            logger.warn('Failed to get data, return AccessPoints: %s'% ex)
    return vector


def getInventory(endpoint, headers, proxies):
    vector = ObjectStateHolderVector()

    start = 0
    getResult = True
    while getResult:
        try:
            rsp = requests.get(endpoint + "/webacs/api/v1/data/InventoryDetails.json?.full=true&.maxResults=100&.firstResult=%s" %start,
                headers=headers, proxies=proxies, debug=False, verify=False)


            json_rsp = rsp.json()
            logger.debug('Visit InventoryDetails start from result %s' %start)
            logger.debug(json_rsp)
            start += 100

            if json_rsp['queryResponse']['entity']:
                for device_json in json_rsp['queryResponse']['entity']:
                    ipaddress = device_json['inventoryDetailsDTO']['summary']['ipAddress']
                    device_id = device_json['inventoryDetailsDTO']['summary']['deviceId']
                    mapLocation = ''
                    if device_json['inventoryDetailsDTO']['summary'].has_key('location'):
                        mapLocation = device_json['inventoryDetailsDTO']['summary']['location']
                    if device_json['inventoryDetailsDTO']['summary'].get('deviceName', 0):
                        device_name = device_json['inventoryDetailsDTO']['summary']['deviceName']
                    else:
                        logger.debug("No device name: deviceId %s ipaddress=%s skipped" % (device_id, ipaddress))
                        continue

                    devtype = device_json['inventoryDetailsDTO']['summary']['deviceType']

                    ip_osh = modeling.createIpOSH(ipaddress)
                    mapping_device_type = 'netdevice'
                    if devtype:
                        if 'switch' in devtype.lower():
                            mapping_device_type = 'switch'
                        elif 'router' in devtype.lower():
                            mapping_device_type = 'router'
                        elif 'wireless lan controller' in devtype.lower():
                            mapping_device_type = 'ras'
                    device_osh = ObjectStateHolder(mapping_device_type)
                    device_osh.setAttribute('name', device_name)
                    device_osh.setAttribute('data_externalid', str(device_id))

                    device_osh.setAttribute('discovered_model', devtype)
                    device_osh.setAttribute('discovered_os_name', "IOS")
                    try:
                        softwareVersion = device_json['inventoryDetailsDTO']['summary']['softwareVersion']
                        softwareVersion and device_osh.setAttribute('discovered_os_version',softwareVersion) \
                        and device_osh.setAttribute('discovered_description', devtype + " " + softwareVersion)
                    except:
                        logger.debug('There is no softwareVersion on %s' % device_name)
                    device_osh.setAttribute('discovered_os_vendor', "Cisco")
                    device_osh.setAttribute('discovered_vendor', "Cisco")
                    device_osh.setAttribute('primary_dns_name', device_name)
                    device_osh.setAttribute('primary_ip_address', ipaddress)
                    device_osh.setAttribute('os_vendor', "cisco_system_inc")
                    device_osh.setAttribute('discovered_location', mapLocation.encode("utf-8"))

                    vector.add(ip_osh)
                    vector.add(device_osh)
                    vector.add(modeling.createLinkOSH('containment', device_osh, ip_osh))

                    if device_json['inventoryDetailsDTO'].get('ethernetInterfaces', None):
                        for interface_json in device_json['inventoryDetailsDTO']['ethernetInterfaces']['ethernetInterface']:
                            interface_name = interface_json['name']
                            if interface_json.get('macAddress', None):
                                interface_mac = interface_json['macAddress']
                            else:
                                logger.debug(
                                    "No macAddress: interface %s ipaddress=%s skipped" % (interface_name, ipaddress))
                                continue
                            interface_speed = interface_json.get('speed')
                            if interface_speed:
                                interface_speed = interface_json['speed']['longAmount']
                            interface_osh = modeling.createInterfaceOSH(interface_mac, device_osh,
                                                                        speed=interface_speed, name=interface_name)
                            vector.add(interface_osh)

                    if device_json['inventoryDetailsDTO'].get('cdpNeighbors', None):
                        neighbor_json_list = device_json['inventoryDetailsDTO']['cdpNeighbors']['cdpNeighbor']
                        if neighbor_json_list:
                            if (type(neighbor_json_list) == list):
                                for neighbor_json in neighbor_json_list:
                                    if (neighbor_json.get('farEndInterface', None) and neighbor_json.get('nearEndInterface', None) and neighbor_json.get('neighborDeviceName', None)):
                                        farEndInterface = neighbor_json['farEndInterface']
                                        nearEndInterface = neighbor_json['nearEndInterface']
                                        neighborDeviceName = neighbor_json['neighborDeviceName']
                                        logger.debug("neighbor_json: %s, %s, %s" % (farEndInterface, nearEndInterface, neighborDeviceName))
                                        vector.addAll(buildLayer2Connection(device_osh, str(device_id), nearEndInterface, neighborDeviceName, farEndInterface))
                            else:
                                if (neighbor_json_list.get('farEndInterface', None) and neighbor_json_list.get('nearEndInterface', None) and neighbor_json_list.get('neighborDeviceName', None)):
                                    farEndInterface = neighbor_json_list['farEndInterface']
                                    nearEndInterface = neighbor_json_list['nearEndInterface']
                                    neighborDeviceName = neighbor_json_list['neighborDeviceName']
                                    logger.debug("neighbor_json: %s, %s, %s" % (farEndInterface, nearEndInterface, neighborDeviceName))
                                    vector.addAll(buildLayer2Connection(device_osh, str(device_id), nearEndInterface, neighborDeviceName, farEndInterface))
            else:
                getResult = False
                logger.warn('no more InventoryDetails')
        except Exception, ex:
            getResult = False
            logger.warnException("Failed to get data, return InventoryDetails: %s" % ex)

    return vector

def buildLayer2Connection(device_osh, device_id, nearEndInterface, neighborDeviceName, farEndInterface):
    vector = ObjectStateHolderVector()

    neighbor_device_osh = ObjectStateHolder("node")
    neighbor_device_osh.setAttribute("name", neighborDeviceName)
    neighbor_interface_osh = ObjectStateHolder("interface")
    neighbor_interface_osh.setAttribute('interface_name', farEndInterface)
    neighbor_interface_osh.setContainer(neighbor_device_osh)
    vector.add(neighbor_device_osh)
    vector.add(neighbor_interface_osh)

    local_interface_osh = ObjectStateHolder("interface")
    local_interface_osh.setAttribute('interface_name', nearEndInterface)
    local_interface_osh.setContainer(device_osh)
    vector.add(local_interface_osh)

    layer2_osh = ObjectStateHolder('layer2_connection')
    layer2_osh.setAttribute('layer2_connection_id', str(hash(device_id + neighborDeviceName)))
    local_member_osh = modeling.createLinkOSH('membership', layer2_osh, local_interface_osh)
    neighbor_member_osh = modeling.createLinkOSH('membership', layer2_osh,
                                                 neighbor_interface_osh)
    vector.add(layer2_osh)
    vector.add(local_member_osh)
    vector.add(neighbor_member_osh)
    return vector

