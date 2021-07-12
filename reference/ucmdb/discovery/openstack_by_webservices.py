# coding=utf-8
import sys
import logger
import modeling
import openstack_discoverer
import openstack_discoverer_v3
import openstack
import re
from openstack_client import OpenStackClientV2
from openstack_client import OpenStackClientV3

from appilog.common.system.types.vectors import ObjectStateHolderVector
from com.hp.ucmdb.discovery.common import CollectorsConstants



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

    zoneOshDict = {}
    serverOshDict = {}
    networkOshDict = {}

    for protocol in protocols:
        try:
            logger.debug("connect with protocol:", protocol)
            username = Framework.getProtocolProperty(protocol, CollectorsConstants.PROTOCOL_ATTRIBUTE_USERNAME)
            credential = Framework.getProtocolProperty(protocol, CollectorsConstants.PROTOCOL_ATTRIBUTE_PASSWORD)
            http_proxy = Framework.getProtocolProperty(protocol, "proxy", "")

            if http_proxy:
                proxies['http'] = http_proxy
                proxies['https'] = http_proxy

            client, module = createClient(endpoint, proxies)
            client.login(username, credential)

            tenant_discover = module.TenantDiscoverer(client)
            tenants = tenant_discover.discover()
            if tenants:
                openstack_software = openstack.OpenStack(ip)
                openstack_osh, openstack_vector = openstack_software.report()
                OSHVResult.addAll(openstack_vector)
            else:
                continue

            for tenant in tenants:
                try:
                    logger.debug("connecting to tenant:", tenant.name)
                    tenant_osh = tenant.report()
                    OSHVResult.add(tenant_osh)
                    OSHVResult.add(modeling.createLinkOSH("composition", openstack_osh, tenant_osh))

                    client.login(username, credential, tenant)

                    if not client.services:
                        service_discover = module.ServiceCatalogDiscoverer(client)
                        services = service_discover.discover()

                        client.services = services

                    novaApi = client.buildApi("compute", tenant)
                    cinderApi = client.buildApi("volume", tenant)
                    glanceApi = client.buildApi("image", tenant)
                    neutronApi = client.buildApi("network", tenant)

                    region_discover = module.RegionDiscoverer(client)
                    regions = region_discover.discover()
                    if regions:
                        tenant_osh.setStringAttribute('credentials_id', protocol)

                    for tmp_region in regions:
                        logger.debug("region:", tmp_region)
                        region = openstack.Region(tmp_region)
                        region_osh = region.report(tenant_osh)
                        OSHVResult.add(region_osh)

                        OSHVResult.addAll(getZones(module, novaApi, region.name, region_osh, zoneOshDict))
                        logger.debug("zoneOshDict:", zoneOshDict)

                        OSHVResult.addAll(getImages(module, glanceApi, region.name, region_osh))
                        OSHVResult.addAll(getHypervisors(module, novaApi, region.name, region_osh))
                        OSHVResult.addAll(getVms(module, novaApi, region.name, region_osh, serverOshDict))

                        OSHVResult.addAll(getVolumes(module, cinderApi, region.name, region_osh, zoneOshDict, serverOshDict))
                        logger.debug("serverOshDict:", serverOshDict)

                        OSHVResult.addAll(getNetworks(module, neutronApi, region.name, region_osh, networkOshDict, openstack_osh))
                        logger.debug("networkOshDict:", networkOshDict)

                        OSHVResult.addAll(getPorts(module, neutronApi, region.name, serverOshDict, networkOshDict))

                        OSHVResult.addAll(getSubnets(module, neutronApi, region.name, networkOshDict, openstack_osh))

                        OSHVResult.addAll(getFlavors(module, novaApi, region.name, region_osh))
                except:
                    strException = str(sys.exc_info()[1])
                    excInfo = logger.prepareJythonStackTrace('')
                    logger.debug(strException)
                    logger.debug(excInfo)
                    pass
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

def createClient(endpoint, proxies):
    match = re.findall("(https?)://([\w.]+):(\d+)/v(\d+)", endpoint)
    for groups in match:
        if len(groups) >= 4:
            version = groups[3]
            if version == "3":
                return OpenStackClientV3(endpoint, proxies), openstack_discoverer_v3
            else:
                return OpenStackClientV2(endpoint, proxies), openstack_discoverer

def getZones(module, novaApi, regionName, region_osh, zoneOshDict):
    vector = ObjectStateHolderVector()
    zone_discover = module.ZoneDiscoverer(novaApi, regionName)
    zones = zone_discover.discover()
    for zone in zones:
        zone_osh, zone_vector = zone.report(region_osh)
        zoneOshDict[zone.name] = zone_osh
        vector.addAll(zone_vector)
    return vector


def getVolumes(module, cinderApi, regionName, region_osh, zoneOshDict, serverOshDict):
    vector = ObjectStateHolderVector()
    volume_discoverer = module.VolumeDiscoverer(cinderApi, regionName)
    volumes = volume_discoverer.discover()
    for volume in volumes:
        vector.addAll(volume.report(region_osh, zoneOshDict, serverOshDict))
    return vector


def getImages(module, glanceApi, regionName, region_osh):
    vector = ObjectStateHolderVector()
    image_discover = module.ImageDiscoverer(glanceApi, regionName)
    images = image_discover.discover()
    for image in images:
        image_osh, image_vector = image.report(region_osh)
        vector.addAll(image_vector)
    return vector


def getHypervisors(module, novaApi, regionName, region_osh):
    vector = ObjectStateHolderVector()
    hypervisor_discover = module.HypervisorDiscoverer(novaApi, regionName)
    hypervisors = hypervisor_discover.discover()
    for hypervisor in hypervisors:
        hypervisor_osh, hypervisor_vector = hypervisor.report(region_osh)
        vector.addAll(hypervisor_vector)
    return vector


def getVms(module, novaApi, regionName, region_osh, serverOshDict):
    vector = ObjectStateHolderVector()
    vm_discoverer = module.VmDiscoverer(novaApi, regionName)
    vms = vm_discoverer.discover()
    for vm in vms:
        vm_osh, vm_vector = vm.report(region_osh)
        serverOshDict[vm.id] = vm_osh
        vector.addAll(vm_vector)
    return vector


def getNetworks(module, neutronApi, regionName, region_osh, networkOshDict, openstack_osh):
    vector = ObjectStateHolderVector()
    network_discover = module.NetworkDiscoverer(neutronApi, regionName)
    networks = network_discover.discover()
    for network in networks:
        network_osh, network_vector = network.report(region_osh, openstack_osh)
        networkOshDict[network.id] = network_osh
        vector.addAll(network_vector)
    return vector


def getPorts(module, neutronApi, regionName, serverOshDict, networkOshDict):
    vector = ObjectStateHolderVector()
    port_discover = module.InterfaceDiscoverer(neutronApi, regionName)
    interfaces = port_discover.discover()
    for interface in interfaces:
        vector.addAll(interface.report(serverOshDict, networkOshDict))
    return vector


def getSubnets(module, neutronApi, regionName, networkOshDict, openstack_osh):
    vector = ObjectStateHolderVector()
    subnet_discover = module.SubnetDiscoverer(neutronApi, regionName)
    subnets = subnet_discover.discover()
    for subnet in subnets:
        vector.addAll(subnet.report(networkOshDict, openstack_osh))
    return vector


def getFlavors(module, novaApi, regionName, region_osh):
    vector = ObjectStateHolderVector()
    flavor_discover = module.FlavorDiscoverer(novaApi, regionName)
    flavors = flavor_discover.discover()
    for flavor in flavors:
        flavor_osh, flavor_vector = flavor.report(region_osh)
        vector.addAll(flavor_vector)
    return vector
