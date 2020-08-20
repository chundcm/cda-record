# coding=utf-8
import logger
import ip_addr
import openstack


class BaseDiscoverer:
    def __init__(self, api):
        if not api:
            raise ValueError('No api passed.')
        self.api = api

    def discover(self):
        raise NotImplemented, "discover"

    def __repr__(self):
        return "BaseDiscoverer (api: %s)" % self.api.version


class VmDiscoverer(BaseDiscoverer):
    def __init__(self, novaApi, regionName):
        BaseDiscoverer.__init__(self, novaApi)
        self.regionName = regionName

    def discover(self):
        vms = []
        try:
            json_rsp = self.api.getJsonResponseByRegion(self.regionName, '/servers/detail')
            for vm_json in json_rsp['servers']:
                vm = self.buildVm(vm_json)
                vm and vms.append(vm)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error("Failed to discover server. Exception: <%s>" % excInfo)
        finally:
            return vms

    def buildVm(self, server_json):
        vm = openstack.Vm()
        vm.referenced_project = server_json['tenant_id']
        vm.name = server_json['name']
        vm.status = server_json['status']
        vm.host_id = server_json['hostId']
        vm.id = server_json['id']
        image_json = server_json['image']
        if image_json:
            logger.debug("server_image:", image_json)
            vm.image = openstack.Image(image_json['id'])
        flavor_json = server_json['flavor']
        if flavor_json:
            logger.debug("server_flavor:", flavor_json)
            vm.flavor = openstack.Flavor(flavor_json['id'])
        vm.hypervisorHostName = server_json['OS-EXT-SRV-ATTR:hypervisor_hostname']

        if server_json['addresses'].get('public', None):
            addrs_json = server_json['addresses']['public']
            for addr_json in addrs_json:
                ip = addr_json['addr']
                if ip and ip_addr.isValidIpAddressNotZero(ip):
                    vm.ips.append(ip_addr.IPAddress(ip))
        return vm


class VolumeDiscoverer(BaseDiscoverer):
    def __init__(self, cinderApi, regionName):
        BaseDiscoverer.__init__(self, cinderApi)
        self.regionName = regionName

    def discover(self):
        volumes = []
        try:
            json_rsp = self.api.getJsonResponseByRegion(self.regionName, '/volumes')
            for volume_json in json_rsp['volumes']:
                vol = openstack.Volume()
                vol.id = volume_json['id']
                vol.name = volume_json['display_name']
                vol.project_id = self.api.tenant
                vol.zone = volume_json['availability_zone']
                vol.type = volume_json['volume_type']
                vol.status = volume_json['status']
                attachments_json = volume_json['attachments']
                if attachments_json:
                    for volume_attachment_json in attachments_json:
                        logger.debug("volume_attachment:", volume_attachment_json)
                        server_id = volume_attachment_json['server_id']
                        vol.attachments.append(server_id)
                volumes.append(vol)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error("Failed to discover volume. Exception: <%s>" % excInfo)
        finally:
            return volumes


class ImageDiscoverer(BaseDiscoverer):
    def __init__(self, glanceApi, regionName):
        BaseDiscoverer.__init__(self, glanceApi)
        self.regionName = regionName

    def discover(self):
        images = []
        try:
            json_rsp = self.api.getJsonResponseByRegion(self.regionName, '/v2/images')
            for image_json in json_rsp['images']:
                img = openstack.Image(image_json['id'])
                img.name = image_json['name']
                img.size = long(image_json['size']) / 1024.0 / 1024.0
                img.disk_format = image_json['disk_format']
                images.append(img)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error("Failed to discover image. Exception: <%s>" % excInfo)
        finally:
            return images


class ZoneDiscoverer(BaseDiscoverer):
    def __init__(self, novaApi, regionName):
        BaseDiscoverer.__init__(self, novaApi)
        self.regionName = regionName

    def discover(self):
        zones = []
        try:
            json_rsp = self.api.getJsonResponseByRegion(self.regionName, '/os-availability-zone')
            for zone_json in json_rsp['availabilityZoneInfo']:
                zones.append(openstack.Zone(zone_json['zoneName']))
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error("Failed to discover zone. Exception: <%s>" % excInfo)
        finally:
            return zones


class HypervisorDiscoverer(BaseDiscoverer):
    def __init__(self, novaApi, regionName):
        BaseDiscoverer.__init__(self, novaApi)
        self.regionName = regionName

    def discover(self):
        hypervisors = []
        try:
            json_rsp = self.api.getJsonResponseByRegion(self.regionName, '/os-hypervisors/detail')
            for hypervisor_json in json_rsp['hypervisors']:
                hypervisor = openstack.Hypervisor(hypervisor_json['hypervisor_hostname'])
                hypervisor.type = hypervisor_json['hypervisor_type']
                hypervisors.append(hypervisor)

        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error("Failed to discover hypervisor. Exception: <%s>" % excInfo)
        finally:
            return hypervisors


class InterfaceDiscoverer(BaseDiscoverer):
    def __init__(self, neutronApi, regionName):
        BaseDiscoverer.__init__(self, neutronApi)
        self.regionName = regionName

    def discover(self):
        interfaces = []
        try:
            json_rsp = self.api.getJsonResponseByRegion(self.regionName, '/v2.0/ports')
            for port_json in json_rsp["ports"]:
                interface = openstack.Interface()
                interface.id = port_json['id']
                interface.name = port_json['name']
                interface.network_id = port_json['network_id']
                interface.mac = port_json['mac_address']
                interface.vm_id = port_json['device_id']
                interface.tenant_id = port_json['tenant_id']
                interfaces.append(interface)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error("Failed to discover interface. Exception: <%s>" % excInfo)
        finally:
            return interfaces


class SubnetDiscoverer(BaseDiscoverer):
    def __init__(self, neutronApi, regionName):
        BaseDiscoverer.__init__(self, neutronApi)
        self.regionName = regionName

    def discover(self):
        subnets = []
        try:
            json_rsp = self.api.getJsonResponseByRegion(self.regionName, '/v2.0/subnets')
            for subnet_json in json_rsp["subnets"]:
                subnet = openstack.Subnet()
                subnet.id = subnet_json['id']
                subnet.name = subnet_json['name']
                subnet.tenant_id = subnet_json['tenant_id']
                subnet.network_id = subnet_json['network_id']
                subnet.gatewayip = subnet_json['gateway_ip']
                subnet.cidr = subnet_json['cidr']
                subnets.append(subnet)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error("Failed to discover subnet. Exception: <%s>" % excInfo)
        finally:
            return subnets


class TenantDiscoverer(BaseDiscoverer):
    def __init__(self, keystoneApi):
        BaseDiscoverer.__init__(self, keystoneApi)

    def discover(self):
        tenants = []
        try:
            json_rsp = self.api.getJsonResponse('/tenants')
            for tenant_json in json_rsp["tenants"]:
                tenants.append(openstack.Tenant(tenant_json["name"], tenant_json["id"]))
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error("Failed to discover tenant. Exception: <%s>" % excInfo)
        finally:
            return tenants


class NetworkDiscoverer(BaseDiscoverer):
    def __init__(self, networkApi, regionName):
        BaseDiscoverer.__init__(self, networkApi)
        self.regionName = regionName

    def discover(self):
        networks = []
        try:
            json_rsp = self.api.getJsonResponseByRegion(self.regionName, '/v2.0/networks')
            for network_json in json_rsp["networks"]:
                network = openstack.Network()
                network.id = network_json['id']
                network.name = network_json['name']
                network.tenant_id = network_json['tenant_id']
                network.physicalNetworkName = network_json['provider:physical_network']
                network.external = network_json['router:external']
                networks.append(network)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error("Failed to discover network. Exception: <%s>" % excInfo)
        finally:
            return networks


class FlavorDiscoverer(BaseDiscoverer):
    def __init__(self, novaApi, regionName):
        BaseDiscoverer.__init__(self, novaApi)
        self.regionName = regionName

    def discover(self):
        flavors = []
        try:
            json_rsp = self.api.getJsonResponseByRegion(self.regionName, '/flavors/detail')
            for flavor_json in json_rsp["flavors"]:
                falvor = openstack.Flavor(flavor_json['id'])
                falvor.name = flavor_json['name']
                falvor.vcpus = flavor_json['vcpus']
                falvor.ram = flavor_json['ram']
                falvor.root_disk = flavor_json['disk']
                falvor.ephemeral_disk = flavor_json['OS-FLV-EXT-DATA:ephemeral']
                falvor.swap_disk = flavor_json['swap']
                flavors.append(falvor)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error("Failed to discover flavor. Exception: <%s>" % excInfo)
        finally:
            return flavors

class RegionDiscoverer(BaseDiscoverer):
    def __init__(self, novaApi):
        BaseDiscoverer.__init__(self, novaApi)

    def discover(self):
        service = self.api.services.get("compute", None)
        if service:
            return service.endpoints.keys()

