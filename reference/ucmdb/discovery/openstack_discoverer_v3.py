# coding=utf-8
import logger
import openstack
import openstack_discoverer

VmDiscoverer = openstack_discoverer.VmDiscoverer

VolumeDiscoverer = openstack_discoverer.VolumeDiscoverer

ImageDiscoverer = openstack_discoverer.ImageDiscoverer

ZoneDiscoverer = openstack_discoverer.ZoneDiscoverer

HypervisorDiscoverer = openstack_discoverer.HypervisorDiscoverer

InterfaceDiscoverer = openstack_discoverer.InterfaceDiscoverer

SubnetDiscoverer = openstack_discoverer.SubnetDiscoverer

NetworkDiscoverer = openstack_discoverer.NetworkDiscoverer

FlavorDiscoverer = openstack_discoverer.FlavorDiscoverer


class TenantDiscoverer(openstack_discoverer.TenantDiscoverer):
    def discover(self):
        tenants = []
        try:
            json_rsp = self.api.getJsonResponse('/projects')
            for tenant_json in json_rsp["projects"]:
                tenants.append(openstack.Tenant(tenant_json["name"], tenant_json["id"]))
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error("Failed to discover tenant. Exception: <%s>" % excInfo)
        finally:
            return tenants


class RegionDiscoverer(openstack_discoverer.BaseDiscoverer):
    def __init__(self, keystoneApi):
        openstack_discoverer.BaseDiscoverer.__init__(self, keystoneApi)

    def discover(self):
        regions = []
        try:
            json_rsp = self.api.getJsonResponse('/regions')
            for region_json in json_rsp["regions"]:
                regions.append(region_json['id'])
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error("Failed to discover region. Exception: <%s>" % excInfo)
        finally:
            return regions


class EndpointDiscoverer(openstack_discoverer.BaseDiscoverer):
    def __init__(self, api):
        openstack_discoverer.BaseDiscoverer.__init__(self, api)

    def discover(self):
        endpoints_service_id = {}
        try:
            json_rsp = self.api.getJsonResponse('/endpoints')
            for endpoint_json in json_rsp["endpoints"]:
                if endpoint_json['interface'] == 'public':
                    if not endpoints_service_id.get(endpoint_json['service_id'], None):
                        endpoints_service_id[endpoint_json['service_id']] = []
                    endpoints_service_id.get(endpoint_json['service_id']).append(
                        openstack.Endpoint(endpoint_json["id"], endpoint_json["url"], endpoint_json["region_id"]))
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error("Failed to discover endpoint. Exception: <%s>" % excInfo)
        finally:
            return endpoints_service_id


class ServiceCatalogDiscoverer(openstack_discoverer.BaseDiscoverer):
    def __init__(self, api):
        openstack_discoverer.BaseDiscoverer.__init__(self, api)

    def discover(self):
        endpoint_discover = EndpointDiscoverer(self.api)
        endpoints_service_id = endpoint_discover.discover()

        services = {}
        try:
            json_rsp = self.api.getJsonResponse('/services')
            for service_json in json_rsp['services']:
                endpoints_region_id = {}
                endpoints_all = endpoints_service_id.get(service_json['id'], None)
                if endpoints_all:
                    for endpoint in endpoints_all:
                        endpoints_region_id[endpoint.region] = endpoint
                services[service_json['type']] = openstack.ServiceCatalog(service_json['name'], service_json['type'],
                                                                          endpoints_region_id)
        finally:
            return services
