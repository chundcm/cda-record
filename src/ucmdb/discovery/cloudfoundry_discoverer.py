import logger
import cloudfoundry

class BaseDiscoverer:
    def __init__(self, client):
        if not client:
            raise ValueError('No client passed.')
        self.client = client
        self.api_version = self.client.url_api_version

    def discover(self):
        raise NotImplemented, "discover"

    def __repr__(self):
        return "BaseDiscoverer (client: %s)" % self.client


class CloudFoundryDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        info_json = self.client.getJsonResponse('/%s/info' % self.api_version)
        cf = cloudfoundry.CloudFoundry(info_json['name'] or self.client.api_endpoint)
        return cf


class OrganizationsDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        orgs = []
        for orgs_json in self.client.getResources('/%s/organizations' % self.api_version):
            for org_json in orgs_json:
                logger.debug('Organization Discover: %s' % org_json)
                org = cloudfoundry.Organization(org_json['metadata']['guid'])
                org.name = org_json['entity']['name']
                org.status = org_json['entity']['status']
                org.quota_definition_guid = org_json['entity']['quota_definition_guid']
                org and orgs.append(org)
        return orgs

class OrgQuotaDefinitionsDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        org_quotas = []
        for org_quotas_json in self.client.getResources('/%s/quota_definitions' % self.api_version):
            for org_quota_json in org_quotas_json:
                logger.debug('Organization Quota Discover: %s' % org_quota_json)
                org_quota = cloudfoundry.QuotaDef(org_quota_json['metadata']['guid'])
                org_quota.name = org_quota_json['entity']['name']
                org_quota.scope = "organization"
                org_quota.non_basic_services_allowed = org_quota_json['entity']['non_basic_services_allowed']
                org_quota.total_services = org_quota_json['entity']['total_services']
                org_quota.total_routes = org_quota_json['entity']['total_routes']
                org_quota.total_private_domains = org_quota_json['entity']['total_private_domains']
                org_quota.memory_limit = org_quota_json['entity']['memory_limit']
                org_quota.instance_memory_limit = org_quota_json['entity']['instance_memory_limit']
                org_quota.app_instance_limit = org_quota_json['entity']['app_instance_limit']
                org_quota and org_quotas.append(org_quota)
        return org_quotas


class SpacesDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        spaces = []
        for spaces_json in self.client.getResources('/%s/spaces' % self.api_version):
            for space_json in spaces_json:
                logger.debug('Space Discover: %s' % space_json)
                space = cloudfoundry.Space(space_json['metadata']['guid'])
                space.name = space_json['entity']['name']
                space.organization_guid = space_json['entity']['organization_guid']
                space.space_quota_definition_guid = space_json['entity']['space_quota_definition_guid']
                space and spaces.append(space)
        return spaces

class SpaceQuotaDefinitionsDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        space_quotas = []
        for org_quotas_json in self.client.getResources('/%s/space_quota_definitions' % self.api_version):
            for space_quota_json in org_quotas_json:
                logger.debug('Space Quota Discover: %s' % space_quota_json)
                space_quota = cloudfoundry.SpaceQuotaDef(space_quota_json['metadata']['guid'])
                space_quota.name = space_quota_json['entity']['name']
                space_quota.non_basic_services_allowed = space_quota_json['entity']['non_basic_services_allowed']
                space_quota.total_services = space_quota_json['entity']['total_services']
                space_quota.total_routes = space_quota_json['entity']['total_routes']
                space_quota.memory_limit = space_quota_json['entity']['memory_limit']
                space_quota.instance_memory_limit = space_quota_json['entity']['instance_memory_limit']
                space_quota.app_instance_limit = space_quota_json['entity']['app_instance_limit']
                space_quota.organization_guid = space_quota_json['entity']['organization_guid']
                space_quota.scope = "space"
                space_quota and space_quotas.append(space_quota)
        return space_quotas

class PrivateDomainsDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        domains = []
        for domains_json in self.client.getResources('/%s/private_domains' % self.api_version):
            for domain_json in domains_json:
                logger.debug('Private Domains Discover: %s' % domain_json)
                domain = cloudfoundry.PrivateDomain(domain_json['metadata']['guid'])
                domain.name = domain_json['entity']['name']
                domain.status = "owned"
                domain.owning_organization_guid = domain_json['entity']['owning_organization_guid']
                domain and domains.append(domain)
        return domains

class SharedDomainsDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        domains = []
        for domains_json in self.client.getResources('/%s/shared_domains' % self.api_version):
            for domain_json in domains_json:
                logger.debug('Shared Domains Discover: %s' % domain_json)
                domain = cloudfoundry.Domain(domain_json['metadata']['guid'])
                domain.name = domain_json['entity']['name']
                domain.status = "shared"
                domain and domains.append(domain)
        return domains


class RoutesDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        routes = []
        for routes_json in self.client.getResources('/%s/routes' % self.api_version):
            for route_json in routes_json:
                logger.debug('Routes Discover: %s' % route_json)
                route = cloudfoundry.Route(route_json['metadata']['guid'])
                route.host = route_json['entity']['host']
                route.domain_guid = route_json['entity']['domain_guid']
                route.space_guid = route_json['entity']['space_guid']
                route.service_instance_guid = route_json['entity']['service_instance_guid']
                route and routes.append(route)
        return routes

class AppsDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        apps = []
        for apps_json in self.client.getResources('/%s/apps' % self.api_version):
            for app_json in apps_json:
                logger.debug('Apps Discover: %s' % app_json)
                app = cloudfoundry.Application(app_json['metadata']['guid'])
                app.name = app_json['entity']['name']
                app.instances = app_json['entity']['instances']
                app.disk_quota = app_json['entity']['disk_quota']
                app.space_guid = app_json['entity']['space_guid']
                app.buildpack = app_json['entity']['buildpack']
                app.space_guid = app_json['entity']['space_guid']
                app and apps.append(app)
        return apps

class ServicesDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        services = []
        for services_json in self.client.getResources('/%s/services' % self.api_version):
            for service_json in services_json:
                logger.debug('Services Discover: %s' % service_json)
                service = cloudfoundry.Service(service_json['metadata']['guid'])
                service.name = service_json['entity']['label']
                service.description = service_json['entity']['description']
                service and services.append(service)
        return services


class ServicePlansDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        service_plans = []
        for plans_json in self.client.getResources('/%s/service_plans' % self.api_version):
            for plan_json in plans_json:
                logger.debug('Service Plans Discover: %s' % plan_json)
                service_plan = cloudfoundry.ServicePlan(plan_json['metadata']['guid'])
                service_plan.name = plan_json['entity']['name']
                service_plan.free = plan_json['entity']['free']
                service_plan.description = plan_json['entity']['description']
                service_plan.public = plan_json['entity']['public']
                service_plan.active = plan_json['entity']['active']
                service_plan.service_guid = plan_json['entity']['service_guid']
                service_plan and service_plans.append(service_plan)
        return service_plans


class ServiceInstancesDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        service_instances = []
        for instances_json in self.client.getResources('/%s/service_instances' % self.api_version):
            for instance_json in instances_json:
                logger.debug('Service Instances Discover: %s' % instance_json)
                service_instance = cloudfoundry.ServiceInstance(instance_json['metadata']['guid'])
                service_instance.name = instance_json['entity']['name']
                service_instance.service_plan_guid = instance_json['entity']['service_plan_guid']
                service_instance.space_guid = instance_json['entity']['space_guid']
                service_instance.dashboard_url = instance_json['entity']['dashboard_url']
                service_instance.type = instance_json['entity']['type']
                service_instance and service_instances.append(service_instance)
        return service_instances

class ServiceBindingsDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        service_bindings = []
        for bindings_json in self.client.getResources('/%s/service_bindings' % self.api_version):
            for binding_json in bindings_json:
                logger.debug('Service Bindings Discover: %s' % binding_json)
                service_binding = cloudfoundry.ServiceBinding(binding_json['metadata']['guid'], binding_json['entity']['app_guid'], binding_json['entity']['service_instance_guid'])
                service_binding and service_bindings.append(service_binding)
        return service_bindings


class RouteMappingsDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        route_mappings = []
        for mappings_json in self.client.getResources('/%s/route_mappings' % self.api_version):
            for mapping_json in mappings_json:
                logger.debug('Route Mapping Discover: %s' % mapping_json)
                route_mapping = cloudfoundry.RouteMapping(mapping_json['metadata']['guid'], mapping_json['entity']['app_guid'], mapping_json['entity']['route_guid'])
                route_mapping and route_mappings.append(route_mapping)
        return route_mappings