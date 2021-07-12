import modeling

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector


class Organization:
    def __init__(self, guid):
        self.guid = guid
        self.name = None
        self.status = None
        self.quota_definition_guid = None

    def report(self, container, quota_osh=None):
        vector = ObjectStateHolderVector()
        org_osh = ObjectStateHolder("cf_organization")
        org_osh.setAttribute("guid", self.guid)
        org_osh.setAttribute("name", self.name)
        if self.status:
            org_osh.setAttribute("status", self.status)
        org_osh.setContainer(container)
        vector.add(org_osh)
        if quota_osh:
            vector.add(modeling.createLinkOSH("dependency", org_osh, quota_osh))
        return org_osh, vector

class QuotaDef():
    def __init__(self, guid):
        self.guid = guid
        self.name = None
        self.non_basic_services_allowed = None
        self.total_services = None
        self.total_routes = None
        self.memory_limit = None
        self.instance_memory_limit = None
        self.total_private_domains = None
        self.app_instance_limit = None
        self.scope = None

    def report(self, container):
        org_quota_osh = ObjectStateHolder("cf_quota")
        org_quota_osh.setAttribute("guid", self.guid)
        org_quota_osh.setAttribute("name", self.name)
        if self.non_basic_services_allowed:
            org_quota_osh.setAttribute("non_basic_services_allowed", self.non_basic_services_allowed)
        if self.total_services:
            org_quota_osh.setAttribute("total_services", self.total_services)
        if self.total_routes:
            org_quota_osh.setAttribute("total_routes", self.total_routes)
        if self.memory_limit:
            org_quota_osh.setAttribute("memory_limit", self.memory_limit)
        if self.instance_memory_limit:
            org_quota_osh.setAttribute("instance_memory_limit", self.instance_memory_limit)
        if self.total_private_domains:
            org_quota_osh.setAttribute("total_private_domains", self.total_private_domains)
        if self.app_instance_limit:
            org_quota_osh.setAttribute("app_instance_limit", self.app_instance_limit)
        org_quota_osh.setContainer(container)
        return org_quota_osh

class SpaceQuotaDef(QuotaDef):
    def __init__(self, guid):
        QuotaDef.__init__(self, guid)
        self.organization_guid = None

class Domain:
    def __init__(self, guid):
        self.guid = guid
        self.name = None
        self.status = None

    def report(self, container):
        domain_osh = ObjectStateHolder("cf_domain")
        domain_osh.setAttribute("guid", self.guid)
        domain_osh.setAttribute("name", self.name)
        domain_osh.setAttribute("status", self.status)
        domain_osh.setContainer(container)
        return domain_osh

class PrivateDomain(Domain):
    def __init__(self, guid):
        Domain.__init__(self,guid)
        self.owning_organization_guid = None

class Application:
    def __init__(self, guid):
        self.guid = guid
        self.name = None
        self.instances = None
        self.disk_quota = None
        self.space_guid = None
        self.buildpack = None
        self.space_guid = None

    def report(self, container):
        app_osh = ObjectStateHolder("cf_app")
        app_osh.setAttribute("guid", self.guid)
        app_osh.setAttribute("name", self.name)
        if self.instances:
            app_osh.setAttribute("instances", self.instances)
        if self.disk_quota:
            app_osh.setAttribute("disk_quota", self.disk_quota)
        if self.buildpack:
            app_osh.setAttribute("buildpack", self.buildpack)
        app_osh.setContainer(container)
        return app_osh


class Space:
    def __init__(self, guid):
        self.guid = guid
        self.name = None
        self.organization_guid = None
        self.space_quota_definition_guid = None

    def report(self, org_osh, space_quota_osh):
        vector = ObjectStateHolderVector()
        space_osh = ObjectStateHolder("cf_space")
        space_osh.setAttribute("guid", self.guid)
        space_osh.setAttribute("name", self.name)
        space_osh.setContainer(org_osh)
        vector.add(space_osh)

        if space_quota_osh:
            vector.add(modeling.createLinkOSH("dependency", space_osh, space_quota_osh))
        return space_osh, vector


class Route:
    def __init__(self, guid):
        self.guid = guid
        self.host = None
        self.domain_guid = None
        self.space_guid = None
        self.service_instance_guid = None

    def report(self, domain_osh, service_instance_osh=None):
        vector = ObjectStateHolderVector()
        route_osh = ObjectStateHolder("cf_route")
        route_osh.setAttribute("guid", self.guid)
        route_osh.setAttribute("name", self.host)
        route_osh.setContainer(domain_osh)
        vector.add(route_osh)
        if service_instance_osh:
            vector.add(modeling.createLinkOSH("containment", service_instance_osh, route_osh))
        return route_osh, vector


class RouteMapping:
    def __init__(self, guid, app_guid, route_guid):
        self.guid = guid
        self.app_guid = app_guid
        self.route_guid = route_guid

    def report(self, app_osh, route_osh):
        return modeling.createLinkOSH("containment", app_osh, route_osh)

class Service:
    def __init__(self, guid):
        self.guid = guid
        self.name = None
        self.description = None

    def report(self, container):
        service_osh = ObjectStateHolder("cf_service")
        service_osh.setAttribute("guid", self.guid)
        service_osh.setAttribute("name", self.name)
        service_osh.setAttribute("description", self.description)
        service_osh.setContainer(container)
        return service_osh


class ServicePlan:
    def __init__(self, guid):
        self.guid = guid
        self.name = None
        self.free = None
        self.public = None
        self.active = None
        self.description = None
        self.service_guid = None

    def report(self, container):
        service_plan_osh = ObjectStateHolder("cf_service_plan")
        service_plan_osh.setAttribute("guid", self.guid)
        service_plan_osh.setAttribute("name", self.name)
        service_plan_osh.setAttribute("free", self.free)
        service_plan_osh.setAttribute("public", self.public)
        service_plan_osh.setAttribute("active", self.active)
        service_plan_osh.setAttribute("description", self.description)
        service_plan_osh.setContainer(container)
        return service_plan_osh

class ServiceInstance:
    def __init__(self, guid):
        self.guid = guid
        self.name = None
        self.service_plan_guid = None
        self.space_guid = None
        self.dashboard_url = None
        self.type = None

    def report(self, space_osh, service_plan_osh):
        vector = ObjectStateHolderVector()
        service_instance_osh = ObjectStateHolder("cf_service_instance")
        service_instance_osh.setAttribute("guid", self.guid)
        service_instance_osh.setAttribute("name", self.name)
        service_instance_osh.setAttribute("dashboard_url", self.dashboard_url)
        service_instance_osh.setAttribute("type", self.type)
        service_instance_osh.setContainer(space_osh)
        vector.add(service_instance_osh)
        vector.add(modeling.createLinkOSH("realization", service_plan_osh, service_instance_osh))
        return service_instance_osh, vector

    def reportDelete(self, space_osh):
        service_instance_osh = ObjectStateHolder("cf_service_instance")
        service_instance_osh.setAttribute("guid", self.guid)
        service_instance_osh.setAttribute("name", self.name)
        service_instance_osh.setContainer(space_osh)
        return service_instance_osh


class ServiceBinding:
    def __init__(self, guid, app_guid, service_instance_guid):
        self.guid = guid
        self.app_guid = app_guid
        self.service_instance_guid = service_instance_guid

    def report(self, app_osh, service_instance_osh):
        return modeling.createLinkOSH("usage", app_osh, service_instance_osh)


class CloudFoundry:
    def __init__(self, name):
        self.name = name

    def report(self, endpoint):
        vector = ObjectStateHolderVector()
        cf_osh = ObjectStateHolder("cloudfoundry")
        if self.name:
            cf_osh.setAttribute("name", self.name)
        vector.add(cf_osh)
        if endpoint:
            uri_osh = ObjectStateHolder('uri_endpoint')
            uri_osh.setAttribute('uri', endpoint)
            uri_osh.setAttribute('type', "cloudfoundry")
            vector.add(uri_osh)
            vector.add(modeling.createLinkOSH("usage", cf_osh, uri_osh))
        return cf_osh, vector
