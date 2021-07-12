# coding=utf-8
import sys
import logger
import cloudfoundry_discoverer
import cloudfoundry_client

from appilog.common.system.types.vectors import ObjectStateHolderVector
from com.hp.ucmdb.discovery.common import CollectorsConstants


dict_org_quota_osh = {}
dict_domain_osh = {}
dict_org_osh = {}
dict_space_quota_osh = {}
dict_space_osh = {}
dict_app_osh = {}
dict_route_osh = {}
dict_service_osh = {}
dict_service_plan_osh = {}
dict_service_instance_osh = {}

def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    api_version = "v2"

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
            password = Framework.getProtocolProperty(protocol, CollectorsConstants.PROTOCOL_ATTRIBUTE_PASSWORD, "")
            http_proxy = Framework.getProtocolProperty(protocol, "proxy", "")

            if http_proxy:
                logger.debug("proxy:", http_proxy)
                proxies['http'] = http_proxy
                proxies['https'] = http_proxy

            cred = cloudfoundry_client.CloudFoudryCredential(username, password)
            try:
                client = cloudfoundry_client.CloudFoundryClient(endpoint, api_version, proxies)
                client.login(cred)
            except:
                api_version = "v3"
                client = cloudfoundry_client.CloudFoundryClient(endpoint, api_version, proxies)
                client.login(cred)

            if not client:
                raise Exception("Failed to connect to cloud foundry api endpoint %s using protocol %s" % (endpoint, protocol))

            # discover
            cf = getCloudFoundry(client)
            org_quotas = getOrgQuotaDefs(client)
            shared_domains = getSharedDomains(client)

            organizations = getOrganizations(client)
            space_quotas = getSpaceQuotaDefs(client)
            private_domains = getPrivateDomains(client)

            spaces = getSpaces(client)
            apps = getApplications(client)
            routes = getRoutes(client)
            route_mappings = getRouteMappings(client)

            services = getServices(client)
            service_plans = getServicePlans(client)
            service_instances = getServiceInstances(client)
            service_bindings = getServiceBindings(client)

            #report
            cf_osh, cf_vector = cf.report(endpoint)
            OSHVResult.addAll(cf_vector)
            OSHVResult.addAll(reportOrgQuotaDefs(org_quotas, cf_osh))
            OSHVResult.addAll(reportSharedDomains(shared_domains, cf_osh))

            OSHVResult.addAll(reportOrganizations(organizations, cf_osh))
            OSHVResult.addAll(reportSpaceQuotaDefs(space_quotas))
            OSHVResult.addAll(reportPrivateDomains(private_domains))

            OSHVResult.addAll(reportSpaces(spaces))
            OSHVResult.addAll(reportApplications(apps))
            OSHVResult.addAll(reportRoutes(routes))
            OSHVResult.addAll(reportRouteMappings(route_mappings))

            OSHVResult.addAll(reportServices(services, cf_osh))
            OSHVResult.addAll(reportServicePlans(service_plans))
            OSHVResult.addAll(reportServiceInstances(service_instances))
            OSHVResult.addAll(reportServiceBindings(service_bindings))


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

def getCloudFoundry(client):
    cf_discoverer = cloudfoundry_discoverer.CloudFoundryDiscoverer(client)
    return cf_discoverer.discover()

def getOrgQuotaDefs(client):
    org_quotas = []
    try:
        org_quota_discover = cloudfoundry_discoverer.OrgQuotaDefinitionsDiscoverer(client)
        org_quotas.extend(org_quota_discover.discover())
        logger.debug("found %s organization quotas:" % len(org_quotas))
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning("Failed to discover cloud foundry organization quota definitions.")
    return org_quotas

def getSharedDomains(client):
    shared_domains = []
    try:
        shared_domain_discover = cloudfoundry_discoverer.SharedDomainsDiscoverer(client)
        shared_domains.extend(shared_domain_discover.discover())
        logger.debug("found %s shared_domains" % len(shared_domains))
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning("Failed to discover cloud foundry shared domains.")
    return shared_domains

def getOrganizations(client):
    organization_discover = cloudfoundry_discoverer.OrganizationsDiscoverer(client)
    organizations = organization_discover.discover()
    logger.debug("found %s organizations" % len(organizations))
    return organizations

def getSpaceQuotaDefs(client):
    space_quotas = []
    try:
        space_quota_discover = cloudfoundry_discoverer.SpaceQuotaDefinitionsDiscoverer(client)
        space_quotas.extend(space_quota_discover.discover())
        logger.debug("found %s space quotas" % len(space_quotas))
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning("Failed to discover cloud foundry space quota definitions.")
    return space_quotas

def getPrivateDomains(client):
    private_domains = []
    try:
        private_domain_discover = cloudfoundry_discoverer.PrivateDomainsDiscoverer(client)
        private_domains.extend(private_domain_discover.discover())
        logger.debug("found %s private_domains" % len(private_domains))
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning("Failed to discover cloud foundry private domains.")
    return private_domains

def getSpaces(client):
    space_discover = cloudfoundry_discoverer.SpacesDiscoverer(client)
    spaces = space_discover.discover()
    logger.debug("found %s spaces" % len(spaces))
    return spaces

def getApplications(client):
    apps = []
    try:
        app_discover = cloudfoundry_discoverer.AppsDiscoverer(client)
        apps.extend(app_discover.discover())
        logger.debug("found %s apps" % len(apps))
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning("Failed to discover cloud foundry applications.")
    return apps

def getRoutes(client):
    routes = []
    try:
        route_discover = cloudfoundry_discoverer.RoutesDiscoverer(client)
        routes.extend(route_discover.discover())
        logger.debug("found %s routes" % len(routes))
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning("Failed to discover cloud foundry routes.")
    return routes

def getRouteMappings(client):
    route_mappings = []
    try:
        route_mapping_discover = cloudfoundry_discoverer.RouteMappingsDiscoverer(client)
        route_mappings.extend(route_mapping_discover.discover())
        logger.debug("found %s route_mappings" % len(route_mappings))
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning("Failed to discover cloud foundry route mappings.")
    return route_mappings

def getServices(client):
    services = []
    try:
        service_discover = cloudfoundry_discoverer.ServicesDiscoverer(client)
        services.extend(service_discover.discover())
        logger.debug("found %s services" % len(services))
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning("Failed to discover cloud foundry services.")
    return services

def getServicePlans(client):
    service_plans = []
    try:
        service_plan_discover = cloudfoundry_discoverer.ServicePlansDiscoverer(client)
        service_plans.extend(service_plan_discover.discover())
        logger.debug("found %s service plans" % len(service_plans))
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning("Failed to discover cloud foundry service plans.")
    return service_plans

def getServiceInstances(client):
    service_instances = []
    try:
        service_instance_discover = cloudfoundry_discoverer.ServiceInstancesDiscoverer(client)
        service_instances.extend(service_instance_discover.discover())
        logger.debug("found %s service instances" % len(service_instances))
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning("Failed to discover cloud foundry service instances.")
    return service_instances

def getServiceBindings(client):
    service_bindings = []
    try:
        service_binding_discover = cloudfoundry_discoverer.ServiceBindingsDiscoverer(client)
        service_bindings.extend(service_binding_discover.discover())
        logger.debug("found %s service_bindings" % len(service_bindings))
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning("Failed to discover cloud foundry service bindings.")
    return service_bindings

def reportOrgQuotaDefs(org_quotas, container):
    vector = ObjectStateHolderVector()
    try:
        for quota in org_quotas:
            quota_osh = quota.report(container)
            dict_org_quota_osh[quota.guid] = quota_osh
            vector.add(quota_osh)
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning("Failed to report cloud foundry organization quota definitions.")
    return vector

def reportSharedDomains(shared_domains, container):
    vector = ObjectStateHolderVector()
    try:
        for domain in shared_domains:
            domain_osh = domain.report(container)
            dict_domain_osh[domain.guid] = domain_osh
            vector.add(domain_osh)
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning("Failed to report cloud foundry shared domains.")
    return vector

def reportOrganizations(organizations, container):
    vector = ObjectStateHolderVector()
    try:
        for organization in organizations:
            quota_osh = dict_org_quota_osh.get(organization.quota_definition_guid, None)
            org_osh, org_vector = organization.report(container, quota_osh)
            dict_org_osh[organization.guid] = org_osh
            vector.addAll(org_vector)
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportError("Failed to report cloud foundry organizations.")
    return vector

def reportSpaceQuotaDefs(space_quotas):
    vector = ObjectStateHolderVector()
    try:
        for quota in space_quotas:
            org_osh = dict_org_osh.get(quota.organization_guid, None)
            quota_osh = quota.report(org_osh)
            if quota_osh:
                dict_space_quota_osh[quota.guid] = quota_osh
                vector.add(quota_osh)
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning("Failed to report cloud foundry space quota definitions.")
    return vector

def reportPrivateDomains(private_domains):
    vector = ObjectStateHolderVector()
    try:
        for domain in private_domains:
            org_osh = dict_org_osh.get(domain.owning_organization_guid, None)
            domain_osh = domain.report(org_osh)
            if domain_osh:
                dict_domain_osh[domain.guid] = domain_osh
                vector.add(domain_osh)
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning("Failed to report cloud foundry private domains.")
    return vector

def reportSpaces(spaces):
    vector = ObjectStateHolderVector()
    try:
        for space in spaces:
            org_osh = dict_org_osh.get(space.organization_guid, None)
            if org_osh:
                quota_osh = None
                if space.space_quota_definition_guid:
                    quota_osh = dict_space_quota_osh.get(space.space_quota_definition_guid, None)

                space_osh, space_vector = space.report(org_osh, quota_osh)
                dict_space_osh[space.guid] = space_osh
                vector.addAll(space_vector)
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportError("Failed to report cloud foundry spaces.")
    return vector

def reportApplications(apps):
    vector = ObjectStateHolderVector()
    try:
        for app in apps:
            space_osh = dict_space_osh.get(app.space_guid, None)
            if space_osh:
                app_osh = app.report(space_osh)
                dict_app_osh[app.guid] = app_osh
                vector.add(app_osh)
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning("Failed to report cloud foundry applications.")
    return vector

def reportRoutes(routes):
    vector = ObjectStateHolderVector()
    try:
        for route in routes:
            domain_osh = dict_domain_osh.get(route.domain_guid, None)
            if domain_osh:
                route_osh, route_vector = route.report(domain_osh)
                dict_route_osh[route.guid] = route_osh
                vector.addAll(route_vector)
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning("Failed to report cloud foundry routes.")
    return vector

def reportRouteMappings(route_mappings):
    vector = ObjectStateHolderVector()
    try:
        for mapping in route_mappings:
            app_osh = dict_app_osh.get(mapping.app_guid, None)
            route_osh = dict_route_osh.get(mapping.route_guid, None)
            if app_osh and route_osh:
                vector.add(mapping.report(app_osh, route_osh))
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning("Failed to report cloud foundry route mappings.")
    return vector

def reportServices(services, container):
    vector = ObjectStateHolderVector()
    try:
        for service in services:
            service_osh = service.report(container)
            dict_service_osh[service.guid] = service_osh
            vector.add(service_osh)
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning("Failed to report cloud foundry services.")
    return vector

def reportServicePlans(service_plans):
    vector = ObjectStateHolderVector()
    try:
        for plan in service_plans:
            service_osh = dict_service_osh.get(plan.service_guid, None)
            if service_osh:
                plan_osh = plan.report(service_osh)
                dict_service_plan_osh[plan.guid] = plan_osh
                vector.add(plan_osh)
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning("Failed to report cloud foundry service plans.")
    return vector

def reportServiceInstances(service_instances):
    vector = ObjectStateHolderVector()
    try:
        for instance in service_instances:
            space_osh = dict_space_osh.get(instance.space_guid, None)
            service_plan_osh = dict_service_plan_osh.get(instance.service_plan_guid, None)
            if space_osh and service_plan_osh:
                instance_osh, instance_vector = instance.report(space_osh, service_plan_osh)
                dict_service_instance_osh[instance.guid] = instance_osh
                vector.addAll(instance_vector)
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning("Failed to report cloud foundry service instances.")
    return vector

def reportServiceBindings(service_bindings):
    vector = ObjectStateHolderVector()
    try:
        for binding in service_bindings:
            app_osh = dict_app_osh.get(binding.app_guid, None)
            service_instance_osh = dict_service_instance_osh.get(binding.service_instance_guid, None)
            if app_osh and service_instance_osh:
                vector.add(binding.report(app_osh, service_instance_osh))
    except:
        strException = str(sys.exc_info()[1])
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        logger.reportWarning("Failed to report cloud foundry service bindings.")
    return vector
