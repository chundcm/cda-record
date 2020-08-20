#coding=utf-8
import logger
import google_cloud_restful_client
import google_cloud
from google_cloud_resolve_json import report_client
from java.lang import Boolean, Integer, Exception

from appilog.common.system.types.vectors import ObjectStateHolderVector


DISCOVERY_COMPONENTS = ['compute']
# Shared resources
Resources = {
    'zoneByName': {}
}


def DiscoveryMain(framework):
    discover_all_project = Boolean.parseBoolean(framework.getParameter("discoverResourcesByAssetInventory"))
    export_timeout = Integer.parseInt(framework.getParameter("exportAssetInventoryTimeOut"))
    discoveredProjects = []
    discoveryWarnings = []
    protocolName = 'google_cloudprotocol'
    credentials = framework.getAvailableProtocols(None, protocolName)
    for credentialsId in credentials:
        try:
            proxies = {}
            logger.info('Establish connection to Google Cloud with credential: ', credentialsId)
            proxyHost = framework.getProtocolProperty(credentialsId, "proxy_host", "")
            proxyPort = framework.getProtocolProperty(credentialsId, "proxy_port", "")
            keyFile = framework.getProtocolProperty(credentialsId, "key_file", "")

            if proxyHost and proxyPort:
                proxy = 'http://' + proxyHost + ':' + proxyPort
                logger.debug("proxy:", proxy)
                proxies['http'] = proxy
                proxies['https'] = proxy
            gcloudClient = google_cloud_restful_client.GoogleCloudClient(keyFile, proxies, export_timeout)
            errorMessage, projectId = gcloudClient.authorize()
            if errorMessage:
                warning = 'Failed to get access token for project %s with credential: %s. \nError is %s'\
                          % (projectId, credentialsId, errorMessage)
                discoveryWarnings.append(warning)
            else:
                projectObj = google_cloud.Project(projectId)
                projectOSH = google_cloud.Reporter(google_cloud.Builder()).reportProject(projectObj)
                framework.sendObject(projectOSH)
                if discover_all_project:
                    gcloudClient.set_project_id(projectId)
                    bucket_status = gcloudClient.get_bucket()
                    if not bucket_status:
                        gcloudClient.create_bucket()
                    list_id = gcloudClient.get_organization_id()
                    for organization_id in list_id:
                        obj_dict = get_cloud_data(gcloudClient, organization_id)

                        if obj_dict:
                            vector = report_client(obj_dict)
                            framework.sendObjects(vector)
                            gcloudClient.delete_data()
                            return
                        else:
                            raise Exception('Failed to get google cloud topology.')

                else:
                    regions = discoverRegions(gcloudClient, framework)
                    if not regions:
                        raise Exception('Failed to get Regions and Zones.')

                    for component in DISCOVERY_COMPONENTS:
                        vector = executeDiscovery(framework, component, gcloudClient, projectObj)
                        framework.sendObjects(vector)
                    discoveredProjects.append(projectId)

        except:
            warning = 'Can not discover Project with credential: %s' % credentialsId
            logger.warnException(warning)
            discoveryWarnings.append(warning)

    if discoveryWarnings:
        discoveredProjectsMsg = 'Discovered Project(s): ' + str(discoveredProjects)
        logger.reportWarning(discoveredProjectsMsg)
        map(logger.reportWarning, discoveryWarnings)


def executeDiscovery(framework, component, gcloudClient, projectObj):
    script = 'google_cloud_' + component + '_discoverer'
    className = 'Discoverer'
    module = __import__(script)
    if hasattr(module, className):
        discovererClass = getattr(module, className)
        discoverer = discovererClass(framework, gcloudClient, projectObj)
        logger.debug('Discover ' + discoverer.description)
        discoverer.discover(Resources)
        return discoverer.report()
    else:
        logger.debug("Failed to import the module %s" % script)


def get_organization(gcloudClient):
    list_id = gcloudClient.get_organization_id()
    return list_id


def get_cloud_data(gcloudClient, organization_id):
    response_code = gcloudClient.export_data(organization_id)
    if response_code:
        logger.debug('Export all of project to determine bucket successfully!')
        write_status = gcloudClient.get_repeat_response()
        if write_status:
            obj_dict = gcloudClient.read_file()
            return obj_dict
    return []


def discoverRegions(gcloudClient, framework):
    try:
        vector = ObjectStateHolderVector()
        service = gcloudClient.getService('compute')
        jsonRsp = gcloudClient.getApiAndExecute(service, 'regions', 'list')
        regions = []

        for region in jsonRsp['items']:
            regionName = region['name']
            regionObj = google_cloud.Region(regionName)
            for zone in region['zones']:
                zoneName = zone.split('/')[-1]
                zoneObj = google_cloud.Zone(zoneName, regionName)
                regionObj.addZone(zoneObj)
            regions.append(regionObj)
        gcloudReporter = google_cloud.Reporter(google_cloud.Builder())
        for region in regions:
            try:
                vector.addAll(gcloudReporter.reportRegion(region))
            except Exception:
                logger.warnException("Failed to report %s" % region)
            else:
                for zone in region.getZones():
                    try:
                        vector.addAll(gcloudReporter.reportZoneInRegion(region, zone))
                        Resources['zoneByName'][zone.getName()] = zone
                    except Exception:
                        logger.warnException("Failed to report %s" % zone)
        framework.sendObjects(vector)
        return regions
    except:
        logger.debugException('Failed to discover Regions and Zones.')
        return None



