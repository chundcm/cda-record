# coding=utf-8
import sys
import time
import datetime
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector

import event_hub
import logger
import modeling
import cloudfoundry_by_webservices
from json_stream import JsonStreamWrapper
import cloudfoundry_client
import cloudfoundry
from com.hp.ucmdb.discovery.common import CollectorsConstants


def DiscoveryMain(Framework):
    logger.info('Job started')
    eventHub = event_hub.EventHub(Framework)
    OSHVResult = ObjectStateHolderVector()
    api_version = "v2"

    endpoint = Framework.getDestinationAttribute('endpoint')
    ip = Framework.getDestinationAttribute('ip')
    protocols = Framework.getAvailableProtocols(ip, "http")
    proxies = {}
    cfClient = None
    cfCredential = None

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
            client = cloudfoundry_client.CloudFoundryClient(endpoint, api_version, proxies)

            client.login(cred)
            cfClient = client
            cfCredential = cred
            break
        except:
            strException = str(sys.exc_info()[1])
            excInfo = logger.prepareJythonStackTrace('')
            logger.debug(strException)
            logger.debug(excInfo)
            pass

    #discover
    if cfClient:
        cf = cloudfoundry_by_webservices.getCloudFoundry(cfClient)
    else:
        msg = 'Failed to connect using all protocols'
        logger.reportError(msg)
        logger.error(msg)
        return OSHVResult
    cfOSH, cfVector = cf.report(endpoint)
    eventHub.sendAndFlushObjects(cfVector)

    cacheDict = {}
    cacheDict['cfOSH'] = {}
    cacheDict['cfOSH']['OSH'] = cfOSH

    @eventHub.source
    class MySource(object):
        def __init__(self, hub):
            super(MySource, self).__init__()
            self.eventHub = hub
            logger.info('Init source')
            self.isAlive = True
            self.eventSource = None
            self.response = None
            self.sleepTime = 2
            self.maxSleepTime = 30

        def start(self):
            logger.info('Start CloudFoundry event source...')
            self.eventSource = CloudFoundryEventSource(Framework)
            self.eventSource.start(self, cfClient, cfCredential)

        def stop(self):
            logger.debug('Stop source')
            self.isAlive = False

        def __repr__(self):
            return 'CloudFoundry event pulling'

    @eventHub.handler
    class MyHandler(object):
        def isApplicable(self, eventCube):
            return True

        def handle(self, eventCube):
            logger.debug('handle event %s' % eventCube.event)
            event = eventCube.event
            eventType = event['entity']['type']
            logger.debug('Event type:', eventType)
            type = eventType.split('.')[2]
            acteeGUID = event['entity']['actee']

            vector = None
            if eventType == 'audit.app.create':
                vector, _ = self.getApp(event)
            elif eventType == 'audit.app.map-route':
                vector, _ = self.getRouteMapping(event)
            elif eventType == 'audit.route.create':
                vector, _ = self.getRoute(event, type)
            elif eventType == 'audit.service_binding.create':
                vector, _ = self.getServiceBinding(event, type)
            elif eventType == 'audit.service.create':
                vector = ObjectStateHolderVector()
                serviceOSH = self.getService(event)
                vector.add(serviceOSH)
            elif eventType == 'audit.service_instance.create':
                vector, _ = self.getServiceInstance(event, type)
            elif eventType == 'audit.service_plan.create':
                vector, _ = self.getServicePlan(event)
            elif eventType == 'audit.space.create':
                vector, _ = self.getSpace(event)
            if vector:
                eventHub.sendAndFlushObjects(vector)
                logger.debug('Reported CIs: ', vector)

            target = None
            if eventType == 'audit.app.delete-request':
                _, target = self.getApp(event)
            elif eventType == 'audit.app.unmap-route':
                _, target = self.getRouteMapping(event)
            elif eventType == 'audit.route.delete-request':
                _, target = self.getRoute(event, type)
            elif eventType == 'audit.service_binding.delete':
                _, target = self.getServiceBinding(event, type)
            elif eventType == 'audit.service.delete':
                target = self.getService(event)
            elif eventType == 'audit.service_instance.delete':
                _, target = self.getServiceInstance(event, type)
            elif eventType == 'audit.service_plan.delete':
                _, target = self.getServicePlan(event)
            elif eventType == 'audit.space.delete-request':
                _, target = self.getSpace(event)
            if target:
                eventHub.deleteAndFlushObjects(target)
                if cacheDict.has_key(acteeGUID):
                    cacheDict.pop(acteeGUID)
                logger.debug('Deleted CI: ', target)

        def getApp(self, event):
            vector = ObjectStateHolderVector()
            app = cloudfoundry.Application(event['entity']['actee'])
            app.name = event['entity']['actee_name']
            app.space_guid = event['entity']['space_guid']
            if event['entity']['metadata']['request'].has_key('instances'):
                app.instances = event['entity']['metadata']['request']['instances']

            spaceOSH, orgOSH = self._getSpaceAndOrg(app.space_guid)
            if spaceOSH:
                vector.add(orgOSH)
                vector.add(spaceOSH)
                appOSH = app.report(spaceOSH)
                self._cacheOSH(event['entity']['actee'], appOSH, app.space_guid)
                vector.add(appOSH)
                return vector, appOSH
            return None, None

        def getRoute(self, event, type):
            if type.find('delete') != -1:
                if cacheDict.has_key(event['entity']['actee']):
                    routeOSH = self._getCacheOSH(event['entity']['actee'])
                    return None, routeOSH
                return None, None

            vector = ObjectStateHolderVector()
            route = cloudfoundry.Route(event['entity']['actee'])
            route.host = event['entity']['actee_name']
            route.space_guid = event['entity']['space_guid']
            if event['entity']['metadata']['request'].has_key('domain_guid'):
                route.domain_guid = event['entity']['metadata']['request']['domain_guid']

            domainOSH = self._getDomain(route.domain_guid)
            if domainOSH:
                routeOSH, tempVector = route.report(domainOSH)
                self._cacheOSH(event['entity']['actee'], routeOSH, route.space_guid)
                vector.addAll(tempVector)
                vector.add(routeOSH)
                vector.add(domainOSH)
                return vector, routeOSH
            return None, None

        def getRouteMapping(self, event):
            vector = ObjectStateHolderVector()
            mapApp = event['entity']['actee']
            mapRoute = event['entity']['metadata']['route_guid']
            tempVector, appOSH = self._getApp(mapApp)
            if appOSH:
                vector.add(appOSH)
                vector.addAll(tempVector)

                tempVector, routeOSH = self._getRoute(mapRoute)
                if routeOSH:
                    vector.add(routeOSH)
                    vector.addAll(tempVector)
                    appRouteLink = modeling.createLinkOSH("containment", appOSH, routeOSH)
                    self._cacheOSH(event['metadata']['guid'], appRouteLink)
                    vector.add(appRouteLink)
                    return vector, appRouteLink
            return None, None

        def getServiceBinding(self, event, type):
            if type.find('delete') != -1:
                if cacheDict.has_key(event['entity']['actee']):
                    appServiceLink = self._getCacheOSH(event['entity']['actee'])
                    return None, appServiceLink
                return None, None

            vector = ObjectStateHolderVector()
            serviceInstance = event['entity']['metadata']['request']['service_instance_guid']
            app = event['entity']['metadata']['request']['app_guid']
            tempVector, appOSH = self._getApp(app)
            if appOSH:
                vector.add(appOSH)
                vector.addAll(tempVector)

                tempVector, serviceInstanceOSH = self._getServiceInstance(serviceInstance)
                if serviceInstanceOSH:
                    vector.add(serviceInstanceOSH)
                    vector.addAll(tempVector)
                    appServiceLink = modeling.createLinkOSH("usage", appOSH, serviceInstanceOSH)
                    self._cacheOSH(event['entity']['actee'], appServiceLink)
                    vector.add(appServiceLink)
                    return vector, appServiceLink
            return None, None

        def getService(self, event):
            service = cloudfoundry.Service(event['entity']['actee'])
            service.name = event['entity']['actee_name']
            if event['entity']['metadata'].has_key('description'):
                service.description = event['entity']['metadata']['description']
            serviceOSH = service.report(cfOSH)
            self._cacheOSH(event['entity']['actee'], serviceOSH)
            return serviceOSH

        def getServiceInstance(self, event, type):
            vector = ObjectStateHolderVector()
            serviceInstance = cloudfoundry.ServiceInstance(event['entity']['actee'])
            serviceInstance.name = event['entity']['actee_name']
            serviceInstance.space_guid = event['entity']['space_guid']
            if event['entity']['metadata']['request'].has_key('service_plan_guid'):
                serviceInstance.service_plan_guid = event['entity']['metadata']['request']['service_plan_guid']

            spaceOSH, orgOSH = self._getSpaceAndOrg(serviceInstance.space_guid)
            if spaceOSH:
                vector.add(orgOSH)
                vector.add(spaceOSH)
                if type.find('delete') != -1:
                    serviceInstanceOSH = serviceInstance.reportDelete(spaceOSH)
                    return None, serviceInstanceOSH
                serviceOSH, servicePlanOSH = self._getServicePlan(serviceInstance.service_plan_guid)
                if serviceOSH:
                    serviceInstanceOSH, tempVector = serviceInstance.report(spaceOSH, servicePlanOSH)
                    self._cacheOSH(event['entity']['actee'], serviceInstanceOSH, serviceInstance.space_guid)
                    vector.add(serviceOSH)
                    vector.add(servicePlanOSH)
                    vector.addAll(tempVector)
                    return vector, serviceInstanceOSH
            return None, None

        def getServicePlan(self, event):
            vector = ObjectStateHolderVector()
            servicePlan = cloudfoundry.ServicePlan(event['entity']['actee'])
            servicePlan.name = event['entity']['actee_name']
            if event['entity']['metadata'].has_key('service_guid'):
                servicePlan.free = event['entity']['metadata']['free']
                servicePlan.description = event['entity']['metadata']['description']
                servicePlan.public = event['entity']['metadata']['public']
                servicePlan.active = event['entity']['metadata']['active']
                servicePlan.service_guid = event['entity']['metadata']['service_guid']

            serviceOSH = self._getService(servicePlan.service_guid)
            if serviceOSH:
                servicePlanOSH = servicePlan.report(serviceOSH)
                self._cacheOSH(event['entity']['actee'], servicePlanOSH, servicePlan.service_guid)
                vector.add(serviceOSH)
                vector.add(servicePlanOSH)
                return vector, servicePlanOSH
            return None, None

        def getSpace(self, event):
            vector = ObjectStateHolderVector()
            space = cloudfoundry.Space(event['entity']['actee'])
            space.name = event['entity']['actee_name']
            space.organization_guid = event['entity']['organization_guid']

            orgOSH = self._getOrg(space.organization_guid)
            if orgOSH:
                spaceOSH, _ = space.report(orgOSH, None)
                vector.add(orgOSH)
                vector.add(spaceOSH)
                self._cacheOSH(event['entity']['actee'], spaceOSH, space.organization_guid)
                return vector, spaceOSH
            return None, None

        def _getApp(self, appGUID):
            if cacheDict.has_key(appGUID):
                vector = ObjectStateHolderVector()
                appOSH = self._getCacheOSH(appGUID)
                spaceOSH, orgOSH = self._getSpaceAndOrg(cacheDict[appGUID]['RC'])
                vector.add(orgOSH)
                vector.add(spaceOSH)
                vector.add(appOSH)
                return vector, appOSH

            vector = ObjectStateHolderVector()
            appJson = cfClient.getJsonRsp('/%s/apps/%s' % (api_version, appGUID))
            if appJson:
                app = cloudfoundry.Application(appJson['metadata']['guid'])
                app.name = appJson['entity']['name']
                app.space_guid = appJson['entity']['space_guid']

                spaceOSH, orgOSH = self._getSpaceAndOrg(app.space_guid)
                if spaceOSH:
                    vector.add(orgOSH)
                    vector.add(spaceOSH)
                    appOSH = app.report(spaceOSH)
                    self._cacheOSH(appJson['metadata']['guid'], spaceOSH, app.space_guid)
                    vector.add(appOSH)
                    return vector, appOSH
            return None, None

        def _getDomain(self, domainGUID):
            if cacheDict.has_key(domainGUID):
                domainOSH = self._getCacheOSH(domainGUID)
                return domainOSH

            domainJson = cfClient.getJsonRsp('/%s/shared_domains/%s' % (api_version, domainGUID))
            if domainJson:
                domain = cloudfoundry.Domain(domainJson['metadata']['guid'])
                domain.name = domainJson['entity']['name']
                domainOSH = domain.report(cfOSH)
                self._cacheOSH(domainGUID, domainOSH)
                return domainOSH

            domainJson = cfClient.getJsonRsp('/%s/private_domains/%s' % (api_version, domainGUID))
            if domainJson:
                domain = cloudfoundry.PrivateDomain(domainJson['metadata']['guid'])
                domain.name = domainJson['entity']['name']
                domainOSH = domain.report(cfOSH)
                self._cacheOSH(domainGUID, domainOSH)
                return domainOSH
            return None

        def _getRoute(self, routeGUID):
            vector = ObjectStateHolderVector()
            routeJson = cfClient.getJsonRsp('/%s/routes/%s' % (api_version, routeGUID))
            if routeJson:
                route = cloudfoundry.Route(routeJson['metadata']['guid'])
                route.host = routeJson['entity']['host']
                route.domain_guid = routeJson['entity']['domain_guid']

                domainOSH = self._getDomain(route.domain_guid)
                if domainOSH:
                    routeOSH, tempVector = route.report(domainOSH)
                    vector.addAll(tempVector)
                    vector.add(routeOSH)
                    vector.add(domainOSH)
                    self._cacheOSH(routeGUID, routeOSH, route.domain_guid)
                    return vector, routeOSH
            return None, None

        def _getServiceInstance(self, serviceInstanceGUID):
            vector = ObjectStateHolderVector()
            serviceJson = cfClient.getJsonRsp('/%s/service_instances/%s' % (api_version, serviceInstanceGUID))
            if serviceJson:
                serviceInstance = cloudfoundry.ServiceInstance(serviceJson['metadata']['guid'])
                serviceInstance.name = serviceJson['entity']['name']
                serviceInstance.service_plan_guid = serviceJson['entity']['service_plan_guid']
                serviceInstance.space_guid = serviceJson['entity']['space_guid']

                serviceOSH, servicePlanOSH = self._getServicePlan(serviceInstance.service_plan_guid)
                if serviceOSH:
                    spaceOSH, orgOSH = self._getSpaceAndOrg(serviceInstance.space_guid)
                    if spaceOSH:
                        vector.add(orgOSH)
                        vector.add(spaceOSH)
                        vector.add(serviceOSH)
                        vector.add(servicePlanOSH)
                        serviceInstanceOSH, tempVector = serviceInstance.report(spaceOSH, servicePlanOSH)
                        self._cacheOSH(serviceJson['metadata']['guid'], serviceInstanceOSH, serviceInstance.space_guid)
                        vector.addAll(tempVector)
                        return vector, serviceInstanceOSH

        def _getService(self, serviceGUID):
            if cacheDict.has_key(serviceGUID):
                serviceOSH = self._getCacheOSH(serviceGUID)
                return serviceOSH

            serviceJson = cfClient.getJsonRsp('/%s/services/%s' % (api_version, serviceGUID))
            if serviceJson:
                service = cloudfoundry.Service(serviceJson['metadata']['guid'])
                service.name = serviceJson['entity']['label']
                serviceOSH = service.report(cfOSH)
                self._cacheOSH(serviceJson['metadata']['guid'], serviceOSH)
                return serviceOSH

        def _getServicePlan(self, servicePlanGUID):
            if cacheDict.has_key(servicePlanGUID):
                servicePlanOSH = self._getCacheOSH(servicePlanGUID)
                if servicePlanOSH:
                    serviceOSH = self._getService(cacheDict[servicePlanGUID]['RC'])
                    return serviceOSH, servicePlanOSH

            servicePlanJson = cfClient.getJsonRsp('/%s/service_plans/%s' % (api_version, servicePlanGUID))
            if servicePlanJson:
                servicePlan = cloudfoundry.ServicePlan(servicePlanJson['metadata']['guid'])
                servicePlan.name = servicePlanJson['entity']['name']
                servicePlan.service_guid = servicePlanJson['entity']['service_guid']

                serviceOSH = self._getService(servicePlan.service_guid)
                if serviceOSH:
                    servicePlanOSH = servicePlan.report(serviceOSH)
                    self._cacheOSH(servicePlanJson['metadata']['guid'], servicePlanOSH, servicePlan.service_guid)
                    return serviceOSH, servicePlanOSH
            return None, None

        def _getSpaceAndOrg(self, spaceGUID):
            if cacheDict.has_key(spaceGUID):
                spaceOSH = self._getCacheOSH(spaceGUID)
                if spaceOSH:
                    orgOSH = self._getOrg(cacheDict[spaceGUID]['RC'])
                    return spaceOSH, orgOSH

            spaceJson = cfClient.getJsonRsp('/%s/spaces/%s' % (api_version, spaceGUID))
            if spaceJson:
                space = cloudfoundry.Space(spaceJson['metadata']['guid'])
                space.name = spaceJson['entity']['name']
                space.organization_guid = spaceJson['entity']['organization_guid']

                orgOSH = self._getOrg(space.organization_guid)
                if orgOSH:
                    spaceOSH, _ = space.report(orgOSH, None)
                    self._cacheOSH(spaceJson['metadata']['guid'], spaceOSH, space.organization_guid)
                    return spaceOSH, orgOSH
            return None, None

        def _getOrg(self, orgGUID):
            if cacheDict.has_key(orgGUID):
                orgOSH = self._getCacheOSH(orgGUID)
                return orgOSH

            orgJson = cfClient.getJsonRsp('/%s/organizations/%s' % (api_version, orgGUID))
            if orgJson:
                org = cloudfoundry.Organization(orgJson['metadata']['guid'])
                org.name = orgJson['entity']['name']

                orgOSH, _ =org.report(cfOSH)
                self._cacheOSH(orgJson['metadata']['guid'], orgOSH)
                return orgOSH
            return None

        def _cacheOSH(self, guid, osh, rc=None):
            cacheDict[guid] = {}
            cacheDict[guid]['RC'] = rc
            cacheDict[guid]['OSH'] = osh

        def _getCacheOSH(self, guid):
            if cacheDict.get(guid):
                return cacheDict.get(guid).get('OSH')

    def shutdownMonitor():
        return not Framework.isExecutionActive()

    eventHub.shutdownMonitor = shutdownMonitor
    try:
        eventHub.start()
    except:
        logger.debugException('')
        logger.reportError('Failed to start event monitor')
    logger.info('Job stopped')

class CloudFoundryEventSource():
    def __init__(self, Framework):
        self.lastTimeStamp = None
        self.EVENT_TYPES = ['audit.app.create',                 'audit.app.delete-request',
                            'audit.route.create',               'audit.route.delete-request',
                            'audit.service.create',             'audit.service.delete',
                            'audit.service_binding.create',   'audit.service_binding.delete',
                            'audit.service_instance.create',  'audit.service_instance.delete',
                            'audit.service_plan.create',       'audit.service_plan.delete',
                            'audit.space.create',               'audit.space.delete-request']
        self.EVENT_TYPES_LINK = ['audit.app.map-route',         'audit.app.unmap-route']
        self.Framework = Framework

    def start(self, source, cfClient, cfCredential):
        self.source = source
        self.cfClient = cfClient
        self.cfCredential = cfCredential
        while True:
            if self.source.isAlive:
                self.response = None
                try:
                    if self.lastTimeStamp:
                        self.response = self.cfClient.getCFEvents(self.lastTimeStamp)
                    else:
                        now = datetime.datetime.utcnow()
                        currentTimeStamp = now.strftime('%Y-%m-%dT%H:%M:%SZ')
                        self.response = self.cfClient.getCFEvents(currentTimeStamp)
                        logger.debug('Use timestamp: ', currentTimeStamp)
                    if self.response.status_code == 200:
                        self.read(self.response.iter_content(1))
                        time.sleep(30)
                    elif self.response.status_code == 401:
                        self.cfClient.headers = {
                                            'Accept-Encoding': 'deflate',
                                            'Accept': 'application/json',
                                            'Content-Type': 'application/x-www-form-urlencoded',
                                            'Authorization': 'Basic Y2Y6'
                                        }
                        self.cfClient.login(self.cfCredential)
                    else:
                        logger.debug('Failed to connect to CloudFoundry. Status code: ', self.response.status_code)
                        self.wait()
                except:
                    logger.debugException('Failed to connect to CloudFoundry.')
                    strException = str(sys.exc_info()[1])
                    excInfo = logger.prepareJythonStackTrace('')
                    logger.debug(strException)
                    logger.debug(excInfo)
                    self.wait()
            else:
                break

    def read(self, iter, clearResult=True, sendResult=True):
        iterEvent = JsonStreamWrapper(iter)
        if clearResult:
            self.EVENTS = {'app': {},
                           'route': {},
                           'service': {},
                           'service_binding': {},
                           'service_instance': {},
                           'service_plan': {},
                           'space': {}
                           }
            self.LINK_EVENTS = {'app': {}}
        try:
            for eventResult in iterEvent:
                for event in eventResult['resources']:
                    eventType = event['entity']['type']
                    if eventType in self.EVENT_TYPES:
                        self.saveEvent(event, self.EVENTS)
                    if eventType in self.EVENT_TYPES_LINK:
                        self.saveEvent(event, self.LINK_EVENTS)

                if eventResult['next_url']:
                    rsp = self.cfClient.getCFNextEvents(eventResult['next_url'])
                    if rsp.status_code == 200:
                        self.read(rsp.iter_content(1), False, False)
                if sendResult:
                    for comp in self.EVENTS.keys():
                        for item in self.EVENTS[comp].keys():
                            self.source.eventHub.send(self.EVENTS[comp][item])
                    for comp in self.LINK_EVENTS.keys():
                        for item in self.LINK_EVENTS[comp].keys():
                            self.source.eventHub.send(self.LINK_EVENTS[comp][item])

            now = datetime.datetime.utcnow()
            if self.lastTimeStamp:
                timeStamp = datetime.datetime.strptime(self.lastTimeStamp, '%Y-%m-%dT%H:%M:%SZ')
                nextTime = timeStamp + datetime.timedelta(seconds=1)
                self.lastTimeStamp = nextTime.strftime('%Y-%m-%dT%H:%M:%SZ')
            else:
                self.lastTimeStamp = now.strftime('%Y-%m-%dT%H:%M:%SZ')
            if sendResult:
                logger.debug('Get Event from: ', self.lastTimeStamp)
                logger.debug('Collect Events: ', self.EVENTS)
                logger.debug('Collect Link Events: ', self.LINK_EVENTS)
        except:
            logger.debugException('')

    def saveEvent(self,event, eventDict):
        if event['entity']['timestamp'] > self.lastTimeStamp:
            self.lastTimeStamp = event['entity']['timestamp']
        _, compotent, type = event['entity']['type'].split('.')
        if not eventDict[compotent].has_key(event['entity']['actee']):
            eventDict[compotent][event['entity']['actee']] = event
        else:
            oldEvent = eventDict[compotent][event['entity']['actee']]
            if event['entity']['timestamp'] > oldEvent['entity']['timestamp']:
                eventDict[compotent][event['entity']['actee']] = event

    def wait(self):
        if self.source.isAlive:
            if self.source.sleepTime > self.source.maxSleepTime:
                logger.debug('Wait %s seconds.' % self.source.maxSleepTime)
                time.sleep(self.source.maxSleepTime)
            else:
                logger.debug('Wait %s seconds.' % self.source.sleepTime)
                time.sleep(self.source.sleepTime)
                self.source.sleepTime += 2

