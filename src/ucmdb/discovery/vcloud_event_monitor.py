# coding=utf-8
from appilog.common.system.types.vectors import ObjectStateHolderVector

__author__ = 'Kane'

from appilog.common.system.types import ObjectStateHolder

import event_hub
import logger
import json
import netutils

import amqp_event_source
import modeling


class MySource(object):
    EVENT_TYPES = ['com/vmware/vcloud/event/vm/create',
                   'com/vmware/vcloud/event/vm/delete',
                   'com/vmware/vcloud/event/vm/undeploy',
                   ]

    def __init__(self, hub, amqp_config):
        super(MySource, self).__init__()
        self.eventHub = hub
        logger.info('Init source')
        self.count = 0
        self.isAlive = True
        self.eventSource = None
        self.amqp_config = amqp_config

    def consume(self, message):
        try:
            if message.properties['content_type'] == 'application/json':
                logger.debug('Raw vCloud event:', message.body)
                event = json.loads(message.body)
                event_type = event['type']
                if event_type and event_type in self.EVENT_TYPES:
                    self.eventHub.send(event)
        except:
            logger.debugException('Failed to consume message:', message.body)

    def start(self):
        if self.isAlive:
            logger.info('Start vCloud source pull.')
            self.eventSource = amqp_event_source.AMQPEventSource(self.amqp_config)
            try:
                self.eventSource.connect()
                self.eventSource.start(self.consume)
            except:
                logger.debugException('Failed to start source')
                raise

    def stop(self):
        logger.debug('Stop source')
        self.isAlive = True
        if self.eventSource:
            try:
                self.eventSource.stop()
            except:
                logger.debugException('')

    def __repr__(self):
        return 'vCloud event pulling'


class MyHandler(object):
    def __init__(self, Framework, vCloudWrapper, eventHub):
        super(MyHandler, self).__init__()
        self.Framework = Framework
        self.vCloudWrapper = vCloudWrapper
        self.eventHub = eventHub

    vm_cache = {}

    def isApplicable(self, eventCube):
        return True

    def handle(self, eventCube):
        Framework = self.Framework
        logger.debug('Handle event %s' % eventCube.event)
        event = eventCube.event
        event_type = event['type']
        logger.debug('Event type:', event_type)
        entity_id = event['entity']
        if event_type == 'com/vmware/vcloud/event/vm/undeploy':
            vm = self.vCloudWrapper.get_vm(entity_id)
            if vm:
                hostname = vm.getGuestCustomizationSection().getComputerName()
                logger.debug('Get VM host name:', hostname)
                self.vm_cache[entity_id] = hostname
        elif event_type == 'com/vmware/vcloud/event/vm/create':
            vm = self.vCloudWrapper.get_vm(entity_id)
            if vm:
                hostname = vm.getGuestCustomizationSection().getComputerName()
                logger.debug('Create host:', hostname)
                self.vm_cache[entity_id] = hostname
                vector = ObjectStateHolderVector()
                node = ObjectStateHolder('host_node')
                node.setStringAttribute('name', hostname)
                node.setStringAttribute('data_note', 'vCloud:' + entity_id)
                vector.add(node)
                self.__createNodeTopology(node, vm, vector)
                self.__createVappTopology(node, vm, vector)
                self.eventHub.sendAndFlushObjects(vector)
                logger.debug('Added CIs', vector)
        elif event_type == 'com/vmware/vcloud/event/vm/delete':
            hostname = self.vm_cache.pop(entity_id, None)
            if hostname:
                logger.debug('Delete host:', hostname)
                node = ObjectStateHolder('host_node')
                node.setStringAttribute('name', hostname)
                self.eventHub.deleteAndFlushObjects(node)
                logger.debug('Deleted CI', node)
            else:
                logger.debug('Entity is not in cache:', entity_id)

    def __createVappTopology(self, node, vm, vector):
        vapp = self.vCloudWrapper.get_vapp(vm.getParentVappReference())
        vapp_name = vapp.getResource().getName()
        logger.debug('Vapp name:', vapp_name)
        vdc = self.vCloudWrapper.get_vdc(vapp.getVdcReference())
        org = self.vCloudWrapper.get_org(vdc.getOrgReference())
        from vcloud_discover import _getUuidFromResource
        org_name = org.getResource().getName()
        logger.debug('Org name:', org_name)
        org_id = _getUuidFromResource(org.getResource())
        logger.debug('Org id:', org_id)
        organizationOsh = ObjectStateHolder("vcloud_managed_organization")
        organizationOsh.setStringAttribute('name', org_name)
        organizationOsh.setStringAttribute('vcloud_uuid', org_id.upper())
        vappOsh = ObjectStateHolder('vcloud_vapp')
        vappOsh.setStringAttribute('name', vapp_name)
        vappOsh.setContainer(organizationOsh)
        vector.add(organizationOsh)
        vector.add(vappOsh)
        vector.add(modeling.createLinkOSH('contained', vappOsh, node))

    def __createNodeTopology(self, node, vm, vector):
        networks = vm.getNetworkConnections()
        for network in networks:
            address = network.getIpAddress()
            vif_mac = network.getMACAddress()
            if address:
                ipOsh = modeling.createIpOSH(address)
                ipAndNodeLink = modeling.createLinkOSH('contained', node, ipOsh)
                vector.add(ipOsh)
                vector.add(ipAndNodeLink)
            if vif_mac:
                inf_osh = modeling.createInterfaceOSH(vif_mac, node)
                vector.add(inf_osh)


class MyFilterForReportDeleteCI(object):
    def __init__(self, Framework):
        super(MyFilterForReportDeleteCI, self).__init__()
        reportDeleteCI = Framework.getParameter('AcceptDeleteNodeEvent')
        reportDeleteCI = reportDeleteCI or reportDeleteCI.strip()
        self.reportDeleteCI = reportDeleteCI == 'true'

    def filter(self, eventCube):
        event_type = eventCube.event['type']
        return self.reportDeleteCI or event_type != 'com/vmware/vcloud/event/vm/delete'


def DiscoveryMain(Framework):
    logger.info('vCloud event monitor started.')
    import sys
    if sys.version_info < (2, 7):
        msg = 'Jython 2.7 is required. Current jython version: %s. The job will stop running.' % sys.version
        logger.warn(msg)
        logger.reportWarning(msg)
        return
    eventHub = event_hub.EventHub(Framework)
    eventHub.shutdownMonitor = lambda: not Framework.isExecutionActive()
    vCloudWrapper = VCloudWrapper(Framework)
    try:
        vCloudWrapper.login()
        config = vCloudWrapper.get_amqp_settings()
        if not config:
            logger.warn('No proper AMQP configuration for the vCloud')
            logger.reportWarning('No proper AMQP configuration for the vCloud.')
            return
        vCloudWrapper.keepAlive()
        eventHub.source(MySource(eventHub, config))
        eventHub.filter(MyFilterForReportDeleteCI(Framework))
        eventHub.handler(MyHandler(Framework, vCloudWrapper, eventHub))
        eventHub.start()
    finally:
        try:
            vCloudWrapper.logout()
        except:
            logger.debugException('')
        logger.info('Job stopped.')


class VCloudWrapper(object):
    def __init__(self, Framework):
        super(VCloudWrapper, self).__init__()
        self.vcloudClient = None
        self.Framework = Framework
        self.isAlive = False
        self.vcloud_url = Framework.getDestinationAttribute('url')
        self.vcloud_credential = Framework.getDestinationAttribute('vcloud_credential')
        logger.info('vcloud url:', self.vcloud_url)
        if not self.vcloud_credential:
            logger.warn('No vcloud credential.')
            raise Exception('No vcloud credential.')

    def get_vm(self, entity_id):
        logger.debug('Get vm by id:', entity_id)
        from com.vmware.vcloud.sdk import VM
        vc = self.vcloudClient
        try:
            return VM.getVMById(vc, entity_id)
        except:
            logger.debugException('Failed to get vm by id:', entity_id)
            return None

    def get_vapp(self, vapp_ref):
        from com.vmware.vcloud.sdk import Vapp
        vapp = Vapp.getVappByReference(self.vcloudClient, vapp_ref)
        return vapp

    def get_vdc(self, vdc_ref):
        from com.vmware.vcloud.sdk import Vdc
        return Vdc.getVdcByReference(self.vcloudClient, vdc_ref)

    def get_org(self, org_ref):
        from com.vmware.vcloud.sdk import Organization
        return Organization.getOrganizationByReference(self.vcloudClient, org_ref)

    def keepAlive(self):
        refresh_interval = self.Framework.getParameter('RefreshSessionInterval')
        if refresh_interval:
            refresh_interval = int(refresh_interval)
        else:
            refresh_interval = 600

        logger.debug('Refresh interval:', refresh_interval)

        def refresh_session():
            while self.Framework.isExecutionActive() and self.isAlive:
                logger.debug('Refresh vcloud client session')
                vcloudClient = self.vcloudClient
                from com.vmware.vcloud.sdk import VCloudException
                try:
                    result = vcloudClient.getUpdatedOrgList()
                    logger.debug('Done refresh vcloud client session', result)
                except VCloudException, e:
                    if 'Access is forbidden' in e.getMessage():
                        logger.debug('vCloud session expired. Need login again.')
                        try:
                            if self.Framework.isExecutionActive():
                                self._login()
                        except:
                            logger.debugException('vCloud login failed.')
                except:
                    logger.debugException('Failed to refresh session')
                import time
                time.sleep(refresh_interval)

        from threading import Thread
        rt = Thread(target=refresh_session, name='vCloud Session Refresh Thread')
        rt.setDaemon(True)
        rt.start()

    def _login(self):
        Framework = self.Framework
        logger.debug('Login to vCloud')
        try:
            from com.hp.ucmdb.discovery.common import CollectorsConstants
            from java.util import Properties

            props = Properties()
            props.setProperty(CollectorsConstants.ATTR_CREDENTIALS_ID, self.vcloud_credential)
            props.setProperty('protocol_version', "1.5")
            props.setProperty('base_url', self.vcloud_url)
            client = Framework.createClient(props)
            agent = client.getAgent()
            vc = agent.getVcloudClient()
            logger.debug('Successfully login to vCloud:', vc)
            self.vcloudClient = vc
            return vc
        except:
            logger.debugException('Failed to connect to vCloud')

    def login(self):
        vc = self._login()
        if not vc:
            raise Exception('Failed to connect to vCloud')
        self.isAlive = True

    def logout(self):
        logger.info('Logout vCloud')
        self.isAlive = False
        if self.vcloudClient:
            self.vcloudClient.logout()

    def get_amqp_settings(self):
        vcloudClient = self.vcloudClient
        Framework = self.Framework
        if not vcloudClient:
            logger.warn('Can not connect to vcloud')
            return
        from com.vmware.vcloud.sdk.admin.extensions import VcloudAdminExtensionSettings

        if not VcloudAdminExtensionSettings.isNotificationsEnabled(vcloudClient):
            logger.warn('The vCloud does not enable notification')
            return
        amqp = VcloudAdminExtensionSettings.getAmqpSettings(vcloudClient)
        host = amqp.getAmqpHost()
        port = amqp.getAmqpPort()
        username = amqp.getAmqpUsername()
        exchange = amqp.getAmqpExchange()
        virtual_host = amqp.getAmqpVHost()
        use_ssl = amqp.isAmqpUseSSL()
        routing_key = 'true.#.com.vmware.vcloud.event.vm.#'
        logger.debug('host:', host)
        logger.debug('username:', username)
        logger.debug('exchange:', exchange)
        logger.debug('routing_key:', routing_key)
        logger.debug('virtual_host:', virtual_host)
        logger.debug('isAmqpUseSSL:', use_ssl)

        credential_candidates = netutils.getAvailableProtocols(Framework, 'amqp', None)

        def filter_by_username(credential):
            protocol_username = Framework.getProtocolProperty(credential, 'protocol_username')
            return protocol_username == username

        credential_candidates = filter(filter_by_username, credential_candidates)
        if not credential_candidates:
            logger.warn('No valid credential for AMQP server')
            return
        for credential in credential_candidates:
            try:
                return amqp_event_source.amqp_connect(Framework, host, port, credential, exchange, routing_key,
                                                      use_ssl=use_ssl, virtual_host=virtual_host)
            except:
                logger.debugException('Failed to connect.')

        logger.warn('No proper credential to connect AMQP server.')
        logger.reportWarning('No proper credential to connect AMQP server.')
