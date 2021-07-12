# coding=utf-8
__author__ = 'Kane'

import sys
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector

import event_hub
import logger
import modeling
import time


class Cache(object):
    def __init__(self, maxSize=-1, maxKeepTime=-1):
        super(Cache, self).__init__()
        self.data = {}
        self.maxSize = maxSize
        self.maxKeepTime = maxKeepTime

    def __sizeExceed(self):
        return not self.maxSize == -1 and len(self.data) >= self.maxSize

    def put(self, name, value):
        if self.__sizeExceed():
            self.__purge_old_items()

        if self.__sizeExceed():
            self.__purge_oldest()
        if not self.__sizeExceed():
            self.data[name] = (time.time(), value)

    def get(self, name):
        value = self.data.get(name)
        if value:
            return value[1]

    def __contains__(self, item):
        return item in self.data

    def pop(self, name):
        if name in self.data:
            return self.data.pop(name)[1]

    def remove(self, name):
        del self.data[name]

    def __purge_old_items(self):
        now = time.time()
        names = self.data.keys()
        for name in names:
            timestamp, _ = self.data[name]
            if timestamp + self.maxKeepTime < now:
                del self.data[name]

    def __purge_oldest(self):
        names = self.data.keys()
        oldest = 0
        oldest_name = None

        for name in names:
            timestamp, _ = self.data[name]
            if timestamp < oldest or oldest == 0:
                oldest_name = name
                oldest = timestamp
        if oldest_name:
            self.remove(oldest_name)


def DiscoveryMain(Framework):
    logger.info('Job started')
    if sys.version_info < (2, 7):
        msg = 'Jython 2.7 is required. Current jython version: %s. The job will stop running.' % sys.version
        logger.warn(msg)
        logger.reportWarning(msg)
        return
    config = get_mq_config(Framework)
    eventHub = event_hub.EventHub(Framework)

    @eventHub.source
    class MySource(object):
        def __init__(self, hub):
            super(MySource, self).__init__()
            self.eventHub = hub
            logger.info('Init source')
            self.count = 0
            self.isAlive = True
            self.eventSource = None

        def consumer(self, message):
            import json
            raw_message_body = json.loads(message.body)
            if 'oslo.message' in raw_message_body:
                oslo_message = raw_message_body['oslo.message']
                oslo_message_body = json.loads(oslo_message)
                if 'event_type' in oslo_message_body:
                    event_type = oslo_message_body['event_type']
                    logger.debug('Insert message to queue:', event_type)
                    self.eventHub.send(oslo_message_body)

        def start(self):
            if self.isAlive:
                logger.info('Start open stack source pull...')
                from amqp_event_source import AMQPEventSource
                self.eventSource = AMQPEventSource(config)
                try:
                    self.eventSource.connect()
                    self.eventSource.start(self.consumer)
                except:
                    logger.debugException('Failed to start source')
                    raise

        def stop(self):
            logger.debug('Stop source')
            self.isAlive = False
            if self.eventSource:
                self.eventSource.stop()

        def __repr__(self):
            return 'Open stack event pulling'

    @eventHub.filter
    class MyFilter(object):
        EVENT_TYPES = ['compute.instance.create.end', 'compute.instance.delete.end',
                       'compute.instance.resize.start', 'compute.instance.finish_resize.end',
                       'compute.instance.live_migration.post.dest.start',
                       'compute.instance.live_migration.post.dest.end',
                       ]

        def filter(self, eventCube):
            event_type = eventCube.event['event_type']
            result = event_type in self.EVENT_TYPES
            logger.debug('Filter event type:', '%s:%s' % (event_type, result))
            return result

    @eventHub.filter
    class MyFilterForReportDeleteCI(object):
        def __init__(self):
            super(MyFilterForReportDeleteCI, self).__init__()
            reportDeleteCI = Framework.getParameter('AcceptDeleteNodeEvent')
            reportDeleteCI = reportDeleteCI or reportDeleteCI.strip()
            self.reportDeleteCI = reportDeleteCI == 'true'

        def filter(self, eventCube):
            event_type = eventCube.event['event_type']
            return self.reportDeleteCI or event_type != 'compute.instance.delete.end'

    @eventHub.handler
    class MyHandler(object):
        def __init__(self):
            super(MyHandler, self).__init__()
            self.vm_states = Cache(100, 60)

        def isApplicable(self, eventCube):
            return True

        def handle(self, eventCube):
            logger.debug('handle event %s' % eventCube.event)
            event = eventCube.event
            event_type = event['event_type']
            logger.debug('Event type:', event_type)
            payload = event['payload']

            def get_vm_id(payload):
                return payload['instance_id']

            def get_vm_name(payload):
                return payload['hostname']

            def get_hypervisor_name(payload):
                return payload['node']

            def create_vm_node(payload):
                name = get_vm_name(payload)
                node = ObjectStateHolder('host_node')
                node.setStringAttribute('name', name)
                return node

            def create_hypervisor(payload):
                nova_compute_host_name = payload['host']
                hypervisor_name = payload['node']
                nova_compute_host = ObjectStateHolder('host_node')
                nova_compute_host.setAttribute("name", nova_compute_host_name)

                hypervisorOsh = ObjectStateHolder('virtualization_layer')
                hypervisorOsh.setStringAttribute('discovered_product_name', 'openstack_hypervisor')
                hypervisorOsh.setAttribute('hypervisor_name', hypervisor_name)
                hypervisorOsh.setContainer(nova_compute_host)
                return hypervisorOsh, nova_compute_host

            def get_flavor_id(payload):
                return payload['instance_flavor_id']

            def create_flavor_osh(payload):
                instance_flavor_id = payload['instance_flavor_id']
                instance_type = payload['instance_type']
                if instance_flavor_id:
                    flavorOsh = ObjectStateHolder("openstack_flavor")
                    flavorOsh.setAttribute("flavor_id", instance_flavor_id)
                    flavorOsh.setAttribute("name", instance_type)
                    return flavorOsh

            def __createFlavor(node, payload, vector):
                flavorOsh = create_flavor_osh(payload)
                if flavorOsh:
                    vector.add(flavorOsh)
                    vector.add(modeling.createLinkOSH('dependency', node, flavorOsh))

            def __createRelationshipWithOpenstack(node, payload, vector):
                hypervisorOsh, container = create_hypervisor(payload)
                vector.add(container)
                vector.add(hypervisorOsh)
                vector.add(modeling.createLinkOSH('execution_environment', hypervisorOsh, node))

            if event_type == 'compute.instance.create.end':
                hostname = payload['hostname']
                instance_id = payload['instance_id']
                logger.debug('Create host:', hostname)
                vector = ObjectStateHolderVector()
                node = ObjectStateHolder('host_node')
                node.setStringAttribute('name', hostname)
                node.setStringAttribute('data_note', 'OpenStack:' + instance_id)
                vector.add(node)
                self.__createNodeTopology(node, payload, vector)
                self.__createImage(node, payload, vector)
                __createFlavor(node, payload, vector)
                __createRelationshipWithOpenstack(node, payload, vector)

                eventHub.sendAndFlushObjects(vector)
                logger.debug('Added CIs', vector)
            elif event_type == 'compute.instance.delete.end':
                hostname = payload['hostname']
                logger.debug('Delete host:', hostname)
                node = ObjectStateHolder('host_node')
                node.setStringAttribute('name', hostname)
                eventHub.deleteAndFlushObjects(node)
                logger.debug('Deleted CI', node)
            elif event_type == 'compute.instance.resize.start' or event_type == 'compute.instance.live_migration.post.dest.start':
                vm_id = get_vm_id(payload)
                self.vm_states.put(vm_id, payload)
            elif event_type == 'compute.instance.finish_resize.end' or event_type == 'compute.instance.live_migration.post.dest.end':
                vm_id = get_vm_id(payload)
                if vm_id in self.vm_states:
                    old_payload = self.vm_states.pop(vm_id)
                    old_hypervisor_name = get_hypervisor_name(old_payload)
                    new_hypervisor_name = get_hypervisor_name(payload)
                    node = create_vm_node(payload)
                    vm_name = get_vm_name(payload)
                    logger.debug('old hypervisor:', old_hypervisor_name)
                    logger.debug('new hypervisor:', new_hypervisor_name)
                    if old_hypervisor_name != new_hypervisor_name:
                        logger.debug('Remove link between hypervisor %s and node %s:' % (old_hypervisor_name, vm_name))
                        logger.debug('Add link between  hypervisor %s and node %s:' % (new_hypervisor_name, vm_name))
                        old_hypervisorOsh, _ = create_hypervisor(old_payload)
                        new_hypervisorOsh, _ = create_hypervisor(payload)
                        old_link = modeling.createLinkOSH('execution_environment', old_hypervisorOsh, node)
                        new_link = modeling.createLinkOSH('execution_environment', new_hypervisorOsh, node)
                        eventHub.deleteAndFlushObjects(old_link)
                        eventHub.sendAndFlushObjects(new_link)
                        logger.debug('Link deleted', old_link)
                        logger.debug('Link added', new_link)

                    old_flavor_id = get_flavor_id(old_payload)
                    new_flavor_id = get_flavor_id(payload)
                    logger.debug('old flavor:', old_flavor_id)
                    logger.debug('new flavor:', new_flavor_id)
                    if old_flavor_id != new_flavor_id:
                        logger.debug('Remove link between flavor %s and node %s:' % (old_flavor_id, vm_name))
                        logger.debug('Add link between  flavor %s and node %s:' % (new_flavor_id, vm_name))
                        old_flavor = create_flavor_osh(old_payload)
                        new_flavor = create_flavor_osh(payload)
                        old_link = modeling.createLinkOSH('dependency', node, old_flavor)
                        new_link = modeling.createLinkOSH('dependency', node, new_flavor)
                        eventHub.deleteAndFlushObjects(old_link)
                        eventHub.sendAndFlushObjects(new_link)

        def __createNodeTopology(self, node, payload, vector):
            fixed_ips = payload['fixed_ips']
            if fixed_ips:
                for fixed_ip in fixed_ips:
                    address = fixed_ip['address']
                    vif_mac = fixed_ip['vif_mac']
                    if address:
                        ipOsh = modeling.createIpOSH(address)
                        ipAndNodeLink = modeling.createLinkOSH('contained', node, ipOsh)
                        vector.add(ipOsh)
                        vector.add(ipAndNodeLink)
                    if vif_mac:
                        inf_osh = modeling.createInterfaceOSH(vif_mac, node)
                        vector.add(inf_osh)

        def __createImage(self, node, payload, vector):
            image_data = payload['image_meta']
            if image_data:
                image_id = image_data['base_image_ref']
                if image_id:
                    disk_format = image_data['disk_format']
                    image_osh = ObjectStateHolder('openstack_image')
                    image_osh.setStringAttribute('image_id', image_id)
                    image_osh.setStringAttribute('disk_format', disk_format)
                    vector.add(image_osh)
                    vector.add(modeling.createLinkOSH('dependency', node, image_osh))

    def shutdownMonitor():
        return not Framework.isExecutionActive()

    eventHub.shutdownMonitor = shutdownMonitor
    try:
        eventHub.start()
    except:
        logger.debugException('')
        logger.reportError('Failed to start event monitor')
    logger.info('Job stopped')


def get_mq_config(Framework):
    host = Framework.getTriggerCIData('ip_address')
    host = host and host.strip()
    if not host:
        raise Exception('No valid host')

    port = Framework.getTriggerCIData('port')
    port = port and port.strip()

    credentialId = Framework.getTriggerCIData('credentialsId')

    if not credentialId:
        raise Exception('No valid credential')
    username = Framework.getProtocolProperty(credentialId, 'protocol_username')
    password = Framework.getProtocolProperty(credentialId, 'protocol_password')
    if not username or not password:
        raise Exception('Invalid username or password')

    timeout = Framework.getProtocolProperty(credentialId, 'protocol_timeout')
    timeout = int(timeout) / 1000.0  # convert milliseconds to seconds for AMQP protocol
    use_ssl = Framework.getProtocolProperty(credentialId, 'use_ssl') == 'true'
    virtual_host = Framework.getProtocolProperty(credentialId, 'virtual_host')

    config = {
        'exchange': 'nova',
        'routing_key': 'notifications.info',
        'hostname': host,
        'ssl': use_ssl,
        'virtual_host': virtual_host,
        'connect_timeout': timeout,
        'userid': username,
        'password': password
    }

    if port:
        config['port'] = port

    return config
