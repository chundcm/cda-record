# coding=utf-8
import sys
import time
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector

import event_hub
import logger
import modeling
import docker_restful_client
from json_stream import JsonStreamWrapper
from java.net import SocketTimeoutException


def DiscoveryMain(Framework):
    logger.info('Job started')
    eventHub = event_hub.EventHub(Framework)
    credentialId = Framework.getDestinationAttribute('credentialId')
    endpoint = Framework.getDestinationAttribute('endpoint')

    if not credentialId:
        dockerClient = docker_restful_client.DockerClient(endpoint, None)
    else:
        keyStorePath = ''
        keyStorePass = ''
        keyPass = ''
        try:
            keyStorePath = Framework.getProtocolProperty(credentialId, 'keyStorePath')
            keyStorePass = Framework.getProtocolProperty(credentialId, 'keyStorePass')
            keyPass = Framework.getProtocolProperty(credentialId, 'keyPass')
        except:
            pass
        useCredential = docker_restful_client.DockerCredential(keyStorePath, keyStorePass, keyPass)
        dockerClient = docker_restful_client.DockerClient(endpoint, useCredential)

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
            logger.info('Start Docker Swarm event source...')
            self.eventSource = DockerEventSource()
            self.eventSource.start(self, dockerClient)

        def stop(self):
            logger.debug('Stop source')
            self.isAlive = False

        def __repr__(self):
            return 'Docker Swarm event pulling'

    @eventHub.filter
    class MyFilter(object):
        EVENT_STATUS = ['start', 'die']

        def filter(self, eventCube):
            event_type = eventCube.event['status']
            result = event_type in self.EVENT_STATUS
            logger.debug('Filter event type:', '%s:%s' % (event_type, result))
            return result

    @eventHub.handler
    class MyHandler(object):
        def isApplicable(self, eventCube):
            return True

        def handle(self, eventCube):
            logger.debug('handle event %s' % eventCube.event)
            event = eventCube.event
            event_type = event['status']
            logger.debug('Event type:', event_type)
            if event_type == 'start':
                vector = ObjectStateHolderVector()
                self.__createContainer(vector, event)
                eventHub.sendAndFlushObjects(vector)
                logger.debug('Added CIs', vector)
            elif event_type == 'die':
                logger.debug('Delete Docker Container:', event['id'])
                nodeOSH = modeling.createHostOSH(event['node']['Ip'])
                nodeOSH.setAttribute('name', event['node']['Name'])
                containerOSH = ObjectStateHolder('docker_container')
                containerOSH.setAttribute('docker_container_id', event['id'])
                containerOSH.setContainer(nodeOSH)
                eventHub.deleteAndFlushObjects(containerOSH)
                logger.debug('Deleted CI', containerOSH)
                logger.debug('Related Node: %s, Ip: %s' % (event['node']['Name'], event['node']['Ip']))

        def __createContainer(self, vector, event):
            nodeOSH = modeling.createHostOSH(event['node']['Ip'])
            nodeOSH.setAttribute('name', event['node']['Name'])
            vector.add(nodeOSH)
            dockerDaemonOSH = ObjectStateHolder('docker_daemon')
            dockerDaemonOSH.setAttribute('name', 'Docker Daemon')
            dockerDaemonOSH.setAttribute('discovered_product_name', 'Docker Daemon')
            dockerDaemonOSH.setContainer(nodeOSH)
            vector.add(dockerDaemonOSH)
            containerOSH = ObjectStateHolder('docker_container')
            containerOSH.setAttribute('docker_container_id', event['id'])
            containerOSH.setContainer(nodeOSH)
            vector.add(containerOSH)
            vector.add(modeling.createLinkOSH('manage', dockerDaemonOSH, containerOSH))

    def shutdownMonitor():
        return not Framework.isExecutionActive()

    eventHub.shutdownMonitor = shutdownMonitor
    try:
        eventHub.start()
    except:
        logger.debugException('')
        logger.reportError('Failed to start event monitor')
    logger.info('Job stopped')

class DockerEventSource():
    def start(self, source, dockerClient):
        self.source = source
        while True:
            if self.source.isAlive:
                self.response = None
                try:
                    self.response = dockerClient.dockerEvents()
                    if self.response.status_code == 200:
                        self.read()
                    else:
                        logger.debug('Failed to connect to swarm. Status code: ', self.response.status_code)
                        self.wait()
                except:
                    logger.debugException('Failed to connect to swarm.')
                    self.wait()
            else:
                break

    def read(self):
        iter_event = JsonStreamWrapper(self.response.iter_content(1))
        try:
            for event in iter_event:
                self.source.eventHub.send(event)
            logger.debug('Connection to swarm lost.')
            self.wait()
        except SocketTimeoutException:
            pass
        except:
            logger.debugException('')

    def wait(self):
        if self.source.isAlive:
            if self.source.sleepTime > self.source.maxSleepTime:
                logger.debug('Wait %s seconds.' % self.source.maxSleepTime)
                time.sleep(self.source.maxSleepTime)
            else:
                logger.debug('Wait %s seconds.' % self.source.sleepTime)
                time.sleep(self.source.sleepTime)
                self.source.sleepTime += 2

