# coding=utf-8
__author__ = 'Kane'
from Queue import Queue, Empty
from threading import Thread, currentThread
import time
import types
import logger

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector


class EventCube(object):
    def __init__(self, event):
        super(EventCube, self).__init__()
        self.event = event
        self.context = {}


TIMEOUT_FOR_QUEUE = 10  # seconds


class EventHub(object):
    def __init__(self, Framework, maxQueueSize=100, maxThreads=2, monitorQueue=False, shutdownMonitor=None,
                 discardWhenQueueFull=True):
        super(EventHub, self).__init__()
        self.workingQueue = Queue(maxQueueSize)
        self.eventSourceClasses = []
        self.eventSources = []
        self.eventFilterClasses = []
        self.eventFilters = []
        self.eventHandlerClasses = []
        self.eventHandlers = []
        self.isActive = False
        self.processedCount = 0
        self.monitorQueue = monitorQueue
        self.shutdownMonitor = shutdownMonitor
        self.discardWhenQueueFull = discardWhenQueueFull
        self.workers = set()
        self.Framework = Framework

        from com.hp.ucmdb.discovery.library.communication.downloader.cfgfiles import GeneralSettingsConfigFile
        useMultiThread = GeneralSettingsConfigFile.getInstance().getPropertyBooleanValue('useMultiThreadForEventHub', True)
        logger.debug("Use multi-thread in EventHub: ", useMultiThread)
        if useMultiThread:
            self.maxPoolSize = maxThreads
        else:
            self.maxPoolSize = 1
        from java.lang import NoSuchMethodException
        try:
            FClass = Framework.getClass()
            FClass.getMethod('flushObjects', [ObjectStateHolderVector, ObjectStateHolderVector])
            # New flushObject method. Caution: this method should be used without calling sendObject method before.
            # public void flushObjects(ObjectStateHolderVector objectForAddOrUpdate, ObjectStateHolderVector objectForDelete)
            self.useNewFlush = True
        except NoSuchMethodException:
            msg = 'LIMITATION: You are running event-based discovery on a UCMDB which does not support multi-thread in ' \
                  'single job. This may cause missing of events. \r\nUpdate to UCMDB 10.31 or later version to avoid this. ' \
                  '\r\nOr set useMultiThreadForEventHub to false in globalsetting.xml as a workaround. ' \
                  'But the performance may not be satisfying'
            logger.warn(msg)
            self.useNewFlush = False

    def init(self):
        logger.info('Init event hub.')
        map(lambda x: self.eventSources.append(x(self)), self.eventSourceClasses)
        map(lambda x: self.eventFilters.append(x()), self.eventFilterClasses)
        map(lambda x: self.eventHandlers.append(x()), self.eventHandlerClasses)
        logger.info("eventSources", self.eventSources)
        logger.info("eventFilters", self.eventFilters)
        logger.info("eventHandlers", self.eventHandlers)

    def _startSource(self):
        logger.info('Start source.')
        for sourceObj in self.eventSources:
            logger.info('Start source', sourceObj)
            sourceObj.start()

    def send(self, event):
        if not event:
            return
        if self.isActive:
            if self.discardWhenQueueFull and self.workingQueue.full():
                return
            self.workingQueue.put(event)
            if not self.workingQueue.empty() and len(self.workers) < self.maxPoolSize:
                self.addWorker()

    def addWorker(self):
        t = Thread(target=self._doWork)
        t.setDaemon(True)
        logger.debug('Current thread name:' + currentThread().getName())
        t.setName(currentThread().getName() + ':Event Hub Worker:' + t.getName())
        t.start()
        self.workers.add(t)
        logger.info('Add a worker:', self.getRuntimeInfo())

    def start(self, waitUntilStop=False):
        self.init()
        logger.info('Start event hub.')

        self.isActive = True
        if self.monitorQueue:
            self._monitorQueueThread()

        if self.shutdownMonitor:
            self._shutdownMonitorThread(waitUntilStop)
        self._startSource()

    def shutdown(self):
        logger.info('Shutdown event hub.')
        self.isActive = False
        for eventSource in self.eventSources:
            try:
                logger.info('Begin stop source...', eventSource)
                eventSource.stop()
            except:
                logger.debugException("Failed to stop source", eventSource)
            else:
                logger.info('Source stopped')

    def source(self, eventSource):
        if isinstance(eventSource, types.TypeType):
            self.eventSourceClasses.append(eventSource)
            return eventSource
        else:
            self.eventSources.append(eventSource)

    def filter(self, eventFilter):
        if isinstance(eventFilter, types.TypeType):
            self.eventFilterClasses.append(eventFilter)
            return eventFilter
        else:
            self.eventFilters.append(eventFilter)

    def handler(self, eventHandler):
        if isinstance(eventHandler, types.TypeType):
            self.eventHandlerClasses.append(eventHandler)
            return eventHandler
        else:
            self.eventHandlers.append(eventHandler)

    def _handle_event(self, event):
        event = EventCube(event)
        for ef in self.eventFilters:
            try:
                if not ef.filter(event):
                    return
            except:
                logger.debugException('Failed to filter event:', event)
                return

        for eh in self.eventHandlers:
            if eh.isApplicable(event):
                try:
                    eh.handle(event)
                except:
                    logger.debugException('Failed to handle event:', event)

    def getTask(self):
        workerCount = len(self.workers)
        if workerCount > self.maxPoolSize:
            return None
        elif self.workingQueue.empty():
            try:
                return self.workingQueue.get(timeout=TIMEOUT_FOR_QUEUE)
            except Empty:
                return None
        else:
            event = self.workingQueue.get()
            return event

    def _doWork(self):
        try:
            while self.isActive:
                event = self.getTask()
                if event is None:
                    break
                self.processedCount += 1
                if event:
                    try:
                        self._handle_event(event)
                    except:
                        logger.debugException('Failed to handle event', event)
        finally:
            self.workers.remove(currentThread())

    def getRuntimeInfo(self):
        msg = 'Thread Pool:%s/%s, Queue: %s/%s' % (
            len(self.workers),
            self.maxPoolSize,
            self.workingQueue._qsize(),
            self.workingQueue.maxsize)
        return msg

    def _monitorQueueThread(self):
        def m_queue():
            lastSeconds = self.processedCount
            while self.isActive:
                time.sleep(10)
                nowProcessed = self.processedCount
                delta = nowProcessed - lastSeconds
                lastSeconds = nowProcessed
                logger.debug(self.getRuntimeInfo())
                logger.debug("Process speed:%s/sec" % delta)

        logger.debug('Current thread name:' + currentThread().getName())
        t = Thread(target=m_queue, name=currentThread().getName() + ':Event Hub Queue Monitor')
        t.setDaemon(True)
        t.start()

    def _shutdownMonitorThread(self, waitUntilStop):
        if waitUntilStop:
            self._waitUntilStop()
        else:
            logger.debug('Current thread name:' + currentThread().getName())
            t = Thread(target=self._waitUntilStop, name=currentThread().getName() + ':Event Hub Shutdown Monitor')
            t.setDaemon(True)
            t.start()

    def _waitUntilStop(self):
        logger.info('Wait for stop...')
        while self.isActive:
            time.sleep(5)
            try:
                isShutdown = self.shutdownMonitor()
                if isShutdown:
                    logger.info('Shutdown event hub by monitor')
                    self.shutdown()
            except:
                pass

    def sendAndFlushObjects(self, object):
        if isinstance(object, ObjectStateHolder):
            if self.useNewFlush:
                vector = ObjectStateHolderVector()
                vector.add(object)
                self.Framework.flushObjects(vector, ObjectStateHolderVector())
            else:
                self.Framework.sendObject(object)
                self.Framework.flushObjects()
        elif isinstance(object, ObjectStateHolderVector):
            if self.useNewFlush:
                self.Framework.flushObjects(object, ObjectStateHolderVector())
            else:
                self.Framework.sendObjects(object)
                self.Framework.flushObjects()
        else:
            raise 'Wrong type to flush. Should be ObjectStateHolder or ObjectStateHolderVector'

    def deleteAndFlushObjects(self, object):
        if isinstance(object, ObjectStateHolder):
            if self.useNewFlush:
                vector = ObjectStateHolderVector()
                vector.add(object)
                self.Framework.flushObjects(ObjectStateHolderVector(), vector)
            else:
                self.Framework.deleteObject(object)
                self.Framework.flushObjects()
        elif isinstance(object, ObjectStateHolderVector):
            if self.useNewFlush:
                self.Framework.flushObjects(ObjectStateHolderVector(), object)
            else:
                self.Framework.deleteObjects(object)
                self.Framework.flushObjects()
        else:
            raise 'Wrong type to flush. Should be ObjectStateHolder or ObjectStateHolderVector'
