# coding=utf-8
__author__ = 'Kane'

import logger
from kombu import BrokerConnection
from kombu import Exchange
from kombu import Queue
from kombu.mixins import ConsumerMixin
from contextlib import contextmanager
import time


class TrustAllCert(object):
    SSL_INITED = False
    TRUST_CONTEXT = None
    DEFAULT_CONTEXT = None

    @classmethod
    def initSSL(cls):
        if cls.SSL_INITED:
            return
        from com.hp.ucmdb.discovery.library.clients import SSLContextManager
        from javax.net.ssl import SSLContext

        # Keep a static reference to the JVM's default SSLContext for restoring at a later time
        cls.DEFAULT_CONTEXT = SSLContext.getDefault()
        cls.TRUST_CONTEXT = SSLContextManager.getAutoAcceptSSLContext()
        cls.SSL_INITED = True

    @classmethod
    def enableTrustAllCertificates(cls, enable=True):
        cls.initSSL()
        from javax.net.ssl import SSLContext
        if enable and TrustAllCert.TRUST_CONTEXT:
            SSLContext.setDefault(TrustAllCert.TRUST_CONTEXT)
        elif TrustAllCert.DEFAULT_CONTEXT:
            SSLContext.setDefault(TrustAllCert.DEFAULT_CONTEXT)


@contextmanager
def trustAllCert(enable=True):
    if enable:
        try:
            TrustAllCert.enableTrustAllCertificates(True)
            yield
        finally:
            TrustAllCert.enableTrustAllCertificates(False)
    else:
        yield


class MyExchange(Exchange):
    def __init__(self, name='', type='', channel=None, **kwargs):
        super(MyExchange, self).__init__(name, type, channel, **kwargs)

    def declare(self, nowait=False, passive=None):
        pass


class AMQPEventSource(object):
    def __init__(self, config):
        super(AMQPEventSource, self).__init__()
        self.conn = None
        self.config = config
        self.event_consumer = None

    def connect(self):
        exchange = self.config.get('exchange')
        routing_key = self.config.get('routing_key')
        if not routing_key:
            routing_key = ''
        logger.info('Exchange:', exchange)
        logger.info('Routing key:', routing_key)
        mq_host = self.config['hostname']
        mq_settings = {'connect_timeout': 3, 'heartbeat': 60}
        setting_keys = ['hostname', 'port', 'userid', 'password', 'connect_timeout', 'ssl', 'virtual_host']

        for key in setting_keys:
            if key in self.config:
                mq_settings[key] = self.config[key]

        try:
            logger.info('Connect to server:', mq_host)
            conn = BrokerConnection(**mq_settings)
            mq_settings['password'] = '******'
            self.config['password'] = '******'
            logger.info('Config for mq:', self.config)
            logger.info('Setting for mq', mq_settings)
            self.ssl = 'ssl' in mq_settings and mq_settings['ssl']
            with trustAllCert(self.ssl):
                conn.connect()
        except Exception, e:
            logger.debugException('Failed to connect')
            raise e
        else:
            self.conn = conn

    def start(self, consumer):
        exchange_name = self.config.get('exchange')
        if not exchange_name:
            raise Exception('No valid exchange')
        routing_key = self.config.get('routing_key')

        class EventConsumer(ConsumerMixin):
            def __init__(self, connection):
                self.connection = connection
                self.last_status_report = time.time()

            def get_consumers(self, consumer, channel):
                exchange = MyExchange(exchange_name)
                queue = Queue(exchange=exchange, routing_key=routing_key, no_ack=True, exclusive=True)
                return [consumer(queue, callbacks=[self.on_message])]

            def on_message(self, body, message):
                consumer(message)

            def on_connection_revived(self):
                logger.info('Connection established.')

            def on_connection_error(self, exc, interval):
                if self.should_stop:
                    raise Exception('Should stop.')
                logger.error('Connection error. Reconnect in %d seconds. Reason:' % interval, exc)

            def on_iteration(self):
                now = time.time()
                if now - self.last_status_report > 60:
                    self.last_status_report = now
                    logger.debug('AMQP connection status check.')

        with trustAllCert(self.ssl):
            with self.conn as connection:
                self.event_consumer = EventConsumer(connection)
                logger.info('Begin drain events...')
                try:
                    self.event_consumer.run()
                except Exception as e:
                    logger.debug('Event source stopped:', e)
                logger.debug('Event source has been stopped.')

    def stop(self, force=False):
        if self.event_consumer:
            logger.info('Set event consumer to stop')
            self.event_consumer.should_stop = True
        if self.conn and force:
            self.conn.release()


def amqp_connect(Framework, host, port, credential, exchange, routing_key, use_ssl=None, virtual_host=None):
    logger.info('Exchange:', exchange)
    logger.info('Routing key:', routing_key)
    if credential:
        username = Framework.getProtocolProperty(credential, 'protocol_username')
        password = Framework.getProtocolProperty(credential, 'protocol_password')
        if not port:
            port = int(Framework.getProtocolProperty(credential, 'protocol_port'))
        if not username or not password:
            raise Exception('Invalid user name or password')
        timeout = Framework.getProtocolProperty(credential, 'protocol_timeout')
        timeout = int(timeout) / 1000.0  # convert milliseconds to seconds for AMQP protocol

        if use_ssl is None:
            use_ssl = Framework.getProtocolProperty(credential, 'use_ssl') == 'true'

        if virtual_host is None:
            virtual_host = Framework.getProtocolProperty(credential, 'virtual_host')

        mq_settings = {
            'exchange': exchange,
            'routing_key': routing_key,
            'hostname': host,
            'port': port,
            'ssl': use_ssl,
            'virtual_host': virtual_host,
            'connect_timeout': timeout,
            'userid': username,
            'password': password
        }
        source = AMQPEventSource(mq_settings.copy())
        try:
            logger.info('Connect to server %s by credential %s' % (host, credential))
            source.connect()
            return mq_settings
        finally:
            logger.debug('Close channel and connection.')
            source.stop()
