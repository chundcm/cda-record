# coding=utf-8
__author__ = 'Kane'
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector

import logger
import modeling
import netutils


def DiscoveryMain(Framework):
    import sys
    if sys.version_info < (2, 7):
        msg = 'Jython 2.7 is required. Current jython version: %s. The job will stop running.' % sys.version
        logger.warn(msg)
        logger.reportWarning(msg)
        return
    consumer = Framework.getParameter('Consumer')
    exchange = Framework.getParameter('Exchange')
    routing_key = Framework.getParameter('RoutingKey')
    ip_address = Framework.getTriggerCIData('ip_address')
    credential_candidates = netutils.getAvailableProtocols(Framework, 'amqp', ip_address)
    if not ip_address:
        logger.reportWarning('AMQP host should not be empty')
        return
    vector = connect(Framework, consumer, ip_address, None, credential_candidates, exchange, routing_key)
    if not vector:
        msg = 'No valid connection to host:' + ip_address
        logger.warnException(msg)
        logger.reportWarning(msg)
    return vector


def connect(Framework, consumer, host, port, credential_candidates, exchange, routing_key):
    import amqp_event_source
    for credential in credential_candidates:
        try:
            setting = amqp_event_source.amqp_connect(Framework, host, port, credential, exchange, routing_key)
            if 'port' in setting:
                port = setting['port']
        except:
            logger.debugException('Failed to connect.')
        else:
            logger.debug('Connected. Report CIs.')
            vector = ObjectStateHolderVector()
            host_osh = modeling.createHostOSH(host)
            mq_osh = ObjectStateHolder('messaging_server')
            mq_osh.setStringAttribute('discovered_product_name', 'AMQP')
            mq_osh.setStringAttribute('credentials_id', credential)
            mq_osh.setStringAttribute('application_ip', host)
            mq_osh.setStringAttribute('data_note', consumer)
            if port:
                mq_osh.setIntegerAttribute('application_port', port)
            mq_osh.setContainer(host_osh)
            vector.add(mq_osh)
            vector.add(host_osh)
            return vector
