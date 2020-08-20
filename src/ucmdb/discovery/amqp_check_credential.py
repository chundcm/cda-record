# coding=utf-8

from check_credential import connect, Result


def DiscoveryMain(framework):
    required_protocol_attribute = ('protocol_username',)
    return connect(framework, amqp_connect, required_protocol_attribute)


def amqp_connect(credentialsId, ipAddress, framework):
    import amqp_event_source
    amqp_event_source.amqp_connect(framework, ipAddress, None, credentialsId, None, None)
    return Result(True)
