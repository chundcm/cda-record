# coding=utf-8

from check_credential import connect, Result


def DiscoveryMain(framework):
    required_protocol_attribute = ('protocol_username', 'consumer_key', 'is_sandbox')
    return connect(framework, salesforce_connect, required_protocol_attribute)


def salesforce_connect(credentialsId, ipAddress, framework):
    from remedyforce_connection_manager import FrameworkBasedConnectionDataManager
    con_mgr = FrameworkBasedConnectionDataManager(framework)
    con_mgr.getClient()
    return Result(True)
