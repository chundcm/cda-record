#coding=utf-8

import logger
import google_cloud_restful_client
from check_credential import Result, connect

def _googleCloudConnect(credentialsId, ipAddress, framework):
    proxies = {}
    keyFile = framework.getProtocolProperty(credentialsId, "key_file", "")
    proxy_host = framework.getProtocolProperty(credentialsId, "proxy_host", "")
    proxy_port = framework.getProtocolProperty(credentialsId, "proxy_port", "")

    if proxy_host and proxy_port:
        proxy = 'http://' + proxy_host + ':' + proxy_port
        proxies['http'] = proxy
        proxies['https'] = proxy
    gcloudClient = google_cloud_restful_client.GoogleCloudClient(keyFile, proxies)
    try:
        errorMessage, projectId = gcloudClient.authorize()
    except:
        errorMessage = logger.prepareFullStackTrace('')
        return Result(False, errorMessage)

    if errorMessage:
        warning = 'Failed to get access token for project %s with credential: %s. \nError is %s'\
                  % (projectId, credentialsId, errorMessage)
        return Result(False, warning)
    else:
        return Result(True)


def DiscoveryMain(framework):
    return connect(framework, checkConnectFn=_googleCloudConnect)
