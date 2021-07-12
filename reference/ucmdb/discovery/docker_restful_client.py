# coding=utf-8
import rest_requests as requests
import logger

api_version = 'v1.18'

class DockerCredential(object):
    def __init__(self, keyStorePath, keyStorePass, keyPass):
        super(DockerCredential, self).__init__()
        self.keyStorePath = keyStorePath
        self.keyStorePass = keyStorePass
        self.keyPass = keyPass


class DockerClient(object):
    def __init__(self, apiEndpoint, credential, proxies=None, verify=False):
        super(DockerClient, self).__init__()
        self.apiEndpoint = apiEndpoint
        self.proxies = proxies
        self.verify = verify
        self.headers = {}
        self.credential = credential

    def getIpAddress(self):
        pass

    def getClientType(self):
        return 'http'

    def getResponse(self, url, params=None):
        if self.credential:
            rsp = requests.get(self.apiEndpoint + url, proxies=self.proxies, debug=False, verify=self.verify, params=params,
                           keystore=(self.credential.keyStorePath, self.credential.keyStorePass, self.credential.keyPass))
        else:
            rsp = requests.get(self.apiEndpoint + url, proxies=self.proxies, debug=False, verify=self.verify, params=params, keystore=None)
        logger.debug('Request: ', self.apiEndpoint + url)
        jsonResponse = rsp.json()
        logger.debug('Get Json Response: ', jsonResponse)
        return jsonResponse

    def dockerVersion(self, apiVersion=api_version):
        return self.getResponse('/%s/version' % apiVersion)

    def dockerInfo(self, apiVersion=api_version):
        return self.getResponse('/%s/info' % apiVersion)

    def dockerImages(self, apiVersion=api_version):
        return self.getResponse('/%s/images/json' % apiVersion)

    def dockerImagesOnNode(self, nodeName, apiVersion=api_version):
        return self.getResponse('/%s/images/json' % apiVersion, params={'filters':'{"node":["%s"]}' % nodeName})

    def dockerPs(self, apiVersion=api_version):
        return self.getResponse('/%s/containers/json' % apiVersion)

    def dockerTop(self, containerId, apiVersion=api_version):
        return self.getResponse('/%s/containers/%s/top' % (apiVersion, containerId))

    def dockerInspectImage(self, imageId, apiVersion=api_version):
        return self.getResponse('/%s/images/%s/json' % (apiVersion, imageId))

    def dockerInspectContainer(self, containerId, apiVersion=api_version):
        return self.getResponse('/%s/containers/%s/json' % (apiVersion, containerId))

    def dockerEvents(self, apiVersion=api_version):
        timeout = (0, 30)
        if self.credential:
            rsp = requests.get(self.apiEndpoint + '/%s/events' % apiVersion, proxies=self.proxies, debug=False, verify=self.verify, params=None,
                               keystore=(self.credential.keyStorePath, self.credential.keyStorePass, self.credential.keyPass), stream=True, timeout=timeout)
        else:
            rsp = requests.get(self.apiEndpoint + '/%s/events' % apiVersion, proxies=self.proxies, debug=False,
                               verify=self.verify, params=None, keystore=None, stream=True, timeout=timeout)
        logger.debug('Request: ', self.apiEndpoint + '/%s/events' % apiVersion)
        return rsp


