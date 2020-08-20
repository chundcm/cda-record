# coding=utf-8
import rest_requests as requests
import logger

apiV1 = 'v1'
apiV1Beta1 = 'v1beta1'
apiV1Beta2 = 'v1beta2'

class K8sCredential(object):
    def __init__(self, keyStorePath, keyStorePass, keyPass):
        super(K8sCredential, self).__init__()
        self.keyStorePath = keyStorePath
        self.keyStorePass = keyStorePass
        self.keyPass = keyPass


class K8sClient(object):
    def __init__(self, apiEndpoint, credential, proxies=None, verify=False):
        super(K8sClient, self).__init__()
        self.apiEndpoint = apiEndpoint
        self.proxies = proxies
        self.verify = verify
        self.headers = {}
        self.credential = credential

    def getIpAddress(self):
        pass

    def getClientType(self):
        return 'http'

    def getResponse(self, url, params=None, apiVersion=apiV1):
        logger.debug('Request: ', self.apiEndpoint + (url % apiVersion))
        if self.credential:
            rsp = requests.get(self.apiEndpoint + (url % apiVersion), proxies=self.proxies, debug=False, verify=self.verify, params=params,
                           keystore=(self.credential.keyStorePath, self.credential.keyStorePass, self.credential.keyPass))
        else:
            rsp = requests.get(self.apiEndpoint + (url % apiVersion), proxies=self.proxies, debug=False, verify=self.verify, params=params, keystore=None)
        jsonResponse = rsp.json()
        logger.debug('Get Json Response: ', jsonResponse)
        return jsonResponse

    def listNodes(self):
        return self.getResponse('/api/%s/nodes')

    def listNamespaces(self):
        return self.getResponse('/api/%s/namespaces')

    def listServices(self):
        return self.getResponse('/api/%s/services')

    def listPods(self):
        return self.getResponse('/api/%s/pods')

    def listJobs(self):
        return self.getResponse('/apis/batch/%s/jobs')

    def listReplicaSets(self):
        return self.getResponse('/apis/apps/%s/replicasets', apiVersion=apiV1Beta2)

    def listDeployments(self):
        return self.getResponse('/apis/apps/%s/deployments', apiVersion=apiV1Beta2)

    def listReplicationControllers(self):
        return self.getResponse('/api/%s/replicationcontrollers')

    def listDaemonSets(self):
        return self.getResponse('/apis/apps/%s/daemonsets', apiVersion=apiV1Beta2)

    def listStatefulSets(self):
        return self.getResponse('/apis/apps/%s/statefulsets', apiVersion=apiV1Beta2)

    def listCronJobs(self):
        return self.getResponse('/apis/batch/v1beta1/cronjobs', apiVersion=apiV1Beta1)

    def listPersistentVolumes(self):
        return self.getResponse('/api/%s/persistentvolumes')

    def listPersistentVolumeClaims(self):
        return self.getResponse('/api/%s/persistentvolumeclaims')

    def listStorageClasses(self):
        return self.getResponse('/apis/storage.k8s.io/%s/storageclasses')

    def listResourcequotas(self):
        return self.getResponse('/api/%s/resourcequotas')

