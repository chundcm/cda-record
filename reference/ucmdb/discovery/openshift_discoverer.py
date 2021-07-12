# coding=utf-8
import logger
import re
import rest_requests as requests
import kubernetes_restful_client
import kubernetes_discoverer
import openshift

from com.hp.ucmdb.discovery.common import CollectorsConstants

blockstroage = 'blockstorage-class.storageclass.storage.k8s.io/requests.storage'
gluster_dyn = 'gluster-dyn.storageclass.storage.k8s.io/requests.storage'

class OpenShiftClient(kubernetes_restful_client.K8sClient):
    def __init__(self, framework, protocol):

        self.endpoint = framework.getDestinationAttribute('endpoint')
        kubernetes_restful_client.K8sClient.__init__(self, self.endpoint, None)

        self.username = framework.getProtocolProperty(protocol, CollectorsConstants.PROTOCOL_ATTRIBUTE_USERNAME, '')
        self.password = framework.getProtocolProperty(protocol, CollectorsConstants.PROTOCOL_ATTRIBUTE_PASSWORD, '')

        http_proxy = framework.getProtocolProperty(protocol, 'proxy', '')

        if http_proxy:
            self.proxies['http'] = http_proxy
            self.proxies['https'] = http_proxy

        basic_token = 'Bearer %s' % self.get_token()

        self.headers = {
            'Authorization': basic_token
        }

    def get_token(self):
        rsp = requests.get(self.endpoint + '/oauth/token/request', auth=(self.username, self.password), proxies=self.proxies, debug=False, verify=False)

        m = re.search('<code>(\S+)</code>', rsp.text)
        return m.group(1)

    def getResponse(self, url, params=None, apiVersion='v1'):
        logger.debug('Request: ', self.endpoint + (url % apiVersion))
        if self.credential:
            rsp = requests.get(self.endpoint + (url % apiVersion), proxies=self.proxies, debug=False, verify=self.verify, params=params,
                               keystore=(self.credential.keyStorePath, self.credential.keyStorePass, self.credential.keyPass))
        else:
            rsp = requests.get(self.endpoint + (url % apiVersion), headers=self.headers, proxies=self.proxies, debug=False, verify=self.verify)
        jsonResponse = rsp.json()
        logger.debug('Get Json Response: ', jsonResponse)
        return jsonResponse

    def listRoutes(self):
        return self.getResponse('/apis/route.openshift.io/%s/routes')

    def listClusterNetworks(self):
        return self.getResponse('/apis/network.openshift.io/%s/clusternetworks')

    def listDeploymentConfigs(self):
        return self.getResponse('/apis/apps.openshift.io/%s/deploymentconfigs')

    def listClusterResourceQuotas(self):
        return self.getResponse('/apis/quota.openshift.io/%s/clusterresourcequotas')


class OpenShiftDiscoverer(kubernetes_discoverer.K8sDiscoverer):
    def __init__(self, client):
        kubernetes_discoverer.K8sDiscoverer.__init__(self, client, client.endpoint, None, None)
        self.routesById = {}
        self.clusterNetworkById = {}
        self.deploymentConfigById = {}
        self.clusterResourceQuotaById = {}
        self.clusterResourceQuotaConfigByNamespaceAndQuotaName = {}

    def discoverComponents(self):
        kubernetes_discoverer.K8sDiscoverer.discoverComponents(self)
        self.parseRoutes(self.client.listRoutes())
        self.parseClusterNetworks(self.client.listClusterNetworks())
        self.parseDeploymentConfigs(self.client.listDeploymentConfigs())
        self.parseClusterResourceQuotas(self.client.listClusterResourceQuotas())

    def parseRoutes(self, routes):
        items = kubernetes_discoverer.getItems(routes)
        for item in items:
            name = item['metadata']['name']
            id = item['metadata']['uid']
            route = openshift.Route(id, name)
            namespace = item['metadata']['namespace']
            route.namespace = self.NamespacesByName.get(namespace)

            route.host = item['spec']['host']
            if item['spec']['to']['kind'] == 'Service':
                service_name = item['spec']['to']['name']
                route.service = self.ServicesByNameSpaceAndName.get(namespace + '-' + service_name)

            if item['spec'].get('port', None):
                route.target_port = item['spec']['port']['targetPort']
            self.routesById[id] = route

    def parseClusterNetworks(self, networks):
        items = kubernetes_discoverer.getItems(networks)
        for item in items:
            name = item['metadata']['name']
            id = item['metadata']['uid']
            network = openshift.ClusterNetwork(id, name)
            network.cluster = self.Cluster
            network.service_cidr = item['serviceNetwork']
            if item.get('network', None):
                network.network_cidr = item['network']
            if item.get('hostsubnetlength', None):
                network.hostsubnet_length = item['hostsubnetlength']
            if item.get('clusterNetworks', None):
                for cluster_network in item['clusterNetworks']:
                    if cluster_network.get('CIDR', None):
                        cidr = cluster_network['CIDR']
                        hostsubnet_length = None
                        if cluster_network.get('hostSubnetLength', None):
                            hostsubnet_length = cluster_network['hostSubnetLength']
                        network.cluster_networks.append((cidr, hostsubnet_length))
            self.clusterNetworkById[id] = network

    def parseDeploymentConfigs(self, configs):
        items = kubernetes_discoverer.getItems(configs)
        for item in items:
            name = item['metadata']['name']
            id = item['metadata']['uid']
            config = openshift.DeploymentConfig(id, name)
            namespace = item['metadata']['namespace']
            config.namespace = self.NamespacesByName.get(namespace)
            config.replicas = item['spec']['replicas']
            config.strategy = item['spec']['strategy']['type']
            for pod_id in self.PodsById:
                pod = self.PodsById[pod_id]
                if config.namespace == pod.Namespace:
                    if pod.Labels:
                        if pod.Labels.get('deploymentconfig', None) and name == pod.Labels['deploymentconfig']:
                            config.pods.append(pod)
                    elif pod.annotations:
                        if pod.annotations.get('openshift.io/deployment-config.name', None) and name == pod.annotations['openshift.io/deployment-config.name']:
                            config.pods.append(pod)

            self.deploymentConfigById[id] = config

    def parseClusterResourceQuotas(self, clusterresourcequotas):
        items = None
        if clusterresourcequotas:
            items = kubernetes_discoverer.getItems(clusterresourcequotas)
        if items:
            for item in items:
                name = item['metadata']['name']
                id = item['metadata']['uid']
                clusterresourcequota = openshift.ClusterResourceQuota(id, name)
                clusterresourcequota.cluster = self.Cluster
                if item['spec'].get('quota'):
                    specHard = item['spec']['quota'].get('hard')
                    # parse_cpu_value need None check
                    if specHard.get('limits.cpu'):
                        clusterresourcequota.specHardLimitCpu = kubernetes_discoverer.parse_cpu_value(specHard.get('limits.cpu'))
                    # parseValue need None check
                    if specHard.get('limits.memory'):
                        clusterresourcequota.specHardLimitMemory = kubernetes_discoverer.parseValue(specHard.get('limits.memory'))
                    if specHard.get('requests.cpu'):
                        clusterresourcequota.specHardRequestCpu = kubernetes_discoverer.parse_cpu_value(specHard.get('requests.cpu'))
                    if specHard.get('requests.memory'):
                        clusterresourcequota.specHardRequestMemory = kubernetes_discoverer.parseValue(specHard.get('requests.memory'))
                    if specHard.get(blockstroage):
                        clusterresourcequota.specHardBlockStorage = kubernetes_discoverer.parseValue(specHard.get(blockstroage))
                    if specHard.get(gluster_dyn):
                        clusterresourcequota.specHardGlusterDyn = kubernetes_discoverer.parseValue(specHard.get(gluster_dyn))

                if item['status'].get('total'):
                    statusHard = item['status']['total'].get('hard')
                    if statusHard.get('limits.cpu'):
                        clusterresourcequota.statusHardLimitCpu = kubernetes_discoverer.parse_cpu_value(statusHard.get('limits.cpu'))
                    if statusHard.get('limits.memory'):
                        clusterresourcequota.statusHardLimitMemory = kubernetes_discoverer.parseValue(statusHard.get('limits.memory'))
                    if statusHard.get('requests.cpu'):
                        clusterresourcequota.statusHardRequestCpu = kubernetes_discoverer.parse_cpu_value(statusHard.get('requests.cpu'))
                    if statusHard.get('requests.memory'):
                        clusterresourcequota.statusHardRequestMemory = kubernetes_discoverer.parseValue(statusHard.get('requests.memory'))
                    if statusHard.get(blockstroage):
                        clusterresourcequota.statusHardBlockStorage = kubernetes_discoverer.parseValue(statusHard.get(blockstroage))
                    if statusHard.get(gluster_dyn):
                        clusterresourcequota.statusHardGlusterDyn = kubernetes_discoverer.parseValue(statusHard.get(gluster_dyn))

                    statusUsed = item['status']['total'].get('used')
                    if statusUsed.get('limits.cpu'):
                        clusterresourcequota.statusUsedLimitCpu = kubernetes_discoverer.parse_cpu_value(statusUsed.get('limits.cpu'))
                    if statusUsed.get('limits.memory'):
                        clusterresourcequota.statusUsedLimitMemory = kubernetes_discoverer.parseValue(statusUsed.get('limits.memory'))
                    if statusUsed.get('requests.cpu'):
                        clusterresourcequota.statusUsedRequestCpu = kubernetes_discoverer.parse_cpu_value(statusUsed.get('requests.cpu'))
                    if statusUsed.get('requests.memory'):
                        clusterresourcequota.statusUsedRequestMemory = kubernetes_discoverer.parseValue(statusUsed.get('requests.memory'))
                    if statusUsed.get(blockstroage):
                        clusterresourcequota.statusUsedBlockStorage = kubernetes_discoverer.parseValue(statusUsed.get(blockstroage))
                    if statusUsed.get(gluster_dyn):
                        clusterresourcequota.statusUsedGlusterDyn = kubernetes_discoverer.parseValue(statusUsed.get(gluster_dyn))

                if item['status'].get('namespaces'):
                    namespaces = item['status']['namespaces']
                    for namespace in namespaces:
                        self.parseOpenshiftClusterResourceQuotaConfig(namespace=namespace, clusterResourceQuota=clusterresourcequota)
                self.clusterResourceQuotaById[id] = clusterresourcequota


    def parseOpenshiftClusterResourceQuotaConfig(self, namespace, clusterResourceQuota):
        # use <namespace name>/<cluster quota name> to be quota config's id and name, for reconciliation and display
        idName=namespace.get('namespace') + '/' + clusterResourceQuota.getName()
        if namespace:
            clusterresourcequotaconfig = openshift.ClusterResourceQuotaConfig(id=idName, name=idName)
            clusterresourcequotaconfig.clusterQuota = clusterResourceQuota
            # status field
            if namespace.get('status'):
                # hard field
                hard = namespace['status'].get('hard')
                if hard.get('limits.cpu'):
                    clusterresourcequotaconfig.hardLimitCpu = kubernetes_discoverer.parse_cpu_value(hard.get('limits.cpu'))
                if hard.get('limits.memory'):
                    clusterresourcequotaconfig.hardLimitMemory = kubernetes_discoverer.parseValue(hard.get('limits.memory'))
                if hard.get('requests.cpu'):
                    clusterresourcequotaconfig.hardRequestCpu = kubernetes_discoverer.parse_cpu_value(hard.get('requests.cpu'))
                if hard.get('requests.memory'):
                    clusterresourcequotaconfig.hardRequestMemory = kubernetes_discoverer.parseValue(hard.get('requests.memory'))
                if hard.get(blockstroage):
                    clusterresourcequotaconfig.hardBlockStorage = kubernetes_discoverer.parseValue(hard.get(blockstroage))
                if hard.get(gluster_dyn):
                    clusterresourcequotaconfig.hardGlusterDyn = kubernetes_discoverer.parseValue(hard.get(gluster_dyn))
                # used field
                used = namespace['status'].get('used')
                if used.get('limits.cpu'):
                    clusterresourcequotaconfig.usedLimitCpu = kubernetes_discoverer.parse_cpu_value(used.get('limits.cpu'))
                if used.get('limits.memory'):
                    clusterresourcequotaconfig.usedLimitMemory = kubernetes_discoverer.parseValue(
                        used.get('limits.memory'))
                if used.get('requests.cpu'):
                    clusterresourcequotaconfig.usedRequestCpu = kubernetes_discoverer.parse_cpu_value(used.get('requests.cpu'))
                if used.get('requests.memory'):
                    clusterresourcequotaconfig.usedRequestMemory = kubernetes_discoverer.parseValue(
                        used.get('requests.memory'))
                if used.get(blockstroage):
                    clusterresourcequotaconfig.usedBlockStorage = kubernetes_discoverer.parseValue(used.get(blockstroage))
                if used.get(gluster_dyn):
                    clusterresourcequotaconfig.usedGlusterDyn = kubernetes_discoverer.parseValue(used.get(gluster_dyn))
            clusterresourcequotaconfig.namespace = self.NamespacesByName[namespace.get('namespace')]
            self.clusterResourceQuotaConfigByNamespaceAndQuotaName[idName] =clusterresourcequotaconfig
            return clusterresourcequotaconfig

    def report(self):
        vector = kubernetes_discoverer.K8sDiscoverer.report(self)
        reporter = openshift.Reporter(openshift.Builder())
        for item in self.routesById:
            object = self.routesById[item]
            vector.addAll(self.safeReport(object, reporter.reportRoute))

        for item in self.clusterNetworkById:
            object = self.clusterNetworkById[item]
            vector.addAll(self.safeReport(object, reporter.reportClusterNetwork))

        for item in self.deploymentConfigById:
            object = self.deploymentConfigById[item]
            vector.addAll(self.safeReport(object, reporter.reportDeploymentConfig))

        for item in self.clusterResourceQuotaById:
            object = self.clusterResourceQuotaById[item]
            vector.addAll(self.safeReport(object, reporter.reportClusterResourceQuota))

        for item in self.clusterResourceQuotaConfigByNamespaceAndQuotaName:
            object = self.clusterResourceQuotaConfigByNamespaceAndQuotaName[item]
            vector.addAll(self.safeReport(object, reporter.reportClusterResourceQuotaConfig))
        return vector

