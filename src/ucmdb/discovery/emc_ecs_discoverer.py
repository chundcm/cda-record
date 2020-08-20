import logger
import re
import rest_requests as requests
import emc_ecs
import modeling

from com.hp.ucmdb.discovery.common import CollectorsConstants
from appilog.common.system.types.vectors import ObjectStateHolderVector


class ECSClient(object):
    def __init__(self, framework, protocol,apiEndpoint, proxies=None, verify=False):
        super(ECSClient, self).__init__()
        self.apiEndpoint = apiEndpoint
        self.proxies = proxies
        self.verify = verify
        self.headers = {}

        self.endpoint = framework.getDestinationAttribute('endpoint')

        self.username = framework.getProtocolProperty(protocol, CollectorsConstants.PROTOCOL_ATTRIBUTE_USERNAME, '')
        self.password = framework.getProtocolProperty(protocol, CollectorsConstants.PROTOCOL_ATTRIBUTE_PASSWORD, '')
        self.trustStorePath = framework.getProtocolProperty(protocol, 'trustStorePath')
        self.trustStorePass = framework.getProtocolProperty(protocol, 'trustStorePass')

        http_proxy = framework.getProtocolProperty(protocol, 'proxy', '')

        if http_proxy:
            self.proxies['http'] = http_proxy
            self.proxies['https'] = http_proxy

        self.headers = {
            'X-SDS-AUTH-TOKEN': self.get_token(),
            'Accept': 'application/json'
        }

    def get_token(self):
        rsp = requests.get(self.endpoint + '/login', auth=(self.username, self.password), proxies=self.proxies, debug=False, verify=False,
                               truststore=(self.trustStorePath, self.trustStorePass))

        token = rsp.headers['X-SDS-AUTH-TOKEN']
        if token:
            logger.debug("get token successfully")
            return token

    def getResponse(self, url, params=None):
        logger.debug('Request: ', self.endpoint)
        rsp = requests.get(self.endpoint + url, headers=self.headers, proxies=self.proxies, debug=False, verify=self.verify,
                           truststore=(self.trustStorePath, self.trustStorePass))
        logger.debug("response is:", rsp.text)
        jsonResponse = rsp.json()
        return jsonResponse


class NameSpaceDiscoverer():
    def __init__(self, client):
        self.client = client

    def getNameSpaces(self):
        return self.client.getResponse('/object/namespaces')

    def getNameSpaceDetails(self, namespaceId):
        return self.client.getResponse('/object/namespaces/namespace/%s' %namespaceId)

    def discoverNameSpace(self):
        nameSpaceResponse = self.getNameSpaces()
        nameSpaces = []
        if nameSpaceResponse:
            nameSpaceElements = nameSpaceResponse['namespace']
            for nameSpaceElement in nameSpaceElements:
                nameSpace = emc_ecs.NameSpace(nameSpaceElement['id'], nameSpaceElement['name'])
                nameSpaceDetails =  self.getNameSpaceDetails(nameSpaceElement['id'])
                default_data_services_vpool = nameSpaceDetails['default_data_services_vpool']
                if default_data_services_vpool:
                    nameSpace.default_data_services_vpool = nameSpaceDetails['default_data_services_vpool']
                    nameSpace.build(emc_ecs.Builder())
                    logger.debug("Build NameSpace successfully:", nameSpace.getName())
                    nameSpaces.append(nameSpace)
                else:
                    logger.error('Can not get default data services pool, namespace discovery failed')
        return nameSpaces


class BucketDiscoverer():
    def __init__(self, client):
        self.client = client

    def getBucket(self, nameSpace):
        return self.client.getResponse('/object/bucket?namespace=%s' %nameSpace)

    def discoverBucket(self, nameSpace, baseUrl):
        nameSpaceName = nameSpace.getName()
        bucketResponse = self.getBucket(nameSpaceName)
        buckets = []
        if bucketResponse and bucketResponse.has_key('object_bucket'):
            bucketElements = bucketResponse['object_bucket']
            for bucketElement in bucketElements:
                bucket = emc_ecs.Bucket(bucketElement['id'], bucketElement['name'], bucketElement['name'] + '.' + baseUrl)
                bucket.nameSpace = nameSpace
                bucket.build(emc_ecs.Builder())
                logger.debug("Build Bucket successfully:", bucket.getName())
                buckets.append(bucket)
        return buckets

# VPool is the replication group in EMC ECS system
class VPoolDiscoverer():
    def __init__(self, client):
        self.client = client

    def getVPools(self):
        return self.client.getResponse('/vdc/data-service/vpools')

    def getVPoolDetails(self, vpoolId):
        return self.client.getResponse('/vdc/data-service/vpools/%s' %vpoolId)

    def discoverVPool(self):
        vPoolsResponse = self.getVPools()
        vPools = []
        if vPoolsResponse:
            vPoolElements = vPoolsResponse['data_service_vpool']
            for vPoolElement in vPoolElements:
                vpool = emc_ecs.VPool(vPoolElement['id'], vPoolElement['name'])
                varrayMappings = vPoolElement['varrayMappings']
                for varrayMapping in varrayMappings:
                    vpool.vdc.append(varrayMapping['name'])
                    vpool.vArray.append(varrayMapping['value'])
                vpool.build(emc_ecs.Builder())
                logger.debug("Build replication group successfully:", vpool.getName())
                vPools.append(vpool)
        return vPools

# Varray is Storage pool in EMC ECS system
class VArrayDiscoverer():
    def __init__(self, client):
        self.client = client

    def getVArraies(self):
        return self.client.getResponse('/vdc/data-services/varrays')

    def discoverVArraies(self):
        vArriesResponse = self.getVArraies()
        vArraies = []
        if vArriesResponse:
            vArriesElements = vArriesResponse['varray']
            for vArrayElement in vArriesElements:
                varray = emc_ecs.VArray(vArrayElement['id'], vArrayElement['name'])
                varray.build(emc_ecs.Builder())
                logger.debug("Build storage pool successfully:", varray.getName())
                vArraies.append(varray)
        return vArraies

# BaseUrl discovery
class BaseUrlDiscoverer():
    def __init__(self, client):
        self.client = client

    def getBaseUrlquery(self):
        return self.client.getResponse('/object/baseurl')

    def discoverBaseUrl(self):
        baseUrlId = self.getBaseUrlquery()['base_url'][0]['link']['href']
        baseUrl = self.client.getResponse(baseUrlId)['baseurl']
        return baseUrl

class VArrayWithNodeDiscover():
    def __init__(self, client):
        self.client = client

    def getVArraiesAndNodes(self, varrayId):
        return self.client.getResponse('/dashboard/storagepools/%s/nodes' %varrayId)

    def discoverNodes2Array(self, varrayId):
        nodesResponse = self.getVArraiesAndNodes(varrayId)
        nodes = []
        if nodesResponse:
            nodeInstances = nodesResponse['_embedded']['_instances']
            for nodeInstance in nodeInstances:
                node = emc_ecs.Node(nodeInstance['id'], nodeInstance['displayName'])
                node.build(emc_ecs.Builder())
                nodes.append(node)
        return nodes


class VDCDiscoverer():
    def __init__(self, client):
        self.client = client

    def getVDCs(self):
        return self.client.getResponse('/object/vdcs/vdc/list')

    def indentifyCurrentVDC(self):
        currentVDCResponse = self.client.getResponse('/object/vdcs/vdc/local')
        return currentVDCResponse['id']

    def discoverVDC(self):
        VDCResponse = self.getVDCs()
        VDCs = []
        if VDCResponse:
            VDCElements = VDCResponse['vdc']
            for VDCElement in VDCElements:
                vdc = emc_ecs.VDC(VDCElement['id'], VDCElement['name'])
                vdc.interVdcEndPoints.append(VDCElement['interVdcEndPoints'])
                vdc.build(emc_ecs.Builder())
                logger.debug("Build VDC successfully:", vdc.getName())
                VDCs.append(vdc)
        return VDCs


class NodeDiscoverer():
    def __init__(self, client):
        self.client = client

    def getNodes(self):
        return self.client.getResponse('/vdc/nodes')

    def discoverNodes(self):
        nodeResponse = self.getNodes()
        Nodes = []
        if nodeResponse:
            nodeElements = nodeResponse['node']
            for nodeElement in nodeElements:
                node = emc_ecs.Node(nodeElement['nodeid'], nodeElement['nodename'])
                node.build(emc_ecs.Builder())
                logger.debug("Build node successfully:", node.getName())
                Nodes.append(node)
        return Nodes


def Discover(client):
    vector = ObjectStateHolderVector()
    # discover VDC
    vdcs = VDCDiscoverer(client).discoverVDC()
    for vdc in vdcs:
        vector.add(vdc.getOsh())
        # discover ipaddress of vdc
        interVdcEndPoints = vdc.interVdcEndPoints
        for interVdcEndPoint in interVdcEndPoints:
            Ips = interVdcEndPoint.split(',')
            for ip in Ips:
                IpOsh = modeling.createIpOSH(str(ip))
                ip_vdc_link = modeling.createLinkOSH('containment', vdc.getOsh(), IpOsh)
                vector.add(ip_vdc_link)
                vector.add(IpOsh)

    # discover storage pool
    Varraies = VArrayDiscoverer(client).discoverVArraies()
    for Varray in Varraies:
        vector.add(Varray.getOsh())
        # discover relationship between node and storage pool
        nodes = VArrayWithNodeDiscover(client).discoverNodes2Array(Varray.getId())
        for node in nodes:
            vector.add(node.getOsh())
            contain_link = modeling.createLinkOSH('containment', Varray.getOsh(), node.getOsh())
            vector.add(contain_link)
        for vdc in vdcs:
            logger.debug("currently vdc is:", VDCDiscoverer(client).indentifyCurrentVDC())
            if vdc.getId() == VDCDiscoverer(client).indentifyCurrentVDC():
                vv_contain_link = modeling.createLinkOSH('containment', vdc.getOsh(), Varray.getOsh())
                vector.add(vv_contain_link)
    # discover replication pool
    Vpools = VPoolDiscoverer(client).discoverVPool()
    for Vpool in Vpools:
        vector.add(Vpool.getOsh())
        for Varray in Varraies:
            for storagepool in Vpool.vArray:
                if storagepool == Varray.getId():
                    mem_link = modeling.createLinkOSH('membership', Vpool.getOsh(), Varray.getOsh())
                    vector.add(mem_link)
        for vdc in vdcs:
            for relatedVdc in Vpool.vdc:
                if relatedVdc == vdc.getId():
                    contain_link = modeling.createLinkOSH('containment', vdc.getOsh(), Vpool.getOsh())
                    vector.add(contain_link)
    # Base Url Discover
    baseUrl = BaseUrlDiscoverer(client).discoverBaseUrl()
    # discover name space
    nameSpaces = NameSpaceDiscoverer(client).discoverNameSpace()
    for nameSpace in nameSpaces:
        for Vpool in Vpools:
            replicationGroup = nameSpace.default_data_services_vpool
            if replicationGroup == Vpool.getId():
                vector.add(nameSpace.getOsh())
                logger.debug("found default replication pool.")
                contain_link2 = modeling.createLinkOSH('containment', Vpool.getOsh(), nameSpace.getOsh())
                vector.add(contain_link2)
                # discover bucket
                buckets = BucketDiscoverer(client).discoverBucket(nameSpace, baseUrl)
                for bucket in buckets:
                    member_link = modeling.createLinkOSH('containment', nameSpace.getOsh(), bucket.getOsh())
                    vector.add(bucket.getOsh())
                    vector.add(member_link)
                break
    return vector


