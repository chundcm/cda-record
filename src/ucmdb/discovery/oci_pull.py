# coding=utf-8
__author__ = 'Kane'

import logger
import modeling
import oci_common
import rest_requests as requests
from oci_entities import Storage, Fabric, DataStore


class OCIClient(object):
    def __init__(self, base_url, username, password):
        super(OCIClient, self).__init__()
        self.base_url = base_url
        self.username = username
        self.password = password

    def request(self, method, path):
        return requests.request(method, self.base_url + path, auth=(self.username, self.password), verify=False).json()

    def list(self, oci_type, owner=None, oci_type_name=None):
        """
        @type oci_type: OCIType
        @type owner: OCIType
        @rtype: list of OCIType
        """
        if not oci_type_name:
            oci_type_name = oci_type.oci_types
        if not owner:
            path = '/rest/v1/assets/%s' % oci_type_name
        else:
            path = '%s/%s' % (owner.self_link, oci_type_name)
        raw_objects = self.request(oci_type.LIST_METHOD, path)
        objects = map(oci_type, raw_objects)
        objects = filter(lambda x: x.isValid(), objects)
        map(self._list_children, objects)
        return objects

    def _list_children(self, oci):
        """
        @type oci: OCIType
        @return:
        """
        for child_type, child in oci.children_mappings.iteritems():
            oci_type = 'type' in child and child['type']
            relationship = 'relationship' in child and child['relationship']
            oci.children_group[child_type] = relationship, self.list(oci_type, oci, child_type)

    def setReporter(self, ociReporter):
        self.reporter = ociReporter


class OCIReporter(object):
    def __init__(self):
        super(OCIReporter, self).__init__()
        self.vector = oci_common.new_vector()

    def reportOne(self, osh):
        self.vector.add(osh)

    def reportVector(self, vector):
        self.vector.addAll(vector)


class OCIDiscover(object):
    def __init__(self, ociClient):
        """
        @type ociClient: OCIClient
        @return:
        """
        super(OCIDiscover, self).__init__()
        self.ociClient = ociClient

    def discover(self):
        storages = self.ociClient.list(Storage)

        storagePortOshMap = {}
        storageResourcesMap = {}

        for storage in storages:
            _, storageNodes = storage.children_group['storageNodes']
            for storageNode in storageNodes:
                self.reporter.reportVector(storageNode.buildCI())
                _, storagePools = storageNode.children_group['storagePools']
                _, storagePorts = storageNode.children_group['ports']

                for storagePort in storagePorts:
                    self.reporter.reportVector(storagePort.buildCI())
                    storagePort.osh.setContainer(storageNode.osh)
                    storagePortOshMap[storagePort.self_link] = storagePort.osh

                for storagePool in storagePools:
                    self.reporter.reportVector(storagePool.buildCI())
                    storagePool.osh.setContainer(storageNode.osh)
                    rel_p_and_v, volumes = storagePool.children_group['volumes']
                    rel_p_and_i, internalVolumes = storagePool.children_group['internalVolumes']
                    rel_p_and_d, disks = storagePool.children_group['disks']
                    volumesOshMap = {}
                    for volume in volumes:
                        self.reporter.reportVector(volume.buildCI())
                        volume.osh.setContainer(storageNode.osh)
                        volumesOshMap[volume.self_link] = volume.osh
                        linkOsh = modeling.createLinkOSH(rel_p_and_v, storagePool.osh, volume.osh)
                        self.reporter.reportOne(linkOsh)
                    for internalVolume in internalVolumes:
                        self.reporter.reportVector(internalVolume.buildCI())
                        internalVolume.osh.setContainer(storageNode.osh)
                        storageResourcesMap[internalVolume.self_link] = internalVolume.osh
                        rel_l_and_v, childrenVolume = internalVolume.children_group['volumes']
                        for childVolume in childrenVolume:
                            childVolumeOsh = volumesOshMap.get(childVolume.self_link)
                            if childVolumeOsh:
                                linkOsh = modeling.createLinkOSH(rel_l_and_v, childVolumeOsh, internalVolume.osh)
                                self.reporter.reportOne(linkOsh)
                        linkOsh = modeling.createLinkOSH(rel_p_and_i, storagePool.osh, internalVolume.osh)
                        self.reporter.reportOne(linkOsh)

                    for disk in disks:
                        self.reporter.reportVector(disk.buildCI())
                        disk.osh.setContainer(storageNode.osh)
                        linkOsh = modeling.createLinkOSH(rel_p_and_d, storagePool.osh, disk.osh)
                        self.reporter.reportOne(linkOsh)

        fabric_list = self.ociClient.list(Fabric)
        for fabric in fabric_list:
            self.reporter.reportVector(fabric.buildCI())
            rel_f_and_s, switches = fabric.children_group['switches']
            for switch in switches:
                self.reporter.reportVector(switch.buildCI())
                linkOsh = modeling.createLinkOSH(rel_f_and_s, fabric.osh, switch.osh)
                self.reporter.reportOne(linkOsh)
                _, fcPorts = switch.children_group['ports']
                for fcPort in fcPorts:
                    self.reporter.reportVector(fcPort.buildCI())
                    fcPort.osh.setContainer(switch.osh)
                    rel_fp_and_sp, storagePorts = fcPort.children_group['connectedPorts']
                    for storagePort in storagePorts:
                        storagePortOsh = storagePortOshMap.get(storagePort.self_link)
                        if storagePortOsh:
                            linkOsh = modeling.createLinkOSH(rel_fp_and_sp, fcPort.osh, storagePortOsh)
                            self.reporter.reportOne(linkOsh)

        dataStoreList = self.ociClient.list(DataStore)
        for dataStore in dataStoreList:
            vcenterIP = dataStore.data['virtualCenterIp']
            vcenterOsh = modeling.createHostOSH(vcenterIP, 'host_node')
            self.reporter.reportOne(vcenterOsh)
            self.reporter.reportVector(dataStore.buildCI())
            dataStore.osh.setContainer(vcenterOsh)
            rel_s_and_h, hosts = dataStore.children_group['hosts']
            rel_s_and_v, storageResources = dataStore.children_group['storageResources']
            for host in hosts:
                self.reporter.reportVector(host.buildCI())
                ip = host.data['ip']
                if ip:
                    ips = ip.split(',')
                    for _ip in ips:
                        _ip = _ip.strip()
                        if _ip:
                            ipOsh = modeling.createIpOSH(_ip)
                            linkOshIP = modeling.createLinkOSH('containment', host.osh, ipOsh)
                            self.reporter.reportOne(ipOsh)
                            self.reporter.reportOne(linkOshIP)
                linkOsh = modeling.createLinkOSH(rel_s_and_h, host.osh, dataStore.osh)
                self.reporter.reportOne(linkOsh)

            for storageResource in storageResources:
                storageResourceOsh = storageResourcesMap.get(storageResource.self_link)
                if storageResourceOsh:
                    linkOsh = modeling.createLinkOSH(rel_s_and_v, dataStore.osh, storageResourceOsh)
                    self.reporter.reportOne(linkOsh)

    def setReporter(self, ociReporter):
        self.reporter = ociReporter


def do_discover(http_type, host, port, username, password):
    api_base_url = '%s://%s:%s' % (http_type, host, port)
    ociClient = OCIClient(api_base_url, username, password)
    oci_discover = OCIDiscover(ociClient)
    ociReporter = OCIReporter()
    oci_discover.setReporter(ociReporter)
    oci_discover.discover()
    return ociReporter.vector


def DiscoveryMain(Framework):
    try:
        logger.debug('Pull topology from OCI')
        host = Framework.getParameter('host')
        host = host and host.strip()
        if not host:
            raise Exception('No valid host')

        credentialId = Framework.getParameter('credentialsId')

        if not credentialId:
            raise Exception('No valid credential')
        username = Framework.getProtocolProperty(credentialId, 'protocol_username')
        password = Framework.getProtocolProperty(credentialId, 'protocol_password')
        http_type = Framework.getProtocolProperty(credentialId, 'protocol')
        port = Framework.getProtocolProperty(credentialId, 'protocol_port')
        if not username or not password:
            raise Exception('Invalid username or password')

        vector = do_discover(http_type, host, port, username, password)
        logger.debug('Total size:', vector.size())
        return vector
    except:
        logger.errorException('Failed to pull data from OCI.')
        import sys
        exc_info = sys.exc_info()
        Framework.reportError('Failed to pull data from OCI.')
        Framework.reportError(str(exc_info[1]))
