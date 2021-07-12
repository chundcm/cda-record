#coding=utf-8
import sys
import logger
import modeling

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector


def createConfigfileOSH(config, containerOSH, configName):
    import json
    if config:
        configString = json.dumps(config)
        fileOSH = modeling.createConfigurationDocumentOSH(
                    configName,
                    None,
                    configString,
                    contentType=modeling.MIME_TEXT_PLAIN,
                    description=configName)
        fileOSH.setContainer(containerOSH)
        return fileOSH


class BaseDiscoverer:
    def __init__(self):
        self.queryMib = None

    def parse(self, instances):
        raise NotImplementedError('')

    def discover(self, client):
        if not self.queryMib:
            raise ValueError('MIB must be set in order to perform query.')
        resultSet = client.executeQuery(self.queryMib)
        return self.parse(resultSet)


statusConverter = {'0': 'ok', '1': 'attn', '2': 'down', '3': 'invalid'}

class ClusterDiscoverer(BaseDiscoverer):
    def __init__(self):
        BaseDiscoverer.__init__(self)
        self.queryMib = '1.3.6.1.4.1.12124.1.1.1,1.3.6.1.4.1.12124.1.1.1,string,1.3.6.1.4.1.12124.1.1.2,int,' \
                        '1.3.6.1.4.1.12124.1.1.3,string,1.3.6.1.4.1.12124.1.1.4,int,1.3.6.1.4.1.12124.1.1.5,string,' \
                        '1.3.6.1.4.1.12124.1.1.6,string,1.3.6.1.4.1.12124.1.1.7,string'

    def parse(self, resultSet):
        count = 0
        vector = ObjectStateHolderVector()
        clusterOSH = None
        clusterResourceOSH = None
        while resultSet.next():
            if count > 1:
                logger.debug('More than one cluster found.')
                return None, None, None
            count = count + 1
            try:
                clusterName = resultSet.getString(2)
                clusterStatus = statusConverter[resultSet.getString(3)]
                clusterGUID = resultSet.getString(4)
                clusterNodeCount = int(resultSet.getString(5))
                clusterConfiguredNodes = resultSet.getString(6)
                clusterOnlineNodes = resultSet.getString(7)
                clusterOfflineNodes = resultSet.getString(8)
                clusterOSH = ObjectStateHolder('failover_cluster')
                clusterOSH.setStringAttribute('name', clusterName)
                config = {'clusterStatus': clusterStatus, 'clusterGUID': clusterGUID, 'clusterNodeCount': clusterNodeCount,
                          'clusterConfiguredNodes': clusterConfiguredNodes, 'clusterOnlineNodes': clusterOnlineNodes,
                          'clusterOfflineNodes': clusterOfflineNodes}
                fileOSH = createConfigfileOSH(config, clusterOSH, 'Cluster Config')
                clusterResourceOSH = ObjectStateHolder('cluster_resource_group')
                clusterResourceOSH.setStringAttribute('name', clusterName)
                vector.add(clusterResourceOSH)
                vector.add(clusterOSH)
                vector.add(fileOSH)
                clusterLink = modeling.createLinkOSH('containment', clusterOSH, clusterResourceOSH)
                vector.add(clusterLink)
            except:
                logger.warnException('Failed parsing cluster #%d.' % count)
                continue
        return clusterOSH, clusterResourceOSH, vector


class NodeDiscoverer(BaseDiscoverer):
    def __init__(self):
        BaseDiscoverer.__init__(self)
        self.queryMib = '1.3.6.1.4.1.12124.2.1.1,1.3.6.1.4.1.12124.2.1.1,string,1.3.6.1.4.1.12124.2.1.2,int,1.3.6.1.4.1.12124.2.1.3,int,1.3.6.1.4.1.12124.2.1.4,int'

    def parse(self, resultSet):
        count = 0
        nodes = []
        vector = ObjectStateHolderVector()
        while resultSet.next():
            count = count + 1
            try:
                nodeName = resultSet.getString(2)
                nodeStatus = statusConverter[resultSet.getString(3)]
                nodeType = resultSet.getString(4)
                nodeReadOnly = int(resultSet.getString(5))
                nodeOSH = ObjectStateHolder('node')
                nodeOSH.setStringAttribute('name', nodeName)
                nodes.append(nodeOSH)
                config = {'nodeStatus': nodeStatus, 'nodeType': nodeType, 'nodeReadOnly': nodeReadOnly}
                fileOSH = createConfigfileOSH(config, nodeOSH, 'Node Config')
                vector.add(nodeOSH)
                vector.add(fileOSH)
            except:
                logger.warnException('Failed parsing node #%d.' % count)
                continue
        logger.debug('Node count: ', count)
        return nodes, vector


class DiskDiscoverer(BaseDiscoverer):
    def __init__(self):
        BaseDiscoverer.__init__(self)
        self.queryMib = '1.3.6.1.4.1.12124.2.52.1.1,1.3.6.1.4.1.12124.2.52.1.1,int,1.3.6.1.4.1.12124.2.52.1.2,int,' \
                        '1.3.6.1.4.1.12124.2.52.1.3,int,1.3.6.1.4.1.12124.2.52.1.4,string,' \
                        '1.3.6.1.4.1.12124.2.52.1.5,string,1.3.6.1.4.1.12124.2.52.1.6,string,' \
                        '1.3.6.1.4.1.12124.2.52.1.7,string,1.3.6.1.4.1.12124.2.52.1.8,string,' \
                        '1.3.6.1.4.1.12124.2.52.1.9,double'

    def parse(self, resultSet):
        count = 0
        disks = []
        vector = ObjectStateHolderVector()
        while resultSet.next():
            count = count + 1
            try:
                diskBay = resultSet.getString(2)
                diskLogicalNumber = resultSet.getString(3)
                diskChassisNumber = resultSet.getString(4)
                diskDeviceName = resultSet.getString(5)
                diskStatus = resultSet.getString(6)
                diskModel = resultSet.getString(7)
                diskSerialNumber = resultSet.getString(8)
                diskFirmwareVersion = resultSet.getString(9)
                diskSizeStr = resultSet.getString(10)
                # disk size in bytes, convert to MB
                diskSize = long(diskSizeStr)/1024/1024
                diskOSH = ObjectStateHolder('disk_device')
                diskOSH.setStringAttribute('name', diskDeviceName)
                diskOSH.setStringAttribute('model_name', diskModel)
                diskOSH.setStringAttribute('serial_number', diskSerialNumber)
                diskOSH.setIntegerAttribute('disk_size', diskSize)
                disks.append(diskOSH)
                config = {'diskBay': diskBay, 'diskLogicalNumber': diskLogicalNumber, 'diskChassisNumber': diskChassisNumber,
                          'diskStatus': diskStatus, 'diskFirmwareVersion': diskFirmwareVersion, 'diskSize': diskSize}
                import json
                configString = json.dumps(config)
                diskOSH.setStringAttribute('description', configString)
                vector.add(diskOSH)
            except:
                logger.warnException('Failed parsing disk #%d.' % count)
                continue
        logger.debug('Disk count: ', count)
        return disks, vector


def discoverIsilon(client):
    vector = ObjectStateHolderVector()
    clusterDiscoverer = ClusterDiscoverer()
    clusterOSH, clusterResourceOSH, vec = clusterDiscoverer.discover(client)
    vector.addAll(vec)
    nodeDiscoverer = NodeDiscoverer()
    nodes, vec = nodeDiscoverer.discover(client)
    vector.addAll(vec)
    diskDiscoverer = DiskDiscoverer()
    disks, vec = diskDiscoverer.discover(client)
    for nodeOSH in nodes:
        link = modeling.createLinkOSH('containment', clusterOSH, nodeOSH)
        vector.add(link)
    for diskOSH in disks:
        diskOSH.setContainer(clusterResourceOSH)
        vector.add(diskOSH)
    return vector
