# coding=utf-8
import logger
import rest_requests as requests
import rhevm_discoverer
import rhevm


RHEVMClient = rhevm_discoverer.RHEVMClient

RHEVMDiscoverer = rhevm_discoverer.RHEVMDiscoverer

NetworkDiscoverer = rhevm_discoverer.NetworkDiscoverer

HostDiscoverer = rhevm_discoverer.HostDiscoverer

VNICProfileDiscoverer = rhevm_discoverer.VNICProfileDiscoverer

VirtualMachineDiscoverer = rhevm_discoverer.VirtualMachineDiscoverer

VMPoolDiscoverer = rhevm_discoverer.VMPoolDiscoverer

StorageDomainDiscoverer = rhevm_discoverer.StorageDomainDiscoverer

DiskDiscoverer = rhevm_discoverer.DiskDiscoverer

VMDiskDiscoverer = rhevm_discoverer.VMDiskDiscoverer

InterfaceDiscoverer = rhevm_discoverer.InterfaceDiscoverer

VMInterfaceDiscoverer = rhevm_discoverer.VMInterfaceDiscoverer


class DataCenterDiscoverer(rhevm_discoverer.BaseDiscoverer):
    def __init__(self, client):
        rhevm_discoverer.BaseDiscoverer.__init__(self, client)

    def discover(self):
        dcs = []
        try:
            rsp = requests.get(self.client.api_endpoint + 'datacenters', auth=(self.client.username, self.client.credential), headers=self.client.headers, proxies=self.client.proxies, debug=False, verify=False)
            json_rsp = rsp.json()
            logger.debug('datacenters:', json_rsp)

            for dc_json in json_rsp['data_center']:
                dc = rhevm.DataCenter(dc_json['name'], dc_json['id'])
                dc.status = dc_json['status']['state']
                if dc_json.get('description', 0):
                    dc.description = dc_json['description']
                dc.major_version = dc_json['version']['major']
                dc.minor_version = dc_json['version']['minor']
                dcs.append(dc)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error('Failed to discover data center. Exception: <%s>' % excInfo)
        finally:
            return dcs


class ClusterDiscoverer(rhevm_discoverer.BaseDiscoverer):
    def __init__(self, client):
        rhevm_discoverer.BaseDiscoverer.__init__(self, client)

    def discover(self):
        clusters = []
        try:
            rsp = requests.get(self.client.api_endpoint + 'clusters', auth=(self.client.username, self.client.credential), headers=self.client.headers, proxies=self.client.proxies, debug=False, verify=False)
            json_rsp = rsp.json()
            logger.debug('clusters:', json_rsp)

            for cluster_json in json_rsp['cluster']:
                logger.debug("cluster_json:", cluster_json)
                cluster = rhevm.Cluster(cluster_json['name'], cluster_json['id'])
                cluster.cpu_architecture = cluster_json['cpu']['architecture']
                cluster.cpu_type = cluster_json['cpu']['id']
                cluster.major_version = cluster_json['version']['major']
                cluster.minor_version = cluster_json['version']['minor']
                if cluster_json.get('description', 0):
                    cluster.description = cluster_json['description']
                cluster.datacenter_id = cluster_json['data_center']['id']
                clusters.append(cluster)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error('Failed to discover cluster. Exception: <%s>' % excInfo)
        finally:
            return clusters
