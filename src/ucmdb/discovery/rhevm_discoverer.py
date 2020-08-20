import logger
import ip_addr
import rest_requests as requests
import rhevm

from com.hp.ucmdb.discovery.common import CollectorsConstants


class RHEVMClient:
    def __init__(self, framework, protocol):
        self.endpoint = framework.getDestinationAttribute('endpoint')
        self.ip = framework.getDestinationAttribute('ip')

        self.proxies = {}
        self.api_endpoint = self.endpoint + '/ovirt-engine/api/'

        self.username = framework.getProtocolProperty(protocol, CollectorsConstants.PROTOCOL_ATTRIBUTE_USERNAME)
        self.credential = framework.getProtocolProperty(protocol, CollectorsConstants.PROTOCOL_ATTRIBUTE_PASSWORD)
        http_proxy = framework.getProtocolProperty(protocol, 'proxy', '')

        if http_proxy:
            self.proxies['http'] = http_proxy
            self.proxies['https'] = http_proxy

        self.headers = {
            'Accept': 'application/json'
        }
        self.auth_endpoint = self.endpoint + '/ovirt-engine/sso/oauth/token'
        self.auth_token = None
        self.token_type = None

    # added by Pierre
    def authenticate(self):
        headers = dict(self.headers)
        data = {}
        data['grant_type'] = 'password'
        data['scope'] = 'ovirt-app-api'
        data['username'] = self.username
        data['password'] = self.credential

        headers['Content-Type'] = 'application/x-www-form-urlencoded'
        auth_resp = requests.post(self.auth_endpoint,
                                  headers=headers,
                                  proxies=self.proxies,
                                  data=data,
                                  debug=True,
                                  verify=False)

        # we need to capture set the token, but first, let's see if one comes in
        if auth_resp:
            json_content = auth_resp.json()
            if json_content.get('access_token'):
                logger.debug('Pi Debug - Set Access Token [%s]' % json_content.get('access_token'))
                self.auth_token = json_content['access_token']
            if json_content.get('token_type'):
                logger.debug('Pi Debug - Set Token Type [%s]' % json_content.get('token_type'))
                # First letter in token type has to be uppercased in order to make things work
                ttype = json_content.get('token_type')
                self.token_type = ttype[:1].upper() + ttype[1:]


class BaseDiscoverer:
    def __init__(self, client):
        if not client:
            raise ValueError('No RHEVMClient passed.')
        self.client = client

    def discover(self):
        raise NotImplemented, 'discover'

    def __repr__(self):
        return 'BaseDiscoverer (RHEVMClient: %s)' % self.client.endpoint


class RHEVMDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        rsp = requests.get(self.client.api_endpoint, auth=(self.client.username, self.client.credential),
                           headers=self.client.headers, proxies=self.client.proxies,
                           token=(self.client.auth_token, self.client.token_type), debug=True, verify=False)
        json_rsp = rsp.json()
        logger.debug('api:', json_rsp)
        name = json_rsp['product_info']['name']
        redhat = rhevm.RHEVM(name)
        try:
            redhat.major_version = json_rsp['product_info']['version']['major']
            redhat.minor_version = json_rsp['product_info']['version']['minor']

            redhat.vendor = json_rsp['product_info']['vendor']
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error('Failed to discover Red Hat Vitualization detailed data. Exception: <%s>' % excInfo)
        finally:
            return redhat


class DataCenterDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        dcs = []
        try:
            rsp = requests.get(self.client.api_endpoint + 'datacenters',
                               auth=(self.client.username, self.client.credential), headers=self.client.headers,
                               proxies=self.client.proxies, token=(self.client.auth_token, self.client.token_type),
                               debug=True, verify=False)
            json_rsp = rsp.json()
            logger.debug('datacenters:', json_rsp)

            for dc_json in json_rsp['data_center']:
                dc = rhevm.DataCenter(dc_json['name'], dc_json['id'])
                dc.status = dc_json['status']
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


class ClusterDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        clusters = []
        try:
            rsp = requests.get(self.client.api_endpoint + 'clusters',
                               auth=(self.client.username, self.client.credential), headers=self.client.headers,
                               proxies=self.client.proxies, token=(self.client.auth_token, self.client.token_type),
                               debug=True, verify=False)
            json_rsp = rsp.json()
            logger.debug('clusters:', json_rsp)

            for cluster_json in json_rsp['cluster']:
                cluster = rhevm.Cluster(cluster_json['name'], cluster_json['id'])
                cluster.cpu_architecture = cluster_json['cpu']['architecture']
                cluster.cpu_type = cluster_json['cpu']['type']
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


class NetworkDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        networks = []
        try:
            rsp = requests.get(self.client.api_endpoint + 'networks',
                               auth=(self.client.username, self.client.credential), headers=self.client.headers,
                               proxies=self.client.proxies, token=(self.client.auth_token, self.client.token_type),
                               debug=True, verify=False)
            json_rsp = rsp.json()
            logger.debug('networks:', json_rsp)

            for network_json in json_rsp['network']:
                network = rhevm.Network(network_json['name'], network_json['id'])
                if network_json.get('description', 0):
                    network.description = network_json['description']
                network.datacenter_id = network_json['data_center']['id']
                networks.append(network)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error('Failed to discover network. Exception: <%s>' % excInfo)
        finally:
            return networks


UNIX_TYPE_REGEX=r"(linux|rhel|unix)+"

import re
class HostDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        hosts = []
        try:
            rsp = requests.get(self.client.api_endpoint + 'hosts', auth=(self.client.username, self.client.credential),
                               headers=self.client.headers, proxies=self.client.proxies,
                               token=(self.client.auth_token, self.client.token_type), debug=True, verify=False)
            json_rsp = rsp.json()
            logger.debug('hosts:', json_rsp)

            for host_json in json_rsp['host']:
                os_type = 'host_node'
                os_type_str = str(host_json['os']['type'])
                if 'windows' in os_type_str.lower():
                    os_type = 'nt'
                elif re.search(UNIX_TYPE_REGEX, os_type_str):
                    os_type = 'unix'
                host = rhevm.Host(host_json['name'], host_json['id'], os_type)
                host.cluster_id = host_json['cluster']['id']
                links_json = host_json['link']
                for link_json in links_json:
                    host.links[link_json['rel']] = link_json['href']

                if host.links.get('nics', None):
                    host.interfaces = InterfaceDiscoverer(self.client).discover_by_url(host.links.get('nics'))
                hosts.append(host)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error('Failed to discover hosts. Exception: <%s>' % excInfo)
        finally:
            return hosts


class InterfaceDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        raise NotImplemented, 'discover'

    def discover_by_url(self, link):
        interfaces = []
        try:
            rsp = requests.get(self.client.endpoint + link, auth=(self.client.username, self.client.credential),
                               headers=self.client.headers, proxies=self.client.proxies,
                               token=(self.client.auth_token, self.client.token_type), debug=True, verify=False)
            json_rsp = rsp.json()
            logger.debug('nics:', json_rsp)

            for nic_json in json_rsp['host_nic']:
                logger.debug("host_interface_mac_check:", nic_json.get('mac', None))
                if nic_json.get('mac', None):
                    interface = rhevm.Interface(nic_json['mac']['address'])
                    interface.name = nic_json['name']
                    interface.ip_address = nic_json['ip']['address']
                    interface.ip_netmask = nic_json['ip']['netmask']
                    if nic_json.get('speed', None):
                        interface.speed = nic_json['speed']
                    if nic_json.get('network', None):
                        interface.network_id = nic_json['network']['id']
                    interfaces.append(interface)
                else:
                    logger.debug("skip interface %s due to no mac address." % nic_json['name'])
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error('Failed to discover host interface. Exception: <%s>' % excInfo)
        finally:
            return interfaces


class VMPoolDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        vm_pools = []
        try:
            rsp = requests.get(self.client.api_endpoint + 'vmpools',
                               auth=(self.client.username, self.client.credential), headers=self.client.headers,
                               proxies=self.client.proxies, token=(self.client.auth_token, self.client.token_type),
                               debug=True, verify=False)
            json_rsp = rsp.json()
            logger.debug('vmpools:', json_rsp)

            for pool_json in json_rsp['vm_pool']:
                vm_pool = rhevm.VMPool(pool_json['name'], pool_json['id'])
                vm_pool.max_user_vms = pool_json['max_user_vms']
                vm_pool.size = pool_json['size']
                vm_pool.prestarted_vms = pool_json['prestarted_vms']
                vm_pool.cluster_id = pool_json['cluster']['id']
                vm_pools.append(vm_pool)

        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error('Failed to discover vm pool. Exception: <%s>' % excInfo)
        finally:
            return vm_pools


class VNICProfileDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        vnic_profiles = []
        try:
            rsp = requests.get(self.client.api_endpoint + 'vnicprofiles',
                               auth=(self.client.username, self.client.credential), headers=self.client.headers,
                               proxies=self.client.proxies, token=(self.client.auth_token, self.client.token_type),
                               debug=True, verify=False)
            json_rsp = rsp.json()
            logger.debug('vnicprofiles:', json_rsp)

            for profile_json in json_rsp['vnic_profile']:
                vnic_profile = rhevm.VNICProfile(profile_json['name'], profile_json['id'])
                vnic_profile.network_id = profile_json['network']['id']
                vnic_profiles.append(vnic_profile)

        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error('Failed to discover vnic profile. Exception: <%s>' % excInfo)
        finally:
            return vnic_profiles


class VirtualMachineDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        vms = []
        try:
            rsp = requests.get(self.client.api_endpoint + 'vms', auth=(self.client.username, self.client.credential),
                               headers=self.client.headers, proxies=self.client.proxies,
                               token=(self.client.auth_token, self.client.token_type), debug=True, verify=False)
            json_rsp = rsp.json()
            logger.debug('vms:', json_rsp)

            for vm_json in json_rsp['vm']:
                vm = rhevm.VirtualMachine(vm_json['name'], vm_json['id'])
                if vm_json.get('host', 0):
                    vm.host_id = vm_json['host']['id']
                vm.cluster_id = vm_json['cluster']['id']
                vm.cpu_architecture = vm_json['cpu']['architecture']
                vm.cpu_core = vm_json['cpu']['topology']['cores']
                vm.cpu_socket = vm_json['cpu']['topology']['sockets']
                vm.cpu_thread = vm_json['cpu']['topology']['threads']
                vm.memory = vm_json['memory']
                if vm_json.get('vm_pool', None):
                    vm.vm_pool_id = vm_json['vm_pool']['id']

                links_json = vm_json['link']
                for link_json in links_json:
                    vm.links[link_json['rel']] = link_json['href']

                if vm.links.get('nics', None):
                    vm.interfaces = VMInterfaceDiscoverer(self.client).discover_by_url(vm.links.get('nics'))
                if vm.links.get('diskattachments', None):
                    vm.disk_ids = VMDiskDiscoverer(self.client).discover_by_url(vm.links.get('diskattachments'))

                vms.append(vm)

        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error('Failed to discover virtual machine. Exception: <%s>' % excInfo)
        finally:
            return vms


class VMInterfaceDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        raise NotImplemented, 'discover'

    def discover_by_url(self, link):
        interfaces = []
        try:
            rsp = requests.get(self.client.endpoint + link, auth=(self.client.username, self.client.credential),
                               headers=self.client.headers, proxies=self.client.proxies,
                               token=(self.client.auth_token, self.client.token_type), debug=True, verify=False)
            json_rsp = rsp.json()
            logger.debug('vm nics:', json_rsp)

            if json_rsp.get('nic', 0):
                for nic_json in json_rsp['nic']:
                    interface = rhevm.Interface(nic_json['mac']['address'])
                    interface.name = nic_json['name']
                    interface.vnic_profile_id = nic_json['vnic_profile']['id']
                    interfaces.append(interface)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error('Failed to discover virtua machine interface. Exception: <%s>' % excInfo)
        finally:
            return interfaces


class VMDiskDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        raise NotImplemented, 'discover'

    def discover_by_url(self, link):
        disk_ids = []
        try:
            rsp = requests.get(self.client.endpoint + link, auth=(self.client.username, self.client.credential),
                               headers=self.client.headers, proxies=self.client.proxies,
                               token=(self.client.auth_token, self.client.token_type), debug=True, verify=False)
            json_rsp = rsp.json()
            logger.debug('vm disk_attachment:', json_rsp)

            if json_rsp.get('disk_attachment', 0):
                for disk_json in json_rsp['disk_attachment']:
                    disk_ids.append(disk_json['disk']['id'])

        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error('Failed to discover virtual machine interface. Exception: <%s>' % excInfo)
        finally:
            return disk_ids


class StorageDomainDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        storages = []
        try:
            rsp = requests.get(self.client.api_endpoint + 'storagedomains',
                               auth=(self.client.username, self.client.credential), headers=self.client.headers,
                               proxies=self.client.proxies, token=(self.client.auth_token, self.client.token_type),
                               debug=True, verify=False)
            json_rsp = rsp.json()
            logger.debug('storagedomains:', json_rsp)

            for storage_json in json_rsp['storage_domain']:
                storage = rhevm.LogicalVolume(storage_json['name'], storage_json['id'])
                storage.datacenter_id = storage_json['data_centers']['data_center'][0]['id']
                storage.available = storage_json['available']
                storage.used = storage_json['used']
                storage.type = storage_json['storage']['type']
                if storage.type == 'nfs':
                    storage.client_ip = storage_json['storage']['address']
                    storage.path = storage_json['storage']['path']

                storages.append(storage)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error('Failed to discover data center. Exception: <%s>' % excInfo)
        finally:
            return storages


class DiskDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        disks = []
        try:
            rsp = requests.get(self.client.api_endpoint + 'disks', auth=(self.client.username, self.client.credential),
                               headers=self.client.headers, proxies=self.client.proxies,
                               token=(self.client.auth_token, self.client.token_type), debug=True, verify=False)
            json_rsp = rsp.json()
            logger.debug('disks:', json_rsp)

            for disk_json in json_rsp['disk']:
                disk = rhevm.Disk(disk_json['name'], disk_json['id'])
                disk.actual_size = disk_json['actual_size']
                disk.storage_domain_id = disk_json['storage_domains']['storage_domain'][0]['id']
                disks.append(disk)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.error('Failed to discover disk. Exception: <%s>' % excInfo)
        finally:
            return disks
