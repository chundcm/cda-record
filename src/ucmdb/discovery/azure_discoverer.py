import logger
import azure


class BaseDiscoverer:
    def __init__(self, client):
        if not client:
            raise ValueError('No client passed.')
        self.client = client
        self.api_url = "https://management.azure.com/"
        self.api_version = '2015-11-01'

    def discover(self):
        raise NotImplemented, "discover"

    def get_api_version(self, resource_type, resource_type_api_mapping):
        if resource_type_api_mapping.has_key(resource_type):
            return resource_type_api_mapping[resource_type][0]

    def __repr__(self):
        return "BaseDiscoverer (client: %s)" % self.client


class TenantDiscover(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        tenants_json = self.client.getValues(self.api_url + 'tenants?api-version=' + self.api_version)
        for tenant_json in tenants_json:
            logger.debug('found tenant: %s' % tenant_json)
            return azure.Tenant(tenant_json['tenantId'])


class SubscriptionDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover(self):
        subscriptions = []
        subscriptions_json = self.client.getValues(self.api_url + 'subscriptions?api-version=' + self.api_version)
        for subscription_json in subscriptions_json:
            logger.debug('found subscription: %s' % subscription_json)
            subscription = azure.Subscription(subscription_json['subscriptionId'])
            subscription.name = subscription_json['displayName']
            subscription.state = subscription_json['state']
            subscriptions.append(subscription)
        return subscriptions


class ResourceGroupDiscoverer(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)

    def discover_by_subscription(self, subscription):
        groups = []
        groups_json = self.client.getValues(
            self.api_url + 'subscriptions/' + subscription.id + '/resourcegroups?api-version=' + self.api_version)
        for group_json in groups_json:
            logger.debug('found resource group: %s' % group_json)
            resource_group = azure.ResourceGroup(group_json['name'])
            resource_group.subscription_id = subscription.id
            resource_group.id = group_json['id']
            resource_group.location = group_json['location']
            groups.append(resource_group)
        return groups


class VMSizeDiscoverer(BaseDiscoverer):
    def __init__(self, client, resource_type_api_mapping):
        BaseDiscoverer.__init__(self, client)
        self.api_version = self.get_api_version("locations/vmSizes", resource_type_api_mapping)

    def discover_by_resource_group(self, resource_group):
        sizes = {}
        sizes_json = self.client.getValues(self.api_url + 'subscriptions/' + resource_group.subscription_id + '/providers/Microsoft.Compute/locations/' + resource_group.location + '/vmSizes?api-version=' + self.api_version)
        for size_json in sizes_json:
            logger.debug('found vm size: %s' % size_json)
            name = size_json['name']
            vm_size = str(size_json)
            sizes[name] = vm_size
        return sizes


class VMDiscoverer(BaseDiscoverer):
    def __init__(self, client, dict_location_vmsize, resource_type_api_mapping):
        BaseDiscoverer.__init__(self, client)
        self.api_version = self.get_api_version("virtualMachines", resource_type_api_mapping)
        self.dict_location_vmsize = dict_location_vmsize

    def discover_by_resource_group(self, resource_group):
        vms = []
        vms_json = self.client.getValues(self.api_url + 'subscriptions/' + resource_group.subscription_id + '/resourceGroups/' + resource_group.name + '/providers/Microsoft.Compute/virtualmachines?api-version='  + self.api_version)
        for vm_json in vms_json:
            logger.debug('found vm: %s' % vm_json)
            vm = azure.VM(vm_json['name'])
            vm.resource_group_id = resource_group.id
            vm.id = vm_json['id']
            vm.location = vm_json['location']
            vm.hardware_profile = vm_json['properties']['hardwareProfile']['vmSize']
            dict_vm_size = self.dict_location_vmsize.get(vm.location, None)
            try:
                vm.osType = vm_json.get('properties').get('storageProfile').get('osDisk').get('osType')
            except:
                logger.debug('error to get osType')
            if dict_vm_size:
                vm.vm_size = dict_vm_size.get(vm.hardware_profile, None)
            vm_interfaces = vm_json['properties']['networkProfile']['networkInterfaces']
            for interface_json in vm_interfaces:
                vm.interface_ids.append(interface_json['id'])
            try:
                vm.osdDescription = vm_json['properties']['storageProfile']['imageReference']['sku']
                vm.location = vm_json['location']
                vm.discoveredDescription = vm_json['properties']['hardwareProfile']['vmSize']
                vm.discoveredOSVersion = vm_json['properties']['storageProfile']['imageReference']['version']
            except:
                logger.debug("failed to get os properties")
            vm.os_disk = azure.OSDisk(vm_json['properties']['storageProfile']['osDisk']['name'])
            vm.os_disk.os_type = vm_json['properties']['storageProfile']['osDisk']['osType']

            data_disks_json = vm_json['properties']['storageProfile']['dataDisks']
            for data_disk_json in data_disks_json:
                logger.debug("data_disk_json:", data_disk_json)
                data_disk = azure.DataDisk(data_disk_json["name"])
                vm.data_disks.append(data_disk)
            logger.debug("vm.data_disks:", vm.data_disks)
            vms.append(vm)
        return vms


class VirtualNetworkDiscoverer(BaseDiscoverer):
    def __init__(self, client, resource_type_api_mapping):
        BaseDiscoverer.__init__(self, client)
        self.api_version = self.get_api_version("virtualNetworks", resource_type_api_mapping)

    def discover_by_resource_group(self, resource_group):
        networks = []
        networks_json = self.client.getValues(self.api_url + 'subscriptions/' + resource_group.subscription_id + '/resourceGroups/' + resource_group.name + '/providers/Microsoft.Network/virtualnetworks?api-version=' + self.api_version)
        for network_json in networks_json:
            logger.debug('found virtual network: %s' % network_json)
            network = azure.Network(network_json['name'])
            network.id = network_json['id']
            network.resource_group_id = resource_group.id
            network.location = network_json['location']
            network.address_prefixes = network_json['properties']['addressSpace']['addressPrefixes']
            for subnet_json in network_json['properties']['subnets']:
                logger.debug("subnet_json:", subnet_json)
                subnet = azure.Subnet(subnet_json['name'])
                subnet.id = subnet_json['id']
                address_prefix = subnet_json['properties']['addressPrefix']
                subnet.address_prefix = address_prefix.split('/')[0]
                subnet.address_prefix_length = int(address_prefix.split('/')[1])
                if subnet_json['properties'].get('ipConfigurations'):
                    for ip_config in subnet_json['properties']['ipConfigurations']:
                        subnet.ip_ids.append(ip_config['id'])
                network.subnets.append(subnet)
            networks.append(network)
        return networks


class NetworkInterfaceDiscoverer(BaseDiscoverer):
    def __init__(self, client, resource_type_api_mapping):
        BaseDiscoverer.__init__(self, client)
        self.api_version = self.get_api_version("networkInterfaces", resource_type_api_mapping)

    def discover_by_resource_group(self, resource_group):
        interfaces = []
        interfaces_json = self.client.getValues(self.api_url + 'subscriptions/' + resource_group.subscription_id + '/resourceGroups/' + resource_group.name + '/providers/Microsoft.Network/networkInterfaces?api-version=' + self.api_version)
        for interface_json in interfaces_json:
            logger.debug('found network interface: %s' % interface_json)
            interface = azure.Interface(interface_json['name'])
            interface.id = interface_json['id']
            interface.resource_group_id = resource_group.id
            if interface_json['properties'].get('macAddress'):
                interface.mac_address = interface_json['properties']['macAddress']
            ip_config_json = interface_json['properties']['ipConfigurations'][0]
            ip_config = azure.IPConfiguration(ip_config_json['name'])
            ip_config.id = ip_config_json['id']
            ip_config.subnet_id = ip_config_json['properties']['subnet']['id']
            ip_config.ip_address = ip_config_json['properties']['privateIPAddress']
            if ip_config_json['properties'].get('publicIPAddress'):
                ip_config.public_ip_id = ip_config_json['properties']['publicIPAddress']['id']
            interface.ip_config = ip_config
            interfaces.append(interface)
        return interfaces


class PublicIPAddressDiscoverer(BaseDiscoverer):
    def __init__(self, client, resource_type_api_mapping):
        BaseDiscoverer.__init__(self, client)
        self.api_version = self.get_api_version("publicIPAddresses", resource_type_api_mapping)

    def discover_by_resource_group(self, resource_group):
        ips = []
        ips_json = self.client.getValues(self.api_url + 'subscriptions/' + resource_group.subscription_id + '/resourceGroups/' + resource_group.name + '/providers/Microsoft.Network/publicIPAddresses?api-version=' + self.api_version)
        for ip_json in ips_json:
            logger.debug('found public ip address: %s' % ip_json)
            if ip_json['properties'].get('ipAddress'):
                ip = azure.IPAddress(ip_json['name'])
                ip.id = ip_json['id']
                ip.location = ip_json['location']
                ip.ip_address = ip_json['properties']['ipAddress']
                ips.append(ip)
        return ips


class StorageAccountDiscoverer(BaseDiscoverer):
    def __init__(self, client, resource_type_api_mapping):
        BaseDiscoverer.__init__(self, client)
        self.api_version = self.get_api_version("storageAccounts", resource_type_api_mapping)

    def discover_by_resource_group(self, resource_group):
        accounts = []
        accounts_json = self.client.getValues(self.api_url + 'subscriptions/' + resource_group.subscription_id + '/resourceGroups/' + resource_group.name + '/providers/Microsoft.Storage/storageAccounts?api-version=' + self.api_version)
        for account_json in accounts_json:
            logger.debug('found storage account: %s' % account_json)
            storage_account = azure.StorageAccount(account_json['name'])
            storage_account.id = account_json['id']
            storage_account.location = account_json['location']
            storage_account.resource_group_id = resource_group.id
            accounts.append(storage_account)
        return accounts

class SqlServerDiscoverer(BaseDiscoverer):
    def __init__(self, client, resource_type_api_mapping):
        BaseDiscoverer.__init__(self, client)
        self.api_version = self.get_api_version("servers/databases", resource_type_api_mapping)

    def discover_sqlserver_by_resource_group(self, resource_group):
        sqlservers = []
        serversqls_json = self.client.getValues(self.api_url + 'subscriptions/' + resource_group.subscription_id + '/resourceGroups/' + resource_group.name + '/providers/Microsoft.Sql/servers?api-version=' + self.api_version)
        dbs_json = self.client.getValues(self.api_url +  'subscriptions/' + resource_group.subscription_id  + '/resources?api-version=2015-11-01')
        logger.debug("dbs_json is :", dbs_json)
        for sqlserver_json in serversqls_json:
            sqlserver_name = sqlserver_json['name']
            sqlservers.append(sqlserver_name)
        return sqlservers

class SqlSeriviceDiscoverer(BaseDiscoverer):
    def __init__(self, client, resource_type_api_mapping):
        BaseDiscoverer.__init__(self, client)
        self.resource_type_api_mapping = resource_type_api_mapping
        self.api_version = self.get_api_version("servers/databases", resource_type_api_mapping)

    def discover_sql_by_resource_group(self, resource_group):
        sql_databases = []
        sqlServerDiscoverer = SqlServerDiscoverer(self.client, self.resource_type_api_mapping)
        sqlservers = sqlServerDiscoverer.discover_sqlserver_by_resource_group(resource_group)
        for sqlserver in sqlservers:
            serversqls = self.client.getValues(self.api_url + 'subscriptions/' + resource_group.subscription_id + '/resourceGroups/' + resource_group.name + '/providers/Microsoft.Sql/servers/' + sqlserver + '/databases?api-version=' + self.api_version)
            for serversql in serversqls:
                if serversql['name'] == 'master':
                    logger.debug('Embedded sql database found, skip it!')
                    continue
                sqlservice = azure.SqlDatabase(serversql['name'], serversql['id'])
                sqlservice.resource_group_id = resource_group.id
                sqlservice.server_address = sqlserver
                sql_databases.append(sqlservice)
        return sql_databases

class ApiVersionDiscover(BaseDiscoverer):
    def __init__(self, client):
        BaseDiscoverer.__init__(self, client)
        self.api_version = '2015-01-01'
        self.namespace_list = ['Microsoft.Compute', 'Microsoft.Network', 'Microsoft.Storage', 'Microsoft.Sql']

    def discvoer_all_resources_api(self, subscription):
        resource_type_api_mapping = {}
        all_providers_json = self.client.getValues(self.api_url + 'subscriptions/' + subscription.id + '/providers?api-version=' + self.api_version)
        for enty in all_providers_json:
            resouce_types = enty['resourceTypes']
            if resouce_types and enty['namespace'] in self.namespace_list:
                for resouce_type in resouce_types:
                    if resouce_type['resourceType'] and resouce_type['apiVersions']:
                        resource_type_api_mapping[resouce_type['resourceType']] = resouce_type['apiVersions']
        return resource_type_api_mapping