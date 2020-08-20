import sys
import logger
import azure_discoverer
import azure_client
import re

from appilog.common.system.types.vectors import ObjectStateHolderVector
from com.hp.ucmdb.discovery.common import CollectorsConstants


dict_subscription_osh = {}
dict_resource_group_osh = {}
dict_public_ip = {}
dict_subnet_osh = {}
dict_subnet_network_osh = {}
dict_interface_public_ip_osh = {}
dict_interface_private_ip_osh = {}
dict_location_vmsize = {}
dict_storage_osh = {}
dict_interface_osh = {}


def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()

    endpoint = Framework.getDestinationAttribute('endpoint').lower()
    ip = Framework.getDestinationAttribute('ip')
    protocols = Framework.getAvailableProtocols(ip, "http")
    proxies = {}

    if len(protocols) == 0:
        msg = 'Protocol not defined or IP out of protocol network range'
        logger.reportWarning(msg)
        logger.error(msg)
        return OSHVResult

    for protocol in protocols:
        try:
            username = Framework.getProtocolProperty(protocol, CollectorsConstants.PROTOCOL_ATTRIBUTE_USERNAME)
            password = Framework.getProtocolProperty(protocol, CollectorsConstants.PROTOCOL_ATTRIBUTE_PASSWORD, "")
            http_proxy = Framework.getProtocolProperty(protocol, "proxy", "")
            if http_proxy:
                logger.debug("proxy:", http_proxy)
                proxies['http'] = http_proxy
                proxies['https'] = http_proxy

            cred = azure_client.AzureClientCredential(username, password)
            client = azure_client.AzureClient(endpoint, proxies)
            client.login_with_client_credentials(cred)
            do_discover(Framework, client, OSHVResult, endpoint)

        except:
            strException = str(sys.exc_info()[1])
            excInfo = logger.prepareJythonStackTrace('')
            logger.debug(strException)
            logger.debug(excInfo)
            pass

    reportError = OSHVResult.size() == 0
    if reportError:
        msg = 'Failed to connect using all protocols'
        logger.reportError(msg)
        logger.error(msg)
    return OSHVResult

def do_discover(Framework, client, OSHVResult, endpoint):
    tenant_discoverer = azure_discoverer.TenantDiscover(client)
    tenant = tenant_discoverer.discover()

    subscription_discoverer = azure_discoverer.SubscriptionDiscoverer(client)
    subscriptions = subscription_discoverer.discover()

    resource_groups = []
    resource_type_api_mapping = {}
    resource_group_discover = azure_discoverer.ResourceGroupDiscoverer(client)
    ApiVersionDiscover = azure_discoverer.ApiVersionDiscover(client)
    for subscription in subscriptions:
        resource_groups.extend(resource_group_discover.discover_by_subscription(subscription))
        resource_type_api_mapping = ApiVersionDiscover.discvoer_all_resources_api(subscription)

    vms = []
    public_ips = []
    networks = []
    interfaces = []
    storage_accounts = []
    sql_services = []
    vm_discover = azure_discoverer.VMDiscoverer(client, dict_location_vmsize, resource_type_api_mapping)
    vm_size_discover = azure_discoverer.VMSizeDiscoverer(client, resource_type_api_mapping)
    virtual_network_discover = azure_discoverer.VirtualNetworkDiscoverer(client, resource_type_api_mapping)
    network_interface_discover = azure_discoverer.NetworkInterfaceDiscoverer(client, resource_type_api_mapping)
    public_ip_discover = azure_discoverer.PublicIPAddressDiscoverer(client, resource_type_api_mapping)
    storage_account_discover = azure_discoverer.StorageAccountDiscoverer(client, resource_type_api_mapping)
    sql_account_discover = azure_discoverer.SqlSeriviceDiscoverer(client, resource_type_api_mapping)

    for resource_group in resource_groups:
        vm_sizes = dict_location_vmsize.get(resource_group.location, None)
        if vm_sizes:
            vm_sizes.update(vm_size_discover.discover_by_resource_group(resource_group))
        else:
            vm_sizes = vm_size_discover.discover_by_resource_group(resource_group)
            dict_location_vmsize[resource_group.location] = vm_sizes
    for resource_group in resource_groups:
        public_ips.extend(public_ip_discover.discover_by_resource_group(resource_group))
        networks.extend(virtual_network_discover.discover_by_resource_group(resource_group))
        interfaces.extend(network_interface_discover.discover_by_resource_group(resource_group))
        vms.extend(vm_discover.discover_by_resource_group(resource_group))
        storage_accounts.extend(storage_account_discover.discover_by_resource_group(resource_group))
        sql_services.extend(sql_account_discover.discover_sql_by_resource_group(resource_group))

    tenant_osh, vector = tenant.report(endpoint)
    OSHVResult.addAll(vector)
    OSHVResult.addAll(report_subscriptions(subscriptions, tenant_osh))
    OSHVResult.addAll(report_resource_groups(resource_groups))
    OSHVResult.addAll(report_public_ip(public_ips))
    OSHVResult.addAll(report_networks(networks))
    OSHVResult.addAll(report_storage_accounts(storage_accounts))
    OSHVResult.addAll(report_vms(vms, interfaces))
    OSHVResult.addAll(report_sqlservices(Framework, sql_services))

    if OSHVResult:
        return OSHVResult

def report_subscriptions(subscriptions, container):
    vector = ObjectStateHolderVector()
    for subscription in subscriptions:
        subscription_osh = subscription.report(container)
        dict_subscription_osh[subscription.id] = subscription_osh
        vector.add(subscription_osh)
    return vector

def report_resource_groups(resource_groups):
    vector = ObjectStateHolderVector()
    for resource_group in resource_groups:
        subscription_osh = dict_subscription_osh.get(resource_group.subscription_id, None)
        if subscription_osh:
            resource_group_osh = resource_group.report(subscription_osh)
            dict_resource_group_osh[resource_group.id] = resource_group_osh
            vector.add(resource_group_osh)
    return vector

def report_networks(networks):
    vector = ObjectStateHolderVector()
    for network in networks:
        resource_group_osh = dict_resource_group_osh.get(network.resource_group_id, None)
        if resource_group_osh:
            network_osh = network.report(resource_group_osh)
            vector.add(network_osh)
            if network.subnets:
                for subnet in network.subnets:
                    subnet_osh, subnet_vector = subnet.report(network_osh)
                    dict_subnet_osh[subnet.id] = subnet_osh
                    dict_subnet_network_osh[subnet.id] = network_osh
                    vector.addAll(subnet_vector)
    return vector

def report_public_ip(public_ips):
    vector = ObjectStateHolderVector()
    for public_ip in public_ips:
        ip_address_osh = public_ip.report()
        dict_public_ip[public_ip.id] = ip_address_osh
        vector.add(ip_address_osh)
    return vector

def report_storage_accounts(storage_accounts):
    vector = ObjectStateHolderVector()
    for storage_account in storage_accounts:
        resource_group_osh = dict_resource_group_osh.get(storage_account.resource_group_id, None)
        storage_account_osh = storage_account.report(resource_group_osh)
        dict_storage_osh[storage_account.name] = storage_account_osh
        vector.add(storage_account_osh)
    return vector

def report_vms(vms, interfaces):
    vector = ObjectStateHolderVector()
    report_interface(interfaces)
    for vm in vms:
        resource_group_osh = dict_resource_group_osh.get(vm.resource_group_id, None)
        dict_vm_sizes = dict_location_vmsize.get(vm.location, None)
        if dict_vm_sizes:
            vm.vm_size = dict_vm_sizes.get(vm.hardware_profile, None)
        ip_oshs = []
        for interface_id in vm.interface_ids:
            public_ip_osh = dict_interface_public_ip_osh.get(interface_id, None)
            if public_ip_osh:
                ip_oshs.append(public_ip_osh)
            private_ip_osh = dict_interface_private_ip_osh.get(interface_id, None)
            if private_ip_osh:
                ip_oshs.append(private_ip_osh)
        vector.addAll(vm.report(resource_group_osh, ip_oshs, dict_storage_osh, dict_interface_osh))
    return vector

def report_sqlservices(Framework, sql_services):
    vector = ObjectStateHolderVector()
    for sql_service in sql_services:
        vector.addAll(sql_service.report(Framework, dict_resource_group_osh.get(sql_service.resource_group_id, None)))
    return vector


def report_interface(interfaces):
    for interface in interfaces:
        public_ip_osh = None
        if interface.ip_config.public_ip_id:
            public_ip_osh = dict_public_ip.get(interface.ip_config.public_ip_id, None)
        private_ip_osh, interface_osh = interface.report()
        if public_ip_osh:
            dict_interface_public_ip_osh[interface.id] = public_ip_osh
        if private_ip_osh:
            dict_interface_private_ip_osh[interface.id] = private_ip_osh
        dict_interface_osh[interface.id] = interface_osh