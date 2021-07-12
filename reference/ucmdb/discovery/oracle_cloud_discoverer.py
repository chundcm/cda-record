# coding=utf-8
import rest_requests as requests
import logger
import oracle_cloud
import netutils
import db

from com.hp.ucmdb.discovery.common import CollectorsConstants
from appilog.common.system.types.vectors import ObjectStateHolderVector

REGIONS = {}


class OracleCloudClient():
    def __init__(self, framework, protocol, region):
        self.endpoint = 'https://compute.%s.oraclecloud.com/' % region.name
        self.region = region

        self.username = framework.getProtocolProperty(protocol, CollectorsConstants.PROTOCOL_ATTRIBUTE_USERNAME, '')
        self.credential = framework.getProtocolProperty(protocol, CollectorsConstants.PROTOCOL_ATTRIBUTE_PASSWORD, '')

        list = self.username.split('/')
        self.container = list[1]
        self.user = list[2]

        self.proxies = None
        http_proxy = framework.getProtocolProperty(protocol, 'proxy', '')

        if http_proxy:
            self.proxies = {}
            self.proxies['http'] = http_proxy
            self.proxies['https'] = http_proxy

        headers = {
            'Content-Type': 'application/oracle-compute-v3+json'
        }
        self.data = '{"user":"%s","password":"%s"}' % (self.username, self.credential)
        rsp = requests.post(self.endpoint + '/authenticate/', headers=headers, proxies=self.proxies, data=self.data, debug=False, verify=False)

        logger.debug('status_code:', rsp.status_code)
        if rsp.status_code >= 401:
            raise Exception('Failed to connect to region %s' % region.name)

        self.cookie = rsp.headers.get('Set-Cookie', None)
        logger.debug('cookie:', self.cookie)

        self.headers = {
            'Cookie': self.cookie,
            'Accept': 'application/oracle-compute-v3+json'
        }

    def get_response(self, url, headers=None):
        try:
            if not headers:
                headers = self.headers
            logger.debug('url:', self.endpoint + url)
            rsp = requests.get(self.endpoint + url, headers=headers, proxies=self.proxies, debug=False, verify=False)
            rsp_json = rsp.json()
            logger.debug('results:', rsp_json)
            if rsp_json and rsp_json.get('result', None):
                return rsp_json['result']
            return []
        except:
            logger.debug('Failed to access: %s' % url)
            logger.debug(logger.prepareJythonStackTrace(''))
            return []

    def list_accounts(self):
        logger.debug('list_accounts:')
        return self.get_response('account/' + self.container + '/')

    def list_instances(self):
        logger.debug('list_instances:')
        return self.get_response('instance' + self.username + '/')

    def list_ip_networks(self):
        logger.debug('list_ip_networks:')
        return self.get_response('network/v1/ipnetwork/' + self.container + '/')

    def list_vnics(self):
        logger.debug('list_vnics:')
        return self.get_response('network/v1/vnic' + self.username + '/')

    def list_ip_reservations(self):
        logger.debug('list_ip_reservations:')
        return self.get_response('network/v1/ipreservation' + self.username + '/')

    def list_shapes(self):
        logger.debug('list_shapes:')
        return self.get_response('shape/')

    def list_images(self, name):
        logger.debug('list_images:')
        return self.get_response('imagelist' + name)

    def list_storage_volumes(self):
        logger.debug('list_storage_volumes:')
        return self.get_response('storage/volume' + self.username + '/')

    def list_storage_attachments(self):
        logger.debug('list_storage_attachments:')
        return self.get_response('storage/attachment' + self.username + '/')

    def list_storage_snapshots(self):
        logger.debug('list_storage_snapshots:')
        return self.get_response('storage/snapshot' + self.username + '/')


class OracleCloudDiscoverer:
    def __init__(self, client):
        if not client:
            raise ValueError('No client passed.')
        self.client = client
        self.region = client.region
        self.accountByName = {}
        self.ipNetworkByName = {}
        self.vnicByName = {}
        self.ipReservationByName = {}
        self.instanceByName = {}
        self.shapeByName = {}
        self.imageByName = {}
        self.storageAttachmentByVolumeName = {}
        self.volumeByName = {}
        self.availability_domainByName = {}

    def discover(self):
        self.discover_accounts()
        self.discover_shapes()
        self.discover_ip_networks()
        # self.discover_vnics()
        self.discover_ip_reservations()
        self.discover_storage_volumes()
        self.discover_instances()
        self.discover_storage_attachments()

    def discover_instances(self):
        self.parse_instances(self.client.list_instances())

    def parse_instances(self, instances):
        for item in instances:
            try:
                name = item['name']
                instance = oracle_cloud.Instance(name)
                self.instanceByName[name] = instance
                instance.hostname = item['hostname']
                instance.ip_address = item['ip']
                instance.domain = item['domain']
                instance.availability_domain = item['availability_domain']
                instance.label = item['label']
                instance.platform = item['platform']
                instance.account = self.accountByName.get(item['account'], None)
                instance.hypervisor_mode = item['hypervisor']['mode']
                network_json = item['networking']['eth0']
                if network_json.get('ipnetwork', None):
                    instance.ip_network = self.ipNetworkByName.get(network_json['ipnetwork'], None)
                if network_json.get('vnic', None):
                    instance.vnic = self.vnicByName.get(item['networking']['eth0']['vnic'], None)
                    if not instance.vnic:
                        vnic_addresses = item['attributes']['network']['nimbula_vcable-eth0']['address']
                        for address in vnic_addresses:
                            if netutils.isValidMac(address):
                                instance.vnic = oracle_cloud.Interface(item['networking']['eth0']['vnic'], address)
                                self.vnicByName[item['networking']['eth0']['vnic']] = instance.vnic
                nats = item['networking']['eth0']['nat']
                if nats:
                    for nat in nats:
                        if nat.startswith('network/v1/ipreservation:'):
                            instance.ip_reservations.append(self.ipReservationByName.get(nat.split(':')[1], None))
                instance.shape = self.shapeByName.get(item['shape'], None)
                availability_domain = item['availability_domain'][1:]
                if self.availability_domainByName.get(availability_domain, None):
                    instance.availability_domain = self.availability_domainByName.get(availability_domain)
                else:
                    instance.availability_domain = oracle_cloud.AvailabilityDomain(availability_domain)
                    self.availability_domainByName[availability_domain] = instance.availability_domain

                if instance.availability_domain.name.startswith(self.region.name):
                    instance.availability_domain.region = self.region

                for storage_attachment in item['storage_attachments']:
                    if storage_attachment.get('storage_volume_name', None):
                        instance.storage_volumes.append(self.volumeByName.get(storage_attachment['storage_volume_name'], None))
                imagelist = item['imagelist']
                if imagelist:
                    if self.imageByName.get(imagelist, None):
                        instance.imagelist = self.imageByName.get(imagelist)
                    else:
                        instance.imagelist = oracle_cloud.Image(imagelist)
                        self.imageByName[instance.imagelist] = instance.imagelist

                if item.get('attributes', None):
                    if item['attributes'].get('componentType', None) and item['attributes']['componentType'] == 'mysql':
                        instance.db_server = self.parse_mysql(item['attributes'])
                    elif item['attributes'].get('ords_config', None) and item['attributes']['ords_config'] == 'yes':
                        instance.db_server = self.parse_oracle(item['attributes'])
            except:
                logger.debug('Failed to parse instance %s' % item)
                logger.debug(logger.prepareJythonStackTrace(''))

    def parse_mysql(self, item):
        endpoint_ip_address = None
        mysql_ip_addresses = item['network']['nimbula_vcable-eth0']['address']
        for ip_address in mysql_ip_addresses:
            if netutils.isValidIp(ip_address):
                endpoint_ip_address = ip_address
        if item.get('userdata', None):
            endpoint_port = item['userdata']['msaas_admin_port']
            db_name = item['userdata']['msaas_db_name']
            version = item['userdata']['mysql_version']
            databases = db_name and [db.Database(db_name)]

            if endpoint_ip_address:
                return oracle_cloud.DBInstance('mysql', None, endpoint_ip_address, endpoint_port, version, databases=databases)

    def parse_oracle(self, item):
        endpoint_ip_address = None
        oracle_ip_addresses = item['network']['nimbula_vcable-eth0']['address']
        for ip_address in oracle_ip_addresses:
            if netutils.isValidIp(ip_address):
                endpoint_ip_address = ip_address
        endpoint_port = item['lsnr_port']
        sid = item['sid']
        dbname = item['dbname']
        pdb_name = item['pdb_name']
        edition = item['edition']
        version = item['version']
        if endpoint_ip_address:
            return oracle_cloud.DBInstance('oracle', sid, endpoint_ip_address, endpoint_port, version, edition)

    def discover_ip_networks(self):
        self.parse_ip_networks(self.client.list_ip_networks())

    def parse_ip_networks(self, networks):
        for item in networks:
            try:
                name = item['name']
                network = oracle_cloud.IpNetwork(name, item['ipAddressPrefix'])
                network.description = item['description']
                self.ipNetworkByName[name] = network
            except:
                logger.debug('Failed to parse ip network %s' % item)
                logger.debug(logger.prepareJythonStackTrace(''))

    def discover_vnics(self):
        self.parse_vnics(self.client.list_vnics())

    def parse_vnics(self, vnics):
        for item in vnics:
            try:
                name = item['name']
                vnic = oracle_cloud.Interface(name, item['macAddress'])
                vnic.description = item['description']
                self.vnicByName[name] = vnic
            except:
                logger.debug('Failed to parse vnic %s' % item)
                logger.debug(logger.prepareJythonStackTrace(''))

    def discover_ip_reservations(self):
        self.parse_ip_reservations(self.client.list_ip_reservations())

    def parse_ip_reservations(self, ip_reservations):
        for item in ip_reservations:
            try:
                name = item['name']
                ip_reservation = oracle_cloud.IpReservation(name, item['ipAddress'])
                ip_reservation.pool = item['ipAddressPool']
                self.ipReservationByName[name] = ip_reservation
            except:
                logger.debug('Failed to parse ip reservation %s' % item)
                logger.debug(logger.prepareJythonStackTrace(''))

    def discover_shapes(self):
        self.parse_shapes(self.client.list_shapes())

    def parse_shapes(self, shapes):
        for item in shapes:
            try:
                name = item['name']
                shape = oracle_cloud.Shape(name)
                shape.cpus = item['cpus']
                shape.gpus = item['gpus']
                shape.ram = item['ram']
                self.shapeByName[name] = shape
            except:
                logger.debug('Failed to parse shape %s' % item)
                logger.debug(logger.prepareJythonStackTrace(''))

    def discover_accounts(self):
        self.parse_accounts(self.client.list_accounts())

    def parse_accounts(self, accounts):
        for item in accounts:
            try:
                name = item['name']
                account = oracle_cloud.OracleCloudAccount(name)
                account.account_type = item['accounttype']
                self.accountByName[name] = account
            except:
                logger.debug('Failed to parse account %s' % item)
                logger.debug(logger.prepareJythonStackTrace(''))

    def discover_storage_volumes(self):
        self.parse_storage_volumes(self.client.list_storage_volumes())

    def parse_storage_volumes(self, volumes):
        for item in volumes:
            try:
                name = item['name']
                volume = oracle_cloud.StorageVolume(name)
                volume.description = item['description']
                imagelist = item['imagelist']
                if imagelist:
                    if self.imageByName.get(imagelist, None):
                        volume.imagelist = self.imageByName.get(imagelist)
                    else:
                        volume.imagelist = oracle_cloud.Image(imagelist)
                        self.imageByName[volume.imagelist] = volume.imagelist
                volume.platform = item['platform']
                volume.storage_pool = item['storage_pool']
                availability_domain = item['availability_domain'][1:]
                if self.availability_domainByName.get(availability_domain, None):
                    volume.availability_domain = self.availability_domainByName.get(availability_domain)
                else:
                    volume.availability_domain = oracle_cloud.AvailabilityDomain(availability_domain)
                    self.availability_domainByName[availability_domain] = volume.availability_domain

                if volume.availability_domain.name.startswith(self.region.name):
                    volume.availability_domain.region = self.region
                volume.properties = item['properties']
                volume.account = self.accountByName.get(item['account'], None)
                volume.status = item['status']
                volume.size = item['size']
                self.volumeByName[name] = volume
            except:
                logger.debug('Failed to parse storage volume %s' % item)
                logger.debug(logger.prepareJythonStackTrace(''))

    def discover_storage_attachments(self):
        self.parse_storage_attachments(self.client.list_storage_attachments())

    def parse_storage_attachments(self, attachments):
        for item in attachments:
            try:
                name = item['name']
                instance = self.instanceByName.get(item['instance_name'], None)
                storage_volume = self.volumeByName.get(item['storage_volume_name'], None)
                attachment = oracle_cloud.StorageAttachment(name, instance, storage_volume)

                self.storageAttachmentByVolumeName[name] = attachment
            except:
                logger.debug('Failed to parse storage attachment %s' % item)
                logger.debug(logger.prepareJythonStackTrace(''))


class OracleCloudReporter:
    def __init__(self, discoverer):
        self.discoverer = discoverer

    def report(self):
        vector = ObjectStateHolderVector()
        reporter = oracle_cloud.Reporter(oracle_cloud.Builder())
        vector.addAll(reporter.reportRegion(self.discoverer.region))

        for name, item in self.discoverer.ipNetworkByName.items():
            try:
                vector.addAll(reporter.reportIpNetwork(item))
            except:
                logger.debugException('Failed to report %s.' % name)
                logger.debug(logger.prepareJythonStackTrace(''))

        for name, item in self.discoverer.imageByName.items():
            try:
                vector.addAll(reporter.reportImage(item))
            except:
                logger.debugException('Failed to report %s.' % name)
                logger.debug(logger.prepareJythonStackTrace(''))

        for name, item in self.discoverer.vnicByName.items():
            try:
                reporter.reportInterface(item)
            except:
                logger.debugException('Failed to report %s.' % name)
                logger.debug(logger.prepareJythonStackTrace(''))

        for name, item in self.discoverer.ipReservationByName.items():
            try:
                vector.addAll(reporter.reportIpAddress(item))
            except:
                logger.debugException('Failed to report %s.' % name)
                logger.debug(logger.prepareJythonStackTrace(''))

        for name, item in self.discoverer.availability_domainByName.items():
            try:
                vector.addAll(reporter.reportAvailabilityDomain(item))
            except:
                logger.debugException('Failed to report %s.' % name)
                logger.debug(logger.prepareJythonStackTrace(''))

        for name, item in self.discoverer.volumeByName.items():
            try:
                vector.addAll(reporter.reportAccount(item.account, self.discoverer.region))
                vector.addAll(reporter.reportStorageVolume(item))
            except:
                logger.debugException('Failed to report %s.' % name)
                logger.debug(logger.prepareJythonStackTrace(''))

        for name, item in self.discoverer.instanceByName.items():
            try:
                vector.addAll(reporter.reportShape(item.shape))
                vector.addAll(reporter.reportAccount(item.account))
                vector.addAll(reporter.reportInstance(item))
            except:
                logger.debugException('Failed to report %s.' % name)
                logger.debug(logger.prepareJythonStackTrace(''))

        for name, item in self.discoverer.storageAttachmentByVolumeName.items():
            try:
                vector.addAll(reporter.reportStorageAttachment(item))
            except:
                logger.debugException('Failed to report %s.' % name)
                logger.debug(logger.prepareJythonStackTrace(''))

        return vector


def init_regions():
    region_us2 = oracle_cloud.Region('us2', 'Chicago, Il. US', 'US Commercial 2')
    region_us6 = oracle_cloud.Region('us6', 'Ashburn, Va. US', 'US Commercial 6')
    region_uscom = oracle_cloud.Region('uscom-central-1', 'Illinois, US', 'US Commercial Central')
    region_em2 = oracle_cloud.Region('em2', ' 	Amsterdam, NL. EMEA', 'EMEA Commercial 2')
    region_em3 = oracle_cloud.Region('em3', 'Slough, UK. EMEA', 'EMEA Commercial 3')
    region_aucom = oracle_cloud.Region('aucom-east-1', 'Sydney, Australia', 'Sydney')

    REGIONS[region_us2.name] = region_us2
    REGIONS[region_us6.name] = region_us6
    REGIONS[region_uscom.name] = region_uscom
    REGIONS[region_em2.name] = region_em2
    REGIONS[region_em3.name] = region_em3
    REGIONS[region_aucom.name] = region_aucom
