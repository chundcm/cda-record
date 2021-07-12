# coding:utf-8

import entity
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector

import re, json
from google_cloud_compute import convertToDate
import logger, modeling
from vendors import PlatformVendors


# 'google.compute.Image': 'Image' Currently, the object do not report
google_cloud_type = {'google.cloud.resourcemanager.Project': 'Project', 'google.compute.Disk': 'Disk',
                     'google.compute.Instance': 'Instance',
                     'google.compute.Snapshot': 'Snapshot', 'google.iam.ServiceAccount': 'Account'}


project_osh_dict = {}
account_osh_dict = {}
snapshot_osh_dict = {}
disk_osh_dict = {}

class Project(entity.HasOsh):
    def __init__(self, resource):
        entity.HasOsh.__init__(self)
        self.name = resource['data']['name']
        self.project_id = resource['data']['projectId']
        self.parent = resource['data']['parent']
        self.project_num = resource['data']['projectNumber']

    def acceptVisitor(self, visitor):
        return visitor.build_project(self)


class Account(entity.HasOsh):
    def __init__(self, resource):
        entity.HasOsh.__init__(self)
        self.name = resource['data']['email']
        self.parent = convert_parent(resource['parent'])

    def acceptVisitor(self, visitor):
        return visitor.build_account(self)


class Disk(entity.HasOsh):
    def __init__(self, resource):
        entity.HasOsh.__init__(self)
        self.parent = convert_parent(resource['parent'])
        self.id = resource['data']['id']
        self.name = resource['data']['name']
        self.size = self.get_size(resource['data'])
        # can link to image,but some disk do not have key (sourceImage,sourceImageId)
        self.source_image = self.get_image(resource['data'])
        self.source_image_id = self.get_image_id(resource['data'])
        self.self_link = self.get_link(resource['data'])
        self.type = self.get_type(resource['data'])
        # resource['data']['zone']
        self.creationTimestamp = self.get_create_time(resource['data'])
        self.status = self.get_status(resource['data'])

    def get_size(self, data):
        size = data.get('sizeGb')
        return size

    def get_image(self, data):
        image = data.get('sourceImage')
        return image

    def get_image_id(self, data):
        image_id = data.get('sourceImageId')
        return image_id

    def get_link(self, data):
        link = data.get('selfLink')
        return link

    def get_type(self, data):
        type = data.get('type')
        if type:
            type_list = type.split('/')
            return type_list[-1]

    def get_create_time(self, data):
        create_time = data.get('creationTimestamp')
        return create_time

    def get_status(self, data):
        status = data.get('status')
        return status

    def acceptVisitor(self, visitor):
        return visitor.build_disk(self)


class Instance(entity.HasOsh):
    def __init__(self, resource):
        entity.HasOsh.__init__(self)
        self.parent = convert_parent(resource['parent'])
        self.id = resource['data']['id']
        self.name = resource['data']['name']
        self.service_account = self.get_account(resource['data'])
        self.creationTimestamp = self.get_create_time(resource['data'])
        self.cpu_platform = self.get_cpu(resource['data'])
        self.status = self.get_status(resource['data'])
        self.os_type, self.volume_dict = self.get_volume(resource['data'])
        self.machine_type = self.get_machine_type(resource['data'])
        # resource['data']['zone']
        self.ip_address = self.get_ipaddress(resource['data'])
        self.__config = None

    def get_create_time(self, data):
        create_time = data.get('creationTimestamp')
        return create_time

    def get_cpu(self, data):
        cpu_platform = data.get('cpuPlatform')
        return cpu_platform

    def get_status(self, data):
        status = data.get('status')
        return status

    def get_volume(self, data):
        volume_list = data.get('disk')
        volume_dict = {}
        os_type = None
        if volume_list:
            for volume in volume_list:
                tmp_dict = {}
                tmp_dict['auto_delete'] = volume.get('autoDelete')
                tmp_dict['boot'] = volume.get('boot')
                tmp_dict['mode'] = volume.get('mode')
                tmp_dict['source'] = volume.get('source')
                tmp_dict['interface'] = volume.get('interface')
                os_type_string = volume.get('license')
                if os_type_string:
                    os_type_string = os_type_string[0]
                    os_type = os_type_string.split('/projects/')[1]
                volume_dict[volume['deviceName']] = tmp_dict
        return os_type, volume_dict

    def setVmConfigOsh(self, cfg):
        self.__config = cfg

    def getVmConfigOsh(self):
        return self.__config

    def get_ipaddress(self, data):
        # Just report external_ip, inner IP may be identical cause different nodes link to
        # the same inner IP under different projects. If no external_ip,do not report Ip Address
        networks = data.get('networkInterface')
        ip_address = []
        if networks:
            for network in networks:
                external_dicts = network.get('accessConfig')
                if external_dicts:
                    external_dict = external_dicts[0]
                    external_ip = external_dict.get('externalIp')
                    if external_ip:
                        ip_address.append(external_ip)
                    else:
                        logger.warn('Node cannot get IP Address')
        return ip_address

    def get_machine_type(self, data):
        machine_string = data.get('machineType')
        if machine_string:
            type_list = machine_string.split('/machineTypes/')
            return type_list[1]

    def get_account(self, data):
        self.service_account = []
        account_list = data.get('serviceAccount')
        if account_list:
            for account in account_list:
                self.service_account.append(account.get('email'))
        return self.service_account

    def acceptVisitor(self, visitor):
        return visitor.build_instance(self)


class Snapshot(entity.HasOsh):
    def __init__(self, resource):
        entity.HasOsh.__init__(self)
        self.id = resource['data']['id']
        self.name = resource['data']['name']
        self.parent = convert_parent(resource['parent'])
        self.creationTimestamp = self.get_create_time(resource['data'])
        self.disk_size = self.get_disk_size(resource['data'])
        self.storage_bytes = self.get_storage_bytes(resource['data'])
        self.status = self.get_status(resource['data'])
        self.disk_id = self.get_disk_id(resource['data'])

    def get_create_time(self, data):
        create_time = data.get('creationTimestamp')
        return create_time

    def get_disk_size(self, data):
        size = data.get('diskSizeGb')
        return size

    def get_storage_bytes(self, data):
        storage_bytes = data.get('storageBytes')
        return storage_bytes

    def get_status(self, data):
        status = data.get('status')
        return status

    def get_disk_id(self, data):
        disk_id = data.get('sourceDiskId')
        return disk_id

    def acceptVisitor(self, visitor):
        return visitor.build_snapshot(self)


class Build(object):

    def build_project(self, project):
        project_osh = ObjectStateHolder('googlecloudproject')
        project_osh.setAttribute('name', project.project_id)
        return project_osh

    def build_account(self, account):
        account_osh = ObjectStateHolder('googlecloudserviceaccount')
        account_osh.setAttribute('name', account.name)
        return account_osh

    def build_instance(self, instance):
        def build_vm_config(instance):
            osh = ObjectStateHolder('google_cloud_vm_config')
            osh.setStringAttribute('name', instance.name)
            date_time = convertToDate(instance)
            if date_time:
                osh.setDateAttribute('creation_time', date_time)
            osh.setStringAttribute('cpu_platform', instance.cpu_platform)
            osh.setStringAttribute('type', instance.machine_type)
            osh.setStringAttribute('status', instance.status)
            instance.setVmConfigOsh(osh)

        osh = build_node(instance.os_type, instance.ip_address)
        osh.setStringAttribute("name", instance.name)
        osh.setBoolAttribute('host_iscomplete', True)
        osh.setBoolAttribute('host_isvirtual', True)
        id = instance.id
        if id:
            osh.setStringAttribute("cloud_instance_id", id)
            osh.setStringAttribute("host_key", id)
        # Host Platform Vendor
        osh.setStringAttribute('platform_vendor', PlatformVendors.Google)
        build_vm_config(instance)
        return osh

    def build_disk(self, disk):
        disk_osh = ObjectStateHolder('google_cloud_disk')
        disk_osh.setAttribute('id', disk.id)
        disk_osh.setStringAttribute('name', disk.name)
        date_time = convertToDate(disk)
        if date_time:
            disk_osh.setDateAttribute('creation_time', date_time)
        if disk.size is not None:
            disk_osh.setIntegerAttribute('size', int(disk.size))
        if disk.status:
            disk_osh.setStringAttribute('status', disk.status)
        if disk.type:
            disk_osh.setStringAttribute('type', disk.type)
        return disk_osh

    def build_snapshot(self, snapshot):
        snapshot_osh = ObjectStateHolder('google_cloud_snapshot')
        snapshot_osh.setStringAttribute('name', snapshot.name)
        if snapshot.disk_size:
            snapshot_osh.setIntegerAttribute('disk_size', int(snapshot.disk_size))
        if snapshot.storage_bytes:
            snapshot_osh.setLongAttribute('snapshot_size', snapshot.storage_bytes)
        if snapshot.status:
            snapshot_osh.setStringAttribute('status', snapshot.status)
        date_time = convertToDate(snapshot)
        if date_time:
            snapshot_osh.setDateAttribute('snapshot_create_time', date_time)
        return snapshot_osh

def resolve_json(json_line):

    item = json.loads(json_line)
    item_type = item.get('asset_type')
    resource = item.get('resource')
    object_type = google_cloud_type.get(item_type)
    if object_type and resource:
        try:
            result_obj = eval(object_type)(resource)
            return object_type, result_obj
        except Exception, e:
            logger.warn('Failed to resolve %s to object: %s' %(str(item_type), str(e)))
            logger.warn('Error json:', str(resource))
            return None, None
    return None, None


class Report(object):
    def __init__(self):
        self.vector = ObjectStateHolderVector()

    def report_project(self, obj_dict):
        project_list = obj_dict.get('Project')
        for project_obj in project_list:
            project_osh = project_obj.build(Build())
            self.vector.add(project_osh)
            project_osh_dict[project_obj.project_num] = project_osh
        return self.vector

    def report_account(self, obj_dict):
        account_list = obj_dict.get('Account')
        for account in account_list:
            account_osh = account.build(Build())
            project_osh = project_osh_dict.get(account.parent)
            if not project_osh:
                logger.errorException('Error! Serice account %s cannot get its project.' % account.name)
                continue
            account_osh.setContainer(project_osh)
            self.vector.add(account_osh)
            account_osh_dict[account.name] = account_osh
        return self.vector

    def report_account_node(self, instance, node_osh):
        if instance.service_account:
            for account_name in instance.service_account:
                account_osh = account_osh_dict.get(account_name)
                if account_osh:
                    self.vector.add(modeling.createLinkOSH('usage', account_osh, node_osh))

    def report_node_ip(self, instance, node_osh):
        if instance.ip_address:
            for ip in instance.ip_address:
                ip_osh = modeling.createIpOSH(ip)
                self.vector.add(ip_osh)
                self.vector.add(modeling.createLinkOSH('containment', node_osh, ip_osh))

    def report_instance(self, obj_dict):
        instance_list = obj_dict.get('Instance')
        disk_list = obj_dict.get('Disk')
        for instance in instance_list:
            node_osh = instance.build(Build())
            project_osh = project_osh_dict.get(instance.parent)
            if not project_osh:
                logger.errorException('Error! Instance %s cannot get its project.' % instance.name)
                continue
            self.report_account_node(instance, node_osh)
            link = modeling.createLinkOSH('containment', project_osh, node_osh)
            self.report_node_ip(instance, node_osh)

            vm_config_osh = instance.getVmConfigOsh()
            vm_config_osh.setContainer(node_osh)
            self.vector.add(vm_config_osh)
            self.vector.add(node_osh)
            self.vector.add(link)

            for name, lv in instance.volume_dict.items():
                for disk in disk_list:
                    if disk.self_link == lv['source']:
                        volume_osh = ObjectStateHolder('logical_volume')
                        volume_osh.setStringAttribute('logical_volume_global_id', disk.id)
                        volume_osh.setStringAttribute('name', name)
                        volume_osh.setContainer(node_osh)
                        disk_osh = disk.build(Build())
                        disk_osh = set_attribute(disk_osh, lv)
                        disk_osh_dict[disk.id] = disk_osh
                        volume_link = modeling.createLinkOSH('usage', volume_osh, disk_osh)
                        self.vector.add(volume_osh)
                        self.vector.add(disk_osh)
                        self.vector.add(volume_link)
        return self.vector


    def report_snapshot(self, obj_dict):
        snapshot_list = obj_dict.get('Snapshot')
        for snapshot in snapshot_list:
            snapshot_osh = snapshot.build(Build())
            project_osh = project_osh_dict.get(snapshot.parent)
            if not project_osh:
                logger.errorException('Error! Snapshot %s cannot get its project.' % snapshot.name)
                continue
            snapshot_osh.setContainer(project_osh)
            self.vector.add(snapshot_osh)
            disk_osh = disk_osh_dict.get(snapshot.disk_id)
            if disk_osh:
                link = modeling.createLinkOSH('usage', disk_osh, snapshot_osh)
                self.vector.add(link)
        return self.vector


def set_attribute(disk_osh, lv_dict):
    if lv_dict.get('auto_delete'):
        disk_osh.setBoolAttribute('is_auto_delete', lv_dict['auto_delete'])
    if lv_dict.get('boot'):
        disk_osh.setBoolAttribute('is_boot_disk', lv_dict['boot'])
    if lv_dict.get('mode'):
        disk_osh.setStringAttribute('mode', lv_dict['mode'])
    if lv_dict.get('interface'):
        disk_osh.setStringAttribute('interface', lv_dict['interface'])
    return disk_osh


def convert_parent(parent):
    parent_id = re.search(r'.*/(\d+)', parent)
    return parent_id.group(1)


def build_node(os_type, ip_address):
    if ip_address and os_type:
        if re.search(r'windows', os_type.lower()):
            osh = modeling.createHostOSH(ip_address[0], hostClassName='nt')
        else:
            osh = modeling.createHostOSH(ip_address[0], hostClassName='unix')

    elif ip_address and not os_type:
        osh = modeling.createHostOSH(ip_address[0], hostClassName='host_node')

    elif not ip_address and os_type:
        if re.search(r'windows', os_type.lower()):
            osh = ObjectStateHolder('nt')
        else:
            osh = ObjectStateHolder('unix')
    else:
        osh = ObjectStateHolder('host_node')
    return osh


def report_client(obj_dict):
    logger.debug('Starting to report CIs.')
    osh_client = Report()
    vector = ObjectStateHolderVector()
    logger.debug('Staring to report project')
    vector.addAll(osh_client.report_project(obj_dict))
    logger.debug('Staring to report account')
    vector.addAll(osh_client.report_account(obj_dict))
    logger.debug('Staring to report instance')
    vector.addAll(osh_client.report_instance(obj_dict))
    logger.debug('Staring to report snapshot')
    vector.addAll(osh_client.report_snapshot(obj_dict))
    logger.debug('All of CIs have been reported.')
    return  vector