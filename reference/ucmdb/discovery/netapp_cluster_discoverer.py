#coding=utf-8
import logger
import netapp_webservice_utils

from netapp_topology import VServerBuilder
from netapp_topology import NetAppNodeBuilder
from netapp_topology import InterfaceBuilder
from netapp_topology import VolumeBuilder
from netapp_topology import DiskBuilder
from netapp_topology import ISCSIAdapterBuilder
from netapp_topology import RemoteISCSIAdapterBuilder
from netapp_topology import FCPortBuilder
from netapp_topology import SnapshotBuilder
from netapp_topology import NetworkShareBuilder

from netapp.manage import NaElement


SCRIPT_NAME='netapp_cluster_discoverer.py'
CHUNK_SIZE = 1000


class NetAppDiscoverer:
    def __init__(self, framework, ws_connection):
        self._framework = framework
        self._ws_connection = ws_connection
        self.chunk_size = eval(self._framework.getParameter('chunkSize')) or 1000
        self._tag = None

    def discover(self):
        raise NotImplementedError()

    def discover_api(self, api_key, entries_key="attributes-list", query_model=None, conditions=None, persistent=None):
        try:
            result = []
            requestElement = NaElement(api_key)
            if self.chunk_size:
                requestElement.addNewChild('max-records', str(self.chunk_size))
            if persistent:
                requestElement.addNewChild('persistent', str(persistent))
            if query_model and conditions:
                requestElement.addChildElem(self._build_queries(query_model, conditions))
            if self._tag:
                requestElement.addNewChild('tag', self._tag)

            responseElement = netapp_webservice_utils.wsInvoke(self._ws_connection, requestElement)
            if not responseElement:
                self._framework.reportWarning('%s information request failed' % api_key)
                return result
            list = responseElement.getChildByName(entries_key) and responseElement.getChildByName(entries_key).getChildren()
            if not list:
                logger.warn('No results discovered: api: %s  %s  %s ' % (api_key, query_model, conditions))
                return result
            for record in list:
                logger.debug(api_key, record)
                result.append(self.build_pdo(record))

            self._tag = responseElement.getChildByName("next-tag") and responseElement.getChildContent("next-tag")
            if self._tag:
                next_list = self.discover_api(api_key, query_model=query_model, conditions=conditions)
                if next_list:
                    result.extend(next_list)
            return result
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':NetAppDiscoverer.discover(%s)] Exception: <%s>' % (api_key, excInfo))
            pass

    def build_pdo(self, record):
         raise NotImplementedError()

    def _build_queries(self, query_model, conditions):
        if query_model and conditions:
            queryElement = NaElement('query')
            for key in conditions.keys():
                requestElement = NaElement(query_model)
                requestElement.addNewChild(key, conditions.get(key))
                queryElement.addChildElem(requestElement)
                return queryElement



class NetAppClusterDiscoverer(NetAppDiscoverer):
    def __init__(self, framework, ws_connection):
        NetAppDiscoverer.__init__(self, framework, ws_connection)

    def discover(self):
        clusters = self.discover_api('cluster-identity-get', entries_key='attributes')
        if clusters:
            return clusters[0]


    def build_pdo(self, record):
        return record.getChildContent("cluster-name")




class NetAppNodeDiscoverer(NetAppDiscoverer):
    def __init__(self, framework, ws_connection):
        NetAppDiscoverer.__init__(self, framework, ws_connection)

    def discover(self):
        return self.discover_api('system-get-node-info-iter')

    def build_pdo(self, node_record):
        try:
            name = node_record.getChildContent('system-name') or ''
            model = node_record.getChildContent('system-model') or ''
            serial_number = node_record.getChildContent('system-serial-number') or ''
            id = node_record.getChildContent('system-id')
            memory_size = node_record.getChildContent('memory-size')
            num_cpus = eval(node_record.getChildContent('number-of-processors'))
            cpu_id = node_record.getChildContent('cpu-processor-id')

            if netapp_webservice_utils.isValidString(name):
                host_pdo = NetAppNodeBuilder.create_pdo(id, name, serial_number, model, memory_size)

                if netapp_webservice_utils.isValidString(cpu_id):
                    for cpu_num in range(num_cpus):
                        cpu_cid = cpu_id + '-' + str(cpu_num)
                        netapp_webservice_utils.debugPrint(2, '[' + SCRIPT_NAME + ':NetAppNodeDiscoverer.build_pdo] Creating cpu CI for Node <%s> with CPUID <%s>' % (name, cpu_cid))
                        host_pdo.addCPU(cpu_cid)
                return host_pdo
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':NetAppNodeDiscoverer.build_pdo] Exception: <%s>' % excInfo)


class VServerDiscoverer(NetAppDiscoverer):
    def __init__(self, framework, ws_connection):
        NetAppDiscoverer.__init__(self, framework, ws_connection)

    def discover(self):
        return self.discover_api('vserver-get-iter')

    def build_pdo(self, vserver_record):
        try:
            name = vserver_record.getChildContent("vserver-name")
            type = vserver_record.getChildContent("vserver-type")
            uuid = vserver_record.getChildContent("uuid")
            return VServerBuilder.create_pdo(uuid, name, type)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':VServerDiscoverer.build_pdo] Exception: <%s>' % excInfo)


class InterfaceDiscoverer(NetAppDiscoverer):
    def __init__(self, framework, ws_connection):
        NetAppDiscoverer.__init__(self, framework, ws_connection)

    def discover(self):
        return self.discover_api('net-interface-get-iter')

    def build_pdo(self, interface_record):
        try:
            interface_name = interface_record.getChildContent('interface-name')
            ip_address = interface_record.getChildContent('address')
            netmask =  interface_record.getChildContent('netmask')
            host = interface_record.getChildContent('vserver')
            role = interface_record.getChildContent('role')
            if role == 'node_mgmt':
                host = interface_record.getChildContent('home-node')
            return InterfaceBuilder.create_pdo(interface_name, ip_address, netmask, host, role)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':InterfaceDiscoverer.build_pdo] Exception: <%s>' % excInfo)
            pass


class AggrDiscoverer(NetAppDiscoverer):
    def __init__(self, framework, ws_connection):
        NetAppDiscoverer.__init__(self, framework, ws_connection)

    def discover(self):
        return self.discover_api('aggr-get-iter')

    def build_pdo(self, aggr_record):
        try:
            name = aggr_record.getChildContent('aggregate-name')
            size = aggr_record.getChildByName('aggr-space-attributes').getChildContent('size-total')
            free = aggr_record.getChildByName('aggr-space-attributes').getChildContent('size-available')
            used = aggr_record.getChildByName('aggr-space-attributes').getChildContent('size-used')
            type = aggr_record.getChildByName('aggr-fs-attributes').getChildContent('type')
            state = aggr_record.getChildByName('aggr-raid-attributes').getChildContent('state')
            id = aggr_record.getChildContent('aggregate-uuid')
            node_name = aggr_record.getChildByName("nodes").getChildContent('node-name')
            return VolumeBuilder.create_pdo(id, name, size, free, used, type, state, node_name)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':AggrDiscoverer.build_pdo] Exception: <%s>' % excInfo)


class VolumeDiscoverer(NetAppDiscoverer):
    def __init__(self, framework, ws_connection):
        NetAppDiscoverer.__init__(self, framework, ws_connection)

    def discover(self):
        return self.discover_api('volume-get-iter')

    def build_pdo(self, volume_record):
        try:
            name = volume_record.getChildByName('volume-id-attributes').getChildContent('name')
            size = volume_record.getChildByName('volume-space-attributes').getChildContent('size-total')
            free = volume_record.getChildByName('volume-space-attributes').getChildContent('size-available')
            used = volume_record.getChildByName('volume-space-attributes').getChildContent('size-used')
            type = volume_record.getChildByName('volume-id-attributes').getChildContent('type')
            state = volume_record.getChildByName('volume-state-attributes').getChildContent('state')
            id = volume_record.getChildByName('volume-id-attributes').getChildContent('instance-uuid')
            node_name = volume_record.getChildByName('volume-id-attributes').getChildContent('node')
            vserver_name = volume_record.getChildByName('volume-id-attributes').getChildContent('owning-vserver-name')
            aggr_name = volume_record.getChildByName('volume-id-attributes').getChildContent('containing-aggregate-name')

            return VolumeBuilder.create_pdo(id, name, size, free, used, type, state, vserver_name, node_name, aggr_name=aggr_name)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':VolumeDiscoverer.build_pdo] Exception: <%s>' % excInfo)


class DiskDiscoverer(NetAppDiscoverer):
    def __init__(self, framework, ws_connection):
        NetAppDiscoverer.__init__(self, framework, ws_connection)

    def discover(self):
        return self.discover_api('storage-disk-get-iter')

    def build_pdo(self, disk_record):
        try:
            name = disk_record.getChildContent("disk-name")
            id = disk_record.getChildByName('disk-ownership-info').getChildContent("disk-uid")
            vendor = disk_record.getChildByName('disk-inventory-info').getChildContent("vendor")
            serial_number = disk_record.getChildByName('disk-inventory-info').getChildContent("serial-number")
            model = disk_record.getChildByName('disk-inventory-info').getChildContent("model")
            container_type = disk_record.getChildByName('disk-raid-info').getChildContent("container-type")
            aggregate_name = None
            if container_type == 'aggregate':
                aggregate_name = disk_record.getChildByName('disk-raid-info').getChildByName("disk-aggregate-info").getChildContent('aggregate-name')
            node_name = disk_record.getChildByName('disk-ownership-info').getChildContent('owner-node-name')

            return DiskBuilder.create_pdo(id, name, serial_number, model, vendor, node_name, aggregate_name)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':DiskDiscoverer.build_pdo] Exception: <%s>' % excInfo)


class LUNDiscoverer(NetAppDiscoverer):
    def __init__(self, framework, ws_connection):
        NetAppDiscoverer.__init__(self, framework, ws_connection)

    def discover(self):
        return self.discover_api('lun-get-iter')


    def build_pdo(self, lun_record):
        try:
            logger.debug('--try to build lun pdo---:', lun_record)
            path = lun_record.getChildContent("path")
            size = lun_record.getChildContent("size")
            id = lun_record.getChildContent("uuid")
            used = lun_record.getChildContent("size-used")
            serial_number = lun_record.getChildContent("serial-number")
            node_name = lun_record.getChildContent("node")
            vserver_name = lun_record.getChildContent("vserver")
            volume_name = lun_record.getChildContent("volume")
            type = lun_record.getChildContent("multiprotocol-type")
            state = lun_record.getChildContent("state")
            if path:
                splits = path.split('/')
                name = splits[len(splits) - 1]
                return VolumeBuilder.create_pdo(id, name, size, None, used, type, state, vserver_name, node_name, path, serial_number, volume_name)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':LUNDiscoverer.build_pdo] Exception: <%s>' % excInfo)


class ISCSIAdapterDiscoverer(NetAppDiscoverer):
    def __init__(self, framework, ws_connection):
        NetAppDiscoverer.__init__(self, framework, ws_connection)

    def discover(self):
        return self.discover_api('iscsi-service-get-iter')


    def build_pdo(self, iscsi_record):
        try:
            iqn = iscsi_record.getChildContent("node-name")
            name = iscsi_record.getChildContent("alias-name")
            vserver_name = iscsi_record.getChildContent("vserver")
            return ISCSIAdapterBuilder.create_pdo(iqn, name, vserver_name)

        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':ISCSIAdapterDiscoverer.build_pdo] Exception: <%s>' % excInfo)


class ISCSIPortGroupDiscoverer(NetAppDiscoverer):
    def __init__(self, framework, ws_connection):
        NetAppDiscoverer.__init__(self, framework, ws_connection)

    def discover(self):
        port_group_map  = {}
        results = self.discover_api('iscsi-tpgroup-get-iter')
        if results:
            for group_name, interface_list in results:
                port_group_map[group_name] = interface_list
        return port_group_map


    def build_pdo(self, group_record):
        try:
            group_name = group_record.getChildContent('tpgroup-name')
            interface_list = []
            interfaces = group_record.getChildByName('interface-list-entries').getChildren()
            for interface in interfaces:
                name = interface.getChildContent('interface-name')
                interface_list.append(name)
            return (group_name, interface_list)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':ISCSIPortGroupDiscoverer.build_pdo] Exception: <%s>' % excInfo)


class ISCSIConnectionDiscoverer(NetAppDiscoverer):
    def __init__(self, framework, ws_connection):
        NetAppDiscoverer.__init__(self, framework, ws_connection)

    def discover(self):
        return self.discover_api('iscsi-connection-get-iter')

    def build_pdo(self, connection_record):
        try:
            interface = connection_record.getChildContent('interface-name')
            remote_ip = connection_record.getChildContent('remote-ip-address')
            return interface, remote_ip
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':ISCSIConnectionDiscoverer.build_pdo] Exception: <%s>' % excInfo)

    def get_interface_remote_ip_map(self):
        interface_ip_map = {}
        results = self.discover()
        if results:
            for interface, remote_ip in results:
                interface_ip_map[interface] = remote_ip
        return interface_ip_map


class ISCSIInitiatorDiscoverer(NetAppDiscoverer):
    def __init__(self, framework, ws_connection):
        NetAppDiscoverer.__init__(self, framework, ws_connection)

    def discover(self):
        results = []
        pg_interface_discoverer = ISCSIPortGroupDiscoverer(self._framework, self._ws_connection)
        pg_interface_map = pg_interface_discoverer.discover()

        interface_ip_discoverer = ISCSIConnectionDiscoverer(self._framework, self._ws_connection)
        interface_ip_map = interface_ip_discoverer.get_interface_remote_ip_map()

        if pg_interface_map and interface_ip_map:
            initiators = self.discover_api('iscsi-initiator-get-iter')
            if initiators:
                for iqn, tpgroup_name, vserver_name in initiators:
                    interface_name_list = pg_interface_map.get(tpgroup_name, None)
                    if interface_name_list:
                        for interface_name in interface_name_list:
                            remote_ip = interface_ip_map.get(interface_name, None)
                            if remote_ip:
                                results.append(RemoteISCSIAdapterBuilder.create_remote_pdo(iqn, None, remote_ip, vserver_name))

        return results


    def build_pdo(self, iscsi_record):
        try:
            iqn = iscsi_record.getChildContent("initiator-nodename")
            tpgroup_name = iscsi_record.getChildContent("tpgroup-name")
            vserver_name = iscsi_record.getChildContent("vserver")
            return iqn, tpgroup_name, vserver_name

        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':buildISCSIInitiatorPdo] Exception: <%s>' % excInfo)


class FCAdapterDiscoverer(NetAppDiscoverer):
    def __init__(self, framework, ws_connection):
        NetAppDiscoverer.__init__(self, framework, ws_connection)

    def discover(self):
        return self.discover_api('fcp-service-get-iter')

    def build_pdo(self, fcp_record):
        try:
            list = fcp_record.getChildByName('fcp-connected-initiators').getChildren()
            vserver_name = fcp_record.getChildContent("vserver")
            logger.debug('there are ', str(list.size()), ' fcp-connected-initiators in list')
            for obj in list:
                fc_port_wwn = obj.getChildContent("port-name")
                fc_port_name = obj.getChildContent("port-address")
                fc_adapter_wwn = obj.getChildContent("node-name")
                logger.debug('fc_port_name: ', fc_port_name, ',fc_port_wwn:', fc_port_wwn, ',fc_adapter_wwn:',
                             fc_adapter_wwn)
                return FCPortBuilder.create_pdo(fc_port_wwn, fc_port_name, fc_adapter_wwn, vserver_name)
        except:
            logger.debug('parse fc failed.')
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':FCPortDiscoverer.build_pdo] Exception: <%s>' % excInfo)
        try:
            fc_adapter_wwn = fcp_record.getChildContent("node-name")
            vserver_name = fcp_record.getChildContent("vserver")
            return fc_adapter_wwn, vserver_name
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':FCAdapterDiscoverer.build_pdo] Exception: <%s>' % excInfo)


class FCPortDiscoverer(NetAppDiscoverer):
    def __init__(self, framework, ws_connection):
        NetAppDiscoverer.__init__(self, framework, ws_connection)

    def discover(self):
        return self.discover_api('fcp-interface-get-iter')

    def build_pdo(self, fcp_record):
        try:
            interface_name = fcp_record.getChildContent("interface-name")
            fc_adapter_wwn = fcp_record.getChildContent("node-name")
            vserver_name = fcp_record.getChildContent("vserver")
            fc_port_wwn = fcp_record.getChildContent("port-name")
            fc_port_name = fcp_record.getChildContent("port-address")
            return FCPortBuilder.create_pdo(fc_port_wwn, fc_port_name, fc_adapter_wwn, vserver_name)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':FCPortDiscoverer.build_pdo] Exception: <%s>' % excInfo)


class FCPInitiatorDiscoverer(NetAppDiscoverer):
    def __init__(self, framework, ws_connection):
        NetAppDiscoverer.__init__(self, framework, ws_connection)

    def discover(self):
        return self.discover_api('fcp-initiator-get-iter')

    def build_pdo(self, fcp_record):
        try:
            fc_adapter_wwn = fcp_record.getChildContent("node-name")
            vserver_name = fcp_record.getChildContent("vserver")
            fc_port_wwn = fcp_record.getChildContent("port-name")
            fc_port_name = fcp_record.getChildContent("port-address")
            return FCPortBuilder.create_pdo(fc_port_wwn, fc_port_name, fc_adapter_wwn, vserver_name)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':FCPortDiscoverer.build_pdo] Exception: <%s>' % excInfo)

class SnapshotDiscoverer(NetAppDiscoverer):
    def __init__(self, framework, ws_connection):
        NetAppDiscoverer.__init__(self, framework, ws_connection)

    def discover(self, conditions=None):
        return self.discover_api('snapshot-get-iter', query_model="snapshot-info", conditions=conditions)

    def build_pdo(self, sp_record):
        try:
            name = sp_record.getChildContent('name')
            busy = sp_record.getChildContent('busy')
            dependency = sp_record.getChildContent('dependency')
            total_blocks = sp_record.getChildContent('percentage-of-total-blocks')
            used_blocks = sp_record.getChildContent('percentage-of-used-blocks')
            volume = sp_record.getChildContent('volume')
            return SnapshotBuilder.create_pdo(name, busy, dependency, total_blocks, used_blocks, volume)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':SnapshotDiscoverer.build_pdo] Exception: <%s>' % excInfo)


class CifsServerDiscoverer(NetAppDiscoverer):
    def __init__(self, framework, ws_connection):
        NetAppDiscoverer.__init__(self, framework, ws_connection)

    def discover(self, conditions=None):
        return self.discover_api('cifs-server-get-iter')

    def build_pdo(self, cifs_record):
        try:
            name =  cifs_record.getChildContent('cifs-server')
            vserver_name = cifs_record.getChildContent('vserver')
            return (name, vserver_name)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':CifsServerDiscoverer.build_pdo] Exception: <%s>' % excInfo)


class CifsShareDiscoverer(NetAppDiscoverer):
    def __init__(self, framework, ws_connection):
        NetAppDiscoverer.__init__(self, framework, ws_connection)

    def discover(self, conditions=None):
        return self.discover_api('cifs-share-get-iter')

    def build_pdo(self, cifs_record):
        try:
            share_name = cifs_record.getChildContent('share-name')
            share_path = cifs_record.getChildContent('path')
            cifs_server = cifs_record.getChildContent('cifs-server')
            vserver_name = cifs_record.getChildContent('vserver')
            type = "CIFS"
            return NetworkShareBuilder.create_pdo(share_name, share_path, type, vserver_name,  cifs_server)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':CifsShareDiscoverer.build_pdo] Exception: <%s>' % excInfo)

class NfsShareDiscoverer(NetAppDiscoverer):
    def __init__(self, framework, ws_connection, vserver):
        NetAppDiscoverer.__init__(self, framework, ws_connection)
        self._vserver = vserver

    def discover(self, conditions=None):
            result = []
            self.chunk_size = None
            result = self.discover_api('nfs-exportfs-list-rules-2', persistent='true', entries_key= 'rules')
            if not result:
                logger.debug('Cannot find NFS info from nfs-exportfs-list-rules-2, try with nfs-exportfs-list-rules')
                result = self.discover_api('nfs-exportfs-list-rules', persistent='true')
            return result

    def build_pdo(self, nfs_record):
        try:
            share_name = nfs_record.getChildContent('pathname')
            # key attribute path
            share_path = nfs_record.getChildContent('pathname')
            # key attribute vserver
            vserver_name = self._vserver
            type = "NFS"
            return NetworkShareBuilder.create_pdo(share_name, share_path, type, vserver_name)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':NfsShareDiscoverer.build_pdo] Exception: <%s>' % excInfo)


class CifsSessionDiscoverer(NetAppDiscoverer):
    def __init__(self, framework, ws_connection):
        NetAppDiscoverer.__init__(self, framework, ws_connection)

    def discover(self, conditions=None):
        return self.discover_api('cifs-session-get-iter')

    def build_pdo(self, cifs_record):
        try:
            remote_ip =  cifs_record.getChildContent('address')
            vserver_name = cifs_record.getChildContent('vserver')
            return (remote_ip, vserver_name)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':CifsSessionDiscoverer.build_pdo] Exception: <%s>' % excInfo)


class OptionDiscoverer(NetAppDiscoverer):
    def __init__(self, framework, ws_connection):
        NetAppDiscoverer.__init__(self, framework, ws_connection)

    def discover(self, conditions=None):
        return self.discover_api('options-get-iter')

    def build_pdo(self, option_record):
        try:
            name =  option_record.getChildContent('name')
            value =  option_record.getChildContent('value')
            vserver_name = option_record.getChildContent('vserver')
            return (name, value, vserver_name)
        except:
            excInfo = logger.prepareJythonStackTrace('')
            logger.warn('[' + SCRIPT_NAME + ':OptionDiscoverer.build_pdo] Exception: <%s>' % excInfo)


