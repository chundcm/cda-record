# coding=utf-8
__author__ = 'Kane'
from types import FunctionType

import oci_common
import logger


class OCIType(object):
    oci_types = None
    LIST_METHOD = 'GET'
    osh_type = None
    mappings = {}
    children_mappings = {}
    reference_mapping = {}
    all_kinds = {}

    def __init__(self, data):
        super(OCIType, self).__init__()
        self.data = data
        self.oci_id = data['id']
        self.self_link = data['self']
        self.simple_name = data['simpleName']
        self.name = data['name']
        self.osh = None
        self.children_group = {}

    def buildCI(self):
        """
        @rtype: tuple of (ObjectStateHolderVector)
        """
        if not self.osh_type:
            raise Exception('osh_type should be defined in subclass')
        vector = oci_common.new_vector()
        osh = oci_common.new_osh(self.osh_type)
        self.__building_ci(osh)
        self.osh = osh
        vector.add(osh)
        return vector

    def __building_ci(self, osh):
        for target_key, (src_key, valueType) in self.mappings.iteritems():
            if isinstance(src_key, str):
                keys = src_key.split('.')
                tmpValue = self.data
                for k in keys:
                    tmpValue = tmpValue[k]
                value = tmpValue
            elif isinstance(src_key, FunctionType):
                try:
                    value = src_key(self.data)
                except:
                    ## this can happen if attributes are not passed from OCI; since the functions defined here require their presence
                    ## mising attributes may or may not be a problem; its use case dependent
                    logger.warn(
                        "Failed to calculate attribute value [" + target_key + "] for CI type [" + self.osh_type + "]")
                    value = None

            else:
                raise Exception('Unknown type of src key:%s' % src_key)
            if value is not None:
                osh.setAttribute(oci_common.new_osh_attribute(target_key, value, valueType))

    def isValid(self):
        return True

    def __repr__(self):
        return unicode(self.data)

    def __hash__(self):
        return hash(self.self_link)

    def __eq__(self, other):
        return self.self_link == other.self_link


class Volume(OCIType):
    oci_types = 'volumes'
    osh_type = 'logical_volume'
    mappings = {
        'name': ('name', 'String'),
        'logicalvolume_size': ('capacity.total.value', 'Double'),
        'logicalvolume_free': (
            lambda data: float(data['capacity']['total']['value']) - float(data['capacity']['used']['value']),
            'Double'),
    }


class InternalVolume(Volume):
    children_mappings = {
        'volumes': {
            'type': Volume,
            'relationship': 'dependency',
        },
    }


class Disk(OCIType):
    oci_types = 'disks'
    osh_type = 'physicalvolume'
    mappings = {
        'name': ('name', 'String'),
        'serial_number': ('serialNumber', 'String'),
        'volume_size': (lambda data: float(data['diskSize']['total']['value']), 'Double'),
    }


class StoragePool(OCIType):
    oci_types = 'storagePools'
    osh_type = 'storagepool'
    mappings = {
        'name': ('name', 'String'),
        'storagepool_pooltype': ('type', 'String'),
        'storagepool_poolid': ('id', 'Integer'),
        'storagepool_mbtotal': ('capacity.total.value', 'Double'),
    }

    children_mappings = {
        'volumes': {
            'type': Volume,
            'relationship': 'membership',
        },
        'internalVolumes': {
            'type': InternalVolume,
            'relationship': 'membership',
        },
        'disks': {
            'type': Disk,
            'relationship': 'usage',
        }
    }


class BasePort(OCIType):
    oci_types = 'ports'
    osh_type = 'fcport'
    mappings = {
        'name': ('name', 'String'),
        'fcport_wwn': ('wwn', 'String'),
        'port_index': ('portIndex', 'Integer'),
        'fcport_speed': ('speed', 'Double'),
        'fcport_status': ('portStatus', 'String'),
        'fcport_state': ('portState', 'String'),
    }


class StoragePort(BasePort):
    pass


class FCPort(BasePort):
    children_mappings = {
        'connectedPorts': {
            'type': StoragePort,
            'relationship': 'fcconnect',
        }
    }

    def isValid(self):
        return self.data['portStatus'] == 3


class StorageNode(OCIType):
    oci_types = 'storageNodes'
    osh_type = 'storagearray'
    mappings = {
        'name': ('name', 'String'),
        'serial_number': ('serialNumber', 'String'),
        'discovered_model': ('model', 'String'),
        'hardware_version': ('nodeVersion', 'String'),
        'storagearray_status': ('state', 'String'),
    }

    children_mappings = {
        'storagePools': {
            'type': StoragePool,
        },
        'ports': {
            'type': StoragePort,
        }
    }


class Storage(OCIType):
    oci_types = 'storages'

    children_mappings = {
        'storageNodes': {
            'type': StorageNode,
        }
    }


class Host(OCIType):
    oci_types = 'hosts'
    osh_type = 'host_node'
    mappings = {
        'name': ('name', 'String'),
        'discovered_model': ('model', 'String'),
        'discovered_vendor': ('manufacturer', 'String'),
    }

    children_mappings = {
        'ports': {
            'type': StoragePort,
        },
    }


class Switch(OCIType):
    oci_types = 'switches'
    osh_type = 'fcswitch'
    mappings = {
        'name': ('name', 'String'),
        'serial_number': ('serialNumber', 'String'),
        'fcswitch_wwn': ('wwn', 'String'),
        'discovered_model': ('model', 'String'),
        'discovered_vendor': ('vendor', 'String'),
        'fcswitch_version': ('firmware', 'String'),
        'fcswitch_domainid': ('domainId', 'String'),
        'fcswitch_role': ('switchRole', 'String'),
        'fcswitch_status': ('switchStatus', 'String'),
    }
    children_mappings = {
        'ports': {
            'type': FCPort,
        }
    }


class Fabric(OCIType):
    oci_types = 'fabrics'
    osh_type = 'storagefabric'
    mappings = {
        'name': ('name', 'String'),
        'storagefabric_wwn': ('wwn', 'String'),
    }
    children_mappings = {
        'switches': {
            'type': Switch,
            'relationship': 'membership',
        },
    }


class DataStore(Volume):
    oci_types = 'dataStores'
    osh_type = 'vmware_datastore'

    children_mappings = {
        'hosts': {
            'type': Host,
            'relationship': 'dependency',
        },
        'storageResources': {
            'type': Volume,
            'relationship': 'dependency',
        },
    }
