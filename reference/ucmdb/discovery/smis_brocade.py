#coding=utf-8
import re
import smis
import logger
import fptools
import cim
import cim_discover
import netutils
import smis_cimv2
from smis_cimv2 import BaseSmisDiscoverer
from smis_cimv2 import topologyFieldHanlder
from smis_cimv2 import stringClean



def getSwitchRoles(instance, property = 'SwitchRole' ):
    SWITCH_ROLE_UNKNOWN = 'Unknown'
    SWITCH_ROLE_VALUE_MAP = {'0': 'Unknown',
                             '1': 'Principal',
                             '2': 'Subordinate',
                             '3': 'Disabled',
                             '4': 'Seed'
                             }
    roleValueList = []
    if instance:
        roleList = instance.getPropertyValue(property)
        if roleList:
            for s in roleList:
                roleValueList.append(SWITCH_ROLE_VALUE_MAP.get(str(s), SWITCH_ROLE_UNKNOWN))

    return ",".join(roleValueList)

class Namespace(smis_cimv2.BaseNamespace):
    def __init__(self):
        smis_cimv2.BaseNamespace.__init__(self)
        self.__namespace = 'root/brocade1'


    def associateTopologyObj2Discoverers(self, topology):
        self.handlers.append(topologyFieldHanlder(topology.storage_fabrics,
                                                  StorageFabricBrocadeDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.fc_switchs,
                                                  SwitchBrocadeDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.hosts,
                                                  ComputerSystemBrocadeDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.switch_2_fabric,
                                                  BrocadeSwitch2FabricLinksDiscover()))

        self.handlers.append(topologyFieldHanlder(topology.physical_switch_2_logical_switch,
                                                  PhysicalSwitch2LogicalSwitchLinksDiscover()))

        self.handlers.append(topologyFieldHanlder(topology.ports,
                                                  FcPortBrocadeDiscoverer()))

        self.handlers.append(topologyFieldHanlder(topology.fcport_connections,
                                                  BrocadeFCPortConnectionsDiscover()))

class StorageFabricBrocadeDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'Brocade_Fabric'

    def discover(self, client):
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        instances = client.getInstances(self.className)
        return self.parse(instances)

    def parse(self, instances):
        result = []
        for instance in instances:
            name = stringClean(instance.getPropertyValue('Name'))
            wwn = name
            fabric = smis.StorageFabric(name,wwn)
            result.append(fabric)
        return result

class ComputerSystemBrocadeDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'CIM_ComputerSystem'

    def discover(self, client):
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        instances = client.getInstances(self.className)
        return self.parse(instances)

    def parse(self, instances):
        hosts = []
        for instance in instances:
            className = stringClean(instance.getPropertyValue('CreationClassName'))
            name = stringClean(instance.getPropertyValue('ElementName'))
            if className != 'Brocade_Switch' and className != 'Brocade_PhysicalComputerSystem':
                id = stringClean(instance.getPropertyValue('Name'))
                hostObj = smis.Host(id,name=name)
                hosts.append(hostObj)
        return hosts

class SwitchBrocadeDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'Brocade_Switch'

    def discover(self, client):
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        instances = []
        switches = []
        physical_switches = []

        switches = client.getInstances(self.className)
        instances.extend(switches)

        pcs_class_name = "Brocade_PhysicalComputerSystem"
        try:
            physical_switches = client.getInstances(pcs_class_name)
        except:
            pass

        instances.extend(physical_switches)

        return self.parse(instances)

    def parse(self, instances):
        switches = []
        for instance in instances:
            name = stringClean(instance.getPropertyValue('ElementName'))

            wwn = stringClean(instance.getPropertyValue('Name'))
            roles = getSwitchRoles(instance)
            domainId = None
            vfId = None
            detailType = None
            identList = instance.getPropertyValue('IdentifyingDescriptions')
            identValue = instance.getPropertyValue('OtherIdentifyingInfo')
            try:
                domainIdIndex = identList.index('DomainID')
                domainId = stringClean(identValue[domainIdIndex])
                detailTypeIndex = identList.index('SNIA:DetailedType')
                detailType = stringClean(identValue[detailTypeIndex])
                vfIdIndex = identList.index('SNIA:VF_ID')
                vfId = stringClean(identValue[vfIdIndex])
            except:
                logger.debug('Failed to get switch domain id, vf id and type')
            switch = smis.FCSwtich(name,wwn,roles,domainId,detailType,vfId)
            switches.append(switch)
        return switches

class PhysicalSwitch2LogicalSwitchLinksDiscover(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'Brocade_SwitchInPCS'

    def discover(self, client):
        links = []
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        links = client.getInstances(self.className)
        return self.parse(links)

    def parse(self, links):
        result = {}
        for link in links:
            try:
                pswitchRef = link.getPropertyValue('Antecedent')
                pswitchId = stringClean(pswitchRef.getKeyValue('Name'))

                lswitchRef = link.getPropertyValue('Dependent')
                lswitchId = stringClean(lswitchRef.getKeyValue('Name'))
                result[lswitchId] = pswitchId
            except:
                logger.debugException('cannot find the physical switch to logical switch linkages')
        return result

class BrocadeSwitch2FabricLinksDiscover(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'Brocade_SwitchInFabric'

    def discover(self, client):
        links = []
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        links = client.getInstances(self.className)
        return self.parse(links)

    def parse(self, links):
        result = {}
        for link in links:
            try:
                fabricRef = link.getPropertyValue('GroupComponent')
                fabricId = stringClean(fabricRef.getKeyValue('Name'))

                switchRef = link.getPropertyValue('PartComponent')
                switchId = stringClean(switchRef.getKeyValue('Name'))
                result[switchId] = fabricId
            except:
                logger.debugException('cannot find the switch to fabric linkages')
        return result

class BrocadeFCPortConnectionsDiscover(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'CIM_FCActiveConnection'

    def discover(self, client):
        links = []
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        links = client.getInstances(self.className)
        return self.parse(links)

    def parse(self, links):
        result = {}
        for link in links:
            try:
                end1Ref = link.getPropertyValue('Antecedent')
                end1Id = stringClean(end1Ref.getKeyValue('Name'))

                end2Ref = link.getPropertyValue('Dependent')
                end2Id = stringClean(end2Ref.getKeyValue('Name'))
                result[end1Id] = end2Id
            except:
                logger.debugException('cannot find the fc port connections')

        return result

class FcPortBrocadeDiscoverer(BaseSmisDiscoverer):
    def __init__(self):
        BaseSmisDiscoverer.__init__(self)
        self.className = 'Brocade_SwitchFCPort'

    def discover(self, client):
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        result = []
        instances = client.getInstances(self.className)
        switchFcs = self.parse(instances)
        result.extend(switchFcs)
        className = 'Brocade_NodeFCPort'
        instances = client.getInstances(className)
        nodeFcs = self.parse(instances)
        result.extend(nodeFcs)
        className = 'Brocade_PCSNetworkPort'
        instances = client.getInstances(className)
        pcsFcs = self.parse(instances)
        result.extend(pcsFcs)
        return result

    def parse(self, instances):
        result = []
        for instance in instances:
            portId = stringClean(instance.getPropertyValue('PortNumber'))
            portName = stringClean(instance.getPropertyValue('ElementName'))
            portWwn  = instance.getPropertyValue('PermanentAddress')
            deviceId = stringClean(instance.getPropertyValue('DeviceID'))
            if deviceId and portWwn is None:
                portWwn = deviceId
            portIndex = stringClean(instance.getPropertyValue('PortNumber'))
            portStatus = instance.getPropertyValue('StatusDescriptions') and ",".join(instance.getPropertyValue('StatusDescriptions'))
            portState = smis_cimv2.PORT_STATE_VALUE_MAP.get(stringClean(instance.getPropertyValue('HealthState')), smis_cimv2.PORT_STATE_UNKNOWN)
            speedBps = stringClean(instance.getPropertyValue('Speed'))
            maxSpeedBps = instance.getPropertyValue('MaxSpeed')
            portType = smis_cimv2.PORT_TYPE_VALUE_MAP.get(stringClean(instance.getPropertyValue('PortType')), smis_cimv2.PORT_TYPE_RESERVED)

            referencedTo = None
            container = stringClean(instance.getPropertyValue('SystemName'))

            try:
                fcpObj = smis.FcPort(portId, portIndex, portWwn, portName, container, referencedTo, portStatus, portState, speedBps, container, maxSpeedBps, portType)
                result.append(fcpObj)
            except:
                logger.debugException('')
        return result
