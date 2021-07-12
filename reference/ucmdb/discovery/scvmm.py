#coding=utf-8
import entity
import logger
import modeling
from vendors import PlatformVendors

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector


class HasId:
    def __init__(self, id):
        if id is None:
            raise ValueError("Id is empty")
        self.__id = id

    def getId(self):
        return self.__id


class VirtualSwitch(entity.HasName, entity.HasOsh):
    def __init__(self, name, vmName=None, hostMacAddress=None):
        entity.HasOsh.__init__(self)
        entity.HasName.__init__(self, name)
        self.Interfaces = []
        self.MacAddress = None
        self.vmName = vmName
        self.hostMacAddress = hostMacAddress
        self.Host = None

    def acceptVisitor(self, visitor):
        return visitor.visitVirtualSwitch(self)

    def __repr__(self):
        return "VirtualSwitch %s" % self.getName()


class HypervHost(HasId, entity.HasName, entity.HasOsh):
    def __init__(self, id, name, hostname, description=None, vmHostGroup=None):
        HasId.__init__(self, id)
        entity.HasOsh.__init__(self)
        entity.HasName.__init__(self, hostname)
        self.Interfaces = []
        self.MacAddresses = []
        self.IpAddresses = []
        self.description = description
        self.vmHostGroup = vmHostGroup
        self.HypervisorOSH = None
        self.lowestMac = None

    def acceptVisitor(self, visitor):
        return visitor.visitHypervHost(self)

    def __repr__(self):
        return "HypervHost(%s, %s)" % (self.getId(), self.getName())


class Interface(entity.HasOsh):
    def __init__(self, name=None, macAddress=None, description=None):
        entity.HasOsh.__init__(self)
        self.LinkedInterface = None
        self.Host = None
        self.name = name
        self.macAddress = macAddress
        self.description = description

    def acceptVisitor(self, visitor):
        return visitor.visitInterface(self)

    def __repr__(self):
        return "Interface(%s, %s)" % (self.name, self.macAddress)


class VM(HasId, entity.HasName, entity.HasOsh):
    def __init__(self, id, name, description=None, creationTime=None, checkpointLocation=None, cpuCount=None,
                 cpuReserve=None, cpuLimit=None, relativeWeight=None, cpuPerVirtualNumaNodeMaximum=None,
                 memoryLimit=None, memoryReserve=None, startAction=None, stopAction=None, status=None):
        HasId.__init__(self, id)
        entity.HasOsh.__init__(self)
        entity.HasName.__init__(self, name)
        self.Host = None
        self.IpAddresses = []
        self.Interfaces = []
        self.MacAddresses = []
        self.description = description
        self.creationTime = creationTime
        self.checkpointLocation = checkpointLocation
        self.cpuCount = cpuCount
        self.cpuReserve = cpuReserve
        self.cpuLimit = cpuLimit
        self.relativeWeight = relativeWeight
        self.cpuPerVirtualNumaNodeMaximum = cpuPerVirtualNumaNodeMaximum
        self.memoryLimit = memoryLimit
        self.memoryReserve = memoryReserve
        self.startAction = startAction
        self.stopAction = stopAction
        self.status = status
        self.__config = None

    def setVmConfigOsh(self, cfg):
        self.__config = cfg

    def getVmConfigOsh(self):
        return self.__config

    def acceptVisitor(self, visitor):
        return visitor.visitVM(self)

    def __repr__(self):
        return 'VM("%s", "%s")' % (self.getId(), self.getName())


class Builder:
    def visitInterface(self, interface):
        osh = ObjectStateHolder("interface")
        if interface.name:
            osh.setStringAttribute('interface_name', interface.name)
        if interface.macAddress:
            osh.setStringAttribute('mac_address', interface.macAddress)
        if interface.description:
            osh.setStringAttribute('interface_description', interface.description)

        return osh

    def visitHypervHost(self, host):
        osh = ObjectStateHolder("nt")
        osh.setStringAttribute('name', host.getName())
        osh.setStringAttribute('description', host.description)
        if host.MacAddresses:
            host.MacAddresses.sort()
            host.lowestMac = host.MacAddresses[0]
        else:
            raise ValueError('No MAC address found for host: %s.' % host)

        return osh

    def visitVirtualSwitch(self, switch):
        if not switch.Host.lowestMac:
            raise ValueError('No MAC address found for host: %s.' % switch.host)
        hostKey = "%s_%s" % (switch.Host.lowestMac, switch.getName())
        osh = modeling.createCompleteHostOSH('switch', hostKey)
        hostBuilder = modeling.HostBuilder(osh)
        hostBuilder.setAsLanSwitch(1)
        hostBuilder.setAsVirtual(1)
        osh = hostBuilder.build()
        osh.setStringAttribute('name', switch.getName())

        return osh

    def visitVM(self, vm):
        def buildVmConfig(vm):
            vmConfigOsh = ObjectStateHolder('hyperv_partition_config')
            vmConfigOsh.setStringAttribute('name', 'Microsoft Hyper-V Partition Configuration')
            vmConfigOsh.setContainer(vm.getOsh())
            vmConfigOsh.setAttribute('partition_guid', vm.getId())
            vmConfigOsh.setAttribute('partition_name', vm.getName())
            vmConfigOsh.setAttribute('enabled_state', vm.status)
            vmConfigOsh.setAttribute('external_data_root', vm.checkpointLocation)
            vmConfigOsh.setAttribute('snapshot_data_root', vm.checkpointLocation)
            vmConfigOsh.setAttribute('automatic_shutdown_action', vm.startAction)
            vmConfigOsh.setAttribute('automatic_startup_action', vm.stopAction)
            vmConfigOsh.setLongAttribute('memory_limit', vm.memoryLimit)
            vmConfigOsh.setLongAttribute('memory_reservation', vm.memoryReserve)
            vmConfigOsh.setIntegerAttribute('processor_limit', vm.cpuLimit)
            vmConfigOsh.setIntegerAttribute('processor_reservation', vm.cpuReserve)
            vmConfigOsh.setIntegerAttribute('processor_weight', vm.relativeWeight)
            vmConfigOsh.setIntegerAttribute('logical_processor_number', vm.cpuCount)
            vm.setVmConfigOsh(vmConfigOsh)

        if vm.IpAddresses:
            address = vm.IpAddresses[0]
            osh = modeling.createHostOSH(address)
        else:
            logger.debug('No IP address found for VM: %s' % vm)
            if vm.MacAddresses:
                vm.MacAddresses.sort()
                lowestMac = vm.MacAddresses[0]
                osh = modeling.createCompleteHostOSH('host', lowestMac)
            else:
                raise ValueError('Both IP and MAC are not found for VM: %s' % vm)

        osh.setStringAttribute('description', vm.description)
        osh.setStringAttribute("name", vm.getName())
        osh.setBoolAttribute('host_iscomplete', True)
        osh.setBoolAttribute('host_isvirtual', True)
        id = vm.getId()
        if id:
            osh.setStringAttribute("cloud_instance_id", id)
            osh.setStringAttribute("host_key", id)
        # Host Platform Vendor
        osh.setStringAttribute('platform_vendor', PlatformVendors.Hyperv)
        buildVmConfig(vm)
        return osh


class Reporter:
    def __init__(self, builder):
        self.__builder = builder

    def reportHypervHost(self, host, scvmmOSH, scvmmHostOSH):
        if not host:
            raise ValueError("Host is not specified")
        vector = ObjectStateHolderVector()
        hostOSH = host.build(self.__builder)
        vector.add(hostOSH)
        hypervisorOSH = modeling.createApplicationOSH('virtualization_layer', 'Microsoft Hyper-V Hypervisor', hostOSH, vendor='microsoft_corp')
        vector.add(hypervisorOSH)
        host.HypervisorOSH = hypervisorOSH
        groupOSH = ObjectStateHolder('scvmm_host_group')
        groupOSH.setStringAttribute('name', host.vmHostGroup)
        groupOSH.setContainer(scvmmHostOSH)
        vector.add(groupOSH)
        vector.add(modeling.createLinkOSH('membership', groupOSH, hostOSH))
        vector.add(modeling.createLinkOSH('membership', scvmmOSH, hypervisorOSH))

        for interface in host.Interfaces:
            interfaceOSH = interface.build(self.__builder)
            interfaceOSH.setContainer(hostOSH)
            vector.add(interfaceOSH)

        vector.addAll(self.linkIpsToNode(host))
        return vector

    def reportVirtualSwitch(self, switch):
        if not switch:
            raise ValueError("VirtualSwitch is not specified")
        vector = ObjectStateHolderVector()
        switchOSH = switch.build(self.__builder)
        vector.add(switchOSH)

        if switch.Host.HypervisorOSH:
            vector.add(modeling.createLinkOSH('run', switch.Host.HypervisorOSH, switchOSH))
        else:
            logger.debug('Hypervisor is not found for node: ', switch)

        for interface in switch.Interfaces:
            interfaceOSH = interface.build(self.__builder)
            interfaceOSH.setContainer(switchOSH)
            vector.add(interfaceOSH)
        return vector

    def reportVM(self, vm):
        if not vm:
            raise ValueError("VM is not specified")
        vector = ObjectStateHolderVector()
        vmOSH = vm.build(self.__builder)
        vector.add(vmOSH)
        if not vm.getVmConfigOsh():
            raise ValueError("Instance Config is not specified")
        vmConfigOSH = vm.getVmConfigOsh()
        vmConfigOSH.setContainer(vm.getOsh())
        vector.add(vmConfigOSH)

        if vm.Host.HypervisorOSH:
            vector.add(modeling.createLinkOSH('run', vm.Host.HypervisorOSH, vmOSH))
        else:
            logger.debug('Hypervisor is not found for VM: ', vm)

        for interface in vm.Interfaces:
            interfaceOSH = interface.build(self.__builder)
            interfaceOSH.setContainer(vmOSH)
            vector.add(interfaceOSH)

        vector.addAll(self.linkIpsToNode(vm))
        return vector

    def linkIpsToNode(self, node):
        # report IPs
        vector = ObjectStateHolderVector()
        for ipAddress in node.IpAddresses:
            logger.debug('link ip %s to node %s' % (ipAddress, node))
            ipOSH = modeling.createIpOSH(ipAddress)
            if node.getOsh():
                vector.add(ipOSH)
                vector.add(modeling.createLinkOSH('containment', node.getOsh(), ipOSH))
            else:
                logger.debug('Node OSH not created for ip: ' % ipAddress)
        return vector
