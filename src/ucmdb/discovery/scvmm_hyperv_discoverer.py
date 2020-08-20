#coding=utf-8
import logger
import re
import rest_json as json
import modeling
import scvmm

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector


class HyperVDiscoverer():
    def __init__(self, Framework, shell, scvmmOSH, scvmmHostOSH):
        self.Framework = Framework
        self.shell = shell
        self.scvmm = scvmmOSH
        self.scvmmHost = scvmmHostOSH
        self.hypervHostsByName = {}
        self.virtualNetworksByName = {}
        self.switchesByName = {}
        self.vmsById = {}
        self.vmNetAdaptersByVmId = {}
        self.cmdletsPrefix = "ipmo virtualmachinemanager.psd1;"

    def discover(self):
        getHostsCmd = "$Hosts = Get-SCVMHost; $Hosts | ConvertTo-Csv | ConvertFrom-Csv | ConvertTo-Json; echo @UCMDB@; " \
                      "$VirtualNetworks = Get-SCVirtualNetwork; $VirtualNetworks | ConvertTo-Json; echo @UCMDB@; " \
                      "$VMs = Get-VM; $VMs | ConvertTo-Csv | ConvertFrom-Csv | ConvertTo-Json; echo @UCMDB@; $VMNetworkAdapters = @{}; " \
                      "foreach($VM in $VMs){$VMNetworkAdapter = Get-SCVirtualNetworkAdapter -VM $VM; $VMNetworkAdapters[$VM.VMId]=$VMNetworkAdapter};" \
                      "$VMNetworkAdapters | ConvertTo-Json"

        output = self.executeCmdByPowerShell(getHostsCmd)

        hypervHostsJson = output.split('@UCMDB@')[0]
        virtualNetworksJson = output.split('@UCMDB@')[1]
        vmsJson = output.split('@UCMDB@')[2]
        vmNetAdaptersJson = output.split('@UCMDB@')[3]

        self.parseHosts(hypervHostsJson, virtualNetworksJson)
        self.parseVms(vmsJson, vmNetAdaptersJson)

        return self.reportScvmm()

    def safeReport(self, item, fn):
        try:
            return fn(item)
        except:
            logger.debugException('Failed to report %s.' % item)

    def reportScvmm(self):
        vector = ObjectStateHolderVector()
        reporter = scvmm.Reporter(scvmm.Builder())
        for item in self.hypervHostsByName:
            object = self.hypervHostsByName[item]
            vector.addAll(reporter.reportHypervHost(object, self.scvmm, self.scvmmHost))
        for item in self.vmsById:
            object = self.vmsById[item]
            vector.addAll(self.safeReport(object, reporter.reportVM))
        for item in self.switchesByName:
            object = self.switchesByName[item]
            vector.addAll(self.safeReport(object, reporter.reportVirtualSwitch))

        vector.addAll(self.reportLayer2(self.hypervHostsByName))
        vector.addAll(self.reportLayer2(self.vmsById))

        return vector

    def reportLayer2(self, ifDict):
        vector = ObjectStateHolderVector()
        for item in ifDict:
            object = ifDict[item]
            for interface in object.Interfaces:
                if interface.LinkedInterface:
                    if interface.getOsh() and interface.LinkedInterface.getOsh():
                        layer2OSH = ObjectStateHolder('layer2_connection')
                        linkId = "%s:%s" % (interface.macAddress, interface.LinkedInterface.name)
                        linkId = str(hash(linkId))
                        layer2OSH.setAttribute('layer2_connection_id', linkId)

                        interfaceMemberLink = modeling.createLinkOSH('member', layer2OSH, interface.getOsh())
                        portMemberLink = modeling.createLinkOSH('member', layer2OSH, interface.LinkedInterface.getOsh())

                        vector.add(layer2OSH)
                        vector.add(interfaceMemberLink)
                        vector.add(portMemberLink)
                    else:
                        logger.debug('Failed to report Layer2 connection for interface: %s, linked interface OSH is not found.' % interface)
        return vector

    def parseHosts(self, hypervHosts, virtualNetworksJson):
        hostList = self.convertToList(hypervHosts)
        virtualNetworkList = self.convertToList(virtualNetworksJson)

        for vn in virtualNetworkList:
            hostName = vn['VMHost']['Name']
            name = vn['Name']
            self.virtualNetworksByName[hostName] = {}
            self.virtualNetworksByName[hostName][name] = vn

        for host in hostList:
            type = host['VirtualizationPlatform']
            name = host['Name']
            if type.lower() == 'hyperv':
                id = host['ID']
                description = host['Description']
                hostName = host['ComputerName']
                vmHostGroup = host['VMHostGroup']
                hypervHost = scvmm.HypervHost(id, name, hostName, description, vmHostGroup)

                self.hypervHostsByName.setdefault(name, hypervHost)
            else:
                logger.debug('Not supported virtualization type. Skip host %s, type: %s.' % (name, type))

    def parseVms(self, vms, vmNetAdaptersJson):
        vmList = self.convertToList(vms)
        self.vmNetAdaptersByVmId = self.reLoadVmNetAdapters(vmNetAdaptersJson)

        for vm in vmList:
            name = vm['Name']
            hostName = vm['HostName']
            virtualMachineState = vm['VirtualMachineState']
            if not hostName in self.hypervHostsByName.keys():
                logger.debug('Skip VM %s. Its host %s is not Hyper-V host.' % (name, hostName))
                continue
            vmId = vm['VMId']
            description = vm['Description']
            memory = vm['Memory']
            startAction = vm['StartAction']
            stopAction = vm['StopAction']
            dynamicMemoryEnabled = vm['DynamicMemoryEnabled']
            dynamicMemoryMaximumMB = vm['DynamicMemoryMaximumMB']
            dynamicMemoryMinimumMB = vm['DynamicMemoryMinimumMB']
            if dynamicMemoryEnabled == 'True':
                memoryLimit = dynamicMemoryMaximumMB
                memoryReserve = dynamicMemoryMinimumMB
            else:
                memoryLimit = memory
                memoryReserve = memory
            creationTime = vm['CreationTime']
            checkpointLocation = vm['CheckpointLocation']
            cpuCount = vm['CPUCount']
            cpuReserve = vm['CPUReserve']
            relativeWeight = vm['RelativeWeight']
            cpuLimit = vm['CPUMax']
            cpuPerVirtualNumaNodeMaximum = vm['CPUPerVirtualNumaNodeMaximum']
            status = vm['Status']

            vmObj = scvmm.VM(vmId, name, description, creationTime, checkpointLocation, cpuCount, cpuReserve, cpuLimit,
                          relativeWeight, cpuPerVirtualNumaNodeMaximum, memoryLimit, memoryReserve, startAction,
                          stopAction, status)
            vmObj.Host = self.hypervHostsByName[hostName]

            self.parseVmNetAdapters(vmObj, hostName)

            self.vmsById.setdefault(vmId, vmObj)

    def parseVmNetAdapters(self, vm, hostName):
        vmNetAdapters = self.vmNetAdaptersByVmId[vm.getId()]
        for vmNetAdapter in vmNetAdapters:
            interfaceId = vmNetAdapter['ID']
            vmMacAddress = reformatMac(vmNetAdapter['MACAddress'])
            vmIpAddresses = vmNetAdapter['IPv4Addresses']
            virtualNetworkName = vmNetAdapter['VirtualNetwork']
            if not virtualNetworkName:
                continue

            if vmMacAddress:
                vmInterface = scvmm.Interface(None, vmMacAddress)
                vm.MacAddresses.append(vmMacAddress)
                vSwitchVMInterface = scvmm.Interface(interfaceId, None)
                vmInterface.LinkedInterface = vSwitchVMInterface
                vm.Interfaces.append(vmInterface)
            else:
                vmInterface = None
            if vmIpAddresses:
                vm.IpAddresses.extend(vmIpAddresses)

            if not vmInterface:
                continue
            virtualNetwork = self.virtualNetworksByName[hostName][virtualNetworkName]
            hostNetworkAdapters = virtualNetwork['VMHostNetworkAdapters']
            for hostNetworkAdapter in hostNetworkAdapters:
                vnName = hostNetworkAdapter['VirtualNetwork']
                if vnName == virtualNetworkName:
                    if not vnName in self.switchesByName.keys():
                        hostIpAddresses = hostNetworkAdapter['IPAddresses']
                        hostMacAddress = reformatMac(hostNetworkAdapter['MacAddress'])
                        if hostMacAddress:
                            hostInterface = scvmm.Interface(None, hostMacAddress)
                            vSwitchHostInterface = scvmm.Interface(vnName, None)
                            hostInterface.LinkedInterface = vSwitchHostInterface
                            vm.Host.Interfaces.append(hostInterface)
                            vm.Host.MacAddresses.append(hostMacAddress)
                            vm.Host.IpAddresses.append(hostIpAddresses)

                            vSwitch = scvmm.VirtualSwitch(vnName, vm.getName(), hostMacAddress)
                            vSwitchVMInterface.Host = vSwitch
                            vSwitchHostInterface.Host = vSwitch
                            vSwitch.Host = vm.Host
                            vSwitch.Interfaces.append(vSwitchVMInterface)
                            vSwitch.Interfaces.append(vSwitchHostInterface)
                            self.switchesByName.setdefault(vnName, vSwitch)
                    else:
                        vSwitch = self.switchesByName[vnName]
                        hostIpAddresses = hostNetworkAdapter['IPAddresses']
                        hostMacAddress = reformatMac(hostNetworkAdapter['MacAddress'])
                        if hostMacAddress:
                            hostInterface = scvmm.Interface(None, hostMacAddress)
                            vSwitchHostInterface = scvmm.Interface(vnName, None)
                            hostInterface.LinkedInterface = vSwitchHostInterface
                            vm.Host.Interfaces.append(hostInterface)
                            vm.Host.MacAddresses.append(hostMacAddress)
                            vm.Host.IpAddresses.append(hostIpAddresses)

                            vSwitchVMInterface.Host = vSwitch
                            vSwitchHostInterface.Host = vSwitch
                            vSwitch.Interfaces.append(vSwitchVMInterface)
                            vSwitch.Interfaces.append(vSwitchHostInterface)

    def convertToList(self, resourceJson):
        resource = json.loads(resourceJson)
        if isinstance(resource, dict):
            return [resource]
        elif isinstance(resource, list):
            return resource
        else:
            return []

    def reLoadVmNetAdapters(self, vmNetAdaptersJson):
        vmNetAdaptersByVmId = {}
        vmNetAdapters = json.loads(vmNetAdaptersJson)
        for (vmId, vmNetAdapter) in vmNetAdapters.items():
            vmNetAdaptersByVmId[vmId] = []
            if isinstance(vmNetAdapter, list):
                vmNetAdaptersByVmId[vmId].extend(vmNetAdapter)
            elif isinstance(vmNetAdapter, dict):
                vmNetAdaptersByVmId[vmId].append(vmNetAdapter)
            else:
                logger.debug('Failed to load VM Network Adapter: ', vmNetAdapter)
        return vmNetAdaptersByVmId

    def executeCmdByPowerShell(self, cmd):
        cmd = ''.join((self.cmdletsPrefix, cmd))
        logger.debug("cmdline:", cmd)

        return self.parsePowerShellEncodeOutput(self.shell.executeCmdlet(cmd))

    def parsePowerShellEncodeOutput(self, content):
        pattern = "< CLIXML([\s\S][^<]*)<"
        match = re.search(pattern, content)
        if match:
            return match.group(1).strip()
        return content


def reformatMac(mac):
    if mac:
        try:
            return mac.replace(':', '')
        except:
            logger.debug('Not supported MAC address format: ', mac)
            return None
    else:
        return None
