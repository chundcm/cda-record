import re
import layer2
import logger
import ip_addr
import netutils
import modeling
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector

class BaseDiscoverer:
    def __init__(self, shell):
        self.shell = shell
    
    def getCommandOutput(self, command, timeout=10000, useCache=1):
        """
        Execute given command and return the output
        Returns None is the execution fails or the output is empty
        """
        if command:
            try:
                return self.shell.execCmd(command, timeout, 1, useCache=useCache)#wait for timeout
            except:
                logger.debugException('')
        else:
            logger.warn('Commands is empty')


class VlanDiscoverer(BaseDiscoverer):
    def __init__(self, shell):
        BaseDiscoverer.__init__(self, shell)

    def _parseVlans(self, output):
        results = []
        if output:
            m = re.search('----------[\r\n]+(.+)', output, re.DOTALL)
            if not m:
                return results
            data = m.group(1)
            vlan_id = None
            vlan_name = None
            vlan_status = None
            vlan_ports_str = None
            for line in re.split('[\r\n]+', data):
                m = re.match('(\d+)\s+([\w\-\.]+)\s+([\w\-]+)\s+(.+)', line)
                #logger.debug('Parsing line "%s"' % line)
                if m and vlan_id:
                    results.append(layer2.Vlan(vlan_id, vlan_name, vlan_status, [x.strip() for x in vlan_ports_str.split(',') if (x and x.strip())]))
                if m:
                    #logger.debug('Matched %s' % m.groups())
                    vlan_id = m.group(1)
                    vlan_name = m.group(2)
                    vlan_status = m.group(3)
                    vlan_ports_str = m.group(4)
                elif line and line.strip() and vlan_ports_str:
                    vlan_ports_str = vlan_ports_str + ', ' + line.strip()
            if vlan_id:
                results.append(layer2.Vlan(vlan_id, vlan_name, vlan_status, [x.strip() for x in vlan_ports_str.split(', ') if x]))
        return results

    def discoverVlans(self):
        output = self.getCommandOutput('sh vlan all-ports | no-more')
        return self._parseVlans(output)
    
class Layer2Discoverer(BaseDiscoverer):
    def __init__(self, shell):
        BaseDiscoverer.__init__(self, shell)

    def _getCdpLayer2Output(self):
        return self.getCommandOutput('show cdp neighbors detail | no-more')

    def _parseCdpLayer2Output(self, output):
        results = {}
        if output:
            blocks = re.split('----------------------------------------', output)
            for block in blocks:
                if not re.search('Device ID', block):
                    continue

                m = re.search('System Name:\s+([\w\-\.\/]+)', block)
                system_name = m and m.group(1)
                if not system_name:
                    continue

                m = re.search('Interface:\s+([\w\-\.\/]+),', block)
                local_interface_name = m and m.group(1)
                if not local_interface_name:
                    continue
                
                remote_ips = []
                elems = re.findall('IPv[46] Address:(.+)\r?\n?Platform', block, re.DOTALL)
                if m:
                    for elem in elems:
                        remote_ips = [ip_addr.IPAddress(raw_ip.strip()) for raw_ip in elem.split(',') if ip_addr.isValidIpAddress(raw_ip.strip())]
                m = re.search('Platform:\s+([\w\-\.]+),', block)
                platform = m and m.group(1)

#                '''
#                Capability Codes: R - Router, T - Trans-Bridge, B - Source-Route-Bridge
#                          S - Switch, H - Host, I - IGMP, r - Repeater,
#                          V - VoIP-Phone, D - Remotely-Managed-Device,
#                          s - Supports-STP-Dispute
#                '''
                m = re.search('Capabilities:\s+([\w\-\.]+)', block)
                type = m and m.group(1)
                mac = None
                iface_name = None
                m = re.search('Port\s+ID\s+\(outgoing\s+port\):\s+([\w\-\.\:\/]+)', block)#can be interface name, interface mac.
                if not m:
                    continue
                
                if netutils.isValidMac(m.group(1)):
                    mac = netutils.parseMac(m.group(1))
                else:
                    iface_name = m.group(1)
                m = re.search('Version:(.+)Advertisement', block, re.DOTALL)
                description = m and m.group(1).strip()
                try:
                    remote_list = results.get(local_interface_name, [])
                    remote_list.append(layer2.RemotePeer(system_name, iface_name, mac, remote_ips, platform, type, description))
                    results[local_interface_name] = remote_list
                except:
                    logger.debugException('')
                
        return results

    def getCdpLayer2(self):
        output = self._getCdpLayer2Output()
        return self._parseCdpLayer2Output(output)
    
    def discover(self):
        return self.getCdpLayer2()

class JuniperDiscoverer(BaseDiscoverer):
    def __init__(self, shell):
        BaseDiscoverer.__init__(self, shell)

    def _getLayer2(self):
        interface2Mac = {}
        output = self.getCommandOutput('show ethernet-switching table brief | no-more', useCache=0)
        if output:
            outputLines = output.splitlines()
            parseStarted = False
            macType = True
            Printed = False
            for i in range(0, len(outputLines)):
                logger.debug(outputLines[i])
                if re.search('vlan\s*mac address\s*type\s*age\s*interface', outputLines[i].lower()) or \
                   re.search('name\s*address\s*flags\s*interface', outputLines[i].lower()):
                    macType = not re.search('name\s*address\s*flags\s*interface', outputLines[i].lower())
                    parseStarted = True
                    continue
                if parseStarted:
                    if not Printed:
                        logger.debug('Start parsing.')
                        Printed = True
                    lineItem = outputLines[i].split()
                    if len(lineItem) == 5:
                        mac = lineItem[1].strip().replace(':', '')
                        type = lineItem[2].strip()
                        interface = lineItem[-1].strip()
                        if not macType or macType and type.lower() == 'learn':
                            if not interface2Mac.has_key(interface):
                                interface2Mac[interface] = {}
                            interface2Mac[interface][mac] = mac
                    else:
                        logger.debug('Not recogonized line: ', outputLines[i])
        logger.debug('interface2Mac: ', interface2Mac)
        return interface2Mac

    def reportLayer2(self, interface2Mac, switchOSH):
        vector = ObjectStateHolderVector()
        for interface in interface2Mac.keys():
            if len(interface2Mac[interface]) == 1:
                localInterfaceOSH = ObjectStateHolder("interface")
                localInterfaceOSH.setAttribute('interface_name', interface)
                localInterfaceOSH.setContainer(switchOSH)
                mac = interface2Mac[interface].popitem()[0]
                remoteInterfaceOSH = ObjectStateHolder("interface")
                remoteInterfaceOSH.setStringAttribute('mac_address', mac)
                remoteInterfaceOSH.setStringAttribute('interface_macaddr', mac)
                logger.debug('localInterfaceOSH --- remoteInterfaceOSH:  %s --- %s' % (interface, mac))
                vector.add(localInterfaceOSH)
                vector.add(switchOSH)
                if netutils.isValidMac(mac):
                    macList = [mac]
                    hostKey = min(macList)
                    hostOSH = modeling.createCompleteHostOSH('node', hostKey)
                    logger.debug('node: ', hostOSH)
                    remoteInterfaceOSH.setContainer(hostOSH)
                    switchId = switchOSH.getCmdbId().toString()
                    logger.debug('switchId: ', switchId)
                    nodeId = mac
                    if switchId and nodeId:
                        layer2OSH = ObjectStateHolder('layer2_connection')
                        layer2OSH.setAttribute('layer2_connection_id', str(hash(switchId + nodeId)))
                        layer2SwitchLink = modeling.createLinkOSH('member', layer2OSH, localInterfaceOSH)
                        layer2NoneLink = modeling.createLinkOSH('member', layer2OSH, remoteInterfaceOSH)
                        vector.add(layer2OSH)
                        vector.add(layer2SwitchLink)
                        vector.add(layer2NoneLink)
                        vector.add(remoteInterfaceOSH)
                        vector.add(hostOSH)
            else:
                #TODO: switch <-> switch
                pass
        return vector

    def discover(self, switchOSH):
        results = self._getLayer2()
        return self.reportLayer2(results, switchOSH)