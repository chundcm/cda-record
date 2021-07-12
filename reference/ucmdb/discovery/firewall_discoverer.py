import logger
import firewall
import snmputils
import ip_addr
     
import re
import xml.etree.ElementTree as ET

class BaseFirewallDiscoverer:
    '''
    Base discoverer class used for all firewall related discovery activities
    '''
    def __init__(self, client):
        self._client = client
        
    def discover(self):
        raise NotImplemented
    

class JuniperFirewallDiscoverer(BaseFirewallDiscoverer):
    '''
        Discoverer for Juniper vendor devices
    '''
    def __init__(self, client):
        BaseFirewallDiscoverer.__init__(self, client)
    
    def parseNatedNetworks(self, elems):
        result = []
        if not elems:
            return []
        for elem in elems:
            logger.debug(elem.meta_data)
            m = re.match('(\d+.\d+.\d+.\d+)\.(\d+.\d+.\d+.\d+)', elem.meta_data)
            if m:
                ip = m.group(1) 
                mask = m.group(2)
                if ip_addr.isValidIpAddressNotZero(ip):
                    network = firewall.NatedNetwork(ip, mask)
                    result.append(network)
        return result
         
    def getNatedNetworks(self):
        result = []
        snmpAgent = snmputils.SnmpAgent(None, self._client)
        queryBuilder = snmputils.SnmpQueryBuilder('1.3.6.1.4.1.2636.3.38.1.1')
        queryBuilder.addQueryElement(1, 'Name')
        try:
            elems = snmpAgent.getSnmpData(queryBuilder)
            result = self.parseNatedNetworks(elems)
        except:
            logger.debugException('')
            logger.warn('Failed getting NAT information')

        return result

    def getNatInformation(self):
        '''jnxJsSrcNatTable: 1.3.6.1.4.1.2636.3.39.1.7.1.1.2'''
        result = []
        snmpAgent = snmputils.SnmpAgent(None, self._client)
        queryBuilder = snmputils.SnmpQueryBuilder('1.3.6.1.4.1.2636.3.39.1.7.1.1.2.1')
        queryBuilder.addQueryElement(1, 'Name')
        queryBuilder.addQueryElement(2, 'Global_address')
        queryBuilder.addQueryElement(4, 'Number_of_used_ports')
        queryBuilder.addQueryElement(5, 'Number_of_sessions')
        queryBuilder.addQueryElement(6, 'Assoc_Interface')
        try:
            result = snmpAgent.getSnmpData(queryBuilder)
        except:
            logger.warn('Failed getting NAT information')

        return result

    def getFilterInformation(self):
        result = []
        snmpAgent = snmputils.SnmpAgent(None, self._client)
        queryBuilder = snmputils.SnmpQueryBuilder('1.3.6.1.4.1.2636.3.5.1.1')
        queryBuilder.addQueryElement(1, 'Name')
        queryBuilder.addQueryElement(2, 'Counter')
        queryBuilder.addQueryElement(4, 'Type')
        try:
            result = snmpAgent.getSnmpData(queryBuilder)
        except:
            logger.warn('Failed getting Filter information')
        return result

    def getJSPolicy(self):
        result = []
        
        snmpAgent = snmputils.SnmpAgent(None, self._client)
        queryBuilder = snmputils.SnmpQueryBuilder('1.3.6.1.4.1.2636.3.39.1.4.1.1.2.1')
        queryBuilder.addQueryElement(1, 'Zone_name')
        queryBuilder.addQueryElement(3, 'Policy_Name')
        queryBuilder.addQueryElement(5, 'Policy_action')
        queryBuilder.addQueryElement(7, 'Policy_state')
        try:
            result = snmpAgent.getSnmpData(queryBuilder)
        except:
            logger.warn('Failed getting JS Policy information')

        return result
        
#information is not present in the dump
#    def discoverVlans(self):
#        pass
    
    def discover(self):
        try:
            config = firewall.FirewallConfig('Firewall configuration')
        except:
            logger.debugException('Failed to get config part')
        
        try:
            config.type_to_rules_dict['Nat'] = self.getNatInformation()
        except:
            logger.debugException('Failed to get config part')
        
        try:
            config.type_to_rules_dict['Filter'] = self.getFilterInformation()
        except:
            logger.debugException('Failed to get config part')
        
        try:
            config.type_to_rules_dict['JSPolicy'] = self.getJSPolicy()
        except:
            logger.debugException('Failed to get config part')
            
        try:
            config.nated_networks = self.getNatedNetworks()
        except:
            logger.debugException('Failed to get nated networks part')

        return config


class JuniperFirewallDiscoverByShell(BaseFirewallDiscoverer):
    '''
        Discoverer for Juniper Firewall with Firwall Topology by Shell job
    '''
    def __init__(self, client):
        BaseFirewallDiscoverer.__init__(self, client)

    def execJunosCmdByShell(self, cmd, timeout=10000):
        try:
            output = self._client.execCmd(cmd, timeout, 1)
            if output.find('</rpc-reply>'):
                output = output.split('</rpc-reply>')[0] + '</rpc-reply>'
            return output
        except:
            logger.debugException('')

    def parseInterface(self, root):
        interfaceList = []
        for configuration in root.getchildren():
            if 'configuration' in configuration.tag:
                for interfaces in configuration.findall('interfaces'):
                    for interfaceItem in interfaces.findall('interface'):
                        interface = None
                        for interfaceAttr in interfaceItem.getchildren():
                            if 'name' == interfaceAttr.tag and interfaceAttr.text:
                                interfaceName = interfaceAttr.text
                                fwInterfaceList = self.parseIpAddressRelatedToInterface(interfaceItem, interfaceName)
                                interfaceList += fwInterfaceList
        return interfaceList

    def parseRoutingInstance(self, root):
        routingInstanceList = []
        for routingInstances in root.findall('routing-instances'):
            for instance in routingInstances.findall('instance'):
                routingInstanceName = ''
                routingInstanceTpye = ''
                for instanceAttr in instance.getchildren():
                    if 'name' == instanceAttr.tag and instanceAttr.text:
                        routingInstanceName = instanceAttr.text
                    if 'instance-type' == instanceAttr.tag and instanceAttr.text:
                        routingInstanceTpye = instanceAttr.text
                if routingInstanceName and routingInstanceTpye:
                    routingInstance = firewall.RoutingInstance(routingInstanceName, routingInstanceTpye)
                    routingInstanceList.append(routingInstance)
        return routingInstanceList

    def parseClusterNode(self, root, firewallCluster):
        for configuration in root.getchildren():
            if 'configuration' in configuration.tag:
                for groups in configuration.findall('groups'):
                    for groupsAttr in groups.getchildren():
                        fw = None
                        if 'system' == groupsAttr.tag:
                            for system in groupsAttr.getchildren():
                                if 'host-name' == system.tag:
                                    fw = firewall.Firewall(system.text)
                                if 'tacplus-server' == system.tag:
                                    for tacplusAttr in system.getchildren():
                                        if 'name' == tacplusAttr.tag and fw:
                                            fw.nodeList.append(tacplusAttr.text)
                        if fw:
                            firewallCluster.firewallList.append(fw)

    def parseClusterResourceGroup(self, root, firewallCluster):
        for configuration in root.getchildren():
            if 'configuration' in configuration.tag:
                for chassis in configuration.findall('chassis'):
                    for cluster in chassis.findall('cluster'):
                        for group in cluster.findall('redundancy-group'):
                            clusterResourceGroup = None
                            for groupAttr in group.getchildren():
                                if 'name' == groupAttr.tag:
                                    clusterResourceGroup = firewall.ClusterResourceGroup(groupAttr.text)
                                if 'node' == groupAttr.tag:
                                    for nodeAttr in groupAttr.getchildren():
                                        if 'name' == nodeAttr.tag and clusterResourceGroup:
                                            clusterResourceGroup.groupsNameList.append('node%s' % nodeAttr.text)
                            if clusterResourceGroup:
                                firewallCluster.clusterResourceGroupList.append(clusterResourceGroup)

    def parseNodeAndUser(self, root):
        userList = []
        nodeList = []
        for configuration in root.getchildren():
            if 'configuration' in configuration.tag:
                for system in configuration.findall('system'):
                    for systemAttr in system.getchildren():
                        if 'name-server' == systemAttr.tag:
                            for nameServer in systemAttr.getchildren():
                                if 'name' == nameServer.tag:
                                    nodeList.append(nameServer.text)
                        if 'tacplus-server' == systemAttr.tag:
                            for tacplusServer in systemAttr.getchildren():
                                if 'name' == tacplusServer.tag:
                                    nodeList.append(tacplusServer.text)
                        if 'login' == systemAttr.tag:
                            for login in systemAttr.findall('user'):
                                for userAttr in login.getchildren():
                                    if 'name' == userAttr.tag:
                                        userList.append(firewall.OsUser(userAttr.text))
                        if 'ntp' == systemAttr.tag:
                            for ntp in systemAttr.findall('server'):
                                for ntpAttr in ntp.getchildren():
                                    if 'name' == ntpAttr.tag:
                                        nodeList.append(ntpAttr.text)
        return nodeList, userList

    def parseVirtualFirewall(self, root):
        vFirewallList = []
        # get virtual firewall
        for configuration in root.getchildren():
            if 'configuration' in configuration.tag:
                for logicalSystem in configuration.findall('logical-systems'):
                    vFirewall = None
                    vfInterfaceList = []
                    for name in logicalSystem.findall('name'):
                        vFirewall = firewall.VirtualFirewall(name.text)
                        for interfaces in logicalSystem.findall('interfaces'):
                            for interface in interfaces.findall('interface'):
                                for interfaceAttr in interface.getchildren():
                                    if 'name' == interfaceAttr.tag and interfaceAttr.text:
                                        interfaceNamePrefix = interfaceAttr.text
                                        vfInterfaceList += self.parseIpAddressRelatedToInterface(interface, interfaceNamePrefix)
                    if vFirewall:
                        vFirewall.interfaceList = vfInterfaceList
                        vFirewall.routingInstanceList = self.parseRoutingInstance(logicalSystem)
                        vFirewallList.append(vFirewall)
        return vFirewallList

    def parseIpAddressRelatedToInterface(self, root, interfaceNamePrefix):
        vfInterfaceList = []
        for unit in root.findall('unit'):
            ipList = []
            vfInterface = None
            for unitAttr in unit.getchildren():
                if unitAttr.tag == 'name' and unitAttr.text:
                    interfaceName = '%s.%s' %(interfaceNamePrefix, unitAttr.text)
                    vfInterface = firewall.Interface(interfaceName)
                    vfInterfaceList.append(vfInterface)
                if unitAttr.tag == 'family':
                    for inet in unitAttr.findall('inet'):
                        for address in inet.findall('address'):
                            for attr in address.findall('name'):
                                ipV4 = attr.text.split('/')[0].strip()
                                ipList.append(ipV4)
                    # for inet in unitAttr.findall('inet6'):
                    #     for address in inet.findall('address'):
                    #         for attr in address.findall('name'):
                    #             ipV6 = attr.text.split('/')[0].strip()
                    #             ipList.append(ipV6)
            if vfInterface:
                vfInterface.ipAddressList = ipList
        return vfInterfaceList

    def parseFirewallConfiguration(self, content, firewallCluster):
        content = content.split('show configuration | display xml')[-1].strip()
        virtualFirewallList = []
        nodeList = []
        userList = []
        interfaceList = []
        routingInstanceList = []
        configDoc = None
        # the output is in xml format
        if content:
            configDoc = firewall.ConfigurationDocument(content)
        root = ET.fromstring(content)
        if root is None:
            logger.debug('No Configuration')
        else:
            # get virtual firewall and its related CIs
            virtualFirewallList = self.parseVirtualFirewall(root)
            # get nodes and OS user related to host
            nodeList, userList = self.parseNodeAndUser(root)
            # get firewall cluster nodes and their related CIs
            if firewallCluster:
                self.parseClusterResourceGroup(root, firewallCluster)
                # groups are the cluster nodes configuration
                self.parseClusterNode(root, firewallCluster)
            # get interfaces belong to the discovered firewall
            interfaceList = self.parseInterface(root)
            # get routing instance and its interface and ipaddress
            routingInstanceList = self.parseRoutingInstance(root)
        return virtualFirewallList, nodeList, userList, interfaceList, routingInstanceList, configDoc

    def parseFirewallCluster(self, content):
        # firewall cluster Id is unique, and treat it as cluster name
        firewallCluster = None
        pattern = re.compile('Cluster ID: (.*)')
        m = pattern.search(content)
        if m and m.group(1):
            firewallCluster = firewall.FirewallCluster(m.group(1))
        return firewallCluster

    def parseChassis(self, content):
        content = content.split('show chassis hardware detail | display xml')[-1].strip()
        chassisList = []
        root = ET.fromstring(content)
        for engineResults in root.findall('multi-routing-engine-results'):
            for engineItem in engineResults.findall('multi-routing-engine-item'):
                for chassisInventory in engineItem.getchildren():
                    if 'chassis-inventory' in chassisInventory.tag:
                        for chassisItem in chassisInventory.getchildren():
                            if 'chassis' in chassisItem.tag:
                                for item in chassisItem.getchildren():
                                    if 'serial-number' in item.tag:
                                        chassisList.append(firewall.Chassis(item.text))

        return chassisList

    def getFirewallConfiguration(self, firewallCluster):
        getFWConfigCmd = 'show configuration | no-more | display xml'
        output = self.execJunosCmdByShell(getFWConfigCmd)
        return self.parseFirewallConfiguration(output, firewallCluster)

    def getFirewallCluster(self):
        getFirewallClustrCmd = 'show chassis cluster status | no-more'
        output = self.execJunosCmdByShell(getFirewallClustrCmd)
        return self.parseFirewallCluster(output)

    def getChassis(self):
        getChassisCmd = 'show chassis hardware detail | no-more | display xml'
        return self.parseChassis(self.execJunosCmdByShell(getChassisCmd))

    def discover(self):
        chassisList = []
        firewallCluster = None
        configDoc = None
        virtualFirewallList = []
        nodeList = []
        userList = []
        interfaceList = []
        routingInstanceList = []
        try:
            chassisList = self.getChassis()
        except:
            logger.debugException('Failed to get chassis')
        try:
            firewallCluster = self.getFirewallCluster()
        except:
            logger.debugException('Failed to get firewall cluster')
        try:
            virtualFirewallList, nodeList, userList, interfaceList, routingInstanceList, configDoc = self.getFirewallConfiguration(firewallCluster)
        except:
            logger.debugException('Failed to get Juniper Firewall Configuration')
        return chassisList, virtualFirewallList, nodeList, userList, interfaceList, routingInstanceList, configDoc, firewallCluster


class FortigateFirewallDiscoverer(BaseFirewallDiscoverer):
    '''
        Discoverer for Fortinet vendor devices,
    '''
    def __init__(self, client):
        BaseFirewallDiscoverer.__init__(self, client)
        
    def getFirewallConfig(self):
        result = []
        snmpAgent = snmputils.SnmpAgent(None, self._client)
        queryBuilder = snmputils.SnmpQueryBuilder('1.3.6.1.4.1.12356.101.5.1.2')
        queryBuilder.addQueryElement(1, 'Pol_Id') #string
        queryBuilder.addQueryElement(4, 'Pkt_Count') #int
        queryBuilder.addQueryElement(3, 'Byte_Count')#int
        try:
            result = snmpAgent.getSnmpData(queryBuilder)
        except:
            logger.warn('Failed getting basic config')

        return result

    def getAntivirusConfig(self):
        result = []
        snmpAgent = snmputils.SnmpAgent(None, self._client)
        queryBuilder = snmputils.SnmpQueryBuilder('1.3.6.1.4.1.12356.101.8.2.1.1')
        queryBuilder.addQueryElement(1, 'AV_Detected')
        queryBuilder.addQueryElement(2, 'AV_Blocked')
        queryBuilder.addQueryElement(3, 'HTTP_AV_Detected')
        queryBuilder.addQueryElement(4, 'HTTP_AV_Blocked')
        queryBuilder.addQueryElement(5, 'SMTP_AV_Detected')
        queryBuilder.addQueryElement(6, 'SMTP_AV_Blocked')
        queryBuilder.addQueryElement(7, 'POP3_AV_Detected')
        queryBuilder.addQueryElement(8, 'POP3_AV_Blocked')
        queryBuilder.addQueryElement(9, 'IMAP_AV_Detected')
        queryBuilder.addQueryElement(10, 'IMAP_AV_Blocked')
        try:
            result = snmpAgent.getSnmpData(queryBuilder)
        except:
            logger.warn('Failed getting Antivirus config')

        return result
    
    def getVpnSslConfig(self):
        result = []
        snmpAgent = snmputils.SnmpAgent(None, self._client)
        queryBuilder = snmputils.SnmpQueryBuilder('1.3.6.1.4.1.12356.101.12.2.4.1')
        queryBuilder.addQueryElement(1, 'Index') 
        queryBuilder.addQueryElement(2, 'VDom') 
        queryBuilder.addQueryElement(3, 'User')
        queryBuilder.addQueryElement(4, 'Src_IP')
        queryBuilder.addQueryElement(5, 'Tunel_IP')
        try:
            result = snmpAgent.getSnmpData(queryBuilder)
        except:
            logger.warn('Failed getting VPN SSL config')

        return result
    
    def getWebCacheConfig(self):
        result = []
        snmpAgent = snmputils.SnmpAgent(None, self._client)
        queryBuilder = snmputils.SnmpQueryBuilder('1.3.6.1.4.1.12356.101.10.113.1')
        queryBuilder.addQueryElement(1, 'RAM_Limit') 
        queryBuilder.addQueryElement(2, 'RAM_Usage') 
        queryBuilder.addQueryElement(3, 'RAM_Hits')
        queryBuilder.addQueryElement(4, 'RAM_Misses')
        queryBuilder.addQueryElement(5, 'Requests')
        queryBuilder.addQueryElement(6, 'Bypass')
        try:
            result = snmpAgent.getSnmpData(queryBuilder)
        except:
            logger.warn('Failed getting Cache config')
        return result
    
    def getProxyConfig(self):
        result = []
        snmpAgent = snmputils.SnmpAgent(None, self._client)
        queryBuilder = snmputils.SnmpQueryBuilder('1.3.6.1.4.1.12356.101.10.112.5.1')
        queryBuilder.addQueryElement(1, 'Blocked_DLP') 
        queryBuilder.addQueryElement(2, 'Blocked_Conn_Type') 
        queryBuilder.addQueryElement(3, 'Examined_URLs')
        queryBuilder.addQueryElement(4, 'Allowed_URLs')
        queryBuilder.addQueryElement(5, 'Blocked_URLs')
        queryBuilder.addQueryElement(6, 'Logged_URLs')
        queryBuilder.addQueryElement(7, 'Overriden_URLs')
        try:
            result = snmpAgent.getSnmpData(queryBuilder)
        except:
            logger.warn('Failed getting Proxy config')

        return result
    
    def discover(self):
        config = firewall.FirewallConfig('Firewall configuration')
        try:
            config.type_to_rules_dict['Firewall'] = self.getFirewallConfig()
        except:
            logger.debugException('Failed to get config part')
        
        try:
            config.type_to_rules_dict['Antivirus'] = self.getAntivirusConfig()
        except:
            logger.debugException('Failed to get config part')
        
        try:
            config.type_to_rules_dict['VPN SSL'] = self.getVpnSslConfig()
        except:
            logger.debugException('Failed to get config part')
        
        try:
            config.type_to_rules_dict['Web Cache'] = self.getWebCacheConfig()
        except:
            logger.debugException('Failed to get config part')
        
        try:
            config.type_to_rules_dict['Proxy'] = self.getProxyConfig()
        except:
            logger.debugException('Failed to get config part')
        return config


def getDiscoverer(vendor, client, cmdType='SNMP'):
    if vendor.lower().find('juniper') != -1:
        if cmdType == 'Shell':
            return JuniperFirewallDiscoverByShell(client)
        else:
            return JuniperFirewallDiscoverer(client)
    if vendor.lower().find('forti') != -1:
        return FortigateFirewallDiscoverer(client)