# coding=utf-8
import modeling
import logger
import ip_addr

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector

class FirewallBuilder:
    def build(self, firewall):
        if firewall is None: raise ValueError("Firewall is None!")

        firewallOsh = ObjectStateHolder('firewall')
        firewallOsh.setStringAttribute('name', firewall.getName())

        return firewallOsh


class VirtualFirewallBuilder:
    def build(self, vFirewall):
        if vFirewall is None: raise ValueError("Virtual Firewall is None!")

        vFirewallOsh = ObjectStateHolder('virtual_firewall')
        vFirewallOsh.setStringAttribute('name', vFirewall.getName())

        return vFirewallOsh


class NodeBuilder:
    def build(self, node):
        if node is None: raise ValueError("Node is None!")

        nodeOsh = ObjectStateHolder('node')
        nodeOsh.setStringAttribute('name', node.getName())

        if node.description:
            nodeOsh.setStringAttribute('description', node.description)

        return nodeOsh


class OSUserBuilder:
    def build(self, user):
        if user is None: raise ValueError("User is None!")

        userOsh = ObjectStateHolder('osuser')
        userOsh.setStringAttribute('name', user.getName())

        return userOsh


class InterfaceBuilder:
    def build(self, interface):
        if interface is None: raise ValueError("Interface is None!")

        interfaceOsh = ObjectStateHolder('interface')
        interfaceOsh.setStringAttribute('interface_name', interface.getName())

        if interface.description:
            interfaceOsh.setStringAttribute('description', interface.description)

        return interfaceOsh


class ChassisBuilder:
    def build(self, chassis):
        if chassis is None: raise ValueError("Chassis is None!")

        chassisOsh = ObjectStateHolder('chassis')
        chassisOsh.setStringAttribute('name', chassis.getName())

        return chassisOsh


class FirewallClusterBuilder:
    def build(self, firewallCluster):
        if firewallCluster is None: raise ValueError("Chassis Cluster is None!")

        chassisClusterOsh = ObjectStateHolder('firewall_cluster')
        chassisClusterOsh.setStringAttribute('name', firewallCluster.getName())

        return chassisClusterOsh


class ClusterResourceGroupBuilder:
    def build(self, chassisClusterGroup):
        if chassisClusterGroup is None: raise ValueError("Cluster Resource Group is None!")

        chassisClusterGroupOsh = ObjectStateHolder('cluster_resource_group')
        chassisClusterGroupOsh.setStringAttribute('name', chassisClusterGroup.getName())

        return chassisClusterGroupOsh


class RoutingInstanceBuilder:
    def build(self, routingInstance):
        if routingInstance is None: raise ValueError("Routing Instance is None!")

        routingInstanceOsh = ObjectStateHolder('routing_instance')
        routingInstanceOsh.setStringAttribute('name', routingInstance.getName())

        if routingInstance.type:
            routingInstanceOsh.setStringAttribute('type', routingInstance.type)

        return routingInstanceOsh


class FirewallReporter:
    def __init__(self, framework):
        self.framework = framework
        self.firewallBuilder = self._createFirewallBuilder()
        self.virtualFirewallBuilder = self._createVirtualFirewallBuilder()
        self.osUserBuilder = self._createOSUserBuilder()
        self.interfaceBuilder = self._createInterfaceBuilder()
        self.chassisBuilder = self._createChassisBuilder()
        self.firewallClusterBuilder = self._createFirewallClusterBuilder()
        self.clusterResourceGroupBuilder = self._createClusterResourceGroupBuilder()
        self.routingInstanceBuilder = self._createRoutingInstanceBuilder()

    def _createFirewallBuilder(self):
        return FirewallBuilder()

    def _createVirtualFirewallBuilder(self):
        return VirtualFirewallBuilder()

    def _createOSUserBuilder(self):
        return OSUserBuilder()

    def _createInterfaceBuilder(self):
        return InterfaceBuilder()

    def _createChassisBuilder(self):
        return ChassisBuilder()

    def _createFirewallClusterBuilder(self):
        return FirewallClusterBuilder()

    def _createClusterResourceGroupBuilder(self):
        return ClusterResourceGroupBuilder()

    def _createRoutingInstanceBuilder(self):
        return RoutingInstanceBuilder()

    def reportNode(self, vector, hostOsh, ipAddress):
        nodeOsh = modeling.createHostOSH(ipAddress)
        vector.add(modeling.createLinkOSH('dependency', hostOsh, nodeOsh))
        vector.add(nodeOsh)

    def reportUser(self, vector, hostOsh, user):
        userOsh = self.osUserBuilder.build(user)
        userOsh.setContainer(hostOsh)
        user.setOsh(userOsh)
        vector.add(userOsh)

    def reportInterface(self, vector, osh, interface, link):
        interfaceOsh = self.interfaceBuilder.build(interface)
        if link == 'composition':
            interfaceOsh.setContainer(osh)
        else:
            vector.add(modeling.createLinkOSH(link, osh, interfaceOsh))
        interface.setOsh(interfaceOsh)
        vector.add(interfaceOsh)
        return interfaceOsh

    def reportIpAddress(self, vector, osh, ipAddress, link):
        ipOsh = modeling.createIpOSH(ip_addr.IPAddress(ipAddress))
        if link == 'composition':
            ipOsh.setContainer(osh)
        else:
            vector.add(modeling.createLinkOSH(link, osh, ipOsh))
        vector.add(ipOsh)

    def reportChassis(self, vector, hostOsh, chassis):
        chassisOsh = self.chassisBuilder.build(chassis)
        vector.add(modeling.createLinkOSH('dependency', hostOsh, chassisOsh))
        chassis.setOsh(chassisOsh)
        vector.add(chassisOsh)
        return chassisOsh

    def reportVirtualFirewall(self, vector, hostOsh, virtualFirewall):
        virtualFirewallOsh = self.virtualFirewallBuilder.build(virtualFirewall)
        virtualFirewallOsh.setContainer(hostOsh)
        virtualFirewall.setOsh(virtualFirewallOsh)
        vector.add(virtualFirewallOsh)
        return virtualFirewallOsh

    def reportFirewallCluster(self, vector, firewallCluster):
        firewallClusterOsh = self.firewallClusterBuilder.build(firewallCluster)
        firewallCluster.setOsh(firewallClusterOsh)
        vector.add(firewallClusterOsh)
        return firewallClusterOsh

    def reportFirewall(self, vector, firewallClusterOsh, firewall):
        firewallOsh = self.firewallBuilder.build(firewall)
        vector.add(modeling.createLinkOSH('membership', firewallClusterOsh, firewallOsh))
        firewall.setOsh(firewallOsh)
        vector.add(firewallOsh)
        return firewallOsh

    def reportClusterResourceGroup(self, vector, firewallClusterOsh, clusterResourceGroup):
        clusterResourceGroupOsh = self.clusterResourceGroupBuilder.build(clusterResourceGroup)
        clusterResourceGroupOsh.setContainer(firewallClusterOsh)
        clusterResourceGroup.setOsh(clusterResourceGroupOsh)
        vector.add(clusterResourceGroupOsh)
        return clusterResourceGroupOsh

    def reportRoutingInstance(self, vector, hostOsh, routingInstance):
        routingInstanceOsh = self.routingInstanceBuilder.build(routingInstance)
        routingInstanceOsh.setContainer(hostOsh)
        routingInstance.setOsh(routingInstanceOsh)
        vector.add(routingInstanceOsh)
        return routingInstanceOsh

    def reportConfigurationDocument(self, vector, hostOsh, configDoc):
        configDocOsh = modeling.createConfigurationDocumentOSH('Firewall Configuration', '', configDoc.content)
        configDocOsh.setContainer(hostOsh)
        vector.add(configDocOsh)

    def reportLinkFromFirewallClusterToChassis(self, vector, chassisOsh, firewallClusterOsh):
        vector.add(modeling.createLinkOSH('dependency', firewallClusterOsh, chassisOsh))

    def report(self, chassisList, virtualFirewallList, nodeList, userList, interfaceList, routingInstanceList, configDoc, firewallCluster, hostOsh):
        vector = ObjectStateHolderVector()
        logger.debug("Start to report Firewall topology!")
        chassisOshList = []
        if configDoc is not None:
            self.reportConfigurationDocument(vector, hostOsh, configDoc)
        for ipAddress in nodeList:
            self.reportNode(vector, hostOsh, ipAddress)
        for user in userList:
            self.reportUser(vector, hostOsh, user)
        for interface in interfaceList:
            self.reportInterface(vector, hostOsh, interface, 'composition')
        for virtualFirewall in virtualFirewallList:
            virtualFirewallOsh = self.reportVirtualFirewall(vector, hostOsh, virtualFirewall)
            for interface in virtualFirewall.interfaceList:
                interfaceOsh = self.reportInterface(vector, virtualFirewallOsh, interface, 'composition')
                for ipAddress in interface.ipAddressList:
                    self.reportIpAddress(vector, interfaceOsh, ipAddress, 'containment')
        if chassisList:
            for chassis in chassisList:
                chassisOshList.append(self.reportChassis(vector, hostOsh, chassis))
        if firewallCluster is not None:
            firewallClusterOsh = self.reportFirewallCluster(vector, firewallCluster)
            for fw in firewallCluster.firewallList:
                fwOsh = self.reportFirewall(vector, firewallClusterOsh, fw)
                for ipAddress in fw.nodeList:
                    self.reportNode(vector, fwOsh, ipAddress)
            for clusterResourceGroup in firewallCluster.clusterResourceGroupList:
                clusterResourceGroupOsh = self.reportClusterResourceGroup(vector, firewallClusterOsh, clusterResourceGroup)
            if chassisOshList:
                for chassisOsh in chassisOshList:
                    self.reportLinkFromFirewallClusterToChassis(vector, chassisOsh, firewallClusterOsh)

        for routingInstance in routingInstanceList:
            routingInstanceOsh = self.reportRoutingInstance(vector, hostOsh, routingInstance)

        return vector