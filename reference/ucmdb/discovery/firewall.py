import modeling
import netutils
import logger
import re

from appilog.common.system.types.vectors import ObjectStateHolderVector

class _HasName:
    ''' Class that extends other classes with 'name' property '''
    def __init__(self):
        self.__name = None

    def setName(self, name):
        if not name: raise ValueError("Name is empty")
        self.__name = name

    def getName(self):
        return self.__name


class _HasOsh:
    ''' Class that extends other classes with ability to have OSH built from them '''
    def __init__(self):
        self.__osh = None

    def setOsh(self, osh):
        if osh is None: raise ValueError("OSH is None")
        self.__osh = osh

    def getOsh(self):
        return self.__osh


class Firewall(_HasName, _HasOsh):
    def __init__(self, name, nodeList = [], interfaceList = []):
        _HasName.__init__(self)
        _HasOsh.__init__(self)

        if not name:
            raise ValueError("Name is not specified, can not create Virtual Firewall instance!")
        self.setName(name)

        self.nodeList = nodeList
        self.interfaceList = interfaceList

    def __repr__(self):
        return "%s (name = %s)" % (self.__class__.__name__, self.getName())


class VirtualFirewall(_HasName, _HasOsh):
    def __init__(self, name, interfaceList = [], routingInstanceList = []):
        _HasName.__init__(self)
        _HasOsh.__init__(self)

        if not name:
            raise ValueError("Name is not specified, can not create Virtual Firewall instance!")
        self.setName(name)

        self.interfaceList = interfaceList
        self.routingInstanceList = routingInstanceList

    def __repr__(self):
        return "%s (name = %s)" % (self.__class__.__name__, self.getName())


class OsUser(_HasName, _HasOsh):
    def __init__(self, name):
        _HasName.__init__(self)
        _HasOsh.__init__(self)

        if not name:
            raise ValueError("Name is not specified, can not create Os User instance!")
        self.setName(name)

    def __repr__(self):
        return "%s (name = %s)" % (self.__class__.__name__, self.getName())


class Interface(_HasName, _HasOsh):
    def __init__(self, name, description = '', ipAddressList = []):
        _HasName.__init__(self)
        _HasOsh.__init__(self)

        if not name:
            raise ValueError("Name is not specified, can not create Node instance!")
        self.setName(name)
        self.description = description

        self.ipAddressList = ipAddressList

    def __repr__(self):
        return "%s (name = %s)" % (self.__class__.__name__, self.getName())


class Chassis(_HasName, _HasOsh):
    def __init__(self, name, chassisCluster = None):
        _HasName.__init__(self)
        _HasOsh.__init__(self)

        if not name:
            raise ValueError("Name is not specified, can not create Chassis instance!")
        self.setName(name)
        self.chassisCluster = chassisCluster

    def __repr__(self):
        return "%s (name = %s)" % (self.__class__.__name__, self.getName())


class FirewallCluster(_HasName, _HasOsh):
    def __init__(self, name, firewallList = [], clusterResourceGroupList = []):
        _HasName.__init__(self)
        _HasOsh.__init__(self)

        if not name:
            raise ValueError("Name is not specified, can not create Chassis Cluster instance!")
        self.setName(name)

        self.firewallList = firewallList
        self.clusterResourceGroupList = clusterResourceGroupList

    def __repr__(self):
        return "%s (name = %s)" % (self.__class__.__name__, self.getName())


class ClusterResourceGroup(_HasName, _HasOsh):
    def __init__(self, name, interfaceList = [], groupsNameList = []):
        _HasName.__init__(self)
        _HasOsh.__init__(self)

        if not name:
            raise ValueError("Name is not specified, can not create Cluster Resource Group instance!")
        self.setName(name)

        self.interfaceList = interfaceList
        self.groupsNameList = groupsNameList

    def __repr__(self):
        return "%s (name = %s)" % (self.__class__.__name__, self.getName())


class RoutingInstance(_HasName, _HasOsh):
    def __init__(self, name, type = '', interfaceList = []):
        _HasName.__init__(self)
        _HasOsh.__init__(self)

        if not name:
            raise ValueError("Name is not specified, can not create Routing Instance instance!")
        self.setName(name)
        self.type = type

        self.interfaceList = interfaceList

    def __repr__(self):
        return "%s (name = %s)" % (self.__class__.__name__, self.getName())


class ConfigurationDocument:
    def __init__(self, content):
        if not content:
            raise ValueError("Name is not specified, can not create Routing Instance instance!")
        self.content = content


class FirewallConfig:
    def __init__(self, name):
        self.name = name
        self.type_to_rules_dict = {}
        self.endpoints = []
        self.nated_networks = []
        
        
class Endpoint:
    def __init__(self):
        self.ip = None
        self.port = None
        self.type =  netutils.ProtocolType.TCP_PROTOCOL
        
class NatedNetwork:
    def __init__(self, ip = None, mask = None):
        self.ip = ip
        self.mask = mask
        
        
def buildFirewallConfig(config, container_osh):
    '''
    @param config: discovered firewall config
    @type config: instance of FirewallConfig
    @param container_osh: OSH allowed to be a container for configuration_document
    @type container_osh: OSH
    @return: configuretion_document OSH
    '''
    content = ''
    for key in config.type_to_rules_dict.keys():
        content += '%s\n' % key
 
        logger.debug('Building %s' % key)
        for obj in config.type_to_rules_dict.get(key):
            attrs = vars(obj)
            
            logger.debug(attrs)
            if attrs:
                content += '%s\n' % ('\n'.join(['%s = %s' % (key, value) for key, value in attrs.items()]))
    config_osh = modeling.createConfigurationDocumentOSH(name = config.name, path=config.name, content = content, containerOSH = container_osh )
    return config_osh

def buildEndpoints(endpoints, container_osh):
    vector = ObjectStateHolderVector()
    if not endpoints or not container_osh:
        return vector
    for endpoint in endpoints:
        endpoint_osh = modeling.createServiceAddressOsh(container_osh, endpoint.ip, endpoint.port, endpoint.type)
        vector.add(endpoint_osh)
    return vector

def reportNatedNetworks(networks, container_osh):
    vector = ObjectStateHolderVector()
    if not networks:
        return vector
    for network in networks:
        network_osh = modeling.createNetworkOSH(network.ip , network.mask)
        vector.add(network_osh)
        link_osh = modeling.createLinkOSH('route', container_osh, network_osh)
        vector.add(link_osh)
    return vector


def reportTopology(config, container_osh):
    vector = ObjectStateHolderVector()
    vector.add(container_osh)
    if config and container_osh:
        config_osh = buildFirewallConfig(config, container_osh)
        config_osh.setContainer(container_osh)
        vector.add(config_osh)
        
    if config and config.endpoints:
        vector.addAll(buildEndpoints(config.endpoints, container_osh))
    logger.debug('Networks %s' % config.nated_networks)    
    if config and config.nated_networks:
        vector.addAll(reportNatedNetworks(config.nated_networks, container_osh))
    return vector
        