# coding=utf-8
import kubernetes
import modeling
import netutils
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector


class Route(kubernetes.HasRepr):
    def __init__(self, id, name):
        kubernetes.HasRepr.__init__(self, name)
        self.setId(id)
        self.host = None
        self.namespace = None
        self.service = None
        self.target_port = 'any'

    def acceptVisitor(self, visitor):
        return visitor.visitRoute(self)


class ClusterNetwork(kubernetes.HasRepr):
    def __init__(self, id, name):
        kubernetes.HasRepr.__init__(self, name)
        self.setId(id)
        self.service_cidr = None
        self.network_cidr = None
        self.hostsubnet_length = None
        self.cluster_networks = []
        self.cluster = None

    def acceptVisitor(self, visitor):
        return visitor.visitClusterNetwok(self)


class DeploymentConfig(kubernetes.HasRepr):
    def __init__(self, id, name):
        kubernetes.HasRepr.__init__(self, name)
        self.setId(id)
        self.namespace = None
        self.replicas = None
        self.strategy = None
        self.pods = []

    def acceptVisitor(self, visitor):
        return visitor.visitDeploymentConfig(self)

class ClusterResourceQuota(kubernetes.HasRepr):
    def __init__(self,id ,name):
        kubernetes.HasRepr.__init__(self, name)
        self.setId(id)
        # object
        self.cluster = None

        self.specHardRequestCpu = None
        self.specHardRequestMemory = None
        self.specHardLimitCpu = None
        self.specHardLimitMemory = None
        self.statusHardRequestCpu = None
        self.statusHardRequestMemory = None
        self.statusHardLimitCpu = None
        self.statusHardLimitMemory = None
        self.statusUsedRequestCpu = None
        self.statusUsedRequestMemory = None
        self.statusUsedLimitCpu = None
        self.statusUsedLimitMemory = None
        self.specHardBlockStorage = None
        self.specHardGlusterDyn = None
        self.statusHardBlockStorage = None
        self.statusHardGlusterDyn = None
        self.statusUsedBlockStorage = None
        self.statusUsedGlusterDyn = None

    def acceptVisitor(self, visitor):
        return visitor.visitClusterResourceQuota(self)

class ClusterResourceQuotaConfig(kubernetes.HasRepr):
    def __init__(self, id, name):
        kubernetes.HasRepr.__init__(self, name)
        self.setId(id)
        self.namespace = None
        # object
        self.clusterQuota = None

        self.hardRequestCpu = None
        self.hardRequestMemory = None
        self.hardLimitCpu = None
        self.hardLimitMemory = None
        self.usedRequestCpu = None
        self.usedRequestMemory = None
        self.usedLimitCpu = None
        self.usedLimitMemory = None
        self.hardBlockStorage = None
        self.hardGlusterDyn = None
        self.usedBlockStorage = None
        self.usedGlusterDyn = None

    def acceptVisitor(self, visitor):
        return visitor.visitClusterResourceQuotaConfig(self)


class Builder(kubernetes.Builder):
    def visitRoute(self, route):
        osh = ObjectStateHolder('k8s_route')
        osh.setAttribute('uid', route.getId())
        osh.setAttribute('name', route.getName())
        osh.setAttribute('hostname', route.host)
        osh.setAttribute('target_port', str(route.target_port))
        return osh

    def visitClusterNetwok(self, cluster_network):
        osh = ObjectStateHolder('k8s_cluster_network')
        osh.setAttribute('uid', cluster_network.getId())
        osh.setAttribute('name', cluster_network.getName())
        return osh

    def visitDeploymentConfig(self, config):
        osh = ObjectStateHolder('k8s_deploy_config')
        osh.setAttribute('uid', config.getId())
        osh.setAttribute('name', config.getName())
        osh.setAttribute('replicas', config.replicas)
        osh.setAttribute('strategy', config.strategy)
        return osh

    def visitClusterResourceQuota(self,clusterresourcequota):
        osh = ObjectStateHolder('cluster_quota')
        osh.setAttribute('uid', clusterresourcequota.getId())
        osh.setAttribute('name', clusterresourcequota.getName())
        if clusterresourcequota.specHardRequestCpu:
            osh.setFloatAttribute('spec_hard_request_cpu', clusterresourcequota.specHardRequestCpu)
        if clusterresourcequota.specHardRequestMemory:
            osh.setIntegerAttribute('spec_hard_request_memory', clusterresourcequota.specHardRequestMemory)
        if clusterresourcequota.specHardLimitCpu:
            osh.setFloatAttribute('spec_hard_limit_cpu', clusterresourcequota.specHardLimitCpu)
        if clusterresourcequota.specHardLimitMemory:
            osh.setIntegerAttribute('spec_hard_limit_memory', clusterresourcequota.specHardLimitMemory)
        if clusterresourcequota.statusHardRequestCpu:
            osh.setFloatAttribute('status_hard_request_cpu', clusterresourcequota.statusHardRequestCpu)
        if clusterresourcequota.statusHardRequestMemory:
            osh.setIntegerAttribute('status_hard_request_memory', clusterresourcequota.statusHardRequestMemory)
        if clusterresourcequota.statusHardLimitCpu:
            osh.setFloatAttribute('status_hard_limit_cpu', clusterresourcequota.statusHardLimitCpu)
        if clusterresourcequota.statusHardLimitMemory:
            osh.setIntegerAttribute('status_hard_limit_memory', clusterresourcequota.statusHardLimitMemory)
        if clusterresourcequota.statusUsedRequestCpu:
            osh.setFloatAttribute('status_used_request_cpu', clusterresourcequota.statusUsedRequestCpu)
        if clusterresourcequota.statusUsedRequestMemory:
            osh.setIntegerAttribute('status_used_request_memory', clusterresourcequota.statusUsedRequestMemory)
        if clusterresourcequota.statusUsedLimitCpu:
            osh.setFloatAttribute('status_used_limit_cpu', clusterresourcequota.statusUsedLimitCpu)
        if clusterresourcequota.statusUsedLimitMemory:
            osh.setIntegerAttribute('status_used_limit_memory', clusterresourcequota.statusUsedLimitMemory)
        if clusterresourcequota.specHardBlockStorage:
            osh.setIntegerAttribute('spec_hard_blockstorage', clusterresourcequota.specHardBlockStorage)
        if clusterresourcequota.specHardGlusterDyn:
            osh.setIntegerAttribute('sepc_hard_gluster_dyn', clusterresourcequota.specHardGlusterDyn)
        if clusterresourcequota.statusHardBlockStorage:
            osh.setIntegerAttribute('status_hard_blockstorage', clusterresourcequota.statusHardBlockStorage)
        if clusterresourcequota.statusHardGlusterDyn:
            osh.setIntegerAttribute('status_hard_gluster_dyn', clusterresourcequota.statusHardGlusterDyn)
        if clusterresourcequota.statusUsedBlockStorage:
            osh.setIntegerAttribute('status_used_blockstorage', clusterresourcequota.statusUsedBlockStorage)
        if clusterresourcequota.statusUsedGlusterDyn:
            osh.setIntegerAttribute('status_used_gluster_dyn', clusterresourcequota.statusUsedGlusterDyn)
        return osh

    def visitClusterResourceQuotaConfig(self,clusterresourcequotaconfig):
        osh = ObjectStateHolder('cluster_quota_config')
        osh.setAttribute('uid', clusterresourcequotaconfig.getId())
        osh.setAttribute('name', clusterresourcequotaconfig.getName())
        if clusterresourcequotaconfig.hardRequestCpu:
            osh.setFloatAttribute('hard_request_cpu', clusterresourcequotaconfig.hardRequestCpu)
        if clusterresourcequotaconfig.hardRequestMemory:
            osh.setIntegerAttribute('hard_request_memory', clusterresourcequotaconfig.hardRequestMemory)
        if clusterresourcequotaconfig.hardLimitCpu:
            osh.setFloatAttribute('hard_limit_cpu', clusterresourcequotaconfig.hardLimitCpu)
        if clusterresourcequotaconfig.hardLimitMemory:
            osh.setIntegerAttribute('hard_limit_memory', clusterresourcequotaconfig.hardLimitMemory)
        if clusterresourcequotaconfig.usedRequestCpu:
            osh.setFloatAttribute('used_request_cpu', clusterresourcequotaconfig.usedRequestCpu)
        if clusterresourcequotaconfig.usedRequestMemory:
            osh.setIntegerAttribute('used_request_memory', clusterresourcequotaconfig.usedRequestMemory)
        if clusterresourcequotaconfig.usedLimitCpu:
            osh.setFloatAttribute('used_limit_cpu', clusterresourcequotaconfig.usedLimitCpu)
        if clusterresourcequotaconfig.usedLimitMemory:
            osh.setIntegerAttribute('used_limit_memory', clusterresourcequotaconfig.usedLimitMemory)
        if clusterresourcequotaconfig.hardBlockStorage:
            osh.setIntegerAttribute('hard_blockstorage',clusterresourcequotaconfig.hardBlockStorage)
        if clusterresourcequotaconfig.hardGlusterDyn:
            osh.setIntegerAttribute('hard_gluster_dyn', clusterresourcequotaconfig.hardGlusterDyn)
        if clusterresourcequotaconfig.usedBlockStorage:
            osh.setIntegerAttribute('used_blockstorage', clusterresourcequotaconfig.usedBlockStorage)
        if clusterresourcequotaconfig.usedGlusterDyn:
            osh.setIntegerAttribute('used_gluster_dyn', clusterresourcequotaconfig.usedGlusterDyn)
        return osh

class Reporter(kubernetes.Reporter):
    def __init__(self, builder):
        self.__builder = builder

    def reportRoute(self, route):
        if not route:
            raise ValueError("Route is not specified")
        vector = ObjectStateHolderVector()
        route_osh = route.build(self.__builder)
        vector.add(route_osh)
        if route.service:
            vector.add(modeling.createLinkOSH('dependency', route.service.getOsh(), route_osh))
        return vector

    def reportClusterNetwork(self, cluster_network):
        if not cluster_network:
            raise ValueError("Cluster network is not specified")
        vector = ObjectStateHolderVector()
        cluster_network_osh = cluster_network.build(self.__builder)
        vector.add(cluster_network_osh)
        vector.add(modeling.createLinkOSH('membership', cluster_network.cluster.getOsh(), cluster_network_osh))

        if cluster_network.service_cidr:
            service_subnet_osh = self.reportIpSubnet(cluster_network.service_cidr)
            service_subnet_osh.setAttribute('description', 'serviceNetwork')
            vector.add(service_subnet_osh)
            vector.add(modeling.createLinkOSH('membership', cluster_network_osh, service_subnet_osh))

        if cluster_network.network_cidr:
            network_subnet_osh = self.reportIpSubnet(cluster_network.network_cidr)
            network_subnet_osh.setAttribute('description', 'network')
            vector.add(network_subnet_osh)
            vector.add(modeling.createLinkOSH('membership', cluster_network_osh, network_subnet_osh))

        if cluster_network.cluster_networks:
            for cidr, hostsubnet_length in cluster_network.cluster_networks:
                subnet_osh = self.reportIpSubnet(cidr)
                subnet_osh.setAttribute('description', 'clusterNetwork')
                vector.add(subnet_osh)
                vector.add(modeling.createLinkOSH('membership', cluster_network_osh, subnet_osh))
        return vector

    def reportIpSubnet(self, cidr):
        ip, netmask = netutils.obtainDotDecimalTuple(cidr)
        return modeling.createNetworkOSH(ip, netmask)

    def reportDeploymentConfig(self, config):
        if not config:
            raise ValueError("Deployment Config is not specified")
        vector = ObjectStateHolderVector()
        config_osh = config.build(self.__builder)
        vector.add(config_osh)
        if config.namespace:
            vector.add(modeling.createLinkOSH('membership', config.namespace.getOsh(), config_osh))
        for pod in config.pods:
            vector.add(modeling.createLinkOSH('dependency', pod.getOsh(), config_osh))
        return vector

    def reportClusterResourceQuota(self, clusterresourcequota):
        if not clusterresourcequota:
            raise ValueError("Cluster Resource Quota is not specified")
        vector = ObjectStateHolderVector()
        clusterresourcequota_osh = clusterresourcequota.build(self.__builder)
        if clusterresourcequota.cluster:
            vector.add(modeling.createLinkOSH('membership', clusterresourcequota.cluster.getOsh(), clusterresourcequota_osh))
        vector.add(clusterresourcequota_osh)
        return vector

    def reportClusterResourceQuotaConfig(self, clusterresourcequotaconfig):
        if not clusterresourcequotaconfig:
            raise ValueError("Cluster Resource Quota Config is not specified")
        vector = ObjectStateHolderVector()
        clusterresourcequotaconfig_osh = clusterresourcequotaconfig.build(self.__builder)
        if clusterresourcequotaconfig.namespace:
            vector.add(modeling.createLinkOSH('composition', clusterresourcequotaconfig.namespace.getOsh(), clusterresourcequotaconfig_osh))
        if clusterresourcequotaconfig.clusterQuota:
            vector.add(modeling.createLinkOSH('composition', clusterresourcequotaconfig.clusterQuota.getOsh(),clusterresourcequotaconfig_osh))
        vector.add(clusterresourcequotaconfig_osh)
        return vector
