#coding=utf-8
import modeling
from docker_reporter import DockerReporter

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector


class DockerSwarmReporter(DockerReporter):
    def __init__(self, client, Framework, swarmDaemon, dockerImages, dockerContainers, dockerVolumes):
        DockerReporter.__init__(self, client, Framework, swarmDaemon, dockerImages, dockerContainers, dockerVolumes)
        self.swarmDaemon = swarmDaemon


    def reportSwarm(self, vector, swarmDaemon):
        # Node
        nodeOSH = modeling.createHostOSH(swarmDaemon.dockerNodeObj.ip)
        swarmDaemon.dockerNodeObj.setOsh(nodeOSH)
        swarmDaemonOSH = ObjectStateHolder('docker_swarm_daemon')
        swarmDaemonOSH.setAttribute('name', swarmDaemon.getName())
        swarmDaemonOSH.setAttribute('discovered_product_name', swarmDaemon.discovered_product_name)
        swarmDaemonOSH.setAttribute('version', swarmDaemon.version)
        if swarmDaemon.usedHttpProtocol:
            swarmDaemonOSH.setAttribute('credentials_id', swarmDaemon.usedHttpProtocol)
        swarmDaemon.setOsh(swarmDaemonOSH)
        swarmClusterOSH = ObjectStateHolder('docker_swarm_cluster')
        swarmClusterOSH.setAttribute('name', swarmDaemon.dockerSwarmClusterObj.getName())
        swarmDaemon.dockerSwarmClusterObj.setOsh(swarmClusterOSH)
        uriEndpointOSH = modeling.createOshByCmdbIdString('uri_endpoint', swarmDaemon.uriId)

        vector.add(swarmDaemonOSH)
        vector.add(swarmClusterOSH)
        vector.add(uriEndpointOSH)

        swarmDaemonOSH.setContainer(nodeOSH)
        swarmDaemonClusterLink = modeling.createLinkOSH('manage', swarmDaemonOSH, swarmClusterOSH)
        swarmDaemonUriLink = modeling.createLinkOSH('usage', swarmDaemonOSH, uriEndpointOSH)
        vector.add(swarmDaemonClusterLink)
        vector.add(swarmDaemonUriLink)

        for (dockerDaemonEndpoint, dockerDaemonObj) in swarmDaemon.dockerSwarmClusterObj.dockerDaemonObjs.items():
            self.reportDocker(vector, dockerDaemonObj)

            swarmDockerDaemonLink = modeling.createLinkOSH('membership', swarmClusterOSH, dockerDaemonObj.getOsh())
            vector.add(swarmDockerDaemonLink)



    def reportDockerTopology(self):
        vector = ObjectStateHolderVector()
        self.reportSwarm(vector, self.swarmDaemon)
        self.reportDockerImage(vector, self.Images)
        self.reportDockerVolume(vector, self.Volumes)
        self.reportDockerContaienr(vector, self.Containers)

        return vector
