import logger
import modeling
import errormessages
from ivm import TopologyReporter
from ivm_discoverer import isIvmGuest, isIvmSystem, VirtualServerDiscoverer, IvmHypervisorDiscoverer, isIvmVpar
from shellutils import ShellUtils

from appilog.common.system.types.vectors import ObjectStateHolderVector
from java.lang import Exception as JavaException

##########################################################
## Function to discover IVM topology
##########################################################
def discoverIvmTopology(shell, hostId, reportHostName):
    vector = ObjectStateHolderVector()
    hypervisor = IvmHypervisorDiscoverer(shell).discover()
    virtual_servers = VirtualServerDiscoverer(shell).discover()
    
    ivmHostOsh = modeling.createOshByCmdbId("unix", hostId)
    vector.add(ivmHostOsh)
    
    vector.addAll(TopologyReporter().report(ivmHostOsh, hypervisor, virtual_servers, reportHostName))
    
    return vector

##########################################################
## Main function block
##########################################################
def DiscoveryMain(Framework):
    protocol = Framework.getDestinationAttribute('Protocol')
    protocolName = errormessages.protocolNames.get(protocol) or protocol
    hostId = Framework.getDestinationAttribute('hostId')
    reportHostName = Framework.getParameter('reportHostNameAsVmName')
    
    vector = ObjectStateHolderVector()
    try:
        client = Framework.createClient()
        try:
            shell = ShellUtils(client)

            if isIvmSystem(shell):
                if isIvmGuest(shell):
                    Framework.reportWarning("Can not get VM info: running inside HPVM guest.")
                    return vector
                if isIvmVpar(shell):
                    Framework.reportWarning("Can not get VM info: running inside HPVM vPar.")
                    return vector
                vector.addAll(discoverIvmTopology(shell, hostId, reportHostName))
            else:
                Framework.reportWarning("The destination host is not a part of HP IVM system")
        finally:
            client.close()
    except JavaException, ex:
        strException = ex.getMessage()
        logger.debugException('')
        errormessages.resolveAndReport(strException, protocolName, Framework)
    except Exception, ex:
        logger.debugException('')
        errormessages.resolveAndReport(str(ex), protocolName, Framework)

    return vector