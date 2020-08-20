#coding=utf-8
import logger
import errormessages
import modeling
from scvmm_hyperv_discoverer import HyperVDiscoverer
from shellutils import ShellUtils

from java.lang import Exception as JException
from appilog.common.system.types.vectors import ObjectStateHolderVector
from java.util import Properties
from com.hp.ucmdb.discovery.common import CollectorsConstants


def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    protocol = Framework.getDestinationAttribute('Protocol')
    credentialsId = Framework.getTriggerCIData('credentialsId')
    ip = Framework.getTriggerCIData('ipAddress')

    hostId = Framework.getTriggerCIData('hostId')
    scvmmHostOSH = modeling.createOshByCmdbIdString("node", hostId)
    scvmmId = Framework.getTriggerCIData("id")
    scvmmOSH = modeling.createOshByCmdbIdString("running_software", scvmmId)
    scvmmOSH.setContainer(scvmmHostOSH)
    OSHVResult.add(scvmmHostOSH)
    OSHVResult.add(scvmmOSH)

    try:
        shell = None
        try:
            props = Properties()
            props.setProperty(CollectorsConstants.DESTINATION_DATA_IP_ADDRESS, ip)
            props.setProperty(CollectorsConstants.ATTR_CREDENTIALS_ID, credentialsId)
            client = Framework.createClient(props)
            shell = ShellUtils(client)

            hypervDiscoverer = HyperVDiscoverer(Framework, shell, scvmmOSH, scvmmHostOSH)
            OSHVResult.addAll(hypervDiscoverer.discover())

        finally:
            if shell is not None:
                shell.closeClient()
    except JException, ex:
        exInfo = ex.getMessage()
        errormessages.resolveAndReport(exInfo, protocol, Framework)
    except:
        exInfo = logger.prepareJythonStackTrace('')
        errormessages.resolveAndReport(exInfo, protocol, Framework)

    return OSHVResult
