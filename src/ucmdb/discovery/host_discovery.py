from asm_error_filter import ASMErrorFilter
import logger
import modeling
import scp

import host_connection
import host_application
import deep_discovery
import next_hop

from appilog.common.system.types.vectors import ObjectStateHolderVector


def reportIpAddress(Framework, OSHVResult):
    ipAddress = Framework.getDestinationAttribute('ip_address')
    if ipAddress:
        logger.debug("reporting ip address:", ipAddress)
        ipOSH = modeling.createIpOSH(ipAddress)
        OSHVResult.add(ipOSH)

    return ipAddress


def DiscoveryMain(Framework):
    # get attribute
    shell = None
    client = None
    scp_id = Framework.getDestinationAttribute("SCP_ID")
    scp_discovery_status = Framework.getDestinationAttribute('SCP_discovery_status')
    scpOsh = scp.createOshById('scp', scp_id)
    OSHVResult = ObjectStateHolderVector()

    try:
        ip = reportIpAddress(Framework, OSHVResult)

        # do host connection
        client, shell, warningsList, errorsList, hostOsh = host_connection.doConnection(Framework, ip, OSHVResult)

        if not (client and shell):
            logger.debug("host connection failed")
            for errobj in warningsList:
                logger.reportWarningObject(errobj)
            reportErrors(Framework, shell, client, errorsList)
            if str(scp_discovery_status) != str(scp.DISCOVERY_STATUS_MANUALLY_ADDED):
                scpOsh.setEnumAttribute('discovery_status', scp.DISCOVERY_STATUS_DEACTIVE)
            OSHVResult.add(scpOsh)
            return OSHVResult

        # do host application
        applicationResults, processes, connectivityEndPoints, connections, errorsList = host_application.doApplication(
            Framework, ip,
            OSHVResult,
            client, shell, hostOsh)

        if not applicationResults:
            logger.debug("host application failed")
            reportErrors(Framework, shell, client, errorsList)
            if str(scp_discovery_status) != str(scp.DISCOVERY_STATUS_MANUALLY_ADDED):
                scpOsh.setEnumAttribute('discovery_status', scp.DISCOVERY_STATUS_DEACTIVE)
            OSHVResult.add(scpOsh)
            return OSHVResult

        #if the status is manully added, skip it
        if str(scp_discovery_status) != str(scp.DISCOVERY_STATUS_MANUALLY_ADDED):
            scpOsh.setEnumAttribute('discovery_status', scp.DISCOVERY_STATUS_ACTIVE)

        OSHVResult.add(scpOsh)
        #do deep discovery
        deep_discovery.do_deep_discovery(Framework, ip, applicationResults, OSHVResult, client,
                                                                shell, hostOsh)

        # do next hop
        next_hop.doNextHop(Framework, ip, OSHVResult, shell, applicationResults,
                           processes, connectivityEndPoints, connections, hostOsh)

    finally:
        #close connection
        if shell:
            try:
                shell.closeClient()
            except:
                logger.warnException('Client was not closed properly')
                # close client anyway
        if client and client.close():
            pass

    return OSHVResult


def reportErrors(Framework, shell, client, errorsList):
    ASMErrorFilter.filterErrors(Framework, shell, client, errorsList)
    for errobj in errorsList:
        logger.reportErrorObject(errobj)
