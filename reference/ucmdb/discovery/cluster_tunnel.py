# coding=utf-8
import string
import re

import logger
import modeling
import scp

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector


def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()

    # # Write implementation to return new result CIs here...
    ipList = Framework.getTriggerCIDataAsList('PHYSICAL_IP_ADDRESS')
    portList = Framework.getTriggerCIDataAsList('PHYSICAL_PORT')
    service_context = Framework.getDestinationAttribute('SERVICE_CONTEXT')
    service_type = Framework.getDestinationAttribute('SERVICE_TYPE')
    cluster_id = Framework.getDestinationAttribute('CLUSTER_ID')
    application_resource_id = Framework.getDestinationAttribute('APPLICATION_RESOURCE_ID')
    cluster_root_class = Framework.getDestinationAttribute('CLUSTER_CLASS')
    application_resource_class = Framework.getDestinationAttribute('APPLICATION_RESOURCE_CLASS')
    scp_id = Framework.getDestinationAttribute('SCP_ID')

    clusterOsh = modeling.createOshByCmdbIdString(cluster_root_class, cluster_id)
    bizOsh = scp.createOshById(application_resource_class, application_resource_id)
    scpOsh = scp.createOshById('scp', scp_id)
    OSHVResult.add(clusterOsh)
    OSHVResult.add(bizOsh)
    OSHVResult.add(scpOsh)

    OSHVResult.addAll(scp.createOwnerShip(scp_id, clusterOsh))
    
    for index in range(len(ipList)):
        OSHVResult.addAll(scp.createScpOSHV(bizOsh, scpOsh, service_type, ipList[index], portList[index], service_context))

    return OSHVResult