# coding=utf-8
import logger
import modeling
import scp
import re
from urlparse import urlparse
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector


def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()

    # # Write implementation to return new result CIs here...
    ipList = Framework.getTriggerCIDataAsList('PHYSICAL_IP_ADDRESS')
    portList = Framework.getTriggerCIDataAsList('PHYSICAL_PORT')
    service_context = Framework.getDestinationAttribute('SERVICE_CONTEXT')
    service_type = Framework.getDestinationAttribute('SERVICE_TYPE')
    application_resource_id = Framework.getDestinationAttribute('APPLICATION_RESOURCE_ID')
    application_resource_class = Framework.getDestinationAttribute('APPLICATION_RESOURCE_CLASS')
    junction_id = Framework.getDestinationAttribute('JUNCTION_ID')
    junction_root_class = Framework.getDestinationAttribute('JUNCTION_CLASS')
    junction_name = Framework.getDestinationAttribute('JUNCTION_NAME')
    virtual_host_name = Framework.getDestinationAttribute('VIRTUAL_HOST_NAME')
    virtual_junction_root_class = Framework.getDestinationAttribute('VIRTUAL_JUNCTION_CLASS')    
    virtual_junction_id = Framework.getDestinationAttribute('VIRTUAL_JUNCTION_ID')
    virtual_ipList = Framework.getTriggerCIDataAsList('VIRTUAL_JUNCTION_PHYSICAL_IP_ADDRESS')
    virtual_portList = Framework.getTriggerCIDataAsList('VIRTUAL_JUNCTION_PHYSICAL_PORT')   
    SCPId = Framework.getDestinationAttribute('id')
    
    junctionOsh = None
    bizOsh = scp.createOshById(application_resource_class, application_resource_id)
    sourceScpOsh = scp.createOshById('scp', SCPId)
    if junction_id:
        junctionOsh = modeling.createOshByCmdbIdString(junction_root_class, junction_id)   
        url = urlparse(service_context)
        if url:
            # get context root path from url
            path = url.path
            if re.match(junction_name.lower() + '(/.*)*/?$', path.lower()):
                #logger.info('Create one consumer-provider link between application and junction')
                #OSHVResult.addAll(scp.createCPLink(application_resource_id, application_resource_class, junction_id,
                #                                   junction_root_class, SCPId, service_context))
                logger.info('Create one ownership link between junction and scp')
                OSHVResult.addAll(scp.createOwnerShip(SCPId, junctionOsh))
                for index in range(len(ipList)):
                    #scpOsh = scp.createScpOsh(junctionOsh, service_type, ip=ipList[index], port=portList[index], context=service_context)
                    OSHVResult.addAll(scp.createScpOSHV(bizOsh, sourceScpOsh, service_type,
                                                        ipList[index], portList[index], service_context))
                    logger.info('Create scp with ip %s and port %s' % (ipList[index], portList[index]))
                    ipOsh = modeling.createIpOSH(ipList[index])
                    #OSHVResult.add(scpOsh)
                    OSHVResult.add(ipOsh)
                        
    if virtual_junction_id:
        junctionOsh = modeling.createOshByCmdbIdString(virtual_junction_root_class, virtual_junction_id)
        #logger.info('Create one consumer-provider link between application and virtual junction')
        #OSHVResult.addAll(scp.createCPLink(application_resource_id, application_resource_class, virtual_junction_id,
        #                                   virtual_junction_root_class, SCPId, service_context))
        logger.info('Create one ownership link between virtual junction and scp')
        OSHVResult.addAll(scp.createOwnerShip(SCPId, junctionOsh))
        for index in range(len(virtual_ipList)):
            #scpOsh = scp.createScpOsh(junctionOsh, service_type, ip=virtual_ipList[index], port=virtual_portList[index], context=service_context)
            OSHVResult.addAll(scp.createScpOSHV(bizOsh, sourceScpOsh, service_type,
                                                ipList[index], portList[index], service_context))
            logger.info('Create scp with ip %s and port %s' % (virtual_ipList[index], virtual_portList[index]))
            ipOsh = modeling.createIpOSH(virtual_ipList[index])
            #OSHVResult.add(scpOsh)
            OSHVResult.add(ipOsh)      

    return OSHVResult