# coding=utf-8
import logger
import scp
import modeling

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector

ATTR_MAP = {
    'CLIENT_APPLICATION_IP': 'application_ip',
    'CLIENT_APPLICATION_DOMAIN': 'application_ip_domain',
    'CLIENT_APPLICATION_PATH': 'application_path',
    'CLIENT_APPLICATION_USER_NAME': 'application_username',
    'CLIENT_APPLICATION_VERSION_DESCRIPTION': 'application_version',
    'CLIENT_APPLICATION_NOTE': 'data_note',
    'CLIENT_APPLICATION_DESCRIPTION': 'description',
    'CLIENT_APPLICATION_PRODUCT_NAME': 'discovered_product_name',
    'CLIENT_APPLICATION_DISPLAY_LABEL': 'display_label',
    'CLIENT_APPLICATION_NAME': 'name',
    'CLIENT_APPLICATION_CONTAINER_NAME': 'root_container_name',
    'CLIENT_APPLICATION_EDITION': 'software_edition',
    'CLIENT_APPLICATION_USER_LABEL': 'user_label',
    'CLIENT_APPLICATION_VENDOR': 'vendor',
    'CLIENT_APPLICATION_VERSION': 'version',
}

REFERENCES = ['CONNECTION_TYPE', 'SERVER_HOST', 'SERVER_IP', 'SERVER_PORT', 'CONTEXT']


def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()

    serverId = Framework.getDestinationAttribute('SERVER_ID')
    serverClass = Framework.getDestinationAttribute('SERVER_CLASS')
    serverOsh = createOsh(serverClass, serverId)

    serverClusterId = Framework.getDestinationAttribute('SERVER_CLUSTER_ID')
    serverClusterOsh = createOsh('cluster', serverClusterId)
    serverJunctionId = Framework.getDestinationAttribute('SERVER_JUNC_ID')
    serverJunctionOsh = createOsh('isam_junction', serverJunctionId)

    businessElementId = Framework.getDestinationAttribute('BUSINESS_ELEMENT_ID')
    businessElementType = Framework.getDestinationAttribute('BUSINESS_ELEMENT_TYPE')
    if businessElementId and businessElementType:
        businessElementOsh = ObjectStateHolder(businessElementType, businessElementId)
        OSHVResult.add(businessElementOsh)
        createLinksForBusinessElement(businessElementOsh, serverOsh, OSHVResult)
        createLinksForBusinessElement(businessElementOsh, serverClusterOsh, OSHVResult)
        createLinksForBusinessElement(businessElementOsh, serverJunctionOsh, OSHVResult)

    clientIdList = Framework.getTriggerCIDataAsList('CLIENT_ID')
    clientClassList = Framework.getTriggerCIDataAsList('CLIENT_CLASS')
    clientOshList = [createOsh(cmdbClass, cmdbId)
                     for cmdbClass, cmdbId in zip(clientClassList, clientIdList)]
    for osh in clientOshList:  # only for running_software
        buildClientOsh(Framework, osh)

    clientClusterIdList = Framework.getTriggerCIDataAsList('CLIENT_CLUSTER_ID')
    clientClusterClassList = ['cluster'] * len(clientClusterIdList)
    clientClusterOshList = [createOsh(cmdbClass, cmdbId)
                            for cmdbClass, cmdbId in zip(clientClusterClassList, clientClusterIdList)]

    clientJunctionIdList = Framework.getTriggerCIDataAsList('CLIENT_JUNC_ID')
    clientJunctionClassList = ['isam_junction'] * len(clientJunctionIdList)
    clientJunctionOshList = [createOsh(cmdbClass, cmdbId)
                            for cmdbClass, cmdbId in zip(clientJunctionClassList, clientJunctionIdList)]

    reference = buildReferenceString(Framework)
    createLinks(OSHVResult, clientOshList, clientIdList, serverId, serverOsh, reference)
    createLinks(OSHVResult, clientClusterOshList, clientClusterIdList, serverId, serverOsh, reference)
    createLinks(OSHVResult, clientJunctionOshList, clientJunctionIdList, serverId, serverOsh, reference)
    createLinks(OSHVResult, clientOshList, clientIdList, serverClusterId, serverClusterOsh, reference)
    createLinks(OSHVResult, clientClusterOshList, clientClusterIdList, serverClusterId, serverClusterOsh, reference)
    createLinks(OSHVResult, clientJunctionOshList, clientJunctionIdList, serverClusterId, serverClusterOsh, reference)
    createLinks(OSHVResult, clientOshList, clientIdList, serverJunctionId, serverJunctionOsh, reference)
    createLinks(OSHVResult, clientClusterOshList, clientClusterIdList, serverJunctionId, serverJunctionOsh, reference)
    createLinks(OSHVResult, clientJunctionOshList, clientJunctionIdList, serverJunctionId, serverJunctionOsh, reference)

    return OSHVResult


def createLinks(OSHVResult, clientOshList, clientIdList, serverId, serverOsh, reference):
    if serverOsh and serverId:
        processedIds = set()
        for index, clientId in enumerate(clientIdList):
            if clientId:
                if clientId in processedIds:
                    logger.debug('Ignore duplication link for id:', clientId)
                    continue
                processedIds.add(clientId)
                if clientId == serverId:
                    logger.debug('Ignore self link from id:', clientId)
                    continue
                logger.debug("creating cp-link for ci:", clientId)
                clientOsh = clientOshList[index]
                OSHVResult.add(clientOsh)
                OSHVResult.addAll(scp.createCPLinkByOsh(clientOsh, serverOsh, None, reference))
                logger.debug('Created C-P link between %s and %s' % (clientOsh, serverOsh))


def createLinksForBusinessElement(businessElementOsh, serverOsh, OSHVResult):
    if serverOsh:
        OSHVResult.add(serverOsh)
        containmentLink = modeling.createLinkOSH('containment', businessElementOsh, serverOsh)
        OSHVResult.add(containmentLink)
        cplinkOsh = modeling.createLinkOSH('consumer_provider', businessElementOsh, serverOsh)
        OSHVResult.add(cplinkOsh)
        logger.debug('Created C-P link between %s and %s' % (businessElementOsh, serverOsh))


def createOsh(cmdbClass, cmdbId):
    if cmdbClass and cmdbId:
        return ObjectStateHolder(cmdbClass, cmdbId)
    else:
        return None


def buildReferenceString(Framework):
    references = []
    for param in REFERENCES:
        value = Framework.getDestinationAttribute(param)
        if value:
            references.append('%s=%s' % (param.lower(), value))
    if references:
        return ', '.join(references)


def buildClientOsh(Framework, clientOsh):
    if clientOsh:
        for param, attr in ATTR_MAP.items():
            value = Framework.getDestinationAttribute(param)
            if value:
                clientOsh.setAttribute(attr, value)
