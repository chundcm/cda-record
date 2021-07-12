# coding=utf-8
import string
import re
import time
import datetime

import logger
import modeling

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector


def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    VLSNameList = Framework.getTriggerCIDataAsList('VLSNameList')
    vls_ids = Framework.getTriggerCIDataAsList('VLSIds')
    lastAccessLinkTime = Framework.getTriggerCIDataAsList('lastAccessLinkTime')
    linkNameList = Framework.getTriggerCIDataAsList('linkNameList')
    host_class = Framework.getDestinationAttribute('host_class')
    vm_id = Framework.getDestinationAttribute('vm_id')

    lastAccessLinkTime = [long(x) for x in lastAccessLinkTime]
    linkNameList = [str(x) for x in linkNameList]
    VLSNameList = [str(x) for x in VLSNameList]
    vls_ids = [str(x) for x in vls_ids]
    logger.debug('VLSNameList: %s' % VLSNameList)
    logger.debug('vls_ids: %s' % vls_ids)
    logger.debug('lastAccessLinkTime: %s' % lastAccessLinkTime)
    logger.debug('linkNameList: %s' % linkNameList)

    minTime = long(min(lastAccessLinkTime))
    maxTime = long(max(lastAccessLinkTime))
    logger.debug('minTime: %s' % minTime)
    logger.debug('maxTime: %s' % maxTime)

    linkName_time = dict(zip(linkNameList, lastAccessLinkTime))
    logger.debug('linkName_time: %s' % linkName_time)

    VLSNmae_Id = dict(zip(VLSNameList, vls_ids))
    logger.debug('VLSNmae_Id: %s' % VLSNmae_Id)

    vm_osh = modeling.createOshByCmdbIdString(host_class, vm_id)
    for linkName, linkTime in linkName_time.items():
        if linkTime != maxTime:
            # find VLS id based on the name
            vlsID = VLSNmae_Id.get(linkName)
            logger.debug('Trying to delete link between vm (%s) with linkName (%s)' % (vm_id, linkName))
            logger.debug('vlsID is %s' % vlsID)

            if not linkName or linkName == 'NA':
                logger.warn('The link missing Related VLS name, try to rerun vCenter/ESX discovery by VIM job')
                return ObjectStateHolderVector()

            vls_osh = modeling.createOshByCmdbIdString('virtualization_layer', vlsID)
            link_osh = modeling.createLinkOSH('execution_environment', vls_osh, vm_osh)
            OSHVResult.add(link_osh)

    if OSHVResult.size() > 0:
        Framework.deleteObjects(OSHVResult)
        Framework.flushObjects()
    return ObjectStateHolderVector()
