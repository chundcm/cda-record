# coding=utf-8
      
import string
import re
import logger
import modeling

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector


def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()   
    svr_id = Framework.getTriggerCIData('svrId')
    pwwn = Framework.getTriggerCIDataAsList('pwwn')
    
    logger.debug('###############')
    logger.debug('svr_id== ',svr_id)
    logger.debug('Port wwn == ', pwwn)
    #logger.debug('pwwn type is: ',type(pwwn))
    logger.debug('Fibre Channel port wwn length == ', len(pwwn))
    logger.debug('###############')

    nodeOSH = modeling.createOshByCmdbId('Z_SVR', svr_id)
    #将pwwn进行排序
    wwn=sorted(pwwn)
    logger.debug('Port wwn == ', wwn)
    
    combined_wwn = ','.join(wwn)
    logger.debug('combined_wwn=', combined_wwn)
    nodeOSH.setStringAttribute('z_wwn', combined_wwn)
    
    OSHVResult.add(nodeOSH)
    logger.debug('z_wwn=', nodeOSH.getAttributeValue('z_wwn'))
    #logger.debug(nodeOSH.getAttributeAll())
    #logger.debug('len(z_wwn)=', len(nodeOSH.getAttributeValue('z_wwn')))
    
    return OSHVResult
    #Framework.sendObjects(OSHVResult)
    Framework.flushObjects()