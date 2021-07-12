# coding=utf-8
from modeling import HostBuilder, OshBuilder
import logger
import modeling
import netutils

from java.lang import Boolean
from appilog.common.utils import IPUtil
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector

from com.hp.ucmdb.discovery.library.scope import DomainScopeManager
from appilog.common.system.types import AttributeStateHolder
import re

def unique(old_list):
    newList = []
    for x in old_list:
        if x not in newList:
            newList.append(x.strip())
    return newList


def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    ## Write implementation to return new result CIs here...

    svr_id = Framework.getDestinationAttribute('svrId')
    sanzone_names = Framework.getTriggerCIDataAsList('sanZone')    

    logger.debug('###############')
    logger.debug('svr_id == ', svr_id)
    logger.debug('sanzone_names == ', sanzone_names)    
    logger.debug('###############')

    sanzone_name_info = ''        
    # sanzone_name_info = getAttributeInfo(sanzone_names, sanzone_name_info)

    #去掉重复项
    sanzone_names_unique = unique(sanzone_names)
    sanzone_names_unique.sort()   # 排序
    logger.debug('sanzone name list after sorted ==', sanzone_names_unique)
    # 已逗号拼接sanzone name 
    sanzone_name_info = ','.join(sanzone_names_unique)
    logger.debug('sanzone_name_info=', sanzone_name_info)   

    svrOSH = ObjectStateHolder('Z_SVR')
    svrOSH.setStringAttribute('global_id', svr_id)
    svrOSH.setStringAttribute("z_sanzone", sanzone_name_info)    

    OSHVResult.add(svrOSH)

    Framework.sendObjects(OSHVResult)
    Framework.flushObjects()