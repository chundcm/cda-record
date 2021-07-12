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

    #获取服务器global ip 和 lun instance
    svr_id = Framework.getDestinationAttribute('svrId')    
    stp_luns = Framework.getTriggerCIDataAsList('stpLun')    

    logger.debug('###############')
    logger.debug('svr_id == ', svr_id)
    logger.debug('stp_luns == ', stp_luns)    
    logger.debug('###############')
    
    stp_luns_info = ''
    stp_luns_unique = unique(stp_luns)    # 去掉重复项
    stp_luns_unique.sort()  # 排序
    logger.debug('stp_luns_unique after sorted ==', stp_luns_unique)

    stp_luns_info = ','.join(stp_luns_unique)
    logger.debug('stp_luns_info=', stp_luns_info)

    # 设置服务器对象属性
    svrOSH = ObjectStateHolder('Z_SVR')
    svrOSH.setStringAttribute('global_id', svr_id)
    svrOSH.setStringAttribute("z_lun", stp_luns_info)

    OSHVResult.add(svrOSH)

    # return OSHVResult
    Framework.sendObjects(OSHVResult)
    Framework.flushObjects()