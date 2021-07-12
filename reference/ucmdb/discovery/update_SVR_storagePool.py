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
    stp_names = Framework.getTriggerCIDataAsList('stpName')
    stp_lunsizes = Framework.getTriggerCIDataAsList('stpLunSize')
    stp_mdisks = Framework.getTriggerCIDataAsList('stpMdisk')

    logger.debug('###############')
    logger.debug('svr_id == ', svr_id)
    logger.debug('stp_names length == ', len(stp_names))
    logger.debug('stp_names == ', stp_names)
    logger.debug('stp_lunsizes length == ', len(stp_lunsizes))
    logger.debug('stp_lunsizes == ', stp_lunsizes)  
    logger.debug('stp_mdisks length == ', len(stp_mdisks))
    logger.debug('stp_mdisks == ', stp_mdisks)       
    logger.debug('###############')

    stp_name_info = ''
    stp_lunsize_info = ''
    stp_mdisk_info=''

    #去掉重复项
    stp_names_unique = unique(stp_names)
    stp_lunsizes_unique = unique(stp_lunsizes)
    stp_mdisks_unique = unique(stp_mdisks)

    #排序
    stp_names_unique.sort()
    stp_lunsizes_unique.sort()
    stp_mdisks_unique.sort()

    #已逗号连接实例名
    stp_name_info = ','.join(stp_names_unique)
    stp_lunsize_info = ','.join(stp_lunsizes_unique)
    stp_mdisk_info = ','.join(stp_mdisks_unique)

    logger.debug('stp_name_info=', stp_name_info)
    logger.debug('stp_lunsize_info=', stp_lunsize_info)
    logger.debug('stp_mdisk_info=', stp_mdisk_info)

    #设置更新属性值
    svrOSH = ObjectStateHolder('Z_SVR')
    svrOSH.setStringAttribute('global_id', svr_id)
    svrOSH.setStringAttribute("z_storagepool", stp_name_info)
    svrOSH.setStringAttribute("z_lun", stp_lunsize_info)
    svrOSH.setStringAttribute("z_storage", stp_mdisk_info)

    OSHVResult.add(svrOSH)

    #return OSHVResult
    Framework.sendObjects(OSHVResult)
    Framework.flushObjects()