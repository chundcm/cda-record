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
    
    # 有可能一个ip 对应多个服务器
    svr_ids = Framework.getTriggerCIDataAsList('svrId')
    switch_names = Framework.getTriggerCIDataAsList('SwitchName')
    interface_names = Framework.getTriggerCIDataAsList('InterfaceName')

    logger.debug('###############')
    logger.debug('svr_ids == ', svr_ids)
    logger.debug('switch_names length == ', len(switch_names))
    logger.debug('interface_names length == ', len(interface_names))
    logger.debug('switch_names == ', switch_names)
    logger.debug('interface_names == ', interface_names)
    logger.debug('###############')

    # switch name 唯一性检查 
    switch_names_unique = unique(switch_names)
    interface_names_unique = unique(interface_names)
    
    #对list进行排序，防止重复重复更新字段    
    switch_names_unique.sort()
    interface_names_unique.sort()
        
    switch_name_info = ''
    interface_name_info = ''    
    #switch_name_info = getAttributeInfo(switch_names, switch_name_info)
    #interface_name_info = getAttributeInfo(interface_names, interface_name_info)
    
    switch_name_info = ','.join(switch_names_unique)
    interface_name_info = ','.join(interface_names_unique)    
    logger.debug('interface_name_info=', interface_name_info)
    logger.debug('switch_name_info=', switch_name_info)

    for svr_id in svr_ids:
        svrOSH = ObjectStateHolder('Z_SVR')
        svrOSH.setStringAttribute('global_id', svr_id)
        svrOSH.setStringAttribute("z_sanswitch", switch_name_info)
        svrOSH.setStringAttribute('z_sanswitchport', interface_name_info)
    
        OSHVResult.add(svrOSH)

    #return OSHVResult
    Framework.sendObjects(OSHVResult)
    Framework.flushObjects()