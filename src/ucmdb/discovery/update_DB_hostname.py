#coding=utf-8
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

    db_id_list = Framework.getTriggerCIDataAsList('dbId')    #有可能一个主机多个数据库
    hostname = Framework.getDestinationAttribute('hostname').upper()
    db_hostname = Framework.getTriggerCIData('db_hostname')

    logger.debug('db_id_list', db_id_list)
    logger.debug('hostname ==', hostname)
    logger.debug('db_hostname ==', db_hostname)

    dbnameList=[]

    if db_hostname and db_hostname != 'NA':
        for name in db_hostname.split(','):
            logger.debug('name=',name)
            dbnameList.append(name)
            logger.debug('dbnameList=', dbnameList)

        if hostname not in db_hostname:
            dbnameList.append(hostname)
    else:
        dbnameList.append(hostname)

    logger.debug('dbnameList before sort==', dbnameList)
    dbnameList_new = unique(dbnameList)
    
    dbnameList_new.sort()
    logger.debug('dbnameList after sort==', dbnameList_new)
    db_hostname=','.join(dbnameList_new)
    logger.debug('database hosts information revised==', db_hostname)

    # create db instance by dbid

    for db_id in db_id_list:
        dbOSH = modeling.createOshByCmdbIdString('Z_DB', db_id)
        dbOSH.setAttribute('z_hostname',db_hostname)
        OSHVResult.add(dbOSH)

    #return OSHVResult
    Framework.sendObjects(OSHVResult)
    Framework.flushObjects()