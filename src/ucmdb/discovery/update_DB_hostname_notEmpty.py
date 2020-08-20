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

def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()

    db_id = Framework.getDestinationAttribute('dbId')
    #hostnameList = Framework.getTriggerCIDataAsList('hostname')
    hostname = Framework.getDestinationAttribute('hostname').upper()
    #db_hostname = Framework.getDestinationAttribute('db_hostname')
    db_hostname = Framework.getTriggerCIData('db_hostname')


    logger.debug('hostname ==', hostname)
    logger.debug('db_hostname ==', db_hostname)

    dbnameList=[]

    if db_hostname:
        for name in db_hostname.split(','):
            logger.debug('name=',name)
            dbnameList.append(name)
            logger.debug('dbnameList=', dbnameList)

        if hostname not in db_hostname:
            dbnameList.append(hostname)
    else:
        dbnameList.append(hostname)

    logger.debug('dbnameList before sort==', dbnameList)
    dbnameList.sort()
    logger.debug('dbnameList after sort==', dbnameList)
    db_hostname=','.join(dbnameList)
    logger.debug('database hosts information revised==', db_hostname)

    dbOSH = modeling.createOshByCmdbIdString('Z_DB', db_id)
    dbOSH.setAttribute('z_hostname',db_hostname)

    OSHVResult.add(dbOSH)
    #return OSHVResult
    Framework.sendObjects(OSHVResult)
    Framework.flushObjects()
