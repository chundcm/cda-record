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

    host_id = Framework.getDestinationAttribute('hostId')
    cpu_name = Framework.getDestinationAttribute('CpuName')
    cpus = Framework.getTriggerCIDataAsList('CpuName')

    cpuInfo = str(len(cpus))+'*'+cpu_name

    logger.debug('cpu information==', cpuInfo)

    hostOSH = ObjectStateHolder('Z_SVR')
    hostOSH.setStringAttribute("global_id", host_id)
    hostOSH.setStringAttribute('z_cpu',cpuInfo )

    OSHVResult.add(hostOSH)
    ## Write implementation to return new result CIs here...

    return OSHVResult