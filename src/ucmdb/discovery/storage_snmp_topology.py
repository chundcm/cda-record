# coding=utf-8
import logger
import errormessages
import sys

import modeling
from snmp_model_finder import SnmpQueryHelper
from storage_snmp_data_domain import DataDomainDiscover
import emc_isilon_discoverer

from appilog.common.system.types.vectors import ObjectStateHolderVector
from com.hp.ucmdb.discovery.library.clients import ClientsConsts


def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    ip_address = Framework.getDestinationAttribute('ip_address')

    try:
        client = Framework.createClient()
        discoverClass = None
    except:
        errMsg = 'Exception while creating %s client: %s' % (ClientsConsts.SNMP_PROTOCOL_NAME, sys.exc_info()[1])
        errormessages.resolveAndReport(str(sys.exc_info()[1]), ClientsConsts.SNMP_PROTOCOL_NAME, Framework)
        logger.debugException(errMsg)
    else:
        if isEMCDataDomain(client):
            discoverClass = DataDomainDiscover(client)
        host_osh = modeling.createHostOSH(ip_address)

        if discoverClass:
            OSHVResult.add(host_osh)
            discoverClass.discoverer()
            OSHVResult.addAll(discoverClass.reporter(host_osh))

        if isEMCIsilon(client):
            OSHVResult.addAll(emc_isilon_discoverer.discoverIsilon(client))

    return OSHVResult


def isEMCDataDomain(client):
    snmpQueryHelper = SnmpQueryHelper(client)
    resultSet = snmpQueryHelper.snmpGet(".1.3.6.1.4.1.19746.1.1.1.1.1.1.1.1.1")
    return resultSet

def isEMCIsilon(client):
    snmpQueryHelper = SnmpQueryHelper(client)
    resultSet = snmpQueryHelper.snmpGet(".1.3.6.1.4.1.12124.1.1.1")
    return resultSet
