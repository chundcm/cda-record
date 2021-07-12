#coding=utf-8
import logger
import modeling
import netutils
import errormessages
import errorobject
import errorcodes

import cim
import cim_discover
import sap_solman_discoverer_by_cim
import sap

from appilog.common.system.types.vectors import ObjectStateHolderVector
from java.lang import Exception as JException
from appilog.common.system.types import AttributeStateHolder, ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector, StringVector


def DiscoveryMain(Framework):
    warningsList = []
    errorsList = []
    vector = ObjectStateHolderVector()
    ip_address = Framework.getDestinationAttribute('ip_address')
    ip_domain = Framework.getDestinationAttribute('ip_domain')
    protocolName = cim.Protocol.DISPLAY

    credentials = netutils.getAvailableProtocols(Framework, cim.Protocol.FULL, ip_address, ip_domain)
    credentials = sap_solman_discoverer_by_cim.getSolManCredentials(credentials, Framework)
    if len(credentials) == 0:
        msg = errormessages.makeErrorMessage(protocolName, pattern=errormessages.ERROR_NO_CREDENTIALS)
        errobj = errorobject.createError(errorcodes.NO_CREDENTIALS_FOR_TRIGGERED_IP, [protocolName], msg)
        warningsList.append(errobj)
        logger.debug(msg)

    solManNamespaces = sap_solman_discoverer_by_cim.getSolManNamespaces(Framework)
    if not solManNamespaces:
        msg = errormessages.makeErrorMessage(protocolName, "No Solution Manager namespaces found")
        errobj = errorobject.createError(errorcodes.INTERNAL_ERROR_WITH_PROTOCOL_DETAILS, [cim.Protocol.DISPLAY, msg], msg)
        errorsList.append(errobj)
        logger.reportErrorObject(errobj)
        return vector

    for credential in credentials:
        testedNamespace = None
        hostOsh = modeling.createHostOSH(ip_address)
        for namespaceObject in solManNamespaces:
            namespace = namespaceObject.getName()
            try:
                testedNamespace = cim_discover.testConnectionWithNamespace(Framework, ip_address, credential, namespaceObject)
                if namespace == "customernetworkadministration" and testedNamespace is not None:
                    reporter = sap.Reporter(sap.Builder())
                    frunOsh = reporter.reportFrunInstance(ip_address, credential, hostOsh)
                    vector.add(frunOsh)

            except JException, ex:
                msg = ex.getMessage()
                msg = cim_discover.translateErrorMessage(msg)
                errormessages.resolveAndAddToObjectsCollections(msg, protocolName, warningsList, errorsList)
            except:
                trace = logger.prepareJythonStackTrace('')
                errormessages.resolveAndAddToObjectsCollections(trace, protocolName, warningsList, errorsList)

        if testedNamespace is not None:
            cimOsh = cim.createCimOsh(ip_address, hostOsh, credential, sap_solman_discoverer_by_cim.CimCategory.SAP)
            cimOsh.setStringAttribute('application_category', 'SAP')
            vector.add(hostOsh)
            vector.add(cimOsh)
            warningsList = []
            errorsList = []
            break

    if vector.size() <= 0:
        Framework.clearState()
        if (len(warningsList) == 0) and (len(errorsList) == 0):
                msg = errormessages.makeErrorMessage(protocolName, pattern=errormessages.ERROR_GENERIC)
                logger.debug(msg)
                errobj = errorobject.createError(errorcodes.INTERNAL_ERROR_WITH_PROTOCOL, [protocolName], msg)
                errorsList.append(errobj)
    if errorsList:
        for errorObj in errorsList:
            logger.reportErrorObject(errorObj)
    if warningsList:
        for warnObj in warningsList:
            logger.reportErrorObject(warnObj)
    return vector