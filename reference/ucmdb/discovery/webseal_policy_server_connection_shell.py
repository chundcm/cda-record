#coding=utf-8

import logger
import errormessages
import errorobject
import errorcodes
import modeling
from appilog.common.system.types.vectors import ObjectStateHolderVector
from pdadmin_shell_webseal_discoverer import WebSealShell
from java.lang import Exception as JException


def find_valid_credential(credential_ids, client, framework):
    webseal_shell = WebSealShell(framework, client, None)
    for credential_id in credential_ids:
        webseal_shell.webseal_credentials_id = credential_id
        try:
            webseal_shell.setup_command()
        except:
            logger.debugException('Failed to setup with error')
            continue
        try:
            if webseal_shell.get_output('server list'):
                return credential_id
        except (Exception, JException), e:
            logger.debugException('')

def reportPolicyServer(policy_id, credential_id):
    vector = ObjectStateHolderVector()
    policyServerOsh = modeling.createOshByCmdbId('isam_policy_server', policy_id)
    policyServerOsh.setStringAttribute('credentials_id', credential_id)
    vector.add(policyServerOsh)
    return vector

def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    ip = Framework.getDestinationAttribute('ip_address')
    policy_ids = Framework.getTriggerCIDataAsList('policyserver_id_list')
    errorsList = []
    
    protocol = "ldap"
    credential_ids = Framework.getAvailableProtocols(ip, protocol)
    lastConnectedCredential = Framework.loadState()
    lastConnectedCredential and credential_ids.append(lastConnectedCredential)
    
    if not credential_ids:
        msg = errormessages.makeErrorMessage('webseal', pattern=errormessages.ERROR_NO_CREDENTIALS)
        errobj = errorobject.createError(errorcodes.NO_CREDENTIALS_FOR_TRIGGERED_IP, ['webseal'], msg)
        errorsList.append(errobj)
    
    client = Framework.createClient()
    credential_id = find_valid_credential(credential_ids, client, Framework)
        
    if credential_id:
        Framework.saveState(credential_id)
        for policy_id in policy_ids: 
            OSHVResult.addAll(reportPolicyServer(policy_id, credential_id))
    else:
        Framework.clearState()
        msg = errormessages.makeErrorMessage('Shell', pattern=errormessages.ERROR_FAILED_TO_CONNECT_TO_SERVER)
        errobj = errorobject.createError(errorcodes.CONNECTION_FAILED, ['webseal'], msg)
        errorsList.append(errobj)
        
    for errobj in errorsList:
        logger.reportErrorObject(errobj)
    
    return OSHVResult