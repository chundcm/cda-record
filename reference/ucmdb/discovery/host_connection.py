import HostConnectionByShell
import Host_Connection_by_powershell
import ConnectedOSCredentialFinder

import logger
import errormessages
import netutils
import errorcodes
import errorobject
import shellutils
import modeling
import dns_resolver

from com.hp.ucmdb.discovery.library.communication.downloader.cfgfiles import GeneralSettingsConfigFile
from com.hp.ucmdb.discovery.library.clients import ClientsConsts
from java.lang import Exception as JException
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector


def connect(Framework, ip, credentialId, codepage):
    SHELL_CLIENT_PROTOCOLS = HostConnectionByShell._getSupportedShellProtocols(Framework)

    #powershell support
    POWERSHELL_PROTOCOL = shellutils.PowerShell.PROTOCOL_TYPE
    SHELL_CLIENT_PROTOCOLS.append(POWERSHELL_PROTOCOL)

    #todo:add other protocol if needed(wmi, etc.)

    warningsList = []
    errorsList = []

    credentialsByType = {}

    # Gather credentials for protocols
    for clientType in SHELL_CLIENT_PROTOCOLS:
        # getting an ordered list of credentials for the given client type and storing them in the credentials dictionary
        protocols = netutils.getAvailableProtocols(Framework, clientType, ip)
        if protocols:
            credentialsByType[clientType] = protocols

    client = None

    if credentialId:
        try:
            client = Framework.createClient()
        except (Exception, JException), jex:
            msg = str(jex)
            logger.debugException(msg)
            logger.debug('Fail to create client by existed credential id, try all credentials again')

    # only for case where no connection established before
    if not client:
        # checks whether there are credential for specified protocol
        if credentialsByType:
            if not client:
                for clientType in SHELL_CLIENT_PROTOCOLS:
                    credentials = credentialsByType.get(clientType)
                    if credentials:
                        client = HostConnectionByShell.createClient(Framework, clientType, credentials, ip, codepage,
                                                                    warningsList, errorsList)
                        if client:
                            warningsList = []
                            errorsList = []
                            # save credentials id for further reuse
                            Framework.saveState(client.getCredentialId())
                            break
        else:
            for shellType in SHELL_CLIENT_PROTOCOLS:
                #error messages, need to refactor
                msg = errormessages.makeErrorMessage(shellType, pattern=errormessages.ERROR_NO_CREDENTIALS)
                errobj = errorobject.createError(errorcodes.NO_CREDENTIALS_FOR_TRIGGERED_IP, [shellType], msg)
                errorsList.append(errobj)

    return client, warningsList, errorsList



def doConnection(Framework, ip, OSHVResult):

    credentialId = Framework.getDestinationAttribute('credentialsId')
    codepage = Framework.getCodePage()

    hostOsh = None
    shell = None
    udaNotAlive = 0
    shell = None
    client, warningsList, errorsList = connect(Framework, ip, credentialId, codepage)
    if not client:
        # the ip can not be connected, try the alter ip.
        alterip = Framework.getDestinationAttribute("alter_ip")
        logger.debug("The ip %s can not be connected, try the alter one:%s" % (ip, alterip))
        if alterip:
            client, warningsList, errorsList = connect(Framework, alterip, credentialId, codepage)

    if not client:
        logger.debug("Both ip and alter ip are not available, connection fail")
        Framework.clearState()
    else:
        # successfully connected, do discovery
        clientType = client.getClientType()

        connectedOSCredentialID = None
        try:
            try:
                shellFactory = shellutils.ShellFactory()
                shell = shellFactory.createShell(client, clientType)

                connectedOSCredentialID = ConnectedOSCredentialFinder.findCredential(Framework, shell, client,
                                                                                     errorsList, warningsList)

                # If we got a default value, we just pass None later
                # Else - we need to signal the existing client which can be only UDA by now that it has a credential
                # to take sudo password from, if it needs it
                if (not connectedOSCredentialID
                    or connectedOSCredentialID == ConnectedOSCredentialFinder.NO_CONNECTED_CRED_ID):
                    connectedOSCredentialID = None
                else:
                    try:
                        client.setConnectedShellCredentialID(connectedOSCredentialID)
                    except:
                        logger.warn('Failed to setConnectedShellCredentialID, sudo commands may not work in this run')

                logger.debug("Successfully connected, start to do host connection")
                OSHVResult.addAll(doDiscovery(Framework, shell, client, ip, codepage, connectedOSCredentialID,
                                              credentialId, warningsList, errorsList))

                for osh in OSHVResult:
                    if osh.getAttributeValue('host_iscomplete') == 1:
                        logger.debug("found host OSH:", osh)
                        hostOsh = osh

            except (Exception, JException), jex:
                msg = str(jex)
                logger.debugException(msg)
                errormessages.resolveAndAddToObjectsCollections(msg, clientType, warningsList, errorsList)
                #todo: add error message here for asm trouble shooting

        finally:
            if udaNotAlive and client:
                logger.debug('find another shell can be connected. ', shell)
                logger.debug('Removing the connected uda shell because it failed to connect')

                agentOsh = ObjectStateHolder(ClientsConsts.DDM_AGENT_PROTOCOL_NAME)

                agentOsh.setAttribute('application_ip', ip)
                agentOsh.setAttribute('data_name', ClientsConsts.DDM_AGENT_PROTOCOL_NAME)

                #agentOsh.setAttribute('application_port', shell.getPort())
                agentOsh.setContainer(modeling.createHostOSH(ip))
                Framework.deleteObject(agentOsh)
                Framework.flushObjects()

                Framework.clearGlobalState(ip)
            if not OSHVResult.size():
                logger.warn('Discovery failed, though shell object will be created')
                hostOsh = modeling.createHostOSH(ip, filter_client_ip=True)
                if hostOsh:
                    languageName = None
                    langBund = Framework.getEnvironmentInformation().getBundle('langNetwork', languageName)
                    shellOsh = HostConnectionByShell.createShellObj(client, client, ip, langBund, languageName,
                                                                    codepage,
                                                                    connectedShellCredId=connectedOSCredentialID)
                    shellOsh.setContainer(hostOsh)

                    OSHVResult.add(shellOsh)
                else:
                    logger.warn(
                        'Failed to create node and shell since IP is of a Client range type, not enough data for reconciliation.')

    return client, shell, warningsList, errorsList, hostOsh


def doDiscovery(Framework, shell, client, ip, codepage, connectedOSCredentialID,
                credentialId, warningsList, errorsList):
    clientType = client.getClientType()
    if clientType == shellutils.PowerShell.PROTOCOL_TYPE:
        return doPowerShellDiscovery(Framework, shell, ip, credentialId,
                                     codepage, clientType, warningsList, errorsList)
    else:
        return HostConnectionByShell.doDiscovery(Framework, shell, client, ip, codepage, connectedOSCredentialID)


def doPowerShellDiscovery(Framework, shell, ip, credentialId, codepage, shellName, warningsList, errorsList,
                          uduid=None):
    vector = ObjectStateHolderVector()
    try:
        languageName = shell.osLanguage.bundlePostfix

        langBund = Framework.getEnvironmentInformation().getBundle('langNetwork', languageName)

        remoteHostnames = dns_resolver.NsLookupDnsResolver(shell).resolve_hostnames(ip)
        remoteHostFqdn = None
        if remoteHostnames:
            remoteHostFqdn = remoteHostnames[0]
        shellObj = Host_Connection_by_powershell.createShellObj(shell, ip, langBund, languageName, codepage,
                                                                remoteHostFqdn)
        try:
            vector.addAll(Host_Connection_by_powershell.discover(shell, shellObj, ip, langBund, Framework, uduid))
        finally:
            # create shell OSH if connection established
            if shellObj and not vector.size():
                hostOsh = modeling.createHostOSH(ip)
                shellObj.setContainer(hostOsh)
                vector.add(shellObj)
    except Exception, ex:
        strException = ex.getMessage()
        errormessages.resolveAndAddToObjectsCollections(strException, shellName, warningsList, errorsList)
    except:
        msg = str(Host_Connection_by_powershell.sys.exc_info()[1])
        logger.debugException('')
        errormessages.resolveAndAddToObjectsCollections(msg, shellName, warningsList, errorsList)

    return vector
