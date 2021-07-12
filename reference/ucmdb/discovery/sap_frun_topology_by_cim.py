import logger
import errormessages
import errorobject
import errorcodes
import modeling
import cim
import cim_discover
import sap_solman_discoverer_by_cim
import sap_solman_topology_by_cim

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector
from java.lang import Boolean
from java.lang import Exception as JavaException


class CustomerNamespacesDiscoverer(sap_solman_discoverer_by_cim.SAPBaseDiscoverer):
    '''
    Frun Customer Namespace discoverer
    '''

    def __init__(self, client):
        sap_solman_discoverer_by_cim.SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_CustomerNetwork'


class sapFunDiscover:
    def __init__(self, framework, reportCmpAsConfig, ipAddress, credentialsId, client, hostOsh, frunOsh):
        self.framework = framework
        self.reportCmpAsConfig = reportCmpAsConfig
        self.custmerNamespaces = []
        self.ipAddress = ipAddress
        self.credentialsId = credentialsId
        self.namespaceInstance = {}
        self.namespaceOsh = {}
        self.client = client
        self.hostOsh = hostOsh
        self.frunOsh = frunOsh

    def discover(self):
        vector = ObjectStateHolderVector()
        vector.addAll(self.getCustomerNamespaces())
        vecSolman = self.buildSolmanTopoplgy()
        vector.addAll(vecSolman)
        return vector

    def getCustomerNamespaces(self):
        vector = ObjectStateHolderVector()
        customerNetworkDis = CustomerNamespacesDiscoverer(self.client)
        self.customerNetwork = customerNetworkDis.discover()
        for (key, instance) in self.customerNetwork.items():
            namespace = instance.getProperty('NamespaceName').getValue()
            caption = instance.getProperty('Caption').getValue()
            customer = instance.getProperty('CustomerName').getValue()
            name = instance.getProperty('Name').getValue()
            funNamespaceOsh = ObjectStateHolder("sap_frun_namespace")
            funNamespaceOsh.setAttribute("caption", caption)
            funNamespaceOsh.setAttribute("namespace", namespace)
            funNamespaceOsh.setAttribute("customer", customer)
            funNamespaceOsh.setAttribute("name", name)
            funNamespaceOsh.setAttribute("customer_network", name)
            vector.add(funNamespaceOsh)
            funNamespaceOsh.setContainer(self.frunOsh)
            self.custmerNamespaces.append(namespace)
            self.namespaceInstance.setdefault(namespace, instance)
            self.namespaceOsh.setdefault(namespace, funNamespaceOsh)

        ###discover the software component for customer network
        swComponentDis = sap_solman_discoverer_by_cim.SoftwareComponentDiscoverer(self.client)
        self.SwComponents = swComponentDis.discover()

        return vector

    def buildSolmanTopoplgy(self):
        vector = ObjectStateHolderVector()

        for namespace in self.custmerNamespaces:
            try:
                if namespace:
                    frunClient = cim_discover.createClient(self.framework, self.ipAddress, namespace, self.credentialsId)
                    logger.debug('Connected to namespace "%s"' % namespace)
                    logger.debug(self.namespaceOsh.get(namespace))
                    funNamespaceOsh = self.namespaceOsh.get(namespace)
                    solManDiscoverer = sap_solman_topology_by_cim.SapSolManDiscover(frunClient, self.reportCmpAsConfig,
                                                                                    funNamespaceOsh, self.SwComponents)
                    solManVector = solManDiscoverer.discover()
            finally:
                try:
                    vector.addAll(solManVector)
                    logger.debug ("close the frunClient for namespace %s" %(namespace))
                    frunClient and frunClient.close()
                except:
                    logger.debug("Cannot close the client for namespace: %s." %(namespace))
        return vector

    # def buildFrunTopology(self):
    #
    #     pass


def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    protocol = cim.Protocol.DISPLAY
    credentialsId = Framework.getDestinationAttribute('credentialsId')
    ipAddress = Framework.getDestinationAttribute('ip_address')
    reportCmpAsConfig = Boolean.parseBoolean(Framework.getParameter("reportComponentsAsConfigFile"))
    frunCmdbId = Framework.getDestinationAttribute('id')
    hostOsh = modeling.createHostOSH(ipAddress)
    frunOsh = modeling.createOshByCmdbIdString('running_software', frunCmdbId)
    solManNamespaces = sap_solman_discoverer_by_cim.getSolManNamespaces(Framework)
    if not solManNamespaces:
        msg = errormessages.makeErrorMessage(cim.Protocol.DISPLAY, "No SAP namespaces found")
        errobj = errorobject.createError(errorcodes.INTERNAL_ERROR_WITH_PROTOCOL_DETAILS, [cim.Protocol.DISPLAY, msg],
                                         msg)
        logger.reportErrorObject(errobj)
        return OSHVResult
    errorMessges = []

    for namespaceObject in solManNamespaces:
        client = None
        namespace = namespaceObject.getName()
        if namespace == "customernetworkadministration":
            try:
                try:
                    client = cim_discover.createClient(Framework, ipAddress, namespace, credentialsId)
                    logger.debug('Connected to namespace "%s"' % namespace)

                    frunDiscoverer = sapFunDiscover(Framework, reportCmpAsConfig, ipAddress, credentialsId, client,
                                                    hostOsh, frunOsh)
                    OSHVResult.addAll(frunDiscoverer.discover())

                    errorMessges = []
                    break
                finally:
                    try:
                        client and client.close()
                    except:
                        logger.error("Unable to close client")
            except JavaException, ex:
                logger.debugException('')
                msg = ex.getMessage()
                msg = cim_discover.translateErrorMessage(msg)
                errorMessges.append(msg)
            except:
                logger.debugException('')
                strException = logger.prepareJythonStackTrace('')
                errorMessges.append(strException)

    if errorMessges:
        for message in errorMessges:
            errormessages.resolveAndReport(message, protocol, Framework)

    return OSHVResult
