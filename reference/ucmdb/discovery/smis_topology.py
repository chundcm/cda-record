#coding=utf-8
import logger 
import errormessages
import errorobject
import errorcodes

import smis_discoverer
import cim
import cim_discover
import smis

from appilog.common.system.types.vectors import ObjectStateHolderVector
from java.lang import Exception as JavaException

DEFAULTNAMESPACE = 'root/cimv2'
L3PARNAMESPACE = 'root/tpd'
EVANAMESPACE = 'root/eva'
LISTARRAY13 = 'root/LsiArray13'
EMCNAMESPACE='root/emc'
BROCADENAMESPACE='root/brocade1'
HUAWEINAMESPACE='root/huawei'
HITACHINAMESPACE='root/smis/current'
IBMNAMESPACE='root/ibm'
PURESTORAGENAMESPACE = 'purestorage'

NAMESPACE2VENDORMAP = {
    DEFAULTNAMESPACE:   'cimv2',
    L3PARNAMESPACE:     'tpd',
    EVANAMESPACE:       'eva',
    LISTARRAY13:        'netapp',
    EMCNAMESPACE:       'emc',
    BROCADENAMESPACE:   'brocade',
    HUAWEINAMESPACE:    'huawei',
    HITACHINAMESPACE:   'hitachi',
    IBMNAMESPACE:       'ibm',
    PURESTORAGENAMESPACE: 'purestorage'
}

class StorageTopology:
    def __init__(self):
        self.storage_systems=[]
        self.storage_processors = []
        self.ports = []
        self.storage_pools = []
        self.logical_volumes = []
        self.hc_hbas = []
        self.physical_volumes = []
        self.file_shares = []
        self.file_systems = []
        self.end_points_links = []
        self.lun_mappings = []
        self.physcial_volumes_2_pool_links = {}
        self.hosts = []
        self.remote_endpoints = []
        self.end_point_links = []
        self.iogroups = []

class FabricTopology:
    def __init__(self):
        self.storage_fabrics=[]
        self.fc_switchs = []
        self.hosts = []
        self.ports = []
        self.switch_2_fabric = {}
        self.physical_switch_2_logical_switch = {}
        self.fcport_connections = {}


class TopologyDiscover:
    def __init__(self, topology=None):
        if not topology:
            topology = StorageTopology()
        self.topology = topology
        self.namespace = None

    def bindNamespace(self, ns):
        vendor = NAMESPACE2VENDORMAP.get(ns)
        script =  'smis_'+vendor
        className = 'Namespace'
        logger.debug("Namespace vendor name %s" % vendor)
        module = __import__(script)
        if hasattr(module, className):
            nampespaceClass = getattr(module, className)
            self.namespace = nampespaceClass()
            logger.debug("Imported the module %s" % script)
            logger.debug("Got the namespace %s" % self.namespace)
        else:
            logger.debug("Failed to import the module %s" % script)

    def discover(self, client):
        if not self.namespace:
            raise ValueError('please bind the namespace first.')

        self.namespace.associateTopologyObj2Discoverers(self.topology)
        self.namespace.discover(client)

def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    protocol = cim.Protocol.DISPLAY
    credentialsId = Framework.getDestinationAttribute('credentialsId')
    ipAddress = Framework.getDestinationAttribute('ip_address')
    
    smisNamespaces = smis_discoverer.getSmisNamespaces(Framework)
    if not smisNamespaces:
        msg = errormessages.makeErrorMessage(cim.Protocol.DISPLAY, "No SMI-S namespaces found")
        errobj = errorobject.createError(errorcodes.INTERNAL_ERROR_WITH_PROTOCOL_DETAILS, [cim.Protocol.DISPLAY, msg], msg)
        logger.reportErrorObject(errobj)
        return OSHVResult
    errorMessges = []
    for namespaceObject in smisNamespaces:
        client = None
        namespace = namespaceObject.getName()
        try:
            try:
                client = cim_discover.createClient(Framework, ipAddress, namespace, credentialsId)
                
                logger.debug('Connected to namespace "%s"' % namespace)
                if BROCADENAMESPACE == namespace:
                    topologyDiscoverer = TopologyDiscover( FabricTopology())
                else:
                    topologyDiscoverer = TopologyDiscover()

                topologyDiscoverer.bindNamespace(namespace)
                topologyDiscoverer.discover(client)
                topoBuilder = smis.TopologyBuilder()
                if BROCADENAMESPACE == namespace:
                    OSHVResult.addAll(topoBuilder.reportFabricTopology(topologyDiscoverer.topology))
                else:
                    OSHVResult.addAll(topoBuilder.reportStorageTopology(topologyDiscoverer.topology))

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
            #errormessages.resolveAndReport(msg, protocol, Framework)
        except:
            logger.debugException('')
            strException = logger.prepareJythonStackTrace('')
            errorMessges.append(strException)
            
    if errorMessges:
        for message in errorMessges:
            errormessages.resolveAndReport(message, protocol, Framework)
        
    return OSHVResult


