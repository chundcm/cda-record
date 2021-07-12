#coding=utf-8
import logger
import fptools
import cim
import cim_discover


class CimCategory:
    SAP = 'SAP'

def getSolManCredentials(allCredentials, framework):

    solManCredentialsFilter = fptools.partiallyApply(cim_discover.isCredentialOfCategory, fptools._, CimCategory.SAP, framework)
    solManCredentials = filter(solManCredentialsFilter, allCredentials)

    noCategoryCredentialsFilter = fptools.partiallyApply(cim_discover.isCredentialOfCategory, fptools._, cim.CimCategory.NO_CATEGORY, framework)
    noCategoryCredentials = filter(noCategoryCredentialsFilter, allCredentials)

    return solManCredentials + noCategoryCredentials

def getSolManNamespaces(framework):
    categories = cim_discover.getCimCategories(framework)
    solManCategory = cim_discover.getCategoryByName(CimCategory.SAP, categories)
    if solManCategory:
        return [ns for ns in solManCategory.getNamespaces()]


class SAPBaseDiscoverer:
    '''
    Basic Discoverer class from which all specific discoverers should derive.
    '''
    def __init__(self, client):
        self.client = client
        self.className = None
        self.keyProperty = 'Name'

    def parse(self, instances):
        result = {}
        for instance in instances:
            if instance:
                if instance.getProperty(self.keyProperty):
                    keyProperty = instance.getProperty(self.keyProperty).getValue()
                    result.setdefault(keyProperty, instance)
        return result

    def discover(self):
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Get instances of class "%s"' % self.className)
        instances = self.client.getInstances(self.className)
        return self.parse(instances)


class SAPRelationDiscoverer(SAPBaseDiscoverer):
    '''
    Basic Discoverer class that discover the links.
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)

    def discover(self):
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Get class "%s"' % self.className)
        instances = self.client.getInstances(self.className)
        return instances


class J2EESystemDiscoverer(SAPBaseDiscoverer):
    '''
    J2EE System Discoverer
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_J2EEEngineCluster'

class J2EEApplicationServerDiscoverer(SAPBaseDiscoverer):
    '''
    J2EE Application Server Discoverer
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_J2EEEngineInstance'

class J2EEApplicationServerHostDiscoverer(SAPRelationDiscoverer):
    '''
    Discover link: J2EE Application Server <-> Host
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_J2EEEngineInstanceHost'

class J2EESystemApplicationServerDiscoverer(SAPRelationDiscoverer):
    '''
    Discover link: J2EE system <-> J2EE Application Server
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_J2EEEngineClusterInstance'

class J2EESystemSystemDBDiscoverer(SAPRelationDiscoverer):
    '''
    Discover link: J2EE system <-> DB System
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_J2EEEngineSystemDB'

class J2EESystemServiceDiscoverer(SAPRelationDiscoverer):
    '''
    Discover link: J2EE system <-> Central Service Instance
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_J2EEEngineServiceInstance'


class BCSystemDiscoverer(SAPBaseDiscoverer):
    '''
    ABAP system Discoverer
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_BCSystem'

class HostDiscoverer(SAPBaseDiscoverer):
    '''
    Host Discoverer
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_ComputerSystem'

class BCApplicationServerDiscoverer(SAPBaseDiscoverer):
    '''
    ABAP Application Server Discoverer
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_BCApplicationServer'

class BCApplicationServerHostDiscoverer(SAPRelationDiscoverer):
    '''
    Discover link: ABAP Application Server <-> Host
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_BCApplicationServerHost'

class BCSystemApplicationServerDiscoverer(SAPRelationDiscoverer):
    '''
    Discover link: ABAP system <-> ABAP Application Server
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_BCSystemApplicationServer'

class DatabaseSystemDiscoverer(SAPBaseDiscoverer):
    '''
    DB System Discoverer
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_DatabaseSystem'

class BCSystemSystemDBDiscoverer(SAPRelationDiscoverer):
    '''
    Discover link: ABAP system <-> DB System
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_BCSystemSystemDB'

class DatabaseInstanceDiscoverer(SAPBaseDiscoverer):
    '''
    DB System Instance Discoverer
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_DatabaseInstance'

class DBSystemInstanceDiscoverer(SAPRelationDiscoverer):
    '''
    Discover link: DB system <-> DB Instance
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_DBSystemInstance'

class DBInstanceHostDiscoverer(SAPRelationDiscoverer):
    '''
    Discover link: DB Instance system <-> DB Instance Host
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_DBInstanceHost'

class BCClientDiscoverer(SAPBaseDiscoverer):
    '''
    ABAP Client Discoverer
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_BCClient'

class BCSystemClientDiscoverer(SAPRelationDiscoverer):
    '''
    Discover link: ABAP system <-> ABAP Client
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_BCSystemClient'

class InstalledSoftwareComponentDiscoverer(SAPBaseDiscoverer):
    '''
    Installed Software Component Discoverer
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_InstalledSoftwareComponent'

class InstalledSWComponentOnApplicationSystemDiscoverer(SAPRelationDiscoverer):
    '''
    Discover link: ABAP System <-> Installed Software Component
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_InstalledSWComponentOnApplicationSystem'

class SoftwareComponentDiscoverer(SAPBaseDiscoverer):
    '''
    Software Component Discoverer
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_SoftwareComponent'

class SoftwareComponentTypeDiscoverer(SAPRelationDiscoverer):
    '''
    Discover link: Software Component <-> Installed Software Component
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_SoftwareComponentType'

class BCCentralServiceDiscoverer(SAPBaseDiscoverer):
    '''
    ABAP Central Service Instance Discoverer
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_BCCentralServiceInstance'

class BCSystemServiceDiscoverer(SAPRelationDiscoverer):
    '''
    Discover link: ABAP system <-> Central Service Instance
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_BCSystemServiceInstance'

class BCCentralServiceHostDiscoverer(SAPRelationDiscoverer):
    '''
    Discover link: ABAP Central Service Instance <-> Host
    '''
    def __init__(self, client):
        SAPBaseDiscoverer.__init__(self, client)
        self.className = 'SAP_BCCentralServiceInstanceHost'
