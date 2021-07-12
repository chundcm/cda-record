#coding=utf-8
import logger
import errormessages
import errorobject
import errorcodes
import re

import modeling
import dns_resolver
import cim
import cim_discover
import db
import db_platform
import db_builder
import sap
import sap_abap
import sap_jee
import sap_solman_discoverer_by_cim

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector
from java.lang import Boolean
from java.lang import Exception as JavaException


OSH = 'osh'
OBJ = 'object'
INS = 'instance'

def stringClean(value):
    '''
    Transforms a value to a string and strips out space and " symbols from it
    @param value: string convertable value
    '''
    if value is not None:
        return str(value).strip(' "\\')
    else:
        return None


def _reportAbapServer(inst, system, isScs, hostOsh, systemOsh):
    '''
    @types: Instance, System, bool, osh, osh -> oshv
    '''
    vector = ObjectStateHolderVector()
    reportInstName = False
    reporter, pdo = None, None
    if not isScs:
        builder = sap_abap.InstanceBuilder(reportInstName=reportInstName)
        reporter = sap_abap.InstanceReporter(builder)
        pdo = sap_abap.InstanceBuilder.createPdo(inst, system)
    else:
        builder = sap_abap.AscsInstanceBuilder(reportInstName=reportInstName)
        reporter = sap_abap.InstanceReporter(builder)
        pdo = sap_abap.AscsInstanceBuilder.createPdo(inst, system)
    instOsh = reporter.reportInstance(pdo, hostOsh)
    vector.add(instOsh)

    linkReporter = sap.LinkReporter()
    vector.add(linkReporter.reportMembership(systemOsh, instOsh))
    return vector


def _reportJavaServer(inst, system, isScs, hostOsh, systemOsh):
    '''
    @types: Instance, System, bool, osh, osh -> oshv
    '''
    vector = ObjectStateHolderVector()
    reportInstName = False
    clusterOsh = sap_jee.reportClusterOnSystem(system, systemOsh)
    reporter, pdo = None, None

    if not isScs:
        builder = sap_jee.InstanceBuilder(reportInstName=reportInstName)
        reporter = sap_jee.InstanceReporter(builder)
        pdo = sap_jee.InstanceBuilder.InstancePdo(inst, system)
    else:
        builder = sap_jee.ScsInstanceBuilder(reportInstName=reportInstName)
        reporter = sap_jee.InstanceReporter(builder)
        pdo = sap_jee.InstanceBuilder.InstancePdo(inst, system)

    instOsh = reporter.reportInstancePdo(pdo, hostOsh)
    vector.add(instOsh)

    linkReporter = sap.LinkReporter()
    vector.add(linkReporter.reportMembership(clusterOsh, instOsh))
    vector.add(linkReporter.reportMembership(systemOsh, instOsh))
    return vector


def _reportDatabase(dbSystem, DbSystemToDbInstance, DbInstances, DbInstanceToHost, Hosts, systemOsh):
    vector = ObjectStateHolderVector()
    name = dbSystem.getProperty('Name').getValue()
    dbName = dbSystem.getProperty('DBName').getValue()
    vendor = dbSystem.getProperty('Manufacturer').getValue()
    type = dbSystem.getProperty('DBTypeForSAP').getValue()
    version = dbSystem.getProperty('Release').getValue()

    if not type or type == 'unknown':
        logger.debug('Ignore DB %s, due to unknown type.' % dbName)
        return vector

    if len(version) > 50:
        patten = re.compile('(\d+).(\d+).(\d+).(\d+).(\d+)')
        match = patten.search(version)
        if match:
            version = match.group(0)

    if type.lower() == 'hdb' and DbSystemToDbInstance.has_key(name):
        hanaDbOSH = ObjectStateHolder('hana_db')
        hanaDbOSH.setStringAttribute('name', dbName)
        hanaDbOSH.setStringAttribute('version_description', version)
        hanaDbOSH.setStringAttribute('vendor', 'SAP SE')

        instances = []

        for dbInstanceName in DbSystemToDbInstance[name]:
            hostName = DbInstanceToHost[dbInstanceName][0]
            hostInstance = Hosts[hostName]
            name = hostInstance.getProperty('Name').getValue()
            ip = hostInstance.getProperty('IPAddress').getValue()
            ips = _resolveIps(ip, name)
            if ips:
                hostReporter = sap.HostReporter(sap.HostBuilder())
                hostOsh, hVector = hostReporter.reportHostWithIps(*ips)
                hostOsh.setStringAttribute('name', hostName)
                vector.addAll(hVector)

                dbInstance = DbInstances[dbInstanceName]
                instanceNumber = dbInstance.getProperty('InstanceNumber').getValue()

                instanceOSH = ObjectStateHolder('hana_instance')
                instanceOSH.setContainer(hostOsh)
                instanceOSH.setStringAttribute('name', dbName)
                instanceOSH.setStringAttribute('number', instanceNumber)
                instanceOSH.setStringAttribute('product_name', 'hana_database')
                instanceOSH.setStringAttribute('discovered_product_name', 'SAP HanaDB')
                instances.append(instanceOSH)
                vector.add(instanceOSH)

        if len(instances) < 1:
            logger.warn("Not enough Hana instance OSHs available for identifying the Hana database. Canceling.")
            return vector

        vector.add(hanaDbOSH)
        vector.add(modeling.createLinkOSH('dependency', systemOsh, hanaDbOSH))
        for instanceOSH in instances:
            vector.add(modeling.createLinkOSH('membership', hanaDbOSH, instanceOSH))
        return vector
    if DbSystemToDbInstance.has_key(name):
        dbInstanceName = DbSystemToDbInstance[name][0]
        if DbInstanceToHost.has_key(dbInstanceName):
            hostName = DbInstanceToHost[dbInstanceName][0]
            hostInstance = Hosts[hostName]
            name = hostInstance.getProperty('Name').getValue()
            ip = hostInstance.getProperty('IPAddress').getValue()
            ips = _resolveIps(ip, name)
            if ips:
                hostReporter = sap.HostReporter(sap.HostBuilder())
                hostOsh, hVector = hostReporter.reportHostWithIps(*ips)
                dbServer = db.DatabaseServer(dbName, instance=dbName, vendor=vendor, version=version)
                platform = db_platform.findPlatformBySignature(type)
                builder = db_builder.getBuilderByPlatform(platform)
                reporter = db.TopologyReporter(builder)
                serverOsh = reporter.reportServer(dbServer, hostOsh)
                systemToDbLink = modeling.createLinkOSH('dependency', systemOsh, serverOsh)
                vector.add(systemToDbLink)
                vector.add(serverOsh)
                vector.addAll(hVector)
    return vector


class SoftwareComponent(sap.SoftwareComponent):
    def __init__(self, name, type,
                 description=None, versionInfo=None):
        r'''
        @types: str, str, str, str, VersionInfo
        @param name: parameter name, like 'ST-A/PI'
        @param packageLevel: level, like '0000'
        @param versionInfo: a particular version of a software component
        @raise ValueError: Support package level is not valid
        @raise ValueError: Component type is not correct
        '''
        assert name and str(name).strip()

        self.name = name
        self.type = type
        self.description = description
        self.versionInfo = versionInfo


class SapSolManDiscover:
    def __init__(self, client, reportCmpAsConfig, namespaceOsh = None, SwComponents = None):
        self.client = client
        self.nameSpace = 'active'
        self.reportCmpAsConfig = reportCmpAsConfig
        self.namespaceOsh = namespaceOsh
        self.SwComponents = SwComponents

    def discover(self):
        vector = ObjectStateHolderVector()
        self.getAbapSystem()
        vector.addAll(self.buildAbapSystemTopology(self.reportCmpAsConfig, self.namespaceOsh))
        self.getJ2eeSystem()
        vector.addAll(self.buildJ2eeSystemTopology(self.reportCmpAsConfig, self.namespaceOsh))
        return vector

    def getJ2eeSystem(self):
        j2eeSystemDis = sap_solman_discoverer_by_cim.J2EESystemDiscoverer(self.client)
        self.J2eeSystems = j2eeSystemDis.discover()

        j2eeAsDis = sap_solman_discoverer_by_cim.J2EEApplicationServerDiscoverer(self.client)
        self.J2eeServers = j2eeAsDis.discover()

        j2eeServerToHostDis = sap_solman_discoverer_by_cim.J2EEApplicationServerHostDiscoverer(self.client)
        self.J2eeServerToHostLinks = j2eeServerToHostDis.discover()
        self.J2eeServerToHost = self.parseLinks(self.J2eeServerToHostLinks)

        j2eeSysToAsDis = sap_solman_discoverer_by_cim.J2EESystemApplicationServerDiscoverer(self.client)
        self.J2eeSysToAsLinks = j2eeSysToAsDis.discover()
        self.J2eeSysToAs = self.parseLinks(self.J2eeSysToAsLinks)

        j2eeSysToDbSystemDis = sap_solman_discoverer_by_cim.J2EESystemSystemDBDiscoverer(self.client)
        self.J2eeSysToDbSystemLinks = j2eeSysToDbSystemDis.discover()
        self.J2eeSysToDbSystem = self.parseLinks(self.J2eeSysToDbSystemLinks)

        j2eeSysToServiceDis = sap_solman_discoverer_by_cim.J2EESystemServiceDiscoverer(self.client)
        self.J2eeSystemToCentralServiceLinks = j2eeSysToServiceDis.discover()
        self.J2eeSystemToCentralService = self.parseLinks(self.J2eeSystemToCentralServiceLinks)

    def getJ2eeServers(self, sysName):
        vector = ObjectStateHolderVector()
        system = self.J2eeSystems[sysName][OBJ]
        systemOsh = self.J2eeSystems[sysName][OSH]
        if self.J2eeSysToAs.has_key(sysName):
            asNames = self.J2eeSysToAs[sysName]
        else:
            return vector
        for asName in asNames:
            if self.J2eeServers.has_key(asName):
                serverInstance = self.J2eeServers[asName]
                name = serverInstance.getProperty('Name').getValue()
                number = serverInstance.getProperty('InstanceID').getValue()
                if self.J2eeServerToHost.has_key(name):
                    hostName = self.J2eeServerToHost[name][0]
                    hostInstance = self.Hosts[hostName]
                    ip = hostInstance.getProperty('IPAddress').getValue()
                    ips = _resolveIps(ip, hostName)
                    if ips:
                        hostReporter = sap.HostReporter(sap.HostBuilder())
                        hostOsh, hVector = hostReporter.reportHostWithIps(*ips)
                        hostOsh.setStringAttribute('name', hostName)
                        inst = sap.Instance('fake', number, hostName)
                        vector.addAll(hVector)
                        vector.addAll(_reportJavaServer(inst, system, False, hostOsh, systemOsh))
        return vector

    def getJ2eeCentralServices(self, sysName):
        vector = ObjectStateHolderVector()
        system = self.J2eeSystems[sysName][OBJ]
        systemOsh = self.J2eeSystems[sysName][OSH]
        if self.J2eeSystemToCentralService.has_key(sysName):
            csNames = self.J2eeSystemToCentralService[sysName]
        else:
            return vector
        for csName in csNames:
            if self.AbapCentralServices.has_key(csName):
                serverInstance = self.AbapCentralServices[csName]
                name = serverInstance.getProperty('Name').getValue()
                number = serverInstance.getProperty('ServiceInstanceID').getValue()
                if self.ServiceToHost.has_key(name):
                    hostName = self.ServiceToHost[name][0]
                    hostInstance = self.Hosts[hostName]
                    ip = hostInstance.getProperty('IPAddress').getValue()
                    ips = _resolveIps(ip, hostName)
                    if ips:
                        hostReporter = sap.HostReporter(sap.HostBuilder())
                        hostOsh, hVector = hostReporter.reportHostWithIps(*ips)
                        inst = sap.Instance('fake', number, hostName)
                        vector.addAll(hVector)
                        vector.addAll(_reportJavaServer(inst, system, True, hostOsh, systemOsh))
        return vector

    def getJ2eeDatabases(self, sysName):
        vector = ObjectStateHolderVector()
        systemOsh = self.J2eeSystems[sysName][OSH]
        if self.J2eeSysToDbSystem.has_key(sysName):
            dbSysNames = self.J2eeSysToDbSystem[sysName]
        else:
            return vector
        for dbSysName in dbSysNames:
            if self.DbSystems.has_key(dbSysName):
                dbSystem = self.DbSystems[dbSysName]
                vector.addAll(_reportDatabase(dbSystem, self.DbSystemToDbInstance, self.DbInstances, self.DbInstanceToHost, self.Hosts, systemOsh))
        return vector

    def getJ2eeComponents(self, sysName, reportCmpAsConfig):
        vector = ObjectStateHolderVector()
        components = []
        systemOsh = self.J2eeSystems[sysName][OSH]
        if self.AbapSysToInstalledSwComp.has_key(sysName):
            installedSwCompNames = self.AbapSysToInstalledSwComp[sysName]
        else:
            return vector
        for installedSwCompName in installedSwCompNames:
            if self.InstalledSwComps.has_key(installedSwCompName):
                installedSwCompsInstance = self.InstalledSwComps[installedSwCompName]
                name = installedSwCompsInstance.getProperty('Name').getValue()
                if self.InstalledToSwComponent.has_key(name):
                    swComponentName = self.InstalledToSwComponent[name][0]
                    if self.SwComponents.has_key(swComponentName):
                        swComponentInstance = self.SwComponents[swComponentName]
                        name = swComponentInstance.getProperty('Name').getValue()
                        version = swComponentInstance.getProperty('Version').getValue()
                        description = swComponentInstance.getProperty('Description').getValue()
                        type = swComponentInstance.getProperty('Type').getValue()
                        versionInfo = sap.VersionInfo(version)
                        component = SoftwareComponent(name, type, description, versionInfo)
                        components.append(component)

        if reportCmpAsConfig:
            vector.addAll(sap_abap.reportSoftwareCmpsAsConfigFile(components, systemOsh))
        else:
            for comp in components:
                vector.add(self.reportJ2eeSoftwareComponent(comp, systemOsh))
        return vector

    def reportJ2eeSoftwareComponent(self, component, systemOsh):
        assert component, "Software component is not specified"
        assert systemOsh, "Software component container is not specified"
        osh = ObjectStateHolder('sap_java_software_component')
        osh.setAttribute('name', component.name)
        if component.description:
            osh.setStringAttribute('description', component.description)
        versionInfo = component.versionInfo
        if versionInfo:
            osh.setStringAttribute('release', versionInfo.release)
            if versionInfo.patchLevel.value() is not None:
                osh.setIntegerAttribute('patch_level',
                                        versionInfo.patchLevel.value())
        osh.setContainer(systemOsh)
        return osh

    def buildJ2eeSystemTopology(self, reportCmpAsConfig, namespaceOsh = None):
        vector = ObjectStateHolderVector()
        self.J2eeSystems = self.getJ2eeSystemInstances()
        for (sysName, systemPair) in self.J2eeSystems.items():
            logger.debug('Discover J2EE system: ', sysName)
            vector.add(systemPair[OSH])
            if namespaceOsh:
                linkReporter = sap.LinkReporter()
                vector.add(linkReporter.reportMembership(namespaceOsh, systemPair[OSH]))
            vector.addAll(self.getJ2eeServers(sysName))
            vector.addAll(self.getJ2eeComponents(sysName, reportCmpAsConfig))
            vector.addAll(self.getJ2eeDatabases(sysName))
            vector.addAll(self.getJ2eeCentralServices(sysName))
        return vector

    def getJ2eeSystemInstances(self):
        systems = {}
        for (key, instance) in self.J2eeSystems.items():
            name = instance.getProperty('Name').getValue()
            systemName = instance.getProperty('SAPSystemName').getValue()
            extSIDName = instance.getProperty('ExtSIDName').getValue()
            if systemName:
                sapSystem = sap.System(systemName, extendedSystemID = extSIDName)
                systems.setdefault(name, {})
                systems[name].setdefault(OBJ, sapSystem)
                systems[name].setdefault(INS, instance)

        reporter = sap.Reporter(sap.Builder())
        for (name, system) in systems.items():
            osh = reporter.reportSystem(system[OBJ])
            systems[name].setdefault(OSH, osh)
        return systems

    def getAbapSystem(self):
        abapSystemDis = sap_solman_discoverer_by_cim.BCSystemDiscoverer(self.client)
        self.AbapSystems = abapSystemDis.discover()

        hostDis = sap_solman_discoverer_by_cim.HostDiscoverer(self.client)
        self.Hosts = hostDis.discover()

        abapAsDis = sap_solman_discoverer_by_cim.BCApplicationServerDiscoverer(self.client)
        self.AbapServers = abapAsDis.discover()

        abapServerToHostDis = sap_solman_discoverer_by_cim.BCApplicationServerHostDiscoverer(self.client)
        self.AbapServerToHostLinks = abapServerToHostDis.discover()
        self.AbapServerToHost = self.parseLinks(self.AbapServerToHostLinks)

        abapSysToAsDis = sap_solman_discoverer_by_cim.BCSystemApplicationServerDiscoverer(self.client)
        self.AbapSysToAsLinks = abapSysToAsDis.discover()
        self.AbapSysToAs = self.parseLinks(self.AbapSysToAsLinks)

        dbSystemDis = sap_solman_discoverer_by_cim.DatabaseSystemDiscoverer(self.client)
        self.DbSystems = dbSystemDis.discover()

        abapSysToDbSystemDis = sap_solman_discoverer_by_cim.BCSystemSystemDBDiscoverer(self.client)
        self.AbapSysToDbSystemLinks = abapSysToDbSystemDis.discover()
        self.AbapSysToDbSystem = self.parseLinks(self.AbapSysToDbSystemLinks)

        dbInsDis = sap_solman_discoverer_by_cim.DatabaseInstanceDiscoverer(self.client)
        self.DbInstances = dbInsDis.discover()

        dbSystemToDbInsDis = sap_solman_discoverer_by_cim.DBSystemInstanceDiscoverer(self.client)
        self.DbSystemToDbInstanceLinks = dbSystemToDbInsDis.discover()
        self.DbSystemToDbInstance = self.parseLinks(self.DbSystemToDbInstanceLinks)

        dbInsToHostDis = sap_solman_discoverer_by_cim.DBInstanceHostDiscoverer(self.client)
        self.DbInstanceToHostLinks = dbInsToHostDis.discover()
        self.DbInstanceToHost = self.parseLinks(self.DbInstanceToHostLinks)

        abapClientDis = sap_solman_discoverer_by_cim.BCClientDiscoverer(self.client)
        self.AbapClients = abapClientDis.discover()

        sysToClientDis = sap_solman_discoverer_by_cim.BCSystemClientDiscoverer(self.client)
        self.SysToClientLinks = sysToClientDis.discover()
        self.SysToClient = self.parseLinks(self.SysToClientLinks)

        installedSwCompDis = sap_solman_discoverer_by_cim.InstalledSoftwareComponentDiscoverer(self.client)
        self.InstalledSwComps = installedSwCompDis.discover()

        installedSwToAppSysDis = sap_solman_discoverer_by_cim.InstalledSWComponentOnApplicationSystemDiscoverer(self.client)
        self.AbapSysToInstalledSwCompLinks = installedSwToAppSysDis.discover()
        self.AbapSysToInstalledSwComp = self.parseLinks(self.AbapSysToInstalledSwCompLinks)
        if not self.SwComponents:
            swComponentDis = sap_solman_discoverer_by_cim.SoftwareComponentDiscoverer(self.client)
            self.SwComponents = swComponentDis.discover()

        installedToSwComponentDis = sap_solman_discoverer_by_cim.SoftwareComponentTypeDiscoverer(self.client)
        self.InstalledToSwComponentLinks = installedToSwComponentDis.discover()
        self.InstalledToSwComponent = self.parseLinks(self.InstalledToSwComponentLinks)

        abapCentralServDis = sap_solman_discoverer_by_cim.BCCentralServiceDiscoverer(self.client)
        self.AbapCentralServices = abapCentralServDis.discover()

        abapSysToServiceDis = sap_solman_discoverer_by_cim.BCSystemServiceDiscoverer(self.client)
        self.AbapSystemToCentralServiceLinks = abapSysToServiceDis.discover()
        self.AbapSystemToCentralService = self.parseLinks(self.AbapSystemToCentralServiceLinks)

        serviceToHostDis = sap_solman_discoverer_by_cim.BCCentralServiceHostDiscoverer(self.client)
        self.ServiceToHostLinks = serviceToHostDis.discover()
        self.ServiceToHost = self.parseLinks(self.ServiceToHostLinks)

    def buildAbapSystemTopology(self, reportCmpAsConfig, namespaceOsh = None):
        vector = ObjectStateHolderVector()
        self.AbapSystems = self.getAbapSystemInstances()
        for (sysName, systemPair) in self.AbapSystems.items():
            logger.debug('Discover ABAP system: ', sysName)
            vector.add(systemPair[OSH])
            if namespaceOsh:
                linkReporter = sap.LinkReporter()
                vector.add(linkReporter.reportMembership(namespaceOsh, systemPair[OSH]))

            vector.addAll(self.getAbapComponents(sysName, reportCmpAsConfig))
            vector.addAll(self.getClients(sysName))
            vector.addAll(self.getAbapDatabases(sysName))
            vector.addAll(self.getAbapServers(sysName))
            vector.addAll(self.getAbapCentralServices(sysName))
        return vector

    def getAbapSystemInstances(self):
        systems = {}
        for (key, instance) in self.AbapSystems.items():
            name = instance.getProperty('Name').getValue()
            systemName = instance.getProperty('SAPSystemName').getValue()
            extSIDName = instance.getProperty('ExtSIDName').getValue()
            if systemName:
                sapSystem = sap.System(systemName, extendedSystemID=extSIDName)
                systems.setdefault(name, {})
                systems[name].setdefault(OBJ, sapSystem)
                systems[name].setdefault(INS, instance)

        reporter = sap.Reporter(sap.Builder())
        for (name, system) in systems.items():
            osh = reporter.reportSystem(system[OBJ])
            systems[name].setdefault(OSH, osh)
        return systems

    def getAbapComponents(self, sysName, reportCmpAsConfig):
        vector = ObjectStateHolderVector()
        components = []
        systemOsh = self.AbapSystems[sysName][OSH]
        if self.AbapSysToInstalledSwComp.has_key(sysName):
            installedSwCompNames = self.AbapSysToInstalledSwComp[sysName]
        else:
            return vector
        for installedSwCompName in installedSwCompNames:
            if self.InstalledSwComps.has_key(installedSwCompName):
                installedSwCompsInstance = self.InstalledSwComps[installedSwCompName]
                name = installedSwCompsInstance.getProperty('Name').getValue()
                if self.InstalledToSwComponent.has_key(name):
                    swComponentName = self.InstalledToSwComponent[name][0]
                    if self.SwComponents.has_key(swComponentName):
                        swComponentInstance = self.SwComponents[swComponentName]
                        name = swComponentInstance.getProperty('Name').getValue()
                        version = swComponentInstance.getProperty('Version').getValue()
                        description = swComponentInstance.getProperty('Description').getValue()
                        type = swComponentInstance.getProperty('Type').getValue()
                        versionInfo = sap.VersionInfo(version)
                        component = SoftwareComponent(name, type, description, versionInfo)
                        components.append(component)

        if reportCmpAsConfig:
            vector.addAll(sap_abap.reportSoftwareCmpsAsConfigFile(components, systemOsh))
        else:
            vector.addAll(sap_abap.reportSoftwareCmpsAsCis(components, systemOsh))
        return vector

    def getClients(self, sysName):
        vector = ObjectStateHolderVector()
        if self.SysToClient.has_key(sysName):
            clientNames = self.SysToClient[sysName]
        else:
            return vector
        systemOsh = self.AbapSystems[sysName][OSH]
        for clientName in clientNames:
            if self.AbapClients.has_key(clientName):
                instance = self.AbapClients[clientName]
                name = instance.getProperty('ClientNumber').getValue()
                role = instance.getProperty('RoleInDevelopmentLandscape').getValue()
                description = instance.getProperty('Description').getValue()
                location = instance.getProperty('Location').getValue()
                # From cim class we get 'SAP reference', but in ucmdb we use 'SAP Reference'
                if role == 'SAP reference':
                    role = 'SAP Reference'

                client = sap.Client(name, role, description, location)
                reporter = sap.ClientReporter(sap.ClientBuilder())
                vector.add(reporter.report(client, systemOsh))
        return vector

    def getAbapDatabases(self, sysName):
        vector = ObjectStateHolderVector()
        systemOsh = self.AbapSystems[sysName][OSH]
        if self.AbapSysToDbSystem.has_key(sysName):
            dbSysNames = self.AbapSysToDbSystem[sysName]
        else:
            return vector
        for dbSysName in dbSysNames:
            if self.DbSystems.has_key(dbSysName):
                dbSystem = self.DbSystems[dbSysName]
                vector.addAll(_reportDatabase(dbSystem, self.DbSystemToDbInstance, self.DbInstances, self.DbInstanceToHost, self.Hosts, systemOsh))
        return vector

    def getAbapServers(self, sysName):
        vector = ObjectStateHolderVector()
        system = self.AbapSystems[sysName][OBJ]
        systemOsh = self.AbapSystems[sysName][OSH]
        if self.AbapSysToAs.has_key(sysName):
            asNames = self.AbapSysToAs[sysName]
        else:
            return vector
        for asName in asNames:
            if self.AbapServers.has_key(asName):
                serverInstance = self.AbapServers[asName]
                name = serverInstance.getProperty('Name').getValue()
                number = serverInstance.getProperty('Number').getValue()
                if self.AbapServerToHost.has_key(name):
                    hostName = self.AbapServerToHost[name][0]
                    hostInstance = self.Hosts[hostName]
                    ip = hostInstance.getProperty('IPAddress').getValue()
                    ips = _resolveIps(ip, hostName)
                    if ips:
                        hostReporter = sap.HostReporter(sap.HostBuilder())
                        hostOsh, hVector = hostReporter.reportHostWithIps(*ips)
                        hostOsh.setStringAttribute('name', hostName)
                        inst = sap.Instance('fake', number, hostName)
                        vector.addAll(hVector)
                        vector.addAll(_reportAbapServer(inst, system, False, hostOsh, systemOsh))
        return vector

    def getAbapCentralServices(self, sysName):
        vector = ObjectStateHolderVector()
        system = self.AbapSystems[sysName][OBJ]
        systemOsh = self.AbapSystems[sysName][OSH]
        if self.AbapSystemToCentralService.has_key(sysName):
            csNames = self.AbapSystemToCentralService[sysName]
        else:
            return vector
        for csName in csNames:
            if self.AbapCentralServices.has_key(csName):
                serverInstance = self.AbapCentralServices[csName]
                name = serverInstance.getProperty('Name').getValue()
                number = serverInstance.getProperty('ServiceInstanceID').getValue()
                if self.ServiceToHost.has_key(name):
                    hostName = self.ServiceToHost[name][0]
                    hostInstance = self.Hosts[hostName]
                    ip = hostInstance.getProperty('IPAddress').getValue()
                    ips = _resolveIps(ip, hostName)
                    if ips:
                        hostReporter = sap.HostReporter(sap.HostBuilder())
                        hostOsh, hVector = hostReporter.reportHostWithIps(*ips)
                        inst = sap.Instance('fake', number, hostName)
                        vector.addAll(hVector)
                        vector.addAll(_reportAbapServer(inst, system, True, hostOsh, systemOsh))
        return vector

    def parseLinks(self, relationships):
        relations = {}

        for relationship in relationships:
            if relationship.getPropertyValue('Dependent'):
                parentRef = relationship.getPropertyValue('Dependent')
                childRef = relationship.getPropertyValue('Antecedent')
            elif relationship.getPropertyValue('GroupComponent'):
                parentRef = relationship.getPropertyValue('GroupComponent')
                childRef = relationship.getPropertyValue('PartComponent')
            elif relationship.getPropertyValue('System'):
                parentRef = relationship.getPropertyValue('System')
                childRef = relationship.getPropertyValue('Software')
            else:
                return relations
            if not parentRef or not childRef:
                continue
            parentId = stringClean(parentRef.getKeyValue('Name'))
            childId = stringClean(childRef.getKeyValue('Name'))
            relations.setdefault(parentId, [])
            relations[parentId].append(childId)

        return relations

def _resolveIps(ip, hostname):
    if not ip:
        resolver = dns_resolver.SocketDnsResolver()
        try:
            ips = resolver.resolve_ips(hostname)
        except dns_resolver.ResolveException, e:
            logger.debug("Failed to resolve %s. %s" % (hostname, str(e)))
            return None
    else:
        ips = [ip]
    return ips

def _dumpInstances(source):
    logger.debug('#########Begin of Dump##########')
    abapDumper = AbapInstanceDumper(source)
    abapDumper.dump()
    j2eeDumper = J2eeInstanceDumper(source)
    j2eeDumper.dump()
    logger.debug('##########End of Dump###########')


class AbapInstanceDumper():
    def __init__(self, source):
        self.Source = source
        self.Instances = {}
        self.ENTRY = 'AbapSystems'
        self.DiscoverSequence = {
            self.ENTRY: {
                'AbapSysToAsLinks': {
                    'AbapServers': {
                        'AbapServerToHostLinks': {
                            'Hosts': ''
                        }
                    }
                },

                'AbapSysToDbSystemLinks': {
                    'DbSystems': {
                        'DbSystemToDbInstanceLinks': {
                            'DbInstances': {
                                'DbInstanceToHostLinks': {
                                    'Hosts': ''
                                }
                            }
                        }
                    }
                },

                'SysToClientLinks': {
                    'AbapClients': ''
                },

                'AbapSysToInstalledSwCompLinks': {
                    'InstalledSwComps': {
                        'InstalledToSwComponentLinks': {
                            'SwComponents': ''
                        }
                    }
                },

                'AbapSystemToCentralServiceLinks': {
                    'AbapCentralServices': {
                        'ServiceToHostLinks': {
                            'Hosts': ''
                        }
                    }
                }
            }
        }
        for (key, value) in self.DiscoverSequence.items():
            self.initResultStruct(key, value)

    def initResultStruct(self, key, value):
        self.Instances[key] = []
        if isinstance(value, dict):
            for (subKey, subValue) in value.items():
                self.initResultStruct(subKey, subValue)

    def dump(self):
        self.collectInstances()
        import datetime
        startTime = datetime.datetime.now()
        for (name, instances) in self.Instances.items():
            if instances:
                className = instances[0].className
                startTime = startTime + datetime.timedelta(seconds=1)
                logger.debug('<CMD>[CDATA: getInstances_%s]</CMD>' % className)
                logger.debug('<RESULT>[CDATA: ')
                listOfInstanceStr = []
                for instance in instances:
                    listOfInstanceStr.append(instance.toString())
                strOfInstances = '\r\n'.join(listOfInstanceStr)
                logger.debug(strOfInstances)
                logger.debug(']</RESULT>')

    def collectInstances(self):
        self.Instances[self.ENTRY].extend(self.getEntryInstances())
        for instance in self.Instances[self.ENTRY]:
            name = instance.getProperty('Name').getValue()
            for (key, value) in self.DiscoverSequence[self.ENTRY].items():
                self.scanInstances(name, key, value)

    def scanInstances(self, name, key, value):
        ids = self.getInstancesByName(name, key)
        if ids and isinstance(ids, list):
            if isinstance(value, dict):
                for (subKey, subValue) in value.items():
                    for id in ids:
                        self.scanInstances(id, subKey, subValue)

    def getEntryInstances(self):
        systems = []
        i = 0
        for (name, instance) in getattr(self.Source, self.ENTRY).items():
            if i == 5:
                break
            systems.append(instance)
            i = i + 1
        return systems

    def getInstancesByName(self, id, instanceName):
        ids = []
        instances = getattr(self.Source, instanceName)
        try:
            for (name, instance) in instances.items():
                if id == name:
                    if self.Instances.has_key(instanceName):
                        self.Instances[instanceName].append(instance)
                        ids.append(id)
            return ids
        except:
            i = 0
            for instance in instances:
                if i == 10:
                    break
                if instance.getPropertyValue('Dependent'):
                    parentRef = instance.getPropertyValue('Dependent')
                    childRef = instance.getPropertyValue('Antecedent')
                elif instance.getPropertyValue('GroupComponent'):
                    parentRef = instance.getPropertyValue('GroupComponent')
                    childRef = instance.getPropertyValue('PartComponent')
                elif instance.getPropertyValue('System'):
                    parentRef = instance.getPropertyValue('System')
                    childRef = instance.getPropertyValue('Software')
                else:
                    continue
                if parentRef and childRef:
                    parentId = stringClean(parentRef.getKeyValue('Name'))
                    childId = stringClean(childRef.getKeyValue('Name'))
                    if id == parentId:
                        if self.Instances.has_key(instanceName):
                            self.Instances[instanceName].append(instance)
                            ids.append(childId)
                            i = i + 1
            return ids


class J2eeInstanceDumper(AbapInstanceDumper):
    def __init__(self, source):
        AbapInstanceDumper.__init__(self, source)
        self.Source = source
        self.Instances = {}
        self.ENTRY = 'J2eeSystems'
        self.DiscoverSequence = {
            self.ENTRY: {
                'J2eeSysToAsLinks': {
                    'J2eeServers': {
                        'J2eeServerToHostLinks': {
                            'Hosts': ''
                        }
                    }
                },

                'J2eeSysToDbSystemLinks': {
                    'DbSystems': {
                        'DbSystemToDbInstanceLinks': {
                            'DbInstances': {
                                'DbInstanceToHostLinks': {
                                    'Hosts': ''
                                }
                            }
                        }
                    }
                },

                'AbapSysToInstalledSwCompLinks': {
                    'InstalledSwComps': {
                        'InstalledToSwComponentLinks': {
                            'SwComponents': ''
                        }
                    }
                },

                'J2eeSystemToCentralServiceLinks': {
                    'AbapCentralServices': {
                        'ServiceToHostLinks': {
                            'Hosts': ''
                        }
                    }
                }
            }
        }
        for (key, value) in self.DiscoverSequence.items():
            self.initResultStruct(key, value)


def _isComponentsReportedAsConfigFile(framework):
    name = 'reportComponentsAsConfigFile'
    reportAsConfigFileEnabled = framework.getParameter(name)
    if reportAsConfigFileEnabled is None:
        return True
    return Boolean.parseBoolean(reportAsConfigFileEnabled)


def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    protocol = cim.Protocol.DISPLAY
    credentialsId = Framework.getDestinationAttribute('credentialsId')
    ipAddress = Framework.getDestinationAttribute('ip_address')

    reportCmpAsConfig = _isComponentsReportedAsConfigFile(Framework)

    solManNamespaces = sap_solman_discoverer_by_cim.getSolManNamespaces(Framework)
    if not solManNamespaces:
        msg = errormessages.makeErrorMessage(cim.Protocol.DISPLAY, "No SAP namespaces found")
        errobj = errorobject.createError(errorcodes.INTERNAL_ERROR_WITH_PROTOCOL_DETAILS, [cim.Protocol.DISPLAY, msg], msg)
        logger.reportErrorObject(errobj)
        return OSHVResult
    errorMessges = []
    for namespaceObject in solManNamespaces:
        client = None
        namespace = namespaceObject.getName()
        try:
            try:
                client = cim_discover.createClient(Framework, ipAddress, namespace, credentialsId)
                logger.debug('Connected to namespace "%s"' % namespace)

                solManDiscoverer = SapSolManDiscover(client, reportCmpAsConfig)

                OSHVResult.addAll(solManDiscoverer.discover())

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