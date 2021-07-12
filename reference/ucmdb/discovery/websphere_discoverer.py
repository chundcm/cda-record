#coding=utf-8
'''
Created on May 15, 2011

@author: vvitvitskiy
'''

from java.lang import Exception as JException, Boolean

import jee_discoverer
import jee_constants
import logger
import websphere
import jee
import jmx
import db
import file_system
import file_topology
import jms
import re
import netutils
import ip_addr
import string
import asm_websphere_discoverer
import websphere_discoverer
from jarray import array
from java.lang import String
from java.lang import Object
from java.util import Arrays
from javax.xml.xpath import XPathConstants
from java.io import BufferedReader;
from java.io import InputStreamReader;
from com.hp.ucmdb.discovery.library.communication.downloader.cfgfiles import GeneralSettingsConfigFile
from fptools import groupby, applyMapping, applySet, findFirst, curry, _, asIs, applyReverseMapping
from appilog.common.system.types.vectors import ObjectStateHolderVector

import fptools


PortTypeEnum = netutils.PortTypeEnum.merge(
    netutils._PortTypeEnum(
        WEBSPHERE=netutils._PortType('websphere'),
        WEBSPHERE_JMX=netutils._PortType('websphere_jmx')
    ))


class JvmDiscovererByShell(jee_discoverer.JvmDiscovererByShell):

    def parseJavaVersion(self, output):
        rawOutputLines = output.splitlines()
        # stip each line
        getStrippedLine = lambda x: x.strip()
        strippedLines = map(getStrippedLine, rawOutputLines)
        # filter empty lines
        isLineEmpty = lambda x: len(x)
        lines = filter(isLineEmpty, strippedLines)
        if len(lines) < 3:
            raise Exception( "Failed to parse java -version output")
        else:
            version = None
            name = None
            matchObj = re.search('java version \"(.+?)\"', lines[0])
            if matchObj:
                version = matchObj.group(1)
            name = lines[2]
            return version, name

    def getJVMInfo(self, javaCommand):
        ''' Get JVM info (version, vendor)
        @types: str -> jee.Jvm
        @command: java -version
        @raise Exception: Failed to get JVM information
        '''
        # "java.exe -version" command prints its output always to standard error stream,
        # instead of standard output.
        # This causes the result to be discarded.
        # A simple workaround is to redirect the output to standard output stream,
        # this can be done by sending the following command: "java.exe -version 2>&1"
        javaCommand = '%s -version 2>&1' % javaCommand
        output = self._getShell().execCmd(javaCommand)
        if self._getShell().getLastCmdReturnCode() != 0:
            raise Exception( "Failed to get JVM information. %s" % output)
        vendor = None
        javaVersion, name = self.parseJavaVersion(output)
        if name.lower().find('ibm') != -1:
            vendor = 'IBM Corporation'
        elif name.lower().find('openjdk ') != -1:
            vendor = 'OpenJDK'
        else:
            vendor = 'Oracle'
        jvm = jee.Jvm(name or 'jvm')
        jvm.javaVersion = javaVersion
        jvm.javaVendor = vendor
        return jvm


class ServerDiscovererByJmx(jee_discoverer.HasJmxProvider):

    def getDmgrName(self):
        dmgrName = None
        query = jmx.QueryByPattern('*:type', 'Server')
        query.patternPart('processType', 'DeploymentManager')
        query.addAttributes('serverVersion', 'serverVendor', 'platformName', 'platformVersion', 'clusterName', 'services')
        for item in self._getProvider().execute(query):
            server = self.__parseServerProperties(item)
            if server:
                dmgrName = server.getName()
 
        logger.debug('dmgr process name: ', dmgrName)
 
        return dmgrName
    
    def discoverServersInDomain(self):
        '''@types: -> jee.Domain
        @raise AccessDeniedException: Access is denied
        @raise Exception: Query execution failed
        '''
        cellName = None
        nodeByName = {}
 
        dmgrName = self.getDmgrName()
 
        for server in self.findServers():
            logger.debug('server: ', server.getName())
            role = server.getRole(websphere.ServerRole)
            cellName = role.cellName
            try:
                fullVersion = self.__findFullServerVersion(server.getObjectName())
                if fullVersion:
                    # Application Version Description = fullVersion
                    # make sure the version = short version, like '8.5.5.10', avoid to exceed the max length of Version
                    server.versionDescription = fullVersion
            except (Exception, JException):
                logger.warnException("Failed to get full version for %s" % server)
            try:
                server.jvm = self.getJVM(server)
            except (Exception, JException):
                logger.warnException("Failed to get JVM for %s" % server)
            node = nodeByName.setdefault(server.nodeName, jee.Node(server.nodeName))

            #trying to find config file for server
            logger.debug("trying to find config file for server: ", server)
            for filename in jee_constants.SERVERCONFIGFILES:
 

                filePath=None
                fileContent=''
                if server.getName()==dmgrName:

                    filePath = 'cells/' + cellName + '/nodes/'+ server.nodeName +"/"+filename
                else:
 

                    if filename == 'serverindex.xml':
                        filePath = 'cells/' + cellName + '/nodes/'+ server.nodeName + "/"+filename
                    else:
                        filePath = 'cells/' + cellName + '/nodes/'+ server.nodeName + "/servers/" + server.getName() + "/"+filename

                try:
                    fileContent = getFileContent(self._getProvider(), filePath, dmgrName)
                except:
                    logger.debug('Document not found: ', filePath)
  
                if fileContent != '':
                    configFile = jee.createXmlConfigFileByContent(filename, fileContent)
                    server.addConfigFile(configFile)
  
                server_index_config = server.getConfigFile('serverindex.xml')
                if server_index_config:
                    parser = jee_discoverer.BaseXmlParser()
                    config_root = parser._getRootElement(server_index_config.content)
                    host_name = config_root.getAttributeValue('hostName')
                    if host_name:
                        server.hostname = host_name
 
            node.addServer(server)
 
        if not cellName:
            raise ValueError("Failed to discover domain topology")
        domain = jee.Domain(cellName)
        for node in nodeByName.values():
            domain.addNode(node)
        logger.debug("find cell: ", cellName)
        logger.debug("trying to find config file for cell: ", cellName)

        for filename in jee_constants.CELLCONFIGFILES:
            filePath = 'cells\\' + cellName + '\\'+ filename
            try:
                logger.debug(filePath)
 
                fileContent = getFileContent(self._getProvider(), filePath, dmgrName)
                logger.debug('fileContent: ', fileContent)
                configFile = jee.createXmlConfigFileByContent(filename, fileContent)
                domain.addConfigFile(configFile)
            except:
                logger.debug('Document not found: ', filePath)
        return domain

    def getJVM(self, server):
        '''
        @see http://publib.boulder.ibm.com/infocenter/wasinfo/v6r1/index.jsp?topic=/com.ibm.websphere.javadoc.doc/public_html/mbeandocs/index.html
        @types: jee.Server -> jee.Jvm
        @raise jmx.AccessDeniedException:
        @raise jmx.ClientException:
        @raise jmx.NoItemsFound:
        '''
        query = jmx.QueryByPattern('*:type', 'JVM')
        query.patternPart('node', server.nodeName)
        query.patternPart('process', server.getName())
        query.addAttributes('heapSize', 'freeMemory', 'maxMemory',
                            'javaVendor', 'javaVersion')
        for item in self._getProvider().execute(query):
            objectName = item.ObjectName
            vmInfo = None
            vmInfoProperty = self._getSystemPropertyValue(objectName,
                                                          'java.vm.info')
            getFirstLine = lambda x: x.splitlines()[0]
            addTrailingBracket = lambda x: x[-1] == ')' and x or '%s)' % x
            if vmInfoProperty:
                # take just 1st line and close bracket if needed
                vmInfo = addTrailingBracket(getFirstLine(vmInfoProperty))
            vmName = self._getSystemPropertyValue(objectName, 'java.vm.name')
            vmVersion = self._getSystemPropertyValue(objectName, 'java.vm.version')
            osType = self._getSystemPropertyValue(objectName, 'os.name')
            osVersion = self._getSystemPropertyValue(objectName, 'os.version')
            jvmName = '%s (build %s, %s' % (vmName, vmVersion, vmInfo)
            jvm = jee.Jvm(jvmName)
            jvm.setObjectName(objectName)
            jvm.javaVendor = item.javaVendor
            jvm.javaVersion = item.javaVersion
            jvm.heapSizeInBytes.set(item.heapSize)
            jvm.freeMemoryInBytes.set(item.freeMemory)
            jvm.maxHeapSizeInBytes.set(item.maxMemory)
            jvm.osType = osType
            jvm.osVersion = osVersion
            return jvm
        raise jmx.NoItemsFound()

    def _getSystemPropertyValue(self, objectName, name):
        jmxProvider = self._getProvider()
        paramType = array(["java.lang.String"], String)
        return jmxProvider.invokeMBeanMethod(objectName, 'getProperty',
                                             paramType,
                                             array([name], Object))

    def findServers(self):
        ''' Get list of servers with ServerRole role set
        @types: -> list(jee.Server)
        @raise jmx.AccessDeniedException:
        @raise jmx.ClientException:
        '''
        clusterNameByMemberMbeanId = {}
        try:
            clusterNameByMemberMbeanId.update(self.getClusterNameByMemberMbeanId())
        except:
            logger.debug('Failed to get cluster members by invoking method')
        servers = []
        query = jmx.QueryByPattern('*:type', 'Server')
        query.addAttributes('serverVersion', 'serverVendor',
                'platformName', 'platformVersion', 'clusterName', 'services')
        for item in self._getProvider().execute(query):
            # fill other server attributes
            server = self.__parseServerProperties(item)
            if server:
                objectName = jmx.restoreObjectName(server.getObjectName())
                serverMbeanId = objectName.getKeyProperty('mbeanIdentifier')
                clusterName = clusterNameByMemberMbeanId.get(serverMbeanId)
                if clusterName and not server.getRole(jee.ClusterMemberServerRole):
                    server.addRole( jee.ClusterMemberServerRole(clusterName) )
                servers.append(server)
        return servers

    def __findFullServerVersion(self, objectName):
        ''' Get full server version by invoking of WebSphere
            AdminClient getVersionsForAllProducts method
        @types: str -> str?
        '''
        productVersions = self._getProvider().invokeMBeanMethod(objectName,
                                            'getVersionsForAllProducts', [], [])
        if productVersions:
            # take the 1st server which name contains 'IBM WebSphere Application Server'
            # in V9.0, the 1st server product could be not a WAS, this logic can filter non-WAS products:
            for pv in productVersions:
                serverVersionXml = Arrays.toString(pv)
                parser = ProductInfoParser(loadExternalDtd=0)
                productInfo = parser.parseProductConfig(serverVersionXml)
                if productInfo and "IBM WebSphere Application Server" in productInfo.name:
                    return ', '.join((productInfo.name, productInfo.version))
                else:
                    continue

    def findClusters(self):
        r''' Find available clusters
        @types: -> list(jee.Cluster)'''
        clusters = []
        query = jmx.QueryByPattern('*:type', 'Cluster')
        query.addAttributes('Name')
        for item in self._getProvider().execute(query):
            clusters.append(jee.createNamedJmxObject(item.ObjectName, jee.Cluster))
        return clusters

    def getClusterNameByMemberMbeanId(self):
        clusterNameByMemberMbeanId = {}
        query = jmx.QueryByPattern('*:type', 'Cluster')
        query.addAttributes('ObjectName')
        jmxProvider = self._getProvider()
        for item in jmxProvider.execute(query):
            clusterObjectName = item.ObjectName
            objectName = jmx.restoreObjectName(clusterObjectName)
            clusterName = objectName.getKeyProperty('name')
            members = jmxProvider.invokeMBeanMethod(clusterObjectName,
                                                    'getClusterMembers', [], [])
            for member in members:
                memberObjectName = None
                try:
                    memberObjectName = member.memberObjectName
                except:
                    logger.debug("Failed to find Cluster Member ObjectName")
                    continue
                if memberObjectName:
                    memberMbeanId = None
                    try:
                        memberMbeanId = memberObjectName.getKeyProperty('mbeanIdentifier')
                    except:
                        logger.debug("Failed to find Cluster Member MBean Identifier")
                        continue
                    if memberMbeanId:
                        clusterNameByMemberMbeanId[memberMbeanId] = clusterName
                    else:
                        logger.debug("Cluster Member Identifier was not found")
                else:
                    logger.debug("Cluster Member ObjectName was not found")
        return clusterNameByMemberMbeanId

    def __parseServerProperties(self, item):
        '''@types: jmx.Provider._ResultItem -> jee.Server or None
        @return: server DO or None if there is no hostname
        '''
        objectName = jmx.restoreObjectName(item.ObjectName)
        name = objectName.getKeyProperty('name')
        cellName = objectName.getKeyProperty('cell')
        nodeName = objectName.getKeyProperty('node')
        hostname, port = None, None
        empty = {}
        for service in (item.services or empty).values():
            for connector in (service.get('connectors') or empty).values():
                connectorAddress = connector.get('SOAP_CONNECTOR_ADDRESS')
                if connectorAddress:
                    hostname = connectorAddress.get('host')
                    port = connectorAddress.get('port')
        hostname = hostname or self._extractHostnameFromNodeName(nodeName)
        server = jee.Server(name, hostname)
        server.setObjectName(item.ObjectName)
        # using short version, if discovery of the full version by invoking method failed
        server.version = objectName.getKeyProperty('version')
        server.nodeName = nodeName
        websphereRole = websphere.ServerRole(nodeName, cellName)
        websphereRole.setPort(port)
        websphereRole.platformName = item.platformName
        websphereRole.platformVersion = item.platformVersion
        websphereRole.serverVersionInfo  = item.serverVersion
        server.vendorName = item.serverVendor
        server.addRole(websphereRole)
        clusterName = item.clusterName
        if clusterName:
            logger.debug('Server %s is member of cluster %s' % (server, clusterName))
            server.addRole( jee.ClusterMemberServerRole(clusterName) )

        additionalRole = None
        serverType = objectName.getKeyProperty('processType')
        if 'DeploymentManager' == serverType:
            additionalRole = jee.AdminServerRole()
        elif 'NodeAgent' == serverType:
            additionalRole = jee.AgentServerRole()
        if additionalRole:
            server.addRole(additionalRole)
        return server

    def _extractHostnameFromNodeName(self, nodeName):
        ''' By default WebSphere node name contains host name that we try to extract
        @types: str -> str or None'''
        index = nodeName.rfind('Node')
        if index <= 0:
            index = nodeName.rfind('Cell') or -1
        if index != -1:
            return nodeName[:index]
        return None

#    JMS Resources discovery
#    ========================
#    Introduction
#    ------------------------

#    Resources are configured as JMS provider, its configuration has two kinds
#    of J2EE resources -- a JMS Connection Factory and a JMS Destination.
#    Connection Factory - used to create connections to the associated
#    JMS provider of JMS (queue|topic) destinations, for (point-to-point|publish/subscribe)
#    messaging. Is used for WebSphere MQ JMS provider only. IBM vendor
#
#    As domain model does not have class for connection factories for particular
#    destination they will be represented as generic Destination with MqServer
#    value set

#    * JMS Provider
#    ------------------------
#    Enables messaging based on the JMS. It provides J2EE connection factories
#    to create connections for JMS destinations.
#        Scopes( the level to which resource definition is visible): cell, node, server, cluster
#        Important Attributes:
#            name - The name by which the is known for administrative purposes
#            description
#            External provider URL - JMS provider URL for external JNDI lookups
#                ie, ldap://hostname.company.com/contextName

#    From the perspective of provider configuration we can deal with such resources

#    * Connection factories
#    ------------------------
#    Connections to the associated JMS provider for JMS destinations.
#    Also applications use it to connect to a service integration bus.
#        Important Attributes:
#            name, JMS provider, description, jndi name

#    * Queue connection factories (used to create
#            connections to the associated JMS provider of the JMS queue destinations,
#            for point-to-point messaging.
#    ------------------------
#        Important Attributes: provider name, jndi name, description, queue manager name
#            channel name, host and port values

#    * Topic connection factories
#    ------------------------
#    Similar to previous one but used to create connections to the associated
#    JMS provider of JMS topic destinations, for publish and subscribe messaging.

#    * Queues or Topics (JMS Destinations)
#    ------------------------
#        Important Attributes: provider name, jndi name, description, name

#    === Popular configurations ===
#    Version 6 may have three predefined messaging providers (MP):
#        * Default
#        * V5 default
#        * WebSphere MQ
#    Configuration of resources under each of them differs by additional attributes
#    like endpoint information, manager and queue names
#
#    Default MP
#    ------------------------
#    For the connection factories addional information appears about name of the
#    integration bus + endpoints to the bootstrap server.

#    WebSphere MQ MP
#    ------------------------
#    Both connection factories and destinations have information about manager
#    port and host


# === JMS resources discovery by JMX ===
class JmsSourceDiscovererByJmx(jee_discoverer.HasJmxProvider):
    r''' Discoverer for the JMS resources
    There are two way to get JMS resources using JMX
    * runtime data, registered MBeans such as JMSProvider and JMSDestination
    * configuration data, not-registered MBeans

    Flow
    ----
    Flow is constructed in such way to get as much data as possible about configuration,
    so first we try to read configuration data and if it fails - read information
    from runtime data.
    '''
    def discoverDatasources(self):
        r'@types: -> list[jms.Datasource]'
        return []

    def findJmsDestinations(self):
        r'''@types: -> list[jms.Destination]'''
        return map(self.__convertToJmsDestination, self._getProvider().
                    execute(jmx.QueryByPattern('*:type', 'JMSDestination').
                            addAttributes('ObjectName', 'name', 'Description',
                                          'jndiName', 'category'
                            )
                    )
        )

    def __convertToJmsDestination(self, item):
        r'@types: jmx.Provider._ResultItem -> jms.Destination'
        return jms.Destination()

    def __convertToJmsProvider(self, item):
        r'@types: jmx.Provider._ResultItem -> JmsProvider'
        return self.JmsProvider(item.ObjectName, item.name, item.description)

    def findJmsProviders(self):
        r'''@types: -> list[jms.Datasource]
        @resource-mbean: JMSProvider
        '''
        return_val = map(self.__convertToJmsProvider, self._getProvider().
                   execute(jmx.QueryByPattern('*:type', 'JMSProvider').
                           addAttributes('ObjectName', 'name', 'description')
                   )
        )
        return return_val



    class JmsProvider:
        r'Represents Websphere JMS Provider'
        def __init__(self, objectName, name, description):
            r'@types: str, str, str'
            self.objectName = objectName
            self.name = name
            self.description = description

# === JMS resources parsing from configuration files ===
class JmsResourcesParser:
    r'''Mixin for the JMS resources parsing from the websphere configuration

    @note: Relies on fact that passed data are classes from JDom XML parsing library
    '''

    def parseJmsProviderEl(self, el):
        r''' Dispatches parser calls on different resource type of JEE resoruces: factories and
        destinations. Dispatching is based on mapping resource resource type to parse method

        @types: org.jdom.Element -> jms.Datasource
        @resource-file: resources.xml
        '''
        # Mapping of supported (connection factory|destination) resource type to the parsing method
        # IMPORTANT: signature of method is 'org.jdom.Element, str -> jms.Destination'
        RESOURCE_TYPE_TO_PARSE_METHOD = {
          # Connection factories
          'resources.jms.mqseries:MQQueueConnectionFactory' : self.parseMqConnectionFactoryEl,
          'resources.jms.mqseries:MQTopicConnectionFactory' : self.parseMqConnectionFactoryEl,
          'resources.jms.mqseries:MQConnectionFactory'      : self.parseMqConnectionFactoryEl,

          'resources.jms.internalmessaging:WASTopicConnectionFactory' : self.parseWasConnectionFactoryEl,
          'resources.jms.internalmessaging:WASQueueConnectionFactory' : self.parseWasConnectionFactoryEl,
          # No need to process GenericJMSConnectionFactory, as there is no useful information
          # about provider (host, port) only external JNDI name

          # Destinations provided for messaging by the WebSphere MQ JMS provider
          'resources.jms.mqseries:MQTopic': self.parseMqTopicDestinationEl,
          'resources.jms.mqseries:MQQueue': self.parseMqQueueDestinationEl,

          'resources.jms.internalmessaging:WASTopic' : self.parseMqTopicDestinationEl,
          'resources.jms.internalmessaging:WASQueue' : self.parseMqQueueDestinationEl,

          'resources.jms:GenericJMSDestination' : self.parseGenericDestinationEl,

          'resources.j2c:J2CConnectionFactory':  self.parseJ2CConnectionFactory
        }

        logger.debug('---jms datasource name: ', el.getAttributeValue('name'))
        logger.debug('---jms datasource description: ', el.getAttributeValue('description'))
        datasource = jms.Datasource(
            el.getAttributeValue('name'),
            el.getAttributeValue('description')
        )
        logger.debug("-> %s" % datasource)
        elNs = el.getNamespace('xmi')
        for factoryEl in el.getChildren('factories'):
            resourceType = factoryEl.getAttributeValue('type', elNs)
            parseResourceMethod = RESOURCE_TYPE_TO_PARSE_METHOD.get(resourceType)
            if parseResourceMethod:
                map(datasource.addDestination,
                    filter(None, (parseResourceMethod(factoryEl),)))
            else:
                logger.debug("JMS Resource of type '%s' is not supported" % resourceType)
        for j2cAdminObjectEl in el.getChildren('j2cAdminObjects'):
            parseResourceMethod = self.parseJ2CAdminObjects
            if parseResourceMethod:
                map(datasource.addDestination,
                    filter(None, (parseResourceMethod(j2cAdminObjectEl),)))
            else:
                logger.debug("JMS Resource of type 'j2cAdminObjectEl' is not supported")
        for j2cActivationSpecEl in el.getChildren('j2cActivationSpec'):
            parseResourceMethod = self.parseJ2CActivationSpec
            if parseResourceMethod:
                map(datasource.addDestination,
                    filter(None, (parseResourceMethod(j2cActivationSpecEl),)))
            else:
                logger.debug("JMS Resource of type 'j2cActivationSpecEl' is not supported" )
        return datasource

    def parseJ2CConnectionFactory(self, el):
        connectionFactory = jms.ConnectionFactory(el.getAttributeValue('name'))
        connectionFactory.setJndiName(el.getAttributeValue('jndiName'))
        logger.debug("-- %s" % connectionFactory)
        return connectionFactory

    def parseJ2CAdminObjects(self, el):
        Destination = jms.Destination(el.getAttributeValue('name'))
        Destination.setJndiName(el.getAttributeValue('jndiName'))
        logger.debug("-- %s" % Destination)
        return Destination

    def parseJ2CActivationSpec(self, el):
        Destination = jms.Destination(el.getAttributeValue('name'))
        Destination.setJndiName(el.getAttributeValue('jndiName'))
        logger.debug("-- %s" % Destination)
        return Destination

    def parseMqConnectionFactoryEl(self, el):
        r''' Parsed connection factory is represented as generic destination
        @return: empty list, if host and port are not specified no need to created destination
        org.jdom.Element -> jms.Destination'''
        connectionFactory = jms.ConnectionFactory(el.getAttributeValue('name'))
        mqServer = self.__parseMqServerInResourceEl(el)
        if not mqServer:
            return None
        connectionFactory.setJndiName(el.getAttributeValue('jndiName'))
        connectionFactory.server = mqServer
        logger.debug("-- %s" % connectionFactory)
        return connectionFactory

    def parseWasConnectionFactoryEl(self, el):
        r''' Parsed connection factory is represented as generic destination
        @return: empty list, if host and port are not specified no need to created fake destination
        org.jdom.Element -> jms.Destination'''
        destination = jms.ConnectionFactory(el.getAttributeValue('name'))
        #NOTE: this type of connection factories for queue and topic
        # has attribute 'node' which contains information different from
        # node where resource defined
        # nodeName = el.getAttributeValue('node')
        # From the documentation
        # The WebSphere node name of the administrative node where the
        # JMS server runs for this connection factory.
        destination.setJndiName(el.getAttributeValue('jndiName'))
        logger.debug("-- %s" % destination)
        return destination

    def parseMqQueueDestinationEl(self, el):
        r'@types: org.jdom.Element -> jms.Destination'
        return self._parseDestinationEl(el, jms.Queue)

    def parseMqTopicDestinationEl(self, el):
        r'@types: org.jdom.Element -> jms.Destination'
        return self._parseDestinationEl(el, jms.Topic)

    def parseGenericDestinationEl(self, el):
        r'@types: org.jdom.Element -> jms.Destination'
        elTypeToDestinationClass = {'queue' : jms.Queue,
                                    'topic' : jms.Topic}
        # destination type is characterized by attribute 'type'
        destinationClass = elTypeToDestinationClass.get(
                            str(el.getAttributeValue('type')).lower()
        )
        return (destinationClass and self._parseDestinationEl(el, destinationClass))

    def __parseMqServerInResourceEl(self, el):
        r''' Parse information about MQ server such as host and port
        @return: None, if host and port cannot be fetched
        @types: org.jdom.Element -> jms.MqServer or None
        '''
        # TODO: INCLUDE ENDPOINTS PARSING PROVIDED BY SOME PROVIDERS (DEFAULT AND V5)
        # [ [host_name] [ ":" [port_number] [ ":" chain_name] ] ]
        # If port_number is not specified, the default is 7276 ? may differ from version to version
        # If a value is not specified, the default is localhost.
        # channel name : If not specified, the default is BootstrapBasicMessaging.
        port = (el.getAttributeValue('port')
                or el.getAttributeValue('queueManagerPort'))
        host = (el.getAttributeValue('host')
                or el.getAttributeValue('queueManagerHost'))
        mqServer = None
        if port and host:
            mqServer = jms.MqServer(host, port)
            mqServer.vendorName = 'ibm_corp'
        return mqServer

    def _parseDestinationEl(self, el, destinationClass):
        r''' Parse destination name and JNDI name for the specified class
        @types: org.jdom.Element -> jms.Destination'''
        destination = destinationClass(
               el.getAttributeValue('name'),
               el.getAttributeValue('description')
        )
        destination.setJndiName(el.getAttributeValue('jndiName'))
        destination.server = self.__parseMqServerInResourceEl(el)
        logger.debug("-- Parsed %s" % destination)
        return destination


class DatasourceDiscovererByJmx(jee_discoverer.HasJmxProvider):

    def findJdbcProviders(self):
        '''@types: jee.Server -> list(JdbcProvider)
        @raise AccessDeniedException
        @raise ClientException
        '''
        query = jmx.QueryByPattern('*:type', 'JDBCProvider').addAttributes('ObjectName')
        providers = []
        for providerItem in self._getProvider().execute(query):
            objectNameString = providerItem.ObjectName
            objectName = jmx.restoreObjectName(objectNameString)
            name = objectName.getKeyProperty( 'name' )
            providers.append( websphere.JdbcProvider(name) )
        return providers

    def discoveryDatasources(self):
        '''@types: jee.Server -> list(jee.Datasource)
        @raise AccessDeniedException
        @raise ClientException
        '''
        datasources = []
        providers = []
        try:
            providers = self.findJdbcProviders()
            logger.debug("Found %s jdbc providers" % len(providers))
        except (Exception, JException):
            logger.warnException("Failed to find JDBC Providers")
        processedProviderByName = {}
        for jdbcProvider in providers:
            if not processedProviderByName.get(jdbcProvider.getName()):
                try:
                    sources = self.findDatasources(jdbcProvider)
                    for dbSource in sources:
                        datasources.append(dbSource)
                    processedProviderByName[jdbcProvider.getName()] = jdbcProvider
                except (Exception, JException):
                    logger.warnException("Failed to find datasources for %s" % jdbcProvider)
        return datasources

    def __extractValue(self, propertySetMap, propertyName):
        r'@types: dict(str, dict) -> str'
        value = None
        if propertySetMap:
            resourcePropertiesMap = propertySetMap.get('resourceProperties') or {}
            for property in resourcePropertiesMap.values():
                name = property.get('name')
                if name == propertyName:
                    value = property.get('value')
                    break
        return value

    def findDatasources(self, jdbcProvider):
        '''@types: jee.Server, websphere.JdbcProvider -> list(jee.Datasource)
        @raise AccessDeniedException
        @raise ClientException
        '''
        query = jmx.QueryByPattern('*:type', 'DataSource')
        query.addAttributes('serverName', 'databaseName', 'portNumber', 'URL', 'jndiName', 'connectionPool', 'propertySet')
        query.patternPart('JDBCProvider', jdbcProvider.getName())
        datasources = []
        for item in self._getProvider().execute(query):
            # process data source properties
            datasource = jee.createNamedJmxObject(item.ObjectName, jee.Datasource)
            datasource.description = jdbcProvider.getName()
            datasource.setJndiName( item.jndiName )
            datasource.url = (item.URL or self.__extractValue(item.propertySet, 'URL'))
            databaseName = (item.databaseName or self.__extractValue(item.propertySet, 'databaseName'))

            # normalize database name
            if databaseName:
                lastSlash = str(databaseName).rfind('/')
                if  lastSlash != -1:
                    databaseName = databaseName[lastSlash+1:len(databaseName)]
                # datasource also has a weak reference (by name) on the database
                datasource.databaseName = databaseName

            # process connection pool data
            if item.connectionPool:
                maxConnections = item.connectionPool.get('maxConnections')
                initialCapacity = item.connectionPool.get('minConnections')
                datasource.maxCapacity.set(maxConnections)
                datasource.initialCapacity.set(initialCapacity)
                datasource.testOnRelease = Boolean.valueOf(item.connectionPool.get('testConnection'))

            # process data base server properties
            serverName = (item.serverName or self.__extractValue(item.propertySet, 'serverName'))
            portNumber = (item.portNumber or self.__extractValue(item.propertySet, 'portNumber'))
            databases = ()
            if serverName:
                databases = databaseName and (db.Database(databaseName),) or ()
                server = db.DatabaseServer(address = serverName, port = portNumber, databases = databases)
                datasource.setServer(server)
            datasources.append(datasource)
        return datasources

def _createModuleByObjectName(objectNameStr):
    module = None
    objectName = jmx.restoreObjectName(objectNameStr)
    moduleType = objectName.getKeyProperty ('type')
    logger.debug('moduleType:', moduleType)
    name = objectName.getKeyProperty ('name')
    if moduleType:
        if moduleType.lower().count('ejbmodule'):
            module = jee.EjbModule(name)
        elif moduleType.lower().count('webmodule'):
            module = jee.WebModule(name)
    if module:
        module.setObjectName(objectNameStr)
    return module

class ApplicationDiscovererByJmx(jee_discoverer.HasJmxProvider,
                                     jee_discoverer.BaseApplicationDiscoverer,):
    def __init__(self, provider, descriptorParser, cellName):
        jee_discoverer.HasJmxProvider.__init__(self, provider)
        jee_discoverer.BaseApplicationDiscoverer.__init__(self, descriptorParser)
        self.cellName = cellName

    def discoverApplications(self):
        '''@types: -> list(jee.Application)
        @raise AccessDeniedException
        @raise ClientException
        '''
        applications = []
        try:
            applications = self.findApplications()
            logger.debug("Found %s applications" % len(applications))
        except (Exception, JException):
            logger.warnException("Failed to discover applications")
        return applications

    def discoverModulesForApp(self, app, jndiNameToName=None):
        r''' Using application descriptor and object names of modules we strive
        to gather as much as possible of information. From descriptor we can get
        context-root for web modules. Using module ObjectName it is possible to
        get all other information, including descriptors. Module descriptor can
        be parsed too and fetched information about entries (servlets, ejb modules)
        and of course resources.

        @types: websphere.Application -> list(jee.Module)'''
        logger.info("Discover modules for %s" % app)
        modules = []
        moduleWithContextRootByName = {}
        # parse application descriptor to get context root information for web modules
        try:
            jeeDescriptors, other = self._splitDescriptorFilesByType(app.getConfigFiles(), 'application.xml')
            if jeeDescriptors:
                descrFile = jeeDescriptors[0]
                appDescriptor = self._getDescriptorParser().parseApplicationDescriptor(descrFile.content, app)
                for module in appDescriptor.getWebModules():
                    moduleWithContextRootByName[module.getName()] = module
        except (Exception, JException):
            logger.debugException("Failed to parse application descriptor")
        # get more detailed module information for each ObjectName in application
        for module in app.getModules():
            logger.debug('found module: ', module)
            logger.debug('trying to find config file for module: ', module)
            configFiles = []
            if isinstance(module, jee.WebModule):
                configFiles = jee_constants.WARCONFIGFILES
            elif isinstance(module, jee.EjbModule):
                configFiles = jee_constants.JARCONFIGFILES
            for filename in configFiles:
                filePath = 'cells\\' + self.cellName + '\\applications\\'+ app.getName() + '.ear\\deployments\\' + app.getName()+'\\'+module.getName() +'\\META-INF\\'+filename
                if isinstance(module, jee.WebModule):
                    filePath = 'cells\\' + self.cellName + '\\applications\\'+ app.getName() + '.ear\\deployments\\' + app.getName()+'\\'+module.getName() +'\\WEB-INF\\'+filename
                logger.debug('file path:', filePath)
                try:
                    fileContent = getFileContent(self._getProvider(), filePath)
                    logger.debug('fileContent: ', fileContent)
                    configFile = jee.createXmlConfigFileByContent(filename, fileContent)
                    module.addConfigFile(configFile)
                except:
                    logger.debug('Document not found: ', filePath)

            # update web modules with context root discovered in another way
            moduleWithContextRoot = moduleWithContextRootByName.get(module.getName())
            if moduleWithContextRoot:
                module.contextRoot = moduleWithContextRoot.contextRoot
                module.setJndiName(module.contextRoot)
            try:
                configFile = self.getDescriptorForModule(module)
            except (Exception, JException):
                logger.warnException("Failed to get descriptor for %s" % module)
            else:
                # parse module descriptor depending on its type (WEB|EJB)
                descriptor = None
                if isinstance(module, jee.WebModule):
                    try:
                        descriptor = self._getDescriptorParser().parseWebModuleDescriptor(configFile.content, module)
                    except:
                        logger.warnException('Failed to get Web module descriptor')
                    else:
                        for servlet in descriptor.getServlets():
                            module.addEntry(servlet)
                elif isinstance(module, jee.EjbModule):
                    try:
                        descriptor = self._getDescriptorParser().parseEjbModuleDescriptor(configFile.content, module)
                    except:
                        logger.warnException('Failed to get Ejb module descriptor')
                    else:
                        for bean in descriptor.getBeans():
                            module.addEntry(bean)

                    files = filter(lambda file: re.match(asm_websphere_discoverer.WebsphereJndiBindingParser.EJB_BINDING_DESCRIPTOR_PATTERN, file.getName(), re.IGNORECASE),
                           module.getConfigFiles())
                    if files:
                        try:
                            logger.debug('Parsing JNDI binding descriptor file %s for %s' % (files[0].name, module))
                            jndiBindingParser = asm_websphere_discoverer.WebsphereJndiBindingParser()
                            bindingDescriptor = jndiBindingParser.parseEjbModuleBindingDescriptor(files[0].content)
                            if bindingDescriptor:
                                for entry in module.getEntrieRefs():
                                    jndiName = bindingDescriptor.getJndiName(entry)
                                    if jndiName:
                                        entry.setJndiName(jndiName)
                                        if jndiNameToName and (jndiName in jndiNameToName.keys()):
                                            entry.setNameInNamespace(jndiNameToName[jndiName])
                                            logger.debug('Found object name for %s:%s' % (repr(entry), jndiNameToName[jndiName]))
                                        logger.debug('Found JNDI name for %s:%s' % (repr(entry), jndiName))
                        except (Exception, JException):
                            logger.warnException('Failed to process EJB binding for: ', module)
                                # process runtime descriptor files
                if descriptor:
                    if descriptor.getJndiName():
                        module.setJndiName(descriptor.getJndiName())

                    for file in module.getConfigFiles():
                        try:
                            if file.getName() == module.getWebServiceDescriptorName():
                                descriptor = self._getDescriptorParser().parseWebServiceDescriptor(descriptor, file.content, module)
                                module.addWebServices(descriptor.getWebServices())
                        except (Exception, JException):
                            logger.debug("Failed to load content for runtime descriptor: %s" % file.name)
            modules.append(module)
        return modules

    def getDescriptorForModule(self, module):
        '''@types: jee.Module -> jee.ConfigFile
        @raise ValueError: Module ObjectName is not specified
        @raise ValueError: Failed to get descriptor
        @raise AccessDeniedException
        @raise ClientException
        '''
        objectNameStr = module.getObjectName()
        if not objectNameStr:
            raise ValueError("Module ObjectName is not specified")
        query = jmx.QueryByName(objectNameStr).addAttributes('deploymentDescriptor')
        for item in self._getProvider().execute(query):
            if item.deploymentDescriptor:
                return jee.createDescriptorByContent(item.deploymentDescriptor, module)
        raise ValueError("Failed to get descriptor")

    def findApplications(self):
        '''@types: -> list(jee.Application)
        @raise AccessDeniedException
        @raise ClientException
        '''
        query = jmx.QueryByPattern('*:type', 'Application')
        query.addAttributes('modules', 'deploymentDescriptor')
        applications = []
        for item in self._getProvider().execute(query):
            application = jee.createNamedJmxObject(item.ObjectName, jee.Application)
            for module in map(_createModuleByObjectName, item.modules.split(';')):
                application.addModule(module)
            if item.deploymentDescriptor:
                configFile = jee.createXmlConfigFileByContent('application.xml', item.deploymentDescriptor)
                try:
                    logger.info("Get JEE deployment descriptor content")
                    descriptor = self._getDescriptorParser().parseApplicationDescriptor(configFile.content, application)
                except (Exception, JException), exc:
                    logger.warnException("Failed to parse application.xml. %s" % exc)
                else:
                    jndiName = descriptor.getJndiName()
                    if not jndiName:
                        jndiName = jee_constants.ModuleType.EAR.getSimpleName(application.getName())
                    application.setJndiName(jndiName)
                application.addConfigFile(configFile)

            #trying to find config file for application
            logger.debug("trying to find config file for application: ", application)
            for filename in jee_constants.EARCONFIGFILES:
                filePath = 'cells\\' + self.cellName + '\\applications\\'+ application.getName() + '.ear\\deployments\\' + application.getName()+"\\META-INF\\"+filename
                logger.debug('file path:', filePath)
                try:
                    fileContent = getFileContent(self._getProvider(), filePath)
                    logger.debug('fileContent: ', fileContent)
                    configFile = jee.createXmlConfigFileByContent(filename, fileContent)
                    application.addConfigFile(configFile)
                except:
                    logger.debug('Document not found: ', filePath)

            applications.append(application)
        return applications


class ServerRuntime(jee_discoverer.ServerRuntime):
    def __init__(self, commandLine):
        r'@types: str'
        commandLineDescriptor = jee.JvmCommandLineDescriptor(commandLine)
        jee_discoverer.ServerRuntime.__init__(self, commandLineDescriptor)

    def findInstallRootDirPath(self):
        r'@types: -> str or None'
        return self._getCommandLineDescriptor().extractProperty('was\.install\.root')

    def __getServerParameters(self):
        ''' Returns <CONFIG_DIR> <CELL_NAME> <NODE_NAME> <SERVER_NAME>
        -> tuple(str) or None
        '''
        commandLine = self.getCommandLine()
         # cmdLine: com.ibm.ws.runtime.WsServer "C:\Program Files\IBM\WebSphere\AppServer/profiles/SecSrv04\config" ddm-rnd-yg-vm4Node01Cell ddm-rnd-yg-vm4Node04 server1
        m  = re.search(r'com\.ibm\.ws\.runtime\.WsServer\s+"?([^"]*)"?\s+([^\s]*)\s+([^\s]*)\s+([^\s]*)\s*', commandLine)
        return m and m.groups()

    def getConfigDirPath(self):
        r'''-> str'''
        params = self.__getServerParameters()
        return params and params[0]

    def getCellName(self):
        r'''-> str'''
        params = self.__getServerParameters()
        return params and params[1]

    def getNodeName(self):
        r'''-> str'''
        params = self.__getServerParameters()
        return params and params[2]

    def getServerName(self):
        r'''-> str'''
        params = self.__getServerParameters()
        return params and params[3]


class ProductInformation:
    def __init__(self, name, version, buildInfo=None):
        r'@types: str, str, str?'
        if not name:
            raise ValueError("Product name is not specified")
        self.name = name
        if not version:
            raise ValueError("Product version is not specified")
        self.version = version
        self.buildInfo = buildInfo

    def __repr__(self):
        return "ProductInformation(%s, %s, %s)" % (self.name, self.version, self.buildInfo)


class ClusterConfiguration:
    def __init__(self, cluster, members):
        r'@types: jee.Cluster, list(Any)'
        self.cluster = cluster
        self.__members = []
        if members:
            self.__members.extend(members)

    def getMembers(self):
        r'@types: -> list(Any)'
        return self.__members[:]


class ProductInfoParser(jee_discoverer.BaseXmlParser):
    def __init__(self, loadExternalDtd):
        jee_discoverer.BaseXmlParser.__init__(self, loadExternalDtd)

    def parseProductConfig(self, content):
        r'@types: str -> websphere.ProductInformation'
        contentWithoutComments = re.sub(r'<!.+?>', '', content)
        contentWithoutHeader = re.sub('<\?xml.+?>', '', contentWithoutComments)
        content = '<xml>' + contentWithoutHeader + '</xml>'
        document = self._buildDocumentForXpath(content, namespaceAware = 0)
        # as versionInfo.bat take 1st server:
        productNodeList = self._getXpath().evaluate('xml/product[1]', document, XPathConstants.NODESET)
        if productNodeList and productNodeList.getLength():
            # as versionInfo.bat takes 1st server
            productNode = productNodeList.item(0)
            name = productNode.getAttribute('name')
            version = self._getXpath().evaluate('version', productNode, XPathConstants.STRING)
            buildLevel = self._getXpath().evaluate('build-info/@level', productNode, XPathConstants.STRING)
            return ProductInformation(name, version, buildLevel)

class ResourceConfigDescriptor:
    def __init__(self):
        self.__jdbcDatasources = []
        self.__jmsDatasources = []

    def addJdbcDatasource(self, datasource):
        r'@types: jee.Datasource'
        if datasource:
            self.__jdbcDatasources.append(datasource)

    def getJdbcDatasources(self):
        r'@types: -> list( jee.Datasource )'
        return self.__jdbcDatasources

    def addJmsDatasource(self, datasource):
        r'@types: jms.Datasource'
        if datasource:
            self.__jmsDatasources.append(datasource)

    def getJmsDatasources(self):
        r'@types: -> list(jms.Datasource)'
        return self.__jmsDatasources[:]


class AppDeploymentDescriptor(jee.HasServers, jee.HasClusters):
    def __init__(self):
        self.__clusters = []
        jee.HasServers.__init__(self)
        jee.HasClusters.__init__(self)


class DescriptorParser(JmsResourcesParser, jee_discoverer.BaseXmlParser):

    def parseProfilesInRegistry(self, content):
        r'@types: str -> list(websphere.ServerProfile)'
        profiles = []
        profilesEl = self._getRootElement(content)
        profilesElNs = profilesEl.getNamespace()
        for profileEl in profilesEl.getChildren('profile', profilesElNs):
            profiles.append(websphere.ServerProfile(
                                            name = profileEl.getAttributeValue('name'),
                                            path = profileEl.getAttributeValue('path'),
                                            template = profileEl.getAttributeValue('template'))
                            )
        return profiles

    def parseCellConfig(self, content):
        r'''@types: str -> websphere.Cell
        @resource-file: cell.xml
        '''
        doc = self._buildDocumentForXpath(content, namespaceAware = 0)
        xpath = self._getXpath()
        return websphere.Cell(xpath.evaluate('//Cell/@name', doc),
                          xpath.evaluate('//Cell/@cellType', doc))

    def parseNodeConfig(self, content):
        r'''types: str -> jee.Node
        @resource-file: node.xml
        '''
        doc = self._buildDocumentForXpath(content, namespaceAware = 0)
        xpath = self._getXpath()
        return jee.Node(xpath.evaluate('//Node/@name', doc))

    def parseServersInServerIndex(self, content):
        r''' @types: str -> list(jee.Server)
        @resource-file: serverindex.xml
        '''
        if content:
            servers = []
            serverTypeParserByType = {
                                      'NODE_AGENT' : self._parseNodeAgentInIndex,
                                      'APPLICATION_SERVER' : self._parseApplicationServerInIndex,
                                      'WEB_SERVER' : self._parseWebServerInIndex,
                                      'DEPLOYMENT_MANAGER' : self._parseAdminServerInIndex,
#                                      'DEPLOYMENT_MANAGER': self._parseSoapEnabledServer,
#                                      'GENERIC_SERVER': self._parseSoapEnabledServer,
#                                      'PROXY_SERVER': self._parseSoapEnabledServer
                                      }
            document = self._buildDocumentForXpath(content, namespaceAware=0)
            xpath = self._getXpath()
            serverIndexPattern = '//*[starts-with(name(),"serverindex")]'
            serverIndexNodes = xpath.evaluate(serverIndexPattern, document,
                                              XPathConstants.NODESET)
            for serverIndex in range(0, serverIndexNodes.getLength()):
                serverIndexNode = serverIndexNodes.item(serverIndex)
                defaultHostname = serverIndexNode.getAttribute('hostName')
                serverEntryNodes = xpath.evaluate('serverEntries',
                                                  serverIndexNode,
                                                  XPathConstants.NODESET)
                for serverEntryIndex in range(0, serverEntryNodes.getLength()):
                    serverEntry = serverEntryNodes.item(serverEntryIndex)
                    serverType = serverEntry.getAttribute('serverType')
                    serverParserMethod = serverTypeParserByType.get(serverType)
                    if serverParserMethod:
                        server = serverParserMethod(serverEntry, defaultHostname)
                        if defaultHostname and not server.address:
                            server.address = defaultHostname
                        servers.append(server)
                    else:
                        logger.debug("Server of type '%s' is skipped " % serverType)
        else:
            logger.debug('Failed retrieve content of serverindex.xml. Please check file location and permissions')
        return servers


    def _parseServerEntries(self, serverEntry, defaultServerName):
        r'@types: XPathConstants.NODESET -> jee.Server'
        server = jee.Server(serverEntry.getAttribute('serverName'))
        endpoints = self.__parseServerEndpoints(serverEntry, defaultServerName)
        if endpoints:
            role = server.addDefaultRole(websphere.RoleWithEndpoints())
            fptools.each(role.addEndpoint, endpoints)
        applications = self.__parseApplications(serverEntry)
        if applications:
            role = server.addDefaultRole(jee.ApplicationServerRole())
            fptools.each(role.addApplication, applications)
        return server

    def __parseServerEndpoints(self, serverEntry, defaultServerName):
        endpoints = []
        xpath = self._getXpath()
        specEndpointsNodes = xpath.evaluate('specialEndpoints', serverEntry,
                                            XPathConstants.NODESET)
        for endpointIndex in range(0, specEndpointsNodes.getLength()):
            specEndpointNode = specEndpointsNodes.item(endpointIndex)
            endpointNode = xpath.evaluate('endPoint', specEndpointNode,
                                          XPathConstants.NODE)
            if not endpointNode: #  skip if inner <endpoint> tag is absent
                continue
            host = endpointNode.getAttribute('host')
            if host == '*': #  set default hostName in case host is asterisk
                host = defaultServerName
            try:
                ip_address = ip_addr.IPAddress(host)
                if ip_address.is_multicast or ip_address.is_loopback:
                    logger.debug('Ignore multi-cast or loop-back ip server endpoint:%s' % host)
                    continue
            except:
                pass
            port = endpointNode.getAttribute('port')
            if port == '0': #  skip random-generated port endpoints
                continue
            additionalPortType = None
            endpointName = specEndpointNode.getAttribute('endPointName')
            if endpointName == 'SOAP_CONNECTOR_ADDRESS':
                additionalPortType = PortTypeEnum.WEBSPHERE_JMX
            elif (endpointName.startswith('WC_defaulthost') or
                  endpointName.startswith('WC_adminhost')):
                        additionalPortType = (endpointName.endswith('_secure')
                                              and PortTypeEnum.HTTPS
                                              or PortTypeEnum.HTTP)
            try:
                endpoints.append(netutils.createTcpEndpoint(host, port,
                                                        PortTypeEnum.WEBSPHERE))
                if additionalPortType:
                    endpoints.append(netutils.createTcpEndpoint(host, port,
                                                additionalPortType.getName()))
            except Exception:
                logger.warn("Failed to create endpoint (%s, %s)" % (host, port))
        return endpoints

    def __parseApplications(self, serverEntry):
        applications = []
        xpath = self._getXpath()
        applicationsNodes = xpath.evaluate('deployedApplications', serverEntry,
                                           XPathConstants.NODESET)
        for applicationIndex in range(0, applicationsNodes.getLength()):
            applicationNode = applicationsNodes.item(applicationIndex)
            relativePath = applicationNode.getTextContent()
            name = relativePath.split('/')[-1]
            application = jee.EarApplication(name)
            application.fullPath = relativePath
            applications.append(application)
        return applications

    def _parseApplicationServerInIndex(self, serverEntriesEl, defaultServerName):
        r'@types: org.jdom.Element -> jee.Server'
        server = self._parseServerEntries(serverEntriesEl, defaultServerName)
        return server

    def _parseAdminServerInIndex(self, serverEntriesEl, defaultServerName):
        r'@types: org.jdom.Element -> jee.Server'
        server = self._parseServerEntries(serverEntriesEl, defaultServerName)
        server.addRole(jee.AdminServerRole())
        return server

    def _parseNodeAgentInIndex(self, serverEntriesEl, defaultServerName):
        r'@types: org.jdom.Element -> jee.Server'
        server = self._parseServerEntries(serverEntriesEl, defaultServerName)
        server.addRole(jee.AgentServerRole())
        return server

    def _parseWebServerInIndex(self, serverEntriesEl, defaultServerName):
        r'@types: org.jdom.Element -> jee.Server'
        server = self._parseServerEntries(serverEntriesEl, defaultServerName)
        return server

    def parseClusterConfig(self, content):
        r'''@types: str -> websphere_discoverer.ClusterConfiguration
        @resource-file: cluster.xml
        '''
        clusterEl = self._getRootElement(content)
        cluster = jee.Cluster(clusterEl.getAttributeValue('name'))
        members = []
        for memberEl in clusterEl.getChildren('members'):
            server = jee.Server(memberEl.getAttributeValue('memberName'))
            server.nodeName = memberEl.getAttributeValue('nodeName')
            members.append(server)
        return ClusterConfiguration(cluster, members)

    def __parseJdbcDatasources(self, resourceEl):
        r'@types: org.jdom.Element -> list(jee.Datasource)'
        datasources = []
        driverClass = resourceEl.getAttributeValue('implementationClassName')
        for factoryEl in resourceEl.getChildren('factories'):
            ds = jee.Datasource(factoryEl.getAttributeValue('name'))
            ds.description = factoryEl.getAttributeValue('description')
            ds.driverClass = driverClass
            ds.setJndiName(factoryEl.getAttributeValue('jndiName'))

            resourcePropertiesEls = factoryEl.getChild('propertySet').getChildren('resourceProperties')
            databaseServer = db.DatabaseServer()
            ds.setServer(databaseServer)
            name = None
            for propEl in resourcePropertiesEls:
                propName = propEl.getAttributeValue('name')
                if propName == 'URL':
                    ds.url = propEl.getAttributeValue('value')
                elif propName == 'serverName':
                    databaseServer.address = propEl.getAttributeValue('value')
                elif propName == 'portNumber':
                    databaseServer.setPort(propEl.getAttributeValue('value'))
                elif propName == 'databaseName':
                    if not name:
                        name = propEl.getAttributeValue('value')
                        if name:
                            databaseServer.addDatabases(db.Database(name))
                elif propName == 'SID':
                    if not name:
                        name = propEl.getAttributeValue('value')
                        if name:
                            databaseServer.setInstance(name.upper())

            connectionPool = factoryEl.getChild('connectionPool')
            if connectionPool is not None:
                for attr in connectionPool.getAttributes():
                    attrName = attr.getName()
                    if attrName == 'minConnections':
                        ds.initialCapacity.set(attr.getValue())
                    if attrName == 'maxConnections':
                        ds.maxCapacity.set(attr.getValue())
            datasources.append(ds)
        return datasources

    def parseResourcesConfig(self, content):
        r'''@types: str -> websphere_discoverer.ResourceConfigDescriptor
        @resource-file: resources.xml
        @raise ValueError: if content is empty or None
        @raise InvalidXmlException: if content is not valid xml
        '''
        descriptor = ResourceConfigDescriptor()
        rootEl = self._getRootElement(content)
        # configure dispatching of parsing and processing result for different types
        # of resources
        parseJmsResources = lambda el, inst = self: [inst.parseJmsProviderEl(el)]
        resourceTypeToMethods = { 'JDBCProvider' : (self.__parseJdbcDatasources,
                                                        descriptor.addJdbcDatasource),
                                     'JMSProvider' : (parseJmsResources,
                                                      descriptor.addJmsDatasource),
                                     'J2CResourceAdapter' : (parseJmsResources,
                                                      descriptor.addJmsDatasource)
                                    }
        for resourceEl in rootEl.getChildren():
            resourceType = resourceEl.getName()
            logger.debug("---resource type: ", resourceType)
            methods = resourceTypeToMethods.get(resourceType)
            if not methods:
                continue
            parserMd, processMd = methods
            map(processMd, parserMd(resourceEl))
        return descriptor

    def parseDeploymentTargets(self, content):
        r'''@types: str -> AppDeploymentDescriptor
        @resource-file: deployment.xml
        '''
        appdeploymentEl = self._getRootElement(content)
        appdeploymentElNs = appdeploymentEl.getNamespace('xmi')
        descriptor = AppDeploymentDescriptor()
        for deploymentTargertEl in appdeploymentEl.getChildren('deploymentTargets'):
            deploymentType = deploymentTargertEl.getAttributeValue('type', appdeploymentElNs)
            if deploymentType:
                name = deploymentTargertEl.getAttributeValue('name')
                if deploymentType.endswith('ServerTarget'):
                    server = jee.Server(name)
                    server.nodeName = deploymentTargertEl.getAttributeValue('nodeName')
                    descriptor.addServer(server)
                elif deploymentType.endswith('ClusteredTarget'):
                    descriptor.addCluster(jee.Cluster(name))
                else:
                    logger.warn("Unknown deployment type for the application: %s" % deploymentType)
        return descriptor



class _FileFilterByPattern(file_system.FileFilter):
    def __init__(self, pattern, acceptFunction):
        r'''@types: str, callable(file)
        @raise ValueError: File pattern is not specified
        @raise ValueError: Accept function for the file filter is not specified
        '''
        if not pattern:
            raise ValueError("File pattern is not specified")
        if not callable(acceptFunction):
            raise ValueError("Accept function for the file filter is not specified")
        self.filePattern = pattern
        self.accept = acceptFunction


class RootLayout(jee_discoverer.Layout):

    def __init__(self, installRootDirPath, fs):
        r'''@types: str, file_system.FileSystem
        @raise ValueError: Root layout should work with absolute path of installation root directory
        '''
        jee_discoverer.Layout.__init__(self, fs)
        if not self.path().isAbsolute(installRootDirPath):
            raise ValueError("Root layout should work with absolute path of installation root directory")
        self.__installRootDirPath = self.path().normalizePath( installRootDirPath )

    def getProfileRegistryPath(self):
        r'@types: -> str'
        return self.path().join(self.__installRootDirPath, 'properties', 'profileRegistry.xml')

    def composeProfileHomePath(self, profileName):
        r'@types: str -> str'
        return self.path().join(self.__installRootDirPath, 'profiles', profileName)




class ProfileLayout(jee_discoverer.Layout):

    def __init__(self, profileHomeDirPath, fs):
        r'''@types: str, file_system.FileSystem
        @raise ValueError: Profile layout should work with absolute path of home directory
        '''
        jee_discoverer.Layout.__init__(self, fs)
        if not self.path().isAbsolute(profileHomeDirPath):
            raise ValueError("Profile layout should work with absolute path of home directory")
        self.__homeDirPath = self.path().normalizePath( profileHomeDirPath )

    def findCellRootPaths(self):
        r'@types: -> list(str)'
        paths = []
        cellsDirPath = self.path().join(self.__homeDirPath, 'config', 'cells')
        cellFiles = self._getFs().getFiles(cellsDirPath, recursive = 1, filters = [_FileFilterByPattern('cell.xml',
                                                                            lambda f: f.name.lower() == 'cell.xml')],
                                           fileAttrs = [file_topology.FileAttrs.NAME,
                                                        file_topology.FileAttrs.PATH])
        for file in cellFiles:
            paths.append(self.path().dirName(file.path))
        return paths

    def composeCellHomePath(self, cellName):
        r'@types: str -> str'
        return self.path().join(self.__homeDirPath, 'config', 'cells', cellName)


class CellLayout(jee_discoverer.Layout):

    def __init__(self, cellHomeDirPath, fs):
        r'''@types: str, file_system.FileSystem
        @raise ValueError: Cell layout should work with absolute path of home directory
        '''
        jee_discoverer.Layout.__init__(self, fs)
        if not self.path().isAbsolute(cellHomeDirPath):
            raise ValueError("Cell layout should work with absolute path of home directory")
        self.__homeDirPath = self.path().normalizePath( cellHomeDirPath )

    def getConfigFilePath(self):
        r'@types: -> str'
        return self.path().join(self.__homeDirPath, 'cell.xml')

    def getResourcesConfigFilePath(self):
        r'@types: -> str'
        return self.path().join(self.__homeDirPath, 'resources.xml')

    def getSecurityConfigFilePath(self):
        r'@types: -> str'
        return self.path().join(self.__homeDirPath, 'security.xml')

    def getNameBindingConfigFile(self):
        r'@types: -> str'
        return self.path().join(self.__homeDirPath, 'namebindings.xml')

    def composeClusterHomePath(self, clusterName):
        r'@types: str -> str'
        return self.path().join(self.__homeDirPath, 'clusters', clusterName)

    def composeNodeHomePath(self, nodeName):
        r'@types: str -> str'
        return self.path().join(self.__homeDirPath, 'nodes', nodeName)

    def composeApplicationDeploymentDirPath(self, relativeAppPath):
        r'@types: str -> str'
        return self.path().join(self.__homeDirPath, 'applications',
                                self.path().normalizePath( relativeAppPath ))

    def composeApplicationDeploymentFilePath(self, relativeAppPath):
        r'@types: str -> str'
        return self.path().join(self.composeApplicationDeploymentDirPath(relativeAppPath),
                                'deployment.xml')

    def findNodeRootPaths(self):
        r'@types: -> list(str)'
        paths = []
        nodesDirPath = self.path().join(self.__homeDirPath, 'nodes')
        nodeFiles = self._getFs().getFiles(nodesDirPath, recursive = 1, filters = [_FileFilterByPattern('serverindex.xml',
                                                                            lambda f: f.name.lower() == 'serverindex.xml')],
                                           fileAttrs = [file_topology.FileAttrs.NAME,
                                                        file_topology.FileAttrs.PATH])
        for file in nodeFiles:
            paths.append(self.path().dirName(file.path))
        return paths

    def findClusterRootPaths(self):
        r'@types: -> list(str)'
        paths = []
        nodesDirPath = self.path().join(self.__homeDirPath, 'clusters')
        nodeFiles = self._getFs().getFiles(nodesDirPath, recursive = 1, filters = [_FileFilterByPattern('cluster.xml',
                                                                            lambda f: f.name.lower() == 'cluster.xml')],
                                           fileAttrs = [file_topology.FileAttrs.NAME,
                                                        file_topology.FileAttrs.PATH])
        for file in nodeFiles:
            paths.append(self.path().dirName(file.path))
        return paths


class NodeLayout(jee_discoverer.Layout):

    def __init__(self, nodeHomeDirPath, fs):
        r'''@types: str, file_system.FileSystem
        @raise ValueError: None layout should work with absolute path of home directory
        '''
        jee_discoverer.Layout.__init__(self, fs)
        if not self.path().isAbsolute(nodeHomeDirPath):
            raise ValueError("Node layout should work with absolute path of home directory")
        self.__homeDirPath = self.path().normalizePath( nodeHomeDirPath )

    def getConfigFilePath(self):
        r'@types: -> str'
        return self.path().join(self.__homeDirPath, 'node.xml')

    def getServerIndexPath(self):
        r'@types: -> str'
        return self.path().join(self.__homeDirPath, 'serverindex.xml')

    def getResourcesConfigFilePath(self):
        r'@types: -> str'
        return self.path().join(self.__homeDirPath, 'resources.xml')

    def composeServerHomePath(self, serverName):
        r'@types: str -> str'
        return self.path().join(self.__homeDirPath, 'servers', serverName)

class ClusterLayout(jee_discoverer.Layout):

    def __init__(self, clusterHomeDirPath, fs):
        r'''@types: str, file_system.FileSystem
        @raise ValueError: Cluster layout should work with absolute path of home directory
        '''
        jee_discoverer.Layout.__init__(self, fs)
        if not self.path().isAbsolute(clusterHomeDirPath):
            raise ValueError("Cluster layout should work with absolute path of home directory")
        self.__homeDirPath = self.path().normalizePath( clusterHomeDirPath )

    def getConfigFilePath(self):
        r'@types: -> str'
        return self.path().join(self.__homeDirPath, 'cluster.xml')

    def getResourcesConfigFilePath(self):
        r'@types: -> str'
        return self.path().join(self.__homeDirPath, 'resources.xml')


class ServerLayout(jee_discoverer.Layout):

    def __init__(self, serverHomeDirPath, fs):
        r'''@types: str, file_system.FileSystem
        @raise ValueError: Server layout should work with absolute path of home directory
        '''
        jee_discoverer.Layout.__init__(self, fs)
        if not self.path().isAbsolute(serverHomeDirPath):
            raise ValueError("Server layout should work with absolute path of home directory")
        self.__homeDirPath = self.path().normalizePath( serverHomeDirPath )

    def getConfigFilePath(self):
        r'@types: -> str'
        return self.path().join(self.__homeDirPath, 'server.xml')

    def getResourcesConfigFilePath(self):
        r'@types: -> str'
        return self.path().join(self.__homeDirPath, 'resources.xml')




class JndiNamedResourceManager:
    r'Manage resource with JNDI name of different scope'

    def __init__(self):
        self.__serverResources = {}
        self.__clusterResources = {}
        self.__nodeResources = {}
        self.__domainResources = {}

    def __add(self, dictionary, resource):
        if not isinstance(resource, jee.HasJndiName):
            raise ValueError("Wrong resource type")
        if not resource.getJndiName():
            raise ValueError("JNDI name is not set")
        dictionary[resource.getJndiName()] = resource

    def addServerResource(self, resource):
        r'@types: jee.HasJndiName'
        self.__add(self.__serverResources, resource)

    def addClusterResource(self, resource):
        r'@types: jee.HasJndiName'
        self.__add(self.__clusterResources, resource)

    def addNodeResource(self, resource):
        r'@types: jee.HasJndiName'
        self.__add(self.__nodeResources, resource)

    def addDomainResource(self, resource):
        r'@types: jee.HasJndiName'
        self.__add(self.__domainResources, resource)

    def lookupResourceByJndiName(self, jndiName):
        r''' Look up for the resource by JNDI in specified order (from first to last)
        server, cluster, node, domain

        @types: str -> jee.HasJndiName'''
        return (self.__serverResources.get(jndiName) or
                self.__clusterResources.get(jndiName) or
                self.__nodeResources.get(jndiName) or
                self.__domainResources.get(jndiName))


def addResource(collection, deploymentTarget, resource):
    r'''@types: _JndiNamedResourceManager, jee.HasResources, jee.HasJndiName
    @raise ValueError: Deployment target is not valid
    '''
    if isinstance(deploymentTarget, jee.Server):
        collection.addServerResource(resource)
    elif isinstance(deploymentTarget, jee.Cluster):
        collection.addClusterResource(resource)
    elif isinstance(deploymentTarget, jee.Node):
        collection.addNodeResource(resource)
    elif isinstance(deploymentTarget, jee.Domain):
        collection.addDomainResource(resource)
    else:
        raise ValueError("Deployment target is not valid")

def getFileContent(provider ,filePath, processFilter=None):
    query = jmx.QueryByPattern('*:type', 'ConfigRepository')
    if processFilter:
        query.patternPart('process', processFilter)
        
    result = ''
    for item in provider.execute(query):
        logger.debug('item:', item)
        objectName = item.ObjectName
        logger.debug('objectname: ', objectName)
        paramType = array(["java.lang.String"], String)
        reader = None
        downloadInputStream = None
        try:
            extractResult = provider.invokeMBeanMethod(objectName, 'extract', paramType, array([filePath], Object))
            logger.debug('extractResult: ', extractResult)
            downloadInputStream = extractResult.getSource();
            downloadInputStream.getOptions().setCompress(False);
            logger.debug('file size:', downloadInputStream.available())
            result = ''
            reader = BufferedReader(InputStreamReader(downloadInputStream));
            line = reader.readLine()

            while line:
                result = result + line + '\n'
                line = reader.readLine()

            logger.debug(result)
            reader.close()
            downloadInputStream.close()
            return result
        except:
            if reader:
                reader.close()
            if downloadInputStream:
                downloadInputStream.close()
            logger.debug('File not found %s' % filePath)
    return result

def _createFileSystemRecursiveSearchEnabled(fs):

    class _FileSystemRecursiveSearchEnabled(fs.__class__):
        r''' Wrapper around file_system module interface created to provide missing
        functionality - recursive search.
        Only one method overriden - getFiles, where if "recursive" is enabled - behaviour changes a bit.
        As filter we expect to get subtype of
        '''
        def __init__(self, fs):
            r'@types: file_system.FileSystem'
            self.__fs = fs
            self.__pathUtil = file_system.getPath(fs)

        def __getattr__(self, name):
            return getattr(self.__fs, name)

        def _findFilesRecursively(self, path, filePattern):
            r'''@types: str, str -> list(str)
            @raise ValueError: Failed to find files recursively
            '''
            r'''@types: str, str -> list(str)
            @raise ValueError: Failed to find files recursively
            '''
            findCommand = 'find ' + path + ' -name ' + filePattern + ' -type f'
            if self._shell.isWinOs():
                if (path.find(' ') > 0) and (path[0] != '\"'):
                    path = r'"%s"' % path
                findCommand = 'dir %s /s /b | findstr %s' % (path, filePattern)

            output = self._shell.execCmd(findCommand)
            if self._shell.getLastCmdReturnCode() == 0:
                return map(string.strip, output.strip().split('\n'))
            if output.lower().find("file not found") != -1:
                raise file_topology.PathNotFoundException()
            raise ValueError("Failed to find files recursively. %s" % output)

        def findFilesRecursively(self, baseDirPath, filters, fileAttrs = None):
            r'''@types: str, list(FileFilterByPattern), list(str) -> list(file_topology.File)
            @raise ValueError: No filters (FileFilterByPattern) specified to make a recursive file search
            '''
            # if filter is not specified - recursive search query becomes not deterministic
            if not filters:
                raise ValueError("No filters (FileFilterByPattern) specified to make a recursive file search")
            # if file attributes are note specified - default set is name and path
            fileAttrs = fileAttrs or [file_topology.FileAttrs.NAME, file_topology.FileAttrs.PATH]
            paths = []
            for filterObj in filters:
                try:
                    paths.extend(self._findFilesRecursively(baseDirPath, filterObj.filePattern))
                except file_topology.PathNotFoundException, pnfe:
                    logger.warn(str(pnfe))
                except (Exception, JException):
                    # TBD: not sure whether we have to swallow such exceptions
                    logger.warnException("Failed to find files for filter with file pattern %s" % filterObj.filePattern)
            files = []
            for path in filter(None, paths):
                files.append(self.__fs.getFile(path, fileAttrs = fileAttrs))
            return files

        def getFiles(self, path, recursive = 0, filters = [], fileAttrs = []):
            r'@types: str, bool, list(FileFilterByPattern), list(str) -> list(file_topology.File)'
            if recursive:
                return self.filter(self.findFilesRecursively(path, filters, fileAttrs), filters)
            else:
                return self.__fs.getFiles(path, filters = filters, fileAttrs = fileAttrs)
    return _FileSystemRecursiveSearchEnabled(fs)

def isLoadExternalDtdEnabled():
    globalSettings = GeneralSettingsConfigFile.getInstance()
    return globalSettings.getPropertyBooleanValue('loadExternalDtd', 0)

def discoverNode(nodeLayout, pathUtil):
    r'@types: websphere_discoverer.NodeLayout, file_topology.Path -> jee.Node'
    # make discovery of node based on directory name where serverindex.xml resides
    return jee.Node(pathUtil.baseName(
                        pathUtil.dirName(
                            nodeLayout.getConfigFilePath()
                        )
                    )
    )

def _sendVectorImmediately(framework, vector, forceVectorClean = 1):
    r'@types: Framework, ObjectStateHolderVector'
    framework.sendObjects(vector)
    #framework.flushObjects()
    if forceVectorClean:
        vector.clear()

def sendTopologyWithDomainVector(framework, vector, domainVector):
    r'''Send vector along with domain vector and forced cleaning
    @types: Framework, ObjectStateHolderVector, ObjectStateHolderVector
    '''
    if vector.size():
        vector.addAll(domainVector.deepClone())
        _sendVectorImmediately(framework, vector)

def createClusterLayout(cluster, cellLayout, fs):
    r'@types: jee.Cluster, websphere_discoverer.CellLayout -> websphere_discoverer.ClusterLayout'
    clusterHomeDirPath = cellLayout.composeClusterHomePath(cluster.getName())
    return ClusterLayout(clusterHomeDirPath, fs)

def groupServersByFullNameInCell(cell):
    # create catalog of all servers grouped by full name (node name + server name)
    serverByFullName = {}
    for node in cell.getNodes():
        serverByFullName.update(applyMapping(jee.Server.getFullName, node.getServers()))
    return serverByFullName

def createNodeLayout(node, cellLayout, fs):
    r'@types: jee.Node, websphere_discoverer.CellLayout -> websphere_discoverer.NodeLayout'
    nodeHomeDirPath = cellLayout.composeNodeHomePath(node.getName())
    return NodeLayout(nodeHomeDirPath, fs)

def createServerLayout(server, nodeLayout, fs):
    r'@types: jee.Server, websphere_discoverer.NodeLayout -> websphere_discoverer.ServerLayout'
    serverHomeDirPath = nodeLayout.composeServerHomePath(server.getName())
    return ServerLayout(serverHomeDirPath, fs)

def discoverResourcesInDomain(cell, cellLayout, fs, parser, reporterCreator, sendResourcesVector):
    r'@types: Cell, CellLayout, FileSystem, DescriptorParser, ReporterCreator, (ObjectStateHolderVector ->) -> JndiNamedResourceManager'
    # ========================== RESOURCES DISCOVERY =======
    logger.info("START GRABBING RESOURCES")
    jndiNamedResourceManager = JndiNamedResourceManager()
    # discover resources for cell
    discoverResources(
        cell, None, curry(asIs, cellLayout),
        parser, reporterCreator, sendResourcesVector,
        jndiNamedResourceManager.addDomainResource
    )

    # discover resources for clusters
    for cluster in cell.getClusters():
        discoverResources(
            cell, cluster,
            curry(createClusterLayout, cluster, cellLayout, fs),
            parser, reporterCreator, sendResourcesVector,
            jndiNamedResourceManager.addClusterResource
        )
    # discover resources for nodes
    for node in cell.getNodes():
        discoverResources(
            cell, node,
            curry(createNodeLayout, node, cellLayout, fs),
            parser, reporterCreator, sendResourcesVector,
            jndiNamedResourceManager.addNodeResource
        )
        # discover resources for server
        nodeLayout = NodeLayout(
                        cellLayout.composeNodeHomePath(node.getName()), fs)
        for server in node.getServers():
            discoverResources(
                cell, server,
                curry(createServerLayout, server, nodeLayout, fs),
                parser, reporterCreator, sendResourcesVector,
                jndiNamedResourceManager.addServerResource
            )
    return jndiNamedResourceManager

def discoverResources(domain, deploymentScope, createLayoutFunc,
                          parser, reporterCreator, sendVectorFunc, processResourceFunc):
    r'''
    Parse resources based on layout and instantly send to the uCMDB
    @types: jee.Domain, entity.HasOsh, (-> ?), websphere_discoverer.DescriptorParser, (vector -> ), (jee.Resource -> )

    @param domain:
    @param deploymentScope:
            Passed domain and deployment scope mostly required for the reporting
            to show the place of discovered resource in topology
    @param createLayoutFunc: Returns layout instance to get resources file content
    @param parser: Parser used to parse resources
    '''
    logger.info("Process %s resources" % (deploymentScope or domain))
    resources = []
    layout = createLayoutFunc()
    cellResourcesConfigPath = layout.getResourcesConfigFilePath()
    try:
        cellResourcesConfigFile = layout.getFileContent(cellResourcesConfigPath)
        descriptor = parser.parseResourcesConfig(cellResourcesConfigFile.content)
    except file_system.PathNotFoundException, e:
        logger.warn("Failed to get resources file. Not found %s" % str(e))
    except:
        logger.warnException("Failed to process resources for the %s" % deploymentScope)
    else:
        vector = ObjectStateHolderVector()
        # report JDBC datasources
        try:
            datasources = descriptor.getJdbcDatasources()
            resources.extend(datasources)
            vector.addAll(reporterCreator.getJdbcDsReporter().reportDatasourcesWithDeployer(
                domain,
                deploymentScope,
                *datasources
            )
            )
        except Exception:
            logger.warnException("Failed to report datasources for the %s" % deploymentScope)
        # report JMS datasources
        try:
            domainOsh = domain.getOsh()
            deploymentScopeOsh = deploymentScope and deploymentScope.getOsh()
            datasources = filter(jms.Datasource.getDestinations, descriptor.getJmsDatasources())
            for datasource in filter(jms.Datasource.getDestinations, descriptor.getJmsDatasources()):
                vector.addAll(reporterCreator.getJmsDsReporter().reportDatasourceWithDeployer(
                    domainOsh,
                    deploymentScopeOsh,
                    datasource
                )
                )
                resources.extend(datasource.getDestinations())
        except Exception:
            logger.warnException("Failed to report JMS destinations for the %s" % deploymentScope)

        # report resources file as configuration file if it contains jdbc or jms definitions
        if resources:
            vector.addAll(reporterCreator.getJdbcDsReporter().reportDatasourceFiles(domain, deploymentScope,
                                                                                    jee.createXmlConfigFile(
                                                                                        cellResourcesConfigFile)))
        sendVectorFunc(vector)
    # jee.Resource
    for resource in resources:
        try:
            processResourceFunc(resource)
        except ValueError:
            logger.warnException("Failed to process %s for %s" % (resource, deploymentScope))
    return resources

def discoverApplicationsInDomain(cell, cellLayout, fs, shell, parser, reporterCreator,
                                     jndiNamedResourceManager,
                                     sendApplicationsVector, NameBindingContent):
    r'@types: Domain, CellLayout, FileSystem, Shell, DescriptorParser, ReporterCreator, JndiNamedResourceManager, (ObjectStateHolderVector -> ) -> '
    # create catalog of serves and cluster by full name and name accordingly
    serverByFullName = groupServersByFullNameInCell(cell)
    clusterByName = applyMapping(jee.Cluster.getName, cell.getClusters())
    # discovery skeleton
    applicationLayout = jee_discoverer.ApplicationLayout(fs)
    descriptorParser = jee_discoverer.ApplicationDescriptorParser(isLoadExternalDtdEnabled())
    appDiscoverer = asm_websphere_discoverer.WebsphereApplicationDiscovererByShell(shell, applicationLayout,
                                                                                   descriptorParser)

    jndiNameToName = {}
    if NameBindingContent:
        logger.debug('namebinding content:', NameBindingContent.content)
        matches = re.findall('<namebindings:EjbNameSpaceBinding.*?nameInNameSpace="(.*?)".*?ejbJndiName="(.*?)"',
                             NameBindingContent.content)
        if matches:
            for match in matches:
                jndiNameToName[match[1]] = match[0]

    logger.debug('jndiNameToName: ', jndiNameToName)
    for server in serverByFullName.values():
        # Information about deployed applications is stored
        # in serverindex.xml per node
        # Each previously discovered server may have
        # role of application container
        appServerRole = server.getRole(jee.ApplicationServerRole)
        # discover applications
        for app in (appServerRole and appServerRole.getApplications()) or ():
            # applications are in the cell independently of the deployment target level
            # cellHome/applications/<app_name|archive_name>/deployments/<module_name>/deployment.xml
            # if not absolute - append needed part
            appDeploymentDirPath = cellLayout.composeApplicationDeploymentDirPath(app.fullPath)
            deploymentDescriptorPath = cellLayout.composeApplicationDeploymentFilePath(app.fullPath)
            isAppReported = 0
            vector = ObjectStateHolderVector()
            try:
                deploymentDescriptorFile = cellLayout.getFileContent(deploymentDescriptorPath)
            except file_topology.PathNotFoundException, pnfe:
                logger.warn(str(pnfe))
            except (Exception, JException):
                logger.warn("Failed to process res file for %s" % server)
            else:
                application = appDiscoverer.discoverEarApplication(app.getName(), appDeploymentDirPath, jndiNameToName)
                if not application: continue

                try:
                    deploymentTargetsDescriptor = parser.parseDeploymentTargets(deploymentDescriptorFile.content)
                except (Exception, JException):
                    logger.warnException("Failed to parse application deployment targets")
                else:
                    applicationReporter = reporterCreator.getApplicationReporter()
                    for server in deploymentTargetsDescriptor.getServers():
                        deploymentScope = serverByFullName.get(server.getFullName())
                        if deploymentScope:
                            try:
                                vector.addAll(applicationReporter.reportApplications(cell, deploymentScope, application))
                                isAppReported = 1
                            except Exception:
                                logger.warnException("Failed to report applications for the %s" % deploymentScope)
                    for cluster in deploymentTargetsDescriptor.getClusters():
                        deploymentScope = clusterByName.get(cluster.getName())
                        if deploymentScope:
                            try:
                                vector.addAll(applicationReporter.reportApplications(cell, deploymentScope, application))
                                for node in cell.getNodes():
                                    for server in node.getServers():
                                        if server.hasRole(jee.ClusterMemberServerRole) and server.getRole(
                                                jee.ClusterMemberServerRole).clusterName == cluster.getName():
                                            vector.addAll(applicationReporter.reportApplications(cell, server, application))
                                isAppReported = 1
                            except Exception:
                                logger.warnException("Failed to report applications for the %s" % deploymentScope)

                    # report as is in scope of domain if deployment targets discovery failed
                    if not isAppReported:
                        try:
                            vector.addAll(applicationReporter.reportApplications(cell, None, application))
                        except Exception:
                            logger.warnException("Failed to report applications for the %s" % cell)

                    # report application resources
                    for module in application.getModules():
                        files = filter(lambda file, expectedName=module.getDescriptorName():
                                       file.getName() == expectedName, module.getConfigFiles())
                        if files:
                            file = files[0]
                            try:
                                descriptor = None
                                if isinstance(module, jee.WebModule):
                                    descriptor = descriptorParser.parseWebModuleDescriptor(file.content, module)
                                elif isinstance(module, jee.EjbModule):
                                    descriptor = descriptorParser.parseEjbModuleDescriptor(file.content, module)
                                else:
                                    logger.warn("Unknown type of JEE module: %s" % module)
                                if descriptor:
                                    for res in descriptor.getResources():
                                        logger.debug('resource:', res)
                                        resource = jndiNamedResourceManager.lookupResourceByJndiName(res.getName())
                                        logger.warn("%s  %s" % (resource, application))
                                        if not (resource and resource.getOsh()):
                                            logger.warn("%s cannot be used for %s" % (resource, application))
                                        else:
                                            vector.addAll(
                                                applicationReporter.reportApplicationResource(application, resource))
                            except (Exception, JException):
                                logger.warnException("Failed to process %s for resources" % module)
                    sendApplicationsVector(vector)

def discoverClusters(cellLayout, fs, parser):
    r''' Discover Clusters in specified <cell>
    recursive - list node.xml in cellHomePath/nodes/*/
    @types: CellLayout, file_system.FileSystem, websphere_discoverer.DescriptorParser -> list[Tuple[Cluster, list[jee.Server]]]
    '''
    clusterInfoList = []
    for clusterRootPath in cellLayout.findClusterRootPaths():
        try:
            # recursive - lsit cluster.xml in cellHomePath/[clusters/*/]
            clusterLayout = websphere_discoverer.ClusterLayout(clusterRootPath, fs)
            clusterConfig = parser.parseClusterConfig(
                                clusterLayout.getFileContent(
                                    clusterLayout.getConfigFilePath()
                                ).content
                            )
            clusterInfoList.append((clusterConfig.cluster, clusterConfig.getMembers()))
        except Exception:
            logger.warnException("Failed to process cluster configuration")
    return clusterInfoList

def getClusterMemberFromRuntimeGroup(member, serverByFullName):
    r''' Determine whether member is present in group where its full name set as a key
    @types: jee.Server, dict[str, jee.Server] -> jee.Server
    '''
    return serverByFullName.get(member.getFullName())
