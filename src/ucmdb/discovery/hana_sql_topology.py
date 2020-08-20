"""
Created on 2015-01-07

@author: Moritz Rupp <moritz.rupp@hp.com>
"""
from appilog.common.system.types import ObjectStateHolder
import dns_resolver
import re
import hana_queries
import logger
import modeling
import shellutils
import ip_addr
import errormessages
from com.hp.ucmdb.discovery.library.clients import ClientsConsts
from java.lang import Exception as JException
from org.python.core import Py
from java.sql import SQLException


class HanaInstance:
    """Containing all information and relations of a logical Hana instance CIT.
    
    This class is a wrapper for the Hana instances to be created as ObjectStateHolders.
    It provides functions for retrieving the information related to a Hana instance 
    like Log files, Trace files, Configurations, etc.
    
    Attributes:
        connection: Object of type DriverHanaInstanceConnection providing the 
            JDBC interface
        hostname: The host name of the host running the Hana instance
        credential_id: The CMDB ID of the used credentials as string
        name: The SID of the Hana instance
        number: The instance number of the Hana instance
        port: The listening port of the Hana instance
        path: The application installed path of the Hana instance
        ips: A list of all IPs of the host running the Hana instance    
    """
    
    def __init__(self, client, hostname, credential_id):
        """Inits the HanaInstance with the information directly 
        retrieved from the database using SQL queries.
        
        The ObjectStateHolders for the Hana instance and the host are 
        created.
        """
        self.client = client
        self.hostname = hostname
        self.credential_id = credential_id
        self.ips = []
        self._hana_instance_osh = None
        self.name = None
        self.instance_number = None
        self.port = None
        self.path = None


    def get_hana_instance(self):
        hana_instances = []
        self.name = self.get_instance_name()
        self.instance_number = self.get_instance_number()
        self.path = '/usr/sap/%s/HDB%s' % (self.name, self.instance_number)
        self.port = self.get_instance_port()
        ips = self.get_instance_ips()

        if ips:
            node_osh = modeling.createHostOSH(ips[0])
            for ip in self.ips:
                ip_osh = modeling.createIpOSH(ip)
                hana_instances.append(ip_osh)
                hana_instances.append(modeling.createLinkOSH('containment', node_osh, ip_osh))

            self._hana_instance_osh = ObjectStateHolder('hana_instance')
            self._hana_instance_osh.setContainer(node_osh)

            self._hana_instance_osh.setStringAttribute('name', self.name)
            self._hana_instance_osh.setStringAttribute('number', self.instance_number)
            self._hana_instance_osh.setIntegerAttribute('application_port', self.port)
            self._hana_instance_osh.setStringAttribute('application_ip', ips[0])
            self._hana_instance_osh.setStringAttribute('application_path', self.path)
            self._hana_instance_osh.setStringAttribute('product_name', 'hana_database')
            self._hana_instance_osh.setStringAttribute('discovered_product_name', 'SAP HanaDB')
            self._hana_instance_osh.setStringAttribute('credentials_id', self.credential_id)

            hana_instances.append(node_osh)
            hana_instances.append(self._hana_instance_osh)
        return hana_instances

    def get_instance_name(self):
        name = None
        resultSet = self.client.executeQuery(set_query_parameter(hana_queries.INSTANCE_SID, self.hostname))
        if resultSet.next():
            name = resultSet.getString(1)
        resultSet.close()
        return name

    def get_instance_number(self):
        number = None
        resultSet = self.client.executeQuery(set_query_parameter(hana_queries.INSTANCE_NUMBER, self.hostname))
        if resultSet.next():
            number = resultSet.getString(1)
        resultSet.close()
        return number

    def get_instance_port(self):
        port = None
        resultSet = self.client.executeQuery(set_query_parameter(hana_queries.INSTANCE_PORT, self.hostname))
        if resultSet.next():
            port = resultSet.getString(1)
        resultSet.close()
        return port

    def get_instance_ips(self):
        ips = []
        publicname_ip = self.get_instance_public_name()
        if ip_addr.isValidIpAddress(publicname_ip):
            logger.debug("publicname_ip:", publicname_ip)
            ips = [publicname_ip]
        else:
            logger.debug("The field 'net_publicname' does not contain a valid IP address. Get all IPs")
            all_ips = self.get_all_ip_addresses()
            if all_ips:
                ips.extend(all_ips)
        return ips


    def get_instance_public_name(self):
        ip_address = None
        resultSet = self.client.executeQuery(set_query_parameter(hana_queries.INSTANCE_PUBLIC_NAME, self.hostname))
        if resultSet.next():
            ip_address = resultSet.getString(1)
        resultSet.close()
        return ip_address

    def get_all_ip_addresses(self):
        ip_address = []
        resultSet = self.client.executeQuery(set_query_parameter(hana_queries.INSTANCE_IPADDRESS, self.hostname))
        if resultSet.next():
            ips = resultSet.getString(1).split(",")
            if ips:
                for ip in ips:
                    ip_address.append(ip.strip())
        resultSet.close()
        return ip_address

        
    def get_db_trace_files(self):
        """Retrieves the DB trace files as ObjectStateHolder objects.

        Returns:
            A list containing the ObjectStateHolder objects.

        Raises:
            SQLException: An error occurred during the SQL connection
        """
        files = []
        rs = self.client.executeQuery(set_query_parameter(hana_queries.DB_TRACE_FILES, self.hostname))
        while rs.next():
            db_trace_file_osh = ObjectStateHolder('db_trace_file')
            
            db_trace_file_osh.setContainer(self._hana_instance_osh)
            db_trace_file_osh.setStringAttribute('name', rs.getString('FILE_NAME'))
            db_trace_file_osh.setLongAttribute('size', rs.getString('FILE_SIZE'))
            
            files.append(db_trace_file_osh)
        rs.close()
        return files
    
    def get_db_log_files(self):
        """Retrieves the DB log files as ObjectStateHolder objects.
        
        Returns:
            A list containing the ObjectStateHolder objects.
        
        Raises:
            SQLException: An error occurred during the SQL connection
        """
        files = []
        rs = self.client.executeQuery(set_query_parameter(hana_queries.DB_LOG_FILES, self.hostname))
        while rs.next():
            db_log_file_osh = ObjectStateHolder('db_log_file')
            
            db_log_file_osh.setContainer(self._hana_instance_osh)
            db_log_file_osh.setStringAttribute('name', rs.getString('FILE_NAME'))
            db_log_file_osh.setLongAttribute('size', rs.getString('TOTAL_SIZE'))
            
            files.append(db_log_file_osh)
        rs.close()
        return files
    
    def get_db_data_files(self):
        """Retrieves the DB data files as ObjectStateHolder objects.
        
        Returns:
            A list containing the ObjectStateHolder objects.
        
        Raises:
            SQLException: An error occurred during the SQL connection
        """
        files = []
        rs = self.client.executeQuery(set_query_parameter(hana_queries.DB_DATA_FILES, self.hostname))
        while rs.next():
            db_data_file_osh = ObjectStateHolder('dbdatafile')
            db_data_file_osh.setContainer(self._hana_instance_osh)
            db_data_file_osh.setStringAttribute('name', rs.getString('FILE_NAME'))
            db_data_file_osh.setStringAttribute('dbdatafile_byte', rs.getString('USED_SIZE'))
            db_data_file_osh.setStringAttribute('dbdatafile_maxbytes', rs.getString('TOTAL_SIZE'))
            db_data_file_osh.setIntegerAttribute('dbdatafile_fileid', rs.getString('VOLUME_ID'))
            
            files.append(db_data_file_osh)
        rs.close()
        return files
    
    def get_instance_configuration(self):
        """Retrieves the Hana instance configurations as ObjectStateHolder objects.
        
        Returns:
            A list containing the ObjectStateHolder objects.
        
        Raises:
            SQLException: An error occurred during the SQL connection
        """
        db_config_osh = ObjectStateHolder('db_configuration')
        db_config_osh.setContainer(self._hana_instance_osh)
        rs = self.client.executeQuery(set_query_parameter(hana_queries.INSTANCE_CONFIG, self.hostname))
        while rs.next():
            val = rs.getString('VALUE')
            key = rs.getString('KEY')
            if len(val) > 900:
                logger.warn("Value for DB configuration '%s' is too long (>900). Skipping" % key)
                continue
            parameter = "key=%s, section=%s, value=%s, file_name=%s, tenant_name=%s" % (key, rs.getString('SECTION'), val, rs.getString('FILE_NAME'), rs.getString('TENANT_NAME'))
            db_config_osh.addAttributeToList("parameters", parameter)
        rs.close()
        return db_config_osh

    def get_hana_instance_osh(self):
        return self._hana_instance_osh

class HanaDatabase(object):
    """Containing all information and relations of a logical Hana database CIT.
    
    This class is a wrapper for the Hana database to be created as ObjectStateHolder.
    It provides functions for retrieving the information related to a Hana instance 
    like DB users, DB schemas, the Hana instances, Configurations, etc.
    
    Attributes:
        connection: Object of type DriverHanaInstanceConnection providing the 
            JDBC interface
        name: The SID of the Hana database
        version: The version of the Hana database
        startup_time: The last startup time of the Hana database
        vendor: The vendor of the Hana database
        instances: A dictionary of all connected Hana instances the database consists of.
            The key is the hostname and the value is the object itself
    """
    def __init__(self, framework, client, discovery_options):
        """Inits the HanaDatabase with the information directly 
        retrieved from the database using SQL queries.
        
        The ObjectStateHolders for the Hana database and the host are 
        created.
        """
        self.framework = framework
        self.client = client
        self.discovery_options = discovery_options
        self.hana_osh = None
        self.instances = {}
        self.name = None
        self.version = None
        self.vendor = None
        self.startup_time = None

    def get_hanadb(self):

        self.name = self.get_db_name()
        self.version = self.get_db_version()
        self.startup_time = self.get_db_startup_time()
        self.vendor = 'SAP SE'

        self.hana_osh = ObjectStateHolder('hana_db')
        
        self.hana_osh.setStringAttribute('name', self.name)
        self.hana_osh.setStringAttribute('version_description', self.version)
        self.hana_osh.setStringAttribute('vendor', self.vendor)
        self.hana_osh.setDateAttribute('startup_time', self.startup_time)
        self.hana_osh.setStringAttribute('role', 'live')
        return self.hana_osh

    def get_db_name(self):
        name = None
        resultSet = self.client.executeQuery(hana_queries.DATABASE_NAME)
        if resultSet.next():
            name = resultSet.getString(1)
        resultSet.close()
        return name

    def get_db_version(self):
        version = None
        resultSet = self.client.executeQuery(hana_queries.DATABASE_VERSION)
        if resultSet.next():
            version = resultSet.getString(1)
        resultSet.close()
        return version

    def get_db_startup_time(self):
        startup_time = None
        resultSet = self.client.executeQuery(hana_queries.DATABASE_STARTUP_TIME)
        if resultSet.next():
            startup_time = resultSet.getDate(1)
        resultSet.close()
        return startup_time

    def get_real_host(self, hostname):
        real_host = None
        resultSet = self.client.executeQuery(set_query_parameter(hana_queries.REAL_HOSTS, hostname))
        if resultSet.next():
            real_host = resultSet.getString(1)
        resultSet.close()
        return real_host

    def get_virtual_host(self, hostname):
        virtual_host = None
        resultSet = self.client.executeQuery(set_query_parameter(hana_queries.VIRTUAL_HOSTS, hostname))
        if resultSet.next():
            virtual_host = resultSet.getString(1)
        resultSet.close()
        return virtual_host

    def get_db_hosts(self):
        hosts = []
        vhost_host_map = {}
        rs = self.client.executeQuery(hana_queries.DATABASE_INSTANCES)
        while rs.next():
            host = rs.getString(1)
            logger.debug("host:", host)
            real_host = self.get_real_host(host)
            logger.debug("real_host:", real_host)
            virtual_host = self.get_virtual_host(host)
            logger.debug("virtual_host:", virtual_host)
            if real_host == '-':
                logger.info("No virtual host names defined for Hana instance")
            else:
                host = real_host
            hosts.append(host)
            vhost_host_map[host] = virtual_host
        rs.close()
        return hosts, vhost_host_map

    def get_db_instances(self):
        db_instances = []
        hosts, vhost_host_map = self.get_db_hosts()
        logger.debug("Connected hosts (%s): %s" % (len(hosts), vhost_host_map))
        logger.debug("Calculate Hana instance topology")
        credential_id = self.framework.getDestinationAttribute('credentialsId')
        if len(hosts) < 1:
            raise Exception("No information about connected data instances could be queried.")
        for host in hosts:
            try:
                hana_instance = HanaInstance(self.client, vhost_host_map[host], credential_id)
                db_instances.extend(hana_instance.get_hana_instance())
                db_instances.append(modeling.createLinkOSH("membership", self.hana_osh, hana_instance.get_hana_instance_osh()))
                self.instances[vhost_host_map[host]] = hana_instance
            except SQLException, ex:
                logger.error("Couldn't create Hana instance ObjectStateHolder on host '%s' due to an SQL exception" % vhost_host_map[host])
                logger.debugException(str(ex))
                errormessages.resolveAndReport(ex.getMessage(), ClientsConsts.SQL_PROTOCOL_NAME, self.framework)
                continue

            if self.discovery_options['tracefiles'] == 'true':
                try:
                    db_instances.extend(hana_instance.get_db_trace_files())
                except SQLException, ex:
                    logger.error("SQL command for getting DB trace files timed out. Skipping")
                    logger.debugException(str(ex))
                    errormessages.resolveAndReport(ex.getMessage(), ClientsConsts.SQL_PROTOCOL_NAME, self.framework)
            else:
                logger.info("Discovery of DB Trace Files disabled")

            if self.discovery_options['logfiles'] == 'true':
                try:
                    db_instances.extend(hana_instance.get_db_log_files())
                except SQLException, ex:
                    logger.error("SQL command for getting DB log files timed out. Skipping")
                    logger.debugException(str(ex))
                    errormessages.resolveAndReport(ex.getMessage(), ClientsConsts.SQL_PROTOCOL_NAME, self.framework)
            else:
                logger.info("Discovery of DB Log Files disabled")

            if self.discovery_options['datafiles'] == 'true':
                try:
                    db_instances.extend(hana_instance.get_db_data_files())
                except SQLException, ex:
                    logger.error("SQL command for getting DB data files timed out. Skipping")
                    logger.debugException(str(ex))
                    errormessages.resolveAndReport(ex.getMessage(), ClientsConsts.SQL_PROTOCOL_NAME, self.framework)
            else:
                logger.info("Discovery of DB Data files disabled")

            if self.discovery_options['instanceconfigurations'] == 'true':
                try:
                    db_instances.append(hana_instance.get_instance_configuration())
                except SQLException, ex:
                    logger.error("SQL command for getting Hana instance configurations timed out. Skipping")
                    logger.debugException(str(ex))
                    errormessages.resolveAndReport(ex.getMessage(), ClientsConsts.SQL_PROTOCOL_NAME, self.framework)
            else:
                logger.info("Discovery of Hana instance configurations disabled")

        return db_instances

    def get_db_users(self, all_users_schemas='false'):
        """Retrieves the DB users as ObjectStateHolder objects.
        
        Args:
            all_users_schemas: String (True/False) whether all database users or 
                only local ones should be created.
        
        Returns:
            A list containing the ObjectStateHolder objects.
        
        Raises:
            SQLException: An error occurred during the SQL connection
        """
        users = []
        if all_users_schemas == 'true':
            query = hana_queries.DB_USERS_ALL
        else:
            query = hana_queries.DB_USERS
        
        rs = self.client.executeQuery(query)
        while rs.next():
            db_user_osh = ObjectStateHolder('dbuser')
            
            db_user_osh.setContainer(self.hana_osh)
            db_user_osh.setStringAttribute('name', rs.getString('USER_NAME'))
            db_user_osh.setDateAttribute('dbuser_created', rs.getDate('CREATE_TIME'))
            
            users.append(db_user_osh)
        rs.close()
        return users
    
    def get_db_schema(self, dbusers, all_users_schemas='false'):
        """Retrieves the DB schemas and the relations to there owners 
        as ObjectStateHolder objects.
        
        Args:
            dbusers: A list containing the ObjectStateHolder objects 
                for the database users to be linked to the schemas
            all_users_schemas: String (True/False) whether all database users or 
                only local ones should be created.
        
        Returns:
            Two list containing the ObjectStateHolder objects for the schemas 
            and secondly for the links between the user and the schema.
        
        Raises:
            SQLException: An error occurred during the SQL connection
        """
        schemas = []
        user_dict = dict((u.getAttribute('name').getStringValue(), u) for u in dbusers)
        if all_users_schemas == 'true':
            query = hana_queries.DB_SCHEMAS_ALL
        else:
            query = hana_queries.DB_SCHEMAS
        
        rs = self.client.executeQuery(query)
        
        while rs.next():
            db_schema_osh = ObjectStateHolder('database_instance')
            
            db_schema_osh.setContainer(self.hana_osh)
            db_schema_osh.setStringAttribute('name', rs.getString('SCHEMA_NAME'))
            
            # Get the user which is Owner of the schema and create the ownership
            schema_owner = rs.getString('SCHEMA_OWNER')
            schemas.append(db_schema_osh)
            
            if schema_owner in user_dict:
                schemas.append(modeling.createLinkOSH('ownership', user_dict[schema_owner], db_schema_osh))
        rs.close()
        return schemas
    
    def get_db_configuration(self):
        """Retrieves the Hana database configurations as ObjectStateHolder objects.
        
        Returns:
            A list containing the ObjectStateHolder objects.
        
        Raises:
            SQLException: An error occurred during the SQL connection
        """
        db_config_osh = ObjectStateHolder('db_configuration')
        db_config_osh.setContainer(self.hana_osh)
        rs = self.client.executeQuery(hana_queries.DATABASE_CONFIG)
        
        while rs.next():
            val = rs.getString('VALUE')
            key = rs.getString('KEY')
            if len(val) > 900:
                logger.warn("Value for DB configuration '%s' is too long (>900). Skipping" % key)
                break
            parameter = "key=%s, section=%s, value=%s, file_name=%s, tenant_name=%s" % (key, rs.getString('SECTION'), val, rs.getString('FILE_NAME'), rs.getString('TENANT_NAME'))
            db_config_osh.addAttributeToList("parameters", parameter)

        rs.close()
        return db_config_osh
            
    def get_license_information(self):
        """Retrieves the Hana database license information as ObjectStateHolder objects.
        
        Returns:
            A list containing the ObjectStateHolder objects.
        
        Raises:
            SQLException: An error occurred during the SQL connection
        """
        licenses = []
        rs = self.client.executeQuery(hana_queries.DATABASE_LICENSE)

        while rs.next():
            license_osh = ObjectStateHolder('db_license')
            license_osh.setContainer(self.hana_osh)
            license_osh.setStringAttribute('name', rs.getString('HARDWARE_KEY'))
            license_osh.setStringAttribute('product_name', rs.getString('PRODUCT_NAME'))
            license_osh.setIntegerAttribute('limit', rs.getString('PRODUCT_LIMIT'))
            license_osh.setDateAttribute('start_date', rs.getDate('START_DATE'))
             
            exp_date = rs.getDate('EXPIRATION_DATE')
            if exp_date:
                license_osh.setDateAttribute('expiration_date', exp_date)
             
            license_osh.setBoolAttribute('enforced', rs.getString('ENFORCED'))
            license_osh.setStringAttribute('install_no', rs.getString('INSTALL_NO'))
            license_osh.setIntegerAttribute('system_no', rs.getString('SYSTEM_NO'))
            license_osh.setIntegerAttribute('usage', rs.getString('PRODUCT_USAGE'))
             
            licenses.append(license_osh)
        rs.close()

        return licenses

    def get_instance(self, key):
        return self.instances[key]
    
    
class Replication(object):
    """Provides an interface to replication information retrieval.
    
    The class is used for creating the ObjectStateHolder objects related 
    to the replication information. Different ways of name resolution are 
    tested and used as a Hana database has no information about IPs related 
    to the replicated hosts.
    
    Attributes:
        connection: Object of type DriverHanaInstanceConnection providing the 
            JDBC interface
        hana_database: The HanaDatabase object which is replicated
        secondary_host_map: A dictionary containing the replication information 
            about the replicated Hana instances (host: replicated host)
    """
    def __init__(self, framework, client, hana_database):
        """Inits the Replication object with the required information.
        
        If replication information is available, the secondary host map is filled.
        """
        self.framework = framework
        self.client = client
        self.hana_database = hana_database

    def get_secondary_host_map(self):
        secondary_host_map = {}
        rs = self.client.executeQuery(hana_queries.DATABASE_REPLICATION)
        
        while rs.next():
            secondary_host_map[rs.getString('HOST')] = rs.getString('SECONDARY_HOST')
        rs.close()
        return secondary_host_map

    def get_replication(self):
        """Retrieves the information required for replication topology 
        as ObjectStateHolder objects.
        
        For host name resolution, a three step mechanism is used (implemented 
        by the dns_resolver.FallbackResolver). First, it is tried to resolve 
        the host name with a shell connection to the remote host. On failure, 
        the resolution is tried from the probe. Last, the resolution is tried 
        using the hosts file on the remote host.
        
        Returns:
            A list containing the ObjectStateHolder objects.
        """
        data = []
        instances = []
        secondary_host_map = self.get_secondary_host_map()
        if secondary_host_map:
            logger.debug("Trying to resolve replicas' host names")
            try:
                resolver = resolver = dns_resolver.SocketDnsResolver()
                for host, sec_host in secondary_host_map.iteritems():
                    ips = []
                    try:
                        ips = resolver.resolve_ips(sec_host)
                    except:
                        logger.warn("Host name '%s' could not be resolved. Skipping" % sec_host)

                    logger.info("Resolved IPs (%s): %s" % (sec_host, ips))

                    if ips:
                        instance_osh = ObjectStateHolder('hana_instance')
                        node_osh = modeling.createHostOSH(ips[0].exploded)
                        data.append(node_osh)
                        for ip in ips:
                            ip_osh = modeling.createIpOSH(ip.exploded)
                            link = modeling.createLinkOSH('containment', node_osh, ip_osh)
                            data.append(ip_osh)
                            data.append(link)

                        instance_osh.setContainer(node_osh)

                        instance_osh.setStringAttribute('name', self.hana_database.get_instance(host).name)
                        instance_osh.setStringAttribute('number', self.hana_database.get_instance(host).instance_number)
                        instance_osh.setIntegerAttribute('application_port', self.hana_database.get_instance(host).port)
                        instance_osh.setStringAttribute('application_ip', ips[0].exploded)
                        instance_osh.setStringAttribute('application_path', self.hana_database.get_instance(host).path)
                        instance_osh.setStringAttribute('product_name', 'hana_database')
                        instance_osh.setStringAttribute('discovered_product_name', 'SAP HanaDB')

                        instances.append(instance_osh)
                        data.append(instance_osh)
                    else:
                        logger.warn("No IPs found for '%s'. Couldn't create node and Hana instance CIs." % sec_host)
                        self.framework.reportWarning("Couldn't retrieve all required replication information")
            except JException, ex:
                logger.error("Error during shell access for retrieving replication information")
                logger.debugException(str(ex))
                stop = errormessages.resolveAndReport(ex.getMessage(), ClientsConsts.SSH_PROTOCOL_NAME, self.framework)
                if stop:
                    return data

            if len(instances) < 1:
                logger.warn("Not enough Hana instance OSHs available for identifying the Hana database. Canceling.")
                return data
            logger.info("Found %s replicated Hana instances" % len(instances))

            database_osh = ObjectStateHolder('hana_db')

            database_osh.setStringAttribute('name', self.hana_database.name)
            database_osh.setStringAttribute('version_description', self.hana_database.version)
            database_osh.setStringAttribute('vendor', self.hana_database.vendor)
            database_osh.setDateAttribute('startup_time', self.hana_database.startup_time)
            database_osh.setStringAttribute('role', 'standby')

            data.append(database_osh)

            for db_instance in instances:
                link = modeling.createLinkOSH('membership', database_osh, db_instance)
                data.append(link)

            replicated = modeling.createLinkOSH('replicated', self.hana_database.hana_osh, database_osh)
            data.append(replicated)
            logger.info("Found a replicated Hana database")
        else:
            logger.info("No replication information available")

        return data


def set_query_parameter(query, data):
    q = Py.newString(query)
    spliter = re.compile("[\?]{2}")
    split = spliter.split(q)
    sb = []
    count = len(split)
    sb.append(split[0])
    idx = 1
    while idx < count:
        sb.append(data)
        sb.append(split[idx])
        idx = idx + 1
    return ''.join(sb)
