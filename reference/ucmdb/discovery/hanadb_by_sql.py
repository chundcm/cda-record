"""
Created on 2014-12-19

@author: Moritz Rupp <moritz.rupp@hp.com>
"""
from appilog.common.system.types.vectors import ObjectStateHolderVector
from com.hp.ucmdb.discovery.library.clients import ClientsConsts
from hana_sql_topology import HanaDatabase, Replication
from java.util import Properties
from java.sql import SQLException
import errormessages
import logger
import sys

def collect_data(Framework, client, discovery_options):
    '''Collects all relevant data from the database in order to create
    the object state holders.

    Using SQL connections, all relevant data from the database is collected and
    stored in corresponding object state holders. Additionally, the relations
    between the object state holders are created. For each host running a Hana instance
    the complete topology information is gathered. Then, the Hana database information
    is collecting and finally (if available) replication information is retrieved. If
    available, an additional shell connection is established used for name resolution
    concerning the replication.

    Args:
        Framework: The Jython Framework instance from UCMDB
        client: SQL Client
        discovery_options: A dictionary containing the job's parameters and values

    Returns:
        A list of ObjectStateHolders. It contains all ObjectStateHolders created during
        the process of data collection (also the relationships).
    '''

    data = ObjectStateHolderVector()

    logger.info("Collecting data of Hana topology")
    try:
        hana_database = HanaDatabase(Framework, client, discovery_options)
        hana_osh = hana_database.get_hanadb()
        data.add(hana_osh)

    except SQLException, ex:
        logger.error("Couldn't create Hana database ObjectStateHolder due to an SQL exception")
        logger.debugException(str(ex))
        errormessages.resolveAndReport(ex.getMessage(), ClientsConsts.SQL_PROTOCOL_NAME, Framework)
        return data

    logger.info("Collecting db instances....")
    try:
        data.addAll(hana_database.get_db_instances())
    except SQLException, ex:
        logger.error("Could not fetch information about Hana instances due to an SQL error")
        logger.debugException(str(ex))
        Framework.reportError("No information about connected data instances could be queried.")
        return ObjectStateHolderVector()

    db_users = []
    if discovery_options['users'] == 'true':
        try:
            data.addAll(hana_database.get_db_users(discovery_options['allusersschemas']))
        except SQLException, ex:
            logger.error("SQL command for getting DB users timed out. Skipping")
            logger.debugException(str(ex))
            errormessages.resolveAndReport(ex.getMessage(), ClientsConsts.SQL_PROTOCOL_NAME, Framework)
    else:
        logger.info("Discovery of DB Users disabled")
        
    if discovery_options['schemas'] == 'true':
        try:
            data.addAll(hana_database.get_db_schema(db_users, discovery_options['allusersschemas']))
        except SQLException, ex:
            logger.error("SQL command for getting DB schemas timed out. Skipping")
            logger.debugException(str(ex))
            errormessages.resolveAndReport(ex.getMessage(), ClientsConsts.SQL_PROTOCOL_NAME, Framework)
    else:
        logger.info("Discovery of DB Schemas disabled")
    
    if discovery_options['licenses'] == 'true':
        try:
            data.addAll(hana_database.get_license_information())
        except SQLException, ex:
            logger.error("SQL command for getting DB licenses timed out. Skipping")
            logger.debugException(str(ex))
            errormessages.resolveAndReport(ex.getMessage(), ClientsConsts.SQL_PROTOCOL_NAME, Framework)
    else:
        logger.info("Discovery of Hana licenses disabled")
        
    if discovery_options['databaseconfigurations'] == 'true':
        try:
            data.add(hana_database.get_db_configuration())
        except SQLException, ex:
            logger.error("SQL command for getting Hana database configurations timed out. Skipping")
            logger.debugException(str(ex))
            errormessages.resolveAndReport(ex.getMessage(), ClientsConsts.SQL_PROTOCOL_NAME, Framework)
    else:
        logger.info("Discovery of Hana database configurations disabled")
    
    # Database Replication
    if discovery_options['replication'] == 'true':
        try:
            replication = Replication(Framework, client, hana_database)
            data.addAll(replication.get_replication())
        except SQLException, ex:
            logger.error("Couldn't retrieve replication information due to an SQL command timeout")
            logger.debugException(str(ex))
            errormessages.resolveAndReport(ex.getMessage(), ClientsConsts.SQL_PROTOCOL_NAME, Framework)
    else:
        logger.info("Discovery of Hana replication information disabled")
    return data


def DiscoveryMain(Framework):
    '''DiscoveryMain is the main entry point for the discovery adapter.
    
    Connection parameters and CI data are collected as well as credentials
    are retrieved.
    This information is then used to elaborate connection details (ports,
    credentials), connect to the system and collect the overall data.
    
    Args:
        Framework: The Jython Framework instance from UCMDB
        
    Returns:
        An object of type ObjectStateHolderVector containing all ObjectStateHolders
        to be sent to the UCMDB.
    '''
    
    OSHVResult = ObjectStateHolderVector()

    discovery_options = {}
    discovery_options['timeout'] = Framework.getParameter('queryTimeout')
    discovery_options['allusersschemas'] = Framework.getParameter('discoverAllUsersSchemas')
    discovery_options['tracefiles'] = Framework.getParameter('discoverTraceFiles')
    discovery_options['logfiles'] = Framework.getParameter('discoverLogFiles')
    discovery_options['datafiles'] = Framework.getParameter('discoverDataFiles')
    discovery_options['instanceconfigurations'] = Framework.getParameter('discoverInstanceConfigurations')
    discovery_options['users'] = Framework.getParameter('discoverUsers')
    discovery_options['schemas'] = Framework.getParameter('discoverSchemas')
    discovery_options['databaseconfigurations'] = Framework.getParameter('discoverDatabaseConfigurations')
    discovery_options['licenses'] = Framework.getParameter('discoverLicenses')
    discovery_options['replication'] = Framework.getParameter('discoverReplication')

    client = None
    try:
        props = Properties()
        port = Framework.getDestinationAttribute('port')
        if port:
            props.setProperty('protocol_port', port)
        client = Framework.createClient(props)
        OSHVResult.addAll(collect_data(Framework, client, discovery_options))
    except:
        excInfo = str(sys.exc_info()[1])
        errormessages.resolveAndReport(excInfo, ClientsConsts.SQL_PROTOCOL_NAME, Framework)
        logger.debug(logger.prepareFullStackTrace(''))
    finally:
        if client:
            client.close()

    return OSHVResult

