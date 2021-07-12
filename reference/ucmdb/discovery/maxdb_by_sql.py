"""
Created on 2015-01-15

@author: Moritz Rupp <moritz.rupp@hp.com>
"""
import errormessages
import logger
import modeling
import sys
from maxdb_topology import MaxDB
from java.sql import SQLException
from java.util import Properties
from appilog.common.system.types.vectors import ObjectStateHolderVector
from com.hp.ucmdb.discovery.library.clients import ClientsConsts

PROTOCOL_NAME = "SQL"


def collect_data(Framework, client, maxdb_osh, discovery_options):
    '''Collects all relevant data from the database in order to create
    the object state holders.
    
    Using SQL connections, all relevant data from the database is collected and 
    stored in corresponding object state holders. Additionally, the relations 
    between the object state holders are created.
    
    Args:
        Framework: The Jython Framework instance from UCMDB
        connection: The object of type DriverMaxDBConnection used for 
            SQL querying
        client: SQL Client
        discovery_options: A dictionary containing the job's parameters and values
        
    Returns:
        A list of ObjectStateHolders. It contains all ObjectStateHolders created during 
        the process of data collection (also the relationships).    
    '''

    data = ObjectStateHolderVector()
    logger.info("Collecting data of MaxDB topology")

    try:
        maxdb = MaxDB(client, maxdb_osh)
    except SQLException, ex:
        logger.error("Could not create MaxDB ObjectStateHolder due to an SQL exception")
        logger.debugException(str(ex))
        errormessages.resolveAndReport(ex.getMessage(), ClientsConsts.SQL_PROTOCOL_NAME, Framework)
        return data

    maxdb_osh = maxdb.get_maxdb()
    data.add(maxdb_osh)

    if discovery_options['databaseconfigurations'].lower() == 'true':
        try:
            data.add(maxdb.get_db_configuration())
        except SQLException, ex:
            logger.error("SQL command for getting MaxDB configurations timed out. Skipping")
            logger.debugException(str(ex))
            stop = errormessages.resolveAndReport(ex.getMessage(), ClientsConsts.SQL_PROTOCOL_NAME, Framework)
            if stop:
                return data
    else:
        logger.info("Discovery of MaxDB configurations disabled")

    if discovery_options['datafiles'].lower() == 'true':
        try:
            data.addAll(maxdb.get_db_data_files())
        except SQLException, ex:
            logger.error("SQL command for getting DB data files timed out. Skipping")
            logger.debugException(str(ex))
            stop = errormessages.resolveAndReport(ex.getMessage(), ClientsConsts.SQL_PROTOCOL_NAME, Framework)
            if stop:
                return data
    else:
        logger.info("Discovery of DB Data Files disabled")

    if discovery_options['logfiles'].lower() == 'true':
        try:
            data.addAll(maxdb.get_db_log_files())
        except SQLException, ex:
            logger.error("SQL command for getting DB log files timed out. Skipping")
            logger.debugException(str(ex))
            stop = errormessages.resolveAndReport(ex.getMessage(), ClientsConsts.SQL_PROTOCOL_NAME, Framework)
            if stop:
                return data
    else:
        logger.info("Discovery of DB Log Files disabled")

    if discovery_options['tracefiles'].lower() == 'true':
        try:
            data.addAll(maxdb.get_db_trace_files())
        except SQLException, ex:
            logger.error("SQL command for getting DB trace files timed out. Skipping")
            logger.debugException(str(ex))
            stop = errormessages.resolveAndReport(ex.getMessage(), ClientsConsts.SQL_PROTOCOL_NAME, Framework)
            if stop:
                return data
    else:
        logger.info("Discovery of DB Trace Files disabled")

    db_users = None
    if discovery_options['users'].lower() == 'true':
        try:
            db_users = maxdb.get_db_users()
            data.addAll(db_users)
        except SQLException, ex:
            logger.error("SQL command for getting DB users timed out. Skipping")
            logger.debugException(str(ex))
            stop = errormessages.resolveAndReport(ex.getMessage(), ClientsConsts.SQL_PROTOCOL_NAME, Framework)
            if stop:
                return data
    else:
        logger.info("Discovery of DB Users disabled")

    if discovery_options['schemas'].lower() == 'true' and db_users:
        try:
            data.addAll(maxdb.get_db_schema(db_users))
        except SQLException, ex:
            logger.error("SQL command for getting DB schemas timed out. Skipping")
            logger.debugException(str(ex))
            stop = errormessages.resolveAndReport(ex.getMessage(), ClientsConsts.SQL_PROTOCOL_NAME, Framework)
            if stop:
                return data
    else:
        logger.info("Discovery of DB Schemas disabled")

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
    discovery_options['tracefiles'] = Framework.getParameter('discoverTraceFiles')
    discovery_options['logfiles'] = Framework.getParameter('discoverLogFiles')
    discovery_options['datafiles'] = Framework.getParameter('discoverDataFiles')
    discovery_options['users'] = Framework.getParameter('discoverUsers')
    discovery_options['schemas'] = Framework.getParameter('discoverSchemas')
    discovery_options['databaseconfigurations'] = Framework.getParameter('discoverDatabaseConfigurations')

    maxdb_id = Framework.getDestinationAttribute('id')
    maxdb_osh = modeling.createOshByCmdbIdString('maxdb', maxdb_id)

    client = None
    try:
        props = Properties()
        instance_name = Framework.getDestinationAttribute('sid')
        if instance_name:
            props.setProperty('sqlprotocol_dbname', instance_name)
        client = Framework.createClient(props)
        OSHVResult.addAll(collect_data(Framework, client, maxdb_osh, discovery_options))
    except:
        excInfo = str(sys.exc_info()[1])
        errormessages.resolveAndReport(excInfo, PROTOCOL_NAME, Framework)
        logger.debug(logger.prepareFullStackTrace(''))
    finally:
        if client:
            client.close()

    return OSHVResult