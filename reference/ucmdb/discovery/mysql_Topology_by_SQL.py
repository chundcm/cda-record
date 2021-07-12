import logger
import modeling
from java.lang import Boolean
from java.util import Properties
from appilog.common.system.types.vectors import ObjectStateHolderVector
from appilog.common.system.types import ObjectStateHolder
from com.mercury.topaz.cmdb.shared.model.object.id import CmdbObjectID


class DB:
    def __init__(self):
        self.all_database = []

def getDataBase(vector,mysqlClient,mysqlId):
    rs = None
    db = None
    try:
        db = DB()
        rs = mysqlClient.getTable("show databases")
        while rs.next():
            database = ObjectStateHolder("mysql_db")
            database.setContainer(mysqlId)
            database.setAttribute("data_name", rs.getString(1))
            #add database into cash
            db.all_database.append(database)
            vector.add(database)
        rs.close()
    except:
        if rs:
            rs.close()
        logger.debugException('Failed to discover dataBases')
    return db


def getServerProperties(mysqlClient,mysql):
    rs = None
    try:
        rs = mysqlClient.getTable("select version()")
        if rs.next():
            if rs.getString(1).find("-") == -1:
                mysql.setAttribute('version', rs.getString(1))
            else:
                mysql.setAttribute('version', rs.getString(1)[:rs.getString(1).find("-")])
        rs.close()
    except:
        if rs:
            rs.close()
        logger.debugException('Failed to discover Properties')
    return mysql

def getDbUsers(vector,mysqlClient,mysql):
    rs = None
    try:
        rs = mysqlClient.getTable("select distinct user from mysql.user")
        while rs.next():
            login = rs.getString('user')
            user = ObjectStateHolder('dbuser')
            user.setAttribute("data_name",login)
            user.setContainer(mysql)
            vector.add(user)
        rs.close()
    except:
        if rs:
            rs.close()
        logger.debugException('Failed discover users')
    return vector

def getDbTables(vector,mysqlClient,db):
    rs = None
    try:
        for data_base in db.all_database:
            if data_base.getAttributeValue("data_name"):
                exec_sql = 'SELECT TABLE_NAME,TABLE_TYPE,TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = \'' + data_base.getAttributeValue("data_name") +'\''
                rs = mysqlClient.getTable(exec_sql)
                while rs.next():
                    db_table = ObjectStateHolder('dbtable')
                    db_table.setAttribute('name',rs.getString("TABLE_NAME"))
                    db_table.setAttribute('description', rs.getString("TABLE_TYPE"))
                    db_table.setAttribute('dbtable_tablespacename',rs.getString("TABLE_SCHEMA"))
                    db_table.setAttribute('dbtable_owner',rs.getString("TABLE_SCHEMA"))
                    db_table.setContainer(data_base)
                    vector.add(db_table)
                    dbLink = modeling.createLinkOSH('composition', data_base, db_table)
                    vector.add(dbLink)
                rs.close()
    except:
        if rs:
            rs.close()
        logger.debugException('Failed to discover tables')
    return vector

# Destination Data
def DiscoveryMain(Framework):

    CmdbOIDFactory = CmdbObjectID.Factory
    mysqlId = CmdbOIDFactory.restoreObjectID(Framework.getDestinationAttribute('id'))
    mysql = modeling.createOshByCmdbId("mysql", mysqlId)
    # return value
    OshVResult = ObjectStateHolderVector()

    props = Properties()
    instance_name = Framework.getDestinationAttribute('instanceName')
    if instance_name and instance_name != 'NA' and instance_name.find('\\') != -1:
        props.setProperty('sqlprotocol_dbsid', instance_name[instance_name.find('\\') + 1:])
    mysqlClient = Framework.createClient(props)
    #get properties
    OshVResult.add(getServerProperties(mysqlClient, mysql))
    #get database;
    db = getDataBase(OshVResult,mysqlClient, mysqlId)
    #get user
    getDbUsers(OshVResult,mysqlClient,mysql)
    # get tables
    try:
        discoverDbtables = Boolean.parseBoolean(Framework.getParameter('discoverDBTables'))
        if discoverDbtables and db:
            getDbTables(OshVResult, mysqlClient,db)
    except:
        logger.debugException('Failed to discover DB tables')
    finally:
        mysqlClient.close()
    return OshVResult