#coding=utf-8
import logger
import string

import OracleLMSDataModel

from java.lang import String
from java.lang import System
from java.sql import SQLException
from java.sql import  Timestamp
from javax.sql.rowset.serial import SerialBlob
from java.util import ArrayList

from com.hp.ucmdb.discovery.library.common import CollectorsParameters

COLUMNS_LMS_OVERVIEW = 'GROUP,' \
                       'AGGREGATION_LEVEL,' \
                       'ORACLE_CSI,' \
                       'ORACLE_PRODUCT_CATEGORY,' \
                       'MACHINE_ID,' \
                       'VMACHINE_ID,' \
                       'DB_EDITION,' \
                       'DB_NAME,' \
                       'VERSION,' \
                       'OPTIONS_INSTALLED,' \
                       'OPTIONS_IN_USE,' \
                       'PACKS_GRANTED,' \
                       'PACKS_AGREED,' \
                       'APPLICATION_NAME,' \
                       'APPLICATION_STATUS,' \
                       'USER_COUNT_DBA_USERS,' \
                       'USER_COUNT_APPLICATION,' \
                       'SERVER_MANUFACTURER,' \
                       'SERVER_MODEL,' \
                       'OPERATING_SYSTEM,' \
                       'SOCKETS_POPULATED_PHYS,' \
                       'TOTAL_PHYSICAL_CORES,' \
                       'PROCESSOR_IDENTIFIER,' \
                       'PROCESSOR_SPEED,' \
                       'SOCKET_CAPACITY_PHYSICAL,' \
                       'TOTAL_LOGICAL_CORES,' \
                       'PARTITIONING_METHOD,' \
                       'DB_ROLE,' \
                       'SERVER_NAME_IN_THE_CLUSTER,' \
                       'TOP_CONCURRENCY_TIMESTAMP,' \
                       'SESSIONS,' \
                       'INSTANCE_SESSIONS_HIGHWATER,' \
                       'INSTALL_DATE,' \
                       'MEASUREMENT_COMMENT'


COLUMNS_LMS_DETAIL = 'RL_SCRIPT_VERSION,' \
                     'TIMESTAMP,' \
                     'MACHINE_ID,' \
                     'VMACHINE_ID,' \
                     'BANNER,' \
                     'DB_NAME,' \
                     'USER_COUNT,' \
                     'SERVER_MANUFACTURER,' \
                     'SERVER_MODEL,' \
                     'OPERATING_SYSTEM,' \
                     'SOCKETS_POPULATED_PHYS,' \
                     'TOTAL_PHYSICAL_CORES,' \
                     'PROCESSOR_IDENTIFIER,' \
                     'PROCESSOR_SPEED,' \
                     'TOTAL_LOGICAL_CORES,' \
                     'PARTITIONING_METHOD,' \
                     'DB_ROLE,' \
                     'INSTALL_DATE'

COLUMNS_LMS_DBA_USERS = 'USERNAME,' \
                        'USER_ID,' \
                        'DEFAULT_TABLESPACE,' \
                        'TEMPORARY_TABLESPACE,' \
                        'CREATED,' \
                        'PROFILE,' \
                        'EXPIRY_DATE,' \
                        'MACHINE_ID,' \
                        'DB_NAME,' \
                        'TIMESTAMP'

COLUMNS_LMS_OPTIONS = 'MACHINE_ID,' \
                      'DB_NAME,' \
                      'TIMESTAMP,' \
                      'HOST_NAME,' \
                      'INSTANCE_NAME,' \
                      'OPTION_NAME,' \
                      'OPTION_QUERY,' \
                      'SQL_ERR_CODE,' \
                      'SQL_ERR_MESSAGE,' \
                      'COL010,' \
                      'COL020,' \
                      'COL030,' \
                      'COL040,' \
                      'COL050,' \
                      'COL060,' \
                      'COL070,' \
                      'COL080,' \
                      'COL090,' \
                      'COL100,' \
                      'COL110,' \
                      'COL120,' \
                      'COL130,' \
                      'COL140,' \
                      'COL150,' \
                      'COL160'

COLUMNS_LMS_V_LICENSE = 'SESSIONS_MAX,' \
                        'SESSIONS_WARNING,' \
                        'SESSIONS_CURRENT,' \
                        'SESSIONS_HIGHWATER,' \
                        'CPU_COUNT_CURRENT,' \
                        'CPU_COUNT_HIGHWATER,' \
                        'USERS_MAX,' \
                        'MACHINE_ID,' \
                        'DB_NAME,' \
                        'TIMESTAMP'

COLUMNS_LMS_V_SESSION = 'SADDR,' \
                        'SID,' \
                        'PADDR,' \
                        'USERNO,' \
                        'USERNAME,' \
                        'COMMAND,' \
                        'STATUS,' \
                        'SERVER,' \
                        'SCHEMANAME,' \
                        'OSUSER,' \
                        'PROCESS,' \
                        'MACHINE,' \
                        'TERMINAL,' \
                        'PROGRAM,' \
                        'TYPE,' \
                        'LAST_CALL_ET,' \
                        'LOGON_TIME,' \
                        'MACHINE_ID,' \
                        'DB_NAME,' \
                        'TIMESTAMP'



class OracleLMSDataServiceDAO:

    COLUMNS_LMS_OVERVIEW_MYSQL = '''
        `GROUP`,
        `AGGREGATION_LEVEL`,
        `ORACLE_CSI`,
        `ORACLE_PRODUCT_CATEGORY`,
        `MACHINE_ID`,
        `VMACHINE_ID`,
        `DB_EDITION`,
        `DB_NAME`,
        `VERSION`,
        `OPTIONS_INSTALLED`,
        `OPTIONS_IN_USE`,
        `PACKS_GRANTED`,
        `PACKS_AGREED`,
        `APPLICATION_NAME`,
        `APPLICATION_STATUS`,
        `USER_COUNT_DBA_USERS`,
        `USER_COUNT_APPLICATION`,
        `SERVER_MANUFACTURER`,
        `SERVER_MODEL`,
        `OPERATING_SYSTEM`,
        `SOCKETS_POPULATED_PHYS`,
        `TOTAL_PHYSICAL_CORES`,
        `PROCESSOR_IDENTIFIER`,
        `PROCESSOR_SPEED`,
        `SOCKET_CAPACITY_PHYSICAL`,
        `TOTAL_LOGICAL_CORES`,
        `PARTITIONING_METHOD`,
        `DB_ROLE`,
        `SERVER_NAME_IN_THE_CLUSTER`,
        `TOP_CONCURRENCY_TIMESTAMP`,
        `SESSIONS`,
        `INSTANCE_SESSIONS_HIGHWATER`,
        `INSTALL_DATE`,
        `MEASUREMENT_COMMENT`'''


    COLUMNS_LMS_DETAIL_MYSQL = '''
        `RL_SCRIPT_VERSION`,
        `TIMESTAMP`,
        `MACHINE_ID`,
        `VMACHINE_ID`,
        `BANNER`,
        `DB_NAME`,
        `USER_COUNT`,
        `SERVER_MANUFACTURER`,
        `SERVER_MODEL`,
        `OPERATING_SYSTEM`,
        `SOCKETS_POPULATED_PHYS`,
        `TOTAL_PHYSICAL_CORES`,
        `PROCESSOR_IDENTIFIER`,
        `PROCESSOR_SPEED`,
        `TOTAL_LOGICAL_CORES`,
        `PARTITIONING_METHOD`,
        `DB_ROLE`,
        `INSTALL_DATE`'''

    COLUMNS_LMS_DBA_USERS_MYSQL = '''
        `USERNAME`,
        `USER_ID`,
        `DEFAULT_TABLESPACE`,
        `TEMPORARY_TABLESPACE`,
        `CREATED`,
        `PROFILE`,
        `EXPIRY_DATE`,
        `MACHINE_ID`,
        `DB_NAME`,
        `TIMESTAMP`'''

    COLUMNS_LMS_DBA_USERS_MYSQL_SELECT = '''
        `USERNAME`,
        `USER_ID`,
        `DEFAULT_TABLESPACE`,
        `TEMPORARY_TABLESPACE`,
         CASE WHEN `CREATED` = '0000-00-00' THEN NULL ELSE `CREATED` END AS `CREATED`,
        `PROFILE`,
         CASE WHEN `EXPIRY_DATE` = '0000-00-00' THEN NULL ELSE `EXPIRY_DATE` END AS `EXPIRY_DATE`,
        `MACHINE_ID`,
        `DB_NAME`,
        `TIMESTAMP`'''

    COLUMNS_LMS_OPTIONS_MYSQL = '''
        `MACHINE_ID`,
        `DB_NAME`,
        `TIMESTAMP`,
        `HOST_NAME`,
        `INSTANCE_NAME`,
        `OPTION_NAME`,
        `OPTION_QUERY`,
        `SQL_ERR_CODE`,
        `SQL_ERR_MESSAGE`,
        `COL010`,
        `COL020`,
        `COL030`,
        `COL040`,
        `COL050`,
        `COL060`,
        `COL070`,
        `COL080`,
        `COL090`,
        `COL100`,
        `COL110`,
        `COL120`,
        `COL130`,
        `COL140`,
        `COL150`,
        `COL160`'''

    COLUMNS_LMS_V_LICENSE_MYSQL = '''
        `SESSIONS_MAX`,
        `SESSIONS_WARNING`,
        `SESSIONS_CURRENT`,
        `SESSIONS_HIGHWATER`,
        `CPU_COUNT_CURRENT`,
        `CPU_COUNT_HIGHWATER`,
        `USERS_MAX`,
        `MACHINE_ID`,
        `DB_NAME`,
        `TIMESTAMP`'''

    COLUMNS_LMS_V_SESSION_MYSQL = '''
        `SADDR`,
        `SID`,
        `PADDR`,
        `USERNO`,
        `USERNAME`,
        `COMMAND`,
        `STATUS`,
        `SERVER`,
        `SCHEMANAME`,
        `OSUSER`,
        `PROCESS`,
        `MACHINE`,
        `TERMINAL`,
        `PROGRAM`,
        `TYPE`,
        `LAST_CALL_ET`,
        `LOGON_TIME`,
        `MACHINE_ID`,
        `DB_NAME`,
        `TIMESTAMP`'''

    COLUMNS_LMS_OVERVIEW_DB = '''
        LMS_GROUP,
        AGGREGATION_LEVEL,
        ORACLE_CSI,
        ORACLE_PRODUCT_CATEGORY,
        MACHINE_ID,
        VMACHINE_ID,
        DB_EDITION,
        DB_NAME,
        VERSION,
        OPTIONS_INSTALLED,
        OPTIONS_IN_USE,
        PACKS_GRANTED,
        PACKS_AGREED,
        APPLICATION_NAME,
        APPLICATION_STATUS,
        USER_COUNT_DBA_USERS,
        USER_COUNT_APPLICATION,
        SERVER_MANUFACTURER,
        SERVER_MODEL,
        OPERATING_SYSTEM,
        SOCKETS_POPULATED_PHYS,
        TOTAL_PHYSICAL_CORES,
        PROCESSOR_IDENTIFIER,
        PROCESSOR_SPEED,
        SOCKET_CAPACITY_PHYSICAL,
        TOTAL_LOGICAL_CORES,
        PARTITIONING_METHOD,
        DB_ROLE,
        SERVER_NAME_IN_THE_CLUSTER,
        TOP_CONCURRENCY_TIMESTAMP,
        SESSIONS,
        INSTANCE_SESSIONS_HIGHWATER,
        INSTALL_DATE,
        MEASUREMENT_COMMENT'''

    CREATE_LMS_OVERVIEW = '''
        CREATE TABLE IF NOT EXISTS lms_overview
        (ID serial, LMS_GROUP varchar(255) DEFAULT NULL, AGGREGATION_LEVEL varchar(40) DEFAULT NULL, ORACLE_CSI varchar(255) DEFAULT NULL,
        ORACLE_PRODUCT_CATEGORY varchar(40) DEFAULT NULL, MACHINE_ID varchar(255) DEFAULT NULL, VMACHINE_ID varchar(255) DEFAULT NULL, DB_EDITION varchar(40) DEFAULT NULL,
        DB_NAME varchar(40) DEFAULT NULL, VERSION varchar(40) DEFAULT NULL, OPTIONS_INSTALLED varchar(255) DEFAULT NULL, OPTIONS_IN_USE varchar(255) DEFAULT NULL,
        PACKS_GRANTED varchar(255) DEFAULT NULL, PACKS_AGREED varchar(255) DEFAULT NULL, APPLICATION_NAME varchar(255) DEFAULT NULL, APPLICATION_STATUS varchar(255) DEFAULT NULL,
        USER_COUNT_DBA_USERS int DEFAULT NULL, USER_COUNT_APPLICATION int DEFAULT NULL, SERVER_MANUFACTURER varchar(255) DEFAULT NULL, SERVER_MODEL varchar(255) DEFAULT NULL,
        OPERATING_SYSTEM varchar(255) DEFAULT NULL, SOCKETS_POPULATED_PHYS int DEFAULT NULL, TOTAL_PHYSICAL_CORES int DEFAULT NULL, PROCESSOR_IDENTIFIER varchar(255) DEFAULT NULL,
        PROCESSOR_SPEED varchar(40) DEFAULT NULL, SOCKET_CAPACITY_PHYSICAL int DEFAULT NULL, TOTAL_LOGICAL_CORES int DEFAULT NULL, PARTITIONING_METHOD varchar(40) DEFAULT NULL,
        DB_ROLE varchar(255) DEFAULT NULL, SERVER_NAME_IN_THE_CLUSTER varchar(255) DEFAULT NULL, TOP_CONCURRENCY_TIMESTAMP timestamp DEFAULT NULL, SESSIONS int DEFAULT NULL,
        INSTANCE_SESSIONS_HIGHWATER int DEFAULT NULL, INSTALL_DATE timestamp DEFAULT NULL, MEASUREMENT_COMMENT text,
        DISCOVERY_ID varchar(255) DEFAULT NULL, PRIMARY KEY (ID))
    '''

    CREATE_LMS_DETAIL = '''
        CREATE TABLE IF NOT EXISTS lms_detail
        (ID serial, RL_SCRIPT_VERSION varchar(25) DEFAULT NULL, TIMESTAMP timestamp DEFAULT NULL, MACHINE_ID varchar(255) DEFAULT NULL,
        VMACHINE_ID varchar(255) DEFAULT NULL, BANNER varchar(128) DEFAULT NULL, DB_NAME varchar(30) DEFAULT NULL, USER_COUNT int DEFAULT NULL,
        SERVER_MANUFACTURER varchar(255) DEFAULT NULL, SERVER_MODEL varchar(255) DEFAULT NULL, OPERATING_SYSTEM varchar(255) DEFAULT NULL, SOCKETS_POPULATED_PHYS int DEFAULT NULL,
        TOTAL_PHYSICAL_CORES int DEFAULT NULL, PROCESSOR_IDENTIFIER varchar(255) DEFAULT NULL, PROCESSOR_SPEED varchar(255) DEFAULT NULL, TOTAL_LOGICAL_CORES int DEFAULT NULL,
        PARTITIONING_METHOD varchar(255) DEFAULT NULL, DB_ROLE varchar(255) DEFAULT NULL, INSTALL_DATE timestamp DEFAULT NULL,
        DISCOVERY_ID varchar(255) DEFAULT NULL, PRIMARY KEY (ID))
    '''

    CREATE_LMS_OPTIONS = '''
        CREATE TABLE IF NOT EXISTS lms_options
        (ID serial, MACHINE_ID varchar(255) DEFAULT NULL, DB_NAME varchar(40) DEFAULT NULL, TIMESTAMP timestamp DEFAULT NULL, HOST_NAME varchar(255) DEFAULT NULL,
        INSTANCE_NAME varchar(255) DEFAULT NULL, OPTION_NAME varchar(255) DEFAULT NULL, OPTION_QUERY varchar(255) DEFAULT NULL, SQL_ERR_CODE varchar(255) DEFAULT NULL,
        SQL_ERR_MESSAGE varchar(255) DEFAULT NULL, COL010 varchar(1000) DEFAULT NULL, COL020 varchar(255) DEFAULT NULL, COL030 varchar(1000) DEFAULT NULL, COL040 varchar(255) DEFAULT NULL,
        COL050 varchar(255) DEFAULT NULL, COL060 varchar(255) DEFAULT NULL, COL070 varchar(255) DEFAULT NULL, COL080 varchar(255) DEFAULT NULL, COL090 varchar(255) DEFAULT NULL,
        COL100 varchar(255) DEFAULT NULL, COL110 varchar(255) DEFAULT NULL, COL120 varchar(255) DEFAULT NULL, COL130 varchar(255) DEFAULT NULL, COL140 varchar(255) DEFAULT NULL,
        COL150 varchar(255) DEFAULT NULL, COL160 varchar(255) DEFAULT NULL, DISCOVERY_ID varchar(255) DEFAULT NULL, PRIMARY KEY (ID))
    '''

    CREATE_LMS_DBA_USERS = '''
        CREATE TABLE IF NOT EXISTS lms_dba_users
        (ID serial, USERNAME varchar(40) DEFAULT NULL, USER_ID int DEFAULT NULL, DEFAULT_TABLESPACE varchar(40) DEFAULT NULL,
        TEMPORARY_TABLESPACE varchar(40) DEFAULT NULL, CREATED timestamp DEFAULT NULL, PROFILE varchar(40) DEFAULT NULL, EXPIRY_DATE timestamp DEFAULT NULL,
        MACHINE_ID varchar(255) DEFAULT NULL, DB_NAME varchar(30) DEFAULT NULL, TIMESTAMP timestamp DEFAULT NULL,
        DISCOVERY_ID varchar(255) DEFAULT NULL, PRIMARY KEY (ID))
    '''

    CREATE_LMS_LICENSE = '''
        CREATE TABLE IF NOT EXISTS lms_v$license
        (ID serial, SESSIONS_MAX int DEFAULT NULL, SESSIONS_WARNING int DEFAULT NULL, SESSIONS_CURRENT int DEFAULT NULL,
        SESSIONS_HIGHWATER int DEFAULT NULL, CPU_COUNT_CURRENT int DEFAULT NULL, CPU_COUNT_HIGHWATER int DEFAULT NULL, USERS_MAX int DEFAULT NULL,
        MACHINE_ID varchar(255) DEFAULT NULL, DB_NAME varchar(30) DEFAULT NULL, TIMESTAMP timestamp DEFAULT NULL,
        DISCOVERY_ID varchar(255) DEFAULT NULL, PRIMARY KEY (ID))
    '''

    CREATE_LMS_SESSION = '''
        CREATE TABLE IF NOT EXISTS lms_v$session
        (ID serial, SADDR varchar(16) DEFAULT NULL, SID int DEFAULT NULL, PADDR varchar(16) DEFAULT NULL, USERNO int DEFAULT NULL,
        USERNAME varchar(40) DEFAULT NULL, COMMAND int DEFAULT NULL, STATUS varchar(16) DEFAULT NULL, SERVER varchar(18) DEFAULT NULL, SCHEMANAME varchar(40) DEFAULT NULL,
        OSUSER varchar(30) DEFAULT NULL, PROCESS varchar(18) DEFAULT NULL, MACHINE varchar(80) DEFAULT NULL, TERMINAL varchar(32) DEFAULT NULL,
        PROGRAM varchar(80) DEFAULT NULL, TYPE varchar(20) DEFAULT NULL, LAST_CALL_ET int DEFAULT NULL, LOGON_TIME timestamp DEFAULT NULL, MACHINE_ID varchar(255) DEFAULT NULL,
        DB_NAME varchar(30) DEFAULT NULL, TIMESTAMP timestamp DEFAULT NULL, DISCOVERY_ID varchar(255) DEFAULT NULL, PRIMARY KEY (ID))
    '''

    CREATE_LMS_OVERVIEW_MYSQL = '''
        CREATE TABLE IF NOT EXISTS  `probemgr`.`lms_overview`
        (`ID` int(10) unsigned NOT NULL AUTO_INCREMENT, `GROUP` varchar(255) DEFAULT NULL, `AGGREGATION_LEVEL` varchar(40) DEFAULT NULL, `ORACLE_CSI` varchar(255) DEFAULT NULL,
        `ORACLE_PRODUCT_CATEGORY` varchar(40) DEFAULT NULL, `MACHINE_ID` varchar(255) DEFAULT NULL, `VMACHINE_ID` varchar(255) DEFAULT NULL, `DB_EDITION` varchar(40) DEFAULT NULL,
        `DB_NAME` varchar(40) DEFAULT NULL, `VERSION` varchar(40) DEFAULT NULL, `OPTIONS_INSTALLED` varchar(255) DEFAULT NULL, `OPTIONS_IN_USE` varchar(255) DEFAULT NULL,
        `PACKS_GRANTED` varchar(255) DEFAULT NULL, `PACKS_AGREED` varchar(255) DEFAULT NULL, `APPLICATION_NAME` varchar(255) DEFAULT NULL, `APPLICATION_STATUS` varchar(255) DEFAULT NULL,
        `USER_COUNT_DBA_USERS` int(15) DEFAULT NULL, `USER_COUNT_APPLICATION` int(15) DEFAULT NULL, `SERVER_MANUFACTURER` varchar(255) DEFAULT NULL, `SERVER_MODEL` varchar(255) DEFAULT NULL,
        `OPERATING_SYSTEM` varchar(255) DEFAULT NULL, `SOCKETS_POPULATED_PHYS` int(15) DEFAULT NULL, `TOTAL_PHYSICAL_CORES` int(15) DEFAULT NULL, `PROCESSOR_IDENTIFIER` varchar(255) DEFAULT NULL,
        `PROCESSOR_SPEED` varchar(40) DEFAULT NULL, `SOCKET_CAPACITY_PHYSICAL` int(15) DEFAULT NULL, `TOTAL_LOGICAL_CORES` int(15) DEFAULT NULL, `PARTITIONING_METHOD` varchar(40) DEFAULT NULL,
        `DB_ROLE` varchar(255) DEFAULT NULL, `SERVER_NAME_IN_THE_CLUSTER` varchar(255) DEFAULT NULL, `TOP_CONCURRENCY_TIMESTAMP` datetime DEFAULT NULL, `SESSIONS` int(15) DEFAULT NULL,
        `INSTANCE_SESSIONS_HIGHWATER` int(15) DEFAULT NULL, `INSTALL_DATE` datetime DEFAULT NULL, `MEASUREMENT_COMMENT` text, `DISCOVERY_ID` varchar(255) DEFAULT NULL, PRIMARY KEY (`ID`),
        INDEX DISCOVERY_ID (DISCOVERY_ID)) ENGINE=MyISAM DEFAULT CHARSET=utf8
    '''

    CREATE_LMS_DETAIL_MYSQL = '''
        CREATE TABLE IF NOT EXISTS  `probemgr`.`lms_detail`
        (`ID` int(11) NOT NULL AUTO_INCREMENT, `RL_SCRIPT_VERSION` varchar(25) DEFAULT NULL, `TIMESTAMP` datetime DEFAULT NULL, `MACHINE_ID` varchar(255) DEFAULT NULL,
        `VMACHINE_ID` varchar(255) DEFAULT NULL, `BANNER` varchar(128) DEFAULT NULL, `DB_NAME` varchar(30) DEFAULT NULL, `USER_COUNT` int(15) DEFAULT NULL,
        `SERVER_MANUFACTURER` varchar(255) DEFAULT NULL, `SERVER_MODEL` varchar(255) DEFAULT NULL, `OPERATING_SYSTEM` varchar(255) DEFAULT NULL, `SOCKETS_POPULATED_PHYS` int(15) DEFAULT NULL,
        `TOTAL_PHYSICAL_CORES` int(15) DEFAULT NULL, `PROCESSOR_IDENTIFIER` varchar(255) DEFAULT NULL, `PROCESSOR_SPEED` varchar(255) DEFAULT NULL, `TOTAL_LOGICAL_CORES` int(15) DEFAULT NULL,
        `PARTITIONING_METHOD` varchar(255) DEFAULT NULL, `DB_ROLE` varchar(255) DEFAULT NULL, `INSTALL_DATE` datetime DEFAULT NULL, `DISCOVERY_ID` varchar(255) DEFAULT NULL, PRIMARY KEY (`ID`),
        INDEX DISCOVERY_ID (DISCOVERY_ID)) ENGINE=MyISAM DEFAULT CHARSET=utf8
    '''

    CREATE_LMS_OPTIONS_MYSQL = '''
        CREATE TABLE IF NOT EXISTS  `probemgr`.`lms_options`
        (`ID` int(11) NOT NULL AUTO_INCREMENT, `MACHINE_ID` varchar(255) DEFAULT NULL, `DB_NAME` varchar(40) DEFAULT NULL, `TIMESTAMP` datetime DEFAULT NULL, `HOST_NAME` varchar(255) DEFAULT NULL,
        `INSTANCE_NAME` varchar(255) DEFAULT NULL, `OPTION_NAME` varchar(255) DEFAULT NULL, `OPTION_QUERY` varchar(255) DEFAULT NULL, `SQL_ERR_CODE` varchar(255) DEFAULT NULL,
        `SQL_ERR_MESSAGE` varchar(255) DEFAULT NULL, `COL010` varchar(1000) DEFAULT NULL, `COL020` varchar(255) DEFAULT NULL, `COL030` varchar(1000) DEFAULT NULL, `COL040` varchar(255) DEFAULT NULL,
        `COL050` varchar(255) DEFAULT NULL, `COL060` varchar(255) DEFAULT NULL, `COL070` varchar(255) DEFAULT NULL, `COL080` varchar(255) DEFAULT NULL, `COL090` varchar(255) DEFAULT NULL,
        `COL100` varchar(255) DEFAULT NULL, `COL110` varchar(255) DEFAULT NULL, `COL120` varchar(255) DEFAULT NULL, `COL130` varchar(255) DEFAULT NULL, `COL140` varchar(255) DEFAULT NULL,
        `COL150` varchar(255) DEFAULT NULL, `COL160` varchar(255) DEFAULT NULL, `DISCOVERY_ID` varchar(255) DEFAULT NULL, PRIMARY KEY (`ID`),
        INDEX DISCOVERY_ID (DISCOVERY_ID)) ENGINE=MyISAM DEFAULT CHARSET=utf8
    '''

    CREATE_LMS_DBA_USERS_MYSQL = '''
        CREATE TABLE IF NOT EXISTS  `probemgr`.`lms_dba_users`
        (`ID` int(11) NOT NULL AUTO_INCREMENT, `USERNAME` varchar(40) DEFAULT NULL, `USER_ID` int(15) DEFAULT NULL, `DEFAULT_TABLESPACE` varchar(40) DEFAULT NULL,
        `TEMPORARY_TABLESPACE` varchar(40) DEFAULT NULL, `CREATED` datetime DEFAULT NULL, `PROFILE` varchar(40) DEFAULT NULL, `EXPIRY_DATE` datetime DEFAULT NULL,
        `MACHINE_ID` varchar(255) DEFAULT NULL, `DB_NAME` varchar(30) DEFAULT NULL, `TIMESTAMP` datetime DEFAULT NULL, `DISCOVERY_ID` varchar(255) DEFAULT NULL, PRIMARY KEY (`ID`),
        INDEX DISCOVERY_ID (DISCOVERY_ID)) ENGINE=MyISAM DEFAULT CHARSET=utf8
    '''

    CREATE_LMS_LICENSE_MYSQL = '''
        CREATE TABLE IF NOT EXISTS `probemgr`.`lms_v$license`
        (`ID` int(11) NOT NULL AUTO_INCREMENT, `SESSIONS_MAX` int(15) DEFAULT NULL, `SESSIONS_WARNING` int(15) DEFAULT NULL, `SESSIONS_CURRENT` int(15) DEFAULT NULL,
        `SESSIONS_HIGHWATER` int(15) DEFAULT NULL, `CPU_COUNT_CURRENT` int(15) DEFAULT NULL, `CPU_COUNT_HIGHWATER` int(15) DEFAULT NULL, `USERS_MAX` int(15) DEFAULT NULL,
        `MACHINE_ID` varchar(255) DEFAULT NULL, `DB_NAME` varchar(30) DEFAULT NULL, `TIMESTAMP` datetime DEFAULT NULL, `DISCOVERY_ID` varchar(255) DEFAULT NULL, PRIMARY KEY (`ID`),
        INDEX DISCOVERY_ID (DISCOVERY_ID)) ENGINE=MyISAM DEFAULT CHARSET=utf8
    '''

    CREATE_LMS_SESSION_MYSQL = '''
        CREATE TABLE IF NOT EXISTS  `probemgr`.`lms_v$session`
        (`ID` int(11) NOT NULL AUTO_INCREMENT, `SADDR` varchar(16) DEFAULT NULL, `SID` int(15) DEFAULT NULL, `PADDR` varchar(16) DEFAULT NULL, `USERNO` int(15) DEFAULT NULL,
        `USERNAME` varchar(40) DEFAULT NULL, `COMMAND` int(15) DEFAULT NULL, `STATUS` varchar(16) DEFAULT NULL, `SERVER` varchar(18) DEFAULT NULL, `SCHEMANAME` varchar(40) DEFAULT NULL,
        `OSUSER` varchar(30) DEFAULT NULL, `PROCESS` varchar(255) DEFAULT NULL, `MACHINE` varchar(80) DEFAULT NULL, `TERMINAL` varchar(32) DEFAULT NULL,
        `PROGRAM` varchar(80) DEFAULT NULL, `TYPE` varchar(20) DEFAULT NULL, `LAST_CALL_ET` int(15) DEFAULT NULL, `LOGON_TIME` datetime DEFAULT NULL, `MACHINE_ID` varchar(255) DEFAULT NULL,
        `DB_NAME` varchar(30) DEFAULT NULL, `TIMESTAMP` datetime DEFAULT NULL, `DISCOVERY_ID` varchar(255) DEFAULT NULL, PRIMARY KEY (`ID`),
        INDEX DISCOVERY_ID (DISCOVERY_ID)) ENGINE=MyISAM DEFAULT CHARSET=utf8
    '''

    SELECT_LMS_OVERVIEW = 'SELECT ' + COLUMNS_LMS_OVERVIEW_DB + ', DISCOVERY_ID FROM LMS_OVERVIEW WHERE DISCOVERY_ID = ?'
    SELECT_LMS_DETAIL = 'SELECT ' + COLUMNS_LMS_DETAIL + ', DISCOVERY_ID FROM LMS_DETAIL WHERE DISCOVERY_ID = ?'
    SELECT_LMS_DBA_USERS = 'SELECT ' + COLUMNS_LMS_DBA_USERS + ', DISCOVERY_ID FROM LMS_DBA_USERS WHERE DISCOVERY_ID = ?'
    SELECT_LMS_OPTIONS = 'SELECT ' + COLUMNS_LMS_OPTIONS + ', DISCOVERY_ID FROM LMS_OPTIONS WHERE DISCOVERY_ID = ?'
    SELECT_LMS_V_LICENSE = 'SELECT ' + COLUMNS_LMS_V_LICENSE + ', DISCOVERY_ID FROM LMS_V$LICENSE WHERE DISCOVERY_ID = ?'
    SELECT_LMS_V_SESSION = 'SELECT ' + COLUMNS_LMS_V_SESSION + ', DISCOVERY_ID FROM LMS_V$SESSION WHERE DISCOVERY_ID = ?'

    SELECT_LMS_OVERVIEW_MYSQL = 'SELECT ' + COLUMNS_LMS_OVERVIEW_MYSQL + ', DISCOVERY_ID FROM probemgr.LMS_OVERVIEW WHERE DISCOVERY_ID = ?'
    SELECT_LMS_DETAIL_MYSQL = 'SELECT ' + COLUMNS_LMS_DETAIL_MYSQL + ', DISCOVERY_ID FROM probemgr.LMS_DETAIL WHERE DISCOVERY_ID = ?'
    SELECT_LMS_DBA_USERS_MYSQL = 'SELECT ' + COLUMNS_LMS_DBA_USERS_MYSQL_SELECT + ', DISCOVERY_ID FROM probemgr.LMS_DBA_USERS WHERE DISCOVERY_ID = ?'
    SELECT_LMS_OPTIONS_MYSQL = 'SELECT ' + COLUMNS_LMS_OPTIONS_MYSQL + ', DISCOVERY_ID FROM probemgr.LMS_OPTIONS WHERE DISCOVERY_ID = ?'
    SELECT_LMS_V_LICENSE_MYSQL = 'SELECT ' + COLUMNS_LMS_V_LICENSE_MYSQL + ', DISCOVERY_ID FROM probemgr.LMS_V$LICENSE WHERE DISCOVERY_ID = ?'
    SELECT_LMS_V_SESSION_MYSQL = 'SELECT ' + COLUMNS_LMS_V_SESSION_MYSQL + ', DISCOVERY_ID FROM probemgr.LMS_V$SESSION WHERE DISCOVERY_ID = ?'

    DELETE_LMS_OVERVIEW = 'DELETE FROM LMS_OVERVIEW WHERE DISCOVERY_ID= ?'
    DELETE_LMS_DETAIL = 'DELETE FROM LMS_DETAIL WHERE DISCOVERY_ID= ?'
    DELETE_LMS_DBA_USERS = 'DELETE FROM LMS_DBA_USERS WHERE DISCOVERY_ID= ?'
    DELETE_LMS_OPTIONS = 'DELETE FROM LMS_OPTIONS WHERE DISCOVERY_ID= ?'
    DELETE_LMS_V_LICENSE = 'DELETE FROM LMS_V$LICENSE WHERE DISCOVERY_ID= ?'
    DELETE_LMS_V_SESSION = 'DELETE FROM LMS_V$SESSION WHERE DISCOVERY_ID= ?'

    DELETE_LMS_OVERVIEW_MYSQL = 'DELETE FROM probemgr.LMS_OVERVIEW WHERE DISCOVERY_ID= ?'
    DELETE_LMS_DETAIL_MYSQL = 'DELETE FROM probemgr.LMS_DETAIL WHERE DISCOVERY_ID= ?'
    DELETE_LMS_DBA_USERS_MYSQL = 'DELETE FROM probemgr.LMS_DBA_USERS WHERE DISCOVERY_ID= ?'
    DELETE_LMS_OPTIONS_MYSQL = 'DELETE FROM probemgr.LMS_OPTIONS WHERE DISCOVERY_ID= ?'
    DELETE_LMS_V_LICENSE_MYSQL = 'DELETE FROM probemgr.LMS_V$LICENSE WHERE DISCOVERY_ID= ?'
    DELETE_LMS_V_SESSION_MYSQL = 'DELETE FROM probemgr.LMS_V$SESSION WHERE DISCOVERY_ID= ?'

    INSERT_LMS_OVERVIEW = 'INSERT INTO LMS_OVERVIEW (' + COLUMNS_LMS_OVERVIEW_DB + ', DISCOVERY_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    INSERT_LMS_DETAIL = 'INSERT INTO LMS_DETAIL (' + COLUMNS_LMS_DETAIL + ', DISCOVERY_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    INSERT_LMS_DBA_USERS = 'INSERT INTO LMS_DBA_USERS (' + COLUMNS_LMS_DBA_USERS + ', DISCOVERY_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    INSERT_LMS_OPTIONS = 'INSERT INTO LMS_OPTIONS (' + COLUMNS_LMS_OPTIONS + ', DISCOVERY_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    INSERT_LMS_V_LICENSE = 'INSERT INTO LMS_V$LICENSE (' + COLUMNS_LMS_V_LICENSE + ', DISCOVERY_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    INSERT_LMS_V_SESSION = 'INSERT INTO LMS_V$SESSION (' + COLUMNS_LMS_V_SESSION + ', DISCOVERY_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

    INSERT_LMS_OVERVIEW_MYSQL = 'INSERT INTO probemgr.LMS_OVERVIEW (' + COLUMNS_LMS_OVERVIEW_MYSQL + ', DISCOVERY_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    INSERT_LMS_DETAIL_MYSQL = 'INSERT INTO probemgr.LMS_DETAIL (' + COLUMNS_LMS_DETAIL_MYSQL + ', DISCOVERY_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    INSERT_LMS_DBA_USERS_MYSQL = 'INSERT INTO probemgr.LMS_DBA_USERS (' + COLUMNS_LMS_DBA_USERS_MYSQL + ', DISCOVERY_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    INSERT_LMS_OPTIONS_MYSQL = 'INSERT INTO probemgr.LMS_OPTIONS (' + COLUMNS_LMS_OPTIONS_MYSQL + ', DISCOVERY_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    INSERT_LMS_V_LICENSE_MYSQL = 'INSERT INTO probemgr.LMS_V$LICENSE (' + COLUMNS_LMS_V_LICENSE_MYSQL + ', DISCOVERY_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    INSERT_LMS_V_SESSION_MYSQL = 'INSERT INTO probemgr.LMS_V$SESSION (' + COLUMNS_LMS_V_SESSION_MYSQL + ', DISCOVERY_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'


    INSERT_BUFFER = 100

    COUNT_COLUMNS_LMS_OPTIONS = "SELECT COUNT(1) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'lms_options'"

    LMS_Column_LENGTH= "SELECT character_maximum_length FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? and  column_name = ?"

    ALTER_LMS_OPTIONS_MYSQL = '''
        ALTER TABLE `probemgr`.`lms_options`
            ADD `COL110` varchar(255) DEFAULT NULL,
            ADD `COL120` varchar(255) DEFAULT NULL,
            ADD `COL130` varchar(255) DEFAULT NULL,
            ADD `COL140` varchar(255) DEFAULT NULL,
            ADD `COL150` varchar(255) DEFAULT NULL,
            ADD `COL160` varchar(255) DEFAULT NULL
    '''
    ALTER_LMS_OPTIONS = '''
        ALTER TABLE lms_options
            ADD COL110 varchar(255) DEFAULT NULL,
            ADD COL120 varchar(255) DEFAULT NULL,
            ADD COL130 varchar(255) DEFAULT NULL,
            ADD COL140 varchar(255) DEFAULT NULL,
            ADD COL150 varchar(255) DEFAULT NULL,
            ADD COL160 varchar(255) DEFAULT NULL
        '''

    def __init__(self, Framework):
        self.Framework = Framework

    def isMySql(self):
        driver = CollectorsParameters.getValue(CollectorsParameters.KEY_AGENT_PROBE_JDBC_DRIVER)
        if string.find(driver, 'mysql') != -1:
            return True
        return False

    def createTablesIfNotExist(self):
        conn = self.Framework.getProbeDatabaseConnection('createOracleLMSTables')
        createStatement = None
        result = False
        try:
            if self.isMySql():
                createStatement = conn.prepareStatement(OracleLMSDataServiceDAO.CREATE_LMS_OVERVIEW_MYSQL)
            else:
                createStatement = conn.prepareStatement(OracleLMSDataServiceDAO.CREATE_LMS_OVERVIEW)
            createStatement.execute()

            if self.isMySql():
                createStatement = conn.prepareStatement(OracleLMSDataServiceDAO.CREATE_LMS_DETAIL_MYSQL)
            else:
                createStatement = conn.prepareStatement(OracleLMSDataServiceDAO.CREATE_LMS_DETAIL)
            createStatement.execute()

            if self.isMySql():
                createStatement = conn.prepareStatement(OracleLMSDataServiceDAO.CREATE_LMS_OPTIONS_MYSQL)
            else:
                createStatement = conn.prepareStatement(OracleLMSDataServiceDAO.CREATE_LMS_OPTIONS)
            createStatement.execute()

            if self.isMySql():
                createStatement = conn.prepareStatement(OracleLMSDataServiceDAO.CREATE_LMS_DBA_USERS_MYSQL)
            else:
                createStatement = conn.prepareStatement(OracleLMSDataServiceDAO.CREATE_LMS_DBA_USERS)
            createStatement.execute()

            if self.isMySql():
                createStatement = conn.prepareStatement(OracleLMSDataServiceDAO.CREATE_LMS_LICENSE_MYSQL)
            else:
                createStatement = conn.prepareStatement(OracleLMSDataServiceDAO.CREATE_LMS_LICENSE)
            createStatement.execute()

            if self.isMySql():
                createStatement = conn.prepareStatement(OracleLMSDataServiceDAO.CREATE_LMS_SESSION_MYSQL)
            else:
                createStatement = conn.prepareStatement(OracleLMSDataServiceDAO.CREATE_LMS_SESSION)
            createStatement.execute()
            result = True
        except SQLException, ex:
            strException = ex.getMessage()
            logger.error('Failed to create Oracle LMS tables.: ', strException)
        finally:
            if (createStatement is not None):
                createStatement.close()
            conn.close()
        return result

    def getLMSColumnLength(self, table_name, column_name):
        conn = self.Framework.getProbeDatabaseConnection('getLMSColumnLength')
        selectStatement = None
        results = None
        try:
            selectStatement = conn.prepareStatement(OracleLMSDataServiceDAO.LMS_Column_LENGTH)
            selectStatement.setString(1, table_name)
            selectStatement.setString(2, column_name)
            results = selectStatement.executeQuery()
            length = 0
            if (results.next()):
                length = results.getInt(1)
            return length
        except SQLException, ex:
            logger.error('Failed to get column(%s) length in table(%s): ' %(column_name, table_name), ex)
        finally:
            if (results is not None):
                results.close()
            if (selectStatement is not None):
                selectStatement.close()
            conn.close()

    def countLMSOptionsColumns(self):
        conn = self.Framework.getProbeDatabaseConnection('countLMSOptionsColumns')
        selectStatement = None
        results = None
        try:
            selectStatement = conn.prepareStatement(OracleLMSDataServiceDAO.COUNT_COLUMNS_LMS_OPTIONS)
            results = selectStatement.executeQuery()
            count = 0
            if (results.next()):
                count = results.getInt(1)
            return count
        except SQLException, ex:
            logger.error('Failed to get LMS_OPTIONS columns count', ex)
        finally:
            if (results is not None):
                results.close()
            if (selectStatement is not None):
                selectStatement.close()
            conn.close()

    def alterLMSColumnLength(self, table_name, column_name, length):
        conn = self.Framework.getProbeDatabaseConnection('alterLMSColumn')
        alterStatement = None
        result = False
        try:
            if self.isMySql():
                sql = 'ALTER TABLE `probemgr`.`%s` modify %s varchar(%s)' %(table_name, column_name, length)
            else:
                sql = 'ALTER TABLE %s ALTER COLUMN %s TYPE varchar(%s)' %(table_name, column_name, length)

            alterStatement = conn.prepareStatement(sql)
            alterStatement.execute()
            result = True
        except SQLException, ex:
            strException = ex.getMessage()
            logger.error('Failed upgrade column(%s) in table (%s): '  %(column_name, table_name), strException)
        finally:
            if (alterStatement is not None):
                alterStatement.close()
            conn.close()
        return result


    def alterLMSOptions(self):
        conn = self.Framework.getProbeDatabaseConnection('alterLMSOptionsTable')
        alterStatement = None
        result = False
        try:
            if self.isMySql():
                alterStatement = conn.prepareStatement(OracleLMSDataServiceDAO.ALTER_LMS_OPTIONS_MYSQL)
            else:
                alterStatement = conn.prepareStatement(OracleLMSDataServiceDAO.ALTER_LMS_OPTIONS)
            alterStatement.execute()
            result = True
        except SQLException, ex:
            strException = ex.getMessage()
            if not strException.find('already exists'):
                logger.error('Failed upgrade LMS_OPTIONS table.', strException)
                logger.reportError('Failed upgrade LMS_OPTIONS table.')
        finally:
            if (alterStatement is not None):
                alterStatement.close()
            conn.close()
        return result


    def getLMSOverviewByDiscoveryId(self, discovery_id):
        conn = self.Framework.getProbeDatabaseConnection('getLMSOverviewByDiscoveryId')
        selectStatement = None
        results = None
        try:
            if self.isMySql():
                selectStatement = conn.prepareStatement(OracleLMSDataServiceDAO.SELECT_LMS_OVERVIEW_MYSQL)
            else:
                selectStatement = conn.prepareStatement(OracleLMSDataServiceDAO.SELECT_LMS_OVERVIEW)
            selectStatement.setString(1, discovery_id)
            results = selectStatement.executeQuery()
            list = ArrayList()
            while (results.next()):
                commentBlob = SerialBlob(String(results.getString(34)).getBytes())
                list.add(OracleLMSDataModel.LMSOverviewObject(
                    results.getString(1),
                    results.getString(2),
                    results.getString(3),
                    results.getString(4),
                    results.getString(5),
                    results.getString(6),
                    results.getString(7),
                    results.getString(8),
                    results.getString(9),
                    results.getString(10),
                    results.getString(11),
                    results.getString(12),
                    results.getString(13),
                    results.getString(14),
                    results.getString(15),
                    results.getInt(16),
                    results.getInt(17),
                    results.getString(18),
                    results.getString(19),
                    results.getString(20),
                    results.getInt(21),
                    results.getInt(22),
                    results.getString(23),
                    results.getString(24),
                    results.getInt(25),
                    results.getInt(26),
                    results.getString(27),
                    results.getString(28),
                    results.getString(29),
                    results.getDate(30),
                    results.getInt(31),
                    results.getInt(32),
                    results.getDate(33),
                    commentBlob,
                    results.getString(35)
                ))
            return list
        except SQLException, ex:
            logger.error('Failed to get LMS_OVERVIEW records by discoveryId', ex)
        finally:
            if (results is not None):
                results.close()
            if (selectStatement is not None):
                selectStatement.close()
            conn.close()

    def getLMSDetailByDiscoveryId(self, discovery_id):
        conn = self.Framework.getProbeDatabaseConnection('getLMSDetailByDiscoveryId')
        selectStatement = None
        results = None
        try:
            if self.isMySql():
                selectStatement = conn.prepareStatement(OracleLMSDataServiceDAO.SELECT_LMS_DETAIL_MYSQL)
            else:
                selectStatement = conn.prepareStatement(OracleLMSDataServiceDAO.SELECT_LMS_DETAIL)
            selectStatement.setString(1, discovery_id)
            results = selectStatement.executeQuery()
            list = ArrayList()
            while (results.next()):
                list.add(OracleLMSDataModel.LMSDetailObject(
                    results.getString(1),
                    results.getTimestamp(2),
                    results.getString(3),
                    results.getString(4),
                    results.getString(5),
                    results.getString(6),
                    results.getInt(7),
                    results.getString(8),
                    results.getString(9),
                    results.getString(10),
                    results.getInt(11),
                    results.getInt(12),
                    results.getString(13),
                    results.getString(14),
                    results.getInt(15),
                    results.getString(16),
                    results.getString(17),
                    results.getDate(18),
                    results.getString(19)
                ))
            return list
        except SQLException, ex:
            logger.error('Failed to get LMS_DETAIL records by discoveryId', ex)
        finally:
            if (results is not None):
                results.close()
            if (selectStatement is not None):
                selectStatement.close()
            conn.close()

    def getLMSDbaUsersByDiscoveryId(self, discovery_id):
        conn = self.Framework.getProbeDatabaseConnection('getLMSDbaUsersByDiscoveryId')
        selectStatement = None
        results = None
        try:
            if self.isMySql():
                selectStatement = conn.prepareStatement(OracleLMSDataServiceDAO.SELECT_LMS_DBA_USERS_MYSQL)
            else:
                selectStatement = conn.prepareStatement(OracleLMSDataServiceDAO.SELECT_LMS_DBA_USERS)
            selectStatement.setString(1, discovery_id)
            results = selectStatement.executeQuery()
            list = ArrayList()
            while (results.next()):
                try:
                    createDate = results.getDate(5)
                except Exception, exCreateDate:
                    createDate = None
                    logger.error("create date is 0000-00-00 00:00:00. ", exCreateDate)

                try:
                    expireDate = results.getDate(7)
                except Exception, exExpireDate:
                    expireDate = None
                    logger.error("expire date is 0000-00-00 00:00:00. ", exExpireDate)

                list.add(OracleLMSDataModel.LMSDbaUsersObject(
                    results.getString(1),
                    results.getInt(2),
                    results.getString(3),
                    results.getString(4),
                    createDate,
                    results.getString(6),
                    expireDate,
                    results.getString(8),
                    results.getString(9),
                    results.getTimestamp(10),
                    results.getString(11)
                ))
            return list
        except SQLException, ex:
            logger.error('Failed to get LMS_DBA_USERS records by discoveryId', ex)
        finally:
            if (results is not None):
                results.close()
            if (selectStatement is not None):
                selectStatement.close()
            conn.close()

    def getLMSOptionsByDiscoveryId(self, discovery_id):
        conn = self.Framework.getProbeDatabaseConnection('getLMSOptionsByDiscoveryId')
        selectStatement = None
        results = None
        try:
            if self.isMySql():
                selectStatement = conn.prepareStatement(OracleLMSDataServiceDAO.SELECT_LMS_OPTIONS_MYSQL)
            else:
                selectStatement = conn.prepareStatement(OracleLMSDataServiceDAO.SELECT_LMS_OPTIONS)
            selectStatement.setString(1, discovery_id)
            results = selectStatement.executeQuery()
            list = ArrayList()
            while (results.next()):
                list.add(OracleLMSDataModel.LMSOptionsObject(
                    results.getString(1),
                    results.getString(2),
                    results.getTimestamp(3),
                    results.getString(4),
                    results.getString(5),
                    results.getString(6),
                    results.getString(7),
                    results.getString(8),
                    results.getString(9),
                    results.getString(10),
                    results.getString(11),
                    results.getString(12),
                    results.getString(13),
                    results.getString(14),
                    results.getString(15),
                    results.getString(16),
                    results.getString(17),
                    results.getString(18),
                    results.getString(19),
                    results.getString(20),
                    results.getString(21),
                    results.getString(22),
                    results.getString(23),
                    results.getString(24),
                    results.getString(25),
                    results.getString(26)
                ))
            return list
        except SQLException, ex:
            logger.error('Failed to get LMS_OPTIONS records by discoveryId', ex)
        finally:
            if (results is not None):
                results.close()
            if (selectStatement is not None):
                selectStatement.close()
            conn.close()

    def getLMSVLicenseByDiscoveryId(self, discovery_id):
        conn = self.Framework.getProbeDatabaseConnection('getLMSVLicenseByDiscoveryId')
        selectStatement = None
        results = None
        try:
            if self.isMySql():
                selectStatement = conn.prepareStatement(OracleLMSDataServiceDAO.SELECT_LMS_V_LICENSE_MYSQL)
            else:
                selectStatement = conn.prepareStatement(OracleLMSDataServiceDAO.SELECT_LMS_V_LICENSE)
            selectStatement.setString(1, discovery_id)
            results = selectStatement.executeQuery()
            list = ArrayList()
            while (results.next()):
                list.add(OracleLMSDataModel.LMSVLicenseObject(
                    results.getInt(1),
                    results.getInt(2),
                    results.getInt(3),
                    results.getInt(4),
                    results.getInt(5),
                    results.getInt(6),
                    results.getInt(7),
                    results.getString(8),
                    results.getString(9),
                    results.getTimestamp(10),
                    results.getString(11)
                ))
            return list
        except SQLException, ex:
            logger.error('Failed to get LMS_V$LICENSE records by discoveryId', ex)
        finally:
            if (results is not None):
                results.close()
            if (selectStatement is not None):
                selectStatement.close()
            conn.close()

    def getLMSVSessionByDiscoveryId(self, discovery_id):
        conn = self.Framework.getProbeDatabaseConnection('getLMSVSessionByDiscoveryId')
        selectStatement = None
        results = None
        try:
            if self.isMySql():
                selectStatement = conn.prepareStatement(OracleLMSDataServiceDAO.SELECT_LMS_V_SESSION_MYSQL)
            else:
                selectStatement = conn.prepareStatement(OracleLMSDataServiceDAO.SELECT_LMS_V_SESSION)
            selectStatement.setString(1, discovery_id)
            results = selectStatement.executeQuery()
            list = ArrayList()
            while (results.next()):
                list.add(OracleLMSDataModel.LMSVSessionObject(
                    results.getString(1),
                    results.getInt(2),
                    results.getString(3),
                    results.getInt(4),
                    results.getString(5),
                    results.getInt(6),
                    results.getString(7),
                    results.getString(8),
                    results.getString(9),
                    results.getString(10),
                    results.getString(11),
                    results.getString(12),
                    results.getString(13),
                    results.getString(14),
                    results.getString(15),
                    results.getInt(16),
                    results.getTimestamp(17),
                    results.getString(18),
                    results.getString(19),
                    results.getTimestamp(20),
                    results.getString(21)
                ))
            return list
        except SQLException, ex:
            logger.error('Failed to get LMS_V$SESSION records by discoveryId', ex)
        finally:
            if (results is not None):
                results.close()
            if (selectStatement is not None):
                selectStatement.close()
            conn.close()

    def deleteLMSOverviewByDiscoveryId(self, discovery_id):
        conn = self.Framework.getProbeDatabaseConnection('clearLMSOverview')
        deleteStatement = None
        try:
            if self.isMySql():
                deleteStatement = conn.prepareStatement(OracleLMSDataServiceDAO.DELETE_LMS_OVERVIEW_MYSQL)
            else:
                deleteStatement = conn.prepareStatement(OracleLMSDataServiceDAO.DELETE_LMS_OVERVIEW)
            deleteStatement.setString(1, discovery_id)
            deleteStatement.executeUpdate()
            return True
        except SQLException, ex:
            logger.error('Failed clear LMS_OVERVIEW', ex)
        finally:
            if (deleteStatement is not None):
                deleteStatement.close()
            conn.close()
        return False

    def deleteLMSDetailByDiscoveryId(self, discovery_id):
        conn = self.Framework.getProbeDatabaseConnection('clearLMSDetail')
        deleteStatement = None
        try:
            if self.isMySql():
                deleteStatement = conn.prepareStatement(OracleLMSDataServiceDAO.DELETE_LMS_DETAIL_MYSQL)
            else:
                deleteStatement = conn.prepareStatement(OracleLMSDataServiceDAO.DELETE_LMS_DETAIL)
            deleteStatement.setString(1, discovery_id)
            deleteStatement.executeUpdate()
            return True
        except SQLException, ex:
            logger.error('Failed clear LMS_DETAIL', ex)
        finally:
            if (deleteStatement is not None):
                deleteStatement.close()
            conn.close()
        return False


    def deleteLMSDbaUsersByDiscoveryId(self, discovery_id):
        conn = self.Framework.getProbeDatabaseConnection('clearLMSDbaUsers')
        deleteStatement = None
        try:
            if self.isMySql():
                deleteStatement = conn.prepareStatement(OracleLMSDataServiceDAO.DELETE_LMS_DBA_USERS_MYSQL)
            else:
                deleteStatement = conn.prepareStatement(OracleLMSDataServiceDAO.DELETE_LMS_DBA_USERS)
            deleteStatement.setString(1, discovery_id)
            deleteStatement.executeUpdate()
            return True
        except SQLException, ex:
            logger.error('Failed clear LMS_DBA_USERS', ex)
        finally:
            if (deleteStatement is not None):
                deleteStatement.close()
            conn.close()
        return False

    def deleteLMSOptionsByDiscoveryId(self, discovery_id):
        conn = self.Framework.getProbeDatabaseConnection('clearLMSOptions')
        deleteStatement = None
        try:
            if self.isMySql():
                deleteStatement = conn.prepareStatement(OracleLMSDataServiceDAO.DELETE_LMS_OPTIONS_MYSQL)
            else:
                deleteStatement = conn.prepareStatement(OracleLMSDataServiceDAO.DELETE_LMS_OPTIONS)
            deleteStatement.setString(1, discovery_id)
            deleteStatement.executeUpdate()
            return True
        except SQLException, ex:
            logger.error('Failed clear LMS_OPTION', ex)
        finally:
            if (deleteStatement is not None):
                deleteStatement.close()
            conn.close()
        return False

    def deleteLMSVLicenseByDiscoveryId(self, discovery_id):
        conn = self.Framework.getProbeDatabaseConnection('clearLMSVLicense')
        deleteStatement = None
        try:
            if self.isMySql():
                deleteStatement = conn.prepareStatement(OracleLMSDataServiceDAO.DELETE_LMS_V_LICENSE_MYSQL)
            else:
                deleteStatement = conn.prepareStatement(OracleLMSDataServiceDAO.DELETE_LMS_V_LICENSE)
            deleteStatement.setString(1, discovery_id)
            deleteStatement.executeUpdate()
            return True
        except SQLException, ex:
            logger.error('Failed clear LMS_V$LICENSE', ex)
        finally:
            if (deleteStatement is not None):
                deleteStatement.close()
            conn.close()
        return False

    def deleteLMSVSessionByDiscoveryId(self, discovery_id):
        conn = self.Framework.getProbeDatabaseConnection('clearLMSVSession')
        deleteStatement = None
        try:
            if self.isMySql():
                deleteStatement = conn.prepareStatement(OracleLMSDataServiceDAO.DELETE_LMS_V_SESSION_MYSQL)
            else:
                deleteStatement = conn.prepareStatement(OracleLMSDataServiceDAO.DELETE_LMS_V_SESSION)
            deleteStatement.setString(1, discovery_id)
            deleteStatement.executeUpdate()
            return True
        except SQLException, ex:
            logger.error('Failed clear LMS_V$SESSION', ex)
        finally:
            if (deleteStatement is not None):
                deleteStatement.close()
            conn.close()
        return False

    def batchSaveLMSOverview(self, lmsOverviewList):
        successCount = 0
        try:
            successCount = self.batchInsertOverviewRow(lmsOverviewList)
        except SQLException, ex:
            logger.error('Failed save to LMS_OVERVIEW.', ex)
            return 0
        finally:
            lmsOverviewList.clear()
        return successCount

    def batchSaveLMSDetail(self, lmsDetailList):
        successCount = 0
        try:
            successCount = self.batchInsertDetailRow(lmsDetailList)
        except SQLException, ex:
            logger.error('Failed save to LMS_DETAIL.', ex)
            return 0
        finally:
            lmsDetailList.clear()
        return successCount

    def batchSaveLMSDbaUsers(self, lmsDbaUsersList):
        successCount = 0
        try:
            successCount = self.batchInsertDbaUsersRow(lmsDbaUsersList)
        except SQLException, ex:
            logger.error('Failed save to LMS_DBA_USERS.', ex)
            return 0
        finally:
            lmsDbaUsersList.clear()
        return successCount

    def batchSaveLMSOptions(self, lmsOptionsList):
        successCount = 0
        try:
            successCount = self.batchInsertOptionsRow(lmsOptionsList)
        except SQLException, ex:
            logger.error('Failed save to LMS_OPTIONS.', ex)
            return 0
        finally:
            lmsOptionsList.clear()
        return successCount

    def batchSaveLMSVLicense(self, lmsVLicenseList):
        successCount = 0
        try:
            successCount = self.batchInsertVLicenseRow(lmsVLicenseList)
        except SQLException, ex:
            logger.error('Failed save to LMS_V$LICENSE.', ex)
            return 0
        finally:
            lmsVLicenseList.clear()
        return successCount

    def batchSaveLMSVSession(self, lmsVSessionList):
        successCount = 0
        try:
            successCount = self.batchInsertVSessionRow(lmsVSessionList)
        except SQLException, ex:
            logger.error('Failed save to LMS_V$SESSION.', ex)
            return 0
        finally:
            lmsVSessionList.clear()
        return successCount

    def batchInsertOverviewRow(self, list):
        conn = self.Framework.getProbeDatabaseConnection('saveAllLMSVOverview')
        insertStatement = None
        count = 0
        success = 0
        try:
            if self.isMySql():
                insertStatement = conn.prepareStatement(OracleLMSDataServiceDAO.INSERT_LMS_OVERVIEW_MYSQL)
            else:
                insertStatement = conn.prepareStatement(OracleLMSDataServiceDAO.INSERT_LMS_OVERVIEW)
            for obj in list:
                insertStatement.setString(1, obj.group)
                insertStatement.setString(2, obj.aggregationLevel)
                insertStatement.setString(3, obj.oracleCsi)
                insertStatement.setString(4, obj.oracleProductCategory)
                insertStatement.setString(5, obj.machineId)
                insertStatement.setString(6, obj.vmachineId)
                insertStatement.setString(7, obj.dbEdition)
                insertStatement.setString(8, obj.dbName)
                insertStatement.setString(9, obj.version)
                insertStatement.setString(10, obj.optionsInstalled)
                insertStatement.setString(11, obj.optionsInUse)
                insertStatement.setString(12, obj.packsGranted)
                insertStatement.setString(13, obj.packsAgreed)
                insertStatement.setString(14, obj.applicationName)
                insertStatement.setString(15, obj.applicationStatus)
                insertStatement.setInt(16, obj.userCountDbaUsers)
                insertStatement.setInt(17, obj.userCountApplication)
                insertStatement.setString(18, obj.serverManufacturer)
                insertStatement.setString(19, obj.serverModel)
                insertStatement.setString(20, obj.operatingSystem)
                insertStatement.setInt(21, obj.socketsPopulatedPhys)
                insertStatement.setInt(22, obj.totalPhysicalCores)
                insertStatement.setString(23, obj.processorIdentifier)
                insertStatement.setString(24, obj.processorSpeed)
                insertStatement.setInt(25, obj.socketCapacityPhysical)
                insertStatement.setInt(26, obj.totalLogicalCores)
                insertStatement.setString(27, obj.partitioningMethod)
                insertStatement.setString(28, obj.dbRole)
                insertStatement.setString(29, obj.serverNameInTheCluster)
                insertStatement.setDate(30, obj.topConcurrencyTimestamp)
                insertStatement.setInt(31, obj.sessions)
                insertStatement.setInt(32, obj.instanceSessionHighwater)
                insertStatement.setDate(33, obj.installDate)
                commentBlob = obj.measurementComment
                measurementCommentStr = String(commentBlob.getBytes(1, int(commentBlob.length())))
                insertStatement.setString(34, measurementCommentStr)
                insertStatement.setString(35, obj.discoveryId)

                insertStatement.addBatch()
                count += 1
                if (count >= OracleLMSDataServiceDAO.INSERT_BUFFER):
                    insertStatement.executeBatch()
                    count = 0
                success += 1
            list.clear()
            insertStatement.executeBatch()
            return success
        finally:
            if (insertStatement is not None):
                insertStatement.close()
            conn.close()

    def batchInsertDetailRow(self, list):
        conn = self.Framework.getProbeDatabaseConnection('saveAllLMSDetail')
        insertStatement = None
        count = 0
        success = 0
        try:
            if self.isMySql():
                insertStatement = conn.prepareStatement(OracleLMSDataServiceDAO.INSERT_LMS_DETAIL_MYSQL)
            else:
                insertStatement = conn.prepareStatement(OracleLMSDataServiceDAO.INSERT_LMS_DETAIL)
            for obj in list:
                insertStatement.setString(1, obj.rlScriptVersion)
                insertStatement.setTimestamp(2, Timestamp(System.currentTimeMillis()))
                insertStatement.setString(3, obj.machineId)
                insertStatement.setString(4, obj.vmachineId)
                insertStatement.setString(5, obj.banner)
                insertStatement.setString(6, obj.dbName)
                insertStatement.setInt(7, obj.userCount)
                insertStatement.setString(8, obj.serverManufacturer)
                insertStatement.setString(9, obj.serverModel)
                insertStatement.setString(10, obj.operatingSystem)
                insertStatement.setInt(11, obj.socketsPopulatedPhys)
                insertStatement.setInt(12, obj.totalPhysicalCores)
                insertStatement.setString(13, obj.processorIdentifier)
                insertStatement.setString(14, obj.processorSpeed)
                insertStatement.setInt(15, obj.totalLogicalCores)
                insertStatement.setString(16, obj.partitioningMethod)
                insertStatement.setString(17, obj.dbRole)
                insertStatement.setDate(18, obj.installDate)
                insertStatement.setString(19, obj.discoveryId)
                insertStatement.addBatch()
                count += 1
                if (count >= OracleLMSDataServiceDAO.INSERT_BUFFER):
                    insertStatement.executeBatch()
                    count = 0
                success += 1
            list.clear()
            insertStatement.executeBatch()
            return success
        finally:
            if (insertStatement is not None):
                insertStatement.close()
            conn.close()

    def batchInsertDbaUsersRow(self, list):
        conn = self.Framework.getProbeDatabaseConnection('saveAllLDbaUsers')
        insertStatement = None
        count = 0
        success = 0
        try:
            if self.isMySql():
                insertStatement = conn.prepareStatement(OracleLMSDataServiceDAO.INSERT_LMS_DBA_USERS_MYSQL)
            else:
                insertStatement = conn.prepareStatement(OracleLMSDataServiceDAO.INSERT_LMS_DBA_USERS)
            for obj in list:
                insertStatement.setString(1, obj.username)
                insertStatement.setInt(2, obj.userId)
                insertStatement.setString(3, obj.defaultTablespace)
                insertStatement.setString(4, obj.temporaryTablespace)
                insertStatement.setDate(5, obj.created)
                insertStatement.setString(6, obj.profile)
                insertStatement.setDate(7, obj.expiryDate)
                insertStatement.setString(8, obj.machineId)
                insertStatement.setString(9, obj.dbName)
                insertStatement.setTimestamp(10, Timestamp(System.currentTimeMillis()))
                insertStatement.setString(11, obj.discoveryId)

                insertStatement.addBatch()
                count += 1
                if (count >= OracleLMSDataServiceDAO.INSERT_BUFFER):
                    insertStatement.executeBatch()
                    count = 0
                success += 1
            list.clear()
            insertStatement.executeBatch()
            return success
        finally:
            if (insertStatement is not None):
                insertStatement.close()
            conn.close()

    def batchInsertOptionsRow(self, list):
        conn = self.Framework.getProbeDatabaseConnection('saveAllLMSOptions')
        insertStatement = None
        count = 0
        success = 0
        try:
            if self.isMySql():
                insertStatement = conn.prepareStatement(OracleLMSDataServiceDAO.INSERT_LMS_OPTIONS_MYSQL)
            else:
                insertStatement = conn.prepareStatement(OracleLMSDataServiceDAO.INSERT_LMS_OPTIONS)
            for obj in list:
                insertStatement.setString(1, obj.machineId)
                insertStatement.setString(2, obj.dbName)
                insertStatement.setTimestamp(3, Timestamp(System.currentTimeMillis()))
                insertStatement.setString(4, obj.hostName)
                insertStatement.setString(5, obj.instanceName)
                insertStatement.setString(6, obj.optionName)
                insertStatement.setString(7, obj.optionQuery)
                insertStatement.setString(8, obj.sqlErrCode)
                insertStatement.setString(9, obj.sqlErrMessage)
                insertStatement.setString(10, obj.col010)
                insertStatement.setString(11, obj.col020)
                insertStatement.setString(12, obj.col030)
                insertStatement.setString(13, obj.col040)
                insertStatement.setString(14, obj.col050)
                insertStatement.setString(15, obj.col060)
                insertStatement.setString(16, obj.col070)
                insertStatement.setString(17, obj.col080)
                insertStatement.setString(18, obj.col090)
                insertStatement.setString(19, obj.col100)
                insertStatement.setString(20, obj.col110)
                insertStatement.setString(21, obj.col120)
                insertStatement.setString(22, obj.col130)
                insertStatement.setString(23, obj.col140)
                insertStatement.setString(24, obj.col150)
                insertStatement.setString(25, obj.col160)
                insertStatement.setString(26, obj.discoveryId)

                insertStatement.addBatch()
                count += 1
                if (count >= OracleLMSDataServiceDAO.INSERT_BUFFER):
                    insertStatement.executeBatch()
                    count = 0
                success += 1
            list.clear()
            insertStatement.executeBatch()
            return success
        finally:
            if (insertStatement is not None):
                insertStatement.close()
            conn.close()


    def batchInsertVLicenseRow(self, list):
        conn = self.Framework.getProbeDatabaseConnection('saveAllLMSVLicense')
        insertStatement = None
        count = 0
        success = 0
        try:
            if self.isMySql():
                insertStatement = conn.prepareStatement(OracleLMSDataServiceDAO.INSERT_LMS_V_LICENSE_MYSQL)
            else:
                insertStatement = conn.prepareStatement(OracleLMSDataServiceDAO.INSERT_LMS_V_LICENSE)
            for obj in list:
                insertStatement.setInt(1, obj.sessionsMax)
                insertStatement.setInt(2, obj.sessionsWarning)
                insertStatement.setInt(3, obj.sessionsCurrent)
                insertStatement.setInt(4, obj.sessionsHighwater)
                insertStatement.setInt(5, obj.cpuCountCurrent)
                insertStatement.setInt(6, obj.cpuCountHighwater)
                insertStatement.setInt(7, obj.usersMax)
                insertStatement.setString(8, obj.machineId)
                insertStatement.setString(9, obj.dbName)
                insertStatement.setTimestamp(10, Timestamp(System.currentTimeMillis()))
                insertStatement.setString(11, obj.discoveryId)
                insertStatement.addBatch()
                count += 1
                if (count >= OracleLMSDataServiceDAO.INSERT_BUFFER):
                    insertStatement.executeBatch()
                    count = 0
                success += 1
            list.clear()
            insertStatement.executeBatch()
            return success
        finally:
            if (insertStatement is not None):
                insertStatement.close()
            conn.close()

    def batchInsertVSessionRow(self, list):
        conn = self.Framework.getProbeDatabaseConnection('saveAllLMSVSession')
        insertStatement = None
        count = 0
        success = 0
        try:
            if self.isMySql():
                insertStatement = conn.prepareStatement(OracleLMSDataServiceDAO.INSERT_LMS_V_SESSION_MYSQL)
            else:
                insertStatement = conn.prepareStatement(OracleLMSDataServiceDAO.INSERT_LMS_V_SESSION)
            for obj in list:
                insertStatement.setString(1, obj.saddr)
                insertStatement.setInt(2, obj.sid)
                insertStatement.setString(3, obj.paddr)
                insertStatement.setInt(4, obj.userNo)
                insertStatement.setString(5, obj.userName)
                insertStatement.setInt(6, obj.command)
                insertStatement.setString(7, obj.status)
                insertStatement.setString(8, obj.server)
                insertStatement.setString(9, obj.schemaName)
                insertStatement.setString(10, obj.osUser)
                insertStatement.setString(11, obj.process)
                insertStatement.setString(12, obj.machine)
                insertStatement.setString(13, obj.terminal)
                insertStatement.setString(14, obj.program)
                insertStatement.setString(15, obj.type)
                insertStatement.setInt(16, obj.lastCallEt)
                insertStatement.setTimestamp(17, obj.logonTime)
                insertStatement.setString(18, obj.machineId)
                insertStatement.setString(19, obj.dbName)
                insertStatement.setTimestamp(20, Timestamp(System.currentTimeMillis()))
                insertStatement.setString(21, obj.discoveryId)

                insertStatement.addBatch()
                count += 1
                if (count >= OracleLMSDataServiceDAO.INSERT_BUFFER):
                    insertStatement.executeBatch()
                    count = 0
                success += 1
            list.clear()
            insertStatement.executeBatch()
            return success
        finally:
            if (insertStatement is not None):
                insertStatement.close()
            conn.close()