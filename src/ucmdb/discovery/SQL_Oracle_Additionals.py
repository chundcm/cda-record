#coding=utf-8
from java.lang import Exception as JavaException
from java.util import Properties, HashSet
from dbutils import DbTypes
from com.hp.ucmdb.discovery.common import CollectorsConstants
import db
import db_builder
import re
import errormessages
import string
import logger
import modeling
import netutils
import protocol
import SQL_Oracle_Additionals

from appilog.common.system.types.vectors import ObjectStateHolderVector
from com.hp.ucmdb.discovery.library.clients import ClientsConsts, \
    MissingJarsException
from file_ver_lib import resolveMSSQLVersion, resolveDB2Version, resolveOracleVersion
from appilog.common.utils import Protocol
from java.lang import Boolean
#depending on dbType provides DB version resolver and CMDB CI type



# 属性:                   字段            字段类型     SQL 查询语句
# 服务名    :              z_services         string      select name from v$services where clb_goal='LONG';
# 字符集    ：             z_characterset       string      select value from v$nls_parameters where parameter='NLS_CHARACTERSET';
# 补丁版本：               z_dbpatch           string      select comments from dba_registry_history;
# 是否集群：               z_israc               string      select value from v$option a where a.PARAMETER='Real Application Clusters'; (注意原先有个israc_query)
# 会话数    ：             z_sessions           string       select value from v$parameter where name='sessions';
# 进程数    ：             z_processes           string       select value from v$parameter where name='processes';
# 游标数    ：             z_open_cursors       string       select value from v$parameter where name='open_cursors';
# 是否审计：               z_audit_trail       string       select value from v$parameter where name='audit_trail';
# SGA大小：               z_sga_size           string       select display_value from v$parameter where name='sga_target';
# PGA大小：               z_pga_size           string       select display_value from v$parameter where name='pga_aggregate_target';
# shared Pool大小：    z_sharedpool_size   string      select display_value from v$parameter where name='shared_pool_size';
# 归档模式：               z_log_mode           string       select log_mode from v$database;
# 读写模式：               z_open_mode           string       select OPEN_MODE from v$database;
# 数据库角色：           z_dbrole               string       select DATABASE_ROLE from v$database;
# 是否强制日志：           z_force_logging       string       select FORCE_LOGGING from v$database;
# 用户：                   z_users               string       select username from dba_users where account_status = 'OPEN';
# 数据库创建时间        z_createdate       date         select created from v$database;
# 数据库ID：                z_dbid               string       select dbid from v$database;
# 数据库实例内存：          z_memory_target          string       select display_value from v$parameter where name = 'memory_target';


class oracleDiscAddtional:
    def __init__(self):        
        self.oracle_ServiceName_Query = "SELECT NAME FROM V$SERVICES WHERE CLB_GOAL='LONG'"
        self.oracle_Characterset_Query = "SELECT VALUE FROM V$NLS_PARAMETERS WHERE PARAMETER='NLS_CHARACTERSET'"
        self.oracle_dbpatch_Query = "SELECT COMMENTS FROM DBA_REGISTRY_HISTORY"
        self.oracle_isRac_Query = "SELECT VALUE FROM V$OPTION WHERE PARAMETER='Real Application Clusters'"
        self.oracle_sessions_Query = "SELECT VALUE FROM V$PARAMETER WHERE NAME='sessions'"
        self.oracle_processes_Query = "SELECT VALUE FROM V$PARAMETER WHERE NAME='processes'"
        self.oracle_open_cursors_Query = "SELECT VALUE FROM V$PARAMETER WHERE NAME='open_cursors'"        
        self.oracle_audit_trail_Query = "SELECT VALUE FROM V$PARAMETER WHERE NAME='audit_trail'"
        self.oracle_sga_size_Query = "SELECT DISPLAY_VALUE FROM V$PARAMETER WHERE NAME='sga_target'"
        self.oracle_pga_size_Query = "SELECT DISPLAY_VALUE FROM V$PARAMETER WHERE NAME='pga_aggregate_target'"
        self.oracle_shared_pool_Query = "SELECT DISPLAY_VALUE FROM V$PARAMETER WHERE NAME='shared_pool_size'"        
        self.oracle_log_mode_Query = "SELECT LOG_MODE FROM V$DATABASE"
        self.oracle_open_mode_Query = "SELECT OPEN_MODE FROM V$DATABASE"
        self.oracle_dbrole_Query = "SELECT DATABASE_ROLE FROM V$DATABASE"
        self.oracle_force_logging_Query = "SELECT FORCE_LOGGING FROM V$DATABASE"
        self.oracle_users_Query = "SELECT USERNAME FROM DBA_USERS WHERE ACCOUNT_STATUS='OPEN'"
        self.oracle_createdate_Query = "SELECT CREATED FROM V$DATABASE"
        self.oracle_dbid_Query = "select dbid from v$database"
        self.oracle_memory_target_Query = "select display_value from v$parameter where name = 'memory_target'"


    def unique(self, old_list):
        newList = []
        for x in old_list:
            if x not in newList:
                newList.append(x.strip())
        return newList

    ## 获取数据库服务名，有多个值
    def getOracleServiceName(self, dbClient, framework=None):
        logger.debug('---- start to get oracle service name ----')
        oracleServiceName_list = []
        oracleServiceName = ''
        res = None
        try:
            res = dbClient.executeQuery(self.oracle_ServiceName_Query)
            while res.next():
                oracleServiceName_item = res.getString(1)
                oracleServiceName_list.append(oracleServiceName_item.strip())    # 添加oracle 实际到list
            res.close()

            logger.debug('oracleServiceName_list', oracleServiceName_list)
            oracleServiceName_list = self.unique(oracleServiceName_list)     # 去掉重复项
            oracleServiceName_list.sort()    # 按名字排列
            logger.debug('List oracle service name list after sort == ', oracleServiceName_list)

            oracleServiceName = ','.join(oracleServiceName_list)
            logger.debug('Attribute oracle service name == ', oracleServiceName)
        except:
            logger.debugException('')
        finally:
            res.close()
        return oracleServiceName


    ## 获取字符集
    def getOracleCharacterset(self, dbClient, framework=None):
        logger.debug(u'---- start to get oracle Characterset ----')
        oracleCharacterset_list = []
        oracleCharacterset = ''
        try:
            res = dbClient.executeQuery(self.oracle_Characterset_Query)
            while res.next():
                oracleCharacterset_item = res.getString(1)
                oracleCharacterset_list.append(oracleCharacterset_item.strip())    # 添加oracle 实际到list
            res.close()

            logger.debug('oracleCharacterset_list', oracleCharacterset_list)
            oracleCharacterset_list = self.unique(oracleCharacterset_list)     # 去掉重复项
            oracleCharacterset_list.sort()    # 按名字排列
            logger.debug('List oracleCharacterset_list after sort == ', oracleCharacterset_list)

            oracleCharacterset = ','.join(oracleCharacterset_list)
            logger.debug('Attribute oracle character set == ', oracleCharacterset)
        except:
            logger.debugException('')
        finally:
            res.close()
        return oracleCharacterset



    ## 获取数据库补丁版本
    def getOraclePatchVersion(self, dbClient, framework=None):
        logger.debug(u'---- start to get oracle patch version ----')
        oracle_dbpatch_List = []
        oracle_dbpatch = ''
        try:
            res = dbClient.executeQuery(self.oracle_dbpatch_Query)
            while res.next():
                oracle_dbpatch_item = res.getString(1)
                oracle_dbpatch_List.append(oracle_dbpatch_item.strip())    # 添加oracle 实际到list
            res.close()

            logger.debug('Query result oracle_dbpatch_List == ', oracle_dbpatch_List)
            oracle_dbpatch_List = self.unique(oracle_dbpatch_List)     # 去掉重复项
            oracle_dbpatch_List.sort()    # 按名字排列
            logger.debug('List oracle_dbpatch_List after sort == ', oracle_dbpatch_List)

            oracle_dbpatch = ','.join(oracle_dbpatch_List)
            logger.debug('Attribute oracle db patch == ', oracle_dbpatch)
        except:
            logger.debugException('')
        finally:
            res.close()
        return oracle_dbpatch

    ## 获取是否rac
    def getOracleIsRac(self, dbClient, framework=None):
        logger.debug(u'---- start to get oracle is rac ----')
        oracle_isRac = ''
        try:
            res = dbClient.executeQuery(self.oracle_isRac_Query)
            while res.next():
                oracle_isRac = res.getString(1)
            res.close()
            logger.debug('Attribute oracle is Rac == ', oracle_isRac)
        except:
            logger.debugException('')
        finally:
            res.close()
        return oracle_isRac


    ## 获取会话数
    def getOracleSessions(self, dbClient, framework=None):
        logger.debug(u'---- start to get oracle sessions ----')
        oracle_sessions = ''
        try:
            res = dbClient.executeQuery(self.oracle_sessions_Query)
            while res.next():
                oracle_sessions = res.getString(1)
            res.close()
            logger.debug('Attribute oracle sessions == ', oracle_sessions)
        except:
            logger.debugException('')
        finally:
            res.close()
        return oracle_sessions


    ## 获取进程数
    def getOracleProcesses(self, dbClient, framework=None):
        logger.debug(u'---- start to get oracle processes ----')
        oracle_processes = ''
        try:
            res = dbClient.executeQuery(self.oracle_processes_Query)
            while res.next():
                oracle_processes = res.getString(1)
            res.close()
            logger.debug('Attribute oracle processes == ', oracle_processes)
        except:
            logger.debugException('')
        finally:
            res.close()
        return oracle_processes


    ## 获取游标数
    def getOracleOpernCursors(self, dbClient, framework=None):
        logger.debug(u'---- start to get oracle open cursors ----')
        oracle_open_cursors = ''
        try:
            res = dbClient.executeQuery(self.oracle_open_cursors_Query)
            while res.next():
                oracle_open_cursors = res.getString(1)
            res.close()
            logger.debug('Attribute oracle open cursors == ', oracle_open_cursors)
        except:
            logger.debugException('')
        finally:
            res.close()
        return oracle_open_cursors


    ## 获取是否审计
    def getOracleAuditTrails(self, dbClient, framework=None):
        logger.debug(u'---- start to get oracle audit trails ----')
        oracle_audit_trail = ''
        try:
            res = dbClient.executeQuery(self.oracle_audit_trail_Query)
            while res.next():
                oracle_audit_trail = res.getString(1)
            res.close()
            logger.debug('Attribute oracle audit trails == ', oracle_audit_trail)
        except:
            logger.debugException('')
        finally:
            res.close()
        return oracle_audit_trail


    ## 获取SGA大小
    def getOracleSgaSize(self, dbClient, framework=None):
        logger.debug(u'---- start to get oracle sga size ----')
        oracle_sga_size = ''
        try:
            res = dbClient.executeQuery(self.oracle_sga_size_Query)
            while res.next():
                oracle_sga_size = res.getString(1)
            res.close()
            logger.debug('Attribute oracle sga size == ', oracle_sga_size)
        except:
            logger.debugException('')
        finally:
            res.close()
        return oracle_sga_size


    ## 获取PGA大小
    def getOraclePgaSize(self, dbClient, framework=None):
        logger.debug(u'---- start to get oracle pga size ----')
        oracle_pga_size = ''
        try:
            res = dbClient.executeQuery(self.oracle_pga_size_Query)
            while res.next():
                oracle_pga_size = res.getString(1)
            res.close()
            logger.debug('Attribute oracle pga size == ', oracle_pga_size)
        except:
            logger.debugException('')
        finally:
            res.close()
        return oracle_pga_size


    ## 获取共享池大小
    def getOracleSharedPool(self, dbClient, framework=None):
        logger.debug(u'---- start to get oracle shared pool ----')
        oracle_shared_pool = ''
        try:
            res = dbClient.executeQuery(self.oracle_shared_pool_Query)
            while res.next():
                oracle_shared_pool = res.getString(1)
            res.close()
            logger.debug('Attribute oracle shared pool == ', oracle_shared_pool)
        except:
            logger.debugException('')
        finally:
            res.close()
        return oracle_shared_pool


    ## 获取归档模式
    def getOracleLogMode(self, dbClient, framework=None):
        logger.debug(u'---- start to get oracle log mode ----')
        oracle_log_mode = ''
        try:
            res = dbClient.executeQuery(self.oracle_log_mode_Query)
            while res.next():
                oracle_log_mode = res.getString(1)
            res.close()
            logger.debug('Attribute oracle shared pool == ', oracle_log_mode)
        except:
            logger.debugException('')
        finally:
            res.close()
        return oracle_log_mode


    ## 获取读写模式
    def getOracleOpenMode(self, dbClient, framework=None):
        logger.debug(u'---- start to get oracle open mode ----')
        oracle_open_mode = ''
        try:
            res = dbClient.executeQuery(self.oracle_open_mode_Query)
            while res.next():
                oracle_open_mode = res.getString(1)
            res.close()
            logger.debug('Attribute oracle open mode == ', oracle_open_mode)
        except:
            logger.debugException('')
        finally:
            res.close()
        return oracle_open_mode


    ## 获取数据库角色
    def getOracleDbRole(self, dbClient, framework=None):
        logger.debug(u'---- start to get oracle db role ----')
        oracle_dbrole = ''
        try:
            res = dbClient.executeQuery(self.oracle_dbrole_Query)
            while res.next():
                oracle_dbrole = res.getString(1)
            res.close()
            logger.debug('Attribute oracle db role == ', oracle_dbrole)
        except:
            logger.debugException('')
        finally:
            res.close()
        return oracle_dbrole


    ## 获取是否强制日志
    def getOracleForceLogging(self, dbClient, framework=None):
        logger.debug(u'---- start to get oracle force logging ----')
        oracle_force_logging = ''
        try:
            res = dbClient.executeQuery(self.oracle_force_logging_Query)
            while res.next():
                oracle_force_logging = res.getString(1)
            res.close()
            logger.debug('Attribute oracle force logging == ', oracle_force_logging)
        except:
            logger.debugException('')
        finally:
            res.close()
        return oracle_force_logging


    ## 获取数据库用户，有多个值
    def getOracleUsers(self, dbClient, framework=None):
        logger.debug(u'---- start to get oracle users ----')
        oracle_users_list = []
        oracle_users = ''
        try:
            res = dbClient.executeQuery(self.oracle_users_Query)
            while res.next():
                oracle_users_item = res.getString(1)
                oracle_users_list.append(oracle_users_item.strip())    # 添加oracle 实际到list
            res.close()

            logger.debug('oracle_users_list', oracle_users_list)
            oracle_users_list = self.unique(oracle_users_list)     # 去掉重复项
            oracle_users_list.sort()    # 按名字排列
            logger.debug('List oracle users list after sort == ', oracle_users_list)

            oracle_users = ','.join(oracle_users_list)
            logger.debug('Attribute oracle users == ', oracle_users)
        except:
            logger.debugException('')
        finally:
            res.close()
        return oracle_users



    ## 获取数据库创建时间, date 类型
    def getOracleCreatedDate(self, dbClient, framework=None):
        logger.debug(u'---- start to get oracle created date ----')
        oracle_createdate = ''
        try:
            res = dbClient.executeQuery(self.oracle_createdate_Query)
            while res.next():
                oracle_createdate = res.getDate(1)
            res.close()
            logger.debug('Attribute oracle created date == ', oracle_createdate)
        except:
            logger.debugException('')
        finally:
            res.close()
        return oracle_createdate
        

    ## 获取数据库ID
    def getOracleDbId(self, dbClient, framework=None):
        logger.debug(u'---- start to get oracle db id ----')
        oracle_db_id = ''
        try:
            res = dbClient.executeQuery(self.oracle_dbid_Query)
            while res.next():
                oracle_db_id = res.getString(1)
            res.close()
            logger.debug('Attribute oracle db id == ', oracle_db_id)
        except:
            logger.debugException('')
        finally:
            res.close()
        return oracle_db_id 
        

    ## 获取数据库实例内存
    def getOracleMemoryTarget(self, dbClient, framework=None):
        logger.debug(u'---- start to get oracle memory target ----')
        oracle_memory_target = ''
        try:
            res = dbClient.executeQuery(self.oracle_memory_target_Query)
            while res.next():
                oracle_memory_target = res.getString(1)
            res.close()
            logger.debug('Attribute oracle memory target == ', oracle_memory_target)
        except:
            logger.debugException('')
        finally:
            res.close()
        return oracle_memory_target               