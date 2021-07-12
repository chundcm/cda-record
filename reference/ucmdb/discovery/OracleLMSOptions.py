#coding=utf-8
import logger
import OracleLMSDataModel
import OracleLMSDBUtils
import OracleLMSUtils

from java.lang import Exception
from java.util import ArrayList

protocolName = "SQL"
sdf = 'YYYY-MM-DD_HH24:MI:SS'

def collectLMSOptionsFromOracle(Framework, oracleClient, machine_id, instance_name, db_name, db_version):
    lmsOptionsList = ArrayList()

    #DB Version
    sql = "SELECT BANNER FROM V$VERSION"
    lmsOptionsList.addAll(getLMSOptions(oracleClient,  sql, 'V$VERSION', '', machine_id, instance_name, db_name, 0, 1))

    #OPTIONS
    sql = "SELECT PARAMETER, VALUE FROM V$OPTION"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'V$OPTION', '', machine_id, instance_name, db_name, 0, 2))

    #DBA_REGISTRY
    sql = "SELECT COMP_NAME, VERSION, STATUS, MODIFIED, SCHEMA FROM DBA_REGISTRY"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'DBA_REGISTRY', '>=9i_r2', machine_id, instance_name, db_name, 0, 5))

    #DBA_FEATURE_USAGE_STATISTICS

    sql = "SELECT NAME, VERSION, DETECTED_USAGES, TOTAL_SAMPLES, CURRENTLY_USED, TO_CHAR(FIRST_USAGE_DATE, '" + sdf + "'), TO_CHAR(LAST_USAGE_DATE, '" + sdf + "'), TO_CHAR(LAST_SAMPLE_DATE, '" + sdf + "'), SAMPLE_INTERVAL, DBID, AUX_COUNT FROM DBA_FEATURE_USAGE_STATISTICS"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'DBA_FEATURE_USAGE_STATISTICS', '10g', machine_id, instance_name, db_name, 0, 11))

    #DBA_FEATURE_USAGE_STATISTICS & FEATURE_INFO
    sql = '''
        select
              replace(replace(replace(to_char(substr(FEATURE_INFO, 1, 1000)), chr(10), '[LF]'), chr(13), '[CR]'),'"',\'\'\'\') as FEATURE_INFO,
              NAME                          ,
              VERSION                       ,
              DBID
          from DBA_FEATURE_USAGE_STATISTICS
          where FEATURE_INFO is not null    
    '''
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'DBA_FEATURE_USAGE_STATISTICS', 'FEATURE_INFO', machine_id, instance_name,db_name, 0, 4))

    #PARTITIONING & PARTITIONED_SEGMENTS
    sql = "SELECT distinct OWNER, OBJECT_TYPE, OBJECT_NAME, min(CREATED), min(LAST_DDL_TIME) from DBA_OBJECTS "\
          "where OBJECT_TYPE LIKE '%PARTITION%' group by OWNER, OBJECT_TYPE, OBJECT_NAME"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'PARTITIONING', 'PARTITIONED_SEGMENTS', machine_id, instance_name, db_name, 1, 5))

    #PARTITIONING & OLAP_AWS_SEGMENTS
    sql = '''
        select distinct
           c.owner as aw_owner,
           c.aw_name,
           'not_collected' as aw_version,
           d.object_type,
           d.owner,
           d.object_name,
           d.object_name as table_name
          from dba_aws      c
          join dba_objects  d on c.owner = d.owner and 'AW$'||c.aw_name = d.object_name
          where d.object_type like '%PARTITION%'
        union all
        select distinct
               e.owner as aw_owner,
               e.aw_name,
               'not_collected' as aw_version,
               g.object_type,
               g.owner,
               g.object_name,
               f.table_name
          from dba_aws            e
          join dba_lobs           f on e.owner = f.owner and 'AW$'||e.aw_name = f.table_name
          join dba_objects        g on f.owner = g.owner and f.segment_name = g.object_name
          where g.object_type like '%PARTITION%'
        union all
        select distinct
               e.owner as aw_owner,
               e.aw_name,
               'not_collected' as aw_version,
               g.object_type,
               g.owner,
               g.object_name,
               f.table_name
          from dba_aws            e
          join dba_indexes        f on e.owner = f.table_owner and 'AW$'||e.aw_name = f.table_name
          join dba_objects        g on f.owner = g.owner and f.index_name = g.object_name
          where g.object_type like '%PARTITION%'
          order by owner, aw_name, object_type, object_name 
    '''
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'PARTITIONING', 'OLAP_AWS_SEGMENTS', machine_id, instance_name, db_name, 1, 7))

    #PARTITIONING & PARTITION_OBJ_RECYCLEBIN
    sql = "SELECT OWNER, ORIGINAL_NAME, OBJECT_NAME, TYPE, CREATETIME, DROPTIME, PARTITION_NAME, SPACE, CAN_UNDROP "\
    "from ( SELECT OWNER, ORIGINAL_NAME, OBJECT_NAME, TYPE, CREATETIME, DROPTIME, PARTITION_NAME, SPACE, CAN_UNDROP "\
        "from DBA_RECYCLEBIN where TYPE not like '%Partition%' "\
          "and (OWNER, OBJECT_NAME) in (SELECT OWNER, OBJECT_NAME from DBA_RECYCLEBIN where TYPE like '%Partition%') "\
      "union all SELECT OWNER, ORIGINAL_NAME, OBJECT_NAME, TYPE, CREATETIME, DROPTIME, PARTITION_NAME, SPACE, CAN_UNDROP "\
        "from DBA_RECYCLEBIN where TYPE like '%Partition%')"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'PARTITIONING', 'PARTITION_OBJ_RECYCLEBIN', machine_id, instance_name, db_name, 1, 9))

    #PARTITIONING - DBA_FLASHBACK_ARCHIVE_TABLES+INDEXES+LOBS
    sql = '''
        select distinct
               c.owner_name as archive_table_owner,
               c.table_name,
               c.archive_table_name,
               d.object_type,
               d.owner,
               d.object_name
          from dba_flashback_archive_tables  c
          join dba_objects  d on c.owner_name = d.owner and c.archive_table_name = d.object_name
        union all
        select distinct
               e.owner_name as archive_table_owner,
               e.table_name,
               e.archive_table_name,
               g.object_type,
               g.owner,
               g.object_name
          from dba_flashback_archive_tables  e
          join dba_lobs           f on e.owner_name = f.owner and e.archive_table_name = f.table_name
          join dba_objects        g on f.owner      = g.owner and f.segment_name       = g.object_name
        union all
        select distinct
               e.owner_name as archive_table_owner,
               e.table_name,
               e.archive_table_name,
               g.object_type,
               g.owner,
               g.object_name
          from dba_flashback_archive_tables  e
          join dba_indexes        f on e.owner_name = f.table_owner and e.archive_table_name = f.table_name
          join dba_objects        g on f.owner      = g.owner       and f.index_name         = g.object_name
    '''
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'PARTITIONING', 'DBA_FLASHBACK_ARCHIVE_TABLES+INDEXES+LOBS', machine_id, instance_name, db_name, 1, 6))

    #PARTITIONING - ALL_CHANGE_TABLES
    sql = '''
        select
                CHANGE_SET_NAME      ,
                SOURCE_SCHEMA_NAME   ,
                SOURCE_TABLE_NAME    ,
                CHANGE_TABLE_SCHEMA  ,
                CHANGE_TABLE_NAME    ,
                CREATED
          from SYS.CDC_CHANGE_TABLES$
    '''
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'PARTITIONING', 'ALL_CHANGE_TABLES', machine_id, instance_name, db_name, 1, 6))


    #PARTITIONING - ALL_CHANGE_TABLES+INDEXES+LOBS
    sql = '''
        select distinct
               c.CHANGE_TABLE_SCHEMA,
               c.CHANGE_TABLE_NAME,
               d.OBJECT_TYPE,
               d.OWNER,
               d.OBJECT_NAME
          from SYS.CDC_CHANGE_TABLES$ c
          join DBA_OBJECTS            d on c.CHANGE_TABLE_SCHEMA = d.OWNER and c.CHANGE_TABLE_NAME = d.OBJECT_NAME
        union all
        select distinct
               e.CHANGE_TABLE_SCHEMA,
               e.CHANGE_TABLE_NAME,
               g.OBJECT_TYPE,
               g.OWNER,
               g.OBJECT_NAME
          from SYS.CDC_CHANGE_TABLES$ e
          join DBA_LOBS               f on e.CHANGE_TABLE_SCHEMA = f.OWNER and e.CHANGE_TABLE_NAME = f.TABLE_NAME
          join DBA_OBJECTS            g on f.OWNER               = g.OWNER and f.segment_name      = g.OBJECT_NAME
        union all
        select distinct
               e.CHANGE_TABLE_SCHEMA,
               e.CHANGE_TABLE_NAME,
               g.OBJECT_TYPE,
               g.OWNER,
               g.OBJECT_NAME
          from SYS.CDC_CHANGE_TABLES$ e
          join DBA_INDEXES            f on e.CHANGE_TABLE_SCHEMA = f.TABLE_OWNER and e.CHANGE_TABLE_NAME = f.TABLE_NAME
          join DBA_OBJECTS            g on f.OWNER               = g.OWNER       and f.INDEX_NAME        = g.OBJECT_NAME
    '''
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'PARTITIONING', 'ALL_CHANGE_TABLES+INDEXES+LOBS', machine_id, instance_name, db_name, 1, 5))

    #PARTITIONING - ALL_CHANGE_SETS_QUEUE_TABLES+INDEXES+LOBS
    sql = '''
        select distinct
               c.SET_NAME as CHANGE_SET_NAME,
               c.PUBLISHER,
               c.QUEUE_TABLE_NAME,
               d.OBJECT_TYPE,
               d.OWNER,
               d.OBJECT_NAME
          from SYS.CDC_CHANGE_SETS$   c
          join DBA_OBJECTS            d on c.PUBLISHER = d.OWNER and c.QUEUE_TABLE_NAME = d.OBJECT_NAME
        union all
        select distinct
               e.SET_NAME as CHANGE_SET_NAME,
               e.PUBLISHER,
               e.QUEUE_TABLE_NAME,
               g.OBJECT_TYPE,
               g.OWNER,
               g.OBJECT_NAME
          from SYS.CDC_CHANGE_SETS$   e
          join DBA_LOBS               f on e.PUBLISHER = f.OWNER and e.QUEUE_TABLE_NAME = f.TABLE_NAME
          join DBA_OBJECTS            g on f.OWNER     = g.OWNER and f.segment_name     = g.OBJECT_NAME
        union all
        select distinct
               e.SET_NAME as CHANGE_SET_NAME,
               e.PUBLISHER,
               e.QUEUE_TABLE_NAME,
               g.OBJECT_TYPE,
               g.OWNER,
               g.OBJECT_NAME
          from SYS.CDC_CHANGE_SETS$   e
          join DBA_INDEXES            f on e.PUBLISHER = f.TABLE_OWNER and e.QUEUE_TABLE_NAME = f.TABLE_NAME
          join DBA_OBJECTS            g on f.OWNER     = g.OWNER       and f.INDEX_NAME       = g.OBJECT_NAME
    '''
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'PARTITIONING', 'ALL_CHANGE_SETS_QUEUE_TABLES+INDEXES+LOBS', machine_id, instance_name, db_name, 1, 6))

    #PARTITIONING - SCHEMA_VERSION_REGISTRY
    sql = '''
        select
                COMP_ID     ,
                COMP_NAME   ,
                MRC_NAME    ,
                MR_NAME     ,
                MR_TYPE     ,
                OWNER       ,
                VERSION     ,
                STATUS      ,
                UPGRADED    ,
                START_TIME  ,
                MODIFIED
          from SCHEMA_VERSION_REGISTRY
    '''
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'PARTITIONING', 'SCHEMA_VERSION_REGISTRY', machine_id, instance_name, db_name, 1, 11))

    #OLAP & ANALYTIC_WORKSPACES
    sql = "SELECT OWNER, AW_NUMBER, AW_NAME, PAGESPACES, GENERATIONS FROM DBA_AWS"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'OLAP', 'ANALYTIC_WORKSPACES', machine_id, instance_name, db_name, 0, 5))

    #RAC & GV$INSTANCE
    sql = "SELECT INSTANCE_NAME, '" + machine_id + "', INST_ID, STATUS FROM GV$INSTANCE"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'RAC', 'GV$INSTANCE', machine_id, instance_name, db_name, 0, 4))

    #LABEL_SECURITY
    sql = "SELECT count(*) FROM LBACSYS.LBAC$POLT WHERE OWNER <> 'SA_DEMO'"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'LABEL_SECURITY', 'LBAC$POLT_COUNT', machine_id, instance_name, db_name, 0, 1))

    #OEM & RUNNING_PROGRAMS
    sql = "SELECT program FROM V$SESSION WHERE upper(program) LIKE '%XPNI.EXE%'"\
    "OR upper(program) LIKE '%VMS.EXE%' OR upper(program) LIKE '%EPC.EXE%' OR upper(program) LIKE '%TDVAPP.EXE%'"\
    "OR upper(program) LIKE 'VDOSSHELL%' OR upper(program) LIKE '%VMQ%' OR upper(program) LIKE '%VTUSHELL%' OR upper(program) LIKE '%JAVAVMQ%'"\
    "OR upper(program) LIKE '%XPAUTUNE%' OR upper(program) LIKE '%XPCOIN%' OR upper(program) LIKE '%XPKSH%' OR upper(program) LIKE '%XPUI%'"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'OEM', 'RUNNING_PROGRAMS', machine_id, instance_name, db_name, 0, 1))

    #OEM & REPOSITORY
    (owners, owners_v2) = getOEMOwnersForRepository(oracleClient)
    for owner in owners:
        sql = "select 'Schema ' || rpad('" + owner + "', 15) || ' has a repository version ' || c_current_version from " + owner + ".smp_rep_version where c_component = 'CONSOLE'"
        lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'OEM', 'REPOSITORY1', machine_id, instance_name, db_name, 0, 1))
    for owner in owners_v2:
        sql = "select 'Schema ' || rpad('" + owner + "', 15) || ' has a repository version 2.x' from dual"
        lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'OEM', 'REPOSITORY2', machine_id, instance_name, db_name, 0, 1))
    if (len(owners) ==0 and len(owners_v2) == 0):
        sql = "select 'There are NO OEM repositories with version prior to 10g on this instance - " + instance_name + " on host " + machine_id + "' from dual"
        lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'OEM', 'REPOSITORY2', machine_id, instance_name, db_name, 0, 1))

    #OEM
    owner = OracleLMSUtils.getOEMOwner(oracleClient)
    #MGMT_ADMIN_LICENSES
    sql = "SELECT b.pack_name, a.pack_label, a.target_type, a.pack_display_label, c.PACK_ACCESS_AGREED FROM " + owner + ".MGMT_LICENSE_DEFINITIONS "\
        "a, " + owner + ".MGMT_ADMIN_LICENSES  b, (SELECT DECODE(COUNT(*), 0, 'NO','YES') AS PACK_ACCESS_AGREED FROM " + owner + ".MGMT_LICENSES "\
        "where upper(I_AGREE)='YES') c WHERE a.pack_label = b.pack_name (+)"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'OEM', 'MGMT_ADMIN_LICENSES', machine_id, instance_name, db_name, 0, 5))
    #MGMT_LICENSES
    sql = "SELECT USERNAME, TIMESTAMP, I_AGREE FROM " + owner + ".MGMT_LICENSES"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'OEM', 'MGMT_LICENSES', machine_id, instance_name, db_name, 0, 3))
    #MGMT_TARGETS
    sql = "SELECT TARGET_NAME, '" + machine_id  + "', LOAD_TIMESTAMP FROM " + owner + ".MGMT_TARGETS WHERE TARGET_TYPE = 'oracle_database'"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'OEM', 'MGMT_TARGETS', machine_id, instance_name, db_name, 0, 3))
    #MGMT_LICENSE_CONFIRMATION
    sql = "SELECT a.CONFIRMATION, a.CONFIRMED_BY, a.CONFIRMED_TIME, b.TARGET_NAME, b.TARGET_TYPE, b.TYPE_DISPLAY_NAME "\
        "FROM " + owner + ".MGMT_LICENSE_CONFIRMATION a, " + owner + ".MGMT_TARGETS b WHERE a.TARGET_GUID = b.TARGET_GUID (+)"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'OEM', 'MGMT_LICENSE_CONFIRMATION', machine_id, instance_name, db_name, 0, 6))
    #SQL_PROFILES
    try:
        enable_profiles = getEnabledProfiles(oracleClient)
        sql = "SELECT " + enable_profiles + ", name, created, last_modified, description ,type, status from dba_sql_profiles"
        lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'OEM', 'SQL_PROFILES', machine_id, instance_name, db_name, 0, 7))
    except Exception:
        logger.warn('failed to get SQL_PROFILES')
    #GRID_CONTROL+11g
    sql = "SELECT tt.type_display_name, '" + machine_id  + "', tg.target_name, ld.pack_display_label, decode(lt.pack_name  , null, 'NO', 'YES'), "\
        "decode(lc.target_guid, null, 'NO', 'YES'), lc.confirmed_time, lc.confirmed_by, tg.target_type, ld.pack_label "\
        "from  " + owner + ".MGMT_TARGETS  tg left outer join " + owner + ".MGMT_TARGET_TYPES  tt on tg.target_type = tt.target_type "\
          "inner join " + owner + ".MGMT_LICENSE_DEFINITIONS  ld on tg.target_type = ld.target_type "\
          "left outer join " + owner + ".MGMT_LICENSED_TARGETS  lt on tg.target_guid = lt.target_guid and ld.pack_label = lt.pack_name "\
          "left outer join " + owner + ".MGMT_LICENSE_CONFIRMATION  lc on tg.target_guid = lc.target_guid "\
        "order by tg.host_name, tt.type_display_name, tg.target_name, ld.pack_display_label"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'OEM', 'GRID_CONTROL+11g', machine_id, instance_name, db_name, 0, 10))

    #SPATIAL
    sql = "SELECT SDO_OWNER, SDO_TABLE_NAME, substr(SDO_COLUMN_NAME, 1, 250) from MDSYS.SDO_GEOM_METADATA_TABLE"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'SPATIAL', 'ALL_SDO_GEOM_METADATA', machine_id, instance_name, db_name, 1, 3))

    # SPATIAL  & SDO_FEATURE_USAGE
    sql = "SELECT FEATURE_NAME, USED FROM MDSYS.SDO_FEATURE_USAGE ORDER BY FEATURE_NAME"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'SPATIAL', 'SDO_FEATURE_USAGE', machine_id, instance_name, db_name, 1, 2))

    #DATA_MINING
    sql = "SELECT count(*) FROM ODM.ODM_MINING_MODEL"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'DATA_MINING', '09i.ODM_MINING_MODEL', machine_id, instance_name, db_name, 0, 1))
    sql = "SELECT count(*) FROM DMSYS.DM$OBJECT"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'DATA_MINING', '10gv1.DM$OBJECT', machine_id, instance_name, db_name, 0, 1))
    sql = "SELECT count(*) FROM DMSYS.DM$P_MODEL"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'DATA_MINING', '10gv2.DM$P_MODEL', machine_id, instance_name, db_name, 0, 1))
    sql = "SELECT count(*) FROM SYS.MODEL$"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'DATA_MINING', '11g.DM$P_MODEL', machine_id, instance_name, db_name, 0, 1))

    #DATA_MINING  & 11g+.DBA_MINING_MODELS
    sql = "SELECT OWNER, MODEL_NAME, MINING_FUNCTION, ALGORITHM, TO_CHAR(CREATION_DATE, '" + sdf + "'), BUILD_DURATION, MODEL_SIZE from SYS.DBA_MINING_MODELS order by OWNER, MODEL_NAME"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'DATA_MINING', '11g+.DBA_MINING_MODELS', machine_id, instance_name, db_name, 1, 7))

    dbaUsersSource = getDBAUsersSource(oracleClient)

    #DATABASE_VAULT  & DVSYS_SCHEMA
    sql = "SELECT MAX(USERNAME) FROM " + dbaUsersSource + " WHERE UPPER(username)='DVSYS'"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'DATABASE_VAULT', 'DVSYS_SCHEMA', machine_id, instance_name, db_name, 0, 1))

    #DATABASE_VAULT  & DVF_SCHEMA
    sql = "SELECT UPPER(USERNAME) FROM " + dbaUsersSource + " WHERE UPPER(username)='DVF'"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'DATABASE_VAULT', 'DVF_SCHEMA', machine_id, instance_name, db_name, 0, 1))

    #DATABASE_VAULT  & DVSYS.DBA_DV_REALM
    sql = "SELECT NAME, DESCRIPTION, ENABLED FROM DVSYS.DBA_DV_REALM ORDER BY NAME"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'DATABASE_VAULT', 'DVSYS.DBA_DV_REALM', machine_id, instance_name, db_name, 0, 3))


    #AUDIT VAULT
    sql = "select MAX(USERNAME) FROM "+ dbaUsersSource +" WHERE UPPER(USERNAME)='AVSYS'"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'AUDIT_VAULT*', 'AVSYS_SCHEMA', machine_id, instance_name, db_name, 0, 1))

    #CONTENT DATABASE and RECORDS DATABASE
    sql = "select MAX(USERNAME) FROM "+ dbaUsersSource +" WHERE UPPER(USERNAME)='CONTENT'"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'CONTENT_AND_RECORDS', 'CONTENT_SCHEMA', machine_id, instance_name, db_name, 0, 1))

    #CONTENT
    sql = "select count(*) FROM ODM_DOCUMENT"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'CONTENT_DATABASE', 'ODM_DOCUMENT', machine_id, instance_name, db_name, 0, 1))

    #RECORDS
    sql = "select count(*) FROM ODM_RECORD"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'RECORDS_DATABASE', 'ODM_RECORD', machine_id, instance_name, db_name, 0, 1))

    #CPU/CORES/SOCKETS (For 10g_r2 and higher)
    sql =  "SELECT                       "\
           "  SESSIONS_HIGHWATER,        "\
           "  CPU_COUNT_CURRENT,         "\
           "  CPU_CORE_COUNT_CURRENT,    "\
           "  CPU_SOCKET_COUNT_CURRENT,  "\
           "  CPU_COUNT_HIGHWATER,         "\
           "  CPU_CORE_COUNT_HIGHWATER,  "\
           "  CPU_SOCKET_COUNT_HIGHWATER "\
           "FROM                         "\
           "   V$LICENSE                 "
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'CPU_CORES_SOCKETS', '10g_r2.V$LICENSE', machine_id, instance_name, db_name, 0, 7))

    #ADVANCED SECURITY
    sql = "SELECT OWNER, TABLE_NAME, COLUMN_NAME FROM DBA_ENCRYPTED_COLUMNS "\
          "WHERE OWNER || '#' || TABLE_NAME|| '#' || COLUMN_NAME NOT IN "\
                 "(SELECT OWNER || '#' || TABLE_NAME|| '#' || COLUMN_NAME FROM DBA_LOBS)"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'ADVANCED_SECURITY', 'COLUMN_ENCRYPTION', machine_id, instance_name, db_name, 0, 3))

    sql = "SELECT TABLESPACE_NAME, ENCRYPTED FROM DBA_TABLESPACES WHERE ENCRYPTED ='YES'"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'ADVANCED_SECURITY', 'TABLESPACE_ENCRYPTION', machine_id, instance_name, db_name, 0, 2))

    sql = '''
        select
               OBJECT_OWNER       ,
               OBJECT_NAME        ,
               POLICY_NAME        ,
               ENABLE             ,
               POLICY_DESCRIPTION
          from REDACTION_POLICIES
          order by OBJECT_OWNER, OBJECT_NAME, POLICY_NAME    
    '''
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'ADVANCED_SECURITY', 'REDACTION_POLICIES', machine_id, instance_name, db_name, 0, 5))

    # GV$PARAMETER
    sql = "SELECT         " \
          "  INST_ID,     " \
          "  NAME,        " \
          "  replace(VALUE,'\"','''') as VALUE,       " \
          "  ISDEFAULT,   " \
          "  DESCRIPTION  " \
          "FROM           " \
          "  GV$PARAMETER " \
          "WHERE UPPER(name) LIKE '%CPU_COUNT%' " \
          "   OR UPPER(name) LIKE '%FAL_CLIENT%' " \
          "   OR UPPER(name) LIKE '%FAL_SERVER%' " \
          "   OR UPPER(name) LIKE '%CLUSTER%'" \
          "   OR UPPER(name) LIKE '%CONTROL_MANAGEMENT_PACK_ACCESS%' " \
          "   OR UPPER(name) LIKE '%ENABLE_DDL_LOGGING%' " \
          "   OR UPPER(name) LIKE '%COMPATIBLE%' " \
          "   OR UPPER(name) LIKE '%LOG_ARCHIVE_DEST%' " \
          "   OR UPPER(name) LIKE '%O7_DICTIONARY_ACCESSIBILITY%' " \
          "   OR UPPER(name) LIKE '%ENABLE_PLUGGABLE_DATABASE%' " \
          "   OR UPPER(name) LIKE '%INMEMORY%' " \
          "   OR UPPER(NAME) like '%DB_UNIQUE_NAME%' " \
          "   OR UPPER(NAME) like '%LOG_ARCHIVE_CONFIG%' " \
          "   OR UPPER(NAME) like '%HEAT_MAP%' " \
          "   OR UPPER(NAME) like '%SPATIAL_VECTOR_ACCELERATION%' " \
          "   OR UPPER(NAME) like '%ENCRYPT_NEW_TABLESPACES%' " \
          "   ORDER BY NAME, INST_ID"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'GV$PARAMETER', '', machine_id, instance_name, db_name, 0, 5))

    #OWB
    owners = getOWBOwners(oracleClient)
    for owner in owners:
        sql = "select 'Schema contains " + owner + " a version ' || installedversion || ' repository' from " + owner + ".CMPInstallation_v where name = 'Oracle Warehouse Builder'"
        lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'OWB', 'REPOSITORY', machine_id, instance_name, db_name, 0, 1))

    #DBA_CPU_USAGE_STATISTICS (For 10g_r2 and higher)
    sql =  "SELECT VERSION, TIMESTAMP, CPU_COUNT, CPU_CORE_COUNT, CPU_SOCKET_COUNT FROM DBA_CPU_USAGE_STATISTICS"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'DBA_CPU_USAGE_STATISTICS', 'DBA_CPU_USAGE_STATISTICS', machine_id, instance_name, db_name, 0, 5))

    #Active Data Guard & 11gr1
    sql = "select " \
          " a.DEST_ID        , " \
          " a.DEST_NAME      , " \
          " a.STATUS         , " \
          " a.TYPE           , " \
          " a.DATABASE_MODE  , " \
          " a.RECOVERY_MODE  , " \
          " a.DESTINATION    , " \
          " a.DB_UNIQUE_NAME , " \
          " b.VALUE " \
          "from V$ARCHIVE_DEST_STATUS a, V$PARAMETER b " \
          "where b.NAME = 'compatible' and b.VALUE like '1%' and b.VALUE not like '10%' " \
          "   and a.RECOVERY_MODE like 'MANAGED%' and a.STATUS = 'VALID' and a.DATABASE_MODE = 'OPEN_READ-ONLY' " \
          "order by a.DEST_ID "
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'ACTIVE_DATA_GUARD', '11gr1', machine_id, instance_name, db_name, 1, 9))
    #Active Data Guard & V$DATABASE
    sql = "select DBID, NAME, DB_UNIQUE_NAME, OPEN_MODE, DATABASE_ROLE, REMOTE_ARCHIVE, DATAGUARD_BROKER, " \
          "GUARD_STATUS, PLATFORM_NAME, CREATED, CONTROLFILE_CREATED " \
          "from V$DATABASE "
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'ACTIVE_DATA_GUARD', 'V$DATABASE', machine_id, instance_name, db_name, 1, 11))

    #Advanced Compression Option (introduced in 11g r1) & SECUREFILES_COMPRESSION_AND_DEDUPLICATION
    sql = "select " \
              "'DBA_TABLES' as SOURCE_, owner, table_name, column_name, compression, deduplication " \
              "from DBA_LOBS " \
              "where compression   not in ('NO', 'NONE')" \
              "or deduplication not in ('NO', 'NONE')" \
          "union all " \
          "select " \
              "'DBA_LOB_PARTITIONS'    as SOURCE_, table_owner, table_name, column_name, compression, deduplication " \
              "from DBA_LOB_PARTITIONS " \
              "where compression   not in ('NO', 'NONE') " \
              "or deduplication not in ('NO', 'NONE') " \
          "union all " \
          "select " \
              "'DBA_LOB_SUBPARTITIONS' as SOURCE_, table_owner, table_name, column_name, compression, deduplication " \
              "from DBA_LOB_SUBPARTITIONS " \
              "where compression   not in ('NO', 'NONE') " \
              "or deduplication not in ('NO', 'NONE') "
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'ADVANCED_COMPRESSION', 'SECUREFILES_COMPRESSION_AND_DEDUPLICATION', machine_id, instance_name, db_name, 1, 6))

    #Advanced Compression Option (introduced in 11g r1) & TABLE_COMPRESSION
    sql = "select 'DBA_TABLES' as source_, owner, table_name, '' as partition_name, compression, compress_for " \
              "from DBA_TABLES " \
              "where compress_for in ('FOR ALL OPERATIONS', 'OLTP', 'ADVANCED') " \
          "union all " \
          "select 'DBA_TAB_PARTITIONS' as source_, table_owner, table_name, partition_name, compression, compress_for " \
              "from DBA_TAB_PARTITIONS " \
              "where compress_for in ('FOR ALL OPERATIONS', 'OLTP', 'ADVANCED') " \
          "union all " \
              "select 'DBA_TAB_SUBPARTITIONS' as source_, table_owner, table_name, partition_name, compression, compress_for " \
              "from DBA_TAB_SUBPARTITIONS " \
              "where compress_for in ('FOR ALL OPERATIONS', 'OLTP', 'ADVANCED')"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'ADVANCED_COMPRESSION', 'TABLE_COMPRESSION', machine_id, instance_name, db_name, 1, 6))

    #Advanced Compression Option (introduced in 11g r1) & DBA_INDEXES.COMPRESSION
    sql = '''
        select
               OWNER        ,
               INDEX_NAME   ,
               TABLE_OWNER  ,
               TABLE_NAME   ,
               COMPRESSION
          from DBA_INDEXES
          where COMPRESSION like '%ADVANCED%'    
    '''
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'ADVANCED_COMPRESSION', 'DBA_INDEXES.COMPRESSION', machine_id, instance_name, db_name, 1, 5))

    #PATCHES
    sql = "select ACTION_TIME, ACTION, NAMESPACE, VERSION, ID, COMMENTS from SYS.REGISTRY$HISTORY order by ACTION_TIME"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'PATCHES', 'SYS.REGISTRY$HISTORY', machine_id, instance_name, db_name, 1, 6))

    #Check User Privileges, for troubleshooting & USER_SYS_PRIVS
    sql = "select USERNAME, PRIVILEGE from USER_SYS_PRIVS"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'USER_PRIVS', 'USER_SYS_PRIVS', machine_id, instance_name, db_name, 1, 2))
    #Check User Privileges, for troubleshooting & USER_ROLE_PRIVS
    sql = "select USERNAME, GRANTED_ROLE from USER_ROLE_PRIVS"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'USER_PRIVS', 'USER_ROLE_PRIVS', machine_id, instance_name, db_name, 1, 2))
    #Check User Privileges, for troubleshooting & ROLE_SYS_PRIV
    sql = "select ROLE, PRIVILEGE from ROLE_SYS_PRIVS"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'USER_PRIVS', 'ROLE_SYS_PRIVS', machine_id, instance_name, db_name, 1, 2))

    sql = "select REALM_NAME, AUTH_RULE_SET_NAME, AUTH_OPTIONS from DVSYS.DBA_DV_REALM_AUTH where GRANTEE=USER order by REALM_NAME"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'USER_PRIVS', 'DVSYS.DBA_DV_REALM_AUTH', machine_id, instance_name, db_name, 1, 3))

    #MULTITENANT (introduced in 12c_r1)
    sql = "select b.CDB, a.CON_ID, a.NAME, a.OPEN_MODE, a.OPEN_TIME, " \
          "decode(a.CON_ID, 0, 'entire CDB or non-CDB', 1, 'ROOT', 2, 'SEED', 'PDB') as container_type " \
          "from V$CONTAINERS a, V$DATABASE b"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'MULTITENANT', 'V$CONTAINERS', machine_id, instance_name, db_name, 1, 6))

    #Advanced Compression "Flashback Data Archive (Total Recall)" feature usage
    sql = "select a.FLASHBACK_ARCHIVE_NAME, b.TABLESPACE_NAME, b.QUOTA_IN_MB, a.RETENTION_IN_DAYS, a.CREATE_TIME, a.LAST_PURGE_TIME, a.STATUS "\
          "from DBA_FLASHBACK_ARCHIVE a "\
          "left join DBA_FLASHBACK_ARCHIVE_TS b on a.FLASHBACK_ARCHIVE# = b.FLASHBACK_ARCHIVE#"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'ADVANCED_COMPRESSION', 'DBA_FLASHBACK_ARCHIVE', machine_id, instance_name, db_name, 1, 7))

    #Advanced Compression "Flashback Data Archive (Total Recall)" feature usage
    sql = "select FLASHBACK_ARCHIVE_NAME, OWNER_NAME, TABLE_NAME, ARCHIVE_TABLE_NAME "\
          "from DBA_FLASHBACK_ARCHIVE_TABLES"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'ADVANCED_COMPRESSION', 'DBA_FLASHBACK_ARCHIVE_TABLES', machine_id, instance_name, db_name, 1, 4))

    #Management Pack Usage Statistics 12c Cloud Control
    sql = "select reg.feature_name, tgts.target_name, tgts.display_name, tgts.type_display_name, tgts.host_name, " \
              "DECODE(stat.isused, 1, 'TRUE', 'FALSE'), stat.detected_samples, stat.total_samples, stat.last_usage_date, " \
              "stat.first_sample_date, stat.last_sample_date, reg.feature_id " \
          "from SYSMAN.mgmt_fu_registrations reg, SYSMAN.mgmt_fu_statistics stat, SYSMAN.mgmt_targets tgts " \
          "where (stat.isused = 1 or stat.detected_samples > 0) " \
             "and stat.target_guid = tgts.target_guid " \
             "and reg.feature_id = stat.feature_id " \
             "and reg.collection_mode = 2"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'OEM', 'PACK_USAGE', machine_id, instance_name, db_name, 1, 12))
    #Management Pack Feature Usage Statistics 12c Cloud Control
    sql = "select reg.feature_name, tgts.target_name, tgts.display_name, tgts.type_display_name, tgts.host_name, " \
              "DECODE(stat.isused, 1, 'TRUE', 'FALSE'), freg.feature_name, DECODE(f_stats.isused, 1, 'TRUE', 'FALSE'), " \
              "f_stats.detected_samples, f_stats.total_samples, f_stats.last_usage_date, f_stats.first_sample_date, " \
              "f_stats.last_sample_date, lmap.pack_label, lmap.pack_id, lmap.feature_id " \
          "from SYSMAN.mgmt_fu_registrations reg, " \
              "SYSMAN.mgmt_fu_statistics    stat, " \
              "SYSMAN.mgmt_targets          tgts, " \
              "SYSMAN.mgmt_fu_statistics    f_stats, " \
              "SYSMAN.mgmt_fu_registrations freg, " \
              "SYSMAN.mgmt_fu_license_map   lmap " \
          "where (stat.isused = 1 or stat.detected_samples > 0 or f_stats.isused = 1 or f_stats.detected_samples > 0) " \
              "and stat.target_guid = tgts.target_guid " \
              "and reg.feature_id = stat.feature_id " \
              "and reg.collection_mode = 2 " \
              "and lmap.pack_id = reg.feature_id " \
              "and lmap.feature_id = freg.feature_id " \
              "and freg.feature_id = f_stats.feature_id " \
              "and f_stats.target_guid = tgts.target_guid"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'OEM', 'PACK_FEATURE_USAGE', machine_id, instance_name, db_name, 1, 16))

    #OEM - TUNING PACK EVIDENCES (10g or higher) - SQL Access Advisor and SQL Tuning Advisor
    sql = "select TASK_ID, OWNER, TASK_NAME, DESCRIPTION, ADVISOR_NAME, CREATED, LAST_MODIFIED, PARENT_TASK_ID, " \
              "EXECUTION_START, EXECUTION_END, STATUS, SOURCE, HOW_CREATED " \
          "from DBA_ADVISOR_TASKS " \
          "where ADVISOR_NAME in ('SQL Tuning Advisor', 'SQL Access Advisor')"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'OEM', 'DBA_ADVISOR_TASKS', machine_id, instance_name, db_name, 1, 13))

    #OEM - TUNING PACK EVIDENCES (10g or higher) - SQL Tuning Sets
    sql = "select ID, NAME, OWNER, CREATED, LAST_MODIFIED, STATEMENT_COUNT, DESCRIPTION "\
          "from DBA_SQLSET"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'OEM', 'DBA_SQLSET', machine_id, instance_name, db_name, 1, 7))

    #OEM - TUNING PACK EVIDENCES (10g or higher) - SQL Tuning Sets references
    sql = "select SQLSET_ID, SQLSET_NAME, SQLSET_OWNER, ID, OWNER, CREATED, DESCRIPTION "\
          "from DBA_SQLSET_REFERENCES"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'OEM', 'DBA_SQLSET_REFERENCES', machine_id, instance_name, db_name, 1, 7))

    #SecureFiles Encryption
    sql = "select x.* "\
          "from ( "\
              "select 'DBA_LOBS' as SOURCE_, OWNER, TABLE_NAME, COLUMN_NAME, ENCRYPT, SECUREFILE "\
                "from DBA_LOBS "\
                "where ENCRYPT not in ('NO', 'NONE') "\
              "union all "\
              "select 'DBA_LOB_PARTITIONS' as SOURCE_, TABLE_OWNER, TABLE_NAME, COLUMN_NAME, ENCRYPT, SECUREFILE "\
                "from DBA_LOB_PARTITIONS "\
                "where ENCRYPT not in ('NO', 'NONE') "\
              "union all "\
              "select 'DBA_LOB_SUBPARTITIONS' as SOURCE_, TABLE_OWNER, TABLE_NAME, COLUMN_NAME, ENCRYPT, SECUREFILE "\
                "from DBA_LOB_SUBPARTITIONS "\
                "where ENCRYPT not in ('NO', 'NONE') "\
               ") x"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'ADVANCED_SECURITY', 'SECUREFILES_ENCRYPTION', machine_id, instance_name, db_name, 1, 6))

    #Active Data Guard "Fast Incremental Backup on Physical Standby" feature usage
    sql = "select b.DATABASE_ROLE, a.STATUS, a.FILENAME, a.BYTES "\
          "from V$BLOCK_CHANGE_TRACKING a, V$DATABASE b "\
          "where b.DATABASE_ROLE like 'PHYSICAL STANDBY' and a.STATUS = 'ENABLED'"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'ACTIVE_DATA_GUARD', 'V$BLOCK_CHANGE_TRACKING', machine_id, instance_name, db_name, 1, 6))

    #OEM 10G AND HIGHER --- version and installation type (database control or grid/cloud control)
    sql = "select COMPONENT_NAME, VERSION, COMPAT_CORE_VERSION, COMPONENT_MODE, STATUS " \
          "from SYSMAN.MGMT_VERSIONS"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'OEM', 'MGMT_VERSIONS', machine_id, instance_name, db_name, 1, 5))

    #OEM 10G AND HIGHER --- components
    sql = "select CONTAINER_TYPE, CONTAINER_NAME, CONTAINER_LOCATION, OUI_PLATFORM, IS_CLONABLE, NAME, VERSION, " \
              "substr(replace(replace(replace(to_char(substr(DESCRIPTION, 1, 1000)), chr(10), '[LF]'), chr(13), '[CR]'),'\"',''''''''), 1, 255), " \
              "EXTERNAL_NAME, INSTALLED_LOCATION, INSTALLER_VERSION, MIN_DEINSTALLER_VERSION, IS_TOP_LEVEL, TIMESTAMP " \
          "from SYSMAN.MGMT_INV_CONTAINER a " \
              "full outer join SYSMAN.MGMT_INV_COMPONENT b on a.CONTAINER_GUID = b.CONTAINER_GUID"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'OEM', 'MGMT_INV_COMPONENT', machine_id, instance_name, db_name, 1, 14))

    # OEM MANAGED TARGETS (10g or higher)
    sql = "select TARGET_NAME,  DISPLAY_NAME, HOST_NAME, TARGET_TYPE, LAST_METRIC_LOAD_TIME, TYPE_DISPLAY_NAME " \
          "from MGMT$TARGET " \
          "where lower(TARGET_TYPE) like '%database%' or lower(TARGET_TYPE) like '%pdb%'"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'OEM', 'MGMT$TARGET', machine_id, instance_name, db_name, 1, 6))

    #CUBES IN OLAPSYS.DBA$OLAP_CUBES
    sql = "select OWNER, CUBE_NAME, DISPLAY_NAME from OLAPSYS.DBA$OLAP_CUBES"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'OLAP', 'OLAPSYS.DBA$OLAP_CUBES', machine_id, instance_name, db_name, 1, 3))

    #CUBES IN DBA_CUBES (introduced in 11.1)
    sql = "select OWNER, CUBE_NAME, AW_NAME from  DBA_CUBES"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'OLAP', 'DBA_CUBES', machine_id, instance_name, db_name, 1, 3))

    #Advanced Analytics Data Mining 10gv1.DM$MODEL
    sql = "select count(*) from DMSYS.DM$MODEL"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'DATA_MINING', '10gv1.DM$MODEL', machine_id, instance_name, db_name, 1, 1))

    #DATABASE IN-MEMORY (introduced in 12.1.0.2.0) - tables configured to use In-Memory Column Store
    sql = "select x.* " \
                           "from ( " \
                               "select 'DBA_TABLES' as SOURCE_, OWNER, TABLE_NAME, '', INMEMORY, INMEMORY_PRIORITY " \
                               "from DBA_TABLES " \
                               "where INMEMORY in ('ENABLED') " \
                           "union all " \
                               "select 'DBA_TAB_PARTITIONS' as SOURCE_, TABLE_OWNER, TABLE_NAME, PARTITION_NAME, INMEMORY, INMEMORY_PRIORITY " \
                               "from DBA_TAB_PARTITIONS " \
                               "where INMEMORY in ('ENABLED') " \
                           "union all " \
                               "select 'DBA_TAB_SUBPARTITIONS' as SOURCE_, TABLE_OWNER, TABLE_NAME, PARTITION_NAME, INMEMORY, INMEMORY_PRIORITY " \
                               "from DBA_TAB_SUBPARTITIONS " \
                               "where INMEMORY in ('ENABLED') " \
                           "union all " \
                               "select 'DBA_OBJECT_TABLES' as SOURCE_, OWNER, TABLE_NAME, OBJECT_ID_TYPE, INMEMORY, INMEMORY_PRIORITY " \
                               "from DBA_OBJECT_TABLES " \
                               "where INMEMORY in ('ENABLED') " \
                           ") x"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'DB_IN_MEMORY', 'INMEMORY_ENABLED_TABLES', machine_id, instance_name, db_name, 1, 6))

    #DATABASE IN-MEMORY - GV$IM_SEGMENTS
    sql = "select "\
          " a.CON_ID, "\
          " a.INST_ID, "\
          " a.SEGMENT_TYPE, "\
          " a.OWNER, "\
          " a.SEGMENT_NAME, "\
          " a.PARTITION_NAME, "\
          " a.POPULATE_STATUS, "\
          " a.INMEMORY_PRIORITY, "\
          " a.INMEMORY_COMPRESSION "\
          "from GV$IM_SEGMENTS a "\
          "order by a.SEGMENT_TYPE, a.OWNER, a.SEGMENT_NAME, a.PARTITION_NAME"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'DB_IN_MEMORY', 'GV$IM_SEGMENTS', machine_id, instance_name, db_name, 1, 9))

    #STANDBY_CONFIG - V$DATAGUARD_CONFIG
    sql = "SELECT DB_UNIQUE_NAME from V$DATAGUARD_CONFIG"
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'STANDBY_CONFIG', 'V$DATAGUARD_CONFIG', machine_id, instance_name, db_name, 1, 1))

    #STANDBY_CONFIG - V$ARCHIVE_DEST_STATUS
    sql = '''
        select 
            DEST_ID        ,
            DEST_NAME      ,
            STATUS         ,
            TYPE           ,
            DATABASE_MODE  ,
            RECOVERY_MODE  ,
            PROTECTION_MODE,
            DESTINATION    ,
            DB_UNIQUE_NAME
        from V$ARCHIVE_DEST_STATUS
        where TYPE!='LOCAL'
    '''
    lmsOptionsList.addAll(getLMSOptions(oracleClient, sql, 'STANDBY_CONFIG', 'V$ARCHIVE_DEST_STATUS', machine_id, instance_name, db_name, 1, 9))

    lmsOptionsCount = saveOptionsToProbe(Framework, lmsOptionsList)
    lmsOptionsList.clear()
    return lmsOptionsCount


def getLMSOptions(oracleClient, sql, option_name, option_query, machine_id, instance_name, db_name, needCount, col_number):
    optionsList = ArrayList()
    count = 0
    try:
        resultSet = oracleClient.executeQuery(sql)
        while (resultSet.next()):
            obj =  OracleLMSDataModel.LMSOptionsObject()
            obj.machineId = machine_id
            obj.dbName =  db_name
            obj.hostName = machine_id
            obj.instanceName = instance_name
            obj.optionName = option_name
            obj.optionQuery = option_query
            obj = enrichOptionObject(obj, resultSet, col_number)
            obj.discoveryId = machine_id + instance_name
            optionsList.add(obj)
            count = count + 1
        if (needCount == 1 and count > 0):
            for obj in optionsList:
                obj.sqlErrCode = str(count)
                obj.sqlErrMessage = 'count'
        if count == 0:
            obj =  OracleLMSDataModel.LMSOptionsObject()
            obj.machineId = machine_id
            obj.dbName =  db_name
            obj.hostName = machine_id
            obj.instanceName = instance_name
            obj.optionName = option_name
            obj.optionQuery = option_query
            obj.sqlErrCode = '0'
            obj.sqlErrMessage = 'no rows selected'
            obj.discoveryId = machine_id + instance_name
            optionsList.add(obj)
        resultSet.close()
    except Exception, message:
        messageString = str(message) #ORA-00942: table or view does not exist
        length = len(messageString)
        index = messageString.find('ORA-')
        if index > -1:
            index = index + 4
            messageString = messageString[index:length]
        subMessages = messageString.split(':')
        if len(subMessages) >= 2:
            obj =  OracleLMSDataModel.LMSOptionsObject()
            obj.machineId = machine_id
            obj.dbName =  db_name
            obj.hostName = machine_id
            obj.instanceName = instance_name
            obj.optionName = option_name
            obj.optionQuery = option_query
            if (OracleLMSUtils.isInteger(subMessages[0].strip()) == True):
                obj.sqlErrCode = '-' + str(int(subMessages[0].strip())) # sqlErrCode = -942
            else:
                obj.sqlErrCode = '-0'
            obj.sqlErrMessage = subMessages[len(subMessages)-1].strip().strip()
            obj.discoveryId = machine_id + instance_name
            optionsList.add(obj)
            logger.warn('Failed to query the database for ', option_name)
        else:
            strException = option_name + '. ' +  message.getMessage()
            logger.error('Failed to get records from ', strException)
    return optionsList

def enrichOptionObject(optionsObject, resultSet, col_number):
    if col_number >= 1:
        optionsObject.col010 = OracleLMSUtils.encodeString(resultSet.getString(1))
    if col_number >= 2:
        optionsObject.col020 =  OracleLMSUtils.encodeString(resultSet.getString(2))
    if col_number >= 3:
        optionsObject.col030 =  OracleLMSUtils.encodeString(resultSet.getString(3))
    if col_number >= 4:
        optionsObject.col040 =  OracleLMSUtils.encodeString(resultSet.getString(4))
    if col_number >= 5:
        optionsObject.col050 =  OracleLMSUtils.encodeString(resultSet.getString(5))
    if col_number >= 6:
        optionsObject.col060 =  OracleLMSUtils.encodeString(resultSet.getString(6))
    if col_number >= 7:
        optionsObject.col070 =  OracleLMSUtils.encodeString(resultSet.getString(7))
    if col_number >= 8:
        optionsObject.col080 =  OracleLMSUtils.encodeString(resultSet.getString(8))
    if col_number >= 9:
        optionsObject.col090 =  OracleLMSUtils.encodeString(resultSet.getString(9))
    if col_number >= 10:
        optionsObject.col100 =  OracleLMSUtils.encodeString(resultSet.getString(10))
    if col_number >= 11:
        optionsObject.col110 =  OracleLMSUtils.encodeString(resultSet.getString(11))
    if col_number >= 12:
        optionsObject.col120 =  OracleLMSUtils.encodeString(resultSet.getString(12))
    if col_number >= 13:
        optionsObject.col130 =  OracleLMSUtils.encodeString(resultSet.getString(13))
    if col_number >= 14:
        optionsObject.col140 =  OracleLMSUtils.encodeString(resultSet.getString(14))
    if col_number >= 15:
        optionsObject.col150 =  OracleLMSUtils.encodeString(resultSet.getString(15))
    if col_number >= 16:
        optionsObject.col160 =  OracleLMSUtils.encodeString(resultSet.getString(16))
    return optionsObject

def saveOptionsToProbe(Framework, lmsOptionsList):
    return OracleLMSDBUtils.OracleLMSDataServiceDAO(Framework).batchSaveLMSOptions(lmsOptionsList)

def clearOptionsByDiscoveryId(Framework, discovery_id):
    return OracleLMSDBUtils.OracleLMSDataServiceDAO(Framework).deleteLMSOptionsByDiscoveryId(discovery_id)

def getOptionsFromProbe(Framework, discovery_id):
    return OracleLMSDBUtils.OracleLMSDataServiceDAO(Framework).getLMSOptionsByDiscoveryId(discovery_id)

def getOptionsColumns():
    return OracleLMSDBUtils.COLUMNS_LMS_OPTIONS

def getOEMOwnersForRepository(oracleClient):
    owner = []
    owner_v2 = []
    sql = "select owner from dba_tables where table_name = 'SMP_REP_VERSION'"
    sql_v2 = "select owner from dba_tables where table_name = 'SMP_VDS_REPOS_VERSION'"
    try:
        resultSet = oracleClient.executeQuery(sql)#@@CMD_PERMISION sql protocol execution
        while (resultSet.next()):
            owner.append(resultSet.getString(1))
        resultSet.close()
        resultSet = oracleClient.executeQuery(sql_v2)#@@CMD_PERMISION sql protocol execution
        while (resultSet.next()):
            owner_v2.append(resultSet.getString(1))
        resultSet.close()
    except:
        logger.warn("Can not get OEM owner for for repository from table dba_tables.")
    return (owner, owner_v2)

def getOWBOwners(oracleClient):
    owner = []
    sql = "select owner from dba_tables where table_name = 'CMPSYSCLASSES'"
    try:
        resultSet = oracleClient.executeQuery(sql)#@@CMD_PERMISION sql protocol execution
        while (resultSet.next()):
            owner.append(resultSet.getString(1))
        resultSet.close()
    except:
        logger.warn("Can not get OWB owner from table dba_tables.")
    return owner

def getEnabledProfiles(oracleClient):
    count = 0
    sql = "select count(*) from dba_sql_profiles where lower(status) like 'enabled'"
    resultSet = oracleClient.executeQuery(sql)
    if resultSet.next():
        count = resultSet.getString(1)
    resultSet.close()
    return count

def getDBAUsersSource(oracleClient):
    sql = "select 'DBA_USERS' C1 from DBA_USERS where rownum = 1"
    dbaUserSource = 'SYS.AUDIT_DBA_USERS'
    try:
        resultSet = oracleClient.executeQuery(sql)#@@CMD_PERMISION sql protocol execution
        if resultSet.next():
            dbaUserSource = resultSet.getString(1)
        resultSet.close()
    except:
        logger.warn("Can not get DBA user source from table SYS.AUDIT_DBA_USERS, try to use default value")
    return dbaUserSource