#coding=utf-8
import string
import re
import logger
import types
import OracleLMSDBUtils

from java.util import Hashtable

from java.lang import String
from java.lang import Byte
from java.sql import Date
from java.sql import SQLException
from java.lang import NumberFormatException
from java.lang import System
from appilog.common.utils.zip import ChecksumZipper
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector

SQL_INSTANCE_NAME = 'SELECT instance_name FROM v$instance'
SQL_BANNER = "select BANNER from v$VERSION where lower(Banner) like '%oracle%'"
SQL_DATABASE = 'SELECT DATABASE_ROLE, CREATED FROM V$DATABASE'
SQL_DATABASE_CREATED = 'SELECT CREATED FROM V$DATABASE'
SQL_LICENSE = 'SELECT SESSIONS_HIGHWATER FROM V$LICENSE'
SQL_SESSION = 'SELECT * FROM ( SELECT COUNT(*) as CONT, LOGON_TIME FROM V$SESSION GROUP BY LOGON_TIME ORDER BY CONT DESC ) A WHERE ROWNUM = 1'
SQL_DBA_USERS_COUNT = "SELECT COUNT(USERNAME) from DBA_USERS"

CUSTOM_GROUP = 'GROUP'
CUSTOM_AGGREGATION_LEVEL = 'AGGREGATION_LEVEL'
CUSTOM_ORACLE_CSI = 'ORACLE_CSI'
CUSTOM_APPLICATION_NAME = 'APPLICATION_NAME'
CUSTOM_APPLICATION_STATUS = 'APPLICATION_STATUS'
CUSTOM_USER_COUNT_APPLICATION = 'USER_COUNT_APPLICATION'
CUSTOM_SERVER_NAME_IN_THE_CLUSTER = 'SERVER_NAME_IN_THE_CLUSTER'
CUSTOM_MEASUREMENT_COMMENT = 'MEASUREMENT_COMMENT'

def getInstanceName(oracleClient):
    resultSet = oracleClient.executeQuery(SQL_INSTANCE_NAME)#@@CMD_PERMISION sql protocol execution
    instance_name = None
    if resultSet.next():
        instance_name = resultSet.getString(1)
    resultSet.close()
    return instance_name

def getDBName(oracleClient, instance_name):
    SQL_DB_NAME = "select '" + instance_name + "'|| decode(value, 'TRUE', '~' || replace(sys_context('USERENV', 'CON_NAME'), '$', '_'), '') C1 from v$parameter where name = 'enable_pluggable_database'"
    resultSet = oracleClient.executeQuery(SQL_DB_NAME)#@@CMD_PERMISION sql protocol execution
    db_name = None
    if resultSet.next():
        db_name = resultSet.getString(1)
    resultSet.close()
    return db_name

def getDbVersion(oracleClient):
    resultSet = oracleClient.executeQuery(SQL_BANNER)#@@CMD_PERMISION sql protocol execution
    banner = None
    db_version = None
    db_edition = None
    if resultSet.next():
        banner = resultSet.getString(1)
    resultSet.close()

    if not banner:
        logger.warn('Cannot get data from Oracle "V$VERSION" table.')
    if string.find(banner, 'Enterprise') != -1:
        db_edition = 'Enterprise'
    elif string.find(banner, 'Personal') != -1:
        db_edition = 'Personal'

    mo = re.search('Release ([0-9.]+)', banner)
    if mo:
        db_version = mo.group(1)
    return banner, db_version, db_edition

def getDatabaseInfo(oracleClient):
    resultSet = None
    db_role = None
    install_date = None
    try:
        resultSet = oracleClient.executeQuery(SQL_DATABASE)#@@CMD_PERMISION sql protocol execution
        if resultSet.next():
            db_role = resultSet.getString(1)
            install_date = resultSet.getDate(2)
    except NumberFormatException, ex:
        install_data = Date(System.currentTimeMillis())
        strException = ex.getMessage()
        logger.warn('Cannot get CREATED FROM "V$DATABASE". ', strException)
    except SQLException, ex:
        logger.warn('Cannot get DATABASE_ROLE, CREATED FROM "V$DATABASE". ', ex)
        logger.debug('Try to query CREATED date')
        resultSet = oracleClient.executeQuery(SQL_DATABASE_CREATED)
        if resultSet.next():
            install_date = resultSet.getDate(1)
    finally:
        resultSet.close()

    if not db_role:
        logger.warn('Cannot get data from Oracle "V$DATABASE" table.')
    return db_role, install_date

def getLicense(oracleClient):
    resultSet = oracleClient.executeQuery(SQL_LICENSE)#@@CMD_PERMISION sql protocol execution
    sessions_highwater = None
    if resultSet.next():
        sessions_highwater = resultSet.getInt(1)
    resultSet.close()

    if not sessions_highwater:
        logger.warn('Cannot get data from Oracle "V$LICENSE" table.')
    return sessions_highwater

def getSession(oracleClient):
    resultSet = oracleClient.executeQuery(SQL_SESSION)#@@CMD_PERMISION sql protocol execution
    sessions = None
    logon_time = None
    if resultSet.next():
        sessions = resultSet.getInt(1)
        logon_time = resultSet.getDate(2)
    resultSet.close()

    if not sessions:
        logger.warn('Cannot get data from Oracle "V$SESSION" table.')
    return sessions, logon_time

def getDBAUsersCount(oracleClient):
    resultSet = oracleClient.executeQuery(SQL_DBA_USERS_COUNT)#@@CMD_PERMISION sql protocol execution
    count = None
    if resultSet.next():
        count = resultSet.getString(1)
    resultSet.close()

    if not count:
        logger.warn('Cannot get data from Oracle "DBA_USERS" table.')
    return count

def getDiscoveredVendor(Framework):
    return Framework.getTriggerCIData('discovered_vendor')

def getDiscoveredModel(Framework):
    return Framework.getTriggerCIData('discovered_model')

def getDiscoveredOSName(Framework):
    return Framework.getTriggerCIData('discovered_os_name')

def isVirtualHost(Framework):
    return Framework.getTriggerCIData('host_isvirtual')

def getCpuNumber(Framework):
    values = Framework.getTriggerCIDataAsList('core_number')
    totalCoreNumber = 0
    totalCpuNumber = 0
    for value in values:
        if (isInteger(value) == True):
            totalCoreNumber = totalCoreNumber + int(value)
            totalCpuNumber = totalCpuNumber + 1
    return  totalCpuNumber, totalCoreNumber

def getCpuSpecifier(Framework):
    specifier = Framework.getTriggerCIData('cpu_specifier')
    if (specifier != 'NA'):
        if (CPU_SPECIFIER.has_key(specifier)):
            specifier = CPU_SPECIFIER[specifier]
        else:
            logger.warn('Cannot find discovered CPU type: ', specifier)
            specifier = CPU_SPECIFIER['0']
    return specifier

def getCpuName(Framework):
    name = Framework.getTriggerCIData('cpu_name')
    if not name:
        cpu_specifier = getCpuSpecifier(Framework)
        if cpu_specifier  != 'NA':
            name = cpu_specifier
    return name

def getCpuClockSpeed(Framework):
    return Framework.getTriggerCIData('cpu_clock_speed')

def getLogicalCpuCount(Framework):
    values = Framework.getTriggerCIDataAsList('logical_cpu_count')
    totalNumber = 0
    for value in values:
        if (isInteger(value) == True):
            totalNumber = totalNumber + int(value)
    return  totalNumber

def getPartitioningMethod(Framework):
    partition = ''
    if (Framework.getTriggerCIData('partitioning_amazon_ec2_config') != 'NA'):
        partition = 'Amazon EC2 Config'
    if (Framework.getTriggerCIData('partitioning_solaris_zone_config') != 'NA'):
        partition = 'Solaris Zone Config'
    if (Framework.getTriggerCIData('partitioning_hyper-v_partition_config') != 'NA'):
        partition = 'Hyper-V Partition Config'
    if (Framework.getTriggerCIData('partitioning_hp_npar_config') != 'NA'):
        partition = 'HP nPar Config'
    if (Framework.getTriggerCIData('partitioning_vmware_host_resource') != 'NA'):
        partition = 'VMware Host Resource'
    if (Framework.getTriggerCIData('partitioning_xen domain config') != 'NA'):
        partition = 'Xen domain config'
    if (Framework.getTriggerCIData('partitioning_hp_vpar_config') != 'NA'):
        partition = 'HP vPar Config'
    if (Framework.getTriggerCIData('partitioning_ibm_lpar_profile') != 'NA'):
        partition = 'IBM LPar Profile'
    return partition

def mergeCustomData(historyContent):
    table = Hashtable()
    buffer = historyContent.split('\n')
    for s in buffer:
        if(s != None and len(s) > 0):
            columns = s.split('=')
            table.put(columns[0].strip(), columns[1].strip())
    return table

def createCustomDataAndContent(group, aggregationLevel, oracleCSI, applicationName, applicationStatus, userCountApplication, serverNameInTheCluster, measurementComment):
    content = 'GROUP = ' + group + '\nAGGREGATION_LEVEL = ' + aggregationLevel
    content = content + '\nORACLE_CSI = ' + oracleCSI + '\nAPPLICATION_NAME = ' + applicationName
    content = content + '\nAPPLICATION_STATUS = ' + applicationStatus + '\nUSER_COUNT_APPLICATION = ' + userCountApplication
    content = content + '\nSERVER_NAME_IN_THE_CLUSTER = ' + serverNameInTheCluster + '\nMEASUREMENT_COMMENT = ' + measurementComment
    table = Hashtable()
    table.put(CUSTOM_GROUP, group)
    table.put(CUSTOM_AGGREGATION_LEVEL, aggregationLevel)
    table.put(CUSTOM_ORACLE_CSI, oracleCSI)
    table.put(CUSTOM_APPLICATION_NAME, applicationName)
    table.put(CUSTOM_APPLICATION_STATUS, applicationStatus)
    table.put(CUSTOM_USER_COUNT_APPLICATION, userCountApplication)
    table.put(CUSTOM_SERVER_NAME_IN_THE_CLUSTER, serverNameInTheCluster)
    table.put(CUSTOM_MEASUREMENT_COMMENT, measurementComment)
    return (table, content)

def getunZippedContent(content):
    zipper = ChecksumZipper()
    zippedArray = content.split(',')
    zippedList = []
    for zippedByte in zippedArray:
        zippedList.append(Byte.parseByte(zippedByte))
    unzipBytes = zipper.unzip(zippedList)
    buffer = str(String(unzipBytes))

    return buffer

def encodeString(content):
    if(content != None):
        content = re.sub(', ',' ', content)
        content = re.sub(',',' ', content)
        content = re.sub('\0',' ', content)
        return content

def isInteger(varObj):
    return varObj.isdigit()

def getOEMOwner(oracleClient):
    owner = 'SYSMAN'
    sql = "select owner from dba_tables where table_name = 'MGMT_ADMIN_LICENSES'"
    try:
        resultSet = oracleClient.executeQuery(sql)#@@CMD_PERMISION sql protocol execution
        if resultSet.next():
            owner = resultSet.getString(1)
        resultSet.close()
    except:
        logger.warn("Can not get OEM owner from table dba_tables, try to use default value.")
    return owner

def createAuditDocumentOSH(name, size, content, containerOSH, description):

    documentOsh = ObjectStateHolder("audit_document")
    documentOsh.setAttribute('data_name', name)
    resolvedContentType = 'text/plain'

    if content:
        bytes = String(content).getBytes()
        zipper = ChecksumZipper()
        zippedBytes = zipper.zip(bytes)
        checksumValue = zipper.getChecksumValue()

        if len(zippedBytes) <= int(size):
            documentOsh.setBytesAttribute('document_data', zippedBytes)
        else:
            logger.reportWarning('Configuration file %s size (%s) is too big' % (name, len(zippedBytes)))
        documentOsh.setLongAttribute('document_checksum', checksumValue)
        documentOsh.setLongAttribute('document_size',len(bytes))

    documentOsh.setAttribute('document_content_type', resolvedContentType)
    documentOsh.setAttribute('data_description', description)
    documentOsh.setContainer(containerOSH)

    return documentOsh

def createOracleLMSTables(Framework):
    return OracleLMSDBUtils.OracleLMSDataServiceDAO(Framework).createTablesIfNotExist()

def upgradeDatabase(Framework):
    dbUtil = OracleLMSDBUtils.OracleLMSDataServiceDAO(Framework)
    result = True
    lengthCOL010 = dbUtil.getLMSColumnLength('lms_options','col010')
    if lengthCOL010 < 1000:
        result = dbUtil.alterLMSColumnLength('lms_options', 'col010', 1000)

    lengthCOL030 = dbUtil.getLMSColumnLength('lms_options', 'col030')
    if lengthCOL030 < 1000:
        result = dbUtil.alterLMSColumnLength('lms_options', 'col030', 1000)

    lengthPI = dbUtil.getLMSColumnLength('lms_overview', 'PROCESSOR_IDENTIFIER')
    if lengthPI < 255:
        result = dbUtil.alterLMSColumnLength('lms_overview', 'PROCESSOR_IDENTIFIER', 255)

    lengthPROCESS = dbUtil.getLMSColumnLength('lms_v$session','PROCESS')
    if lengthPROCESS < 255:
        result = dbUtil.alterLMSColumnLength('lms_v$session', 'PROCESS', 255)

    count = dbUtil.countLMSOptionsColumns()
    if (count > 0 and count < 27):
        result = dbUtil.alterLMSOptions() and result
    return result

#Defined in cpu_specifier_enum.xml
CPU_SPECIFIER = {'0' : 'Unknown',
                 '1':'V20',
                 '2':'V30',
                 '3':'8088',
                 '4':'8086',
                 '5':'80186',
                 '6':'80188',
                 '7':'80286',
                 '8':'80386',
                 '9':'80386SX',
                 '10':'i486DX/2',
                 '11':'i486DX',
                 '12':'i486SX',
                 '13':'Pentium',
                 '14':'Pentium Pro',
                 '15':'Pentium II',
                 '16':'Pentium III',
                 '17':'Pentium 4',
                 '18':'P7 Family',
                 '19':'Non-Intel',
                 '20':'Cyrix 5x86',
                 '21':'Cyrix 6x86',
                 '22':'Cyrix 6x86M',
                 '23':'Am486',
                 '24':'AMD K5',
                 '25':'AMD K6',
                 '26':'AMD Athlon',
                 '27':'UltraSPARC',
                 '28':'UltraSPARC-II',
                 '29':'HyperSPARC',
                 '30':'microSPARC-I',
                 '31':'microSPARC-II',
                 '32':'SuperSPARC',
                 '33':'SuperSPARC-II',
                 '34':'UltraSPARC-IIi',
                 '35':'UltraSPARC-III',
                 '36':'UltraSPARC-IIe',
                 '37':'UltraSPARC-III+',
                 '38':'TurboSPARC',
                 '39':'SuperSPARC+',
                 '40':'HP PA-RISC 1.0',
                 '41':'HP PA-RISC 1.1',
                 '42':'HP PA-RISC 1.2',
                 '43':'HP PA-RISC 2.0',
                 '44':'HP PA-Unknown',
                 '50':'68000',
                 '51':'68020',
                 '52':'68030',
                 '53':'68040',
                 '54':'680LC40',
                 '55':'PowerPC 601',
                 '56':'PowerPC 603',
                 '57':'PowerPC 603e',
                 '58':'PowerPC 604',
                 '59':'PowerPC 604e',
                 '60':'PowerPC 750 (G3)',
                 '61':'PowerPC 7400 (G4)',
                 '62':'PowerPC 7410 (G4)',
                 '63':'PowerPC 7450 (G4)',
                 '70':'Power',
                 '71':'Power2',
                 '72':'Power3',
                 '73':'Power3-II',
                 '74':'PowerPC',
                 '75':'PowerPersonal',
                 '76':'P2SC',
                 '77':'RD64-II',
                 '78':'RS-III',
                 '79':'RS64',
                 '80':'RS64-II',
                 '81':'RS64-III',
                 '82':'RS64-IV',
                 '83':'RSC',
                 '84':'ThinkPad',
                 '85':'AMD K6-2',
                 '86':'AMD K6-III',
                 '87':'AMD Duron',
                 '88':'SPARC',
                 '89':'Itanium',
                 '90':'Itanium 2',
                 '91':'Pentium M',
                 '92':'AMD Athlon 64',
                 '93':'AMD Opteron',
                 '94':'AMD Sempron',
                 '95':'AMD Turion 64',
                 '96':'Power4',
                 '97':'PowerPC 970 (G5)',
                 '98':'Celeron',
                 '99':'Mobile Celeron',
                 '100':'Pentium II Xeon',
                 '101':'Pentium III Xeon',
                 '102':'Mobile Pentium III',
                 '103':'Mobile Pentium 4',
                 '104':'Xeon',
                 '105':'Xeon MP',
                 '106':'Genuine',
                 '107':'Celeron M',
                 '108':'Pentium D',
                 '109':'AMD Mobile Athlon',
                 '110':'AMD Mobile Duron',
                 '111':'AMD Athlon MP',
                 '112':'AMD Athlon XP',
                 '113':'AMD Mobile Athlon XP',
                 '114':'AMD Mobile Athlon 64',
                 '115':'AMD Athlon 64 X2',
                 '116':'AMD Mobile Sempron',
                 '117':'AMD Dual Opteron',
                 '118':'AMD Geode NX',
                 '119':'VIA Ezra',
                 '120':'VIA Nehemiah',
                 '121':'VIA Samuel',
                 '122':'VIA Samuel 2',
                 '123':'VIA Esther',
                 '124':'Transmeta TM5400',
                 '125':'Transmeta TM5500',
                 '126':'Transmeta TM5600',
                 '127':'Transmeta TM5700',
                 '128':'Transmeta TM5800',
                 '129':'Transmeta TM8000',
                 '130':'Core Solo',
                 '131':'Core Duo',
                 '132':'microSPARC-IIep',
                 '133':'UltraSPARC-IIIi',
                 '134':'UltraSPARC-IV',
                 '135':'UltraSPARC-IV+',
                 '136':'UltraSPARC-T1',
                 '137':'UltraSPARC-T2',
                 '138':'TMS390Z50',
                 '139':'TMS390Z55',
                 '140':'PowerPC 620',
                 '141':'PowerPC 740 (G3)',
                 '142':'PowerPC 745 (G3)',
                 '143':'PowerPC 750CX (G3)',
                 '144':'PowerPC 750CXe (G3)',
                 '145':'PowerPC 750CXr',
                 '146':'PowerPC 750FL',
                 '147':'PowerPC 750FX (G3)',
                 '148':'PowerPC 750GL',
                 '149':'PowerPC 750GX (G3)',
                 '150':'PowerPC 755 (G3)',
                 '151':'PowerPC 7441 (G4)',
                 '152':'PowerPC 7445 (G4)',
                 '153':'PowerPC 7447 (G4)',
                 '154':'PowerPC 7447a (G4)',
                 '155':'PowerPC 7451 (G4)',
                 '156':'PowerPC 7455 (G4)',
                 '157':'PowerPC 7457 (G4)',
                 '158':'PowerPC 970FX (G5)',
                 '159':'PowerPC 970MP (G5)',
                 '160':'Power5',
                 '161':'Power6',
                 '162':'Power7',
                 '163':'Fujitsu Siemens SPARC64 V',
                 '164':'Core 2',
                 '165':'Core 2 Quad',
                 '166':'AMD Athlon 64 FX',
                 '167':'AMD Turion 64 X2',
                 '168':'VIA C7',
                 '169':'VIA C7-D',
                 '170':'VIA C7-M',
                 '171':'VIA Eden',
                 '172':'Core 2 Extreme',
                 '173':'Core i7',
                 '174':'Pentium Dual-Core',
                 '175':'Xeon MV',
                 '176':'Celeron D',
                 '177':'Atom',
                 '178':'AMD Sempron XP',
                 '179':'AMD Sempron Dual-Core',
                 '180':'AMD Athlon X2 Dual-Core',
                 '181':'AMD Athlon XP-M',
                 '182':'AMD Turion X2 Dual-Core',
                 '183':'AMD Turion X2 Ultra Dual-Core',
                 '184':'AMD Mobile Athlon XP-M',
                 '185':'AMD Quad-Core Opteron',
                 '186':'AMD Phenom',
                 '187':'UltraSPARC-T2+',
                 '188':'Celeron Dual-Core',
                 '189':'Core i5',
                 '190':'VIA Nano',
                 '191':'AMD Athlon II X2',
                 '192':'AMD Athlon II X4',
                 '193':'AMD Athlon Neo',
                 '194':'AMD Turion II',
                 '195':'AMD Turion Neo',
                 '196':'Core i3',
                 '197':'Fujitsu SPARC64 VI',
                 '198':'Fujitsu SPARC64 VII',
                 '199':'Itanium 9000 Series',
                 '200':'Itanium 9100 Series',
                 '201':'Itanium 9300 Series',
                 '202':'AMD Athlon II X3',
                 '203':'AMD Athlon II Neo',
                 '204':'AMD Athlon II',
                 '205':'AMD Six-Core Opteron',
                 '206':'AMD Phenom II',
                 '207':'AMD V105',
                 '208':'AMD V120',
                 '209':'AMD V140',
                 '210':'Fujitsu SPARC64 VII+',
                 '211':'Fujitsu SPARC64 VIII',
                 '212':'UltraSPARC-T3',
                 '213':'AMD V160',
                 '214':'AMD C-30',
                 '215':'AMD C-50',
                 '216':'AMD E-240',
                 '217':'AMD E-350',
                 '218':'Fujitsu SPARC64 IV',
                 '219':'SPARC-T4',
                 '220':'SPARC-T5',
                 '221':'Fujitsu SPARC64 VII++',
                 '222':'AMD C60',
                 '223':'AMD A4',
                 '224':'AMD A6',
                 '225':'AMD A8',
                 '226':'AMD E2',
                 '227':'AMD E-300',
                 '228':'AMD E-450',
                 '229':'AMD FX Quad-Core',
                 '230':'AMD FX Six-Core',
                 '231':'AMD FX Eight-Core'}  