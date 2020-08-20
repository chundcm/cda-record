# coding=utf-8
import shellutils
import logger
import file_system
import netutils
import dbutils
import re
import process_discoverer
import shell_interpreter
from oracle_shell_topology import OracleInstanceBuilder
from oracle_shell_topology import OracleDataGuardBuilder
from oracle_shell_topology import OracleRACBuilder
from oracle_shell_topology import OracleDataFileBuilder
from appilog.common.utils import Protocol

ORACLE_DATABASE_ROLE = "select database_role from v$database;"
ARCHIVE_DEST_STANDBY_COUNT = "select count(*) as standby_count from v$archive_dest where status = 'VALID' and target = 'STANDBY';"
LOG_ARCHIVE_CONFIG = "select value from v$parameter where name='log_archive_config';"
LOG_ARCHIVE_DEST = "select value from v$parameter where name like 'log_archive_dest_%' and name not like 'log_archive_dest_state_%' and  value is not null order by name;"
ORACLE_HOST_NAME = "SELECT INSTANCE_NUMBER,INSTANCE_NAME,HOST_NAME, DATABASE_STATUS FROM GV$INSTANCE order by HOST_NAME;"
ORACLE_INSTANCES = "SELECT INSTANCE_NAME, HOST_NAME FROM GV$INSTANCE order by HOST_NAME;"
ORACLE_DATA_FILE = "SELECT a.file_name, a.file_id FROM DBA_DATA_FILES a, V$DATAFILE b, V$RECOVER_FILE c, V$BACKUP d WHERE a.file_id=b.file# AND a.file_id=c.file#(+) AND a.file_id=d.file#(+) union SELECT a.file_name, a.file_id * 10000 FROM DBA_TEMP_FILES a, V$DATAFILE b, V$RECOVER_FILE c, V$BACKUP d WHERE a.file_id=b.file# AND a.file_id=c.file#(+) AND a.file_id=d.file#(+);"
ORACLE_DB_NAME = "select name, dbid from v$database;"
CLUSTERED_DATABASE = "SELECT VALUE from V$SPPARAMETER WHERE NAME='cluster_database';"
RAC_SERVICE_NAME = "SELECT VALUE from V$SPPARAMETER WHERE NAME ='db_name';"
TEST_CONNECTION = "select 1 from dual;"
DATAGUARD_CONFIG = "select db_unique_name from V$DATAGUARD_CONFIG order by db_unique_name;"


class SQLPlusShell:
    def __init__(self, framework, client, shell, cred_id, path, as_sysdba=True):
        self._framework = framework
        self._client = client
        self._shell = shell
        self._cred_id = cred_id
        self._path = path
        if as_sysdba:
            self._sysdba = "as sysdba"
        else:
            self._sysdba = ""
        self.setup_command()

    def setup_command(self):
        logger.debug('Inside setup_command')
        if self._cred_id:
            username = self._framework.getProtocolProperty(self._cred_id, Protocol.PROTOCOL_ATTRIBUTE_USERNAME)
            self._client.clearCommandToInputAttributeMatchers()
            self._sqlplus_cmd = "%ssqlplus %s %s" % (self._path, username, self._sysdba)
        else:
            self._sqlplus_cmd = "%ssqlplus / as sysdba" % (self._path)

        sqlplus_cmd = "%ssqlplus" % (self._path)
        self._client.addCommandToInputAttributeMatcher(sqlplus_cmd,
                                                       "Enter user-name:",
                                                       Protocol.PROTOCOL_ATTRIBUTE_USERNAME,
                                                       self._cred_id)
        self._client.addCommandToInputAttributeMatcher(sqlplus_cmd,
                                                       "Enter password:",
                                                       Protocol.PROTOCOL_ATTRIBUTE_PASSWORD,
                                                       self._cred_id)

    def __call__(self, command):
        try:
            return self._shell.execCmd('%s @%s' % (self._sqlplus_cmd, command))
        except:
            exInfo = logger.prepareJythonStackTrace('')
            logger.debug("Failed to execute command: %s" % command, exInfo)
            return "ERROR"


class TNSNameOra:
    def __init__(self):
        self.connection_identifier = None
        self.db_port = None
        self.sid = None
        self.host_ip = None


class OracleTopologyDiscoverer:
    def __init__(self, sqlPlusCmd, oracle_home=None):
        self._sqlPlusCmd = sqlPlusCmd
        self._oracle_home = oracle_home
        self._shell = self._sqlPlusCmd._shell
        self.__fs = file_system.createFileSystem(self._shell)

    def discover(self):
        raise NotImplementedError()

    def check_oracle_env(self):
        file_name = self.saveCmdToScript(self.enrich_cmd(TEST_CONNECTION))
        output = self._sqlPlusCmd(file_name)
        self._remove_temp_file(file_name)
        self.parse_sqlplus_output(TEST_CONNECTION, output)
        if output and self._shell.getLastCmdReturnCode() == 0:
            if output.find("Connected to an idle instance") != -1 or output.find("ORA-01034") != -1 or output.find(
                    "ERROR") != -1:
                return False
            return True
        return False

    def __getUniqueTempSQLFileName(self, file_path):
        return file_path + 'ucmdb-temp-sqlplus.sql'

    def saveCmdListToScript(self, cmdlist):
        file_path = self.__fs.getTempFolder()
        file_name = self.__getUniqueTempSQLFileName(file_path)
        if self._shell.isWinOs():
            self._shell.execCmd('echo. > %s' % file_name)
        else:
            self._shell.execCmd('echo > %s' % file_name)
        for cmd in cmdlist:
            self._shell.execCmd('echo %s >> %s' % (cmd, file_name))
        self._shell.execCmd('echo exit  >> ' + file_name)
        return file_name

    def saveCmdToScript(self, cmd):
        cmdlist = []
        cmdlist.append(cmd)
        return self.saveCmdListToScript(cmdlist)

    def _remove_temp_file(self, file_name):
        try:
            if file_name.find('ucmdb-temp-sqlplus.sql') != -1:
                logger.debug("Removing file:", file_name)
                self.__fs.removeFile(file_name)
        except:
            exInfo = logger.prepareJythonStackTrace('')
            logger.debug("Failed to remove file %s:" % file_name, exInfo)

    def enrich_cmd(self, command):
        if not self._shell.isWinOs():
            command = '"' + command + '"'
            command = command.replace("$", "\$")
        return command

    def parse_sqlplus_result_space(self, output):
        return self.parse_sqlplus_result(output, ' ')

    def parse_sqlplus_result_tab(self, output):
        return self.parse_sqlplus_result(output, '\t')

    def parse_sqlplus_result(self, output, separator):
        data = []
        endOfHeader = 0
        for line in output.strip().splitlines():
            if (line.find('-----') != -1) and (endOfHeader == 0):
                endOfHeader = 1
                continue
            if endOfHeader == 1:
                for item in line.split(separator):
                    if item.strip():
                        data.append(item.strip())
                return data

    def parse_sqlplus_output(self, command, output):
        if (output.find('-----') != -1 or output.find('ERROR') == -1 or output.find('no rows selected') != -1):
            return
        else:
            raise Exception("Failed to execute query with SQLPlus:" + command)

class OracleServerDiscoverer():
    def __init__(self, framework, shell):
        self._framework = framework
        self._shell = shell

    def discover(self):
        result = []
        oracle_sids = []

        ip_address = self._framework.getDestinationAttribute("ip_address")

        discoverer = process_discoverer.getDiscovererByShell(self._shell)
        processes = discoverer.discoverAllProcesses()
        if not processes:
            return None

        for process in processes:
            bin_path = None
            sids = []
            oracleProcess = self.is_oracle_process(process.getName(), self._shell.isWinOs())
            if oracleProcess:
                oracle_discoverer = self.get_oracle_discoverer()
                logger.debug("oracle process:", process)
                name = oracle_discoverer.get_oracle_sid_by_process(process)
                if name:
                    if self.is_sid_discovered(oracle_sids, name):
                        continue
                    sids.append(name)
                    bin_path = oracle_discoverer.get_oracle_bin_path(process, name)
            if sids:
                for name in sids:
                    result.append((name, bin_path, OracleInstanceBuilder.create_pdo(name, ip_address)))
        return result

    def get_oracle_discoverer(self):
        if self._shell.isWinOs():
            oracle_discoverer = OracleDiscovererOnWindows(self._shell)
        else:
            oracle_discoverer = OracleDiscovererOnUnix(self._shell)
        return oracle_discoverer

    def is_oracle_process(self, process_name, is_win_os):
        if is_win_os:
            return process_name.find("oracle.exe") != -1
        else:
            return process_name.startswith("ora") or process_name.startswith("asm")

    def is_tns_listener_process(self, process_name, is_win_os):
        if is_win_os:
            return process_name.find("tnslsnr.exe") != -1 or process_name.find("oracle-tns-listener") != -1
        else:
            return process_name.find("tnslsnr") != -1

    def is_sid_discovered(self, oracle_sids, name):
        if len(oracle_sids):
            if name in oracle_sids:
                logger.debug("skipping discovered sid %s." % name)
                return True
            else:
                oracle_sids.append(name)
        else:
            oracle_sids.append(name)
        return False


class OracleDiscoverer:
    def __init__(self, shell):
        self._shell = shell

    def get_oracle_sid_by_process(self, process):
        raise NotImplementedError()

    def get_oracle_bin_path(self, process=None, sid=None):
        raise NotImplementedError()

    def get_oracle_info_by_listener(self, process):
        sidlist = []
        bin_path = self.get_oracle_binpath_from_process_path(process.getExecutablePath(), '(.*)(?i)tnslsnr.*$')
        if bin_path:
            match = re.match(r"(.*)[/\\]bin[/\\]+", bin_path, re.IGNORECASE)
            if match:
                self.set_environment_variable("ORACLE_HOME", match.group(1).strip())
                listenerStatus = self._shell.execCmd('\"' + bin_path + 'lsnrctl\" status')
                if listenerStatus and self._shell.getLastCmdReturnCode() == 0:
                    logger.debug('Status fetched sid')
                    for line in listenerStatus.split('\n'):
                        sidline = re.search(r"Instance \"(\w*)\", status.*", line)
                        if sidline:
                            sidlist.append(sidline.group(1).strip())
                else:
                    logger.debug('Error getting sid list.')
        return set(sidlist), bin_path

    def get_oracle_binpath_from_process_path(self, process_path, pattern):
        matchPath = re.match(pattern, process_path, re.IGNORECASE)
        if matchPath:
            return matchPath.group(1).strip()

    def set_environment_variable(self, name, value):
        environment = shell_interpreter.Factory().create(self._shell).getEnvironment()
        try:
            environment.setVariable(name, value)
        except:
            logger.debugException('Failed to set ' + name)
        else:
            logger.debug(name + ' set to: %s' % value)


class OracleDiscovererOnWindows(OracleDiscoverer):
    def __init__(self, shell):
        OracleDiscoverer.__init__(self, shell)

    def get_oracle_sid_by_process(self, process):
        if process.commandLine:
            match = re.match(".+\s+(\w+)$", process.commandLine, re.IGNORECASE)
            if match:
                return match.group(1).strip()

    def get_oracle_bin_path(self, process=None, sid=None):
        if process:
            return self.get_oracle_binpath_from_process_path(process.getExecutablePath(), r'(.+\\)[^\\]*?$')


class OracleDiscovererOnUnix(OracleDiscoverer):
    def __init__(self, shell):
        OracleDiscoverer.__init__(self, shell)

    def get_oracle_sid_by_process(self, process):
        if process.commandLine:
            match = re.match(".*(ora|asm)\_\w{4}\_([\+\w]+)\s*$", process.commandLine, re.IGNORECASE)
            if match:
                return match.group(2).strip()

    def get_oracle_bin_path(self, process=None, sid=None):
        if sid:
            return self.get_ora_bin(sid)

    def escape_character(self, str_escape):
        if str_escape.find("+") != -1:
            str_escape = str_escape.replace("+", "\+")
        return str_escape

    def get_ora_bin(self, db_sid):
        try:
            db_sid = self.escape_character(db_sid)
            oratabContent = self._shell.safecat('/etc/oratab')
            if oratabContent and self._shell.getLastCmdReturnCode() == 0:
                for line in oratabContent.split('\n'):
                    oracleHome = re.match(r"\s*" + db_sid + "\s*:(.*?):.*", line, re.IGNORECASE)
                    if oracleHome:
                        return oracleHome.group(1).strip() + '/bin/'
        except:
            exInfo = logger.prepareJythonStackTrace('')
            logger.debug("Failed to get oracle bin path from /etc/oratab:", exInfo)


class OracleDatabaseRoleDiscoverer(OracleTopologyDiscoverer):
    def __init__(self, sqlPlusCmd, oracle_home=None):
        OracleTopologyDiscoverer.__init__(self, sqlPlusCmd, oracle_home)

    def discover(self):
        try:
            file_name = self.saveCmdToScript(self.enrich_cmd(ORACLE_DATABASE_ROLE))
            output = self._sqlPlusCmd(file_name)
            self._remove_temp_file(file_name)
            if output:
                if (output.find('ORA-01033') != -1 or output.find('ORACLE initialization or shutdown in progress') != -1):
                    return "STANDBY"
                self.parse_sqlplus_output(ORACLE_DATABASE_ROLE, output)
                result = self.parse_sqlplus_result_tab(output)
                if result:
                    return result[0]
        except:
            logger.debug("Failed to discover database role.", logger.prepareJythonStackTrace(''))
            return None


class OracleDataGuardDiscoverer(OracleTopologyDiscoverer):
    def __init__(self, sqlPlusCmd, database_role, oracle_home=None):
        OracleTopologyDiscoverer.__init__(self, sqlPlusCmd, oracle_home)
        self.database_role = database_role

    def discover(self):
        if self.database_role == 'PRIMARY':
            archiveDestStandbyCount = self.get_dest_standby_count()
            if archiveDestStandbyCount == 0:
                logger.debug("It's not Oracle Data Guard.")
                return (None, None)

        logger.debug("discover_db_service_mapping.....")
        db_unique_name_service_map = self.discover_db_service_mapping()
        logger.debug("db_unique_name_service_mapping:", db_unique_name_service_map)
        dg_name, oracles = self.discover_dg_config(db_unique_name_service_map)
        if not dg_name:
            logger.debug("It's not Oracle Data Guard.")
            return (None, None)

        db_name, dbid = self.get_db_name()
        log_archive_config = self.get_log_archive_config()
        pattern = "dg_config=\((\S+)\)"
        match = re.search(pattern, log_archive_config)
        if match:
            dg_config = match.group(1).strip()
            return OracleDataGuardBuilder.create_pdo(dg_name, dg_config, db_name, dbid), oracles

    def get_dest_standby_count(self):
        file_name = self.saveCmdToScript(self.enrich_cmd(ARCHIVE_DEST_STANDBY_COUNT))
        output = self._sqlPlusCmd(file_name)
        self._remove_temp_file(file_name)
        if output:
            self.parse_sqlplus_output(ARCHIVE_DEST_STANDBY_COUNT, output)
            count = self.parse_sqlplus_result_tab(output)
            if count:
                return count[0]

    def discover_db_service_mapping(self):
        map = {}
        cmdlist = []
        cmdlist.append("set linesize 32767")
        cmdlist.append("column value format a32000")
        cmdlist.append(self.enrich_cmd(LOG_ARCHIVE_DEST))
        file_name = self.saveCmdListToScript(cmdlist)
        output = self._sqlPlusCmd(file_name)
        self._remove_temp_file(file_name)
        if output:
            self.parse_sqlplus_output(LOG_ARCHIVE_DEST, output)
            log_archive_dests = self.parse_dg_config(output)
            for log_archive_dest in log_archive_dests:
                if log_archive_dest:
                    pattern = "service=\"(\S+)\"[\s+\S+]*db_unique_name=\"(\S+)\""
                    net_service_name, db_unique_name = self.parse_db_service_mapping(log_archive_dest, pattern)
                    if not net_service_name:
                        pattern = "service=(\S+)[\s+\S+]*db_unique_name=(\S+)"
                        net_service_name, db_unique_name = self.parse_db_service_mapping(log_archive_dest, pattern)
                    if net_service_name and db_unique_name:
                        map[db_unique_name] = net_service_name
        return map

    def discover_dg_config(self, db_unique_name_service_map):
        result = []
        dg_name = None
        db_unique_names = self.get_db_unique_names()
        ora_tns_discoverer = OracleTNSDiscoverer(self._shell, self._oracle_home)
        if db_unique_names and len(db_unique_names)> 1:
            if self._oracle_home:
                for db_unique_name in db_unique_names:
                    logger.debug("get other Data Guard servers.", db_unique_name)
                    net_service_name = db_unique_name_service_map.get(db_unique_name, None)
                    if net_service_name:
                        tns = ora_tns_discoverer.getTNSInfoByConnectionIdentifier(net_service_name)
                        if tns:
                            logger.debug("data guard tns:", tns.host_ip)
                            dg_name = ','.join(db_unique_names)[:-1]
                            services = [tns.sid]
                            result.append(OracleInstanceBuilder.create_pdo(None, tns.host_ip, tns.db_port, services))
                        else:
                            logger.debug("Failed to get Oracle host ip address and port from tnsnames.ora:", net_service_name)
                    else:
                        logger.debug("Cannot find net service name for db unique name:", db_unique_name)
            else:
                logger.debug("Failed to discover all dg_configs without Oracle Home.")
        return dg_name, result

    def parse_db_service_mapping(self, log_archive_dest, pattern):
        match = re.search(pattern, log_archive_dest, re.IGNORECASE)
        if match:
            logger.debug("service:", match.group(1).strip())
            logger.debug("db_unique_names:", match.group(2).strip())
            return match.group(1).strip(), match.group(2).strip()
        return None, None

    def get_db_unique_names(self):
        file_name = self.saveCmdToScript(self.enrich_cmd(DATAGUARD_CONFIG))
        output = self._sqlPlusCmd(file_name)
        self._remove_temp_file(file_name)
        if output:
            self.parse_sqlplus_output(DATAGUARD_CONFIG, output)
            db_unique_names = self.parse_dg_config(output)
            return db_unique_names

    def parse_dg_config(self, output):
        data = []
        endOfHeader = 0
        endOfBody = 0
        for line in output.strip().splitlines():
            if (line.find('-----') != -1) and (endOfHeader == 0):
                endOfHeader = 1
                continue
            if (line.find('SQL> ') != -1) or (line.find('Disconnected from Oracle') != -1):
                endOfBody = 1
                break
            if endOfHeader == 1 and endOfBody == 0:
                item = line.split('\t')
                if item and len(item) > 0 and item[0].strip():
                    data.append(item[0].strip())
        return data


    def get_db_name(self):
        file_name = self.saveCmdToScript(self.enrich_cmd(ORACLE_DB_NAME))
        output = self._sqlPlusCmd(file_name)
        self._remove_temp_file(file_name)
        if output:
            self.parse_sqlplus_output(ORACLE_DB_NAME, output)
            result = self.parse_sqlplus_result_space(output)
            if result and len(result) == 2:
                return result[0], result[1]

    def get_log_archive_config(self):
        file_name = self.saveCmdToScript(self.enrich_cmd(LOG_ARCHIVE_CONFIG))
        output = self._sqlPlusCmd(file_name)
        self._remove_temp_file(file_name)
        if output:
            self.parse_sqlplus_output(LOG_ARCHIVE_CONFIG, output)
            log_archive_config = self.parse_sqlplus_result_tab(output)
            if log_archive_config:
                return log_archive_config[0]


class OracleRACDiscoverer(OracleTopologyDiscoverer):
    def __init__(self, sqlPlusCmd, oracle_home=None):
        OracleTopologyDiscoverer.__init__(self, sqlPlusCmd, oracle_home)

    def discover(self):
        is_clustered = self.is_clustered_database()
        if is_clustered:
            return self.discover_rac()
        return (None, None)

    def is_clustered_database(self):
        file_name = self.saveCmdToScript(self.enrich_cmd(CLUSTERED_DATABASE))
        output = self._sqlPlusCmd(file_name)
        self._remove_temp_file(file_name)
        if output:
            self.parse_sqlplus_output(CLUSTERED_DATABASE, output)
            is_clustered = self.parse_sqlplus_result_tab(output)
            if is_clustered and len(is_clustered) == 1:
                return is_clustered[0] == "true"

    def discover_rac(self):
        file_name = self.saveCmdToScript(self.enrich_cmd(RAC_SERVICE_NAME))
        output = self._sqlPlusCmd(file_name)
        self._remove_temp_file(file_name)
        if output:
            self.parse_sqlplus_output(RAC_SERVICE_NAME, output)
            rac_servicename_reslut = self.parse_sqlplus_result_tab(output)
            if rac_servicename_reslut:
                rac_servicename = rac_servicename_reslut[0]
                rac_data_name, oracles = self.get_oracle_nodes()
                return (OracleRACBuilder.create_pdo(rac_servicename, rac_data_name), oracles)

    def get_oracle_nodes(self):
        rac_data_name = ''
        oracles = []
        cmdlist = []
        cmdlist.append("column INSTANCE_NAME format a32")
        cmdlist.append("column HOST_NAME format a32")
        cmdlist.append(self.enrich_cmd(ORACLE_INSTANCES))
        file_name = self.saveCmdListToScript(cmdlist)
        output = self._sqlPlusCmd(file_name)
        self._remove_temp_file(file_name)
        if output:
            self.parse_sqlplus_output(ORACLE_INSTANCES, output)
            rac_nodes = self.parse_rac_node(output)
            for node in rac_nodes:
                instance_name = node[0]
                host_name = node[1]
                if rac_data_name != '':
                    rac_data_name = rac_data_name + ':' + host_name
                else:
                    rac_data_name = host_name
                oracles.append(self.get_oracle_pdo(host_name, instance_name))
        return rac_data_name, oracles

    def parse_rac_node(self, output):
        result = []
        endOfHeader = 0
        try:
            for line in output.strip().splitlines():
                data = []
                if (line.find('-----') != -1) or (line.find('INSTANCE_NAME') != -1):
                    endOfHeader = 1
                    continue
                if endOfHeader == 1:
                    for item in line.split('\t'):
                        if item.strip():
                            data.append(item.strip())
                if data and len(data) == 2:
                    result.append((data[0], data[1]))
            return result
        except:
            exInfo = logger.prepareJythonStackTrace('')
            logger.debug("Failed to parse rac node.", exInfo)

    def get_oracle_pdo(self, host_name, instance_name):
        machine_ip = netutils.getHostAddress(host_name, None)
        if machine_ip is None:
            logger.debug('Failed to resolve host:', str(host_name))
        elif not netutils.isLocalIp(machine_ip):
            return OracleInstanceBuilder.create_pdo(instance_name, machine_ip)


class OracleDataFileDiscoverer(OracleTopologyDiscoverer):
    def __init__(self, sqlPlusCmd, oracle_home=None):
        OracleTopologyDiscoverer.__init__(self, sqlPlusCmd, oracle_home)

    def discover(self):
        cmdlist = []
        cmdlist.append('column file_name format a64')
        cmdlist.append(self.enrich_cmd(ORACLE_DATA_FILE))
        file_name = self.saveCmdListToScript(cmdlist)
        output = self._sqlPlusCmd(file_name)
        self._remove_temp_file(file_name)
        if output:
            self.parse_sqlplus_output(ORACLE_DATA_FILE, output)
            return self.parse_data_file(output)

    def parse_data_file(self, output):
        result = []
        endOfHeader = 0
        try:
            for line in output.strip().splitlines():
                data_file = []
                if (line.find('-----') != -1) or (line.find('FILE_NAME') != -1):
                    endOfHeader = 1
                    continue
                if endOfHeader == 1:
                    for item in line.split('\t'):
                        if item.strip():
                            data_file.append(item.strip())
                if data_file and len(data_file) == 2:
                    result.append(OracleDataFileBuilder.create_pdo(data_file[0], data_file[1]))
            return result
        except:
            exInfo = logger.prepareJythonStackTrace('')
            logger.debug("Failed to parse data file.", exInfo)


class OracleTNSDiscoverer:
    def __init__(self, shell, oracle_home):
        self._shell = shell
        self._oracle_home = oracle_home
        self._tns_list = None

    def getTNSInfoByConnectionIdentifier(self, connection_identifier):
        if not self._tns_list:
            self._tns_list = self.getTNSNameOra()
        for tns in self._tns_list:
            if tns.connection_identifier.lower() == connection_identifier.lower():
                return tns

    def getSQLPlusConnectionIdentifier(self, ipaddress):
        if not self._tns_list:
            self._tns_list = self.getTNSNameOra()
        for tns in self._tns_list:
            if tns.host_ip == ipaddress:
                return tns.connection_identifier

    def getTNSNameOra(self):
        tns_list = []

        # get config path
        if self._shell.isWinOs():
            configDir = self._oracle_home + '\\network\\admin\\'
        else:
            configDir = self._oracle_home + '/network/admin/'
        try:
            configFileContent = self._shell.safecat(configDir + 'tnsnames.ora')
            # parse config content
            tnsEntries = dbutils.parseTNSNames(configFileContent, '')

            if tnsEntries:
                for tns_entry in tnsEntries:
                    tns = TNSNameOra()
                    tns.connection_identifier = tns_entry[0]
                    tns.db_port = tns_entry[2]
                    tns.sid = tns_entry[3].upper()
                    host_ip = tns_entry[5]
                    if netutils.isValidIp(host_ip):
                        tns.host_ip = host_ip
                    tns_list.append(tns)
        except:
            logger.debug('Failed to get port for Oracle.')
        return tns_list