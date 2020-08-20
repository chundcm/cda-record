# coding=utf-8
import re
import sys
import shellutils
import logger
import oracle_shell_discoverer
from oracle_shell_topology import Reporter
from appilog.common.system.types.vectors import ObjectStateHolderVector
from oracle_shell_discoverer import SQLPlusShell
from com.hp.ucmdb.discovery.common import CollectorsConstants


def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    error_list = []
    try:
        client = Framework.createClient()
        OSHVResult.addAll(discoverOracle(Framework, client, Framework.getDestinationAttribute("credentialsId")))
    except:
        strException = str(sys.exc_info()[1])
        error_list.append(strException)
        exInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException, exInfo)
        OSHVResult.addAll(connectWithAvailableCred(Framework, error_list))

    reportError = OSHVResult.size() == 0
    if reportError:
        Framework.reportWarning('No Oracle topology discovered.')
        reportWarnings(error_list)

    return OSHVResult

PROTOCOL_TYPE = "oracle"
NA = "NA"

def connectWithAvailableCred(Framework, error_list=None):
    result = ObjectStateHolderVector()
    protocol = Framework.getDestinationAttribute('protocol')
    ip = Framework.getDestinationAttribute('ip_address')
    available_credentials = Framework.getAvailableProtocols(ip, protocol)

    if available_credentials:
        for credential in available_credentials:
            try:
                client = Framework.createClient(credential)
                result.addAll(discoverOracle(Framework, client, credential))
            except:
                strException = str(sys.exc_info()[1])
                error_list.append(strException)
                exInfo = logger.prepareJythonStackTrace('')
                logger.debug("Failed to connect using:", credential)
                logger.debug(strException, exInfo)
    return result


def discoverOracle(framework, client, shell_cred_id):
    shell = shellutils.ShellUtils(client)
    oracle_server_discoverer = oracle_shell_discoverer.OracleServerDiscoverer(framework, shell)
    oracle_details = oracle_server_discoverer.discover()
    logger.debug("oracle_details:", oracle_details)
    if not oracle_details:
        return
    oracle_home = None
    result = ObjectStateHolderVector()
    for sid, bin_path, oracle in oracle_details:
        logger.debug("discover oracle sid=%s, bin_path=%s:" % (sid, bin_path))
        oracle_server_discoverer.get_oracle_discoverer().set_environment_variable("ORACLE_SID", sid)
        if not bin_path:
            bin_path = ""
        else:
            match = re.match(r"(.*)[/\\]bin[/\\]+", bin_path, re.IGNORECASE)
            if match:
                oracle_home = match.group(1).strip()
                oracle_server_discoverer.get_oracle_discoverer().set_environment_variable("ORACLE_HOME", oracle_home)

        try:
            vector = discoverOracleTopology(framework, client, shell, bin_path, oracle, oracle_home)
            if vector:
                result.addAll(vector)
                continue
        except:
            exInfo = logger.prepareJythonStackTrace('')
            logger.debug("Failed to run sqlplus as sysdba")
            logger.debug(exInfo)

        logger.debug("connect with Oracle credential")
        credential_ids = framework.getTriggerCIDataAsList("db_credential_id")
        logger.debug("credential_ids from Trigger CI [", credential_ids, "]")

        if credential_ids:
            for credential_id in credential_ids:
                if credential_id:
                    try:
                        vector = discoverOracleTopology(framework, client, shell, bin_path, oracle, oracle_home, credential_id)
                        if vector:
                            result.addAll(vector)
                            continue
                    except:
                        exInfo = logger.prepareJythonStackTrace('')
                        logger.debug("Failed to run with Oracle credential:", credential_id)
                        logger.debug(exInfo)

        logger.debug("connect with all available credential")
        ip = framework.getDestinationAttribute('ip_address')
        available_credentials = framework.getAvailableProtocols(ip, "sql")
        logger.debug("Available db credentials [", available_credentials, "] for ip: ", ip)

        if available_credentials:
            for credential_id in available_credentials:
                try:
                    protocolDbType = framework.getProtocolProperty(credential_id, CollectorsConstants.SQL_PROTOCOL_ATTRIBUTE_DBTYPE, NA)
                    if not re.match(PROTOCOL_TYPE, protocolDbType, re.IGNORECASE):
                        logger.debug("Ignore credential [", credential_id, "], because the protocol_type is: ", protocolDbType)
                        continue

                    vector = discoverOracleTopology(framework, client, shell, bin_path, oracle, oracle_home, credential_id)
                    if vector:
                        result.addAll(vector)
                        continue
                except:
                    exInfo = logger.prepareJythonStackTrace('')
                    logger.debug("Failed to run with db credential:", credential_id)
                    logger.debug(exInfo)
    if result and result.size() > 0:
        return result
    raise Exception("No Oracle Topology discover with shell credential:" + shell_cred_id)

def discoverOracleTopology(framework, client, shell, bin_path, oracle, oracle_home, cred_id=None):
    vector = ObjectStateHolderVector()
    reporter = Reporter()
    sqlPlusCmd = SQLPlusShell(framework, client, shell, cred_id, bin_path, True)
    database_role = oracle_shell_discoverer.OracleDatabaseRoleDiscoverer(sqlPlusCmd, oracle_home).discover()
    logger.debug("database_role_main as sysdba:", database_role)
    if not database_role:
        sqlPlusCmd = SQLPlusShell(framework, client, shell, cred_id, bin_path, False)
        database_role = oracle_shell_discoverer.OracleDatabaseRoleDiscoverer(sqlPlusCmd, oracle_home).discover()
        logger.debug("database_role_main:", database_role)

    if database_role == "STANDBY":
        database_role = calculateDatabaseRole(framework, oracle)
        if database_role:
            oracle_osh, oracle_vector = reporter.report_oracle_instance(oracle, database_role)
            vector.addAll(oracle_vector)
            return vector


    if not database_role:
        raise Exception("Failed to connect to sqlplus:", cred_id)

    data_guard, dg_oracles = oracle_shell_discoverer.OracleDataGuardDiscoverer(sqlPlusCmd, database_role, oracle_home).discover()
    logger.debug("data_guard:", data_guard)
    oracle_rac, rac_oracles = oracle_shell_discoverer.OracleRACDiscoverer(sqlPlusCmd, oracle_home).discover()
    data_files = oracle_shell_discoverer.OracleDataFileDiscoverer(sqlPlusCmd, oracle_home).discover()

    oracle_osh, oracle_vector = reporter.report_oracle_instance(oracle, database_role)
    vector.addAll(oracle_vector)

    oracle_data_guard_osh = None
    if data_guard and dg_oracles:
        logger.debug("Reporting Oracle Data Guard...")
        oracle_data_guard_osh, dg_vector = reporter.report_oracle_data_guard(data_guard, dg_oracles, oracle_osh)
        vector.addAll(dg_vector)

    logger.debug("oracle_rac:", oracle_rac)
    logger.debug("rac_oracles:", rac_oracles)

    if oracle_rac and rac_oracles:
        logger.debug("Reporting Oracle RAC...")
        vector.addAll(reporter.report_oracle_rac(oracle_rac, rac_oracles, oracle_osh, oracle_data_guard_osh))

    if data_files:
        for data_file in data_files:
            vector.addAll(reporter.report_oracle_data_file(data_file, oracle_osh))
    return vector

def reportWarnings(errorList):
    if errorList:
        for error in errorList:
            if error.strip():
                logger.reportWarning(error)

def calculateDatabaseRole(framework, oracle):
    logger.debug("calculating database role:", oracle.name.lower())
    dg_config = framework.getTriggerCIDataAsList("dg_config")
    logger.debug("dg_config:", dg_config)
    if dg_config:
        return "PHYSICAL STANDBY"

    return None

