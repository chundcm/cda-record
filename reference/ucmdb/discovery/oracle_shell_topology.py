# coding=utf-8
from collections import namedtuple
import modeling
import netutils
import logger
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector


class OracleInstanceBuilder:
    CIT = "oracle"
    _Pdo = namedtuple('Pdo', ('name', 'host_ip', 'db_port', 'service_names'))

    @staticmethod
    def create_pdo(name, host_ip, db_port=None, service_names=None):
        return OracleInstanceBuilder._Pdo(name, host_ip, db_port, service_names)

    def build(self, pdo):
        vector = ObjectStateHolderVector()
        host_osh = modeling.createHostOSH(pdo.host_ip)
        oracle_osh = modeling.createDatabaseOSH('oracle', pdo.name, pdo.db_port, pdo.host_ip, host_osh)
        vector.add(host_osh)
        vector.add(oracle_osh)

        if pdo.service_names:
            logger.debug("pdo.service_names:", pdo.service_names)
            listener_builder = OracleListenerBuilder()
            listener_osh = listener_builder.build(host_osh)

            endpoint = netutils.createTcpEndpoint(pdo.host_ip, pdo.db_port)
            endpoint_builder = netutils.ServiceEndpointBuilder()
            endpoint_reporter = netutils.EndpointReporter(endpoint_builder)
            endpoint_osh = endpoint_reporter.reportEndpoint(endpoint, host_osh)

            vector.add(endpoint_osh)
            vector.add(modeling.createLinkOSH("usage", listener_osh, endpoint_osh))
            vector.add(listener_osh)

            for service_name in pdo.service_names:
                logger.debug("service_name:", service_name)
                service_name_osh = ObjectStateHolder("oracle_servicename")
                service_name_osh.setAttribute('name', service_name)
                service_name_osh.setContainer(listener_osh)
                vector.add(service_name_osh)
                vector.add(modeling.createLinkOSH('realization', service_name_osh, oracle_osh))
        return oracle_osh, vector

class OracleListenerBuilder:
    def build(self, host_osh):
        listener_osh = ObjectStateHolder('oracle_listener')
        listener_osh.setStringAttribute('vendor', 'oracle_corp')
        listener_osh.setStringAttribute('application_category', 'Database')
        listener_osh.setAttribute('discovered_product_name', 'TNS Listener')
        modeling.setApplicationProductName(listener_osh, applicationName='Oracle DB')

        listener_osh.setContainer(host_osh)
        return listener_osh


class OracleDataGuardBuilder:
    CIT = "oracle_data_guard"
    _Pdo = namedtuple('Pdo', ('name', 'dg_config', 'dbname', 'dbid'))

    @staticmethod
    def create_pdo(name, dg_config=None, db_name=None, dbid=None):
        return OracleDataGuardBuilder._Pdo(name, dg_config, db_name, dbid)

    def build(self, pdo):
        data_guard_osh = ObjectStateHolder('oracle_data_guard')
        data_guard_osh.setAttribute('dbid', pdo.dbid)
        data_guard_osh.setAttribute('db_name', pdo.dbname)
        data_guard_osh.setAttribute('dg_config', pdo.dg_config)
        return data_guard_osh


class OracleRACBuilder:
    CIT = "rac"
    _Pdo = namedtuple('Pdo', ('rac_servicename', 'data_name'))

    @staticmethod
    def create_pdo(rac_servicename=None, data_name=None):
        return OracleRACBuilder._Pdo(rac_servicename, data_name)

    def build(self, pdo):
        rac_osh = ObjectStateHolder('rac')
        rac_osh.setAttribute('rac_servicename', pdo.rac_servicename)
        rac_osh.setAttribute('data_name', pdo.data_name)
        return rac_osh


class OracleDataFileBuilder:
    CIT = "dbdatafile"
    _Pdo = namedtuple('Pdo', ('data_name', 'dbdatafile_fileid'))

    @staticmethod
    def create_pdo(data_name=None, dbdatafile_fileid=None):
        return OracleDataFileBuilder._Pdo(data_name, dbdatafile_fileid)

    def build(self, pdo):
        dbdatafile_osh = ObjectStateHolder('dbdatafile')
        dbdatafile_osh.setAttribute("dbdatafile_fileid", int(pdo.dbdatafile_fileid))
        dbdatafile_osh.setAttribute("data_name", pdo.data_name)
        return dbdatafile_osh


class Reporter(object):
    def __init__(self, oracle_builder=OracleInstanceBuilder(),
                 oracle_data_guard_builder=OracleDataGuardBuilder(),
                 oracle_rac_builder=OracleRACBuilder(),
                 oracle_data_file_builder=OracleDataFileBuilder()
                 ):
        self.oracle_builder = oracle_builder
        self.oracle_data_guard_builder = oracle_data_guard_builder
        self.oracle_rac_builder = oracle_rac_builder
        self.oracle_data_file_builder = oracle_data_file_builder

    def report_oracle_instance(self, pdo, database_role=None):
        oracle_osh, vector = self.oracle_builder.build(pdo)
        if database_role:
            oracle_osh.setAttribute("database_role", database_role)
        return oracle_osh, vector

    def report_oracle_data_guard(self, pdo, oracle_pdos, oracle_osh):
        vector = ObjectStateHolderVector()
        data_guard_osh = self.oracle_data_guard_builder.build(pdo)
        vector.add(data_guard_osh)
        vector.add(modeling.createLinkOSH('membership', data_guard_osh, oracle_osh))
        for oracle_pdo in oracle_pdos:
            oracle_osh_new, oracle_vector = self.oracle_builder.build(oracle_pdo)
            vector.addAll(oracle_vector)
            vector.add(modeling.createLinkOSH('membership', data_guard_osh, oracle_osh_new))
        return data_guard_osh, vector

    def report_oracle_rac(self, pdo, oracle_pdos, oracle_osh, oracle_dg_osh=None):
        vector = ObjectStateHolderVector()
        rac_osh = self.oracle_rac_builder.build(pdo)
        vector.add(rac_osh)
        vector.add(modeling.createLinkOSH('membership', rac_osh, oracle_osh))
        for oracle_pdo in oracle_pdos:
            oracle_osh_new, oracle_vector = self.oracle_builder.build(oracle_pdo)
            vector.addAll(oracle_vector)
            vector.add(modeling.createLinkOSH('membership', rac_osh, oracle_osh_new))

        if oracle_dg_osh:
            vector.add(modeling.createLinkOSH('membership', oracle_dg_osh, rac_osh))
        return vector

    def report_oracle_data_file(self, pdo, container_osh):
        vector = ObjectStateHolderVector()
        data_file_osh = self.oracle_data_file_builder.build(pdo)
        data_file_osh.setContainer(container_osh)
        vector.add(data_file_osh)
        return vector
