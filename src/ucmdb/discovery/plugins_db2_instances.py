# coding=utf-8
'''
Created on Mar 29, 2013

@author: ekondrashev
'''
from plugins import Plugin
from iteratortools import first
from fptools import partiallyApply as Fn, safeFunc as safeFn
import fptools
import logger
import netutils
import regutils
import file_system
import shell_interpreter
import host_topology
import host_base_parser
import modeling

from db import DatabaseServer
from dns_resolver import FallbackResolver, NsLookupDnsResolver, SocketDnsResolver
from dns_resolver import create as create_resolver

import db2_topology
import db2_base_shell_discoverer as base_shell_discoverer
import db2_win_shell_discoverer as winshell_discoverer
import db2_unix_shell_discoverer as unixshell_discoverer
import db2_win_reg_base_discoverer as winreg_base_discoverer
import db2_win_reg_discoverer as winreg_discoverer
from db2_topology import SoftwareBuilder, build_version_pdo


class Db2Plugin(Plugin):
    MAIN_PROCESS_NAME = None

    def __init__(self, *args, **kwargs):
        Plugin.__init__(self, *args, **kwargs)
        self._main_process = None
        self._db2_home_path = None
        self._db2_version = None
        self._shell = None

    def isApplicable(self, context):
        get_process = context.application.getProcess
        self._main_process = get_process(self.MAIN_PROCESS_NAME)
        self._shell = context.client
        if not self._main_process:
            logger.warn("No %s process found" % self.MAIN_PROCESS_NAME)
            return False
        return True

    def get_application_port(self, context):
        endpoints = context.application.getEndpointsByProcess(self._main_process)
        endpoints = sorted(endpoints, key=netutils.Endpoint.getPort)
        endpoint = first(endpoints)
        if endpoint:
            return endpoint.getPort()

    def get_db2_version(self, context):
        if not self._db2_version:
            self._db2_version = self._get_db2_version(context)
        return self._db2_version

    def get_shell_based_discoverer(self, context):
        raise NotImplemented('get_shell_based_discoverer')

    def _get_db2_version(self, context):
        raise NotImplemented('_get_db2_version')

    def get_instance_name(self, context):
        raise NotImplemented('get_instance_name')

    def get_db2_home_path(self, context):
        if not self._db2_home_path:
            self._db2_home_path = self._get_db2_home_path(context)
        return self._db2_home_path

    def _get_db2_home_path(self, context):
        raise NotImplemented('_get_db2_home_path')

    def resolve_ip_by_instance_name(self, instance_name):
        resolver = create_resolver(self._shell)
        try:
            ips = resolver.resolve_ips(instance_name)
            logger.debug('Resolved IPs %s' % ips)
            return ips and str(ips[0])
        except:
            logger.debugException('')
            logger.debug('Failed to resolve DB node by instance name %s' % instance_name)

    def _discover_local_databases(self, context, instance_name, application_port, db2_home_path,
                                  discoverer, executor, interpreter):
        db2_instance_osh = context.application.applicationOsh
        logger.debug('Processing instance %s' % instance_name)
        logger.debug('Using home %s' % db2_home_path)
        logger.debug('Using dsicoverer %s' % discoverer)
        get_local_databases = safeFn(discoverer.get_local_databases)
        local_dbs = get_local_databases(executor,
                                              interpreter,
                                              instance_name,
                                              db2_home_path=db2_home_path)
        #db2_cmd = base_shell_discoverer.get_configured_db2_cmd(interpreter, instance_name, db2_home_path)
        #db2_cmd.terminate()

        #logger.debug('Local databases %s' % local_dbs)
        if local_dbs:
            reporter = db2_topology.Reporter()

            address = context.application.getApplicationIp()
            hostOsh = context.hostOsh
            resolved_ip = self.resolve_ip_by_instance_name(instance_name)


            if resolved_ip:
                address = resolved_ip
                hostOsh = modeling.createHostOSH(address)
                db2_instance_osh.setContainer(hostOsh)
            logger.debug('Detected IP %s for instance name %s' % (address, instance_name))

            if not application_port:
                resolve_servicename = safeFn(discoverer.resolve_servicename)
                get_svcename = safeFn(discoverer.get_svcename_by_instancename)
                svce_name = get_svcename(executor, interpreter, instance_name, db2_home_path=db2_home_path)
                if svce_name:
                    net_service = resolve_servicename(executor, svce_name)
                    if net_service:
                        application_port = net_service.port
            #logger.debug('Detected local dbs %s' % local_dbs)
            inst = DatabaseServer(address, application_port)

            #logger.debug('Detected local instance IP and port %s , %s' % (address, application_port))
            local_dbs = [db2_topology.build_database_pdo(inst, db)
                               for db in local_dbs]
            oshs, ret_val = reporter.updateInstanceDatabases(db2_instance_osh, local_dbs, hostOsh, 1)
            context.resultsVector.addAll(oshs)
            return ret_val
        else:
            logger.debug('No local databases found for %s' % instance_name)

    def _discover_remote_databases(self, context, instname, db2_home_path,
                                   discoverer, executor, interpreter, local_dbs = None):
        local_dbserver_osh = context.application.applicationOsh
        get_remote_databases = safeFn(discoverer.get_remote_databases)
        node_db_pairs = get_remote_databases(executor, interpreter, instname, db2_home_path=db2_home_path) or ()
        #db2_cmd = base_shell_discoverer.get_configured_db2_cmd(interpreter, instname, db2_home_path)
        #db2_cmd.terminate()

        logger.debug('Processing instance %s' % instname)
        logger.debug('Using home %s' % db2_home_path)
        logger.debug('Using dsicoverer %s' % discoverer)
        get_node = safeFn(discoverer.get_node)
        reporter = db2_topology.Reporter()
        node_reporter = host_topology.Reporter()

        shell = context.client
        resolvers = (NsLookupDnsResolver(shell), SocketDnsResolver())
        resolve_ips_fn = FallbackResolver(resolvers).resolve_ips
        for nodename, remote_dbs in node_db_pairs:
          #  logger.debug('Processing node name %s' % nodename)
            node = get_node(executor, interpreter, instname,
                            nodename, db2_home_path=db2_home_path)
           # logger.debug('Node class %s' % node.__class__)
           # logger.debug('Fetched node object %s' % node)
            if node:
                host_osh = None
                address = None
                instance_name_ = None
                if node.is_local():
                    host_osh = context.hostOsh
                    address = context.application.getApplicationIp()
                    instance_name_ = node.instance_name
                else:
                    host = host_base_parser.parse_from_address(node.hostname,
                                     fptools.safeFunc(resolve_ips_fn))
                    if host and host.ips:
                        instance_name_ = node.remote_instance_name
                        address = first(host.ips)
                        host_osh, _, oshs_ = node_reporter.report_host_with_ips(host.ips)
                        context.resultsVector.addAll(oshs_)

                if host_osh:
                    get_port_fn = safeFn(discoverer.get_instance_port_by_node)
                    port = get_port_fn(executor, interpreter, node, db2_home_path=db2_home_path)
                    logger.debug('Db port is %s' % port)
                    logger.debug('Db address is %s' % address)
                    remote_instance = DatabaseServer(address, port)

                    remote_inst_osh, endpoint_osh, db_oshs, vector = reporter.reportServerAndDatabases(remote_instance, host_osh)
                    if str(remote_instance.address) == '127.0.0.1':
                        logger.debug('Encountered a localhost bound DB.')
                        #in case the ip is a localhost need to set the container to the current node
                        #this is actually a local db but configured in such a manner
                        currentNode = context.application.getOsh().getAttribute("root_container").getObjectValue()
                        endpoint_osh.setContainer(currentNode)
                        endpoint_osh.setStringAttribute('bound_to_ip_address', context.application.getApplicationIp())
                        remote_inst_osh.setContainer(currentNode)
                        #remote_inst_osh.setStringAttribute('application_ip', context.application.getApplicationIp())
                        SoftwareBuilder.updateName(remote_inst_osh, instname)
                        if local_dbs and len(local_dbs) == 1:
                            db_osh = local_dbs[0]
                            db_name = db_osh.getAttributeValue('name')
                            db_osh.setAttribute('application_port', port )
                            context.resultsVector.add(modeling.createLinkOSH('usage', db_osh , endpoint_osh))
                            #update other entries in vector
                            for obj in context.resultsVector:
                                if obj.getObjectClass() == 'db2_database' and obj.getAttributeValue('name') == db_name:
                                    #check if the port value is set for this instance
                                    if not obj.getAttribute('application_port'):
                                        obj.setAttribute('application_port', port )
                                for link_end in ("link_end1", "link_end2"):
                                    if  obj.getAttribute(link_end):
                                        end_obj = obj.getAttribute(link_end).getObjectValue()
                                        if end_obj.getObjectClass() == 'db2_database' and end_obj.getAttributeValue('name') == db_name:
                                            if not end_obj.getAttribute('application_port'):
                                                end_obj.setAttribute('application_port', port )

                    if instance_name_:
                        SoftwareBuilder.updateName(remote_inst_osh, instance_name_)
                        context.resultsVector.addAll(vector)
                        _, oshs = reporter.reportRemoteDatabases(remote_dbs, local_dbserver_osh, remote_inst_osh)
                        context.resultsVector.addAll(oshs)
                    else:
                        logger.debug('No instance name')
                        if endpoint_osh:
                            #logger.debug('Reporting IPSE')
                            context.resultsVector.add(endpoint_osh)
                            context.resultsVector.add(modeling.createLinkOSH('usage', remote_inst_osh, endpoint_osh))
                            alias_oshs, oshs = reporter.reportRemoteDatabases(remote_dbs, local_dbserver_osh, remote_inst_osh)
                            context.resultsVector.addAll(oshs)
                        #logger.debug('Will report IPSes for dbs %s' % db_oshs)
                        for db_osh in db_oshs:
                            context.resultsVector.add(modeling.createLinkOSH('usage', db_osh, endpoint_osh))
                else:
                    logger.debug('Host is not resolved %s' % node.hostname)
            else:
                logger.debug('No node found with name %s' % nodename)

    def execute_profile(self, shell, home_path):
        if home_path and shell and not shell.isWinOs():
            shell.execCmd(". %s/db2profile" % home_path)

    def process(self, context):
        r'''
         @types: applications.ApplicationSignatureContext
        '''

        shell = context.client
        language = shell.getOsLanguage().bundlePostfix

        db2_instance_osh = context.application.applicationOsh

        instance_name = self.get_instance_name(context)

        if instance_name:
            resolved_ip = self.resolve_ip_by_instance_name(instance_name)
            if resolved_ip:
                hostOsh = modeling.createHostOSH(resolved_ip)
                db2_instance_osh.setContainer(hostOsh)

            db2_home_path = self.get_db2_home_path(context)
            if db2_home_path:
                self.execute_profile(shell, db2_home_path)
                try:
                    version = self.get_db2_version(context)
                    SoftwareBuilder.updateVersion(db2_instance_osh,
                                                  build_version_pdo(version))
                except:
                    logger.debugException("DB2 version not supported")
                    logger.reportWarning("DB2 version not supported")

                SoftwareBuilder.updateName(db2_instance_osh, instance_name)
                SoftwareBuilder.updateApplciationPath(db2_instance_osh, db2_home_path)

                application_port = self.get_application_port(context)
                if application_port:
                    SoftwareBuilder.updateApplicationPort(db2_instance_osh,
                                                          application_port)

                discoverer = self.get_shell_based_discoverer(context)
                executor = discoverer.get_db2_command_executor(shell)
                interpreter = shell_interpreter.Factory().create(shell)

                base_shell_discoverer.Db2.set_db2_bundle(language)

                local_db_objs = self._discover_local_databases(context, instance_name, application_port, db2_home_path, discoverer, executor, interpreter)

                self._discover_remote_databases(context, instance_name, db2_home_path, discoverer, executor, interpreter, local_db_objs)
                db2_cmd = base_shell_discoverer.get_configured_db2_cmd(interpreter, instance_name, db2_home_path)
                try:
                    db2_cmd.terminate() | executor
                except:
                    logger.debugException("Failed to execute the db2 command")
            else:
                logger.debug('No db2 home path found')
        else:
            logger.debug('Failed to discover instance instance_name')


class Db2UnixPlugin(Db2Plugin):
    MAIN_PROCESS_NAME = r'db2sysc'

    def get_instance_name(self, context):
        return self._main_process.owner

    def _get_db2_version(self, context):
        instance_name = self.get_instance_name(context)
        shell = context.client
        get_version = unixshell_discoverer.get_version_by_instance_name
        return get_version(shell, instance_name)

    def get_shell_based_discoverer(self, context):
        version = self.get_db2_version(context)
        return unixshell_discoverer.registry.get_discoverer(version)

    def _get_db2_home_path(self, context):
        discoverer = self.get_shell_based_discoverer(context)
        instance_name = self.get_instance_name(context)
        shell = context.client
        return discoverer.get_instance_home_by_instance_name(shell,
                                                             instance_name)


class Db2WindowsPlugin(Db2Plugin):
    MAIN_PROCESS_NAME = r'db2syscs.exe'

    def _get_db2_version(self, context):
        home_path = self.get_db2_home_path(context)
        executor = base_shell_discoverer.get_command_executor(context.client)
        return winshell_discoverer.get_db2_version_by_home_path(executor,
                                                                home_path)
    def execute_profile(self, shell, home_path):
        return None

    def _get_db2_home_path(self, context):
        fileSystem = file_system.createFileSystem(context.client)
        path_tool = file_system.getPathTool(fileSystem)
        if self._main_process.executablePath:
            exe_path = file_system.Path(self._main_process.executablePath,
                                        path_tool)
            return exe_path.get_parent().get_parent()

    def get_shell_based_discoverer(self, context):
        version = self.get_db2_version(context)
        return winshell_discoverer.registry.get_discoverer(version)

    def get_instance_name(self, context):
        pid = self._main_process.getPid()
        if pid is not None:
            shell = context.client
            os_bitcount = shell.is64BitMachine() and 64 or 32

            reg_provider = regutils.getProvider(shell)
            version = self.get_db2_version(context)
            discoverer = winreg_discoverer.registry.get_discoverer(version,
                                                                   os_bitcount)
            execute_reg_command = Fn(winreg_base_discoverer.execute_reg_query,
                                     reg_provider,
                                     fptools._)
            execute_reg_command = safeFn(execute_reg_command)
            return fptools.findFirst(bool, map(execute_reg_command,
                                  (discoverer.GetInstanceNameByPid(pid),
                                   discoverer.GetClusterInstanceNameByPid(pid))
                                  ))
        else:
            logger.debug('pid is not available for the main db2 process')
