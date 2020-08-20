import re
import logger
import shellutils
import host_base_parser
import webseal_topology
import netutils
from appilog.common.utils import Protocol
from com.hp.ucmdb.discovery.library.common import CollectorsParameters
import datetime
import time
from com.hp.ucmdb.discovery.library.clients.protocols.command import TimeoutException
from java.lang import Exception as JavaException

USER_PERMISSION_ERRORS = [ 'HPDIA0205W', #The user's account has expired
                          'HPDIA0306W', #This account has been temporarily locked out due to too many failed login attempts
                          'HPDIA0200W', #Authentication failed. You have used an invalid user name, password or client certificate
                          'HPDIA0202W', #An unknown user name was presented to Access Manager
                          'UNKNOWN USER' #An unknown user name  or password.
                          ]
class UnsupportedCredsForDomain(JavaException):
    pass

class WebSealShell:
    def __init__(self, framework, client, webseal_credentials_id, prefix = '', shell_timeout=15000):
        self.client = client
        self.framework = framework
        self.webseal_credentials_id = webseal_credentials_id
        self.shell = shellutils.ShellUtils(client)
        self.prefix = prefix
        self.binary_name = 'pdadmin'
        self.pdadmin_cmd = None
        self.sudo_path = ''
        try:
            self.shell_timeout = int(shell_timeout)
        except:
            logger.warn('Failed to convert %s to integer. Default value 15000 will be used')
            self.shell_timeout = 15000
        self.timout_encountered = 0
        if not client.isInteractiveAuthenticationSupported():
            raise ValueError('Unsupported protocol')
        if self.shell.isWinOs():
            self.enable_shell_proxy()
        else:
            self.sudo_path = self.shell._UnixShell__getSudoPath()
        self.setup_command()
        logger.debug('Using command %s ' % self.pdadmin_cmd)
        
    def setup_command(self):
        if not self.webseal_credentials_id:
            return None
        logger.debug('Inside setup_command')
        username = self.framework.getProtocolProperty(self.webseal_credentials_id, "protocol_username")
        self.client.clearCommandToInputAttributeMatchers()
        self.pdadmin_cmd = self.prefix + self.binary_name + ' -a %s ' % username
        matcher_cmd = self.pdadmin_cmd
        if not self.shell.isWinOs() and self.shell.isSudoConfigured():
            #this is a unix destination with configured sudo
            #need to check if pdadmin will be preffixed
            if self.shell._UnixShell__canUseSudo(self.pdadmin_cmd):
                matcher_cmd = "%s %s" %(self.sudo_path, self.pdadmin_cmd)
        self.client.addCommandToInputAttributeMatcher(matcher_cmd,
                                        "Enter Password:",
                                        Protocol.PROTOCOL_ATTRIBUTE_PASSWORD,
                                        self.webseal_credentials_id)

    def enable_shell_proxy(self):
        localFile = CollectorsParameters.BASE_PROBE_MGR_DIR + CollectorsParameters.getDiscoveryResourceFolder() + \
            CollectorsParameters.FILE_SEPARATOR + 'pdadmin_proxy.bat'
        remote_file = self.shell.copyFileIfNeeded(localFile)
        if not remote_file:
            raise ValueError("Failed to set up pdadmin call proxy.")
        self.binary_name = 'pdadmin_proxy.bat'
        m = re.search('\$(.+)pdadmin_proxy.bat', remote_file)
        if not m:
            raise ValueError("Failed to set up pdadmin call proxy.")
        self.prefix = '%SystemRoot%' + m.group(1)
        
    def enumerate_domains(self):
        output = self.get_output('-d Default domain list')
        if output and output.strip():
            return [x.strip() for x in re.split('[\r\n]+', output) if x and x.strip()]
        
    def get_output(self, command, timeout=None, err_codes = (0, 9009)):
        time_out = timeout or self.shell_timeout
        output = self.shell.execCmd(cmdLine='%s%s' % (self.pdadmin_cmd, command), timeout=time_out)
        
        if self.shell.getLastCmdReturnCode() == 2 and output:
            uppercased = output.upper()
            for message in USER_PERMISSION_ERRORS:
                if uppercased.find(message) != -1:
                    raise UnsupportedCredsForDomain(output)
        
        if (self.shell.getLastCmdReturnCode() in err_codes ):
            return output
            
    def get_bulk_output(self, command, timeout=None):
        return self.get_output(command, timeout, (0, 1, 9009))
        
    def get_result_as_dict(self, output, separator = ''):
        result_dict = {}
        if output and output.strip():
            for line in output.splitlines():
                m = re.match('(.+?):(.+)', line)
                if m:
                    result_dict[m.group(1).strip()] = m.group(2).strip()
        return result_dict

JUNCTION_TYPE_TO_DEFAULT_PORT_MAP = {'tcp' : 8080, 'ssl' : 8443, 'local' : 80}
class JunctionDiscoverer:
    
    def __init__(self, web_shell, resolver, local_host, domain = 'Default'):
        self._web_shell = web_shell
        self._dns_resolver = resolver
        self._local_host = local_host
        self._domain = domain

    def _parse_list_junctions(self, output):
        if not output:
            return []
        return output.strip().splitlines()
    
    def list_junctions(self, server_name):
        if not server_name:
            return []
        try:
            output = self._web_shell.get_output('-d %s server task %s list' % (self._domain, server_name))
        except TimeoutException:
            logger.debug('Skipping server junctions due to timeout issue.')
            return []
        return self._parse_list_junctions(output)
        
    def _parse_junction(self, output):
        if not output:
            return []
        buffs = re.split('Server\s\d+', output)
        details_dict = self._web_shell.get_result_as_dict(buffs[0])
        endpoints = []
        port = None
        server_state = None
        for buff in buffs[1:]:
            server_details = self._web_shell.get_result_as_dict(buff)
            server_state = server_details.get('Server State')
            host = server_details.get('Hostname')
            port = server_details.get('Port')
            #server_id = server_details.get('ID')
            endpoint = None
            try:
                endpoint = self._resolve( (host, port) )
            except:
                logger.warn('Failed to resolve Junction server IP. Ip and port data will be missing. Host name %s' % host)
            if endpoint:
                endpoints.append( endpoint )
            junction_type = details_dict.get('Type').lower()
            port = JUNCTION_TYPE_TO_DEFAULT_PORT_MAP.get(junction_type)
        return [details_dict.get('Junction point'),  endpoints, server_state, port ]

    def _resolve(self,  info):
        host, port = info
        endpoints = []
        if host.lower() == 'localhost' or host.lower() == 'localhost.localdomain':
            host = self._local_host

        host = host_base_parser.parse_from_address(host, self._dns_resolver.resolve_ips)
        host = host_base_parser.HostDescriptor(ips=host.ips, name=None, fqdns=[])
        for ip in host.ips:
            endpoint = netutils.createTcpEndpoint(ip, port)
            endpoints.append(endpoint)

        return (host, endpoints)

    def get_junction(self, server_name, junction_name):
        if not (junction_name and server_name):
            return []
        try:
            output = self._web_shell.get_output('-d %s server task %s show %s' % (self._domain, server_name, junction_name))
        except TimeoutException:
            logger.debug('Skipping junction due to timeout issue.')
            return
        details = self._parse_junction(output)
        logger.debug('Fetched junction details %s' % str(details))
        return details
        
    def discover(self, servers):
        
        #logger.debug('Passed servers %s' % servers)
        server_to_junction_map = {}
        server_to_junction_local_port_map = {}
        for server_name in servers:
            #self.getInstanceConfig(server_name)
            junction_names = self.list_junctions(server_name)
            logger.debug('List of junctions for server %s is %s' % (server_name, junction_names))
            for junction_name in junction_names:
                try:
                    details = self.get_junction(server_name, junction_name)
                    if details and details[0]:
                        junctions = server_to_junction_map.get(server_name, [])
                        junctions.append(details[:3])
                        server_to_junction_map[server_name] = junctions
                        if details[3]:
                            ports = server_to_junction_local_port_map.get(server_name, [])
                            ports.append(details[3])
                            server_to_junction_local_port_map[server_name] = ports
                except:
                    logger.debugException('Failed to discover junction')
        return server_to_junction_map, server_to_junction_local_port_map

class VirtualHostJunctionDiscoverer:
    def __init__(self, web_shell, resolver, local_host, domain = 'Default'):
        self._web_shell = web_shell
        self._dns_resolver = resolver
        self._local_host = local_host
        self._domain = domain

    def _parse_list_junctions(self, output):
        if not output:
            return []
        return output.strip().splitlines()
    
    def list_junctions(self, server_name):
        if not server_name:
            return []
        try:
            output = self._web_shell.get_output('-d %s server task %s virtualhost list' % (self._domain, server_name))
        except TimeoutException:
            logger.debug('Skipping server virtual junctions due to timeout issue.')
            return []
        return self._parse_list_junctions(output)
    
    def _parse_junction(self, output):
        if not output:
            return []
        buffs = re.split('Server\s\d+', output)
        details_dict = self._web_shell.get_result_as_dict(buffs[0])
        endpoints = []
        port = None
        server_state = None
        virtual_host_name = details_dict.get('Virtual hostname')
        for buff in buffs[1:]:
            server_details = self._web_shell.get_result_as_dict(buff)
            server_state = server_details.get('Server State')
            host = server_details.get('Hostname')
            port = server_details.get('Port')
            #server_id = server_details.get('ID')
            endpoint = None
            try:
                endpoint = self._resolve( (host, port) )
            except:
                logger.warn('Failed to resolve Junction server IP. Ip and port data will be missing. Host name %s' % host)
            if endpoint:
                endpoints.append( endpoint )
            junction_type = details_dict.get('Type').lower()
            port = JUNCTION_TYPE_TO_DEFAULT_PORT_MAP.get(junction_type)
        return [details_dict.get('Virtual Host Junction label'), endpoints, server_state, virtual_host_name, port]

    def _resolve(self,  info):
        host, port = info
        endpoints = []
        if host.lower() == 'localhost' or host.lower() == 'localhost.localdomain':
            host = self._local_host

        host = host_base_parser.parse_from_address(host, self._dns_resolver.resolve_ips)
        host = host_base_parser.HostDescriptor(ips=host.ips, name=None, fqdns=[])
        for ip in host.ips:
            endpoint = netutils.createTcpEndpoint(ip, port)
            endpoints.append(endpoint)

        return (host, endpoints)

    def get_junction(self, server_name, junction_name):
        if not (junction_name and server_name):
            return []
        try:
            output = self._web_shell.get_output('-d %s server task %s virtualhost show %s' % (self._domain, server_name, junction_name))
        except TimeoutException:
            logger.debug('Skipping junction due to timeout issue.')
            return
        details = self._parse_junction(output)
        logger.debug('Fetched virtual host junction details %s' % str(details))
        return details
    
    def discover(self, servers):
        
        #logger.debug('Passed servers %s' % servers)
        server_to_junction_map = {}
        server_to_junction_local_port_map = {}
        for server_name in servers:
            #self.getInstanceConfig(server_name)
            junction_names = self.list_junctions(server_name)
            logger.debug('List of virtual host junctions for server %s is %s' % (server_name, junction_names))
            for junction_name in junction_names:
                try:
                    details = self.get_junction(server_name, junction_name)
                    if details and details[0]:
                        junctions = server_to_junction_map.get(server_name, [])
                        junctions.append(details[:4])
                        server_to_junction_map[server_name] = junctions
                        if details[4]:
                            ports = server_to_junction_local_port_map.get(server_name, [])
                            ports.append(details[4])
                            server_to_junction_local_port_map[server_name] = ports
                except:
                    logger.debugException('Failed to discover junction')
        return server_to_junction_map, server_to_junction_local_port_map
    
class VirtualHostJunctionDiscovererUnix(VirtualHostJunctionDiscoverer):
    
    def __init__(self, web_shell, resolver, local_host, domain = 'Default'):
        VirtualHostJunctionDiscoverer.__init__(self, web_shell, resolver, local_host, domain)
        self.timestamp = str(time.mktime(datetime.datetime.now().timetuple()))
        self.file_path = '/tmp/'+ self.timestamp + '.txt'
        self.server_junctions_map = {}
        
    def _create_cmd_tmp_file(self, server_name, junction_names):
        shell = self._web_shell.shell
        if not junction_names:
            return
        for junction_name in junction_names:
            shell.execCmd('echo "server task %s virtualhost show %s" >> %s' % (server_name, junction_name, self.file_path))
            
    def _fetch_server_junction_details(self, amount_of_junctions=1):
        timeout = amount_of_junctions * self._web_shell.shell_timeout
        output = self._web_shell.get_bulk_output('-d %s %s' % (self._domain ,self.file_path), timeout)
        try:
            if output and output.strip():
                elems = re.split('.*server task\s+(.+)\s+virtualhost\s+show\s+(.+)', output)
                if not elems:
                    raise ValueError('Failed to discover junctions')
                elems = elems[1:]
                for i in xrange(0, len(elems), 3):
                    self.server_junctions_map['%s%s' % (elems[i].strip(), elems[i+1].strip())] = elems[i+2]
                
        finally:
            self._web_shell.shell.execCmd('rm %s' % self.file_path)
    
    def get_junction(self, server_name, junction_name):
        if not (junction_name and server_name):
            return []
        output = self.server_junctions_map.get('%s%s' % (server_name, junction_name))
        if output and output.lower().find('error') != -1:
            return []
        details = self._parse_junction(output)
        logger.debug('Fetched virtualhost junction details %s' % str(details))
        return details
    
    def _parse_list_junctions(self, output):
        if not output:
            return {}
        result_dict = {}
        elems = re.split('.*server task\s+(.+)\s+virtualhost\s+list', output)
        #logger.debug('_parse_list_junctions elements %s' % elems)
        if not elems:
            raise ValueError('Failed to discover virtualhost junctions')
        elems = elems[1:]
        for i in xrange(0, len(elems), 2):
            result_dict[elems[i].strip()] = []
            if elems[i+1] and elems[i+1].strip():
                result_dict[elems[i].strip()] = [x and x.strip() for x in elems[i+1].splitlines() if x and x.upper().find('ERROR') == -1] 
        return result_dict
    
    def prepare_list_junctions(self, server_names):
        if not server_names:
            return None
        shell = self._web_shell.shell
        output = None
        try:
            for server_name in server_names:
                shell.execCmd('echo "server task %s virtualhost list" >> %s' % (server_name, self.file_path))
            timeout = len(server_names) * self._web_shell.shell_timeout
            output = self._web_shell.get_bulk_output('-d %s %s' % (self._domain, self.file_path), timeout)
            #logger.debug('Got output %s' % output)
        finally:
            self._web_shell.shell.execCmd('rm %s' % self.file_path)
        return self._parse_list_junctions(output)
    
    def discover(self, servers):
        
        #logger.debug('Passed servers %s' % servers)
        logger.debug('VitrualHost Junctions discovery start')
        server_to_junction_map = {}
        server_to_junction_local_port_map = {}
        server_to_junction_dict = self.prepare_list_junctions(servers)
        logger.debug('server_to_junction_dict value is %s' % server_to_junction_dict)
        for server_name in servers:
            logger.debug('Processing "%s"' % server_name)
            junction_names = server_to_junction_dict.get(server_name, [])
            self._create_cmd_tmp_file(server_name, junction_names)
        elems_quont = sum(map(lambda x: x and len(x) or 0, server_to_junction_dict.values()))
        self._fetch_server_junction_details(elems_quont)
        for server_name in servers:
            #self.getInstanceConfig(server_name)
            junction_names = server_to_junction_dict.get(server_name, [])
            logger.debug('List of virtualhost junctions for server %s is %s' % (server_name, junction_names))
            for junction_name in junction_names:
                try:
                    details = self.get_junction(server_name, junction_name)
                    if details and details[0]:
                        junctions = server_to_junction_map.get(server_name, [])
                        junctions.append(details[:4])
                        server_to_junction_map[server_name] = junctions
                        if details[4]:
                            ports = server_to_junction_local_port_map.get(server_name, [])
                            ports.append(details[4])
                            server_to_junction_local_port_map[server_name] = ports
                except:
                    logger.debugException('Failed to discover virtualhost junction')
        return server_to_junction_map, server_to_junction_local_port_map


class JunctionDiscovererUnix(JunctionDiscoverer):
    
    def __init__(self, web_shell, resolver, local_host, domain = 'Default'):
        JunctionDiscoverer.__init__(self, web_shell, resolver, local_host, domain)
        self.timestamp = str(time.mktime(datetime.datetime.now().timetuple()))
        self.file_path = '/tmp/'+ self.timestamp + '.txt'
        self.server_junctions_map = {}
        
    def _create_cmd_tmp_file(self, server_name, junction_names):
        shell = self._web_shell.shell
        if not junction_names:
            return
        for junction_name in junction_names:
            shell.execCmd('echo "server task %s show %s" >> %s' % (server_name, junction_name, self.file_path))
            
    def _fetch_server_junction_details(self, amount_of_junctions=1):
        timeout = amount_of_junctions * self._web_shell.shell_timeout
        output = self._web_shell.get_bulk_output('-d %s %s' % (self._domain ,self.file_path), timeout)
        try:
            if output and output.strip():
                elems = re.split('.*server task\s+(.+)\s+show\s+(.+)', output)
                if not elems:
                    raise ValueError('Failed to discover junctions')
                elems = elems[1:]
                for i in xrange(0, len(elems), 3):
                    self.server_junctions_map['%s%s' % (elems[i].strip(), elems[i+1].strip())] = elems[i+2]
                
        finally:
            self._web_shell.shell.execCmd('rm %s' % self.file_path)
    
    def get_junction(self, server_name, junction_name):
        if not (junction_name and server_name):
            return []
        output = self.server_junctions_map.get('%s%s' % (server_name, junction_name))
        if output and output.lower().find('error') != -1:
            return []
        details = self._parse_junction(output)
        logger.debug('Fetched junction details %s' % str(details))
        return details
    
    def _parse_list_junctions(self, output):
        if not output:
            return {}
        result_dict = {}
        elems = re.split('.*server task\s+(.+)\s+list', output)
        logger.debug('_parse_list_junctions elements %s' % elems)
        if not elems:
            raise ValueError('Failed to discover junctions')
        elems = elems[1:]
        for i in xrange(0, len(elems), 2):
            result_dict[elems[i].strip()] = []
            if elems[i+1] and elems[i+1].strip():
                result_dict[elems[i].strip()] = [x and x.strip() for x in elems[i+1].splitlines() if x and x.upper().find('ERROR') == -1] 
        return result_dict
    
    def prepare_list_junctions(self, server_names):
        if not server_names:
            return None
        shell = self._web_shell.shell
        output = None
        try:
            for server_name in server_names:
                shell.execCmd('echo "server task %s list" >> %s' % (server_name, self.file_path))
            timeout = len(server_names) * self._web_shell.shell_timeout
            output = self._web_shell.get_bulk_output('-d %s %s' % (self._domain, self.file_path), timeout)
            logger.debug('Got output %s' % output)
        finally:
            self._web_shell.shell.execCmd('rm %s' % self.file_path)
        return self._parse_list_junctions(output)
    
    def discover(self, servers):
        
        #logger.debug('Passed servers %s' % servers)
        server_to_junction_map = {}
        server_to_junction_local_port_map = {}
        server_to_junction_dict = self.prepare_list_junctions(servers)
        logger.debug('server_to_junction_dict value is %s' % server_to_junction_dict)
        for server_name in servers:
            logger.debug('Processing "%s"' % server_name)
            junction_names = server_to_junction_dict.get(server_name, [])
            self._create_cmd_tmp_file(server_name, junction_names)
        elems_quont = sum(map(lambda x: x and len(x) or 0, server_to_junction_dict.values()))
        self._fetch_server_junction_details(elems_quont)
        for server_name in servers:
            #self.getInstanceConfig(server_name)
            junction_names = server_to_junction_dict.get(server_name, [])
            logger.debug('List of junctions for server %s is %s' % (server_name, junction_names))
            for junction_name in junction_names:
                try:
                    details = self.get_junction(server_name, junction_name)
                    if details and details[0]:
                        junctions = server_to_junction_map.get(server_name, [])
                        junctions.append(details[:3])
                        server_to_junction_map[server_name] = junctions
                        if details[3]:
                            ports = server_to_junction_local_port_map.get(server_name, [])
                            ports.append(details[3])
                            server_to_junction_local_port_map[server_name] = ports
                except:
                    logger.debugException('Failed to discover junction')
        return server_to_junction_map, server_to_junction_local_port_map
    
def getJunctionDiscoverer(isWin):
    if isWin:
        return JunctionDiscoverer
    return JunctionDiscovererUnix

def getVirtualHostJunctionDiscoverer(isWin):
    if isWin:
        return VirtualHostJunctionDiscoverer
    return VirtualHostJunctionDiscovererUnix

class PolicyServerDiscoverer:
    def __init__(self, web_shell, dns_resolver, local_host, domain = 'Default'):
        self._web_shell = web_shell
        self._dns_resolver = dns_resolver
        self._local_host = local_host
        self._domain = domain
        
    def getInstanceConfig(self, server_name):
            if not server_name:
                return None
                
            m = re.match('(\w+)\-', server_name)
            instance_name = m and m.group(1)
            
            if not instance_name:
                return None
                
            return self._web_shell.get_output('server task %s file cat /opt/pdweb/etc/webseald-%s.conf 0' % (server_name, instance_name))
            
            #return self._web_shell.get_output('server task %s file cat /opt/pdweb/etc/webseald-%s.conf 0 | grep -E "https|http|auth" | grep -v "#"' % (server_name, instance_name))
                    
    def discover(self):
        results = []
        server_names = self.list_servers()
        if not server_names:
            raise ValueError('No Policy Servers found.')
        for server_name in server_names:
            try:
                try:
                    host, port = self.get_server_details(server_name)
                    #self.getInstanceConfig(server_name)
                    m = re.match('^([\w\-\.]+)\-webseald', server_name)
                    instance_name = m and m.group(1)
                    results.append( self._resolve( (server_name, instance_name, host, port)) )
                except TimeoutException:
                    logger.debugException('Failed to get server details. Skipping.')
            except:
                logger.debugException('')
                logger.warn('Failed to get required server data, server %s is skipped.' % server_name)
                logger.reportWarning('Failed to get required server data')
        return results
    
    def _resolve(self,  policy_server_info):
        webseal_name, instance_name, host_str, port = policy_server_info
        endpoints = []
        if host_str.lower() == 'localhost':
            host_str = self._local_host
        try:
            host = host_base_parser.parse_from_address(host_str, self._dns_resolver.resolve_ips)
        except:
            logger.debug('Failed to resolve host %s' % host_str)
            if host_str.find('.') != -1:
                logger.debug('Host is an FQDN host, will try to resolve host name')
                host = host_base_parser.parse_from_address(host_str.split('.')[0], self._dns_resolver.resolve_ips)
            else:
                raise ValueError('Failed to resolve WebSeal host.')
        
        host = host_base_parser.HostDescriptor(ips=host.ips, name=None, fqdns=[])
        for ip in host.ips:
            endpoint = netutils.createTcpEndpoint(ip, port)
            endpoints.append(endpoint)

        return webseal_topology.WebsealServerBuilder.create_pdo(name=webseal_name, instance_name=instance_name), host, endpoints
    
    def _parse_server_details(self, output):
        hostname = None
        port = None
        if output:
            m = re.search('Hostname:\s+([\w\.\-]+)[\r\n]', output)
            hostname = m and m.group(1).strip()
            
            m = re.search('Administration Request Port:\s*(\d+)', output)
            port = m and m.group(1)
            
        return (hostname, port)
    
    def get_server_details(self, server_name):
        output = self._web_shell.get_output('-d %s server show %s' % (self._domain, server_name))
        return self._parse_server_details(output)
    
    def __parse_server_list(self, output):
        if output:
            return [x.strip() for x in output.splitlines() if x and x.strip()]
        
    def list_servers(self):
        return self.__parse_server_list( self._web_shell.get_output('-d %s server list' % self._domain))
    
class PolicyServerDiscovererUnix(PolicyServerDiscoverer):
    def __init__(self, web_shell, dns_resolver, local_host, domain = 'Default'):
        PolicyServerDiscoverer.__init__(self, web_shell, dns_resolver, local_host, domain)
        self.timestamp = str(time.mktime(datetime.datetime.now().timetuple()))
        self.file_path = None
        self.server_name_server_details_map = {}
        
    def _create_cmd_tmp_file(self, server_names):
        self.file_path = '/tmp/'+ self.timestamp + '.txt'
        shell = self._web_shell.shell
        for server_name in server_names:
            if re.search('webseald', server_name.lower()):
                shell.execCmd('echo "server show %s" >> %s' % (server_name, self.file_path))
            else:
                continue
        
    def _fetch_server_details(self, amount_of_servers=1): 
        timeout = amount_of_servers * self._web_shell.shell_timeout
        output = self._web_shell.get_output('-d %s %s' % (self._domain ,self.file_path), timeout)
        try:
            if output and output.strip():
                elems = re.split('.*server show\s+(.+)', output)
                if not elems:
                    raise ValueError('Failed to discover servers')
                elems = elems[1:]
                self.server_name_server_details_map = dict(zip([x and x.strip() for x in elems[::2]], elems[1::2]))
                logger.debug('self.server_name_server_details_map = %s' % self.server_name_server_details_map)
        except:
            logger.debugException('')
        finally:
            self._web_shell.shell.execCmd('rm %s' % self.file_path)
            
    def discover(self):
        results = []
        server_names = self.list_servers()
        if not server_names:
            raise ValueError('No Policy Servers found.')
        try:
            try:
                self._create_cmd_tmp_file(server_names)
                self._fetch_server_details()
            except TimeoutException, ex:
                logger.debug('An unrecoverable timeout exception for bulk get has been encountered.')
                raise ex
        except Exception, ex:
                logger.debugException('')
                logger.warn('Failed to get required server data')
                #logger.reportWarning('Failed to get required server data')
        for server_name in server_names:
            try:
                host, port = self.get_server_details(server_name)
                logger.debug('Parsed out host is %s , port %s for server_name %s' % (host, port, server_name))
                #self.getInstanceConfig(server_name)
                m = re.match('(\w+)\-webseald', server_name)
                instance_name = m and m.group(1)
                logger.debug('instance name is %s' % instance_name)
                results.append( self._resolve( (server_name, instance_name, host, port)) )
            except:
                logger.debugException('')
                logger.warn('Failed to get required server data, server %s is skipped.' % server_name)
                #logger.reportWarning('Failed to get required server data')
        return results
    
    def get_server_details(self, server_name):
        output = self.server_name_server_details_map.get(server_name and server_name.strip())
        return self._parse_server_details(output)
        
def getServerDiscovererClass(isWin):
    if isWin:
        return PolicyServerDiscoverer
    return PolicyServerDiscovererUnix

class ReverseProxyDiscoverer:
    def __init__(self, web_shell):
        self._web_shell = web_shell
    
    def discover(self):
        pass
    
def enrich_ports_information(servers, server_to_junction_local_port_map):
    result = []
    for pdo, host, endpoints in servers:
        ports = server_to_junction_local_port_map.get(pdo.name)
        if ports and host and host.ips:
            for ip in host.ips:
                for port in ports:
                    endpoints.append(netutils.createTcpEndpoint(ip, port))
        result.append([pdo, host, endpoints])
    return result
        