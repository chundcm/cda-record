# __author__ = 'gengt'
import asm_file_system
import asm_signature_processor
import logger
import re
from apache_config_item import LoadModuleProperty, Block, Property, IncludeProperty
from apache_config_item_utils import getPathOperation


apache_plugins = {}    # all the apache config plugins


def Plugin(c):
    apache_plugins[c.__name__] = c()
    return c


class BasePlugin(object):
    def suitable(self, items, content):
        """If this plugin's effective condition is matched"""
        return True

    def findVariables(self, shell, items, content, variableResolver):
        """find useful information in config, return a variable dictionary"""
        raise NotImplementedError


# @Plugin
# class SCPPlugin(BasePlugin):
#     def findVariables(self, items, content):
#         protocols, hosts, ports, contexts = [], [], [], []
#         for protocol, host, _, port in re.findall("(https?)://([^/^:]+)(:(\d+))?/", content):
#             port = port or None
#             map(lambda l, v: l.append(v), (protocols, hosts, ports, contexts), (protocol, host, port, '/'))
#         if protocols and hosts and ports and contexts:
#             return {"protocol": protocols, "host": hosts, "port": ports, "context": contexts}
#         else:
#             return {}


@Plugin
class WebLogicPlugin(BasePlugin):
    def suitable(self, items, content):
        return len(items.filter(cls=LoadModuleProperty, module_name='weblogic_module'))

    def findVariables(self, shell, items, content, variableResolver):
        weblogicProxyDict = {}
        self.parseWeblogicIfModuleBlocks(items, weblogicProxyDict, variableResolver)
        self.parseWeblogicLocationBlocks(items, weblogicProxyDict, variableResolver)
        return weblogicProxyDict

    def parseWeblogicIfModuleBlocks(self, items, weblogicProxyDict, variableResolver):
        """
        Parse IfModule block related to weblogic. Such as:
        1)
        <IfModule mod_weblogic.c>
         WebLogicHost my-weblogic.server.com
         WebLogicPort 7001
         MatchExpression *.jsp
         DebugConfigInfo ON
        </IfModule>
        2)
        <IfModule mod_weblogic.c>
         WebLogicCluster w1s1.com:7001,w1s2.com:7001,w1s3.com:7001
         MatchExpression *.jsp
         MatchExpression *.xyz
        </IfModule>
        3)
        <IfModule mod_weblogic.c>
         MatchExpression *.jsp WebLogicHost=myHost|WebLogicPort=7001|Debug=ON
         MatchExpression *.html WebLogicCluster=myHost1:7282,myHost2:7283|ErrorPage=http://www.xyz.com/error.html
        </IfModule>
        4)
        <IfModule weblogic_module>
         WebLogicHost <WEBLOGIC_HOST>
         WebLogicPort <WEBLOGIC_PORT>
         MatchExpression *.jsp
        </IfModule>
        """
        logger.debug('try to find weblogic IfModule block.')
        blocks = items.filter(cls=Block, name='IfModule', value__contains=('mod_weblogic.c', 'weblogic_module'))
        for block in blocks:
            WebLogicHost = block.configs.filter(cls=Property, name='WebLogicHost').first_value('value', flat=True)
            WebLogicPort = block.configs.filter(cls=Property, name='WebLogicPort').first_value('value', flat=True)
            WebLogicCluster = block.configs.filter(cls=Property, name='WebLogicCluster').first_value('value', flat=True)
            MatchExpression = block.configs.filter(cls=Property, name='MatchExpression').values('value', flat=True)
            if (WebLogicHost and WebLogicPort) or WebLogicCluster:
                self.generateWeblogicUrl(weblogicProxyDict, WebLogicHost, WebLogicPort, WebLogicCluster)
            else:
                for expression in MatchExpression:
                    # expression such as:
                    #  MatchExpression *.jsp WebLogicHost=myHost|WebLogicPort=7001|Debug=ON or
                    #  MatchExpression *.html WebLogicCluster=myHost1:7282,myHost2:7283
                    expression_split = re.split(r"\s", expression, 2)
                    if len(expression_split) == 2:
                        params_str = expression_split[1]
                        params = {}
                        for param_str in params_str.split('|'):
                            param_str = param_str.strip()
                            param_name, param_value = param_str.split('=', 2)
                            params[param_name] = param_value
                            params.get('WebLogicHost')
                        self.generateWeblogicUrl(weblogicProxyDict, params.get('WebLogicHost'),
                                                 params.get('WebLogicPort'), params.get('WebLogicCluster'))

    def parseWeblogicLocationBlocks(self, items, weblogicProxyDict, variableResolver):
        """
        Parse Location block related to weblogic. Such as:
        1)
        <Location /weblogic>
        WLSRequest On
        PathTrim /weblogic
        </Location>
        2)
        <Location /weblogic>
        WLSRequest On
        WebLogicHost myweblogic.server.com
        WebLogicPort 7001
        </Location>
        3)
        <Location /weblogic>
        WLSRequest On
        WebLogicCluster w1s1.com:7001,w1s2.com:7001,w1s3.com:7001
        </Location>
        4)
        <Location /weblogic>
         SetHandler weblogic-handler
         PathTrim /weblogic
        </Location>
        5)
        <LocationMatch /weblogic/.*>
         WLSRequest On
        </LocationMatch>
        """
        scp_context = variableResolver.get('scp.context')[0] if variableResolver.get('scp.context') else '/'
        logger.debug('try to find weblogic Location block.')
        blocks = items.filter(cls=Block, name__contains=('Location', 'LocationMatch'))
        for block in blocks:
            # narrow Location configs by scp.context
            if re.match(r'^%s$' % block.value, scp_context):
                if Property('WLSRequest', 'On') in block.configs or \
                        Property('SetHandler', 'weblogic-handler') in block.configs:
                    PathTrim = block.configs.filter(cls=Property, name='PathTrim').first_value('value', flat=True)
                    WebLogicHost = block.configs.filter(cls=Property, name='WebLogicHost'
                                                        ).first_value('value', flat=True)
                    WebLogicPort = block.configs.filter(cls=Property, name='WebLogicPort'
                                                        ).first_value('value', flat=True)
                    WebLogicCluster = block.configs.filter(cls=Property, name='WebLogicCluster'
                                                           ).first_value('value', flat=True)
                    if not PathTrim:
                        PathTrim = block.value
                    self.generateWeblogicUrl(weblogicProxyDict, WebLogicHost, WebLogicPort,
                                             WebLogicCluster, PathTrim)
            else:
                logger.warn('Skip the location config %s. It has been narrowed by scp context %s' % (
                    block.value, scp_context))

    def generateWeblogicUrl(self, weblogicProxyDict, WebLogicHost, WebLogicPort, WebLogicCluster, root='/'):
        """
        Get proxied weblogic host, port & context root; insert into weblogicProxyDict.
        """
        WEBLOGIC_PROTOCOL = 'http'
        weblogic_urls = []
        if WebLogicHost and WebLogicPort:
            if (WEBLOGIC_PROTOCOL, WebLogicHost, WebLogicPort, root) not in weblogic_urls:
                weblogic_urls.append((WEBLOGIC_PROTOCOL, WebLogicHost, WebLogicPort, root))
        elif WebLogicCluster:
            clusters = WebLogicCluster.split(',')
            for cluster in clusters:
                cluster = cluster.strip()
                matched = re.search(r"([^/^:]+):(\d+)(/.*)?", cluster)
                if matched:
                    host, port, context_root = matched.groups()
                    if not context_root or context_root == '/':
                        context_root = root
                    if (WEBLOGIC_PROTOCOL, host, port, context_root) not in weblogic_urls:
                        weblogic_urls.append((WEBLOGIC_PROTOCOL, host, port, context_root))
        elif weblogicProxyDict.get('weblogic_host'):
            if (WEBLOGIC_PROTOCOL, weblogicProxyDict.get('weblogic_host')[0],
                    weblogicProxyDict.get('weblogic_port')[0], root) not in weblogic_urls:
                weblogic_urls.append((WEBLOGIC_PROTOCOL, weblogicProxyDict.get('weblogic_host')[0],
                                      weblogicProxyDict.get('weblogic_port')[0], root))
        for protocol, host, port, context in weblogic_urls:
            logger.debug('found weblogic scp: %s://%s:%s%s' % (protocol, host, port, context))
            weblogicProxyDict.setdefault('weblogic_protocol', []).append(protocol)
            weblogicProxyDict.setdefault('weblogic_host', []).append(host)
            weblogicProxyDict.setdefault('weblogic_port', []).append(port)
            weblogicProxyDict.setdefault('weblogic_context_root', []).append(context)


@Plugin
class OAMWebgatePlugin(BasePlugin):
    def suitable(self, items, content):
        return len(items.filter(cls=LoadModuleProperty, module_name='obWebgateModule'))

    def findVariables(self, shell, items, content, variableResolver):
        """parse oam webgate include config and return oam webgate root"""
        pattern = re.compile(r'(.*)/webgate\.conf')
        for item in items.filter(cls=IncludeProperty):
            matched = pattern.search(item.filePath)
            if matched:
                logger.debug('found oam webgate root: %s' % matched.group(1))
                return {'webgate_root': (matched.group(1), )}
        return {}

@Plugin
class SiteMindereWebAgentPlugin(BasePlugin):
    def suitable(self, items, content):
        return len(items.filter(cls=Property, name='SmInitFile'))

    def findVariables(self, shell, items, content, variableResolver):
        """parse apache site mindere web plugin and return WebAgent.conf path"""
        webAgentFile = items.filter(cls=Property, name='SmInitFile').first_value('value', flat=True)
        if webAgentFile:
            if webAgentFile[0] in ("'", '"'):
                webAgentFile = webAgentFile[1:]
            if webAgentFile[-1] in ("'", '"'):
                webAgentFile = webAgentFile[:-1]
            webAgentPath, webAgentName = getPathOperation(shell).split(webAgentFile)
            logger.debug('found site mindere web plugin config: name = "%s", path = "%s"' % (webAgentName, webAgentPath))
            return {'webagent_path': (webAgentPath, ), 'webagent_name': (webAgentName, )}
        return {}


class BaseReverseProxyPlugin(BasePlugin):
    def _narrowCheck(self, item, variableResolver):
        return self._contextRootNarrowCheck(item, variableResolver) and \
               self._portNarrowCheck(item,variableResolver) and \
               self._serverNameNarrowCheck(item,variableResolver)

    def _contextRootNarrowCheck(self, item, variableResolver):
        # context root check
        virtual_context = item.value1
        scp_context = variableResolver.get('scp.context')[0] if variableResolver.get('scp.context') else '/'
        if not re.match(virtual_context, scp_context):
            logger.warn('Skip the config "%s". It has been narrowed by scp context %s' % (item, scp_context))
            return False
        return True

    def _portNarrowCheck(self, item, variableResolver):
        # port check
        virtual_host_blocks = item.parents.filter(cls=Block, name='VirtualHost')
        scp_port = variableResolver.get('scp.port')[0] if variableResolver.get('scp.port') else '80'
        for virtual_host_block in virtual_host_blocks:
            virtual_host_value = virtual_host_block.value
            virtual_host_values = virtual_host_value.rsplit(':', 1)
            if len(virtual_host_values) == 2:
                virtual_host_port = virtual_host_values[1]
                if virtual_host_port != '*' and virtual_host_port != scp_port:
                    logger.warn('Skip the config "%s". It has been narrowed by scp port %s' % (item, scp_port))
                    return False
        return True

    def _serverNameNarrowCheck(self, item, variableResolver):
        virtual_host_blocks = item.parents.filter(cls=Block, name='VirtualHost')
        bizurl = variableResolver.get('biz.url')[0] if variableResolver.get('biz.url') else ""
        if bizurl:
            bizurl = bizurl[1:] if bizurl[0] == '[' else bizurl
            bizurl = bizurl[:-1] if bizurl[-1] == ']' else bizurl
            url_pattern = re.compile(r"(https?)://([^/^:]+)(:(\d+))?(/.*)?")
            matched = url_pattern.findall(bizurl)
            if matched and len(matched[0]) == 5:
                serverName = matched[0][1]
                if serverName:
                    checkedServerNames = item.block.configs.filter(cls=Property, name='ServerName')
                    if checkedServerNames:
                        checkedServerName = checkedServerNames[0].value
                        return serverName == checkedServerName
        return True


@Plugin
class ModProxyPlugin(BaseReverseProxyPlugin):
    def suitable(self, items, content):
        return len(items.filter(cls=Property, name='ProxyPass'))

    def findVariables(self, shell, items, content, variableResolver):
        """Parse ProxyPass config and return reverse proxy protocol, host, port & context root"""
        protocols, hosts, ports, contexts = [], [], [], []
        proxyPassItems = items.filter(cls=Property, name='ProxyPass')
        url_pattern = re.compile(r"(https?)://([^/^:]+)(:(\d+))?(/.*)?")
        for item in proxyPassItems:
            if self._narrowCheck(item, variableResolver):
                source = item.value2
                matched = url_pattern.match(source)
                if matched:
                    protocol, host, _, port, context = matched.groups()
                    protocol = protocol or 'http'
                    port = port or ''
                    context = context or '/'
                    if context == '/':
                        context = variableResolver.get('scp.context')[0] if variableResolver.get('scp.context') else '/'
                    logger.debug('found ProxyPass reverse proxy scp: %s://%s:%s%s' % (protocol, host, port, context))
                    map(lambda l, v: l.append(v), (protocols, hosts, ports, contexts), (protocol, host, port, context))
        if protocols and hosts and ports and contexts:
            return {"protocol": protocols, "host": hosts, "port": ports, "context": contexts}
        else:
            return {}


@Plugin
class WAS22Plugin(BasePlugin):
    def suitable(self, items, content):
        return len(items.filter(cls=LoadModuleProperty, module_name='was_ap22_module'))

    def findVariables(self, shell, items, content, variableResolver):
        """parse apache websphere plugin and return plugin-cfg xml path"""
        wascfgfile = items.filter(cls=Property, name='WebSpherePluginConfig').first_value('value', flat=True)
        if wascfgfile:
            if wascfgfile[0] in ("'", '"'):
                wascfgfile = wascfgfile[1:]
            if wascfgfile[-1] in ("'", '"'):
                wascfgfile = wascfgfile[:-1]
            wascfgpath, wascfgname = getPathOperation(shell).split(wascfgfile)
            logger.debug('found websphere plugin: name = "%s", path = "%s"' % (wascfgname, wascfgpath))
            return {'pluginPath': (wascfgpath, ), 'pluginName': (wascfgname, )}
        return {}


@Plugin
class ModRewritePlugin(BaseReverseProxyPlugin):
    def suitable(self, items, content):
        return len(items.filter(cls=LoadModuleProperty, module_name='rewrite_module'))

    def findVariables(self, shell, items, content, variableResolver):
        """Parse RewriteRule config and return redirect protocol, host, port & context root"""
        protocols, hosts, ports, contexts = [], [], [], []
        rewriteRuleItems = items.filter(cls=Property, name='RewriteRule')
        url_pattern = re.compile(r"(https?)://([^/^:]+)(:(\d+))?(/.*)?")
        for item in rewriteRuleItems:
            virtual_context = item.value1
            scp_context = variableResolver.get('scp.context')[0] if variableResolver.get('scp.context') else '/'
            if self._narrowCheck(item, variableResolver):
                substitution = item.value2
                substitution = substitution.replace('$', '\\')
                substitution = re.sub(virtual_context, substitution, scp_context)
                matched = url_pattern.match(substitution)
                if matched:
                    protocol, host, _, port, context = matched.groups()
                    protocol = protocol or 'http'
                    port = port or ''
                    context = context or '/'
                    logger.debug('found RewriteRule redirect scp: %s://%s:%s%s' % (protocol, host, port, context))
                    map(lambda l, v: l.append(v), (protocols, hosts, ports, contexts), (protocol, host, port, context))
        if protocols and hosts and ports and contexts:
            return {"protocol": protocols, "host": hosts, "port": ports, "context": contexts}
        else:
            return {}


@Plugin
class ModJkPlugin(BasePlugin):
    def suitable(self, items, content):
        return len(items.filter(cls=LoadModuleProperty, module_name='jk_module'))

    def findVariables(self, shell, items, content, variableResolver):
        protocols, hosts, ports, contexts = [], [], [], []
        if items:
            # tomcat worker property file
            path_operation = getPathOperation(shell)
            jkWorkersFile = items.getValueOrNone(cls=Property, name='JkWorkersFile') or \
                            path_operation.join('conf', 'workers.properties')
            logger.debug('found tomcat worker property file: %s' % jkWorkersFile)
            # use server_root for relative path
            path_prefix = items[0].globalAttributes.get('server_root', items[0].globalAttributes.get('filePath', ''))
            if not path_operation.isabs(jkWorkersFile):
                jkWorkersFile = path_operation.join(path_prefix, jkWorkersFile)
                logger.debug('load property file using absolute path: %s' % jkWorkersFile)

            # narrow load balance controller
            lb_controllers = []
            scp_context = variableResolver.get('scp.context')[0] if variableResolver.get('scp.context') else '/'
            jkMount = items.filter(cls=Property, name='JkMount').values('value1', 'value2')
            for context_root, lb_controller in jkMount:
                context_root = context_root.replace("*", ".*")
                if re.match(context_root, scp_context):
                    lb_controllers.append(lb_controller)
                    logger.debug('found tomcat worker load balance controller: %s' % lb_controller)
                else:
                    logger.debug('skip tomcat worker load balance controller "%s" due to narrowed by scp context'
                                 % lb_controller)

            if not lb_controllers:
                logger.debug('tomcat worker load balance controller does not found')
                return {}

            # read & parse property file
            processor = self.__read_worker_property(shell, jkWorkersFile, variableResolver)
            if not processor:
                return {}

            for lb_controller in lb_controllers:
                balanced_workers = processor.getProperty("worker.%s.balanced_workers" % lb_controller) or \
                    processor.getProperty("worker.%s.balance_workers" % lb_controller)
                if balanced_workers:
                    worker_servers = balanced_workers.split(",")
                    for worker_server in worker_servers:
                        worker_server = worker_server.strip()
                        protocol = processor.getProperty("worker.%s.type" % worker_server)
                        host = processor.getProperty("worker.%s.host" % worker_server)
                        port = processor.getProperty("worker.%s.port" % worker_server)
                        if ':' in host:
                            host, port = host.split(":", 2)
                        if protocol and host:
                            map(lambda l, v: l.append(v), (protocols, hosts, ports, contexts),
                                (protocol, host, port, scp_context))

        if protocols and hosts and ports and contexts:
            return {"protocol": protocols, "host": hosts, "port": ports, "context": contexts}
        else:
            return {}

    @staticmethod
    def __read_worker_property(shell, jkWorkersFile, variableResolver):
        path_operation = getPathOperation(shell)
        jkWorkersFilePath, jkWorkersFileName = path_operation.split(jkWorkersFile)
        files = asm_file_system.getConfigFilesFromPath(shell, jkWorkersFilePath, jkWorkersFileName, False)
        if files:
            processor = asm_signature_processor.PropertyFileProcessor(variableResolver)
            try:
                processor._prepareFile(files[0].getContent())
                return processor
            except Exception, e:
                logger.error('Error occurred when parsing property file %s:' % jkWorkersFile, e)
