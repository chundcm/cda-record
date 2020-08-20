import scp
import logger
import inspect
import asm_signature_parser
from java.lang import Exception as JException

PLUGIN_FILE_NAME = "asm_deep_discovery_plugins.xml"


def do_deep_discovery(Framework, ip, applicationResults, OSHVResult, client, shell, hostOsh):
    scp_id = Framework.getDestinationAttribute("SCP_ID")
    pluginFile = Framework.getConfigFile(PLUGIN_FILE_NAME)
    if not pluginFile or not pluginFile.getText():
        logger.debug("Cannot find plugin config file, will skip the deep discovery:", PLUGIN_FILE_NAME)
        return
    pluginFile = pluginFile.getText()
    deep_discovery_plugin_config = DeepDiscoveryPluginConfig(pluginFile)

    for applicationResult in applicationResults:
        # todo:add plugin logic here

        pluginModule = deep_discovery_plugin_config.getPluginModule(applicationResult)
        plugin = None
        try:
            plugin = __import__(pluginModule)
        except (Exception, JException), e:
            logger.debug('Fail to load plugin: "%s"' % pluginModule, e)

        if plugin:
            func = getattr(plugin, 'discover', None)
            if inspect.isfunction(func) and func.func_code.co_argcount == 6:
                try:
                    func(Framework, shell, client, applicationResult, OSHVResult, hostOsh)
                except Exception, e:
                    logger.debug('Error occurred when running plugin "%s":' % pluginModule, e)
            else:
                logger.debug("The plugin should have a function 'discover(Framework, shell, client, applicationResult)'")

        for resource in applicationResult.applicationresources:
            OSHVResult.add(resource)
            OSHVResult.addAll(scp.createOwnerShip(scp_id, resource))


class DeepDiscoveryPluginConfig:
    def __init__(self, content):
        self.plugins = self.parseContent(content)

    def parseContent(self, content):
        result = []
        plugins = asm_signature_parser.parseString(content)
        for plugin in plugins.children:
            result.append(
                DeepDiscoveryPluginConfigItem(plugin.id, plugin.name, plugin.productName, plugin.cit, plugin.module))
        return result

    def getPluginModule(self, applicationResult):
        application = applicationResult.application
        cit = application.getOsh().getObjectClass()
        productName = application.getDiscoveredProductName() or application.getOsh().getAttributeValue('data_name')
        name = application.getName()
        logger.debug("Do deep discovery for application: name = '%s', productName = '%s', cit = '%s'"
                     % (name, productName, cit))
        for plugin in self.plugins:
            if plugin.accept(applicationResult):
                logger.debug("found plugin:", plugin.id)
                return plugin.module


class DeepDiscoveryPluginConfigItem:
    def __init__(self, id, name, productName, cit, module):
        self.id = id
        self.name = name
        self.productName = productName
        self.cit = cit
        self.module = module

    def accept(self, runningApplication):
        application = runningApplication.application
        cit = application.getOsh().getObjectClass()
        productName = application.getDiscoveredProductName() or application.getOsh().getAttributeValue('data_name')
        name = application.getName()
        if (self.productName and productName == self.productName) or (self.name and name == self.name) or (
                self.cit and cit == self.cit):
            return True
        return False
