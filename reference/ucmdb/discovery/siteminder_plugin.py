import re
import logger

def parseConfigFile(shell, filePath, fileName, fileContent, variableResolver):
    lines = fileContent.split('\n')
    for line in lines:
        if not line.startswith('#'):
            pattern = re.compile(r'policyserver\s*=\s*\"?(.*)\"?')
            matched = pattern.search(line)
            if matched:
                server_config = matched.group(1)
                server_list = server_config.split(',')
                logger.debug('Get SiteMinder policyservers as "%s".' % server_list)
                server = server_list[0]
                ports = server_list[1:len(server_list)]
                for port in ports:
                    variableResolver.add("policyserver_host", server)
                    variableResolver.add("policyserver_port", port)

