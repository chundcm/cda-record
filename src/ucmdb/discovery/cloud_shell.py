# coding=utf-8

import logger
import sys
import re

AWS_URI = "http://169.254.169.254/"
AWS_META_URI = AWS_URI + "latest/meta-data/%s"


class AWSDiscoverer():
    def __init__(self, shell):
        self.shell = shell
        self._cmd = None

    def is_applicable(self):
        raise NotImplementedError()

    def discoverInstanceId(self):
        raise NotImplementedError()

    def discoverAmiId(self):
        raise NotImplementedError()

    def getMetaCmd(self):
        return self._cmd % AWS_URI

    def getInstanceIdCmd(self):
        return self._cmd % (AWS_META_URI % "instance-id")

    def getAmiIdCmd(self):
        return self._cmd % (AWS_META_URI % "ami-id")


class WindowsAWSDiscoverer(AWSDiscoverer):
    def __init__(self, shell):
        AWSDiscoverer.__init__(self, shell)
        self._cmd = "Invoke-WebRequest -UseBasicParsing -Uri %s"

    def is_applicable(self):
        command = self.getMetaCmd()
        return parsePowershellResult(self.shell.executeCmdlet(command))

    def discoverInstanceId(self):
        command = self.getInstanceIdCmd()
        return parse_ciid(self.shell.executeCmdlet(command))

    def discoverAmiId(self):
        command = self.getAmiIdCmd()
        return parsePowershellResult(self.shell.executeCmdlet(command))

IGNORED_VALUES = ['Connect failed']
class UnixAWSDiscoverer(AWSDiscoverer):
    def __init__(self, shell):
        AWSDiscoverer.__init__(self, shell)
        self._cmd = "curl -s %s"

    def is_applicable(self):
        command = self.getMetaCmd() + " -m 20"  # set curl timeout as 20 seconds
        return self.parseHTML(self.shell.execCmd(command)) and self.shell.getLastCmdReturnCode() == 0

    def discoverInstanceId(self):
        result = self.shell.execCmd(self.getInstanceIdCmd())
        if result and result not in IGNORED_VALUES:
            return result

    def discoverAmiId(self):
        result = self.shell.execCmd(self.getAmiIdCmd())
        if result and result not in IGNORED_VALUES:
            return result

    def parseHTML(self, output):
        # modified by Pierre
        # This does not take into account a name space could be provided as attribute on the html tag
        # if str(output).lower().find("<html>") >= 0 or str(output).lower().find("&lt;html&gt") >= 0:
        if str(output).lower().find("<html") >= 0 or str(output).lower().find("&lt;html") >= 0:
            return None
        else:
            return output


def parse_ciid(output):
    # This does take into account only if the output matches ID pattern
    logger.debug('Output of getting cloud_instance_id: %s' % output)
    regex = r"i-\d*\w*"
    matches = re.finditer(regex, output, re.MULTILINE)
    m_str = None
    if matches:
        for matchNum, match in enumerate(matches):
            m_str = match.group()
            logger.debug('Matched in output: %s' % m_str)
            break
    return m_str


def parsePowershellResult(output):
    endOfHeader = 0
    for line in output.splitlines():
        if (line.find('-----') != -1) and (endOfHeader == 0):
            endOfHeader = 1
            continue
        if endOfHeader == 1:
            return line.split()[0]
