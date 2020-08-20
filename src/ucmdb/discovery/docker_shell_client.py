# coding=utf-8
import logger

class DockerClient(object):
    def __init__(self, shell, host=None):
        self.shell = shell
        if host:
            self.cmdPrefix = 'docker %s ' % host
        else:
            self.cmdPrefix = 'docker '

    def _checkLastCmd(self, lastCmdOutput, splitLines):
        if self.shell.getLastCmdReturnCode() != 0 or not lastCmdOutput:
            return None
        else:
            if splitLines:
                lastCmdOutputLines = lastCmdOutput.splitlines()
                return lastCmdOutputLines
            else:
                return lastCmdOutput

    def _execCmd(self, cmd, splitLines=True):
        # command timeout in ms
        TIMEOUT = 60000
        cmdOutput = self.shell.execCmd(self.cmdPrefix + cmd, timeout=TIMEOUT)
        cmdOutputLines = self._checkLastCmd(cmdOutput, splitLines)
        if cmdOutputLines:
            return cmdOutputLines
        else:
            logger.reportWarning('Failed in command: %s.' % cmd)
            return False

    def dockerVersion(self):
        cmdOutput = self.shell.execCmd('docker -v')
        if self._checkLastCmd(cmdOutput, True):
            version = cmdOutput.split(',')[0].split('Docker version ')[1].strip()
            return version
        else:
            logger.reportWarning('Failed in command: docker version.')
            return False

    def dockerInfo(self):
        cmdOutput = self._execCmd('info')
        return cmdOutput

    def dockerImages(self):
        cmdOutput = self._execCmd('images')
        return cmdOutput

    def dockerPs(self):
        cmdOutput = self._execCmd('ps')
        return cmdOutput

    def dockerPsFormated(self):
        cmdOutput = self._execCmd('ps --format "{{.ID}}{SEPARATOR}{{.Image}}{SEPARATOR}{{.Names}}{SEPARATOR}{{.Ports}}"')
        return cmdOutput

    def dockerTop(self, containerId):
        cmdOutput = self._execCmd('top %s' % containerId)
        return cmdOutput

    def dockerInspect(self, id):
        cmdOutput = self._execCmd('inspect %s' % id, splitLines=False)
        return cmdOutput

    def dockerInspectFormated(self, id):
        cmdOutput = self._execCmd('inspect -f {{.Id}} %s' % id, splitLines=False)
        return cmdOutput

    def df(self):
        cmdOutput = self.shell.execCmd('df')
        cmdOutputLines = self._checkLastCmd(cmdOutput, True)
        if cmdOutputLines:
            return cmdOutputLines
        else:
            logger.reportWarning('Failed in command: df.')
            return False

