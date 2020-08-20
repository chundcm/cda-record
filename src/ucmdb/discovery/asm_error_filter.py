# __author__ = 'gengt'
import re
import asm_Disc_TCP
import errorcodes
import errormessages
import errorobject
import logger
from appilog.common.utils import Protocol


class ASMErrorFilter(object):
    rules = []

    @classmethod
    def filterErrors(cls, Framework, shell, client, errorList):
        logger.debug("enter the asm_error_filter")
        subclasses = BaseRule.__subclasses__()
        if not cls.rules:
            while len(subclasses) > 0:
                rule = subclasses.pop()
                if rule.__subclasses__():
                    subclasses.extend(rule.__subclasses__())
                elif rule not in cls.rules:
                    cls.rules.append(rule)
            cls.rules = sorted(cls.rules, key=lambda r: r.priority)
        for rule in cls.rules:
            rule(errorList, Framework, shell, client).handle()
        return errorList


class RulePriority(object):
    HIGH = 1
    NORMAL = 2
    LOW = 3


class BaseRule(object):
    priority = RulePriority.NORMAL

    def __init__(self, errorList, Framework, shell, client):
        self.errList = errorList
        self.shell = shell
        self.client = client
        self.ip = Framework.getDestinationAttribute('ip_address')
        self.port = Framework.getDestinationAttribute("PORT")
        self.Framework = Framework

    def isConnected(self):
        return self.shell is not None

    def isOSMatched(self):
        return True

    def isShellMatched(self):
        return self.isConnected() and self.isOSMatched()

    def isCmdResultsMatched(self):
        return True

    def isCurrentErrorMatched(self, error):
        raise NotImplementedError

    def getMatchedErrors(self):
        return filter(lambda e: self.isCurrentErrorMatched(e), self.errList)

    def getCmdResult(self, cmdLine):
        if self.shell.cmdCache.containsKey(cmdLine):
            return self.shell.cmdCache.get(cmdLine).output
        return self.shell.execCmd(cmdLine)

    def handle(self):
        matchedErrors = self.getMatchedErrors()
        if self.isShellMatched() and self.isCmdResultsMatched() and matchedErrors:
            logger.debug("Match ASMErrorFilter %s, create new errors instead of the following errors %s" % (
                type(self), matchedErrors))
            map(lambda e: self.errList.remove(e), matchedErrors)
            newErrors = self.createNewErrors()
            if isinstance(newErrors, errorobject.ErrorObject):
                self.errList.append(newErrors)
            else:
                self.errList.extend(newErrors)

    def createNewErrors(self):
        raise NotImplementedError


class CredentialError(BaseRule):
    def isShellMatched(self):
        return not self.isConnected()

    def isCurrentErrorMatched(self, error):
        return error.errCode in range(300, 309) or error.errCode == 934 or \
            error.errCode == 103 and len(error.params) >= 2 and \
            (re.match("No such attribute: protocol_port in protocol", error.params[1]) or
             re.match("Server not reachable by netbios", error.params[1]))

    def createNewErrors(self):
        # "We need your access to the host " + ipAddress
        msg = errormessages.makeErrorMessage(None, message=self.ip, pattern=errormessages.ERROR_CREDENTIAL)
        return errorobject.createError(errorcodes.ERROR_CREDENTIAL, [self.ip], msg)


class LoadBalanceError(BaseRule):
    def isShellMatched(self):
        return not self.isConnected()

    def isCurrentErrorMatched(self, error):
        return error.errCode == 114

    def createNewErrors(self):
        # unaccessible network path. virtual IP?
        msg = errormessages.makeErrorMessage(None, message=self.ip, pattern=errormessages.ERROR_MEET_LOAD_BALANCER)
        return errorobject.createError(errorcodes.MEET_LOAD_BALANCER, [self.ip], msg)


class LinuxNetstatSudoError(BaseRule):
    def isOSMatched(self):
        return not self.shell.isWinOs() and re.search(r'Linux', self.shell.getOsType())

    def isCurrentErrorMatched(self, error):
        return error.errCode == 510

    def isCmdResultsMatched(self):
        cmdResult = self.getCmdResult("/bin/netstat -nap")
        if cmdResult and 'No info could be read for "-p"' in cmdResult:
            output = asm_Disc_TCP.sanitizeIps(cmdResult)
            # tcp        0      0 0.0.0.0:80              0.0.0.0:*               LISTEN      -
            regExp = r'(tcp|tcp6|udp|udp6)\s+\d+\s+\d+[\sa-z:]+(\d+.\d+.\d+.\d+|:+):(%s)[\sa-z:]+(\d+.\d+.\d+.\d+|:+):(\d+|\*).+\s+\-\s+' % self.port
            if re.findall(regExp, output):
                return True
        return False

    def createNewErrors(self):
        # netstat not sudo permission
        username = self.client.getProperty(Protocol.PROTOCOL_ATTRIBUTE_USERNAME)
        msg = errormessages.makeErrorMessage(None, message="for user \"%s\" to use command \"netstat\" on the host %s" %
                                                           (username, self.ip), pattern=errormessages.ERROR_NOT_SUDO)
        return errorobject.createError(errorcodes.NOT_SUDO, [self.ip, 'netstat', username], msg)


class AbstractLsofError(BaseRule):
    def isCurrentErrorMatched(self, error):
        return error.errCode == 510

    def _isCmdResultsMatched(self, commands):
        raise NotImplementedError

    def isCmdResultsMatched(self):
        useLSOF = (self.Framework.getParameter('useLSOF') == "true")
        lsofPath = self.Framework.getParameter('lsofPath')
        if useLSOF and lsofPath:
            lsofPaths = lsofPath.split(',')
            commands = ('nice ' + path + ' -i -P -n' for path in lsofPaths)
            return self._isCmdResultsMatched(commands)
        return False

    def _createNoLsofError(self):
        # create a error no lsof installed
        msg = errormessages.makeErrorMessage(None, message=self.ip, pattern=errormessages.ERROR_NO_LSOF_INSTALL)
        return errorobject.createError(errorcodes.NO_LSOF_INSTALL, [self.ip], msg)

    def _createLsofNoSudoError(self):
        # lsof not sudo permission
        username = self.client.getProperty(Protocol.PROTOCOL_ATTRIBUTE_USERNAME)
        msg = errormessages.makeErrorMessage(None, message="for user \"%s\" to use command \"lsof\" on the host %s" %
                                                           (username, self.ip), pattern=errormessages.ERROR_NOT_SUDO)
        return errorobject.createError(errorcodes.NOT_SUDO, [self.ip, 'lsof', username], msg)


class SolarisNoLsofInstalledError(AbstractLsofError):
    def isOSMatched(self):
        return not self.shell.isWinOs() and re.search(r'sunos', self.shell.getOsType().lower())

    def _isCmdResultsMatched(self, commands):
        for command in commands:
            cmdResult = self.getCmdResult(command)
            if not re.search(r'No such file or directory:', cmdResult):
                return False
        return True

    def createNewErrors(self):
        # create a error no lsof installed
        return self._createNoLsofError()


class SolarisLsofLocalZoneError(AbstractLsofError):
    def isOSMatched(self):
        return not self.shell.isWinOs() and re.search(r'sunos', self.shell.getOsType().lower())

    def _isCmdResultsMatched(self, commands):
        for command in commands:
            cmdResult = self.getCmdResult(command)
            if re.search(r'can\'t read namelist from /dev/ksyms', cmdResult):
                return True
        return False

    def createNewErrors(self):
        # create a error the is local zone
        msg = errormessages.makeErrorMessage(None, message=self.ip, pattern=errormessages.ERROR_SOLARIS_LOCALZONE)
        return errorobject.createError(errorcodes.SOLARIS_LOCALZONE, [self.ip], msg)


class AIXNoLsofInstalledError(AbstractLsofError):
    def isOSMatched(self):
        return not self.shell.isWinOs() and re.search(r'aix', self.shell.getOsType().lower())

    def _isCmdResultsMatched(self, commands):
        for command in commands:
            cmdResult = self.getCmdResult(command)
            if not re.search(r'A file or directory in the path name does not exist', cmdResult):
                return False
        return True

    def createNewErrors(self):
        # create a error no lsof installed
        return self._createNoLsofError()


class AIXLsofSudoError(AbstractLsofError):
    def isOSMatched(self):
        return not self.shell.isWinOs() and re.search(r'aix', self.shell.getOsType().lower())

    def _isCmdResultsMatched(self, commands):
        for command in commands:
            cmdResult = self.getCmdResult(command)
            if re.search(r'Permission denied', cmdResult):
                return True
        return False

    def createNewErrors(self):
        # lsof not sudo permission
        return self._createLsofNoSudoError()
