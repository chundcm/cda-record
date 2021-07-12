# coding=utf-8
import logger
import ip_addr
import modeling
import file_system
import netutils
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector
from java.util import Properties
import shellutils
from com.hp.ucmdb.discovery.library.clients.agents import BaseAgent
from com.hp.ucmdb.discovery.library.credentials.dictionary import ProtocolDictionaryManager
from java.net import ConnectException
from java.lang import Exception as JException

__file_name = "nnmi_query_mac_tempfile"

class NNMiContext:
    def __init__(self):
        self.mac_interface_list = {}
        self.mac_address=[]
        # switchname : created switch osh
        self.created_switch={}
        # for exmaple: switchname : [ifname01,ifname02]
        self.created_interface={}
        # add for layer2 connection
        self.vector=[]

    def filterMac(self,x,_connectingInterface):
        if 'UNABLE_TO_LOCATE_ENTRY_IN_FDB' not in x and 'SUCCESS' in x:
            _connectingInterface.append(x)

#get client and IP
def generateClientandIP(Framework):

    codePage = Framework.getCodePage()
    targetIpAddress = Framework.getTriggerCIData('ip_address')
    if not targetIpAddress or not ip_addr.isValidIpAddress(targetIpAddress):
        raise Exception, "Trigger IP address is empty or invalid"

    credentialsId = Framework.getParameter('credentialsId')
    # protocol = ProtocolDictionaryManager.getProtocolById(credentialsId)
    client = None
    if not credentialsId:
        raise Exception, "Credential is empty or invalid"

    Props = Properties()
    Props.setProperty(BaseAgent.ENCODING, codePage)
    try:
        client = Framework.createClient(credentialsId, Props)
    except ConnectException, ce:
        msg = 'Connection failed: %s' % ce.getMessage()
        logger.debug(msg)
        Framework.reportError(msg)
    except (Exception, JException), e:
        logger.warnException(str(e))
        Framework.reportError(e.getMessage())
    return client,targetIpAddress

class NNMiShell:
    def __init__(self,shell,command_path,filePath,Framework):
        self.shell = shell
        self.command_path = command_path
        self.filePath = filePath
        self.index = 0
        self.confirm_class_path = None
        self.Framework = Framework

    def __report_error(self,result):
        if self.shell.getLastCmdReturnCode() == 0 and result:
            return result
        else:
            logger.error(str(result))
            self.Framework.reportError(str(result))
            return None

    def executeQuery(self,command_path,filePath):
        _command = 'nnmfindattachedswport.ovpl'

        logger.debug('run command times:' + str(self.index))
        if self.confirm_class_path:
            try:
                result = self.shell.execCmd(self.confirm_class_path + _command + " -i " + str(filePath))
                result = self.__report_error(result)
                return result
            except Exception, e:
                logger.error(str(e))
                logger.debug('run command ' + _command + ' error')
        elif command_path and filePath:
            if len(command_path) == 1:
                try:
                    result = self.shell.execCmd(command_path[0] + _command + " -i " + str(filePath))
                    result = self.__report_error(result)
                    self.confirm_class_path = command_path[0]
                    return result
                except Exception, e:
                    logger.error(str(e))
                    logger.debug('run command ' + _command + ' error')

            elif self.index == len(command_path) -1 :
                try:
                    result = self.shell.execCmd(command_path[self.index] + _command + " -i " + str(filePath))
                    result = self.__report_error(result)
                    self.confirm_class_path = command_path[0]
                    return result
                except Exception, e:
                    logger.error(str(e))
                    logger.debug('run command ' + _command + ' error')

            else:
                try:
                    result = self.shell.execCmd(command_path[self.index] + _command + " -i " + str(filePath))
                    if self.shell.getLastCmdReturnCode() == 0 and result:
                        logger.debug(str(self.confirm_class_path))
                        logger.debug(str(command_path[self.index]))
                        self.confirm_class_path = command_path[self.index]
                        return result
                    else:
                        self.index = self.index + 1
                        result = self.executeQuery(command_path, filePath)
                    return result
                except Exception, e:
                    self.index = self.index + 1
                    result = self.executeQuery(command_path,filePath)
                    logger.error(str(e))
                    logger.debug('run command ' + _command + ' error')
                    return result


#get Process
def doprocess(shell, data_list,context,Framework, filePath=None,command_path=None,):
    osType = "windows" if shell.isWinOs() else "linux"
    if not filePath:
        fs = file_system.createFileSystem(shell)
        _file_path = fs.getTempFolder()
        if osType == "linux" and str(shell.execCmd('echo '+_file_path)) !='/':
            _file_path = "/tmp/"

        filePath = _file_path+__file_name
    logger.debug('temporary file path:'+str(filePath))

    if not command_path:
        if osType == 'linux':
            command_path = ('/opt/OV/bin/',)
        elif osType == 'windows':
            command_path = ('', 'C:\\Program Files (x86)\\HP\\HP BTO Software\\bin\\',)
    logger.debug('command_path:' + str(command_path))

    connectingInterface = []
    logger.debug('command_path:'+str(command_path))
    logger.debug('filePath:' + str(filePath))
    nnmiShell = NNMiShell(shell, command_path, filePath,Framework)

    for index in range(0, len(data_list), 100):
        gererateTempFile(shell, osType, data_list[index:index+100], filePath)
        #TODO run command to getresult
        logger.debug('starting run command')
        nnmiShell.index = 0
        result = nnmiShell.executeQuery(nnmiShell.command_path,nnmiShell.filePath)
        if result:
            each_line = result.split('\n')
            _connectingInterface = []
            map(lambda x: context.filterMac(x,_connectingInterface) , each_line)
            logger.debug('validated connected interface:' + str(_connectingInterface))

            for item in _connectingInterface:
                sourcebulk = item.split(',')
                mac_address = str(sourcebulk[0])
                switch_hostname = str(sourcebulk[1])
                interface_name = str(sourcebulk[2])
                l2id = ''
                if not context.created_switch.has_key(switch_hostname):
                    # create switch osh
                    switchosh = ObjectStateHolder('switch')
                    switchosh.setAttribute("name",switch_hostname.split('.')[0])
                    context.created_switch[switch_hostname] = switchosh
                    # create interface osh
                    interfaceosh = ObjectStateHolder('interface')
                    interfaceosh.setAttribute('interface_name', interface_name)
                    interfaceosh.setContainer(switchosh)
                    context.created_interface.get(switch_hostname, []).append(interfaceosh)
                    # interface use as query and link with result switch
                    layer2Osh_source = context.mac_interface_list[mac_address]
                    layer2Osh = ObjectStateHolder('layer2_connection')

                    l2id = "%s-%s" % (l2id, interface_name)  ## Create layer2_connection object's ID
                    layer2Osh.setAttribute('layer2_connection_id', str(hash(l2id)))
                    member1 = modeling.createLinkOSH('member', layer2Osh, layer2Osh_source)
                    member2 = modeling.createLinkOSH('member', layer2Osh,interfaceosh)
                    context.vector.append(layer2Osh)
                    context.vector.append(member1)
                    context.vector.append(member2)
        else:
            break

#get Shell by clinet
def generateShell(client):
    clientType = client.getClientType()
    shellFactory = shellutils.ShellFactory()
    shell = shellFactory.createShell(client, clientType)
    return shell

def gererateTempFile(shell,osType,data,filePath):

    if isinstance(data,list) and len(data)>100:
        raise Exception, "parse data error"

    #windows OS
    if osType == "windows":
        shell.execCmd('echo %s > %s' % (data[0], filePath))
        _str = "echo %s >> %s"
        map((lambda x : shell.execCmd(_str % (x,filePath))), data[1:])
        logger.debug("generate file successfully")

    elif osType == "linux":
        shell.execCmd('echo %s > %s' % (data[0], filePath))
        _str = "echo -e %s >> %s"

        _data = reduce((lambda x ,y : str(x)+"\\n"+str(y)), data[1:])
        command_line_cfile = _str % ("\""+_data+"\"",filePath)
        logger.debug("command:"+str(command_line_cfile))
        shell.execCmd(command_line_cfile)
        logger.debug("generate file successfully")

def report(resultVector,context):
    map(lambda x: resultVector.add(x),context.created_switch.values())
    map(lambda x: resultVector.add(x),context.created_interface.values())
    map(lambda x: resultVector.add(x),context.vector)


def DiscoveryMain(Framework):
    resultVector = ObjectStateHolderVector()

    command_path = (Framework.getParameter('NNMiBinPath'),) if Framework.getParameter('NNMiBinPath') else None

    client,targetIpAddress = generateClientandIP(Framework)
    if client:
        shell = generateShell(client)

        interfaces_global_id = Framework.getTriggerCIDataAsList('interface_cmdbid')
        interfaces_mac_address = Framework.getTriggerCIDataAsList('mac_address')

        context = NNMiContext()
        context.mac_address = interfaces_mac_address
        for x in range(len(interfaces_global_id)):
            if interfaces_global_id[x] and interfaces_mac_address[x] and interfaces_mac_address[x] != "NA":
                if netutils.isValidMac(interfaces_mac_address[x]):
                    context.mac_interface_list[str(interfaces_mac_address[x]).strip()] = modeling.createOshByCmdbId("interface",interfaces_global_id[x])

        logger.debug('context.mac_address:'+str(context.mac_address))
        doprocess(shell,context.mac_address,context,Framework,filePath=None,command_path=command_path)

        report(resultVector,context)

    return resultVector