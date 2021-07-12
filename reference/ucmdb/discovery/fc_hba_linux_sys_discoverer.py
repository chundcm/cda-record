# coding=utf-8
'''
Created on Dec 26, 2013

@author: ekondrashev
'''
import re
from itertools import ifilter
from collections import defaultdict
from functools import partial
from operator import attrgetter

import vendors
import fptools
import logger
import wwn
from fptools import safeFunc as Sfn, identity, comp, methodcaller
from file_system import UnixPath

import service_loader
from os_platform_discoverer import enum as os_platforms
import fc_hba_discoverer
from ls import find as find_ls_impl
import command
import fc_hba_model
from fc_hba_model import _parse_port_speed

def _parse_lspci(lines):
    '''Parses `lspci -v -m -n -s <device_id>` command output, returning
    key/value dictionary.

    @param lines: output of lspci splitted by lines
    @type lines: list[basestring]
    @return: dictionary of name/value pairs
    @rtype: dict[str,str]
    '''
    sep_pattern = re.compile('\s*:\s*')

    lines = ifilter(identity, lines)

    result = {}
    for line in lines:
        key, value = sep_pattern.split(line.strip(), maxsplit=1)
        key = key.strip().lower()
        value = value.strip()
        result[key] = value

    return result


class cat(command.UnixBaseCmd):
    '''
    Command class for `cat` executable extending
    command.BaseCmd.DEFAULT_HANDLERS static attribute with additional
    handlers specific to `cat` command.
    '''

    DEFAULT_HANDLERS = (command.UnixBaseCmd.DEFAULT_HANDLERS +
                        (command.cmdlet.raiseOnNonZeroReturnCode,
                         command.cmdlet.raiseWhenOutputIsNone,
                         command.cmdlet.stripOutput,
                         ))

    def __init__(self, path, handler=None):
        '''
        @param path: file path to get content for
        @type path: basestring or file_system.Path
        @param handler: handler to use for current command
        @type handler: callable[command.Result] -> ?. The default handler returns `cat` command output stripped
        '''
        command.UnixBaseCmd.__init__(self, 'cat "%s"' % path, handler=handler)


class ls(command.UnixBaseCmd):
    '''
    Command class for `ls` executable extending
    command.BaseCmd.DEFAULT_HANDLERS static attribute with additional
    handlers specific to `ls` command.
    '''

    DEFAULT_HANDLERS = (command.UnixBaseCmd.DEFAULT_HANDLERS +
                        (command.cmdlet.raiseOnNonZeroReturnCode,
                         command.cmdlet.raiseWhenOutputIsNone,
                         command.cmdlet.stripOutput,
                         ))

    def __init__(self, path, handler=None):
        '''
        @param path: the path to get file name and folder name
        @type path: basestring or file_system.Path
        @param handler: handler to use for current command
        @type handler: callable[command.Result] -> ?. The default handler returns `cat` command output stripped
        '''
        command.UnixBaseCmd.__init__(self, 'ls "%s"' % path, handler=handler)


class readlink(command.UnixBaseCmd):
    '''
    Command class for `readlink`(print value of a symbolic link or canonical
     file name) executable extending
    command.BaseCmd.DEFAULT_HANDLERS static attribute with additional
    handlers specific to `readlink` command.
    '''
    DEFAULT_HANDLERS = (command.UnixBaseCmd.DEFAULT_HANDLERS +
                        (command.cmdlet.raiseOnNonZeroReturnCode,
                         command.cmdlet.raiseWhenOutputIsNone,
                         command.cmdlet.stripOutput,
                         UnixPath
                         ))

    def __init__(self, path, handler=None):
        '''
        @param path: file path to resolve
        @type path: basestring or file_system.Path
        @param handler: handler to use for current command
        @type handler: callable[command.Result] -> ?. The default handler returns UnixPath instance
        '''
        command.UnixBaseCmd.__init__(self, 'readlink "%s"' % path, handler=handler)

    @property
    def e(self):
        '''Creates new command appending '-e' option
        responsible for "canonicalize by following every symlink in every
        component of the given name recursively, all components must exist"

        @return: new command instance with '-e' option
        @rtype: command.UnixBaseCmd
        '''
        return command.UnixBaseCmd(self.cmdline + " -e", handler=self.handler)


@service_loader.service_provider(fc_hba_discoverer.Discoverer)
class Discoverer(fc_hba_discoverer.Discoverer):
    OS_PLATFORM = os_platforms.LINUX

    def __init__(self):
        self.get_driver_version = self._scsi_host_attribute_getter(('driver_version','lpfc_drvr_version'))
        self.get_model_name = self._scsi_host_attribute_getter(('model_name','modelname','model'))
        self.get_serial_num = self._scsi_host_attribute_getter(('serial_num','serialnum','serial_number'))
        self.get_fw_version = self._scsi_host_attribute_getter(('fw_version','fwrev','firmware_version'))
        self.get_emulex_driver = self._scsi_host_attribute_getter(('lpfc_drvr_version',))
        self.get_hfcldd_proc = self._scsi_host_attribute_getter(('hfcldd_proc',))
        self.get_port_id = self._fc_host_attribute_getter(('port_id',))
        self.get_port_type = self._fc_host_attribute_getter(('port_type',))
        self.get_port_name = self._fc_host_attribute_getter(('port_name',))
        self.get_node_name = self._fc_host_attribute_getter(('node_name',))
        self.get_port_speed = self._fc_host_attribute_getter(('speed',))

    def _getRedHatVersion(self, shell):
        path = "/etc/redhat-release"
        executor = self.__get_produce_result_executor(shell)
        try:
            verInfo = Sfn(executor.process)(cat(path)).strip()
            # example for verInfo: Red Hat Enterprise Linux ES release 4 (Nahant Update 9)
            m = re.search("\d+\.*\d*", verInfo)
            return m and m.group(0)
        except:
            logger.info("It is not RedHat!")
            return None

    @staticmethod
    def list_fc_host_instances(list_dir_fullnames_fn):
        '''Returns list of paths of corresponding to fc_host class instances

        @param list_dir_fullnames_fn: callable returning list of child path
            instances corresponding to passed path.
        @type list_dir_fullnames_fn: callable[basestring or file_system.Path]
            -> list[file_system.Path].
            The callable may throw command.ExecuteException on list dir names failure

        @return: list of pathes of child directories for the target folder
        @rtype: list[file_system.Path]
        @raise command.ExecuteException: on list directory failure
        '''
        return list_dir_fullnames_fn(UnixPath('/sys/class/fc_host'))

    @staticmethod
    def list_fchost_remote_ports(list_dir_fullnames_fn, fc_host):
        '''Returns list of paths of corresponding to fc_host class remote instances

        @param list_dir_fullnames_fn: callable returning list of child path
            instances corresponding to passed path.
        @type list_dir_fullnames_fn: callable[basestring or file_system.Path]
            -> list[file_system.Path].
            The callable may throw command.ExecuteException on list dir names failure

        @param fc_host: name of fc host instance to list remote ports for
        @type fc_host: basestring
        @return: list of pathes of child directories for the target folder
        @rtype: list[file_system.Path]
        @raise command.ExecuteException: on list directory failure
        '''
        path = UnixPath('/sys/class/fc_host/%s/device' % fc_host)
        return filter(methodcaller('startswith', 'rport-'),
                      map(attrgetter('basename'),
                          list_dir_fullnames_fn(path)))

    @staticmethod
    def get_sys_class_attribute(get_file_content_fn, cls, inst_name, attr_name):
        '''Returns sysfs instance attribute value for target class

        @param get_file_content_fn: callable returning file content for passed path.
        @type get_file_content_fn: callable[basestring or file_system.Path]
            -> basestring.
            The callable may throw command.ExecuteException on get file content failure

        @param cls: name of a class to get the attribute for
        @type cls: basestring
        @param inst_name: name of the instance to query attribute for
        @type inst_name: basestring
        @param attr_name:
        @type attr_name:
        @return: Content of a file
        @rtype: basestring
        @raise command.ExecuteException: on get file content failure
        '''
        for attr in attr_name:
            path = UnixPath('/sys/class/%s/%s/%s' % (cls, inst_name, attr))
            value = get_file_content_fn(path)
            if value is None:
                logger.info("Command %s not found, try next one" % attr)
                continue
            return value

    @classmethod
    def get_scsi_host_attribute(cls, get_file_content_fn, scsi_name, attr_name):
        '''Returns sysfs instance attribute value for scsi_host class

        @param get_file_content_fn: callable returning file content for passed path.
        @type get_file_content_fn: callable[basestring or file_system.Path]
            -> basestring.
            The callable may throw command.ExecuteException on get file content failure

        @param scsi_name: name of the scsi_host instance to query attribute for
        @type scsi_name: basestring
        @param attr_name: name of the attribute to get value for
        @type attr_name: basestring
        @return: attribute value for target scsi device
        @rtype: basestring
        @raise command.ExecuteException: on get file content failure
        '''
        return cls.get_sys_class_attribute(get_file_content_fn, "scsi_host",
                                           scsi_name, attr_name)

    @classmethod
    def get_fc_host_attribute(cls, get_file_content_fn, fc_name, attr_name):
        '''Returns sysfs instance attribute value for fc_host class

        @param get_file_content_fn: callable returning file content for passed path.
        @type get_file_content_fn: callable[basestring or file_system.Path]
            -> basestring.
            The callable may throw command.ExecuteException on get file content failure

        @param fc_name: name of the fc_name instance to query attribute for
        @type fc_name: basestring
        @param attr_name: name of the attribute to get value for
        @type attr_name: basestring
        @return: attribute value for target fchost device
        @rtype: basestring
        @raise command.ExecuteException: on get file content failure
        '''
        return cls.get_sys_class_attribute(get_file_content_fn, "fc_host",
                                           fc_name, attr_name)

    @classmethod
    def _fc_host_attribute_getter(cls, attr_name):
        return partial(cls.get_fc_host_attribute, attr_name=attr_name)

    @classmethod
    def _scsi_host_attribute_getter(cls, attr_name):
        return partial(cls.get_scsi_host_attribute, attr_name=attr_name)

    def _get_list_dir_fullnames_fn(self, shell):
        executor = self.__get_produce_result_executor(shell)
        ls = find_ls_impl(command.cmdlet.executeCommand(shell))

        def fn(path):
            cmd = ls(path + '*').d.file_per_line
            return map(UnixPath, executor.process(cmd))
        return fn

    def _get_readlink_fn(self, shell):
        executor = self.__get_produce_result_executor(shell)

        def fn(path):
            return executor.process(readlink(path).e)
        return fn

    def _get_file_content_fn(self, shell):
        executor = self.__get_produce_result_executor(shell)

        def fn(path):
            return Sfn(executor.process)(cat(path))
        return fn

    def get_vendor_by_device_id(self, device_id, executor):
        '''Returns vendor name by device id

        @param device_id: id of device in form <domain>:<bus>:<slot>
        @type device_id: basestring
        @param executor: instance of a command executor
        @type executor: command.Executor
        @return: vendor name
        @rtype: basestring
        '''
        handler = comp(*reversed((command.cmdlet.raiseOnNonZeroReturnCode,
                                 command.cmdlet.raiseWhenOutputIsNone,
                                 command.cmdlet.stripOutput,
                                 fptools.methodcaller('splitlines'),
                                 _parse_lspci)))
        logger.info(device_id)
        lspci = command.UnixBaseCmd("lspci -v -m -n -s %s" % device_id,
                                    handler=handler)
        result = executor.process(lspci)
        logger.info(result)
        return vendors.find_name_by_id_in_hex(result.get('vendor'))

    def get_vendor_by_device_path(self, path, executor):
        '''Returns vendor name by device fs path

        @param path: path to device
        @type path: file_system.Path
        @param executor: instance of a command executor
        @type executor: command.Executor
        @return: vendor name
        @rtype: basestring
        '''
        device_path = executor.process(readlink(path).e)
        device_id = device_path.get_parent().get_parent().get_parent().basename
        return self.get_vendor_by_device_id(device_id, executor)

    def __get_produce_result_executor(self, shell):
        return command.ChainedCmdlet(command.cmdlet.executeCommand(shell),
                                         command.cmdlet.produceResult)

    def get_remote_port_descriptors(self, list_dir_fullnames_fn, get_content_fn, fchost):
        '''Returns dictionary of remote port details corresponding to passed fchost instance

        @param list_dir_fullnames_fn: callable returning list of child path
            instances corresponding to passed path.
        @type list_dir_fullnames_fn: callable[basestring or file_system.Path]
            -> list[file_system.Path].
            The callable may throw command.ExecuteException on list dir names failure

        @param get_content_fn: callable returning file content for passed path.
        @type get_content_fn: callable[basestring or file_system.Path]
            -> basestring.
            The callable may throw command.ExecuteException on get file content failure

        @param fchost: name of fc host instance to list remote ports for
        @type fchost: basestring
        @return: dictionary of remote port details:
            node wwn as a key, list of port wwn and port id pairs as value
        @rtype: dict[basestring, list[tuple[basestring, basestring]]
        @raise command.ExecuteException: on list directory failure
        '''
        names = self.list_fchost_remote_ports(list_dir_fullnames_fn, fchost)
        result = defaultdict(list)
        for name in names:
            portid = get_content_fn(UnixPath('/sys/class/fc_remote_ports') + name + 'port_id')
            nodename = get_content_fn(UnixPath('/sys/class/fc_remote_ports') + name + 'node_name')
            portname = get_content_fn(UnixPath('/sys/class/fc_remote_ports') + name + 'port_name')
            result[nodename].append((portid, portname))
        return result

    def _parse_hfcldd(self, hfcldd):
        ''' Returns the value of driver_version, firmware_version and model name
        only for Hitachi fcHBA under SUSE Linux.
        @param hfcldd: the content of /sys/class/scsi_host/host#n/hfcldd_proc
        oradbaasmi202:~ #  cat /sys/class/scsi_host/host1/hfcldd_proc
          Hitachi PCI to Fibre Channel Host Bus Adapter
            Driver version 4.11.17.2166  Firmware version 390472
            Package_ID              = 0x94
            Special file name       = hfcldd0
            Major_number            = 251
            Minor_number            = 0
            Instance_number         = 0
            Host# = 1, Unique id   = 0
            PCI memory space address= 0xffffc9001d41c000 (8)
            Adapter information
             Vender ID              =  1054
             Device ID              =  3020
             Port name              =  50000870005bd6f8
             Node name              =  50000870005bd6f9
             DID                    =  010700
             adapter ID             =  50000870005bd6f850000870005bd6f9
             port number            =  0
             manufacturer ID        =  HITACHI
             parts number           =  3HBX65201-B
             ec level               =  A
             model name             =  HFCE0802-M
             location               =  1b:00.00
             slot location          =  00:03.00
            Current Information
             Connection Type        =  Point to Point (fabric)
             ...
            Device Information
              target id [0] : port name = 50060e80101e2649 node name = 50060e80101e2649 DID = 10200  (pseq = 00 flags=00000031 status=00000000)

            FC persistent binding information
             automap is  ON (find configuration automatically)
             HFC-PCM                       = OFF
             Isolate setting of HBA port:
        @type: basestring
        @return: Driver_version, firmware_version, model_name
        '''
        lines = hfcldd.split('\n')
        for line in lines:
            line = line.strip()
            logger.info(line)
            if line.startswith('Driver version '):
                arr = line.split('  ')
                if arr and len(arr) >= 2:
                    drv = arr[0].split(' ')
                    fwv = arr[1].split(' ')
                    if drv and len(drv) >= 3:
                        driver_ver = drv[2].strip()
                    if fwv and len(fwv) >= 3:
                        fmware_ver = fwv[2].strip()
            elif line.startswith('model name '):
                arr = line.split('=')
                if arr and len(arr) >= 2:
                    model_name = arr[1].strip()
        return driver_ver, fmware_ver, model_name

    def _getFcHbaInEachPath(self, content, path, hostIndex):
        logger.info("Start to get information for host%s" % hostIndex)
        dictAttribute = {}  # key: attribute name, value: pattern
        dictAttribute['model_name'] = "Host Adapter for (.*):"
        dictAttribute['driver_version'] = "Driver version (.*)"
        dictAttribute['fw_version'] = "Firmware version (.*)"
        dictAttribute['serial_num'] = "Serial# (.*)"
        dictAttribute['node_wwn'] = "scsi-qla%s-adapter-node=(.*);" % hostIndex
        dictAttribute['port_wwn'] = "scsi-qla%s-adapter-port=(.*);" % hostIndex
        for attribute in dictAttribute:
            pattern = re.compile(dictAttribute[attribute])
            m = pattern.search(content)
            if m:
                value = m.group(1).strip()
                if attribute == "fw_version" and value.strip().find(' '):
                    value = value.split(' ')[0]
                dictAttribute[attribute] = value
            else:
                dictAttribute[attribute] = None
                logger.info("Can not get %s value" % attribute)
        # FcHba: 'id', 'name', 'wwn', 'vendor', 'model', 'serial_number', 'driver_version', 'firmware_version'
        vendor = "QLogic Corporation"
        node_wwn = wwn.parse_from_str(dictAttribute['node_wwn'])
        fcHba = fc_hba_model.FcHba(hostIndex,
                                  unicode(path),
                                  wwn=node_wwn,
                                  vendor=vendor,
                                  model=dictAttribute['model_name'],
                                  serial_number=dictAttribute['serial_num'],
                                  driver_version=dictAttribute['driver_version'],
                                  firmware_version=dictAttribute['fw_version'])
        return fcHba

    def _getFcPortInEachPath(self, content, hostIndex):
        ports = []
        remotePortIndex = 0
        fcPortList = defaultdict(list)
        dictAttribute = {}  # key: attribute name, value: pattern
        dictAttribute['port_wwn'] = "scsi-qla%s-adapter-port=(.*);" % hostIndex
        for attribute in dictAttribute:
            pattern = re.compile(dictAttribute[attribute])
            m = pattern.search(content)
            if m:
                value = m.group(1).strip()
                dictAttribute[attribute] = value
            else:
                dictAttribute[attribute] = None
                logger.info("Can not get %s value" % attribute)
        port_wwn = wwn.parse_from_str(dictAttribute['port_wwn'])
        while remotePortIndex != -1:
            # fc port info example: scsi-qla0-port-0=500a09808d02bf56:500a09828d02bf56:020c00:81;
            str1 = "scsi-qla%s-port-%s=(.*);" % (hostIndex, str(remotePortIndex))
            pattern = re.compile(str1)
            m = pattern.search(content)
            if m:
                eachFcPortInfo = m.group(1)
                pattern = re.compile("(.*):(.*):(.*):(.*)")
                m = pattern.search(eachFcPortInfo)
                if m:
                    port_remote_wwn = wwn.parse_from_str(m.group(2).strip())
                    if port_wwn in fcPortList:
                        fcPortList[port_wwn].append(port_remote_wwn)
                    else:
                        fcPortList[port_wwn] = [port_remote_wwn]
                else:
                    logger.warn("No remote port information!")
                    if port_wwn not in fcPortList:
                        fcPortList[port_wwn] = []
                remotePortIndex += 1
            else:
                remotePortIndex = -1
        fcPort = fc_hba_model.FcPort(None, port_wwn, None, None, None)
        remote = []
        for port_wwn in fcPortList.keys():
            for port_remote_wwn in fcPortList[port_wwn]:
                remote.append((None, fc_hba_model.FcPort(None, port_remote_wwn, None, None, None), None))
        ports.append((fcPort, tuple(remote)))
        return ports

    def get_fc_hbas_under_rh4(self, result, shell, list_file_names_fn, get_content_fn):
        # get hoxtX information from the X file(index X is the file name)
        pathPrefix = "/proc/scsi/qla2xxx/"
        executor = self.__get_produce_result_executor(shell)
        res = Sfn(executor.process)(ls('/proc/scsi/qla2xxx/')).strip()  # res contains folder name and file name
        if res:
            resLineList = res.split('\n')
            for resLine in resLineList:
                if resLine:
                    for hostIndex in resLine.strip().split(' '):
                        path = "%s%s" % (pathPrefix, hostIndex.strip())
                        try:
                            content = Sfn(executor.process)(cat(path)).strip()
                        except:
                            continue
                        else:
                            # cat each file's content, then get the needed info
                            fchba = self._getFcHbaInEachPath(content, path, hostIndex.strip())
                            ports = self._getFcPortInEachPath(content, hostIndex.strip())
                            result[fchba].extend(ports)
        return result

    def get_fc_hbas_under_new_version(self, result, list_file_names_fn, get_content_fn, executor):
        for path in self.list_fc_host_instances(list_file_names_fn):
            try:
                name = path.basename
                logger.info('base name: ' + name)
                driver_version = Sfn(self.get_driver_version)(get_content_fn, name)
                fw_version = Sfn(self.get_fw_version)(get_content_fn, name)
                model = Sfn(self.get_model_name)(get_content_fn, name)
                portwwn = self.get_port_name(get_content_fn, name)
                nodewwn = self.get_node_name(get_content_fn, name)
                nodewwn = wwn.parse_from_str(nodewwn)
                serialnum = Sfn(self.get_serial_num)(get_content_fn, name)
                vendor = Sfn(self.get_vendor_by_device_path)(path, executor)
                logger.info(vendor)

                # Need to check if it is Emulex LPFC SCSI driver
                # 'Emulex LightPulse Fibre Channel SCSI driver 8.2.0.87.1p'
                if not driver_version:
                    lpfc_driver = Sfn(self.get_emulex_driver)(get_content_fn, name)
                    if lpfc_driver:
                        arr = re.search('(Emulex).*driver\s+(\S*)$', lpfc_driver)
                        if arr:
                            if not vendor:
                                vendor = arr.group(1) + ' Corporation'
                            driver_version = arr.group(2).strip()

                # Need to check if it is Hitachi fcHBA under SUSE Linux
                if fw_version is None and model is None:
                    hfcldd = Sfn(self.get_hfcldd_proc)(get_content_fn, name)
                    if hfcldd is not None:
                        driver_version, fw_version, model = self._parse_hfcldd(hfcldd)
                        logger.info(driver_version)
                        logger.info(fw_version)
                        logger.info(model)
                fchba = fc_hba_model.FcHba(name, unicode(path),
                                           wwn=nodewwn,
                                           vendor=vendor, model=model,
                                           serial_number=serialnum,
                                           driver_version=driver_version,
                                           firmware_version=fw_version)

                remote_ports = self.get_remote_port_descriptors(list_file_names_fn, get_content_fn, name)
                logger.info(remote_ports)
                ports = []
                try:
                    port_id = self.get_port_id(get_content_fn, name)
                    port_id = Sfn(int)(port_id, 16)
                    port_wwn = wwn.parse_from_str(portwwn)
                    logger.info(port_wwn)
                    type_ = self.get_port_type(get_content_fn, name)
                    port_speed = _parse_port_speed(self.get_port_speed(get_content_fn, name))
                    ports.append((fc_hba_model.FcPort(port_id, port_wwn,
                                                      type_, None, port_speed),
                                  self._create_target_fchba_details(remote_ports)))
                except:
                    logger.debugException('Failed to create fcport data object')

                result[fchba].extend(ports)
                logger.info(result)
            except:
                logger.debugException('Failed to create fchba data object')
        return result

    def get_fc_hbas(self, shell):
        executor = self.__get_produce_result_executor(shell)
        list_file_names_fn = self._get_list_dir_fullnames_fn(shell)
        get_content_fn = self._get_file_content_fn(shell)
        result = defaultdict(list)
        rhVersion = self._getRedHatVersion(shell)
        if rhVersion == "4":
            logger.info('rhVersion is 4')
            result = self.get_fc_hbas_under_rh4(result, shell, list_file_names_fn, get_content_fn)
        else:
            logger.info('It is not RedHat.')
            result = self.get_fc_hbas_under_new_version(result, list_file_names_fn, get_content_fn, executor)
        return result.items()

    def _create_target_fchba_details(self, remote_descriptors):
        result = []
        if remote_descriptors:
            for nodewwn, port_descriptors in remote_descriptors.items():
                for port_descriptor in port_descriptors:
                    portid, portwwn = port_descriptor
                    try:
                        port_id = Sfn(int)(portid, 16)
                        wwpn = wwn.normalize(portwwn)
                        wwnn = wwn.normalize(nodewwn)
                        port_name = ''
                        node_name = ''
                        fchba = fc_hba_model.FcHba('', node_name, wwn=wwnn)
                        fcport = fc_hba_model.FcPort(port_id, wwpn, type=None, name=port_name)
                        result.append((fchba, fcport, None))
                    except (TypeError, ValueError):
                        logger.debugException('Failed to create target fchba/fcport data object')
        return tuple(result)
