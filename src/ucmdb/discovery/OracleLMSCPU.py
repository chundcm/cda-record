# coding=utf-8
import re

import logger

import modeling
import shellutils
import errormessages
import errorobject
import errorcodes
from OracleLMSCPUDiscoverer import OracleCPUUtils
import OracleLMSUtils

from java.lang import Exception as JException

from appilog.common.system.types.vectors import ObjectStateHolderVector


FILE_DESCRIPTION = 'This document was created by executing Oracle scripts for Oracle LMS CPU Data Collection. It represents hardware configuration.'


def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    hostId = Framework.getDestinationAttribute('host_id')
    logger.debug('hostId', hostId)
    protocol = Framework.getDestinationAttribute('Protocol')
    size = 2097152
    shell = None

    try:
        client = Framework.createClient()
        shell = shellutils.ShellUtils(client)
        name, content = OracleCPUUtils(Framework, client, shell).discover()

        if content:
            hostOsh = modeling.createOshByCmdbIdString('host', hostId)
            cpuFileOsh = OracleLMSUtils.createAuditDocumentOSH('Oracle LMS Audit CPU Data', size, content, hostOsh,
                                                               FILE_DESCRIPTION)
            if name:
                cpuFileOsh.setAttribute('data_note', name)
            link = modeling.createLinkOSH('composition', hostOsh, cpuFileOsh)
            OSHVResult.add(hostOsh)
            OSHVResult.add(cpuFileOsh)
            OSHVResult.add(link)
    except JException, ex:
        exInfo = ex.getMessage()
        errormessages.resolveAndReport(exInfo, protocol, Framework)
    except:
        exInfo = logger.prepareJythonStackTrace('')
        errormessages.resolveAndReport(exInfo, protocol, Framework)
    finally:
        if shell:
            try:
                shell and shell.closeClient()
            except:
                errobj = errorobject.createError(errorcodes.CLIENT_NOT_CLOSED_PROPERLY, None, "Client was not closed properly")
                logger.reportWarningObject(errobj)
    return OSHVResult