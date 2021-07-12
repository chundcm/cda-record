# coding=utf-8

import logger
import errormessages
import shellutils
import modeling
from cloud_shell import WindowsAWSDiscoverer, UnixAWSDiscoverer
from vendors import PlatformVendors

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector


def DiscoveryMain(framework):
    OSHVResult = ObjectStateHolderVector()
    shell = None
    protocol = framework.getDestinationAttribute('protocol')
    try:
        client = framework.createClient()
        shell = shellutils.ShellUtils(client)
        OSHVResult.addAll(discoverPlatform(framework, shell))
    except:
        exInfo = logger.prepareJythonStackTrace('')
        errormessages.resolveAndReport(exInfo, protocol, framework)
    finally:
        if shell:
            try:
                shell.closeClient()
            except:
                logger.debug("Client was not closed properly")

    if OSHVResult.size() == 0:
        logger.reportWarning("No platform information discovered")

    return OSHVResult


def discoverPlatform(framework, shell):
    vector = ObjectStateHolderVector()
    if shell.isWinOs():
        aws_discoverer = WindowsAWSDiscoverer(shell)
    else:
        aws_discoverer = UnixAWSDiscoverer(shell)
    if aws_discoverer.is_applicable():
        instance_id = aws_discoverer.discoverInstanceId()
        host_id = framework.getDestinationAttribute('host_id')
        host_osh = modeling.createOshByCmdbIdString('host', host_id)
        host_osh.setStringAttribute('platform_vendor', PlatformVendors.AWS)
        if instance_id:
            host_osh.setAttribute("cloud_instance_id", instance_id)
            vector.add(host_osh)

        ami_id = aws_discoverer.discoverAmiId()
        if ami_id:
            ami_osh = ObjectStateHolder("aws_ami")
            ami_osh.setAttribute("ami_id", ami_id)
            vector.add(ami_osh)
            vector.add(modeling.createLinkOSH("dependency", host_osh, ami_osh))
    return vector
