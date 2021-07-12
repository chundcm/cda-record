#coding=utf-8
import modeling
import shellutils
import logger

import firewall_discoverer
import firewall_report

from appilog.common.system.types.vectors import ObjectStateHolderVector

def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    shell = None
    try:
        client = Framework.createClient()
        shell = shellutils.ShellFactory().createShell(client)

        vendor = Framework.getDestinationAttribute('discovered_vendor')
        hostId = Framework.getDestinationAttribute('hostId')
        host_osh = modeling.createOshByCmdbId('firewall', hostId)

        discoverer = firewall_discoverer.getDiscoverer(vendor, shell, 'Shell')
        if not discoverer:
            raise ValueError('Unsupported device.')
        reporter = firewall_report.FirewallReporter(Framework)
        chassisList, virtualFirewallList, nodeList, userList, interfaceList, routingInstanceList, configDoc, firewallCluster = discoverer.discover()
        OSHVResult.addAll(reporter.report(chassisList, virtualFirewallList, nodeList, userList,
                                          interfaceList, routingInstanceList, configDoc,
                                          firewallCluster, host_osh))
    except:
        import sys
        logger.debugException('')
        error_str = str(sys.exc_info()[1]).strip()
        logger.reportError(error_str)
    finally:
        try:
            shell and shell.closeClient()
        except:
            logger.debugException('')
            logger.error('Unable to close shell')

    if OSHVResult.size() == 0:
            logger.debug('No data discovered from destination.')
            logger.reportWarning('No data discovered from destination.')
    return OSHVResult
