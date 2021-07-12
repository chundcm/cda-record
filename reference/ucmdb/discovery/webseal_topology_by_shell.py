#coding=utf-8
import logger
import modeling
import pdadmin_shell_webseal_discoverer
from webseal_topology import Reporter
from dns_resolver import create as create_resolver

from appilog.common.system.types.vectors import ObjectStateHolderVector
from com.hp.ucmdb.discovery.library.clients.protocols.command import TimeoutException

def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    client = Framework.createClient()
    creds_list = Framework.getTriggerCIDataAsList('webseal_credentials_id')
    shell_timeout = Framework.getParameter('WebSealShellTimeout')
    webseal_credentials_id = creds_list[0]
    logger.debug('Using credentials %s' % webseal_credentials_id)
    local_host = Framework.getDestinationAttribute('ip_address')
    webseal_shell = pdadmin_shell_webseal_discoverer.WebSealShell(Framework, client, webseal_credentials_id, shell_timeout=shell_timeout)
    webseal_domains = webseal_shell.enumerate_domains() or []
    resolver = create_resolver(webseal_shell.shell)
    
    webseal_domains.append('Default')
    try:
        for domain in webseal_domains:
            serverDiscovererClass = pdadmin_shell_webseal_discoverer.getServerDiscovererClass(webseal_shell.shell.isWinOs())
            try:
                try:
                    serverDiscoverer = serverDiscovererClass(webseal_shell, resolver, local_host, domain)
                    server_details = serverDiscoverer.discover()
                except pdadmin_shell_webseal_discoverer.UnsupportedCredsForDomain:
                    logger.debugException('Failed to enumerate servers for domain. Skipping.')
                    continue
            except (Exception, TimeoutException) :
                logger.debugException('')
                logger.warn('Failed to get server information for domain %s. Ignoring.' % domain)
                continue
            #logger.debug(server_details)
            junctionDiscovererClass = pdadmin_shell_webseal_discoverer.getJunctionDiscoverer(webseal_shell.shell.isWinOs())
            junction_discoverer = junctionDiscovererClass(webseal_shell, resolver, local_host, domain)
            junctions, server_to_junction_local_port_map = junction_discoverer.discover([x[0].name for x in server_details if x])
            
            virtualHostJunctionDiscovererClass = pdadmin_shell_webseal_discoverer.getVirtualHostJunctionDiscoverer(webseal_shell.shell.isWinOs())
            vrh_discoverer = virtualHostJunctionDiscovererClass(webseal_shell, resolver, local_host, domain)
            vrh_junctions, vrh_server_to_junction_local_port_map = vrh_discoverer.discover([x[0].name for x in server_details if x])
            
            server_details = pdadmin_shell_webseal_discoverer.enrich_ports_information(server_details, server_to_junction_local_port_map)
            server_details = pdadmin_shell_webseal_discoverer.enrich_ports_information(server_details, vrh_server_to_junction_local_port_map)
            #logger.debug('Discovered junctions %s' % junctions)
            reporter = Reporter()
                                       
            
            for server in server_details:
                #logger.debug('Processing server %s' % list(server))
                webseal_server_osh, container, _, oshs = reporter.report_webseal_server(pdo = server)
                OSHVResult.addAll(oshs)
                juncts = junctions.get(server[0].name, [])
                logger.debug(junctions)
                if juncts:
                    for junction in juncts:
                        _, _, _, oshs = reporter.report_junction(junction, webseal_server_osh, container, server[2])
                        OSHVResult.addAll(oshs)
                vr_juncts = vrh_junctions.get(server[0].name, [])
                if vr_juncts:
                    for junction in vr_juncts:
                        _, _, _, oshs = reporter.report_junction(junction, webseal_server_osh, container, server[2])
                        OSHVResult.addAll(oshs)
    except pdadmin_shell_webseal_discoverer.UnsupportedCredsForDomain:
        logger.debugException('')
        logger.reportWarning('The discovery has been interrupted due to authentication issue. Please check logs for details')
    return OSHVResult