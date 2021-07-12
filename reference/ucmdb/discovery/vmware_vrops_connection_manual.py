# coding=utf-8

import logger
import modeling
from dns_resolver import SocketDnsResolver
from piezo.discovery import Discovery
from piezo.discovery.credentials import credential_loop
from piezo.exceptions import CredentialError
from piezo.ports.requests import RequestException
from piezo.stateholders.osh import ObjStateHolder
from vmware_vrops_rest import VROpsClient
from vmware_vrops_utils import create_baseurl_from_credential
from vmware_vrops_utils import format_credential_port
from vmware_vrops_utils import get_http_credentials
from vmware_vrops_utils import Node
from vmware_vrops_utils import Trigger
from vmware_vrops_utils import VMwareVROps
from vmware_vrops_utils import IPServiceEndpoint

VROPS_API_URI = 'suite-api/api'
UCMDB_IP_CIT = 'ip_address'
CONTAINMENT = 'containment'
USAGE = 'usage'


def test_connection(credential, ip_address):
    """Authenticate to vrops API with given credentials to check if
    credentials are valid."""
    base_url = create_baseurl_from_credential(credential, ip_address)
    client = VROpsClient(base_url)
    logger.info("Trying to authenticate at {}".format(base_url))
    try:
        client.authenticate(credential.username, credential.password)
    except (RequestException, ValueError) as error:
        raise CredentialError(error)
    return


def resolve_hostname(ip_address):
    """Resolve hostname for given IP address."""
    dns_resolver = SocketDnsResolver()
    # Resolve exception not catched: If hostname cannot be resolved,
    # job shall run in error to prevent reconciliation error
    host_names = dns_resolver.resolve_hostnames(ip_address)
    host_names.sort()
    return host_names[0]


def discovery(add_oshv, _):
    trigger = Trigger()
    ip_address = trigger.get(trigger.IP_ADDRESS)
    credentials_id = trigger.get(trigger.VROPS_CREDENTIALSID)
    if credentials_id == 'NA':
        credentials_id = None
    credentials = get_http_credentials(credentials_id)

    valid_cred = credential_loop(credentials, test_connection, ip_address)
    base_url = create_baseurl_from_credential(valid_cred, ip_address)
    port = format_credential_port(valid_cred.port, valid_cred.secure)
    credentials_id = valid_cred.credential_id
    host_name = resolve_hostname(ip_address)

    node_osh = Node.create_osh(host_name)
    add_oshv.append(node_osh)

    ip_service_endpoint_osh = IPServiceEndpoint.create_osh(node_osh, port,
                                                           ip_address)
    add_oshv.append(ip_service_endpoint_osh)

    vrops_osh = VMwareVROps.create_osh(node_osh, base_url, ip_address,
                                       credentials_id)
    add_oshv.append(vrops_osh)
    add_oshv.append(modeling.createLinkOSH(USAGE, vrops_osh.get_osh(),
                                           ip_service_endpoint_osh.get_osh()))

    input_ipaddress_id = trigger.get(trigger.GLOBAL_ID)
    input_ipaddress_osh = ObjStateHolder(
        modeling.createOshByCmdbIdString(UCMDB_IP_CIT, input_ipaddress_id))
    add_oshv.append(input_ipaddress_osh)
    add_oshv.append(modeling.createLinkOSH(CONTAINMENT, node_osh.get_osh(),
                                           input_ipaddress_osh.get_osh()))


DiscoveryMain = Discovery(discovery)
