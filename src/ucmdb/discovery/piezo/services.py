"""@package piezo.services
Module regarding services and connecting to UCMDB.
"""

__author__ = "Kevin Woldt"
__copyright__ = "Copyright (C) 2017"
__license__ = "MIT License"
__credits__ = "Tim Schneider, Dominik Bartsch"

# UCMDB JAVA API
from com.hp.ucmdb.api import CustomerNotAvailableException
from com.hp.ucmdb.api import InvalidCredentialsException
from com.hp.ucmdb.discovery.library.common import CollectorsParameters
from com.hp.ucmdb.api import UcmdbServiceFactory
from com.hp.ucmdb.discovery.library.credentials.dictionary import ProtocolDictionaryManager
from com.hp.ucmdb.discovery.library.clients import ScriptsExecutionManager
from com.hp.ucmdb.api import UcmdbService

# piezo lib (relative import)
from .exceptions import UserlabelNotFoundError


def get_framework():
    """Retrieves the framework from ScriptExecutionManager."""

    return ScriptsExecutionManager.getFramework()


def get_topology_by_name(service, name, parameters={}):
    """Run parametrized query and return all CIs. Executes a named query
    with given parameters.
    @param[in] service Can be either com.hp.ucmdb.api.UcmdbService or
        com.hp.ucmdb.api.topology.TopologyQueryService
    @param[in] name Name of the query you want to execute
    @param[in] parameters is an optional mapping with key as parameter name
        defined for the CI attribute in the TQL and value as value to apply.
    @exception UcmdbException Raised if an error at executing the query occurs
    @return com.hp.ucmdb.api.topology.Topology

        >>> service = get_default_ucmdb_service(api_cred_user_label)
        >>> name = "node_by_ip_or_dns"
        >>> parameters = {
        ...     'name': '1.2.3.4',
        ...     'authoritative_dns_name': 'abc.def.xyz',
        ... }
        >>> topo = get_topology_by_name(service, name, parameters)
        >>> for ci in topo.getAllCIs():
        ...     ci.getPropertyValue('name')
        'test'
        'test1'

    """

    if isinstance(service, UcmdbService):
        topo_query_srvc = service.getTopologyQueryService()
    else:
        topo_query_srvc = service

    topo_query_fctry = topo_query_srvc.getFactory()
    named_query = topo_query_fctry.createNamedQuery(name)
    query_executable = named_query.toExecutable()

    qparams = query_executable.queryParameters()
    for key, value in parameters.iteritems():
        qparams.addValue(key, value)

    topo = topo_query_srvc.executeQuery(query_executable)
    return topo


def get_ucmdb_service(srvc_prvdr, userlabel=None,
                      username=None, password=None):
    """Connects to the UCMDB via given service provider.
    If a user label is provided, the credentials are retrieved from the
    generic protocol. Otherwise username and password are used to connect to
    the UCMDB service.
    @param srvc_prvdr Services to obtain a com.hp.ucmdb.api.UcmdbService and
        server information. The service provider can be created using
        get_service_provider() function.
    @param[in] userlabel User label of the credential to use from the generic
        protocol.
    @param[in] username User name to use to connect to UCMDB service.
    @param[in] password Password to use to connect to UCMDB service.
    @exception com.hp.ucmdb.api.InvalidCredentialsException Raised if username
        or password is incorrect.
    @exception com.hp.ucmdb.api.CustomerNotAvailableException Raised if the
        server is not ready yet to accept connections.
    @exception TypeError Raised if neither userlabel nor username and password
        is given.
    @return com.hp.ucmdb.api.UcmdbService
    """

    service = None
    if userlabel is not None:
        credentials = get_generic_proto_creds(userlabel)
        if not credentials:
            raise UserlabelNotFoundError(userlabel, 'generic')
        for cred in credentials:
            username, password = cred[0:2]
            try:
                service = get_ucmdb_service(srvc_prvdr,
                                            username=username,
                                            password=password)
                break
            except CustomerNotAvailableException, err:
                raise err
            except InvalidCredentialsException, err:
                service = err
        if isinstance(service, InvalidCredentialsException):
            raise service
    elif username is not None and password is not None:
        cred_obj = srvc_prvdr.createCredentials(username, password)
        client_context = srvc_prvdr.createClientContext(
            'UCMDB Service connection')
        service = srvc_prvdr.connect(cred_obj, client_context)
    else:
        err_msg = 'neither userlabel nor username and password provided'
        raise TypeError(err_msg)
    return service


def get_default_ucmdb_service(userlabel):
    """Connects to the UCMDB service of the UCMDB the probe is connected to.
    @see get_ucmdb_service
    @param[in] userlabel User label of the credential to use from the generic
        protocol.
    @return com.hp.ucmdb.api.UcmdbService
    """

    srvc_prvdr = get_service_provider()
    return get_ucmdb_service(srvc_prvdr, userlabel=userlabel)


def get_linked_ucmdb():
    """Returns protocol, host name and port of the UCMDB the probe is connected
    to.
    @return tuple Returning a tuple with protocol, host name and port.
    """

    protocol = CollectorsParameters.getValue(
        CollectorsParameters.KEY_AGENT_PROBE_PROTOCOL_TYPE)
    hostname = CollectorsParameters.getValue(
        CollectorsParameters.KEY_SERVER_NAME)
    if protocol == 'HTTPS':
        port = int(CollectorsParameters.getValue(
            CollectorsParameters.KEY_SERVER_PORT_HTTPS))
    else:
        port = int(CollectorsParameters.getValue(
            CollectorsParameters.KEY_SERVER_PORT_HTTP))

        return protocol, hostname, port


def get_service_provider(protocol=None, hostname=None, port=None):
    """Returns an instance of a UCMDB service provider from the specified host
    port using the specified protocol. In difference to
    com.hp.ucmdb.api.UcmdbServiceFactory.getServiceProvider this function
    retrieves missing parameters from
    com.hp.ucmdb.discovery.library.common.CollectorsParameters.
    @param[in] protocol either 'http' or 'https'
    @param[in] hostname name of the UCMDB server
    @param[in] port     port the UCMDB api is listening on
    @exception java.net.MalformedURLException Raised if the host name results
        in an invalid URL.
    @exception com.hp.ucmdb.api.IncompatibleVersionException Raised if the
       version of the client is unsupported by the version running on the
       server.
    @return com.hp.ucmdb.api.UcmdbServiceProvider An instance of the service
       provider.
    """

    if protocol is None:
        protocol = CollectorsParameters.getValue(
            CollectorsParameters.KEY_AGENT_PROBE_PROTOCOL_TYPE)
    if hostname is None:
        hostname = CollectorsParameters.getValue(
            CollectorsParameters.KEY_SERVER_NAME)
    if port is None:
        if protocol.lower() == 'https':
            port = int(CollectorsParameters.getValue(
                CollectorsParameters.KEY_SERVER_PORT_HTTPS))
        else:
            port = int(CollectorsParameters.getValue(
                CollectorsParameters.KEY_SERVER_PORT_HTTP))
    provider = UcmdbServiceFactory.getServiceProvider(protocol,
                                                      hostname,
                                                      port)
    return provider


def get_generic_proto_creds(userlabel):
    """Get username and password for each credential in generic protocol,
    where the user label is matching.
    @param[in] userlabel User label of the credential to use from the generic
    @return list List of tuples containing username and password.
    @deprecated Use get_credentials() from piezo.discovery.credentials instead.
    """

    protocols = ProtocolDictionaryManager.getProtocolParameters(
        'genericprotocol', 'DEFAULT')
    credentials = []
    for protocol in protocols:
        if userlabel == protocol.getProtocolAttribute('user_label'):
            user = protocol.getProtocolAttribute('protocol_username')
            pwd = protocol.getProtocolAttribute('protocol_password')
            credentials.append((user, pwd))
    if not credentials:
        raise UserlabelNotFoundError(userlabel, 'generic')
    return credentials
