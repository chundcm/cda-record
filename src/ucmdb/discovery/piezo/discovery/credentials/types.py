# coding=utf-8

"""@package piezo.discovery.credentials.types
Credential types library corresponding to UCMDB protocols.
"""

# python stdlib
from functools import partial

__author__ = "Kevin Woldt"
__copyright__ = "Copyright (C) 2017"
__license__ = "MIT License"


TYPES = {
    'GENERIC': 'genericprotocol',
    'HTTP': 'httpprotocol',
}


class Credential(object):

    """Abstract credential class. Specific credentials are implemented in
    pieoz.discovery.credentials.types.
    """

    registered_types = {}

    def __init__(self, credential_id, userlabel, external=False):
        self.credential_id = credential_id
        self.userlabel = userlabel
        self.external = external

    @classmethod
    def from_protocol(cls, protocol):
        """Abstract method converting a UCMDB protocol instannce into a piezo
        Credential instance.
        """
        raise NotImplementedError

    @classmethod
    def register_class(cls, type_class):
        """Class decorator to decorate credential classes. It will register
        the decorated class under its supported credential type.
        @param[in] type_class Class to register.
        """
        cls.registered_types[type_class.credential_type] = type_class
        return type_class

    @staticmethod
    def _get_protocol_attr(protocol, name, default=None):
        unset_attr = repr(_UnsetAttribute)
        result = protocol.getProtocolAttribute(name, unset_attr)
        if result == unset_attr:
            result = default
        return result


class _UnsetAttribute(object):
    pass


@Credential.register_class
class GenericCredential(Credential):

    """A generic protocol credential."""

    credential_type = TYPES['GENERIC']

    def __init__(self, credential_id, userlabel, username, password, netaddress=None, external=False):
        super(GenericCredential, self).__init__(
            credential_id, userlabel, external)
        self.username = username
        self.password = password
        self.netaddress = netaddress

    @classmethod
    def from_protocol(cls, protocol):
        """Return a generic credential with values extracted from a UCMDB
        generic protocol.
        @param[in] protocol A UCMDB protocol instance to convert from.
        @returns GenericCredential A generic credential with data filled from
            given protocol instance.
        """
        get_attr = partial(cls._get_protocol_attr, protocol)
        kwargs = {
            'credential_id': get_attr('cm_credential_id'),
            'userlabel': get_attr('user_label'),
            'username': get_attr('protocol_username'),
            'password': get_attr('protocol_password'),
            'netaddress': get_attr('protocol_netaddress', None),
            'external': get_attr('external_vault_type', None) is not None,
        }
        return cls(**kwargs)


@Credential.register_class
class HTTPCredential(GenericCredential):

    """A HTTP protocol credential."""

    credential_type = TYPES['HTTP']

    def __init__(self, credential_id, userlabel, username, password, netaddress=None, port=None, timeout=None, secure=False, realm=None, cert_path=None, cert_password=None, external=False):
        super(HTTPCredential, self).__init__(
            credential_id, userlabel, username, password, netaddress, external)
        self.port = port
        self.timeout = timeout
        self.secure = secure
        self.realm = realm
        self.cert_path = cert_path
        self.cert_password = cert_password

    @classmethod
    def from_protocol(cls, protocol):
        """Return a HTTP credential with values extracted from a UCMDB HTTP
        protocol.
        @param[in] protocol A UCMDB protocol instance to convert from.
        @returns HTTPCredential A HTTP credential with data filled from given
            protocol instance.
        """
        get_attr = partial(cls._get_protocol_attr, protocol)
        kwargs = {
            'credential_id': get_attr('cm_credential_id'),
            'userlabel': get_attr('user_label'),
            'username': get_attr('protocol_username'),
            'password': get_attr('protocol_password'),
            'netaddress': get_attr('protocol_netaddress', None),
            'port': get_attr('protocol_port', None),
            'timeout': get_attr('protocol_timeout', None),
            'secure': get_attr('protocol', 'http').lower() == 'https',
            'realm': get_attr('realm', None),
            'cert_path': get_attr('trustStorePath', None),
            'cert_password': get_attr('trustStorePass', None),
            'external': get_attr('external_vault_type', None) is not None,
        }
        return cls(**kwargs)
