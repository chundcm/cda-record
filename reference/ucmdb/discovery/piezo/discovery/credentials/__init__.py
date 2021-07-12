# coding=utf-8

"""@package piezo.discovery.credentials
Useful collection of helper functions and classes for credential retrieval and
testing.
"""

# UCMDB java lib
from com.hp.ucmdb.discovery.library.credentials.dictionary import ProtocolDictionaryManager

# UCMDB python lib
import logger

# piezo
from ...exceptions import CredentialError
from .types import Credential

__author__ = "Kevin Woldt"
__copyright__ = "Copyright (C) 2017"
__license__ = "MIT License"


def credential_loop(credentials, func, *args, **kwargs):
    """Calls func for each provided credential until a function call ended
    successfully.
    @param[in] credentials An iterable of a sequence of credentials.
    @param[in] func Function to call for each credential till success. The
        credential is the first argument passed.
    @param[in] *args Arguments to pass to function call.
    @param[in] **kwargs Keyword arguments to pass to function call.
    @return Last credential the function was called with.
    @exception CredentialError Thrown if the credential sequence is empty and
        no function call ended successfully.
    """
    valid_credential = None
    last_error = CredentialError('no credential provided')
    for credential in credentials:
        try:
            func(credential, *args, **kwargs)
        except CredentialError as error:
            logger.info(error)
            last_error = error
        else:
            valid_credential = credential
            break

    if valid_credential is not None:
        return valid_credential
    raise last_error


def get_credential(cred_id):
    """Retrieve credential from UCMDB by ID.
    @param[in] cred_id ID of credential as string.
    @return Credential
    @exception CredentialError Raised if ID is not found in UCMDB Credential
        Manager
    """
    protocol = ProtocolDictionaryManager.getProtocolById(cred_id)
    protocol_type = protocol.getProtocolAttribute('protocol_type', None)
    cls = Credential.registered_types.get(protocol_type)
    if cls is None:
        msg = 'credential not found: {}'.format(cred_id)
        raise CredentialError(msg)
    return cls.from_protocol(protocol)


def get_credentials(cred_type, netaddress='DEFAULT', domain=None, userlabel=None, credential_id=None):
    """Retrieve all credentials of given credential type from UCMDB. Use the
    keyword arguments to filter the result.
    @param[in] cred_type Type of the credentials to retrieve.
    @param[in] netaddress IP address as string, which needs to be in the
        scope of the credential.
    @param[in] domain Retrieve the credentials from the given domain.
    @param[in] userlabel Credentials matching the given user labels. Can be a
        string or a list of strings.
    @param[in] credential_id Credentials matching the given credential IDs. Can
        be a string or a list of strings.
    @return List of credentials matching all filters.
    """
    credentials = []
    if isinstance(userlabel, basestring):
        userlabel = (userlabel,)
    if isinstance(credential_id, basestring):
        credential_id = (credential_id,)

    # UCMDB is talking about protocols instead of credentials
    protocols = ProtocolDictionaryManager.getProtocolParameters(
        cred_type, netaddress, domain)
    for protocol in protocols:
        protocol_type = protocol.getProtocolAttribute('protocol_type', None)
        cls = Credential.registered_types.get(protocol_type)
        if cls is None:
            continue
        credential = cls.from_protocol(protocol)

        if userlabel and credential.userlabel not in userlabel:
            continue
        elif credential_id and credential.credential_id not in credential_id:
            continue
        credentials.append(credential)
    return credentials
