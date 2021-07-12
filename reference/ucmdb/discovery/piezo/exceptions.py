# coding=utf-8

"""@package piezo.exceptions
Some general exception for piezo library and to distinguish discovery errors.
"""

__author__ = "Kevin Woldt"
__copyright__ = "Copyright (C) 2017"
__license__ = "MIT License"


class OSHError(Exception):

    """Raised if an error occurs preventing the creation of an OSH."""
    pass


class TopologyError(Exception):

    """Raised if an error occurs preventing the creation of a hole topology."""
    pass


class CredentialError(Exception):
    """Base class for all credential related errors."""
    pass


class UserlabelNotFoundError(CredentialError):

    """Raised if a user label is not found in a given protocol."""

    def __init__(self, userlabel, protocol):
        """Class constructor. Initializes Exception with a meaningful error
        message.
        @param[in] userlabel    user label searched for
        @param[in] protocol     protocol searched in
        """

        super(UserlabelNotFoundError, self).__init__(
            'user label "%s" not found in protocol "%s"' % (userlabel, protocol))


class ClearException(Exception):

    """Raise this exception to clear the OSHV. Used in conjunction with the
    Discovery class.
    """

    def __init__(self, error="", clear_add_oshv=True, clear_del_oshv=True):
        self.error = error
        self.clear_add_oshv = clear_add_oshv
        self.clear_del_oshv = clear_del_oshv

    def __str__(self):
        return str(self.error)
