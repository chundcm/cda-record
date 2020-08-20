#coding=utf-8
import base64
from pyasn1.codec.der import decoder
from pyasn1_modules import pem
from pyasn1_modules.rfc5208 import PrivateKeyInfo
import rsa
import json
import logger
import datetime
from StringIO import StringIO

GOOGLE_REVOKE_URI = 'https://accounts.google.com/o/oauth2/revoke'
GOOGLE_TOKEN_URI = 'https://www.googleapis.com/oauth2/v4/token'
# Set token valid period to 30 minutes
TOKEN_VALID_PERIOD = 1800

_PKCS1_MARKER = ('-----BEGIN RSA PRIVATE KEY-----',
                 '-----END RSA PRIVATE KEY-----')
_PKCS8_MARKER = ('-----BEGIN PRIVATE KEY-----',
                 '-----END PRIVATE KEY-----')
_PKCS8_SPEC = PrivateKeyInfo()


def _urlsafe_b64encode(raw_bytes):
    raw_bytes = _to_bytes(raw_bytes, encoding='utf-8')
    return base64.urlsafe_b64encode(raw_bytes).rstrip(b'=')


def _json_encode(data):
    return json.dumps(data, separators=(',', ':'))


def _from_bytes(value):
    """Converts bytes to a string value, if necessary.

    Args:
        value: The string/bytes value to be converted.

    Returns:
        The original value converted to unicode (if bytes) or as passed in
        if it started out as unicode.

    Raises:
        ValueError if the value could not be converted to unicode.
    """
    result = (value.decode('utf-8')
              if isinstance(value, str) else value)
    if isinstance(result, unicode):
        return result
    else:
        raise ValueError(
            '{0!r} could not be converted to unicode'.format(value))


def _to_bytes(value, encoding='ascii'):
    """Converts a string value to bytes, if necessary.

    Unfortunately, ``six.b`` is insufficient for this task since in
    Python2 it does not modify ``unicode`` objects.

    Args:
        value: The string/bytes value to be converted.
        encoding: The encoding to use to convert unicode to bytes. Defaults
                  to "ascii", which will not allow any characters from ordinals
                  larger than 127. Other useful values are "latin-1", which
                  which will only allows byte ordinals (up to 255) and "utf-8",
                  which will encode any unicode that needs to be.

    Returns:
        The original value converted to bytes (if unicode) or as passed in
        if it started out as bytes.

    Raises:
        ValueError if the value could not be converted to bytes.
    """
    result = (value.encode(encoding)
              if isinstance(value, unicode) else value)
    if isinstance(result, str):
        return result
    else:
        raise ValueError('{0!r} could not be converted to bytes'.format(value))


def _datetime_to_secs(utc_time):
    # TODO(issue 298): use time_delta.total_seconds()
    # time_delta.total_seconds() not supported in Python 2.6
    epoch = datetime.datetime(1970, 1, 1)
    time_delta = utc_time - epoch
    return time_delta.days * 86400 + time_delta.seconds


def make_signed_jwt(signer, payload, key_id=None):
    """Make a signed JWT.

    See http://self-issued.info/docs/draft-jones-json-web-token.html.

    Args:
        signer: crypt.Signer, Cryptographic signer.
        payload: dict, Dictionary of data to convert to JSON and then sign.
        key_id: string, (Optional) Key ID header.

    Returns:
        string, The JWT for the payload.
    """
    header = {'typ': 'JWT', 'alg': 'RS256'}
    if key_id is not None:
        header['kid'] = key_id

    segments = [
        _urlsafe_b64encode(_json_encode(header)),
        _urlsafe_b64encode(_json_encode(payload)),
    ]
    signing_input = b'.'.join(segments)

    signature = signer.sign(signing_input)
    segments.append(_urlsafe_b64encode(signature))

    print (str(segments))

    return b'.'.join(segments)


class RsaSigner(object):
    def __init__(self, pkey):
        self._key = pkey

    def sign(self, message):
        """Signs a message.

        Args:
            message: bytes, Message to be signed.

        Returns:
            string, The signature of the message for the given key.
        """
        message = _to_bytes(message, encoding='utf-8')
        return rsa.pkcs1.sign(message, self._key, 'SHA-256')

    @classmethod
    def from_string(cls, key, password='notasecret'):
        """Construct an RsaSigner instance from a string.

        Args:
            key: string, private key in PEM format.
            password: string, password for private key file. Unused for PEM
                      files.

        Returns:
            RsaSigner instance.

        Raises:
            ValueError if the key cannot be parsed as PKCS#1 or PKCS#8 in
            PEM format.
        """
        key = _from_bytes(key)  # pem expects str in Py3
        marker_id, key_bytes = pem.readPemBlocksFromFile(
            StringIO(key), _PKCS1_MARKER, _PKCS8_MARKER)

        if marker_id == 0:
            pkey = rsa.key.PrivateKey.load_pkcs1(key_bytes,
                                                 format='DER')
        elif marker_id == 1:
            key_info, remaining = decoder.decode(
                key_bytes, asn1Spec=_PKCS8_SPEC)
            if remaining != b'':
                raise ValueError('Unused bytes', remaining)
            pkey_info = key_info.getComponentByName('privateKey')
            pkey = rsa.key.PrivateKey.load_pkcs1(pkey_info.asOctets(),
                                                 format='DER')
        else:
            raise ValueError('No key could be detected.')

        return cls(pkey)

def getSignedJWT(file, scope):
    jsonCredential = json.loads(file)

    project_id = jsonCredential['project_id']
    service_account_email = jsonCredential['client_email']
    private_key_pkcs8_pem = jsonCredential['private_key']
    private_key_id = jsonCredential['private_key_id']
    client_id = jsonCredential['client_id']
    token_uri = jsonCredential.get('token_uri', GOOGLE_TOKEN_URI)
    revoke_uri = jsonCredential.get('revoke_uri', GOOGLE_REVOKE_URI)

    signer = RsaSigner.from_string(private_key_pkcs8_pem)
    now = datetime.datetime.utcnow()
    lifetime = datetime.timedelta(seconds=TOKEN_VALID_PERIOD)
    expiry = now + lifetime
    payload = {
        'iat': _datetime_to_secs(now),
        'exp': _datetime_to_secs(expiry),
        'iss': service_account_email,
        'aud': GOOGLE_TOKEN_URI,
        'scope': scope
    }
    jwt = make_signed_jwt(signer, payload, key_id=private_key_id)
    return jwt, project_id
