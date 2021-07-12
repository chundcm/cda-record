# coding=utf-8

"""
This is a common http client which api is similar with requests.
For now, it supports:
    Multiple http methods
    Proxy without auth
    Request and response Json
    Response compression: Deflate and Gzip
    Skip SSL verify
    Http Basic Auth
    Timeout

Yet not support:
Cookie
Proxy auth

"""
import functools
import urllib
import zlib

import logger
import rest_cache

from java.io import ByteArrayOutputStream
from java.net import URI
from org.apache.http import HttpHost
from org.apache.http.client.methods import HttpPost
from org.apache.http.entity import StringEntity
from org.apache.http.entity import ContentType
from org.apache.http.conn.routing import HttpRoute
from org.apache.http.conn.routing import HttpRoutePlanner
from org.apache.http.conn.scheme import Scheme
from org.apache.http.conn.ssl import SSLSocketFactory
from org.apache.http.conn.ssl import TrustStrategy
from org.apache.http.conn.ssl import AllowAllHostnameVerifier
from org.apache.http.impl.client import DefaultHttpClient
from org.apache.http.params import CoreConnectionPNames
from java.security import KeyStore
from java.io import FileInputStream

from java.util import ArrayList
from org.apache.http.client.entity import UrlEncodedFormEntity
from org.apache.http.message import BasicNameValuePair

cache = rest_cache.default_cache()


class DeflateDecoder(object):
    def __init__(self):
        self._first_try = True
        self._data = ''
        self._obj = zlib.decompressobj()

    def __getattr__(self, name):
        return getattr(self._obj, name)

    def decompress(self, data):
        if not data:
            return data

        if not self._first_try:
            return self._obj.decompress(data)

        self._data += data
        try:
            return self._obj.decompress(data)
        except zlib.error:
            self._first_try = False
            self._obj = zlib.decompressobj(-zlib.MAX_WBITS)
            try:
                return self.decompress(self._data)
            finally:
                self._data = None


class GzipDecoder(object):
    def __init__(self):
        self._obj = zlib.decompressobj(16 + zlib.MAX_WBITS)

    def __getattr__(self, name):
        return getattr(self._obj, name)

    def decompress(self, data):
        if not data:
            return data
        return self._obj.decompress(data)


class GzipDecoderByJava(object):
    """
    Deflate gzip data with Java
    """

    def decompress(self, data):
        from java.util.zip import GZIPInputStream
        from java.io import ByteArrayInputStream
        from java.io import ByteArrayOutputStream
        import jarray

        gis = GZIPInputStream(ByteArrayInputStream(data))
        baos = ByteArrayOutputStream()
        buffer = jarray.zeros(1024, 'b')

        try:
            while True:
                len = gis.read(buffer)
                if len == -1:
                    break
                else:
                    baos.write(buffer, 0, len)
        finally:
            gis.close()
        return baos.toByteArray().tostring()


def _get_decoder(mode):
    if mode == 'gzip':
        return GzipDecoderByJava()

    return DeflateDecoder()


class Response(object):
    def __init__(self, status_code, headers, text, raw=None):
        super(Response, self).__init__()
        self.status_code = status_code
        self.headers = headers
        self.text = text
        self.raw = raw

    def json(self):
        import rest_json as json
        return json.loads(self.text)

    def iter_content(self, chunk_size=1):
        """Iterates over the response data.  When stream=True is set on the
        request, this avoids reading the content at once into memory for
        large responses.  The chunk size is the number of bytes it should
        read into memory.
        """

        def generate():
            if self.raw:
                while True:
                    if chunk_size == 1:
                        ch = self.raw.read()
                        if ch == -1:
                            self.raw.close()
                            break
                        #If the char size out of range, it will split some part of char when read one by one
                        yield chr(ch)
                    else:
                        import jarray
                        buffer = jarray.zeros(chunk_size, 'b')
                        size = self.raw.read(buffer, 0, chunk_size)
                        if size == -1:
                            self.raw.close()
                            break
                        data = buffer[:size]
                        data = map(update_chr, data)
                        rspString = ''.join(data)
                        yield rspString

        return generate()


def update_chr(i):
    try:
        # keep the data format(not unicode) convert to string
        return str(unichr(i))
    except ValueError:
        logger.debug('Cannot encode the value:', i)
        # Because unichr function has range limitation(e.g. Chinese) in Python2, here just filter the char
        # Also can convert the char, and can write to the file(e.g. '\U-0000068'), when read must resolve this char
        # i = "\\U%08x" % i, c = i.encode('utf-8'), return c
        return ''


class RequestWithMethodJava(HttpPost):
    def __init__(self, method, url):
        HttpPost.__init__(self, url)
        self.new_method = method

    def getMethod(self):
        return self.new_method


class ProxyRoutePlanner(HttpRoutePlanner):
    def __init__(self, proxies, defaultRoutePlanner):
        self.proxies = proxies
        self.defaultRoutePlanner = defaultRoutePlanner

    def determineRoute(self, target, request, context):
        proxy = self.proxies.get(target.getSchemeName())
        if proxy:
            uri = URI.create(proxy)
            proxyHost = HttpHost(uri.getHost(), uri.getPort(), uri.getScheme())
            return HttpRoute(target, None, proxyHost, "https" == target.getSchemeName())
        return self.defaultRoutePlanner.determineRoute(target, request, context)


def _request_by_java(method, url, headers=None, data=None, proxies=None, verify=None, auth=None, token=None,
                     timeout=None,
                     keystore=None, stream=None):
    request = RequestWithMethodJava(method, url)

    if headers:
        for key, value in headers.iteritems():
            request.addHeader(key, unicode(value))

    if data:
        if type(data) is dict:
            nvps = ArrayList()
            for k in data.keys():
                nvps.add(BasicNameValuePair(k, data.get(k)))
            request.setEntity(UrlEncodedFormEntity(nvps))
        else:
            request.setEntity(StringEntity(data))
    http_client = DefaultHttpClient()

    if proxies:
        http_client.setRoutePlanner(ProxyRoutePlanner(proxies, http_client.getRoutePlanner()))

    if verify is None:
        verify = True
    if verify is not True:
        https_scheme = get_no_verify_https_factory(keystore)
        http_client.getConnectionManager().getSchemeRegistry().register(https_scheme)

    if token:
        if isinstance(token, tuple) and len(token) == 2:
            request.addHeader('Authorization', token[1] + ' ' + token[0])
    if not request.getHeaders('Authorization'):
        if auth and isinstance(auth, tuple) and len(auth) == 2:
            request.addHeader('Authorization', 'Basic ' + (auth[0] + ':' + auth[1]).encode('base64').strip())

    params = http_client.getParams()
    if timeout is not None:
        if isinstance(timeout, tuple):
            connection_timeout, read_timeout = timeout
        else:
            connection_timeout = read_timeout = float(timeout)
        connection_timeout *= 1000
        read_timeout *= 1000
        params.setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, int(connection_timeout))
        params.setIntParameter(CoreConnectionPNames.SO_TIMEOUT, int(read_timeout))

    response = http_client.execute(request)
    status_code = response.getStatusLine().getStatusCode()
    response_headers = response.getAllHeaders()
    entity = response.getEntity()
    content_type = ContentType.get(entity)
    charset = None
    if content_type:
        charset = content_type.getCharset()
    outputStream = ByteArrayOutputStream()
    body = ''
    response_headers_map = {}
    for h in response_headers:
        response_headers_map[h.getName()] = h.getValue()
    if entity:
        if stream:
            inputStream = entity.getContent()
            return status_code, inputStream, response_headers_map, charset

        entity.writeTo(outputStream)
        bytes = outputStream.toByteArray()
        body = bytes.tostring()

    return status_code, body, response_headers_map, charset


def get_no_verify_https_factory(keystore):
    class MyTS(TrustStrategy):
        def isTrusted(self, chain, authType):
            return True

    if keystore:
        keyStorePath = None
        keyStorePass = ''
        keyPass = ''
        if isinstance(keystore, tuple):
            if len(keystore) == 3:
                keyStorePath = keystore[0]
                keyStorePass = keystore[1]
                keyPass = keystore[2]
            elif len(keystore) == 2:
                keyStorePath = keystore[0]
                keyStorePass = keystore[1]
        elif isinstance(keystore, basestring):
            keyStorePath = keystore

        keyStoreResource = FileInputStream(keyStorePath)
        keyStore = KeyStore.getInstance('JKS')
        keyStore.load(keyStoreResource, keyStorePass)

        sf = SSLSocketFactory('TLS', keyStore, keyPass, None, None, MyTS(), AllowAllHostnameVerifier())
    else:
        sf = SSLSocketFactory(MyTS(), AllowAllHostnameVerifier())
    return Scheme("https", 443, sf)


@cache
def request_with_java(method, url, **kwargs):
    debug = kwargs.get('debug') is not False
    if debug:
        logger.debug('Request method: %s and URL: %s' % (method, url))
    data = kwargs.get('data') or None
    headers = kwargs.get('headers') or {}
    params = kwargs.get('params') or None
    json_obj = kwargs.get('json') or None
    if params:
        encoded_params = urllib.urlencode(params)
        if '?' not in url:
            url = url + '?' + encoded_params
        else:
            url = url + '&' + encoded_params

    if json_obj:
        import rest_json as json
        data = json.dumps(json_obj)

    proxies = kwargs.get('proxies') or None

    verify = kwargs.get('verify')
    auth = kwargs.get('auth') or None
    timeout = kwargs.get('timeout') or None
    keystore = kwargs.get('keystore') or None
    stream = kwargs.get('stream') or None
    token = kwargs.get('token') or None
    status_code, body, response_headers, charset = _request_by_java(method, url, headers, data, proxies,
                                                                    verify, auth, token, timeout, keystore, stream)

    if stream:
        return Response(status_code, response_headers, '', body)
    encoding = response_headers.get('Content-Encoding', '')
    if encoding:
        decoder = _get_decoder(encoding)
        body = decoder.decompress(body)
    charset = charset and charset.name() or 'UTF-8'
    body = body.decode(charset, errors='replace')
    if debug:
        logger.debug('Response code:', status_code)
        logger.debug('response headers: [%s]' % str(response_headers))
        logger.debug('Response body:', body)
    return Response(status_code, response_headers, body)


def request(method, url, **kwargs):
    """
    @param method: http request method, e.g. GET, POST, DELETE, OPTION, PATCH
    @param url: http request url
    @param kwargs: http request options
    @param json: json data
    @param data: post data
    @param params: http param on url
    @param headers: http request headers
    @param proxies: http proxy, e.g. proxies = {'http': 'http://proxy.com'}
    @param verify: whether verify ssl certificate, True or False
    @param auth: basic http auth, e.g. auth=(username, password)
    @param keystore: (optional) if String, path to ssl client keystore file (.jks). If Tuple, ('keyStorePath', 'keyStorePass') or ('keyStorePath', 'keyStorePass', 'keyPass')
    @param timeout: connection timeout and read timeout, unit is seconds, e.g. time=(3, 5) or timeout = 3, both of them will be 3 seconds
    @param stream: (optional) if True, get the stream of response, else the response content will be immediately downloaded.
    @return:
    """
    return request_with_java(method, url, **kwargs)


get = functools.partial(request, 'GET')
post = functools.partial(request, 'POST')
delete = functools.partial(request, 'DELETE')
head = functools.partial(request, 'HEAD')
patch = functools.partial(request, 'PATCH')
put = functools.partial(request, 'PUT')
options = functools.partial(request, 'OPTIONS')
