__author__ = 'gongze'
from arxview_rest import RestClient, RestError
from datetime import datetime
import logger

verbose = False

ARXVIEW_API_URI = {
    'loginSessions': "/api/login.php",
    'version': "/rest/version",
    'api': "/api/api.php"
}

class ArxviewException(Exception):
    pass


class ServerException(ArxviewException):
    pass


class NotLoggedInException(ArxviewException):
    pass


def requiredLogin(fun):
    def wrapper(self, *args, **kwargs):
        if not self.isLoggedIn:
            raise NotLoggedInException()
        return fun(self, *args, **kwargs)

    return wrapper


class ArxviewClient(object):
    def __init__(self, url, trustAllCerts=True):
        self.url = url
        self._apiVersion = 4
        self._headers = { 'Content-Type': 'application/x-www-form-urlencoded' }
        self.isLoggedIn = False
        if url.lower().startswith('https') and trustAllCerts:
            self.trustAllCerts = True
        else:
            self.trustAllCerts = False

        RestClient.initSSL(self.trustAllCerts)


    def getVersion(self):
        #return self.request(RestClient.GET, ARXVIEW_API_URI['version'])
        return "Unknown"

    def makeUrl(self, path):
        path_ = '%s%s' % (self.url, path)
        if verbose:
            print path_
        return path_

    def call(self, method, params=None):
        if params:
            paramsAll = "PHPSESSID=%s&m=%s&format=json&%s" % ( self._headers['PHPSESSID'], method, params)
        else:
            paramsAll = "PHPSESSID=%s&m=%s&format=json" % ( self._headers['PHPSESSID'], method)
        #print paramsAll
        return self.post( ARXVIEW_API_URI['api'], paramsAll )
    
    def post(self, path, params=None):
        return self.request(RestClient.POST, path, params)

    @requiredLogin
    def get(self, path):
        #print path
        return self.request(RestClient.GET, path)

    def delete(self, path):
        return self.request(RestClient.DELETE, path)

    def request(self, method, path, params=None):
        try:
            timeStart = datetime.now()
            data = RestClient.request(method, self.makeUrl(path), params, self._headers)
            timeEnd = datetime.now()
            logger.debug( "API Call Execution Time: %s" % ((timeEnd-timeStart)))  
            #print "*****"
            #print data
            #print "*****"
            return data
        
        except RestError, e:
            if verbose:
                print e
            if e.code == 400:
                raise NotLoggedInException(e)
            elif e.code >= 500:
                raise ServerException(e)
            raise e

    def login(self, username, password):
        #cred = {"userName": username, "password": password}
        cred = "p=%s&u=%s" % ( password, username )
        res = self.post(ARXVIEW_API_URI['loginSessions'], cred)
        #print "Login Resonse: %s" % (res)
        tokenName, tokenValue = res.split("=", 1)
        if tokenName == "PHPSESSID":
            sessionID = tokenValue
        else:
            sessionID = None
            
        if sessionID:
            #self._headers['auth'] = sessionID
            self._headers['PHPSESSID'] = sessionID
            self.isLoggedIn = True
        return self.isLoggedIn

    def logout(self):
        try:
            self.delete(ARXVIEW_API_URI['loginSessions'])
        except RestError, e:
            if e.code == 204:
                #del self._headers['auth']
                del self._headers['PHPSESSID']
        else:
            if verbose:
                print 'Not logout successfully'
