# coding=utf-8
import logger
from remedyforce_client import RemedyForceClient
from remedyforce_decorators import mandatory_attribute, abstract_method, trim
from salesforce_client import SalesForceClient
from salesforce_client import SalesForceCredential


class AbstractConnectionDataManager:
    @abstract_method
    def getUsername(self):
        "@types: -> str"

    @abstract_method
    def getPassword(self):
        "@types: -> str"

    @abstract_method
    def getConnectionUrl(self):
        "@types: -> str"


class FrameworkBasedConnectionDataManager(AbstractConnectionDataManager):
    def __init__(self, Framework):
        self.__framework = Framework
        cre = Framework.getParameter('credentialsId')
        if not cre:
            cre = Framework.getDestinationAttribute('credentialsId')
        if not cre:
            raise Exception('No credential configured.')
        self.__credentialsId = cre
        self.__client = None

    def get_credential_property(self, property_name):
        return self.__framework.getProtocolProperty(self.__credentialsId, property_name)

    @trim
    @mandatory_attribute
    def getUsername(self):
        return self.get_credential_property('protocol_username')

    @trim
    @mandatory_attribute
    def getPassword(self):
        return self.get_credential_property('protocol_password')

    @trim
    @mandatory_attribute
    def getSecurityToken(self):
        return self.get_credential_property('security_token')

    @trim
    @mandatory_attribute
    def getConsumerKey(self):
        return self.get_credential_property('consumer_key')

    @trim
    @mandatory_attribute
    def getConsumerSecret(self):
        return self.get_credential_property('consumer_secret')

    @trim
    @mandatory_attribute
    def isSandbox(self):
        return self.get_credential_property('is_sandbox')

    @trim
    def getProxy(self):
        proxy = self.get_credential_property('http_proxy')
        return proxy and proxy.strip()

    def validate(self):
        result = True
        try:
            self.getUsername()
        except:
            self.__framework.reportError('No username.')
            logger.errorException('No username.')
            result = False
        try:
            self.getPassword()
        except:
            self.__framework.reportError('No password.')
            logger.errorException('No password.')
            result = False
        logger.debug('Test connection...')
        client = self.getClient()
        if client:
            logger.debug('Test connection successfully')
        return result

    def getClient(self):
        if not self.__client:
            proxies = None
            proxy = self.getProxy()
            if proxy:
                proxies = {'https': proxy}
            logger.debug('Use proxy:', proxies)
            sf = SalesForceClient(proxies, self.isSandbox() == 'true')
            credential = SalesForceCredential(self.getConsumerKey(), self.getConsumerSecret(), self.getUsername(),
                                              self.getPassword(), self.getSecurityToken())
            sf.login(credential)
            ovc = RemedyForceClient(sf)
            self.__client = ovc
        return self.__client

    def closeClient(self):
        if self.__client:
            logger.info("Remedyforce Logout.")
            try:
                self.__client = None
            except:
                pass
