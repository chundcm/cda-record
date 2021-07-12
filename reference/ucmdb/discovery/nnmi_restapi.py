# coding=utf-8
import sys
import logger
import ip_addr
import modeling
import rest_requests as requests
import netutils
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector
from com.hp.ucmdb.discovery.common import CollectorsConstants
from com.hp.ucmdb.discovery.library.credentials.dictionary import ProtocolDictionaryManager
import json
from functools import reduce

SCRIPT_NAME = "nnmi_restapi.py"

# Client for NNMI RESTFAPI
class NNMI_Client:

    def __init__(self, url, username, password, proxies=None):
        self.url = url
        self.proxies = proxies
        self.username = username
        self.password = password
        self.headers = None
        self.params = {'grant_type': 'password', 'username': str(self.username), 'password': str(self.password)}

    # get TokenKey
    def createAccess(self, url_token="/idp/oauth2/token"):
        response = requests.post(self.url + url_token, params=self.params, verify=False, proxies=self.proxies)

        bearer = None
        if response.status_code == 200:
            bearer = 'Bearer %s' % json.loads(response.text)["access_token"]
            self.headers = {
                'Authorization': bearer
            }
        else:
            logger.error(SCRIPT_NAME + ":invalidate username and password")
            raise Exception, SCRIPT_NAME + ":invalidate username and password"

        return bearer

    # get Data By url & return Json object
    def getDataByURL(self, url_request):
        if self.headers and self.headers["Authorization"]:
            http_url = self.url + url_request
            response = requests.get(http_url, headers=self.headers, verify=False, proxies=self.proxies, debug=False)
            logger.debug("reqeust url: " + http_url )
            logger.debug("response: " + response.text )
            if response.status_code == 200:
                return json.loads(response.text)
            elif response.status_code == 401 and "requires HTTP authen" in response.text:
                self.createAccess()
                self.getDataByURL(url_request)
            else:
                logger.error(SCRIPT_NAME + ":" + response.text)
                raise Exception, SCRIPT_NAME + ":" + response.text
        else:
            self.createAccess()
            self.getDataByURL(url_request)


# NNMI Integration job parameters
class NNMI_Config:

    def __init__(self, url, username, password, proxies):
        self.url = url
        self.username = username
        self.password = password
        self.proxies = proxies


# get NNMI Integration parameters in UCMDB
def getNNMiConfig(Framework):
    targetIpAddress = Framework.getTriggerCIData('ip_address')
    if not targetIpAddress or not ip_addr.isValidIpAddress(targetIpAddress):
        raise Exception, "Trigger IP address is empty or invalid"
    # get credentials_id
    credentialsId = Framework.getParameter('credentialsId')
    proxies = {}
    username = None
    password = None
    port = Framework.getParameter('NNMi_REST_APIport')
    if credentialsId:
        protocolObject = ProtocolDictionaryManager.getProtocolById(credentialsId)

        if protocolObject:
            username = protocolObject.getProtocolAttribute(CollectorsConstants.PROTOCOL_ATTRIBUTE_USERNAME)
            password = protocolObject.getProtocolAttribute(CollectorsConstants.PROTOCOL_ATTRIBUTE_PASSWORD)
            http_proxy = Framework.getProtocolProperty(credentialsId, "proxy", "")
            if http_proxy:
                proxies['http'] = http_proxy
                proxies['https'] = http_proxy
        else:
            logger.debug("Failed to get Protocol by provided credentialsId")
            raise Exception, "Protocol [%s] not defined" % "http"



    url = "https://" + str(targetIpAddress) + ":" + str(port)
    return NNMI_Config(url=url, username=username, password=password, proxies=proxies)


#get Interface info from UCMDB {'mac':Interface_obj}
def getInterfaceConfig(Framework):
    mac_interface_list = {}
    nnmId_interface_list = {}
    interfaces_global_id = Framework.getTriggerCIDataAsList('interface_cmdbid')
    interfaces_mac_address = Framework.getTriggerCIDataAsList('mac_address')
    interfaces_NNMid = Framework.getTriggerCIDataAsList('interface_nnm_uid')
    if not len(interfaces_global_id) == len(interfaces_mac_address):
        raise Exception,"Data Error About Interface"
    for x in range(len(interfaces_global_id)):
        if interfaces_global_id[x] and interfaces_mac_address[x] and interfaces_mac_address[x] != "NA":
            if netutils.isValidMac(interfaces_mac_address[x]):
                mac_interface_list[str(interfaces_mac_address[x]).strip()] = modeling.createOshByCmdbId("interface",interfaces_global_id[x])
    #get NNMI ID and related Interface
    for x in range(len(interfaces_global_id)):
        if interfaces_global_id[x] and interfaces_NNMid[x] and interfaces_NNMid[x] != "NA":
            nnmi_uuid = str(interfaces_NNMid[x]).split("|")[-1]
            nnmId_interface_list[str(nnmi_uuid).strip()] = modeling.createOshByCmdbId("interface",interfaces_global_id[x])

    logger.debug("mac_interface_list:"+str(mac_interface_list))
    logger.debug("nnmId_interface_list:"+str(nnmId_interface_list))
    return mac_interface_list,nnmId_interface_list

class DiscoverNNmi:

    def __init__(self, client,interface_list,nnmId_interface_list):
        self.client = client
        self.interface_list = interface_list
        # self.switch_list = switch_list
        self.nnmId_interface_list = nnmId_interface_list

    def addParameters(self, *args, **kwargs):
        parameters = "?"
        for key, value in kwargs.items():
            parameters = parameters + key + "=" + value + "&"
        parameters = parameters[:-1]
        return parameters

    def getAttachedSwitchPortByMac(self,vector):

        if not self.interface_list:
            return None

        mac_list = self.interface_list.keys()

        for index in range(0,len(mac_list),100):
            mac = reduce(( lambda x,y: str(x)+","+str(y) ),mac_list[index:index+100])
            url_mac_switch = '/nnmi/api/disco/v1/attachedSwitchPort' + self.addParameters(mac=mac)
            url_switch_interface = '/nnmi/api/disco/v1/attachedSwitchPort/'
            json_result = self.client.getDataByURL(url_mac_switch)
            nodes_Mac = json_result.get("_links", {}).get("items", None)
            if nodes_Mac:
                for switch in nodes_Mac:
                    end_mac = switch.get("title", None)
                    switch_uuid = switch.get("href","").split("/")[-1]
                    logger.debug(str(end_mac)+" attached switched switch_uuid : "+ str(switch_uuid))
                    if switch_uuid and end_mac:
                        # https: // [nnmi - server] // nnmi / api / disco / v1 / attachedSwitchPort / [UUID]
                        switch_interface = self.client.getDataByURL(url_switch_interface+str(switch_uuid))
                        if switch_interface and switch_interface.get("_links",None):
                            switch_interface_uuid = switch_interface.get("_links",{}).get("interface", {}).get("href","").split("/")[-1]
                            logger.debug(str(end_mac) + "get switch Interface UUID:" + str(switch_interface_uuid))
                            if self.nnmId_interface_list[switch_interface_uuid]:
                                # create Layer2 Link between  interface_list[end_mac] and nnmId_interface_list[switch_interface_uuid]
                                layer2Osh = ObjectStateHolder('layer2_connection')
                                l2id = "%s-%s" % (switch_interface_uuid, end_mac)  ## Create layer2_connection object's ID
                                layer2Osh.setAttribute('layer2_connection_id', str(hash(l2id)))
                                member1 = modeling.createLinkOSH('member', layer2Osh, self.interface_list[str(end_mac).strip()])
                                member2 = modeling.createLinkOSH('member', layer2Osh, self.nnmId_interface_list[str(switch_interface_uuid).strip()])
                                vector.add(layer2Osh)
                                vector.add(member1)
                                vector.add(member2)


def DiscoveryMain(Framework):
    resultVector = ObjectStateHolderVector()
    nnmi_config = getNNMiConfig(Framework)
    interface_list,nnmId_interface_list = getInterfaceConfig(Framework)

    try:

        client = NNMI_Client(nnmi_config.url, nnmi_config.username, nnmi_config.password, nnmi_config.proxies)
        token_key = client.createAccess()
        if token_key:
            discovery = DiscoverNNmi(client,interface_list,nnmId_interface_list)
            discovery.getAttachedSwitchPortByMac(resultVector)
        else:
            raise Exception, "connection failed"

    except Exception, e:
        strException = str(sys.exc_info()[1])
        excInfo = logger.prepareJythonStackTrace('')
        logger.debug(strException)
        logger.debug(excInfo)
        Framework.reportError(strException)
    return resultVector

