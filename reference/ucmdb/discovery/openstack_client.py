# coding=utf-8
import logger
import rest_requests as requests
import openstack


class OpenStackClient(object):
    def __init__(self, endpoint, proxies=None, verify=False):
        super(OpenStackClient, self).__init__()
        self.endpoint = endpoint
        self.proxies = proxies
        self.verify = verify
        self.headers = {
            'Content-Type': 'application/json'
        }
        self.services = None

    def buildApi(self, provider, tenant):
        service = self.services.get(provider, None)
        if service:
            api = OpenStackAPI(service.endpoints, tenant.id, self.proxies, self.verify)
            api.headers = self.headers
            return api

    def getJsonResponse(self, url):
        rsp = requests.get(self.endpoint + url, headers=self.headers, proxies=self.proxies, debug=False,
                           verify=self.verify)
        json_rsp = rsp.json()
        if rsp.status_code != 200:
            raise Exception('%s:%s' % (json_rsp['error']['title'], json_rsp['error']['message']))
        return json_rsp


class OpenStackClientV2(OpenStackClient):
    def __init__(self, endpoint, proxies=None, verify=False):
        OpenStackClient.__init__(self, endpoint, proxies)
        self.version = "v2"
        self.services = {}

    def login(self, username, password, tenant=None):
        if tenant:
            data = '''{"auth": {
                "tenantName": "%s",
                "passwordCredentials": {"username": "%s", "password": "%s"}
            }
            }''' % (tenant.name, username, password)

        else:
            data = '''{"auth": {
                "passwordCredentials": {"username": "%s", "password": "%s"}
            }
            }''' % (username, password)

        rsp = requests.post(self.endpoint + "/tokens", headers=self.headers, proxies=self.proxies, data=data,
                            debug=False, verify=self.verify)
        json_rsp = rsp.json()
        if rsp.status_code != 200:
            raise Exception('%s:%s' % (json_rsp['error']['title'], json_rsp['error']['message']))

        token = json_rsp["access"]["token"]["id"]
        self.headers.update({'X-Auth-Token': token})

        if json_rsp["access"].get('serviceCatalog', None):
            for service_json in json_rsp["access"]['serviceCatalog']:
                name = service_json['name']
                type = service_json['type']
                endpoints = {}
                for endpoint_json in service_json['endpoints']:
                    id = endpoint_json['id']
                    publicUrl = endpoint_json['publicURL']
                    internalUrl = endpoint_json['internalURL']
                    adminUrl = endpoint_json['adminURL']
                    region = endpoint_json['region']
                    endpoints[region] = openstack.Endpoint(id, publicUrl, internalUrl, adminUrl, region)
                self.services[type] = openstack.ServiceCatalog(name, type, endpoints)


class OpenStackClientV3(OpenStackClient):
    def __init__(self, endpoint, proxies=None, verify=False):
        OpenStackClient.__init__(self, endpoint, proxies)
        self.version = "v3"

    def login(self, username, password, tenant=None):
        if tenant:
            data = '''{"auth": {
                "identity": {
                    "methods": ["password"],
                    "password": {
                        "user": {
                            "name": "%s",
                            "domain": {"id": "default"},
                            "password": "%s"
                        }
                    }
                },
                "scope": {
                    "project": {
                        "id": "%s"
                    }
                }
            }
            }''' % (username, password, tenant.id)

        else:
            data = '''{"auth": {
                "identity": {
                    "methods": ["password"],
                    "password": {
                        "user": {
                            "name": "%s",
                            "domain": {"id": "default"},
                            "password": "%s"
                        }
                    }
                }
            }
            }''' % (username, password)

        rsp = requests.post(self.endpoint + "/auth/tokens", headers=self.headers, proxies=self.proxies, data=data,
                            debug=False, verify=self.verify)

        json_rsp = rsp.json()
        if rsp.status_code != 201:
            raise Exception('%s:%s' % (json_rsp['error']['title'], json_rsp['error']['message']))

        token = rsp.headers['X-Subject-Token']
        self.headers.update({'X-Auth-Token': token})


class OpenStackAPI(object):
    def __init__(self, endpoints, tenant, proxies=None, verify=False):
        self.endpoints = endpoints
        self.proxies = proxies
        self.verify = verify
        self.headers = {
            'Content-Type': 'application/json'
        }
        self.tenant = tenant

    def getJsonResponseByRegion(self, region, url):
        endpoint = self.endpoints.get(region, None)
        if endpoint:
            rsp = requests.get(endpoint.get_public_url().replace("%(tenant_id)s", self.tenant) + url, headers=self.headers, proxies=self.proxies, debug=False, verify=self.verify)
            json_rsp = rsp.json()
            logger.debug(endpoint.get_public_url().replace("%(tenant_id)s", self.tenant) + url)
            logger.debug(json_rsp)
            if rsp.status_code != 200:
                raise Exception('%s:%s' % (json_rsp['error']['title'], json_rsp['error']['message']))
            return json_rsp
