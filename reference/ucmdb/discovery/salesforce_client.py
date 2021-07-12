# coding=utf-8
import rest_json as json
import rest_requests as requests

PROD_GET_TOKEN_URL = 'https://login.salesforce.com/services/oauth2/token'
SANDBOX_GET_TOKEN_URL = 'https://test.salesforce.com/services/oauth2/token'


def debug(*args, **kwargs):
    print args
    import logger
    logger.debug(*args)


class SalesForceCredential(object):
    def __init__(self, consumer_key, consumer_secret, username, password, security_token):
        super(SalesForceCredential, self).__init__()
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.username = username
        self.password = password
        self.security_token = security_token


class SalesForceClient(object):
    PREFIX_SERVICES_DATA = '/services/data'

    def __init__(self, proxies=None, is_sandbox=True):
        super(SalesForceClient, self).__init__()
        self.proxies = proxies
        self.URI_MAP = {}
        self.headers = {
            'Accept-Encoding': 'deflate',
            'X-PrettyPrint': 1
        }
        if is_sandbox:
            self.get_token_url = SANDBOX_GET_TOKEN_URL
        else:
            self.get_token_url = PROD_GET_TOKEN_URL

    def login(self, credential):
        params = {
            'grant_type': 'password',
            'client_id': credential.consumer_key,
            'client_secret': credential.consumer_secret,
            'username': credential.username,
            'password': credential.password + credential.security_token
        }
        rsp = requests.post(self.get_token_url, proxies=self.proxies, params=params, debug=False)
        json_rsp = rsp.json()
        if not json_rsp:
            raise Exception('Login failed')
        if rsp.status_code == 400:
            raise Exception('%s:%s' % (json_rsp['error'], json_rsp['error_description']))
        self.access_token = json_rsp['access_token']
        self.instance_url = json_rsp['instance_url']
        self.headers.update({'Authorization': 'Bearer %s' % self.access_token})
        self.getVersion()
        self.listResources()

    def getVersion(self):
        rsp = requests.get(self.instance_url + self.PREFIX_SERVICES_DATA, proxies=self.proxies, headers=self.headers)
        versions = rsp.json()
        latest_version = versions[-1]
        self.version_url = latest_version['url']
        self.latest_version_url = self.version_url
        return latest_version

    def listResources(self):
        rsp = requests.get(self.instance_url + self.latest_version_url, proxies=self.proxies, headers=self.headers)
        resources = rsp.json()
        self.resource_url_map = resources
        self.objects_url = resources['sobjects']
        self.query_url = resources['query']
        self.composite_url = resources['composite']
        return resources

    def getClassBasic(self, objectType):
        debug('get basic object info')
        rsp = requests.get(self.instance_url + self.objects_url + '/%s/' % objectType, proxies=self.proxies,
                           headers=self.headers)
        return rsp.json()

    def getClassDetail(self, objectType):
        debug('get object details')
        rsp = requests.get(self.instance_url + self.objects_url + '/%s/describe/' % objectType, proxies=self.proxies,
                           headers=self.headers)
        return rsp.json()

    def getObjectById(self, objectType, id, fields=()):
        debug('get object by id:%s, %s' % (objectType, id))
        params = None
        if fields:
            params = {'fields': ','.join(fields)}
        rsp = requests.get(self.instance_url + self.objects_url + '/%s/%s/' % (objectType, id), proxies=self.proxies,
                           headers=self.headers, params=params)
        return rsp.json()

    def query(self, query):
        debug('query objects:', query)
        url = self.instance_url + self.query_url
        while True:
            rsp = requests.get(url, proxies=self.proxies, params={'q': query}, headers=self.headers)
            result = rsp.json()
            yield result['records']
            if result['done']:
                break
            else:
                url = self.instance_url + result['nextRecordsUrl']

    def listObjectId(self, object_type):
        all_ids = []
        for records in self.query('Select Id from %s' % object_type):
            all_ids += [record['Id'] for record in records]
        return all_ids

    def insertObject(self, object_type, data):
        debug('Insert object', object_type, data)

        headers = {'Content-Type': 'application/json'}
        headers.update(self.headers)
        rsp = requests.post(self.instance_url + self.objects_url + '/%s/' % object_type, proxies=self.proxies,
                            headers=headers, json=data)
        if rsp.status_code == 201:
            return rsp.json()['id']

    def updateRecord(self, object_type, obj_id, data):
        debug('Update object', object_type, data)
        headers = {'Content-Type': 'application/json'}
        headers.update(self.headers)
        url = self.instance_url + self.objects_url + '/%s/%s' % (object_type, obj_id)
        rsp = requests.patch(url, proxies=self.proxies, headers=headers, data=json.dumps(data))
        return rsp.status_code == 204

    def deleteRecord(self, object_type, obj_id):
        debug('delete object')
        headers = {}
        headers.update(self.headers)
        url = self.instance_url + self.objects_url + '/%s/%s' % (object_type, obj_id)
        rsp = requests.delete(url, proxies=self.proxies, headers=headers)
        if rsp.status_code != 204:
            raise Exception(rsp.json()[0]['message'])

    def upsertRecord(self, object_type, fieldName, fieldValue, data):
        debug('Upsert object', data)
        headers = {'Content-Type': 'application/json'}
        headers.update(self.headers)
        url = self.instance_url + self.objects_url + '/%s/%s/%s' % (object_type, fieldName, fieldValue)
        rsp = requests.patch(url, proxies=self.proxies, headers=headers, data=json.dumps(data))
        if rsp.status_code == 201:  # insert
            return rsp.json()['id']
        if rsp.status_code == 204:  # update
            return None
        else:
            raise Exception(rsp.json()[0]['message'])

    def batch(self, batch_requests):
        debug('batch ')
        headers = {'Content-Type': 'application/json'}
        headers.update(self.headers)
        batch_requests = []
        data = {
            'batchRequests': batch_requests
        }

        rsp = requests.post(self.instance_url + self.composite_url + '/batch', proxies=self.proxies,
                            headers=headers, json=data)
        if rsp.status_code == 200:
            o = rsp.json()
            return o['results']

    def batchUpsertRecord(self, object_type, mutiple_reuest):
        pass
        #todo
        # for batch_request in batch_requests:
        #     sub_request = self.upsertRecord(object_type, *batch_request, dryrun = True)
        # results = self.batch(batch_requests)
        # print results


class SubRequest(object):
    def __init__(self):
        super(SubRequest, self).__init__()


def get_full_object_type(object_type, namespace=None, is_custom=False):
    if namespace:
        object_type = namespace + '__' + object_type

    if is_custom:
        object_type += '__c'

    return object_type
