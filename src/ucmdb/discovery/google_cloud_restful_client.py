# coding=utf-8
import os
import rest_requests as requests
import logger
import google_cloud_jws
import base64
import time
from collections import defaultdict
from google_cloud_resolve_json import resolve_json
from com.hp.ucmdb.discovery.library.common import CollectorsParameters


GRANT_TYPE = 'grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion='
REST_URL = 'https://www.googleapis.com/discovery/v1/apis/{api}/{api_version}/rest'
API = '{api}'
API_VERSION = '{api_version}'
DEFAULT_API_VERSION = 'v1'

API_BETA_VERSION = 'v1beta1'
ORGANIZATION_ID = '{organization_id}'

BUCKET_NAME = '{bucket_name}'
FILE_NAME = '{file_name}'
DEFAULT_BUCKET_NAME = 'ucmdb_'
DEFAULT_FILE_NAME = 'allprojectfile'

CREATE_BUCKET_URL = 'https://www.googleapis.com/storage/v1/b'

ORGANIZATION_ID_URL = 'https://cloudresourcemanager.googleapis.com/{api_version}/organizations:search'

ORGANIZATION_URL = 'https://cloudasset.googleapis.com/{api_version}/organizations/{organization_id}:exportAssets'

ORGANIZATION_DATA = {"contentType":"RESOURCE"}
GCS_URL = 'gs://{bucket_name}/{file_name}'
DOWNLOAD_OBJECT = 'https://www.googleapis.com/storage/v1/b/{bucket_name}/o/{file_name}?alt=media'

DELETE_OBJECT = 'https://www.googleapis.com/storage/v1/b/{bucket_name}/o/{file_name}'
TMP_DICTIONARY = CollectorsParameters.PROBE_MGR_TEMPDOWNLOAD
JSON_PATH = TMP_DICTIONARY + DEFAULT_FILE_NAME
AUTH_HEADER = {'Content-Type': 'application/x-www-form-urlencoded'}
REQUEST_HEADER = {'Content-Type': 'application/json'}

DISCOVERY_SCOPES = 'https://www.googleapis.com/auth/cloud-platform'

DEBUG = False

class GoogleCloudClient(object):
    def __init__(self, keyFile, proxies, timeout=None):
        self.encodedKeyFile = keyFile
        self.keyFile = None
        self.proxies = proxies
        self.headers = REQUEST_HEADER
        self.projectId = None
        self.bucket_name = None
        self.timeout = timeout

    def set_project_id(self, project_id):
        self.projectId = project_id
        self.bucket_name = DEFAULT_BUCKET_NAME + self.projectId

    def authorize(self):
        if self.encodedKeyFile:
            self.keyFile = base64.b64decode(self.encodedKeyFile)
        else:
            raise Exception('Key file is not loaded.')
        logger.debug('Authorizing API access with scope: ', DISCOVERY_SCOPES)
        headers = AUTH_HEADER
        jwtString, projectId = google_cloud_jws.getSignedJWT(self.keyFile, DISCOVERY_SCOPES)
        data = GRANT_TYPE + jwtString

        try:
            rsp = requests.post(google_cloud_jws.GOOGLE_TOKEN_URI, proxies=self.proxies, data=data, headers=headers, verify=False, debug=DEBUG)
            jsonRsp = rsp.json()
        except:
            logger.debugException('Failed to get access token.')
            return 'Failed to request the access token.', projectId
        if rsp.status_code == 401:
            return '%s:%s' % (jsonRsp['error'], jsonRsp['error_description']), projectId
        if jsonRsp and jsonRsp.get('access_token'):
            self.headers.update({'Authorization': 'Bearer ' + jsonRsp['access_token']})
            self.projectId = projectId
            return None, projectId
        else:
            sc = 'Status code: %s. ' % rsp.status_code
            return sc + str(jsonRsp), projectId

    def getService(self, api, apiVersion=DEFAULT_API_VERSION):
        url = REST_URL.replace(API, api, 1).replace(API_VERSION, apiVersion, 1)
        rsp = requests.get(url, proxies=self.proxies, headers=self.getHeaders(), debug=DEBUG)
        service = rsp.json()
        return service

    def get_organization_id(self, api_version=DEFAULT_API_VERSION):
        logger.info('Starting to get the organization id.')
        organization_id_list = []
        url = ORGANIZATION_ID_URL.replace(API_VERSION, api_version, 1)
        rsp = requests.post(url, proxies=self.proxies, headers=self.getHeaders(), debug=DEBUG)

        organization_info = rsp.json()
        for organization in organization_info.get('organizations'):
            organization_name = organization.get('name')
            organization_id = organization_name.split('/')[1]
            organization_id_list.append(organization_id)
        return organization_id_list

    def create_bucket(self):
        try:
            logger.info('Starting to create a new bucket for ucmdb discovery.')
            url = CREATE_BUCKET_URL
            data = {'name': self.bucket_name}
            params = {'project': self.projectId}
            rsp = requests.post(url, proxies=self.proxies, headers=self.getHeaders(), params=params, json=data, debug=DEBUG)
            if rsp.status_code == 200:
                logger.debug('Create bucket successfully!')
                return True
            else:
                raise Exception('Please check permission, cannot create bucket:', self.bucket_name)
        except Exception, e:
            raise Exception('Cannot create new bucket for discovery:', str(e))

    def get_bucket(self):
        url = CREATE_BUCKET_URL + '/' + self.bucket_name
        logger.debug('get_bucket url:', url)
        rsp = requests.get(url, proxies=self.proxies, headers=self.getHeaders(), debug=DEBUG)
        if rsp.status_code == 200:
            return True

        logger.info('Cannot request uCMDB Bucket, must create a new bucket for uCMDB.')
        return False

    def export_data(self, organization_id):
        logger.info('Starting to export metadata to %s bucket under the organization.' % self.bucket_name)
        url_inner = GCS_URL.replace(BUCKET_NAME, self.bucket_name, 1).replace(FILE_NAME, DEFAULT_FILE_NAME, 1)
        destination = {}
        config = {}
        output = {}
        destination['uri'] = url_inner
        config['gcsDestination'] =  destination
        output['outputConfig'] = config
        ORGANIZATION_DATA.update(output)
        url = ORGANIZATION_URL.replace(API_VERSION, API_BETA_VERSION, 1).replace(ORGANIZATION_ID, organization_id, 1)
        response_obj = requests.post(url, proxies=self.proxies, headers=self.getHeaders(), json=ORGANIZATION_DATA, debug=DEBUG)
        if response_obj.status_code == 200:
            return True

        return False

    def get_request(self):
        url = DOWNLOAD_OBJECT.replace(BUCKET_NAME, self.bucket_name, 1).replace(FILE_NAME, DEFAULT_FILE_NAME, 1)
        response_obj = requests.get(url, proxies=self.proxies, headers=self.getHeaders(), stream=True, debug=DEBUG)
        return response_obj

    def get_repeat_response(self):
        # Cannot get the response immediately when the last response return 200
        # So here try request for some times until can get json file on cloud
        if not os.path.exists(TMP_DICTIONARY):
            os.mkdir(TMP_DICTIONARY)
        count = 0
        while True:
            response_obj = self.get_request()
            if response_obj.status_code == 200:
                logger.info('Starting to download metadata to local dictionary.')
                try:
                    with open(JSON_PATH, 'wb') as fw:
                        for word in response_obj.iter_content(10000):
                            fw.write(word)
                    return True
                except Exception, e:
                    raise Exception('Cannot download metadata to local ucmdb dictionary:', str(e))
            else:
                if self.timeout < 0:
                    raise Exception('TIME OUT, please enlarge exportDataToBucketTimeOut job parameter.')
                count = count + 1
                sleep_time = 10*count
                time.sleep(sleep_time)
                self.timeout = self.timeout - sleep_time

    def read_file(self):
        logger.info('Starting read local metadata file.')
        obj_dict = defaultdict(list)
        try:
            with open(JSON_PATH, 'rb') as fr:
                lines = fr.readlines()
                for line in lines:
                    object_type, result_obj = resolve_json(line)
                    if object_type:
                        obj_dict[object_type].append(result_obj)
            return obj_dict
        except Exception, e:
            raise Exception('Cannot read local json file:', str(e))


    def delete_data(self):
        url = DELETE_OBJECT.replace(BUCKET_NAME, self.bucket_name, 1).replace(FILE_NAME, DEFAULT_FILE_NAME, 1)
        rsp = requests.delete(url, proxies=self.proxies, headers=self.getHeaders(), debug=DEBUG)
        if rsp.status_code == 204:
            logger.debug('Delete metadata that used by uCMDB discovery successfully!')
            return True
        logger.debug('Cannot delete the allprojectfile in bucket.')
        return False

    def getApiAndExecute(self, service, target, method, params=None, **kwargs):
        """Get the method url from the document.

        @param service: json, retrieved from getService.
        @param target: string, the resource which is requested. (e.g. zones)
        @param method: string, list or get.
        @param **kwargs: key, value parameters for the request. (e.g. zone=us-central1-a)

        """
        from urlparse import urljoin
        baseUrl = service.get('baseUrl')
        resource = service.get('resources')
        if resource.get(target) and resource[target]['methods'].get(method):
            methodPath = resource[target]['methods'][method]['path']
            methodType = resource[target]['methods'][method]['httpMethod']
            if methodType.lower() != 'get':
                raise Exception('Only GET method should be executed.')
            methodPath = methodPath.replace('{project}', self.getProjectId())
            for key, value in kwargs.items():
                methodPath = methodPath.replace('{%s}' % key, value)
            url = urljoin(baseUrl, methodPath)
            rsp = requests.get(url, proxies=self.proxies, headers=self.getHeaders(), debug=DEBUG, params=params)
            jsonRsp = rsp.json()
            return jsonRsp

    def getListedItems(self, service, target, params=None, **kwargs):
        jsonRsp = self.getApiAndExecute(service, target, 'list', params, **kwargs)
        if jsonRsp and 'items' in jsonRsp:
            return jsonRsp['items']
        return []

    def getItemByUrl(self, url):
        rsp = requests.get(url, proxies=self.proxies, headers=self.getHeaders(), debug=DEBUG)
        jsonRsp = rsp.json()
        return jsonRsp

    def getClientType(self):
        return 'http'

    def getHeaders(self):
        return self.headers

    def getProjectId(self):
        return self.projectId

