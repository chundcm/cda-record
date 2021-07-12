# coding=utf-8

import entity
import logger
import aws
import re
from appilog.common.system.types.vectors import ObjectStateHolderVector
from appilog.common.system.types import ObjectStateHolder
import lambda_function, ec2
import modeling

def discoverApi_GatewayTopology(framework, service, account, credential_id, resourceDict):
    vector = ObjectStateHolderVector()

    from com.amazonaws.regions import RegionUtils
    regions = RegionUtils.getRegionsForService(service.ENDPOINT_PREFIX)
    for region in regions:
        vpcLink_dict = {}
        logger.debug('Starting discover API Gateway, from region:', region)
        endpoint = region.getServiceEndpoint(service.ENDPOINT_PREFIX)
        service.setEndpoint(endpoint)
        aws_region = aws.Region(region.getName())
        region_osh = aws.Reporter(aws.Builder()).reportRegion(aws_region)
        discoverer = ApiGatewayDiscoverer(service)

        try:
            VpcLinks = discoverer.getVpclink()
            # Get the VPCLink and convert to VpcLink object
            if VpcLinks:
                for VpcLink in VpcLinks:
                    vpcLink_dict[VpcLink.id] = VpcLink

            items = discoverer.getRestApisResult()
            logger.debug("APIs:", items)
            for item in items:
                # use _build() function to report API CI
                api_osh = item._build(Builder())
                api_link = modeling.createLinkOSH('membership', region_osh.get(0), api_osh)
                vector.add(api_osh)
                vector.add(api_link)
                resources = discoverer.getResources(item.id)
                for resource in resources:
                    resource_osh = resource._build(Builder())
                    resource_link = modeling.createLinkOSH('containment', api_osh, resource_osh)
                    vector.add(resource_osh)
                    vector.add(resource_link)
                    if resource.method:
                        for method in resource.method:
                            # account,region,item,resource are object as argument, filter NoneType method
                            method = discoverer.getMethod(account, region, item, resource, method)
                            if method:
                                method_osh = method._build(Builder())
                                method_link = modeling.createLinkOSH('containment', resource_osh, method_osh)
                                vector.add(method_osh)
                                vector.add(method_link)
                                # report lambda function
                                if method.match_lambda_name():
                                    lambda_name = method.match_lambda_name()
                                    lambda_arn = 'arn:aws:lambda:'+region.getName()+':'+account.getId()+':function:'+lambda_name
                                    lambda_obj = lambda_function.LambdaFunction(lambda_arn, lambda_name)
                                    lambda_osh = lambda_obj._build(lambda_function.Builder())
                                    lambda_link = modeling.createLinkOSH('dependency', method_osh, lambda_osh)
                                    vector.add(lambda_osh)
                                    vector.add(lambda_link)
                                if method.match_http():
                                    uri_name, connId, connType = method.match_http()
                                    http_obj = Http_obj(uri_name)
                                    http_osh, host = http_obj._build(Builder())

                                    if connType == 'VPC_LINK':
                                        VpcLink = vpcLink_dict.get(connId)
                                        vpcLink_osh = VpcLink._build(Builder())
                                        # build link between Network loadbalance and vpc link
                                        if VpcLink.nlbId and VpcLink.nlbName:
                                            lbName = VpcLink.nlbName.lower()
                                            lbDNS = lbName + '-' + VpcLink.nlbId + '.elb.' + region.getName() + '.amazonaws.com'
                                            lb = ec2.Builder().Ec2LoadBalancer(ec2.LoadBalancer(lbName, lbDNS))
                                            lbOsh = lb.build(ec2.Builder())
                                            lb_link = modeling.createLinkOSH('dependency', vpcLink_osh, lbOsh)
                                            vector.add(lbOsh)
                                            vector.add(lb_link)
                                        link = modeling.createLinkOSH('dependency', method_osh, vpcLink_osh)
                                        vector.add(vpcLink_osh)
                                        vector.add(link)

                                    http_link = modeling.createLinkOSH('dependency', method_osh, http_osh)
                                    node_link = modeling.createLinkOSH('composition', host, http_osh)
                                    vector.add(http_osh)
                                    vector.add(http_link)
                                    vector.add(host)
                                    vector.add(node_link)

        except:
            logger.warnException('Fail to discover API in region:', region)
    return vector


class ApiGatewayDiscoverer(object):
    def __init__(self, service):
        self._service = service

    def getRestApisResult(self):
        from com.amazonaws.services.apigateway.model import GetRestApisRequest
        request = GetRestApisRequest()
        try:
            results = self._service.getRestApis(request)
            return map(self._convertToAPIs, results.getItems())
        except Exception, e:
            logger.debug('Failed to get API:', str(e))

    def getResources(self, apiId):
        from com.amazonaws.services.apigateway.model import GetResourcesRequest, GetMethodRequest
        # do not need arguement
        request = GetResourcesRequest().withRestApiId(apiId)
        #request = GetResourceRequest().withRestApiId('bgnoh8nc0m').withResourceId('cba7xo8aog')
        try:
            resources = self._service.getResources(request)
            logger.debug('Get result:', resources)
            return map(self._converToResources, resources.getItems())
        except Exception, e:
            logger.debug('Cannot get API resources:', str(e))

    def getMethod(self, account, region, api, resource, method):
        from com.amazonaws.services.apigateway.model import GetMethodRequest
        request = GetMethodRequest().withRestApiId(api.id).withResourceId(resource.id).withHttpMethod(method)
        try:
            method = self._service.getMethod(request)
            if not method.getMethodIntegration():
                return None
            api_method = method.getHttpMethod()
            if api_method == 'ANY':
                api_method = '*'
            arn = 'arn:aws:execute-api:' + region.getName() +':'+ account.getId() +':'+ api.id +'/*/'+ api_method + resource.path
            return Method(arn, api_method, method)
        except Exception,e:
            logger.debug('Failed get API resource method:', str(e))

    def getVpclink(self):
        from com.amazonaws.services.apigateway.model import GetVpcLinksRequest
        request = GetVpcLinksRequest()
        try:
            results = self._service.getVpcLinks(request)
            return map(self._converToVpcLinks, results.getItems())
        except Exception, e:
            logger.debug('Failed to get VPC link:', str(e))

    def _convertToAPIs(self, item):
        return RestfulApi(item.getId(),
                        item.getName(),
                        item.getEndpointConfiguration())

    def _converToResources(self, item):
        return Resource(item.getId(),
                        item.getPath(),
                        item.getPathPart(),
                        item.getResourceMethods())

    def _converToVpcLinks(self, item):
        return VpcLink(item.getId(), item.getName(), item.getStatus(), item.getTargetArns())


class Builder:
    def buildApiOsh(self, api):
        api_osh = ObjectStateHolder('aws_rest_api')
        api_osh.setAttribute('cloud_resource_identifier', api.id)
        api_osh.setAttribute('name', api.name)
        return api_osh

    def buildApiResource(self, resource):
        resource_osh = ObjectStateHolder('aws_api_resource')
        resource_osh.setAttribute('api_resource_id', resource.id)
        resource_osh.setAttribute('name', resource.pathPart)
        resource_osh.setAttribute('api_resource_path', resource.path)
        return resource_osh

    def buildApiMethod(self, method):
        method_osh = ObjectStateHolder('aws_api_method')
        method_osh.setAttribute('amazon_resource_name', method.arn)
        if method.api_method == '*':
            method.api_method = 'ANY'
        method_osh.setAttribute('name', method.api_method)
        method_osh.setAttribute('api_method', method.api_method)
        if not method.authorization:
            method.authorization = "None"
        method_osh.setAttribute('authorization', method.authorization)
        if method.integration_type:
            method_osh.setAttribute('integration_type', method.integration_type)
        return method_osh

    def buildHttp_endpoint(self, uri):
        endpoint_osh = ObjectStateHolder('uri_endpoint')
        endpoint_osh.setAttribute('uri', uri.uri_name)
        # report fake node relationship with uri_endpoint
        result_name = re.search(r'[http,https]://(.*?)\.com(.*)', uri.uri_name).group(1)
        # The regular expression for VPC link and http may be different, so use '.com' split the uriEndpoint name
        node_name = result_name + '.com'
        host = ObjectStateHolder('node')
        host.setAttribute('name', node_name)
        host.setAttribute("host_key", '%s %s' % (node_name, 'default'))

        return endpoint_osh, host

    def buildVpcLink(self, vpcLink):
        vpcLink_osh = ObjectStateHolder('aws_vpc_link')
        vpcLink_osh.setAttribute('vpc_link_id', vpcLink.id)
        vpcLink_osh.setAttribute('name', vpcLink.name)
        vpcLink_osh.setAttribute('status', vpcLink.status)
        vpcLink_osh.setAttribute('target_arn', vpcLink.targetArn)
        return vpcLink_osh

class RestfulApi(entity.HasOsh):
    # inherit hasOsh _build method
    # def _build(self, builder):
    #     return self.acceptVisitor(builder)
    def __init__(self, id, name, config):
        entity.HasOsh.__init__(self)
        if not id:
            raise ValueError("API is empty")
        self.id = id
        self.name = name
        self.config = config

    def acceptVisitor(self, visitor):
        return visitor.buildApiOsh(self)


class Resource(entity.HasOsh):
    def __init__(self, id, path,  pathPart, method=None):
        entity.HasOsh.__init__(self)
        self.id = id
        self.path = path
        self.pathPart = pathPart
        self.method = method

    def acceptVisitor(self, visitor):
        return visitor.buildApiResource(self)


class Method(entity.HasOsh):
    def __init__(self, arn, api_method, method):
        entity.HasOsh.__init__(self)
        self.arn = arn
        self.api_method = api_method
        self.authorization = method.getAuthorizationType()
        self.integration_type = method.getMethodIntegration().getType()
        self.integration_method = method.getMethodIntegration().getHttpMethod()
        self.integration_uri = method.getMethodIntegration().getUri()
        self.connection_id = method.getMethodIntegration().getConnectionId()
        self.connection_type = method.getMethodIntegration().getConnectionType()

    def match_lambda_name(self):
        # get lambda function name by uri
        if self.integration_type in ['AWS', 'AWS_PROXY']:
            try:
                lambda_name = re.search(r'arn:aws:apigateway:(.*):function:(.*)/invocations', self.integration_uri)
                lambda_function_name = lambda_name.group(2)
                split_index = lambda_function_name.find(':')
                if split_index != -1:
                    lambda_function_name = lambda_function_name[:split_index]
                return lambda_function_name
            except:
                logger.debug('Cannot match the Lamdba Function, maybe it is HTTP type!')
        return None

    def match_http(self):
        # Both HTTP and VPC Link    integration_type = 'HTTP'
        # HTTP: connection_type='INTERNET', do not has connection_id
        # VPC Link: connection_type='VPC_LINK', connection_id is VPC Link ip
        if self.integration_type in ['HTTP', 'HTTP_PROXY']:
            return self.integration_uri, self.connection_id, self.connection_type
        return None

    def acceptVisitor(self, visitor):
        return visitor.buildApiMethod(self)


class Http_obj(entity.HasOsh):
    def __init__(self, name):
        entity.HasOsh.__init__(self)
        self.uri_name = name

    def acceptVisitor(self, visitor):
        return visitor.buildHttp_endpoint(self)


class VpcLink(entity.HasOsh):
    def __init__(self, id, name, status, targetArns):
        entity.HasOsh.__init__(self)
        self.id = id
        self.name = name
        self.status = status
        self.targetArn = None
        self.nlbName = None
        self.nlbId = None
        # Here the targetArns type is list, do not know why is list
        # currently, guess there is ont-to-one match between VPC Link and NLB
        self.targetArn = targetArns[0]
        results = re.search(r'(.*)/(.*?)/(.*)', self.targetArn)
        if results:
            self.nlbName = results.group(2)
            self.nlbId = results.group(3)

    def acceptVisitor(self, visitor):
        return visitor.buildVpcLink(self)