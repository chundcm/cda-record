# coding=utf-8

import entity
import logger
import modeling
import aws
from appilog.common.system.types.vectors import ObjectStateHolderVector
from appilog.common.system.types import ObjectStateHolder
from java.lang import Exception as JavaException

SERVICE_MAP = {
    # 'apigateway.amazonaws.com': ('apigateway_discoverer', 'ApiGatewayDiscoverer'),
    'sns.amazonaws.com': ('sns', 'SNSTopic'),
}

IS_INVOKER = {
    'lambda:InvokeFunction': True
}


def discoverLambdaTopology(framework, service, account, credential_id, resourceDict):
    r'@types: Framework, AWSLambdaService, aws.Account'
    vector = ObjectStateHolderVector()

    from com.amazonaws.regions import RegionUtils
    regions = RegionUtils.getRegionsForService(service.ENDPOINT_PREFIX)
    logger.debug("lambda regions:", regions)
    for region in regions:
        logger.debug("Discover lambda in region:", region)
        try:
            endpoint = region.getServiceEndpoint(service.ENDPOINT_PREFIX)
            service.setEndpoint(endpoint)
            aws_region = aws.Region(region.getName())
            discoverer = LambdaDiscoverer(service)
            lambda_functions = discoverer.get_lambda_functions()
            logger.debug("lambda_functions:", lambda_functions)
            lambda_function_map = {}
            region_osh = aws.Reporter(aws.Builder()).reportRegion(aws_region)
            if lambda_functions and len(lambda_functions) > 0:
                for lambda_function in lambda_functions:
                    function_name = lambda_function.get_name()
                    function_arn = lambda_function.get_arn()
                    # Use getPolicy() to get the triggers of SNS, Gateway...
                    lambda_policy = discoverer.get_lambda_policy(function_name)
                    lambda_function_osh = lambda_function._build(Builder())
                    if lambda_policy and lambda_policy.find('Statement') > 0:
                        import json
                        json_policy = json.loads(lambda_policy)
                        statement_list = json_policy['Statement']
                        for statement in statement_list:
                            logger.info(statement)
                            policy_service = statement['Principal']['Service']
                            if policy_service:
                                src_arn = statement['Condition']['ArnLike']['AWS:SourceArn']
                                service_map = SERVICE_MAP.get(policy_service)
                                if src_arn and service_map:
                                    module_name = service_map[0]
                                    class_name = service_map[1]
                                    if module_name and class_name:
                                        import_class = __import__(module_name, fromlist=[class_name])
                                        module_class = getattr(import_class, class_name)
                                        obj = module_class(src_arn)
                                        import importlib
                                        module = importlib.import_module(module_name)
                                        osh = obj._build(module.Builder())
                                        if IS_INVOKER.get(statement['Action']):
                                            vector.add(osh)
                                            vector.add(modeling.createLinkOSH('dependency', lambda_function_osh, osh))
                    vector.add(lambda_function_osh)
                    lambda_function_map[function_arn] = lambda_function_osh
                    vector.add(modeling.createLinkOSH('membership', region_osh.get(0), lambda_function_osh))

            # Use listEventSourceMappings() to get SQS...
            lambda_sources = discoverer.get_lambda_sources()
            logger.debug("lambda_sources:", lambda_sources)
            logger.debug("lambda_function_map:", lambda_function_map)
            if lambda_sources and len(lambda_sources) > 0:
                for lambda_source in lambda_sources:
                    arn = lambda_source.get_arn()
                    function_arn = lambda_source.get_function_arn()
                    logger.debug('arn: ', arn)
                    logger.debug('function_arn: ', function_arn)
                    lambda_function_osh = lambda_function_map.get(function_arn)
                    if lambda_function_osh:
                        #if is SQS
                        import re
                        match = re.search('arn:aws:sqs:.*', arn)
                        if match:
                            logger.debug('add relation to SQS')
                            import sqs
                            parts = arn.split(':')
                            url = 'https://sqs.%s.amazonaws.com/%s/%s' % (parts[3], parts[4], parts[5])
                            sqs_osh = sqs.SQSqueue(arn, url, parts[5])._build(sqs.Builder())
                            vector.add(sqs_osh)
                            vector.add(modeling.createLinkOSH('dependency', lambda_function_osh, sqs_osh))

        except Exception as e:
            logger.debug('Error when discover lambda: ', e)
    return vector


class LambdaDiscoverer:
    def __init__(self, service):
        self._service = service

    def get_lambda_functions(self):
        try:
            result = self._service.listFunctions()
            if result:
                functions = result.getFunctions()
                if len(functions) > 0:
                    return map(self._convert_to_function, functions)
                else:
                    logger.debug('No function listed.')
            else:
                logger.debug('No lambda function result.')
        except:
            logger.debug('No listFunctions()')

    def _convert_to_source(self, item):
        r'@types: com.amazonaws.services.lambda.model.EventSourceMappingConfiguration -> aws.lambda_source'
        # logger.debug('item.getEventSourceArn(): ',item.getEventSourceArn())
        # logger.debug('item.getFunctionArn(): ',item.getFunctionArn())
        # logger.debug('item.getLastProcessingResult(): ',item.getLastProcessingResult())
        # logger.debug('item.getState(): ',item.getState())
        # logger.debug('item.getStateTransitionReason(): ',item.getStateTransitionReason())
        # logger.debug('item.getUUID(): ',item.getUUID())
        return LambdaSource(item.getEventSourceArn(), item.getFunctionArn())

    def _convert_to_function(self, item):
        r'@types: com.amazonaws.services.lambda.model.FunctionConfiguration -> aws.lambda_function'
        return LambdaFunction(item.getFunctionArn(),
                              item.getFunctionName(),
                              item.getVersion(),
                              item.getDescription(),
                              item.getRuntime(),
                              item.getHandler(),
                              item.getRevisionId(),
                              item.getRole(),
                              item.getTimeout(),
                              item.getLastModified(),
                              item.getCodeSize(),
                              item.getMemorySize()
                              )

    def get_lambda_sources(self):
        try:
            from com.amazonaws.services.lambda.model import ListEventSourceMappingsRequest
            request = ListEventSourceMappingsRequest()
            result = self._service.listEventSourceMappings(request)
            if result:
                sources = result.getEventSourceMappings()
                if len(sources) > 0:
                    return map(self._convert_to_source, sources)
                else:
                    logger.debug('No source listed.')
            else:
                logger.debug('No lambda source result.')
        except:
            logger.debug('No listEventSourceMappings()')


    def get_lambda_policy(self, function_name):
        try:
            from com.amazonaws.services.lambda.model import GetPolicyRequest
            request = GetPolicyRequest().withFunctionName(function_name)
            result = self._service.getPolicy(request)
            logger.debug(result)
            if result:
                policy = result.getPolicy()
                # revision_id = result.getRevisionId()
                logger.debug('Get %s Policy: %s' % (function_name, policy))
                return policy
            else:
                logger.debug('No lambda policy result.')

        except:
            logger.debug('No getPolicy()')


class LambdaSource():
    def __init__(self, arn, function_arn):
        self.arn = arn
        self.function_arn = function_arn

    def get_arn(self):
        return self.arn

    def get_function_arn(self):
        return self.function_arn


class LambdaFunction(entity.HasName, entity.HasOsh):
    def __init__(self, arn, name, version=None, description=None, runtime=None, handler=None, revision_id=None,
                 role=None,
                 timeout=None, last_modified=None, code_size=None, memory_size=None):

        entity.HasName.__init__(self)
        entity.HasOsh.__init__(self)
        self.setName(name)
        if not arn:
            raise ValueError("Function arn is empty")
        if not name:
            raise ValueError("Function name is empty")
        self.arn = arn
        self.name = name
        self.version = version
        self.description = description,
        self.runtime = runtime
        self.handler = handler
        self.revision_id = revision_id
        self.role = role
        self.timeout = timeout
        self.last_modified = last_modified
        self.code_size = code_size
        self.memory_size = memory_size

    def get_arn(self):
        return self.arn

    def get_name(self):
        return self.name

    def get_version(self):
        return self.version

    def get_description(self):
        return self.description

    def get_runtime(self):
        return self.runtime

    def get_handler(self):
        return self.handler

    def get_revision_id(self):
        return self.revision_id

    def get_role(self):
        return self.role

    def get_timeout(self):
        return self.timeout

    def get_last_modified(self):
        return self.last_modified

    def get_code_size(self):
        return self.code_size

    def get_memory_size(self):
        return self.memory_size

    def acceptVisitor(self, visitor):
        r'''@types: CanVisitAwsStack -> object
        Introduce interface for visitor expected here
        '''
        return visitor.buildLambdaFunctionOsh(self)

    def __repr__(self):
        return 'Lambda.Function("%s", "%s", "%s")' % (
            self.get_arn(), self.getName(), self.get_version())


class Builder:
    def buildLambdaFunctionOsh(self, lambda_function):
        '''
        version       :  $LATEST
        description   :  (u'',)
        runtime       :  nodejs6.10
        handler       :  index.handler
        revision_id   :  c61036c1-98ed-405f-9b4c-6514a3aa6b6e
        role          :  arn:aws:iam::325880643273:role/service-role/demoRole
        timeout       :  3
        last_modified :  2018-04-10T08:04:32.035+0000
        code_size     :  216L
        memory_size   :  128
        '''
        osh = ObjectStateHolder('aws_lambda_function')
        osh.setAttribute('cloud_resource_identifier', lambda_function.get_arn())
        osh.setAttribute('name', lambda_function.get_name())
        if lambda_function.get_version():
            osh.setStringAttribute('version', lambda_function.get_version())
        description = lambda_function.get_description()
        for description_split in description:
            if description_split:
                description = ''.join(description)
                osh.setStringAttribute('description', description)
        if lambda_function.get_runtime():
            osh.setStringAttribute('runtime', lambda_function.get_runtime())
        if lambda_function.get_handler():
            osh.setStringAttribute('handler', lambda_function.get_handler())
        if lambda_function.get_revision_id():
            osh.setStringAttribute('revision_id', lambda_function.get_revision_id())
        if lambda_function.get_role():
            osh.setStringAttribute('role', lambda_function.get_role())
        if lambda_function.get_timeout():
            osh.setIntegerAttribute('timeout', lambda_function.get_timeout())
        if lambda_function.get_last_modified():
            osh.setStringAttribute('last_modified', lambda_function.get_last_modified())
        if lambda_function.get_code_size():
            osh.setLongAttribute('code_size', lambda_function.get_code_size())
        if lambda_function.get_memory_size():
            osh.setIntegerAttribute('memory_size', lambda_function.get_memory_size())
        return osh
