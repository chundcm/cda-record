import entity
import logger
import modeling
import aws
from appilog.common.system.types.vectors import ObjectStateHolderVector
from appilog.common.system.types import ObjectStateHolder
from java.lang import Exception as JavaException


def discover_sqs_topology(framework, service, account, credential_id, resource_dict):
    vector = ObjectStateHolderVector()
    from com.amazonaws.regions import RegionUtils
    regions = RegionUtils.getRegionsForService(service.ENDPOINT_PREFIX)
    for region in regions:
        logger.debug("Discover sqs in region:", region)
        service.setEndpoint(region.getServiceEndpoint(service.ENDPOINT_PREFIX))
        region_osh = aws.Reporter(aws.Builder()).reportRegion(aws.Region(region.getName()))
        discoverer = SQSDiscoverer(service)
        queues = discoverer.get_queues()
        logger.debug("queues:", queues)
        if queues and len(queues) > 0:
            for queue in queues:
                queue_osh = queue._build(Builder())
                vector.add(queue_osh)
                vector.add(modeling.createLinkOSH('membership', region_osh.get(0), queue_osh))

    return vector


class SQSDiscoverer:
    def __init__(self, service):
        self._service = service

    def get_queues(self):
        try:
            from com.amazonaws.services.sqs.model import ListQueuesRequest
            request = ListQueuesRequest()
            result = self._service.listQueues(request)
            if result:
                queues = result.getQueueUrls()
                if len(queues) > 0:
                    return map(self.convert_to_queue, queues)
                else:
                    logger.debug('No queue listed.')
            else:
                logger.debug('No sqs queue result.')
        except JavaException:
            logger.warn('Error when listQueues()')

    def convert_to_queue(self, url):
        import re
        logger.debug(url)
        matcher = re.search('https:\/\/sqs\.([\w\d-]+)\.amazonaws\.com\/([\d]+)\/([\w\d\-_]+)', url)
        if matcher:
            name = matcher.group(3)
            arn = 'arn:aws:sqs:%s:%s:%s' % (matcher.group(1), matcher.group(2), name)
            return SQSqueue(arn, url, name)
        else:
            return None


class SQSqueue(entity.HasOsh):
    def __init__(self, arn, url, name):
        entity.HasOsh.__init__(self)
        if not arn:
            raise ValueError("SQSqueue arn is empty")
        self.arn = arn
        self.url = url
        self.name = name

    def get_arn(self):
        return self.arn

    def get_url(self):
        return self.url

    def get_name(self):
        return self.name

    def acceptVisitor(self, visitor):
        return visitor.build_queue_osh(self)

    def __repr__(self):
        return 'sqs.queue("%s")' % (self.get_arn())


class Builder:

    def build_queue_osh(self, queue):
        osh = ObjectStateHolder('aws_sqs_queue')
        osh.setAttribute('cloud_resource_identifier', queue.get_arn())
        osh.setAttribute('name', queue.get_name())
        return osh
