import entity
import logger
import modeling
import aws
from appilog.common.system.types.vectors import ObjectStateHolderVector
from appilog.common.system.types import ObjectStateHolder
from java.lang import Exception as JavaException


def vector_add_topic(vector, region_osh, topics, topic_osh_map):
    for topic in topics:
        topic_osh = topic._build(Builder())
        topic_osh_map[topic.get_arn()] = topic_osh
        vector.add(topic_osh)
        vector.add(modeling.createLinkOSH('membership', region_osh.get(0), topic_osh))

def vector_add_subscription(vector, region_osh, subscriptions, topic_osh_map):
    for subscription in subscriptions:
        subscription_osh = subscription._build(Builder())
        topic_arn = subscription.get_topic_arn()
        if len(topic_osh_map) > 0 and topic_osh_map.get(topic_arn):
            vector.add(subscription_osh)
            vector.add(modeling.createLinkOSH('usage', subscription_osh, topic_osh_map.get(topic_arn)))
        else:
            vector.add(subscription_osh)
        vector.add(modeling.createLinkOSH('membership', region_osh.get(0), subscription_osh))

def discover_sns_topology(framework, service, account, credential_id, resource_dict):
    vector = ObjectStateHolderVector()
    from com.amazonaws.regions import RegionUtils
    regions = RegionUtils.getRegionsForService(service.ENDPOINT_PREFIX)
    for region in regions:
        logger.debug("Discover SNS in region:", region)
        service.setEndpoint(region.getServiceEndpoint(service.ENDPOINT_PREFIX))
        region_osh = aws.Reporter(aws.Builder()).reportRegion(aws.Region(region.getName()))
        discoverer = SNSDiscoverer(service)
        topic_osh_map = {}
        topics = discoverer.get_topics()
        logger.debug("topics:", topics)
        if topics and len(topics) > 0:
            vector_add_topic(vector, region_osh, topics, topic_osh_map)
        # --Subscription should be reported as link between SNS and SQS
        # logger.debug('topic_osh_map: ', topic_osh_map)
        # subscriptions = discoverer.get_subscriptions()
        # logger.debug("subscriptions:", subscriptions)
        # if subscriptions and len(subscriptions) > 0:
        #     vector_add_subscription(vector, region_osh, subscriptions, topic_osh_map)
    return vector


class SNSDiscoverer:
    def __init__(self, service):
        self._service = service

    def get_topics(self):
        try:
            from com.amazonaws.services.sns.model import ListTopicsRequest
            request = ListTopicsRequest()
            logger.debug(request)
            result = self._service.listTopics(request)
            if result:
                topics = result.getTopics()
                if len(topics) > 0:
                    return map(self.convert_to_topic, topics)
                else:
                    logger.debug('No topic listed.')
            else:
                logger.debug('No SNS topic result.')
        except JavaException:
            logger.warn('Error when get_topics()')

    def get_subscriptions(self):
        try:
            from com.amazonaws.services.sns.model import ListSubscriptionsRequest
            request = ListSubscriptionsRequest()
            result = self._service.listSubscriptions(request)
            if result:
                subscriptions = result.getSubscriptions()
                if len(subscriptions) > 0:
                    return map(self.convert_to_subscription, subscriptions)
                else:
                    logger.debug('No subscription listed.')
            else:
                logger.debug('No SNS subscription result.')
        except JavaException:
            logger.warn('Error when get_subscriptions()')

    def convert_to_topic(self, item):
        r'@types: com.amazonaws.services.sns.model.Topic -> aws.function'
        return SNSTopic(item.getTopicArn())

    def convert_to_subscription(self, item):
        r'@types: com.amazonaws.services.sns.model.subscription -> aws.function'
        return SNSSubscription(item.getSubscriptionArn(), item.getEndpoint(), item.getOwner(), item.getProtocol(),
                               item.getTopicArn())


class SNSTopic(entity.HasOsh):
    def __init__(self, arn):
        entity.HasOsh.__init__(self)
        if not arn:
            raise ValueError("SNSTopic arn is empty")
        self.arn = arn
        self.name = arn.split(':')[-1]

    def get_arn(self):
        return self.arn

    def get_name(self):
        return self.name

    def acceptVisitor(self, visitor):
        return visitor.build_topic_osh(self)

    def __repr__(self):
        return 'SNS.Topic("%s")' % (self.get_arn())


class SNSSubscription(entity.HasOsh):
    def __init__(self, arn, endpoint, owner, protocol, topic_arn):
        entity.HasOsh.__init__(self)
        if not arn:
            raise ValueError("SNSSubscription arn is empty")
        self.arn = arn
        self.endpoint = endpoint
        self.owner = owner
        self.protocol = protocol,
        self.topic_arn = topic_arn

    def get_arn(self):
        return self.arn

    def get_endpoint(self):
        return self.endpoint

    def get_owner(self):
        return self.owner

    def get_protocol(self):
        return self.protocol

    def get_topic_arn(self):
        return self.topic_arn

    def acceptVisitor(self, visitor):
        return visitor.build_subscription_osh(self)

    def __repr__(self):
        return 'SNS.Subscription("%s", "%s", "%s")' % (
            self.get_arn(), self.get_endpoint(), self.get_protocol())


class Builder:

    def build_topic_osh(self, topic):
        osh = ObjectStateHolder('aws_sns_topic')
        osh.setAttribute('cloud_resource_identifier', topic.get_arn())
        osh.setAttribute('name', topic.get_name())
        return osh

    def build_subscription_osh(self, subscription):
        osh = ObjectStateHolder('aws_sns_subscription')
        osh.setAttribute('amazon_resource_name', subscription.get_arn())
        endpoint = subscription.get_endpoint()
        owner = subscription.get_owner()
        protocol = subscription.get_protocol()
        topic_arn = subscription.get_topic_arn()
        if endpoint:
            osh.setStringAttribute('endpoint', endpoint)
        if owner:
            osh.setStringAttribute('owner', endpoint)
        if protocol:
            osh.setStringAttribute('protocol', endpoint)
        if topic_arn:
            osh.setStringAttribute('topic_arn', endpoint)
        return osh
