import logger
import modeling

import aws
import s3

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector

def discoverS3Topology(framework, service, account, credential_id, resourceDict):
    logger.info('S3 TOPOLOGY DISCOVERY')
    s3Discoverer = S3Discoverer(service)
    try:
        bucketList = s3Discoverer.discover()
        return reportS3Topology(account, bucketList)
    except:
        logger.warnException('Fail to discover AWS S3 Topology')
        raise Exception('Fail to discover AWS S3 Topology.')

def reportS3Topology(account, bucketList):
    vector = ObjectStateHolderVector()
    accountOsh = aws.Reporter(aws.Builder()).reportAccount(account)
    vector.add(accountOsh)
    for bucket in bucketList:
        bucketOsh = BucketBuilder().build(bucket)
        bucketOsh.setContainer(accountOsh)
        vector.add(bucketOsh)
        if bucket.region:
            regionOsh = aws.Region(bucket.region).build(aws.Builder())
            membershipLink = modeling.createLinkOSH('membership', regionOsh, bucketOsh)
            vector.add(membershipLink)
            vector.add(regionOsh)
    return vector


class S3Discoverer:
    def __init__(self, service):
        self._service = service
        self.Buckets = []

    def getBucket(self):
        bucketList = []
        items = self._service.listBuckets()
        for item in items:
            bucketName = item.getName()
            bucket = s3.Bucket(bucketName)
            bucket.arn = 'arn:aws:s3:::' + bucketName

            bucketOwner = item.getOwner().getDisplayName()
            bucket.owner = bucketOwner

            bucket.create_time = item.getCreationDate()

            bucketLocation = self.getBucketLocation(bucketName)
            if bucketLocation:
                bucket.region = str(bucketLocation)

            bucket.is_versioning = self.getVersioningConfiguration(bucketName)
            bucket.is_cross_region_replication = self.getReplicationConfiguration(bucketName)

            bucketList.append(bucket)
        return bucketList

    def getBucketLocation(self, bucketName):
        return self._service.getBucketLocation(bucketName)

    def getVersioningConfiguration(self, bucketName):
        status = self._service.getBucketVersioningConfiguration(bucketName).getStatus()
        if status.lower() == 'enabled':
            return True
        else:
            return False

    def getReplicationConfiguration(self, bucketName):
        try:
            res = self._service.getBucketReplicationConfiguration(bucketName)
            return True
        except:
            return False

    def discover(self):
        bucketList = self.getBucket()
        return bucketList

class BucketBuilder:
    def build(self, bucket):
        if bucket is None: raise ValueError("Bucket is None!")

        bucketOsh = ObjectStateHolder('amazon_s3_bucket')
        bucketOsh.setStringAttribute('name', bucket.getName())
        bucketOsh.setStringAttribute('amazon_resource_name', bucket.arn)

        if bucket.owner:
            bucketOsh.setStringAttribute('owner', bucket.owner)

        if bucket.create_time:
            bucketOsh.setAttribute('bucket_creation_time', bucket.create_time)
        bucketOsh.setAttribute('is_versioning', bucket.is_versioning)
        bucketOsh.setAttribute('is_cross_region_replication', bucket.is_cross_region_replication)

        return bucketOsh
