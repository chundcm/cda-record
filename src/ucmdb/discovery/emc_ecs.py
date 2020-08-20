import modeling
import netutils
import logger
import entity
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector

class HasId:
    def __init__(self, id):
        self.__id = None
        if id is not None:
            self.setId(id)

    def setId(self, id):
        if not (id and id.strip()):
            raise ValueError("id is empty")
        self.__id = id
        return self

    def getId(self):
        return self.__id


class HasRepr(entity.HasName, entity.HasOsh, HasId):
    def __init__(self, name):
        entity.HasName.__init__(self, name)
        entity.HasOsh.__init__(self)
        HasId.__init__(self, id=None)

    def __repr__(self):
        return "%s (name: %s, id: %s)" % (self.__class__.__name__, self.getName(), self.getId())


# {"name":"ns1","id":"ns1","link":{"rel":"self","href":"/object/namespaces/namespace/ns1"},"inactive":false,"global":null,"remote":null,"vdc":null,"default_data_services_vpool":"urn:storageos:ReplicationGroupInfo:fa32ba1d-6655-40b1-80da-55714e3038fc:global","allowed_vpools_list":[],"disallowed_vpools_list":[],"namespace_admins":"root","user_mapping":[],"is_encryption_enabled":"false","default_bucket_block_size":-1,"is_stale_allowed":false,"is_compliance_enabled":false}
class NameSpace(HasRepr):
    def __init__(self, id, name):
        HasRepr.__init__(self, name)
        self.default_data_services_vpool = None
        self.setId(id)

    def acceptVisitor(self, visitor):
        return visitor.visitEcsNameSpace(self)

# {"object_bucket":[{"name":"VPC_Buk01_Ohio","id":"ns1.VPC_Buk01_Ohio","link":{"rel":"self","href":"/object/bucket/ns1.VPC_Buk01_Ohio"},"namespace":"ns1","locked":false,"created":"2018-08-16T07:25:31.218Z","retention":0,"vpool":"urn:storageos:ReplicationGroupInfo:fa32ba1d-6655-40b1-80da-55714e3038fc:global","fs_access_enabled":false,"softquota":"-1","is_stale_allowed":false,"is_tso_read_only":false,"default_retention":0,"block_size":-1,"notification_size":-1,"is_encryption_enabled":"false","TagSet":[],"default_group_file_read_permission":false,"default_group_file_write_permission":false,"default_group_file_execute_permission":false,"default_group_dir_read_permission":false,"default_group_dir_write_permission":false,"default_group_dir_execute_permission":false,"min_max_governor":{"enforce_retention":false},"owner":"pdxc_admin","api_type":"S3","search_metadata":{"isEnabled":false,"maxKeys":0,"metadata":[]}},{"name":"bat6","id":"ns1.bat6","link":{"rel":"self","href":"/object/bucket/ns1.bat6"},"namespace":"ns1","locked":false,"created":"2018-08-26T13:14:48.435Z","retention":0,"vpool":"urn:storageos:ReplicationGroupInfo:fa32ba1d-6655-40b1-80da-55714e3038fc:global","fs_access_enabled":false,"softquota":"1","is_stale_allowed":false,"is_tso_read_only":false,"default_retention":0,"block_size":-1,"notification_size":1,"is_encryption_enabled":"false","TagSet":[],"default_group_file_read_permission":false,"default_group_file_write_permission":false,"default_group_file_execute_permission":false,"default_group_dir_read_permission":false,"default_group_dir_write_permission":false,"default_group_dir_execute_permission":false,"min_max_governor":{"enforce_retention":false},"owner":"object_admin1","api_type":"S3","search_metadata":{"isEnabled":false,"maxKeys":0,"metadata":[]}},{"name":"test1_vdc","id":"ns1.test1_vdc","link":{"rel":"self","href":"/object/bucket/ns1.test1_vdc"},"namespace":"ns1","locked":false,"created":"2018-08-21T07:14:06.951Z","retention":0,"vpool":"urn:storageos:ReplicationGroupInfo:fa32ba1d-6655-40b1-80da-55714e3038fc:global","fs_access_enabled":false,"softquota":"1","is_stale_allowed":false,"is_tso_read_only":false,"default_retention":0,"block_size":-1,"notification_size":1,"is_encryption_enabled":"false","TagSet":[],"default_group_file_read_permission":false,"default_group_file_write_permission":false,"default_group_file_execute_permission":false,"default_group_dir_read_permission":false,"default_group_dir_write_permission":false,"default_group_dir_execute_permission":false,"min_max_governor":{"enforce_retention":false},"owner":"object_admin1","api_type":"S3","search_metadata":{"isEnabled":false,"maxKeys":0,"metadata":[]}},{"name":"test2_vdc","id":"ns1.test2_vdc","link":{"rel":"self","href":"/object/bucket/ns1.test2_vdc"},"namespace":"ns1","locked":false,"created":"2018-08-21T07:14:46.813Z","retention":0,"vpool":"urn:storageos:ReplicationGroupInfo:fa32ba1d-6655-40b1-80da-55714e3038fc:global","fs_access_enabled":false,"softquota":"1","is_stale_allowed":false,"is_tso_read_only":false,"default_retention":0,"block_size":-1,"notification_size":1,"is_encryption_enabled":"false","TagSet":[],"default_group_file_read_permission":false,"default_group_file_write_permission":false,"default_group_file_execute_permission":false,"default_group_dir_read_permission":false,"default_group_dir_write_permission":false,"default_group_dir_execute_permission":false,"min_max_governor":{"enforce_retention":false},"owner":"object_admin1","api_type":"S3","search_metadata":{"isEnabled":false,"maxKeys":0,"metadata":[]}},{"name":"test3_vdc","id":"ns1.test3_vdc","link":{"rel":"self","href":"/object/bucket/ns1.test3_vdc"},"namespace":"ns1","locked":false,"created":"2018-08-21T07:15:29.038Z","retention":0,"vpool":"urn:storageos:ReplicationGroupInfo:fa32ba1d-6655-40b1-80da-55714e3038fc:global","fs_access_enabled":false,"softquota":"1","is_stale_allowed":false,"is_tso_read_only":false,"default_retention":0,"block_size":-1,"notification_size":1,"is_encryption_enabled":"false","TagSet":[],"default_group_file_read_permission":false,"default_group_file_write_permission":false,"default_group_file_execute_permission":false,"default_group_dir_read_permission":false,"default_group_dir_write_permission":false,"default_group_dir_execute_permission":false,"min_max_governor":{"enforce_retention":false},"owner":"object_admin1","api_type":"S3","search_metadata":{"isEnabled":false,"maxKeys":0,"metadata":[]}}],"Filter":"namespace=ns1&name=*"}
class Bucket(HasRepr):
    def __init__(self, id, name, url):
        HasRepr.__init__(self, name)
        self.nameSpace = None
        self.setId(id)
        self.url = url

    def acceptVisitor(self, visitor):
        return visitor.visitEcsBucket(self)

# {"data_service_vpool":[{"name":"rg1","id":"urn:storageos:ReplicationGroupInfo:fa32ba1d-6655-40b1-80da-55714e3038fc:global","inactive":false,"global":null,"remote":null,"vdc":null,"description":"Default replication group description","useReplicationTarget":false,"varrayMappings":[{"name":"urn:storageos:VirtualDataCenterData:e5657cd7-40a1-43fd-af29-78fbc6dd2302","value":"urn:storageos:VirtualArray:d900b0cd-f1ab-4f11-8add-f6cbd0af945c","is_replication_target":false}],"creation_time":1535347223479,"isAllowAllNamespaces":true,"enable_rebalancing":true,"isFullRep":false}]}
class VPool(HasRepr):
    def __init__(self, id, name):
        HasRepr.__init__(self, name)
        self.vdc = []
        self.vArray = []
        self.setId(id)

    def acceptVisitor(self, visitor):
        return visitor.visitEMCVPool(self)


# {"varray":[{"id":"urn:storageos:VirtualArray:d900b0cd-f1ab-4f11-8add-f6cbd0af945c","name":"sp1","isProtected":false,"isColdStorageEnabled":false}]}
class VArray(HasRepr):
    def __init__(self, id, name):
        HasRepr.__init__(self, name)
        self.nodes = None
        self.setId(id)

    def acceptVisitor(self, visitor):
        return visitor.visitEMCVArray(self)

# {"vdc":[{"name":"vdc1","id":"urn:storageos:VirtualDataCenterData:e5657cd7-40a1-43fd-af29-78fbc6dd2302","link":{"rel":"self","href":"/object/vdcs/vdc/vdc1"},"inactive":false,"global":null,"remote":null,"vdc":null,"vdcId":"urn:storageos:VirtualDataCenterData:e5657cd7-40a1-43fd-af29-78fbc6dd2302","vdcName":"vdc1","interVdcEndPoints":"192.168.0.153","interVdcCmdEndPoints":"192.168.0.153","managementEndPoints":"192.168.0.153","secretKeys":"W6OfThPmsfRQ5ArXzkSu","permanentlyFailed":false,"local":true,"hosted":false,"is_encryption_enabled":true},{"name":"vdc2","id":"urn:storageos:VirtualDataCenterData:29c5a232-5985-496b-9228-b2e3a7251165","link":{"rel":"self","href":"/object/vdcs/vdc/vdc2"},"inactive":false,"global":null,"remote":null,"vdc":null,"vdcId":"urn:storageos:VirtualDataCenterData:29c5a232-5985-496b-9228-b2e3a7251165","vdcName":"vdc2","interVdcEndPoints":"192.168.0.56","interVdcCmdEndPoints":"192.168.0.56","managementEndPoints":"192.168.0.56","secretKeys":"WDQn6BUOs3GRyNzHO6Nh","permanentlyFailed":false,"local":false,"hosted":false,"is_encryption_enabled":true}]}
class VDC(HasRepr):
    def __init__(self, id, name):
        HasRepr.__init__(self, name)
        self.setId(id)
        self.interVdcEndPoints = []

    def acceptVisitor(self, visitor):
        return visitor.visitEMCVDC(self)

# {"node":[{"rackId":"","version":"3.1.0.3.95716.cf3f257","isLocal":true,"ip":"192.168.0.153","nodename":"luna","mgmt_ip":"192.168.0.153","geo_ip":"192.168.0.153","data_ip":"192.168.0.153","private_ip":"192.168.0.153","nodeid":"b443fb5e-a122-11e8-b6c3-0afc7e723904","data2_ip":"192.168.0.153"}]}
class Node(HasRepr):
    def __init__(self, id, name):
        HasRepr.__init__(self, name)
        self.setId(id)

    def acceptVisitor(self, visitor):
        return visitor.visitEMCNodes(self)


class Builder:

    def visitEcsNameSpace(self, nameSpace):
        osh = ObjectStateHolder('emc_ecs_namespace')
        osh.setStringAttribute('name', nameSpace.getName())
        osh.setStringAttribute('guid', nameSpace.getId())
        logger.debug("namespace osh created!")
        return osh

    def visitEcsBucket(self, bucket):
        osh = ObjectStateHolder('emc_ecs_bucket')
        osh.setStringAttribute('name', bucket.getName())
        osh.setStringAttribute('guid', bucket.getId())
        osh.setStringAttribute('url', bucket.url)
        logger.debug("bucket osh created!")
        return osh

    def visitEMCVPool(self, VPool):
        osh = ObjectStateHolder('emc_ecs_replication_group')
        osh.setStringAttribute('name', VPool.getName())
        osh.setStringAttribute('guid', VPool.getId())
        logger.debug("replication group osh created!")
        return osh

    def visitEMCVArray(self, VArray):
        osh = ObjectStateHolder('emc_ecs_storage_pool')
        osh.setStringAttribute('name', VArray.getName())
        osh.setStringAttribute('guid', VArray.getId())
        logger.debug("storage pool osh created!")
        return osh

    def visitEMCVDC(self, VDC):
        osh = ObjectStateHolder('emc_ecs_vdc')
        osh.setStringAttribute('name', VDC.getName())
        osh.setStringAttribute('guid', VDC.getId())
        logger.debug("vdc osh created!")
        return osh

    def visitEMCNodes(self, node):
        osh = ObjectStateHolder('node')
        osh.setStringAttribute('name', node.getName())
        osh.setStringAttribute('cloud_instance_id', node.getId())
        logger.debug("node osh created!")
        return osh




