# coding=utf-8
from salesforce_client import SalesForceClient


def debug(*args, **kwargs):
    print args
    import logger
    logger.debug(*args)


class RemedyForceRelationship(object):
    def __init__(self, rel_class, rel_id, source_class, source_id, destination_class, destination_id):
        super(RemedyForceRelationship, self).__init__()
        self.rel_class = rel_class
        self.rel_id = rel_id
        self.source_class = source_class
        self.source_id = source_id
        self.destination_class = destination_class
        self.destination_id = destination_id

    def __repr__(self):
        return '{%s: %s (%s: %s => %s: %s) }' % (self.rel_class,
                                                 self.rel_id,
                                                 self.source_class,
                                                 self.source_id,
                                                 self.destination_class,
                                                 self.destination_id)


class RemedyForceClient(object):
    BMC_SERVICE_DESK_BMC_BASE_ELEMENT = 'BMCServiceDesk__BMC_BaseElement__c'
    BMC_SERVICE_DESK_BMC_BASE_RELATIONSHIP = 'BMCServiceDesk__BMC_BaseRelationship__c'

    def __init__(self, force_client):
        """
        @type force_client: SalesForceClient
        @return:
        """
        super(RemedyForceClient, self).__init__()
        self.force_client = force_client
        self.BMC_CLASS_MAP = {}

    def getCIsByType(self, rf_class, fields=None, conditions=None):
        fields = fields or set()
        if 'Id' not in fields:
            fields.add('Id')
        fields = ','.join(fields)

        conditions = conditions and conditions.strip()
        if conditions:
            conditions = ' and ' + conditions
        else:
            conditions = ''

        query = "select %s from %s where BMCServiceDesk__ClassName__c = '%s' %s" % (
            fields, self.BMC_SERVICE_DESK_BMC_BASE_ELEMENT, rf_class, conditions)
        all_data = []
        for records in self.force_client.query(query):
            all_data += records
        return all_data

    def getRelationshipIdsByType(self, rf_rel):
        query = ("select Id from BMCServiceDesk__BMC_BaseRelationship__c "
                 " where BMCServiceDesk__ImpactDirection__c = '%s'" % rf_rel)
        all_ids = []
        for records in self.force_client.query(query):
            all_ids += [record['Id'] for record in records]
        return all_ids

    def getRelationshipsByType(self, rf_rel):
        """
        @param rf_rel:
        @rtype: list of RemedyForceRelationship
        """
        query = (
            "select Id, BMCServiceDesk__ImpactDirection__c"
            ", BMCServiceDesk__Source_ClassName__c, BMCServiceDesk__Source__c"
            ", BMCServiceDesk__Destination_ClassName__c, BMCServiceDesk__Destination__c "
            " from BMCServiceDesk__BMC_BaseRelationship__c "
            " where BMCServiceDesk__ImpactDirection__c = '%s'" % rf_rel)
        all_data = []
        for records in self.force_client.query(query):
            all_data += [RemedyForceRelationship(
                record['BMCServiceDesk__ImpactDirection__c'],
                record['Id'],
                record['BMCServiceDesk__Source_ClassName__c'],
                record['BMCServiceDesk__Source__c'],
                record['BMCServiceDesk__Destination_ClassName__c'],
                record['BMCServiceDesk__Destination__c'])
                         for record in records]
        return all_data

    def getRelationshipsByTypeAndCIType(self, rf_rel, source_type, destination_type):
        """
        @param rf_rel:
        @rtype: list of RemedyForceRelationship
        """
        query = (
            "select Id"
            ", BMCServiceDesk__Source__c"
            ", BMCServiceDesk__Destination__c "
            " from BMCServiceDesk__BMC_BaseRelationship__c "
            " where BMCServiceDesk__ImpactDirection__c = '%s'"
            " and BMCServiceDesk__Source_ClassName__c = '%s'"
            " and BMCServiceDesk__Destination_ClassName__c = '%s'"
            % (rf_rel, source_type, destination_type))
        all_data = []
        for records in self.force_client.query(query):
            all_data += [RemedyForceRelationship(
                rf_rel, record['Id'],
                source_type, record['BMCServiceDesk__Source__c'],
                destination_type, record['BMCServiceDesk__Destination__c'])
                         for record in records]
        return all_data

    def getRelationshipById(self, rel_id, fields=None):
        return self.force_client.getObjectById(self.BMC_SERVICE_DESK_BMC_BASE_RELATIONSHIP, rel_id, fields)

    def getObjectById(self, object_id, fields=()):
        return self.force_client.getObjectById(self.BMC_SERVICE_DESK_BMC_BASE_ELEMENT, object_id, fields)

    def updateObject(self, object_id, data):
        debug('Update object by object id:', object_id)
        self.force_client.updateRecord(self.BMC_SERVICE_DESK_BMC_BASE_ELEMENT, object_id, data)

    def upsertObject(self, object_type, unique_id, data):
        debug('Upsert object by unique id:', unique_id)
        if not unique_id:
            raise Exception('No unique id.')
        data['BMCServiceDesk__CMDB_Class__c'] = self.getBmcClassMapping()[object_type]
        return self.force_client.upsertRecord(self.BMC_SERVICE_DESK_BMC_BASE_ELEMENT,
                                              'BMCServiceDesk__UniqueCISourceID__c',
                                              unique_id, data)

    def deleteObject(self, object_id):
        debug('Delete object by object id:', object_id)
        self.force_client.deleteRecord(self.BMC_SERVICE_DESK_BMC_BASE_ELEMENT, object_id)

    def getBmcClassMapping(self):
        if not self.BMC_CLASS_MAP:
            records_group = self.force_client.query(
                'select Id, BMCServiceDesk__ClassName__c from BMCServiceDesk__CMDB_Class__c')

            for records in records_group:
                class_map = ((x['BMCServiceDesk__ClassName__c'], x['Id']) for x in records)
                print class_map
                self.BMC_CLASS_MAP.update(class_map)
        return self.BMC_CLASS_MAP

    def insertObject(self, object_type, data, unique_name=None, unique_id=None):
        """
            BMCServiceDesk__Name__c is unique
            # "BMCServiceDesk__Name__c": "cs3",
            # "BMCServiceDesk__InstanceID__c": "xx" + str(time.time()),
            # "BMCServiceDesk__CMDB_Class__c": cmdb_class_id
        @param object_type:
        @param data:
        @return:
        """
        if object_type not in self.getBmcClassMapping():
            raise Exception('Unknown object type:' + object_type)

        if unique_name:
            data['BMCServiceDesk__Name__c'] = unique_name

        if 'BMCServiceDesk__Name__c' not in data:
            raise Exception('CI must have a [Name] attribute.')

        data['BMCServiceDesk__CMDB_Class__c'] = self.getBmcClassMapping()[object_type]
        if unique_id:
            data['BMCServiceDesk__InstanceID__c'] = unique_id

        return self.force_client.insertObject(self.BMC_SERVICE_DESK_BMC_BASE_ELEMENT, data)

    def __prepareRelationshipData(self, destination_id, fields, rel_type, source_id):
        data = {
            'BMCServiceDesk__ImpactDirection__c': rel_type,
            'BMCServiceDesk__Source__c': source_id,
            'BMCServiceDesk__Destination__c': destination_id,
        }
        if fields:
            data.update(fields)
        if 'BMCServiceDesk__Name__c' not in data:
            name = '%s-%s' % (source_id, destination_id)
            data['BMCServiceDesk__Name__c'] = name
        return data

    def insertRelationship(self, rel_type, source_id, destination_id, fields=None):
        """
        @param fields:

        """
        data = self.__prepareRelationshipData(destination_id, fields, rel_type, source_id)
        return self.force_client.insertObject(self.BMC_SERVICE_DESK_BMC_BASE_RELATIONSHIP, data)

    def upsertRelationship(self, rel_type, source_id, destination_id, unique_id, fields=None):
        """
        @param fields:

        """
        data = self.__prepareRelationshipData(destination_id, fields, rel_type, source_id)

        return self.force_client.upsertRecord(self.BMC_SERVICE_DESK_BMC_BASE_RELATIONSHIP,
                                              'BMCServiceDesk__UniqueCIRelationshipID__c', unique_id, data)

    def getObjectByUniqueId(self, unique_id):
        debug('Get object by unique id:', unique_id)
        query = "select Id from %s where  BMCServiceDesk__UniqueCISourceID__c = '%s'" % (
            self.BMC_SERVICE_DESK_BMC_BASE_ELEMENT, unique_id)
        records = self.force_client.query(query)
        for result in records:
            for data in result:
                return data['Id']  # it should be only one record if exists
        return None

    def getRelationshipByUniqueId(self, unique_id):
        debug('Get relationship by unique id:', unique_id)
        query = "select Id from %s where  BMCServiceDesk__UniqueCIRelationshipID__c = '%s'" % (
            self.BMC_SERVICE_DESK_BMC_BASE_RELATIONSHIP, unique_id)
        records = self.force_client.query(query)
        for result in records:
            for data in result:
                return data['Id']  # it should be only one record if exists
        return None

    def deleteRelationship(self, rel_id):
        debug('Delete relationship by rel_id id:', rel_id)
        self.force_client.deleteRecord(self.BMC_SERVICE_DESK_BMC_BASE_RELATIONSHIP, rel_id)
