# coding=utf-8
from xml.etree import ElementTree as ET

import logger

UNIQUE_ID = '__ID__'


class PushCIField(object):
    def __init__(self, key, value, dataType):
        super(PushCIField, self).__init__()
        self.key = key
        self.value = value
        self.dataType = dataType
        self.parsedValue = value

    def __repr__(self):
        return '(%s:%s)' % (self.key, self.value)


class PushEntity(object):
    EXCLUDE_FIELDS = ()

    def __init__(self, name, mamId, externalId):
        super(PushEntity, self).__init__()
        self.name = name
        self.mamId = mamId
        self.externalId = externalId
        self.fields = {}

    def getName(self):
        return self.name

    def getMamId(self):
        return self.mamId

    def getExternalId(self):
        return self.externalId

    def addField(self, key, value, dataType):
        if key == UNIQUE_ID:
            self.__ID__ = value
        else:
            self.fields[key] = PushCIField(key, value, dataType)

    def _getRawData(self):
        data = {}
        for key, field in self.fields.iteritems():
            data[key] = field.value
        return data

    def getData(self):
        data = {}
        for key, field in self.fields.iteritems():
            if key not in self.EXCLUDE_FIELDS and field.dataType != 'none':
                data[key] = field.parsedValue
        return data

    def parse(self):
        for field in self.fields.itervalues():
            if field.dataType == 'eval':
                field.parsedValue = self._parseExpression(field.value)
            elif field.dataType == 'function':
                field.parsedValue = self._parseFunction(field.value)

    def _parseExpression(self, expression):
        return eval(expression, {'CI': self._getRawData()})

    def _parseFunction(self, function):
        (module, method) = function.split('.')
        module = __import__(module)
        reload(module)
        return getattr(module, method)(self._getRawData())


class PushCI(PushEntity):
    def __init__(self, name, mamId, externalId):
        super(PushCI, self).__init__(name, mamId, externalId)
        self.name = name
        self.mamId = mamId
        self.externalId = externalId
        self.__ID__ = None

    def __repr__(self):
        return '%s[%s]' % (self.name, self.fields)

    def getId(self):
        return self.__ID__


class PushRelation(PushEntity):
    EXCLUDE_FIELDS = ('end1Id', 'end2Id', 'DiscoveryID1', 'DiscoveryID2')

    def __init__(self, name, parent_name, child_name, mam_id, external_id):
        super(PushRelation, self).__init__(name, mam_id, external_id)
        self.parent_name = parent_name
        self.child_name = child_name

    def getId(self):
        return self.mamId

    def getExternalCIEnd1(self):
        return self.fields['end1Id'].value

    def getExternalCIEnd2(self):
        return self.fields['end2Id'].value

    def getCIEnd1(self):
        return self.fields['DiscoveryID1'].value

    def getCIEnd2(self):
        return self.fields['DiscoveryID2'].value

    def __repr__(self):
        return '{%s:%s}' % (self.name, self.fields)


def parse(content):
    """
    @param content:
    @return:
    @rtype (list of PushCI, list of PushRelation)
    """
    root = ET.fromstring(content)
    objects = root.findall('./data/objects/Object')
    cis = set()
    ci_mamid_mapping = {}
    for obj in objects:
        mode = obj.get('mode')
        if mode == 'ignore':
            continue
        name = obj.get('name')
        mamId = obj.get('mamId')
        operation = obj.get('operation')
        externalId = obj.get('id')
        fields = obj.findall('field')
        ci = PushCI(name, mamId, externalId)
        ci_key_valid = True
        if operation != 'delete':  # delete operation doesn't have any data in field direct mapping
            for field in fields:
                field_name = field.get('name')
                data_type = field.get('datatype')
                is_key = field.get('key') == 'true'
                value = field.text
                if value is not None:
                    ci.addField(field_name, value, data_type)
                elif is_key:
                    logger.debug('Key attribute %s is empty, ignore the ci' % field_name)
                    ci_key_valid = False
                    break
        if ci_key_valid:
            ci.parse()
            cis.add(ci)
            ci_mamid_mapping[ci.mamId] = ci

    links = root.findall('./data/links/link')
    relations = set()
    for link in links:
        mode = link.get('mode')
        if mode == 'ignore':
            continue
        operation = link.get('operation')
        name = link.get('targetRelationshipClass')
        parent = link.get('targetParent')
        child = link.get('targetChild')
        mamId = link.get('mamId')
        externalId = link.get('id')
        fields = link.findall('field')

        rel = PushRelation(name, parent, child, mamId, externalId)
        if operation != 'delete':  # delete operation doesn't have any data in field direct mapping
            for field in fields:
                field_name = field.get('name')
                value = field.text
                data_type = field.get('datatype')
                if value is not None:
                    rel.addField(field_name, value, data_type)
            if rel.getCIEnd1() in ci_mamid_mapping:
                rel.addField('__END1__', ci_mamid_mapping[rel.getCIEnd1()]._getRawData(), 'none')
            if rel.getCIEnd2() in ci_mamid_mapping:
                rel.addField('__END2__', ci_mamid_mapping[rel.getCIEnd2()]._getRawData(), 'none')
            rel.parse()
        relations.add(rel)

    return cis, relations
