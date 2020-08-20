# coding=utf-8
import os
import sys
from collections import defaultdict

from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector
from com.hp.ucmdb.discovery.library.common import CollectorsParameters

import logger
import modeling
from remedyforce_connection_manager import FrameworkBasedConnectionDataManager
from remedyforce_mapping_file_manager import RemedyforceMappingFileManager
from remedyforce_mapping_implementation import SimpleLink
from remedyforce_mapping_interfaces import AbstractSourceSystem, \
    AbstractTargetSystem, Ci, CiBuilder, LinkMappingProcessor
from remedyforce_mapping_interfaces import InvalidValueException

REMEDY_FORCE_PULL_CONFIG_FOLDER = 'RemedyforcePull'

DEBUG = False


class UcmdbTargetSystem(AbstractTargetSystem):
    class OshBuilder(CiBuilder):
        def __init__(self, targetCiType):
            self.__type = targetCiType
            self.__osh = ObjectStateHolder(self.__type)

        def setCiAttribute(self, name, value):
            attributeType = self.__getAttributeType(self.__type, name)
            if value:
                self.__setValue(name, attributeType, value)
            else:
                logger.debug("Meet none value for %s, type:%s" % (name, attributeType))

        def build(self):
            return self.__osh

        def __setValue(self, name, attributeType, value):
            if attributeType == 'string':
                if not isinstance(value, basestring):
                    value = str(value)
                self.__osh.setStringAttribute(name, value)
            elif attributeType == 'integer':
                self.__osh.setIntegerAttribute(name, int(value))
            elif attributeType.endswith('enum'):
                if isinstance(value, int):
                    self.__osh.setEnumAttribute(name, value)
                else:
                    self.__osh.setAttribute(name, value)
            elif attributeType == 'string_list':
                self.__osh.setListAttribute(name, value)
            else:
                raise ValueError('no setter defined for type %s' % attributeType)

        def __getAttributeType(self, ciType, attributeName):
            try:
                attributeDefinition = modeling._CMDB_CLASS_MODEL.getAttributeDefinition(ciType, attributeName)
                return attributeDefinition.getType()
            except:
                if DEBUG:
                    if attributeName in ['memory_size', 'port_index']:
                        return 'integer'
                    return 'string'
                logger.errorException("%s.%s" % (ciType, attributeName))
                raise ValueError("Failed to determine type of %s.%s" % (ciType, attributeName))

    def __init__(self):
        self._vector = ObjectStateHolderVector()
        self._cis = {}
        self._links = []
        self.sourceCIMap = {}

    def addCi(self, osh, sourceCi, sourceType):
        "@type: ObjectStateHolder, str, str"
        sourceCiId = sourceCi.getId()
        targetType = osh.getObjectClass()
        ciId = self.createComplexId(sourceCiId, sourceType, targetType)
        self._cis[ciId] = osh
        self.sourceCIMap[ciId] = sourceCi

    # logger.info('adding osh for %s' % ciId)

    def addLink(self, linkMapping, link):
        "@types: LinkMapping, Link"

        sourceType1 = linkMapping.getSourceEnd1Type()
        sourceType2 = linkMapping.getSourceEnd2Type()
        targetType1 = linkMapping.getTargetEnd1Type()
        targetType2 = linkMapping.getTargetEnd2Type()
        sourceId1 = link.getEnd1Id()
        sourceId2 = link.getEnd2Id()

        targetEnd1Id = self.createComplexId(sourceId1, sourceType1, targetType1)
        targetEnd2Id = self.createComplexId(sourceId2, sourceType2, targetType2)

        if not self.hasOsh(targetEnd1Id) or not self.hasOsh(targetEnd2Id):
            failurePolicy = linkMapping.getFailurePolicy()

            if failurePolicy == 'exclude_end1':
                self.excludeCi(targetEnd1Id)

            if failurePolicy == 'exclude_end2':
                self.excludeCi(targetEnd2Id)

            if failurePolicy == 'exclude_both':
                self.excludeCi(targetEnd1Id)
                self.excludeCi(targetEnd2Id)
        else:
            logger.info('adding %s -- %s --> %s' % (targetEnd1Id, linkMapping.getTargetType(), targetEnd2Id))
            self._links.append((linkMapping, link))

    def createCiBuilder(self, targetCiType):
        "@types: str -> OshBuilder"
        return UcmdbTargetSystem.OshBuilder(targetCiType)

    def getTopology(self):
        self.links_map = defaultdict(list)
        for (linkMapping, link) in self._links:
            targetType = linkMapping.getTargetType()
            sourceType1 = linkMapping.getSourceEnd1Type()
            sourceType2 = linkMapping.getSourceEnd2Type()
            sourceId1 = link.getEnd1Id()
            sourceId2 = link.getEnd2Id()
            targetType1 = linkMapping.getTargetEnd1Type()
            targetType2 = linkMapping.getTargetEnd2Type()
            isContainer = linkMapping.isContainer()

            targetEnd1Id = self.createComplexId(sourceId1, sourceType1, targetType1)
            targetEnd2Id = self.createComplexId(sourceId2, sourceType2, targetType2)

            msg = "%s -- %s --> %s" % (targetEnd1Id, targetType, targetEnd2Id)
            if self.hasOsh(targetEnd1Id) and self.hasOsh(targetEnd2Id):
                logger.info(msg)

                (osh1, osh2) = (self.getOsh(targetEnd1Id), self.getOsh(targetEnd2Id))
                if linkMapping.isReverse():
                    (osh1, osh2) = (osh2, osh1)

                link_osh = modeling.createLinkOSH(targetType, osh1, osh2)
                self.links_map[(osh1, osh2)].append(link_osh)
                if targetType == 'composition' or isContainer:
                    osh2.setContainer(osh1)
        import time
        t0 = time.time()
        self.get_validated_vector()
        logger.debug('Total time for validate ci:', time.time() - t0)
        return self._vector

    def get_validated_vector(self):
        validated_cis, discarded_cis = self.filter_valid_cis()
        logger.debug('len of validated_cis:', len(validated_cis))
        validated_links = set()
        for (osh1, osh2), link_list in self.links_map.iteritems():
            if osh1 in validated_cis and osh2 in validated_cis:
                validated_links.update(link_list)
        map(self._vector.add, validated_cis)
        map(self._vector.add, validated_links)
        logger.debug('Total vector size:', self._vector.size())

    def addCisWithRootContainer(self, need_container_cis, validated_cis, discarded_cis, osh):
        # if osh not in need_container_cis or osh in validated_cis:  # already in or doesn't need container
        if osh in validated_cis:  # already in or doesn't need container
            return True
        container = osh.getAttributeValue("root_container")
        if container and self.addCisWithRootContainer(need_container_cis, validated_cis, discarded_cis,
                                                      container):  # root container in, in too
            validated_cis.add(osh)
            return True
        else:
            discarded_cis.add(osh)
            return False

    def filter_valid_cis(self):
        need_container_cis = set()
        validated_cis = set()
        discarded_cis = set()
        self.needRelationshipCIs = {}
        for key, osh in self._cis.items():
            sourceCI = self.sourceCIMap[key]
            standalone = True
            if sourceCI.getMapping().needContainer():
                need_container_cis.add(osh)
                standalone = False
            if standalone:
                validated_cis.add(osh)

        logger.debug('Need container ci number:', len(need_container_cis))
        for osh in need_container_cis:
            self.addCisWithRootContainer(need_container_cis, validated_cis, discarded_cis, osh)
        return validated_cis, discarded_cis

    def excludeCi(self, complexId):
        if self.hasOsh(complexId):
            logger.info("Excluding %s" % complexId)
            del self._cis[complexId]

    def getOsh(self, complexId):
        return self._cis[complexId]

    def hasOsh(self, complexId):
        return complexId in self._cis

    def createComplexId(self, sourceId, sourceType, targetType):
        return targetType and "%s: %s_%s" % (targetType, sourceId, sourceType) or "%s_%s" % (sourceId, sourceType)


def getValueByExpression(obj, expression):
    exs = expression.split('.')
    tmpObj = obj
    for ex in exs:
        if tmpObj and ex in tmpObj:
            tmpObj = tmpObj[ex]
        else:
            return None
    return tmpObj


class SourceSystem(AbstractSourceSystem):
    class __Ci(Ci):
        def __init__(self, ciType, jsonCi, idKey=None, parent=None, mapping=None):
            self.__ciType = ciType
            self.__ci = jsonCi
            self.__idKey = idKey or 'Id'
            self.__parent = parent
            self.__id = None
            self.mapping = mapping
            self.children = {}

        def getId(self):
            if not self.__id:
                if self.__ci.has_key(self.__idKey):
                    self.__id = self.__ci[self.__idKey]
                else:
                    parent = self.getParent()
                    self.__id = hash('%s_%s' % (parent and parent.getId(), self.getObj()))
                self.__ci['__id'] = self.__id
            return self.__id

        def getType(self):
            return self.__ciType

        def getValue(self, name):
            return self.__ci[name]

        def __repr__(self):
            return self.__ciType + ':' + str(self.__ci)

        def getObj(self):
            return self.__ci

        def getParent(self):
            return self.__parent

        def getMapping(self):
            return self.mapping

    class RelationshipDriveMappingProcessor(LinkMappingProcessor):
        def __init__(self, client, linkMapping, sourceEnd1CIs, sourceEnd2CIs):
            """
            @type client: remedyforce_client.RemedyForceClient
            @param linkMapping:
            @param sourceEnd1CIs:
            @param sourceEnd2CIs:
            @return:
            """
            self.__client = client
            #     self.__refName, self.__refId = linkMapping.getReferenceAttribute().split('.')
            self.__sourceType = linkMapping.getSourceType()
            self.__sourceEnd1Type = linkMapping.getSourceEnd1Type()
            self.__sourceEnd2Type = linkMapping.getSourceEnd2Type()
            self.__isContainer = linkMapping.isContainer()
            self.__sourceEnd1Cis = sourceEnd1CIs
            self.__sourceEnd2CIs = sourceEnd2CIs

        def getLinks(self):
            links = []
            records = self.__client.getRelationshipsByTypeAndCIType(self.__sourceType, self.__sourceEnd1Type,
                                                                    self.__sourceEnd2Type)
            for record in records:
                link = SimpleLink(self.__sourceType, record.source_id, record.destination_id)
                links.append(link)
            return links

    class __CiCache:
        def __init__(self):
            self.__cache = {}

        def addCi(self, ciType, ci):
            # logger.debug("%s + %s" % (ciType, ciId))
            cache = self.__cache.get(ciType)
            if cache is None:
                cache = {}
                self.__cache[ciType] = cache
            cache[ci.getId()] = ci

        def getIdsByType(self, ciType):
            return self.__cache.get(ciType).keys()

        def getCisByType(self, ciType):
            return self.__cache.get(ciType)

    def __init__(self, connectionDataManager):
        self.__connectionDataManager = connectionDataManager
        self.__client = None
        self.__linkNameToId = {}
        self.__ciCache = SourceSystem.__CiCache()
        self.__linkCache = {}

    def __createClient(self):
        '''
        @param ciType:
        @return a
        @rtype : remedyforce_client.RemedyForceClient
        '''
        if not self.__client:
            ovc = self.__connectionDataManager.getClient()
            self.__client = ovc
        return self.__client

    def getCis(self, sourceCiType, ciMapping):
        logger.info('Get ci type:%s' % sourceCiType)
        query = ciMapping.getQuery()
        ref = ciMapping.getRef()
        base = ciMapping.getBase()
        idKey = ciMapping.getIdKey()
        client = self.__createClient()
        children = ciMapping.getChildren()

        records = []
        if ref:
            by_type = self.__ciCache.getCisByType(ref)
            if by_type:
                records = by_type.values()
        else:
            records = client.getCIsByType(sourceCiType, ciMapping.getRequiredFields(), query)
        cis = []
        parent = None
        relationship_map = self.get_children_relationship(sourceCiType, client, children)
        for record in records:
            ci = SourceSystem.__Ci(sourceCiType, record, idKey, parent, ciMapping)
            self.fill_ci_with_relations(client, relationship_map, ci, children)
            cis.append(ci)
            self.__ciCache.addCi(ci.getType(), ci)
        logger.info('Ci count:', len(cis))
        logger.debug('Cis:', cis)
        return cis

    def get_children_relationship(self, current_type, client, children):
        relation_map = {}
        for child in children:
            records = client.getRelationshipsByTypeAndCIType(child['relationship'], current_type,
                                                             child['related_ci_name'])
            source_to_dst = defaultdict(list)
            for record in records:
                source_to_dst[record.source_id].append(record.destination_id)
            relation_map[child['alias']] = source_to_dst
        return relation_map

    def fill_ci_with_relations(self, client, relationship_map, current_ci, relation_definition):
        for child in relation_definition:
            key = child['alias']
            all_rels = relationship_map[key]
            dst_ids = all_rels[current_ci.getId()]
            dsts = [client.getCIsByType(current_ci.getType()) for x in dst_ids]
            current_ci.children[key] = dsts

    def createLinkMappingProcessor(self, linkMapping):
        sourceEnd1Cis = self.__ciCache.getCisByType(linkMapping.getSourceEnd1Type())
        sourceEnd2Cis = self.__ciCache.getCisByType(linkMapping.getSourceEnd2Type())

        if sourceEnd1Cis is None:
            raise ValueError(
                    'No CIs of type %s found. Make sure mapping definition exists.' % linkMapping.getSourceEnd1Type())

        if sourceEnd2Cis is None:
            raise ValueError(
                    'No CIs of type %s found. Make sure mapping definition exists.' % linkMapping.getSourceEnd2Type())

        return SourceSystem.RelationshipDriveMappingProcessor(self.__createClient(), linkMapping, sourceEnd1Cis,
                                                              sourceEnd2Cis)


def replicateTopologyUsingMappingFile(mappingFile, connectionDataManager, mappingFileManager):
    sourceSystem = SourceSystem(connectionDataManager)
    ucmdbSystem = UcmdbTargetSystem()

    mapping = mappingFileManager.getMapping(mappingFile)
    replicateTopology(mapping, sourceSystem, ucmdbSystem)

    return ucmdbSystem.getTopology()


def replicateTopologyFromRemedyforce(connectionDataManager, mappingFileManager):
    sourceSystem = SourceSystem(connectionDataManager)
    ucmdbSystem = UcmdbTargetSystem()

    for mappingFile in mappingFileManager.getAvailableMappingFiles():
        mapping = mappingFileManager.getMapping(mappingFile)
        replicateTopology(mapping, sourceSystem, ucmdbSystem)

    return ucmdbSystem.getTopology()


def getMappingFileFromFramework(Framework):
    mappingFile = Framework.getParameter('Mapping file')
    mappingFile = mappingFile and mappingFile.strip()
    if mappingFile:
        if mappingFile.lower().endswith('.xml'):
            return mappingFile
        else:
            return "%s.xml" % mappingFile
    else:
        return None


def replicateTopology(mapping, sourceSystem, targetSystem):
    for ciMapping in mapping.getCiMappings():
        sourceType = ciMapping.getSourceType()
        logger.info('processing %s to %s' % (sourceType, ciMapping.getTargetType()))
        for sourceCi in sourceSystem.getCis(sourceType, ciMapping):
            try:
                targetCiBuilder = targetSystem.createCiBuilder(ciMapping.getTargetType())
                for attributeMapping in ciMapping.getAttributeMappings():
                    value = attributeMapping.getValue(sourceCi)

                    for filter in attributeMapping.getFilters():
                        value = filter.filter(value)

                    for validator in attributeMapping.getValidators():
                        validator.validate(value)
                    targetCiBuilder.setCiAttribute(attributeMapping.getTargetName(), value)
                targetCi = targetCiBuilder.build()
                targetSystem.addCi(targetCi, sourceCi, sourceType)
            except InvalidValueException:
                logger.info('%s CI %s skipped because %s' % (sourceType, sourceCi.getId(), sys.exc_info()[1]))

    for linkMapping in mapping.getLinkMappings():
        logger.info('processing link %s(%s) -- %s --> %s(%s)' % (
            linkMapping.getTargetEnd1Type(), linkMapping.getSourceEnd1Type(), linkMapping.getTargetType(),
            linkMapping.getTargetEnd2Type(), linkMapping.getSourceEnd2Type()))
        try:
            linkMappingProcessor = sourceSystem.createLinkMappingProcessor(linkMapping)
            for link in linkMappingProcessor.getLinks():
                logger.info("link:=====", link)
                targetSystem.addLink(linkMapping, link)
        except:
            logger.info('CI Links skipped because %s' % (sys.exc_info()[1]))


def DiscoveryMain(Framework):
    connectionDataManager = None
    try:
        logger.debug('Replicating topology from HP Remedyforce')

        connectionDataManager = FrameworkBasedConnectionDataManager(Framework)
        if not connectionDataManager.validate():
            return
        mappingFileFolder = os.path.join(CollectorsParameters.BASE_PROBE_MGR_DIR,
                                         CollectorsParameters.getDiscoveryConfigFolder(),
                                         REMEDY_FORCE_PULL_CONFIG_FOLDER)
        mappingFileManager = RemedyforceMappingFileManager(mappingFileFolder)

        mappingFile = getMappingFileFromFramework(Framework)
        if mappingFile:
            return replicateTopologyUsingMappingFile(os.path.join(mappingFileFolder, mappingFile),
                                                     connectionDataManager, mappingFileManager)
        else:
            return replicateTopologyFromRemedyforce(connectionDataManager, mappingFileManager)
    except:
        logger.errorException('Failed to pull data from Remedyforce.')
        exc_info = sys.exc_info()
        Framework.reportError('Failed to pull data from Remedyforce.')
        Framework.reportError(str(exc_info[1]))
    finally:
        if connectionDataManager:
            connectionDataManager.closeClient()
