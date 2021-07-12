# coding=utf-8
import os
import sys
from collections import defaultdict

import modeling
import logger
from ucs_connection_data_manager import FrameworkBasedConnectionDataManager
from ucs_mapping_interfaces import AbstractSourceSystem, \
    AbstractTargetSystem, Ci, CiBuilder, LinkMappingProcessor
from ucs_mapping_implementation import SimpleLink
from ucs_mapping_file_manager import UCSMappingFileManager
from ucs_mapping_interfaces import InvalidValueException
from appilog.common.system.types.vectors import ObjectStateHolderVector
from appilog.common.system.types import ObjectStateHolder
from com.hp.ucmdb.discovery.library.common import CollectorsParameters


MAPPING_CONFIG_FOLDER = 'cisco_ucs'

MAPPING_FILE = 'ucs_mapping.xml'

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
                self.__osh.setStringAttribute(name, str(value))
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
                raise ValueError("Failed to determine type of %s.%s" % (ciType, attributeName))

    def __init__(self):
        self.__vector = ObjectStateHolderVector()
        self.__cis = {}
        self.__links = []
        self.sourceCIMap = {}

    def addCi(self, osh, sourceCi, sourceType):
        "@type: ObjectStateHolder, str, str"
        sourceCiId = sourceCi.getId()
        targetType = osh.getObjectClass()
        ciId = self.__createComplexId(sourceCiId, sourceType, targetType)
        self.__cis[ciId] = osh
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

        targetEnd1Id = self.__createComplexId(sourceId1, sourceType1, targetType1)
        targetEnd2Id = self.__createComplexId(sourceId2, sourceType2, targetType2)

        if not self.__hasOsh(targetEnd1Id) or not self.__hasOsh(targetEnd2Id):
            failurePolicy = linkMapping.getFailurePolicy()

            if failurePolicy == 'exclude_end1':
                self.__excludeCi(targetEnd1Id)

            elif failurePolicy == 'exclude_end2':
                self.__excludeCi(targetEnd2Id)

            elif failurePolicy == 'exclude_both':
                self.__excludeCi(targetEnd1Id)
                self.__excludeCi(targetEnd2Id)
        else:
            logger.info('adding %s -- %s --> %s' % (targetEnd1Id, linkMapping.getTargetType(), targetEnd2Id))
            self.__links.append((linkMapping, link))

    def createCiBuilder(self, targetCiType):
        "@types: str -> OshBuilder"
        return UcmdbTargetSystem.OshBuilder(targetCiType)

    def getTopology(self):
        self.linksMap = defaultdict(list)
        for (linkMapping, link) in self.__links:
            targetType = linkMapping.getTargetType()
            sourceType1 = linkMapping.getSourceEnd1Type()
            sourceType2 = linkMapping.getSourceEnd2Type()
            sourceId1 = link.getEnd1Id()
            sourceId2 = link.getEnd2Id()
            targetType1 = linkMapping.getTargetEnd1Type()
            targetType2 = linkMapping.getTargetEnd2Type()
            isContainer = linkMapping.isContainer()

            targetEnd1Id = self.__createComplexId(sourceId1, sourceType1, targetType1)
            targetEnd2Id = self.__createComplexId(sourceId2, sourceType2, targetType2)

            msg = "%s -- %s --> %s" % (targetEnd1Id, targetType, targetEnd2Id)
            logger.warn(msg)
            if self.__hasOsh(targetEnd1Id) and self.__hasOsh(targetEnd2Id):
                logger.info(msg)

                (osh1, osh2) = (self.__getOsh(targetEnd1Id), self.__getOsh(targetEnd2Id))
                if linkMapping.isReverse():
                    (osh1, osh2) = (osh2, osh1)

                link_osh = modeling.createLinkOSH(targetType, osh1, osh2)
                self.__vector.add(link_osh)
                self.linksMap[osh1].append(link_osh)
                self.linksMap[osh2].append(link_osh)
                if targetType == 'composition' or isContainer:
                    osh2.setContainer(osh1)

        self.addValidCis()

        return self.__vector

    def addCisWithRootContainer(self, osh):
        if self.__vector.contains(osh) or osh not in self.needContainerCIs:  # already in or doesn't need container
            return True
        rootContainer = osh.getAttributeValue("root_container")
        if rootContainer and self.addAllDependencies(rootContainer):  # root container in, in too
            self.__vector.add(osh)
            return True
        else:
            logger.debug('No root container for osh', osh)
            if osh in self.linksMap:
                links = self.linksMap[osh]
                logger.debug("Remove links of isolated CIs")
                for link in links:
                    self.__vector.remove(link)
            return False

    def addCisWithDependency(self, osh):
        if self.__vector.contains(osh) or osh not in self.needRelationshipCIs:  # already in or doesn't need dependency
            return True
        deps = self.needRelationshipCIs[osh].split(',')
        allFound = True
        for dep in deps:
            relationship, targetCIType = dep.split(':')
            singleFound = False
            if osh in self.linksMap:
                links = self.linksMap[osh]
                logger.debug("Get link of target ci", osh)

                for link in links:
                    if link.getObjectClass() == relationship:
                        end1 = link.getAttributeValue('link_end1')
                        end2 = link.getAttributeValue('link_end2')
                        that = end1 if end2 == osh else end2
                        if that.getObjectClass() == targetCIType:
                            singleFound = self.addAllDependencies(that)
                            if singleFound:
                                break
            if not singleFound:
                allFound = False
                break
        if allFound:
            self.__vector.add(osh)
        else:
            for link in self.linksMap[osh]:
                self.__vector.remove(link)
        return allFound

    def addAllDependencies(self, targetOsh):
        return self.addCisWithRootContainer(targetOsh) and self.addCisWithDependency(targetOsh)

    def addValidCis(self):
        self.needContainerCIs = []
        self.needRelationshipCIs = {}

        for key, osh in self.__cis.items():
            sourceCI = self.sourceCIMap[key]
            standalone = True
            if sourceCI.getMapping().needContainer():
                self.needContainerCIs.append(osh)
                standalone = False
            if sourceCI.getMapping().needRelationship():
                logger.debug('Need relationship:', sourceCI.getMapping().needRelationship())
                self.needRelationshipCIs[osh] = sourceCI.getMapping().needRelationship()
                standalone = False
            if standalone:
                self.__vector.add(osh)

        allDependencyCIs = []
        allDependencyCIs.extend(self.needContainerCIs)
        allDependencyCIs.extend(self.needRelationshipCIs.keys())
        for osh in allDependencyCIs:
            self.addAllDependencies(osh)

    def __excludeCi(self, complexId):
        if self.__hasOsh(complexId):
            logger.info("Excluding %s" % complexId)
            del self.__cis[complexId]

    def __getOsh(self, complexId):
        return self.__cis[complexId]

    def __hasOsh(self, complexId):
        return complexId in self.__cis.keys()

    def __createComplexId(self, sourceId, sourceType, targetType):
        return targetType and "%s: %s_%s" % (targetType, sourceId, sourceType) or "%s_%s" % (sourceId, sourceType)


def getValueByExpression(obj, expression):
    exs = expression.split('.')
    tmpObj = obj
    for ex in exs:
        if tmpObj and ex in tmpObj:
            tmpObj = tmpObj[ex]
        elif tmpObj and hasattr(tmpObj, ex):
            tmpObj = getattr(tmpObj, ex)
        else:
            return None
    return tmpObj


class SourceSystem(AbstractSourceSystem):
    class __Ci(Ci):
        def __init__(self, ciType, jsonCi, idKey=None, parent=None, mapping=None):
            self.__ciType = ciType
            self.__ci = jsonCi
            self.__idKey = idKey or 'dn'
            self.__parent = parent
            self.__id = None
            self.mapping = mapping

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

    class __ParentToChildLinkMappingProcessor(LinkMappingProcessor):
        MAPPING = '__ParentToChild'

        def __init__(self, client, linkMapping, sourceEnd1CIs, sourceEnd2CIs):
            self.__client = client
            self.__refName, self.__refId = linkMapping.getReferenceAttribute().split('.')
            self.__sourceEnd1Type = linkMapping.getSourceEnd1Type()
            self.__sourceEnd2Type = linkMapping.getSourceEnd2Type()
            self.__isContainer = linkMapping.isContainer()
            self.__sourceEnd1Cis = sourceEnd1CIs
            self.__sourceEnd2CIs = sourceEnd2CIs

        def getLinks(self):
            links = []
            for sourceEntity in self.__sourceEnd1Cis.values():
                obj = sourceEntity.getObj()
                children = obj[self.__refName]
                for child in children:
                    if child[self.__refId] in self.__sourceEnd2CIs:
                        childEntity = self.__sourceEnd2CIs[child[self.__refId]]
                        link = SimpleLink(self.MAPPING, sourceEntity.getId(), childEntity.getId(), self.__isContainer)
                        links.append(link)
                    else:
                        logger.info('skip non exists key: %s' % child[self.__refId])
            return links

    class __ChildToParentLinkMappingProcessor(LinkMappingProcessor):
        MAPPING = '__ChildToParent'

        def __init__(self, client, linkMapping, sourceEnd1CIs, sourceEnd2CIs):
            self.__client = client
            self.__refName = linkMapping.getReferenceAttribute() or 'dn'
            self.__sourceEnd1Type = linkMapping.getSourceEnd1Type()
            self.__sourceEnd2Type = linkMapping.getSourceEnd2Type()
            self.__isContainer = linkMapping.isContainer()
            self.__parentLevel = linkMapping.getParentLevel()

            self.__sourceEnd1Cis = sourceEnd1CIs
            self.__sourceEnd2CIs = sourceEnd2CIs

        def getParentId(self, cid):
            index = 1
            tmpId = cid
            while index <= self.__parentLevel:
                x = tmpId.rfind('/')
                if x != -1:
                    tmpId = tmpId[:x]
                    index += 1
                else:
                    break
            return tmpId

        def getLinks(self):
            links = []
            for childEntity in self.__sourceEnd2CIs.values():
                cid = childEntity.getValue(self.__refName)
                if not cid:
                    continue
                pid = self.getParentId(cid)
                if not pid or pid == cid:
                    continue
                link = SimpleLink(self.MAPPING, pid, childEntity.getId(), self.__isContainer)
                links.append(link)
            return links

    class __PeerToPeerLinkMappingProcessor(LinkMappingProcessor):
        MAPPING = '__PeerToPeer'

        def __init__(self, client, linkMapping, sourceEnd1CIs, sourceEnd2CIs):
            self.__client = client
            self.__refName = linkMapping.getReferenceAttribute() or 'peerDn'
            self.__sourceEnd1Type = linkMapping.getSourceEnd1Type()
            self.__sourceEnd2Type = linkMapping.getSourceEnd2Type()
            self.__isContainer = linkMapping.isContainer()
            self.__parentLevel = linkMapping.getParentLevel()

            self.__sourceEnd1CIs = sourceEnd1CIs
            self.__sourceEnd2CIs = sourceEnd2CIs

        def getPeerId(self, cid):
            index = 1
            tmpId = cid
            while index <= self.__parentLevel:
                x = tmpId.rfind('/')
                if x != -1:
                    tmpId = tmpId[:x]
                    index += 1
                else:
                    break
            return tmpId

        def getLinks(self):
            links = []
            for childEntity in self.__sourceEnd1CIs.values():
                cid = childEntity.getValue(self.__refName)
                if not cid:
                    continue
                pid = self.getPeerId(cid)
                if not pid:
                    continue
                link = SimpleLink(self.MAPPING, childEntity.getId(), pid, self.__isContainer)
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
        @rtype : UCSClient
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
        results = []
        if ref:
            by_type = self.__ciCache.getCisByType(ref)
            if by_type:
                results = by_type.values()
        elif query:
            queryResult = client.getByClass(query)
            if isinstance(queryResult, list):
                results = queryResult
            elif queryResult is not None:
                results = [queryResult]
            else:
                logger.info('queryResult is None')
                results = []
        else:
            raise Exception("No data source, need query or ref")
        cis = []
        parent = None
        for result in results:
            if ref:
                parent = result
                if query:
                    refQuery = query % parent.getObj()
                    result = client.getByClass(refQuery)
            if base:
                if isinstance(result, SourceSystem.__Ci):
                    obj = result.getObj()
                else:
                    obj = result
                records = getValueByExpression(obj, base)
            elif isinstance(result, list):
                records = result
            elif result:
                records = [result]
            else:
                records = []

            records = records or []
            for record in records:
                ci = SourceSystem.__Ci(sourceCiType, record, idKey, parent, ciMapping)
                cis.append(ci)
                self.__ciCache.addCi(ci.getType(), ci)
        return cis


    def createLinkMappingProcessor(self, linkMapping):
        sourceType = linkMapping.getSourceType()
        sourceEnd1Cis = self.__ciCache.getCisByType(linkMapping.getSourceEnd1Type())
        sourceEnd2Cis = self.__ciCache.getCisByType(linkMapping.getSourceEnd2Type())

        if sourceEnd1Cis is None:
            raise ValueError(
                'No CIs of type %s found. Make sure mapping definition exists.' % linkMapping.getSourceEnd1Type())

        if sourceEnd2Cis is None:
            raise ValueError(
                'No CIs of type %s found. Make sure mapping definition exists.' % linkMapping.getSourceEnd2Type())

        if sourceType == SourceSystem.__ParentToChildLinkMappingProcessor.MAPPING:
            return SourceSystem.__ParentToChildLinkMappingProcessor(self.__createClient(),
                                                                    linkMapping,
                                                                    sourceEnd1Cis,
                                                                    sourceEnd2Cis)
        elif sourceType == SourceSystem.__ChildToParentLinkMappingProcessor.MAPPING:
            return SourceSystem.__ChildToParentLinkMappingProcessor(self.__createClient(),
                                                                    linkMapping,
                                                                    sourceEnd1Cis,
                                                                    sourceEnd2Cis)
        elif sourceType == SourceSystem.__PeerToPeerLinkMappingProcessor.MAPPING:
            return SourceSystem.__PeerToPeerLinkMappingProcessor(self.__createClient(),
                                                                 linkMapping,
                                                                 sourceEnd1Cis,
                                                                 sourceEnd2Cis)

        else:
            raise Exception('Unrecognized reference:%s' % sourceType)


def replicateTopologyUsingMappingFile(mappingFile, connectionDataManager, mappingFileManager):
    sourceSystem = SourceSystem(connectionDataManager)
    ucmdbSystem = UcmdbTargetSystem()

    mapping = mappingFileManager.getMapping(mappingFile)
    replicateTopology(mapping, sourceSystem, ucmdbSystem)

    return ucmdbSystem.getTopology()


def replicateTopologyFromucs(connectionDataManager, mappingFileManager):
    sourceSystem = SourceSystem(connectionDataManager)
    ucmdbSystem = UcmdbTargetSystem()

    for mappingFile in mappingFileManager.getAvailableMappingFiles():
        mapping = mappingFileManager.getMapping(mappingFile)
        replicateTopology(mapping, sourceSystem, ucmdbSystem)

    return ucmdbSystem.getTopology()


def getMappingFileFromFramework(Framework):
    mappingFile = Framework.getParameter('Mapping file') or MAPPING_FILE
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
                    try:
                        value = attributeMapping.getValue(sourceCi)
                    except KeyError, e:
                        logger.info('target missing attr: %s' % e)
                        value = "Unknown"

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


def discovery(Framework, connectionManager=None):
    connectionDataManager = connectionManager
    try:
        logger.debug('Replicating topology from HP ucs')
        if not connectionDataManager:
            connectionDataManager = FrameworkBasedConnectionDataManager(Framework)
            if not connectionDataManager.validate():
                return
        mappingFileFolder = os.path.join(CollectorsParameters.BASE_PROBE_MGR_DIR,
                                         CollectorsParameters.getDiscoveryConfigFolder(), MAPPING_CONFIG_FOLDER)
        mappingFileManager = UCSMappingFileManager(mappingFileFolder)

        mappingFile = getMappingFileFromFramework(Framework)
        if mappingFile:
            return replicateTopologyUsingMappingFile(os.path.join(mappingFileFolder, mappingFile),
                                                     connectionDataManager, mappingFileManager)
        else:
            Framework.reportError('No mapping file found.')
            logger.errorException("No mapping file found.")
    except Exception, e:
        Framework.reportError('%s' % e)
        logger.errorException('%s' % e)
    except:
        Framework.reportError('Failed to pull data from ucs.')
        logger.errorException('Failed to pull data from ucs.')
    finally:
        if connectionDataManager:
            connectionDataManager.closeClient()