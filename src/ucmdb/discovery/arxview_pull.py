# coding=utf-8
import os
import sys
import modeling
import logger
from collections import defaultdict
from arxview_connection_data_manager import FrameworkBasedConnectionDataManager
from arxview_mapping_interfaces import AbstractSourceSystem, \
    AbstractTargetSystem, Ci, CiBuilder, LinkMappingProcessor
from arxview_mapping_implementation import SimpleLink
from arxview_mapping_file_manager import ArxviewMappingFileManager
from arxview_mapping_interfaces import InvalidValueException

from appilog.common.system.types.vectors import ObjectStateHolderVector
from appilog.common.system.types import ObjectStateHolder
from com.hp.ucmdb.discovery.library.common import CollectorsParameters

from datetime import datetime

ARXVIEW_CONFIG_FOLDER = 'Arxview'

ARXVIEW_MAPPING_FILE = 'arxview.xml'

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
            elif attributeType == 'double':
                    self.__osh.setDoubleAttribute(name, float(value))
            elif attributeType == 'boolean':
                    self.__osh.setBoolAttribute(name, bool(value))
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
                    return 'string'
                logger.errorException("%s.%s" % (ciType, attributeName))
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

            if failurePolicy == 'exclude_end2':
                self.__excludeCi(targetEnd2Id)

            if failurePolicy == 'exclude_both':
                self.__excludeCi(targetEnd1Id)
                self.__excludeCi(targetEnd2Id)
        else:
            #logger.info('adding %s -- %s --> %s' % (targetEnd1Id, linkMapping.getTargetType(), targetEnd2Id))
            self.__links.append((linkMapping, link))
            

    def createCiBuilder(self, targetCiType):
        "@types: str -> OshBuilder"
        return UcmdbTargetSystem.OshBuilder(targetCiType)

    def getTopology(self):
        self.linksMap = defaultdict(list)
        
        tstart = datetime.now()
        count = 0
        for (linkMapping, link) in self.__links:
            count = count + 1
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
            logger.debug(msg)
            if self.__hasOsh(targetEnd1Id) and self.__hasOsh(targetEnd2Id):
                #logger.info(msg)

                (osh1, osh2) = (self.__getOsh(targetEnd1Id), self.__getOsh(targetEnd2Id))
                if linkMapping.isReverse():
                    (osh1, osh2) = (osh2, osh1)

                link_osh = modeling.createLinkOSH(targetType, osh1, osh2)
                self.__vector.add(link_osh)
                self.linksMap[osh1].append(link_osh)
                self.linksMap[osh2].append(link_osh)
                if targetType == 'composition' or isContainer:
                    osh2.setContainer(osh1)
        
        
        logger.info( ">>>> getTopology Link Count:%s - Time:%s >>>> " % (count,(datetime.now() - tstart)) )
        #self.addValidCis()
                                           
        tstart = datetime.now()
        
        count = 0
        countNeedContainer = 0
        countBad = 0
        for key, osh in self.__cis.items():
            count = count + 1 
            sourceCI = self.sourceCIMap[key]
            if sourceCI.getMapping().needContainer():
                countNeedContainer = countNeedContainer + 1
                rootContainer = osh.getAttributeValue("root_container")
                if rootContainer:
                    self.__vector.add(osh)
                else:
                    countBad = countBad + 1;                    
            else:
                self.__vector.add(osh)
        logger.info( "getTopology CI Count:%s/%s/%s - Time:%s" % (countNeedContainer, count, countBad, (datetime.now() - tstart)) )
        return self.__vector


    def __excludeCi(self, complexId):
        if self.__hasOsh(complexId):
            logger.info("Excluding %s" % complexId)
            del self.__cis[complexId]

    def __getOsh(self, complexId):
        return self.__cis[complexId]

#    def __hasOsh(self, complexId):
#        return complexId in self.__cis.keys()

    def __hasOsh(self, complexId):
        try:
            return self.__cis.has_key(complexId)
        except KeyError:
            return 0


    def __createComplexId(self, sourceId, sourceType, targetType):
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
            self.__idKey = idKey or 'arx_asset_id'
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
            
            self.__sourceEnd2CisDict = {}
            for sourceEnd2Ci in self.__sourceEnd2Cis.values():
                self.__sourceEnd2CisDict[sourceEnd2Ci.getValue(self.__refName)] = sourceEnd2Ci            

        def getLinks(self):
            links = []
            for childEntity in self.__sourceEnd1CIs.values(): 
                parentLink = childEntity.getValue(self.__refId)       
                parentEntity = self.__sourceEnd2CisDict[parentLink]
                link = SimpleLink(self.MAPPING, parentEntity.getId(), childEntity.getId(), self.__isContainer)
                #print ">>>>> %s - %s - %s" % (self.MAPPING, parentEntity.getId(), childEntity.getId())
                logger.debug(">>>>> %s - %s - %s" % (self.MAPPING, parentEntity.getId(), childEntity.getId()))    
                links.append(link)
            return links


    class __ChildToParentLinkMappingProcessor(LinkMappingProcessor):
        MAPPING = '__ChildToParent'

        def __init__(self, client, linkMapping, sourceEnd1CIs, sourceEnd2CIs):
            self.__client = client
            self.__sourceEnd1Type = linkMapping.getSourceEnd1Type()
            self.__sourceEnd2Type = linkMapping.getSourceEnd2Type()
            self.__isContainer = linkMapping.isContainer()
            self.__refName, self.__refId = linkMapping.getReferenceAttribute().split('.')

            self.__sourceEnd1Cis = sourceEnd1CIs
            self.__sourceEnd2CIs = sourceEnd2CIs
            
            self.__sourceEnd1CisDict = {}
            for sourceEnd1Ci in self.__sourceEnd1Cis.values():
                self.__sourceEnd1CisDict[sourceEnd1Ci.getValue(self.__refName)] = sourceEnd1Ci

        def getLinks(self):
            links = []
            for childEntity in self.__sourceEnd2CIs.values(): 
                parentLink = childEntity.getValue(self.__refId)
                if self.__sourceEnd1CisDict.has_key(parentLink):
                    parentEntity = self.__sourceEnd1CisDict[parentLink]
                    link = SimpleLink(self.MAPPING, parentEntity.getId(), childEntity.getId(), self.__isContainer)
                    links.append(link)    
                else:
                    logger.debug("Link ignored for CIs type: %s - id: %s" % (self.__sourceEnd1Type, parentLink))
            return links


    class __MappingMappingProcessor(LinkMappingProcessor):
        MAPPING = '__Mapping'

        def __init__(self, client, linkMapping, sourceEnd1CIs, sourceEnd2CIs, sourceMappingCis ):
            self.__client = client
            self.__sourceEnd1Type = linkMapping.getSourceEnd1Type()
            self.__sourceEnd2Type = linkMapping.getSourceEnd2Type()
            self.__isContainer = linkMapping.isContainer()
            self.__refName, self.__refId = linkMapping.getReferenceAttribute().split('.')       
            
            self.__mappingAttributeName1, self.__mappingAttributeName2 = linkMapping.getMappingAttribute().split('.')
            self.__sourceMappingCis = sourceMappingCis
            
            self.__sourceEnd1Cis = sourceEnd1CIs
            self.__sourceEnd2Cis = sourceEnd2CIs   
            
            self.__sourceEnd1CisDict = {}
            for sourceEnd1Ci in self.__sourceEnd1Cis.values():
                self.__sourceEnd1CisDict[sourceEnd1Ci.getValue(self.__refName)] = sourceEnd1Ci      
                
            self.__sourceEnd2CisDict = {}
            for sourceEnd2Ci in self.__sourceEnd2Cis.values():
                self.__sourceEnd2CisDict[sourceEnd2Ci.getValue(self.__refId)] = sourceEnd2Ci                

        def getLinks(self):
            links = []
            for mappingEntity in self.__sourceMappingCis.values(): 
                linkId1 =  mappingEntity.getValue(self.__mappingAttributeName1);
                linkId2 =  mappingEntity.getValue(self.__mappingAttributeName2);  
                
                if self.__sourceEnd1CisDict.has_key(linkId1) and self.__sourceEnd2CisDict.has_key(linkId2) :
                    link = SimpleLink(self.MAPPING, self.__sourceEnd1CisDict[linkId1].getId(), self.__sourceEnd2CisDict[linkId2].getId(), self.__isContainer)
                    links.append(link)
                else:
                    logger.debug("Link ignored for CIs type: %s - id: %s" % (linkId1, linkId2))
            return links

    class __CiCache:
        def __init__(self):
            self.__cache = {}

        def addCi(self, ciType, ci):
            # logger.debug("%s + %s" % (ciType, ciId)) 
            #print "****** %s + %s" % (ciType, ci)
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
        @rtype : ArxviewClient
        '''
        if not self.__client:
            avc = self.__connectionDataManager.getClient()
            self.__client = avc
        return self.__client


    def getCis(self, sourceCiType, ciMapping):
        logger.info('Get ci type:%s' % sourceCiType)
        query = ciMapping.getQuery()
        ref = ciMapping.getRef()
        base = ciMapping.getBase()
        idKey = ciMapping.getIdKey()
        parentType = ciMapping.getParentType()
        parentQuery = ciMapping.getParentQuery()
        client = self.__createClient()
        results = []
        if ref:
            by_type = self.__ciCache.getCisByType(ref)
            if by_type:
                results = by_type.values()
        elif query:
            #queryResult = client.get(query) 
            queryResult = client.call(query)
            if isinstance(queryResult, list):
                results = queryResult
            else:
                results = [queryResult]
        else:
            raise Exception("No data source, need query or ref")

        if parentType and parentQuery:
            for result in results:
                child_id = result['arx_asset_id']
                pquery = parentQuery + '&' + 'child_arx_asset_id=' + child_id + '&' + 'assettype_name=' + parentType
                pqueryResult = client.call(pquery)
                if isinstance(queryResult, list):
                    presults = pqueryResult
                else:
                    presults = [pqueryResult]
                if len(presults) == 1:
                    presult = presults[0]
                    result['parent_arx_asset_id'] = presult['arx_asset_id']

        cis = []
        parent = None
        for result in results:
            #print "***** result: %s" % result
            if ref:
                result = result.getObj()
                if query:
                    refQuery = query % parent.getObj()
                    result = client.call(refQuery)
            if base:
                if isinstance(result, SourceSystem.__Ci):
                    obj = result.getObj()
                else:
                    obj = result
                records = getValueByExpression(obj, base)
            elif isinstance(result, list):
                records = result
            else:
                records = [result]

            records = records or []
            for record in records:
                #print "record: %s" % record
                ci = SourceSystem.__Ci(sourceCiType, record, idKey, parent, ciMapping)
                cis.append(ci)
                self.__ciCache.addCi(ci.getType(), ci)
        logger.info('Ci count:', len(cis))
        logger.debug('Cis:', cis)
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
        elif sourceType == SourceSystem.__MappingMappingProcessor.MAPPING:
            mappinmgCis = self.__ciCache.getCisByType(linkMapping.getSourceMappingType())
            if mappinmgCis is None:
                raise ValueError( 'No Mapping CIs of type %s found. Make sure mapping definition exists.' % linkMapping.getSourceMappingType())
            return SourceSystem.__MappingMappingProcessor(self.__createClient(),
                                                                    linkMapping,
                                                                    sourceEnd1Cis,
                                                                    sourceEnd2Cis, mappinmgCis )
        else:
            raise Exception('Unrecognized reference:%s' % sourceType)


def replicateTopologyUsingMappingFile(mappingFile, connectionDataManager, mappingFileManager):
    sourceSystem = SourceSystem(connectionDataManager)
    ucmdbSystem = UcmdbTargetSystem()

    mapping = mappingFileManager.getMapping(mappingFile)
    replicateTopology(mapping, sourceSystem, ucmdbSystem)

    return ucmdbSystem.getTopology()


def replicateTopologyFromArxview(connectionDataManager, mappingFileManager):
    sourceSystem = SourceSystem(connectionDataManager)
    ucmdbSystem = UcmdbTargetSystem()

    for mappingFile in mappingFileManager.getAvailableMappingFiles():
        mapping = mappingFileManager.getMapping(mappingFile)
        replicateTopology(mapping, sourceSystem, ucmdbSystem)

    return ucmdbSystem.getTopology()


def getMappingFileFromFramework(Framework):
    mappingFile = Framework.getParameter('Mapping file') or ARXVIEW_MAPPING_FILE
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
        logger.info('Processing %s' % sourceType)
        for sourceCi in sourceSystem.getCis(sourceType, ciMapping):
            try:
                targetCiBuilder = targetSystem.createCiBuilder(ciMapping.getTargetType())
                for attributeMapping in ciMapping.getAttributeMappings():
                    value = attributeMapping.getValue(sourceCi)              
                    for validator in attributeMapping.getValidators():
                        #print "Validator-Before (%s): %s" % (attributeMapping.getSourceName(), value)
                        #value = validator.validate(value) 
                        validator.validate(value)
                        #print "Validator-After (%s): %s" % (attributeMapping.getSourceName(), value)
                    targetCiBuilder.setCiAttribute(attributeMapping.getTargetName(), value)
                targetCi = targetCiBuilder.build()       
                if ciMapping.getTargetType() != "NONE":
                    targetSystem.addCi(targetCi, sourceCi, sourceType)
            except InvalidValueException:
                logger.debug('%s CI skipped because %s' % (sourceType, sys.exc_info()[1]))

    for linkMapping in mapping.getLinkMappings():
        logger.info('processing link %s -- %s --> %s' % ( linkMapping.getTargetEnd1Type(), linkMapping.getTargetType(), linkMapping.getTargetEnd2Type()))
        #print "Processing source link %s -- %s --> %s" % ( linkMapping.getTargetEnd1Type(), linkMapping.getTargetType(), linkMapping.getTargetEnd2Type())
        try:                                  
            linkMappingProcessor = sourceSystem.createLinkMappingProcessor(linkMapping)
            links = linkMappingProcessor.getLinks()     
            total = len(links)
            count = 0 
            #logger.info('processing link %s -- %s --> %s (%s)' % ( linkMapping.getTargetEnd1Type(), linkMapping.getTargetType(), linkMapping.getTargetEnd2Type(), total))
            for link in links:
                if (count % 5000) == 0:
                    #logger.info("link:=====", link)
                    logger.debug( "Processing links: %s/%s" % (count, total) )
                #logger.info( "Processing links: %s/%s ---->>> %s-%s-%s" % (count, total, link.getType(), link.getEnd1Id(), link.getEnd2Id()) )
                targetSystem.addLink(linkMapping, link)
                count = count + 1
            logger.debug( "Processing source links: %s/%s" % (count, total) )                
        except:
            logger.info('CI Source Links skipped because %s' % (sys.exc_info()[1]))


def DiscoveryMain(Framework):
    connectionDataManager = None
    try:
        logger.debug('Replicating topology from Arxview')

        connectionDataManager = FrameworkBasedConnectionDataManager(Framework)
        if not connectionDataManager.validate():
            return
        mappingFileFolder = os.path.join(CollectorsParameters.BASE_PROBE_MGR_DIR,
                                         CollectorsParameters.getDiscoveryConfigFolder(), ARXVIEW_CONFIG_FOLDER)
        mappingFileManager = ArxviewMappingFileManager(mappingFileFolder)

        mappingFile = getMappingFileFromFramework(Framework)
        if mappingFile:
            return replicateTopologyUsingMappingFile(os.path.join(mappingFileFolder, mappingFile),
                                                     connectionDataManager, mappingFileManager)
        else:
            Framework.reportError('No mapping file found.')
            logger.errorException("No mapping file found.")
    except:
        Framework.reportError('Failed to pull data from Arxview.')
        logger.errorException('Failed to pull data from Arxview.')
    finally:
        if connectionDataManager:
            connectionDataManager.closeClient()