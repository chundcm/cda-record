# coding=utf-8
import os
import xml.etree.ElementTree as ET

from remedyforce_decorators import abstract_method
from remedyforce_mapping_implementation import *


class AbstractMappingFileManager:
    @abstract_method
    def getMapping(self, fileName):
        "@types: str -> Mapping"

    @abstract_method
    def getAvailableMappingFiles(self):
        "@types: -> (str)"


class FolderBasedMappingFileManager(AbstractMappingFileManager):
    def __init__(self, mappingFileFolderName):
        self.__mappingFileFolderName = mappingFileFolderName

    def getAvailableMappingFiles(self):
        mappingFileNames = []
        for fileName in os.listdir(self.__mappingFileFolderName):
            fullPath = os.path.join(self.__mappingFileFolderName, fileName)
            if fileName.endswith('.xml') and os.path.isfile(fullPath):
                mappingFileNames.append(fullPath)
        return mappingFileNames


class RemedyforceMappingFileManager(FolderBasedMappingFileManager):
    def getMapping(self, fileName):
        mapping = SimpleMapping()

        tree = ET.parse(fileName)
        root = tree.getroot()

        for mappingDef in root.findall("./targetcis/source_ci_type"):
            sourceCiName = mappingDef.get('name')
            targetCiDef = mappingDef.find('target_ci_type')
            targetCiName = targetCiDef.get('name')
            query = mappingDef.get('query')
            ref = mappingDef.get('ref')
            base = mappingDef.get('base')
            idKey = mappingDef.get('idKey')
            needContainer = mappingDef.get('needContainer') == 'true'
            needRelationship = mappingDef.get('needRelationship')
            related_source_ci_types = mappingDef.find('related_source_ci_types')
            related_source_ci_children = []
            if related_source_ci_types is not None:
                related_source_ci_type_list = related_source_ci_types.findall('related_source_ci_type') or []
                for related_source_ci_type in related_source_ci_type_list:
                    related_ci_name = related_source_ci_type.get('name')
                    relationship = related_source_ci_type.get('relationship')
                    alias = related_source_ci_type.get('alias')
                    related_source_ci_children.append(
                            {'alias': alias, 'relationship': relationship, 'related_ci_name': related_ci_name})

            ciMapping = SimpleCiMapping(sourceCiName, targetCiName, query, ref, base, idKey,
                                        needContainer=needContainer, needRelationship=needRelationship,
                                        children=related_source_ci_children)

            for attributeMappingDef in targetCiDef.findall('target_attribute'):
                targetAttributeName = attributeMappingDef.get('name')
                attributeMappingType = attributeMappingDef.find('map').get('type')
                if attributeMappingType == 'direct':
                    sourceAttributeName = attributeMappingDef.find('map').get('source_attribute')
                    owner = attributeMappingDef.find('map').get('owner')
                    attributeMapping = DirectAttributeMapping(sourceAttributeName, targetAttributeName, owner)
                elif attributeMappingType == 'const':
                    value = attributeMappingDef.find('map').get('value')
                    attributeMapping = ConstAttributeMapping(value, targetAttributeName)
                elif attributeMappingType == 'eval':
                    value = attributeMappingDef.find('map').get('value')
                    attributeMapping = EvalAttributeMapping(value, targetAttributeName)
                elif attributeMappingType == 'method':
                    value = attributeMappingDef.find('map').get('value')
                    attributeMapping = MethodAttributeMapping(value, targetAttributeName)

                filtersElement = attributeMappingDef.find('filters')
                if filtersElement:
                    for filter in filtersElement.findall('filter'):
                        (filterModule, filterMethod) = filter.text.split('.')
                        attributeMapping.addFilter(SimpleFilter(filterModule, filterMethod))

                validatorsElement = attributeMappingDef.find('validators')
                if validatorsElement:
                    for validator in validatorsElement.findall('validator'):
                        (validatorModule, validatorMethod) = validator.text.split('.')
                        attributeMapping.addValidator(SimpleValidator(validatorModule, validatorMethod))

                ciMapping.addAttributeMapping(attributeMapping)
            mapping.addCiMapping(ciMapping)

        for linkMappingDef in root.findall("./targetrelations/link"):
            sourceLinkName = linkMappingDef.get('source_link_type')
            targetLinkName = linkMappingDef.get('target_link_type')

            sourceCi1Name = linkMappingDef.get('source_ci_type_end1')
            sourceCi2Name = linkMappingDef.get('source_ci_type_end2')

            direction = linkMappingDef.get('direction') or 'forward'

            targetCi1Name = None
            targetCi2Name = None
            targetCi1 = linkMappingDef.find('target_ci_type_end1')
            targetCi2 = linkMappingDef.find('target_ci_type_end2')

            if targetCi1 is not None and targetCi2 is not None:
                targetCi1Name = targetCi1.get('name')
                targetCi2Name = targetCi2.get('name')

            failurePolicy = linkMappingDef.get('failure_policy') or 'ignore'
            isContainer = linkMappingDef.get('isContainer') or 'false'
            isContainer = 'true' == isContainer

            referenceAttribute = linkMappingDef.get('reference_attribute')
            linkMapping = ReferenceLinkMapping(sourceLinkName, targetLinkName, sourceCi1Name, sourceCi2Name,
                                               referenceAttribute, targetCi1Name, targetCi2Name, direction,
                                               failurePolicy, isContainer)

            mapping.addLinkMapping(linkMapping)

        return mapping
