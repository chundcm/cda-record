import re
import sys

from net.sf.saxon.s9api import Processor
from java.io import StringReader
from javax.xml.transform.stream import StreamSource


class processor:
    DEFAULT_NAMESPACE = r'xmlns\s*=\s*[^\'"]*[\'"][^\'"]*[\'"]'
    NON_DEFAULT_NAMESPACES = re.compile(r'xmlns:(\S+)\s*=\s*[^"]*"([^"]*)"')
    DOCTYPE = re.compile(r'<!DOCTYPE[^>]*?>')

    def __init__(self, content=None):
        self.__processor = Processor(False)
        self.__builder = self.__processor.newDocumentBuilder()
        self.__xPathCompiler = self.__processor.newXPathCompiler()
        if content:
            self.loadContent(content)
        else:
            self.__document = None

    def loadContent(self, content):
        content = re.sub(self.DEFAULT_NAMESPACE, '', content)  # remove default namespace

        # remove DOCTYPE in this way, because both self.__builder.setDTDValidation(False)
        # and self.__processor.setConfigurationProperty(DTD_VALIDATION, False) do NOT work.
        content = re.sub(self.DOCTYPE, '', content, re.IGNORECASE)

        reader = StringReader(content)
        self.__document = self.__builder.build(StreamSource(reader))
        self._declareNonDefaultNamespaces(content)

    def getVersion(self):
        return self.__processor.getSaxonProductVersion()

    def compile(self, xpathExp):
        try:
            executable = self.__xPathCompiler.compile(xpathExp)
            return executable
        except:
            raise SyntaxError, sys.exc_info()[1]

    def evaluateItem(self, xpathExp, context=None):
        if not self.__document:
            raise ValueError('Load xml first')
        executable = self.compile(xpathExp)
        selector = executable.load()
        if not context:
            context = self.__document
        selector.setContextItem(context)
        evaluatedResult = selector.evaluate()
        results = []
        map(results.append, evaluatedResult)
        return results

    def evaluate(self, xpathExp, context=None):
        results = self.evaluateItem(xpathExp, context)
        return [result.getStringValue() for result in results]

    def _declareNonDefaultNamespaces(self, content):
        for m in self.NON_DEFAULT_NAMESPACES.finditer(content):
            prefix, uri = m.groups()
            self.__xPathCompiler.declareNamespace(prefix, uri)

