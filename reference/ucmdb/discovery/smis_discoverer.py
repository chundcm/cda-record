#coding=utf-8
import re
import smis
import logger
import fptools
import cim
import cim_discover
import netutils

def stringClean(value):
    '''
    Transforms a value to a string and strips out space and " symbols from it
    @param value: string convertable value
    '''
    if value is not None:
        return str(value).strip(' "\\')

def getOperationalStatus(instance, property = 'OperationalStatus' ):
    STATE_UNKNOWN = 'Unknown'
    STATUS_VALUE_MAP = { '0' : 'Unknown',
                         '1' : 'Other',
                         '2' : 'OK',
                         '3' : 'Degraded',
                         '4' : 'Stressed',
                         '5' : 'Predictive Failure',
                         '6' : 'Error',
                         '7' : 'Non-Recoverable Error,',
                         '8' : 'Starting',
                         '9' : 'Stopping',
                         '10' : 'Stopped',
                         '11' : 'In Service',
                         '12' : 'No Contact',
                         '13' : 'Lost Communication',
                         '14' : 'Aborted',
                         '15' : 'Dormant',
                         '16' : 'Supporting Entity in Error',
                         '17' : 'Completed',
                         '18' : 'Power Mode',
                         '19' : 'Relocating',
                         '32769' : 'ONLINE'
                       }
    statusValueList = []
    if instance:
        statusList = instance.getProperty(property).getValue()
        for s in statusList:
            statusValueList.append(STATUS_VALUE_MAP.get(str(s), STATE_UNKNOWN))

    return ",".join(statusValueList)

class BaseSmisDiscoverer:
    '''
    Basic Discoverer class from which all specific discoverers should derive.
    '''
    def __init__(self):
        self.className = None

    def parse(self, instances):
        raise NotImplementedError('')

    def discover(self, client):
        if not self.className:
            raise ValueError('CIM class name must be set in order to perform query.')
        logger.debug('Queuing class "%s"' % self.className)
        instances = client.getInstances(self.className)
        return self.parse(instances)

def getSmisCredentials(allCredentials, framework):

    smisCredentialsFilter = fptools.partiallyApply(cim_discover.isCredentialOfCategory, fptools._, smis.CimCategory.SMIS, framework)
    smisCredentials = filter(smisCredentialsFilter, allCredentials)

    noCategoryCredentialsFilter = fptools.partiallyApply(cim_discover.isCredentialOfCategory, fptools._, cim.CimCategory.NO_CATEGORY, framework)
    noCategoryCredentials = filter(noCategoryCredentialsFilter, allCredentials)

    return smisCredentials + noCategoryCredentials

def getSmisNamespaces(framework):
    categories = cim_discover.getCimCategories(framework)
    smisCategory = cim_discover.getCategoryByName(smis.CimCategory.SMIS, categories)
    if smisCategory:
        return [ns for ns in smisCategory.getNamespaces()]

