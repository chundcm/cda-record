import logger
from appilog.common.system.types.vectors import ObjectStateHolderVector

selfips = ['127.0.0.1',
           '0.0.0.0',
           '*']

class EndPoint:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port

class ApplicationResult:
    def __init__(self, application, resultVector):
        self.resultVector = resultVector
        self.endpointlist = []
        self.application = application
        self.applicationresources = []
        for osh in resultVector:
            if osh.getObjectClass() == 'ip_service_endpoint':
                ip = osh.getAttributeValue('bound_to_ip_address')
                logger.debug("found ipaddress:", ip)
                port = osh.getAttributeValue('network_port_number')
                logger.debug("found port:", port)
                self.addEndPoint(ip, port)

    def addAll(self, resultVector):
        self.resultVector.addAll(resultVector)
        for osh in resultVector:
            if osh.getObjectClass() == 'ip_service_endpoint':
                ip = osh.getAttributeValue('bound_to_ip_address')
                logger.debug("found ipaddress:", ip)
                port = osh.getAttributeValue('network_port_number')
                logger.debug("found port:", port)
                self.addEndPoint(ip, port)

    def addEndPoint(self, ip, port):
        endpoint = EndPoint(str(ip), str(port))
        self.endpointlist.append(endpoint)



class FrameworkDelegator(object):
    def __init__(self):
        self.applicationResults = []
        self.index = -1
        self.application = None

    def sendObjects(self, resultVector):
        if self.index >= 0:
            if len(self.applicationResults) < self.index + 1:
                applicationresult = ApplicationResult(self.application, resultVector)
                self.applicationResults.append(applicationresult)
            else:
                applicationresult = self.applicationResults[self.index]
                applicationresult.addAll(resultVector)

        else:
            logger.debug("you should set current application before send objects")

    def setCurrentApplication(self, application):
        self.index += 1
        self.application = application


    def filterApplicationResults(self, ipaddress, port, useStrict=True):
        applicationResults = []

        logger.debug("Firstly, we need to filter the application by endpoint: %s:%s" % (ipaddress, port))
        for applicationResult in self.applicationResults:
            for endpoint in applicationResult.endpointlist:
                if (str(endpoint.ip) in selfips or str(endpoint.ip) == str(ipaddress)) and str(endpoint.port) == str(port):
                    logger.debug("found application:", applicationResult.application.getName())
                    applicationResults.append(applicationResult)
                    break

        if len(applicationResults) >= 1:
            logger.debug("found %s application with endpoint:%s:%s" % (len(applicationResults), ipaddress, port))
            return applicationResults

        if useStrict:
            return applicationResults

        logger.debug("cannot find any application with the endpoint, try to filter by port")
        for applicationResult in self.applicationResults:
            for endpoint in applicationResult.endpointlist:
                if str(endpoint.port) == str(port):
                    logger.debug("found application:", applicationResult.application.getName())
                    applicationResults.append(applicationResult)
                    break

        logger.debug("found %s application with port: %s" % (len(applicationResults), port))
        if len(applicationResults) <= 1:
            return applicationResults

        logger.debug("multiple match on port, try to filter by ipaddress:", ipaddress)

        results = []
        for applicationResult in applicationResults:
            for endpoint in applicationResult.endpointlist:
                if str(endpoint.ip) == str(ipaddress):
                    logger.debug("found application:", applicationResult.application.getName())
                    results.append(applicationResult)
                    break

        if len(results) > 0:
            logger.debug("found %s application with ip: %s" % (len(applicationResults), ipaddress))
            return results

        if len(results) == 0:
            logger.debug("no application match by ip, return results by port")
            return applicationResults

        return applicationResults


