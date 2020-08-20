import re

import jee
import logger
import modeling

handlers = {}


class Dependency(object):
    """If one CI depends on another CI, we say there's a dependency between them."""

    def __init__(self, scpType, scpContext, applicationTopology):
        self.scpType = scpType
        self.scpContext = scpContext
        clazz = handlers.get(scpType, HttpDependencyHandler)
        logger.debug('Got handler: ', clazz)
        self.handler = clazz(scpType, scpContext, applicationTopology)

    def resolve(self):
        return self.handler.handle()


def handler(type):
    def decorate(func):
        handlers[type] = func
        return func

    return decorate


class DependencyHandler(object):
    def __init__(self, scpType, scpContext, applicationTopology):
        self.scpType = scpType
        self.scpContext = scpContext
        self.applicationTopology = applicationTopology

    def handle(self):
        self._parseScpContext()

        result = []
        for t in self.applicationTopology:
            container, applications = t
            matchedApplications = [app for app in applications if app is not None and self._match(app)]
            if matchedApplications:
                logger.debug('Found matched application: ', matchedApplications)
                result.append((container, matchedApplications))

        return result

    def _parseScpContext(self):
        raise NotImplementedError('DependencyHandler is supposed to be an interface or abstract class')

    def _match(self, applications):
        raise NotImplementedError('DependencyHandler is supposed to be an interface or abstract class')

    def _hasContextRoot(self, app, contextRoot):
        contextList = self._toContextList(contextRoot)
        moduleContexts = [m.contextRoot for m in app.getModules() if isinstance(m, jee.WebModule)]
        for context in contextList:
            for moduleContext in moduleContexts:
                logger.debug('Try to match module "%s" with context "%s" ' % (moduleContext, context))
                if moduleContext is not None and (moduleContext == context \
                        or filter(None, moduleContext.split('/')) == filter(None, context.split('/'))):
                    return True
        logger.debug('No module matched with contexts ', contextList)
        return False

    def _toContextList(self, contextRoot):
        # '/a/b/c' -> 'a/b/c'
        # '/a/b/login.do' -> '/a/b'
        result = [contextRoot]
        a = [c for c in contextRoot.split('/')]
        trimLastContext = '/'.join(a[0:-1])
        if trimLastContext:
            result.append(trimLastContext)
        logger.debug('Resolve context list: ', result)
        return result


@handler(type='websphere_ws')
@handler(type='weblogic_ws')
class WebServiceDependencyHandler(DependencyHandler):
    WS_PATTERNS = {'websphere_ws': re.compile(r'/?(\w+)/services/(\w+)/?'),
                   'weblogic_ws': re.compile(r'/?(\w+)/(\w+)/?')}

    def __init__(self, scpType, scpContext, applicationTopology):
        super(WebServiceDependencyHandler, self).__init__(scpType, scpContext, applicationTopology)
        self.contextRoot = None
        self.serviceName = None

    def _parseScpContext(self):
        pattern = WebServiceDependencyHandler.WS_PATTERNS[self.scpType]
        m = pattern.match(self.scpContext)
        if m:
            self.contextRoot = m.group(1)
            self.serviceName = m.group(2)
        else:
            raise ValueError('%s: %s cannot be parsed.' % (self.scpType, self.scpContext))

    def _match(self, app):
        return self._hasContextRoot(app, self.contextRoot) and self._hasWebService(app)

    def _hasWebService(self, app):
        for m in app.getModules():
            for w in m.getWebServices():
                if w.getName() == self.serviceName:
                    return True
        return False

    def __repr__(self):
        pass


@handler(type='ejb')
class EjbDependencyHandler(DependencyHandler):
    def __init__(self, scpType, scpContext, applicationTopology):
        super(EjbDependencyHandler, self).__init__(scpType, scpContext, applicationTopology)
        self.jndiName = None

    def _parseScpContext(self):
        self.jndiName = self.scpContext

    def _match(self, app):
        return self._hasJndiName(app)

    def _hasJndiName(self, app):
        for m in app.getModules():
            for e in m.getEntries():
                if e.getJndiName() == self.jndiName:
                    return True
        return False


    def __repr__(self):
        pass


@handler(type='http')
class HttpDependencyHandler(DependencyHandler):
    def __init__(self, scpType, scpContext, applicationTopology):
        super(HttpDependencyHandler, self).__init__(scpType, scpContext, applicationTopology)
        self.contextRoot = None

    def _parseScpContext(self):
        self.contextRoot = self.scpContext

    def _match(self, app):
        return self._hasContextRoot(app, self.contextRoot)

    def __repr__(self):
        pass


def resolveApplicationDependency(scpType, scpContext, applicationTopology):
    logger.debug('Start to resolve application dependency for SCP: type = "%s", context = "%s"' % (scpType, scpContext))
    d = Dependency(scpType, scpContext, applicationTopology)
    return d.resolve()


def resolveJmsDependency(scpType, scpContext, jmsTopology):
    logger.debug('Start to resolve jms dependency for SCP: type = "%s", context = "%s"' % (scpType, scpContext))
    result = []
    if scpType == 'jms':
        for t in jmsTopology:
            containerOsh, jmsDestinations = t
            matchedJmsDestination = [jms for jms in jmsDestinations if jms.getName() == scpContext]
            result.append((containerOsh, matchedJmsDestination))
    return result


def resolveDependency(scpType, scpContext, reporterCreator, applicationResult, OSHVResult):
    if scpType == 'jms':
        jmsTopology = reporterCreator.getJmsDsReporter().jmsTopology
        logger.debug('Found jms topology: %s' % jmsTopology)

        matchedJmsTopology = resolveJmsDependency(scpType, scpContext, jmsTopology)
        for t in matchedJmsTopology:
            containerOsh, jmsDestinations = t
            OSHVResult.add(containerOsh)
            # J2EE domain needs a membership link to J2EE server
            if containerOsh.getObjectClass() == 'j2eedomain':
                OSHVResult.add(modeling.createLinkOSH('member', containerOsh, applicationResult.application.getOsh()))

            for j in jmsDestinations:
                OSHVResult.add(j.getOsh())
    else:
        applicationTopology = reporterCreator.getApplicationReporter().applicationTopology
        logger.debug('Found application topology: %s' % applicationTopology)

        matchedAppTopology = resolveApplicationDependency(scpType, scpContext, applicationTopology)
        for t in matchedAppTopology:
            container, applications = t
            OSHVResult.add(container.getOsh())
            # J2EE domain needs a membership link to J2EE server
            if isinstance(container, jee.Domain):
                OSHVResult.add(modeling.createLinkOSH('member', container.getOsh(),
                                                      applicationResult.application.getOsh()))

            for a in applications:
                osh = a.getOsh()
                OSHVResult.add(osh)
                applicationResult.applicationresources.append(osh)
