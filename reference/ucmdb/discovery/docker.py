#coding=utf-8

class _HasName:
    ''' Class that extends other classes with 'name' property '''
    def __init__(self):
        self.__name = None

    def setName(self, name):
        if not name: raise ValueError("name is empty")
        self.__name = name

    def getName(self):
        return self.__name

class _HasOsh:
    ''' Class that extends other classes with ability to have OSH built from them '''
    def __init__(self):
        self.__osh = None

    def setOsh(self, osh):
        if osh is None: raise ValueError("OSH is None")
        self.__osh = osh

    def getOsh(self):
        return self.__osh


class Node(_HasName, _HasOsh):
    def __init__(self, ip):
        _HasName.__init__(self)
        _HasOsh.__init__(self)
        self.ip = ip

    def __repr__(self):
        return "%s (name = %s)" % (self.__class__.__name__, self.getName())


class Docker(_HasName, _HasOsh):
    def __init__(self):
        _HasName.__init__(self)
        _HasOsh.__init__(self)
        self.dockerNodeObj = None

        self.setName('Docker')

    def __repr__(self):
        return "%s (name = %s)" % (self.__class__.__name__, self.getName())


class DockerSwarmDaemon(_HasName, _HasOsh):
    def __init__(self, name):
        _HasName.__init__(self)
        _HasOsh.__init__(self)
        self.dockerNodeObj = None
        self.dockerObj = None
        self.dockerSwarmClusterObj = None
        self.usedHttpProtocol = None
        self.uriId = None

        self.setName(name)
        self.version = None
        self.discovered_product_name = 'Docker Swarm Daemon'

    def __repr__(self):
        return "%s (name = %s)" % (self.__class__.__name__, self.getName())


class DockerSwarmCluster(_HasName, _HasOsh):
    def __init__(self, name):
        _HasName.__init__(self)
        _HasOsh.__init__(self)
        self.dockerSwarmDaemonObj = None
        self.dockerDaemonObjs = {}

        self.setName(name)

    def __repr__(self):
        return "%s (name = %s)" % (self.__class__.__name__, self.getName())


class DockerDaemon(_HasName, _HasOsh):
    def __init__(self, name):
        _HasName.__init__(self)
        _HasOsh.__init__(self)
        self.dockerNodeObj = None
        self.dockerObj = None
        self.ipServiceEndpoint = []

        self.setName(name)
        self.version = None
        self.discovered_product_name = 'Docker Daemon'
        self.hostName = None
        self.loggingDriver = None
        self.labelList = []

    def __repr__(self):
        return "%s (name = %s)" % (self.__class__.__name__, self.getName())


class DockerVolume(_HasName, _HasOsh):
    def __init__(self):
        _HasName.__init__(self)
        _HasOsh.__init__(self)
        self.volumeNodeObj = None
        self.logicalVolumeObj = None

        self.setName('Docker Volume')
        self.volumeSrc = None
        self.volumeDst = None
        self.accessType = None

    def __repr__(self):
        return "%s (name = %s)" % (self.__class__.__name__, self.getName())


class DockerImage(_HasName, _HasOsh):
    def __init__(self):
        _HasName.__init__(self)
        _HasOsh.__init__(self)
        self.imageNodeObj = None
        self.imageTemplateObj = None

        self.imageId = None
        self.imageName = None
        self.imageTag = None
        self.virtualSize = None
        self.entryPointList = []
        self.volumeList = []
        self.cmdList = []
        self.labelList = []
        self.portlList = []

    def __repr__(self):
        imageName = self.getName() + ':' + self.imageTag
        return "%s (name = %s)" % (self.__class__.__name__, imageName)


class DockerImageTemplate(_HasName, _HasOsh):
    def __init__(self):
        _HasName.__init__(self)
        _HasOsh.__init__(self)
        self.imageId = None

    def __repr__(self):
        return "%s (name = %s)" % (self.__class__.__name__, self.getName())


class DockerContainer(_HasName, _HasOsh):
    def __init__(self):
        _HasName.__init__(self)
        _HasOsh.__init__(self)
        self.imageObj = None
        self.linkToContainerObjs = []
        self.usedVolumeObjs = []
        self.daemonObj = None
        self.processList = []

        self.containerId = None
        self.imageId = None
        self.imageName = None

        self.containerPorts = ''
        self.restartPolicy = ''
        self.restartMaxCount = 0
        self.loggingDriver = ''
        self.memoryLimitInMB = 0
        self.cmdList = []
        self.labelList = []

    def __repr__(self):
        return "%s (name = %s)" % (self.__class__.__name__, self.getName())

class LogicalVolume(_HasName, _HasOsh):
    def __init__(self, name):
        _HasName.__init__(self)
        _HasOsh.__init__(self)
        self.setName(name)

    def __repr__(self):
        return "%s (name = %s)" % (self.__class__.__name__, self.getName())




