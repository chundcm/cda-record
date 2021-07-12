#coding=utf-8
import modeling
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector

class _HasArn:
    ''' Class that extends other classes with 'ARN' property '''
    def __init__(self):
        self.__arn = None

    def setArn(self, name):
        if not name: raise ValueError("ARN is empty")
        self.__arn = name

    def getArn(self):
        return self.__arn


class _HasOsh:
    ''' Class that extends other classes with ability to have OSH built from them '''
    def __init__(self):
        self.__osh = None

    def setOsh(self, osh):
        if osh is None: raise ValueError("OSH is None")
        self.__osh = osh

    def getOsh(self):
        return self.__osh


class _base(_HasOsh, _HasArn):
    def __init__(self):
        _HasArn.__init__(self)
        _HasOsh.__init__(self)

    def __repr__(self):
        return "%s (name = %s)" % (self.__class__.__name__, self.getArn())

    def report(self):
        raise  NotImplementedError()


class TaskDefinition(_base):
    def __init__(self, account, region):
        _base.__init__(self)
        self.Account = account
        self.Region = region
        self.family = ''
        self.revision = ''
        self.status = ''

    def report(self):
        vector = ObjectStateHolderVector()
        accountLink = modeling.createLinkOSH('containment', self.Account.getOsh(), self.getOsh())
        vector.add(accountLink)
        regionLink = modeling.createLinkOSH('membership', self.Region.getOsh(), self.getOsh())
        vector.add(regionLink)
        vector.add(self.getOsh())
        return vector


class Task(_HasArn, _base):
    def __init__(self, node, taskDef, cluster):
        _base.__init__(self)
        self.Ec2Instance = node
        self.TaskDefinition = taskDef
        self.Cluster = cluster

    def report(self):
        vector = ObjectStateHolderVector()
        nodeLink = modeling.createLinkOSH('dependency', self.getOsh(), self.Ec2Instance)
        vector.add(nodeLink)
        taskDefLink = modeling.createLinkOSH('resource', self.TaskDefinition.getOsh(), self.getOsh())
        vector.add(taskDefLink)
        clusterLink = modeling.createLinkOSH('containment', self.Cluster.getOsh(), self.getOsh())
        vector.add(clusterLink)
        vector.add(self.getOsh())
        return vector


class Cluster(_base):
    def __init__(self, account, region):
        _base.__init__(self)
        self.Account = account
        self.Region = region
        self.Ec2Instances = []

    def report(self):
        vector = ObjectStateHolderVector()
        accountLink = modeling.createLinkOSH('containment', self.Account.getOsh(), self.getOsh())
        vector.add(accountLink)
        regionLink = modeling.createLinkOSH('membership', self.Region.getOsh(), self.getOsh())
        vector.add(regionLink)
        vector.add(self.getOsh())
        for ec2Instance in self.Ec2Instances:
            ec2InstanceLink = modeling.createLinkOSH('containment', self.getOsh(), ec2Instance)
            vector.add(ec2InstanceLink)
        return vector


class Service(_base):
    def __init__(self, cluster, taskDef):
        _base.__init__(self)
        self.Cluster = cluster
        self.TaskDefinition = taskDef

    def report(self):
        vector = ObjectStateHolderVector()
        clusterLink = modeling.createLinkOSH('containment', self.Cluster.getOsh(), self.getOsh())
        vector.add(clusterLink)
        taskDefLink = modeling.createLinkOSH('dependency', self.getOsh(), self.TaskDefinition.getOsh())
        vector.add(taskDefLink)
        vector.add(self.getOsh())
        return vector


class DockerDaemon(_base):
    def __init__(self, ec2Instance):
        _base.__init__(self)
        self.DockerNode = ec2Instance
        self.dockerObj = None
        self.ipServiceEndpoint = []

        self.name = 'Docker Daemon'
        self.version = None
        self.productName = 'Docker Daemon'
        self.setArn(self.name)

    def report(self):
        vector = ObjectStateHolderVector()
        osh = self.getOsh()
        osh.setContainer(self.DockerNode)
        vector.add(osh)
        dockerOSH = ObjectStateHolder('docker')
        dockerOSH.setAttribute('name', 'Docker')
        dockerOSH.setContainer(self.DockerNode)
        vector.add(dockerOSH)
        dockerDeamonLink = modeling.createLinkOSH('membership', dockerOSH, osh)
        vector.add(dockerDeamonLink)
        return vector


class DockerContainer(_base):
    def __init__(self, dockerDaemon, task):
        _base.__init__(self)
        self.Ec2Instance = task.Ec2Instance
        self.DockerDaemon = dockerDaemon
        self.Task = task
        self.labels = []
        self.commands = []
        self.loggingDriver = ''
        self.imageName = None

    def report(self):
        vector = ObjectStateHolderVector()
        osh = self.getOsh()
        osh.setContainer(self.Ec2Instance)
        vector.add(osh)
        daemonLink = modeling.createLinkOSH('manage', self.DockerDaemon.getOsh(), osh)
        vector.add(daemonLink)
        taskLink = modeling.createLinkOSH('containment', self.Task.getOsh(), osh)
        vector.add(taskLink)
        return vector


class DockerImage(_base):
    def __init__(self):
        _base.__init__(self)
        self.Repository = None
        self.fromPrivateRegistry = False


class DockerRepository(_base):
    def __init__(self, name):
        _base.__init__(self)
        self.name = name
        self.description = ''


class DockerRegistry(_base):
    def __init__(self, accountId, region):
        _base.__init__(self)
        self.name = str(accountId) + '_' + region.getName()

