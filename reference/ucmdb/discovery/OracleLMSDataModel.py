#coding=utf-8
import logger

from java.lang import String
from java.text import SimpleDateFormat

sdf = SimpleDateFormat('yyyy-MM-dd_HH:mm:ss')

class LMSOverviewObject:
    def __init__(self, group = None, aggregationLevel = None, oracleCsi = None, oracleProductCategory = None, machineId = None,
                 vmachineId = None, dbEdition = None, dbName = None, version = None, optionsInstalled = None, optionsInUse = None,
                 packsGranted = None, packsAgreed = None, applicationName = None, applicationStatus = None, userCountDbaUsers = None,
                 userCountApplication = None, serverManufacturer = None, serverModel = None, operatingSystem = None,
                 socketsPopulatedPhys = None, totalPhysicalCores = None, processorIdentifier = None, processorSpeed = None,
                 socketCapacityPhysical = None, totalLogicalCores = None, partitioningMethod = None, dbRole = None, serverNameInTheCluster = None,
                 topConcurrencyTimestamp = None, sessions = None, instanceSessionHighwater = None, installDate = None,
                 measurementComment = None, discoveryId = None):
        self.group = group
        self.aggregationLevel = aggregationLevel
        self.oracleCsi = oracleCsi
        self.oracleProductCategory = oracleProductCategory
        self.machineId = machineId
        self.vmachineId = vmachineId
        self.dbEdition = dbEdition
        self.dbName = dbName
        self.version = version
        self.optionsInstalled = optionsInstalled
        self.optionsInUse = optionsInUse
        self.packsGranted = packsGranted
        self.packsAgreed = packsAgreed
        self.applicationName = applicationName
        self.applicationStatus = applicationStatus
        self.userCountDbaUsers = userCountDbaUsers
        self.userCountApplication = userCountApplication
        self.serverManufacturer = serverManufacturer
        self.serverModel = serverModel
        self.operatingSystem = operatingSystem
        self.socketsPopulatedPhys = socketsPopulatedPhys
        self.totalPhysicalCores = totalPhysicalCores
        self.processorIdentifier = processorIdentifier
        self.processorSpeed = processorSpeed
        self.socketCapacityPhysical = socketCapacityPhysical
        self.totalLogicalCores = totalLogicalCores
        self.partitioningMethod = partitioningMethod
        self.dbRole = dbRole
        self.serverNameInTheCluster = serverNameInTheCluster
        self.topConcurrencyTimestamp = topConcurrencyTimestamp
        self.sessions = sessions
        self.instanceSessionHighwater = instanceSessionHighwater
        self.installDate = installDate
        self.measurementComment = measurementComment
        self.discoveryId = discoveryId

    def __repr__(self):
        measurementCommentStr = None
        if self.measurementComment is not None:
            measurementCommentStr = String(self.measurementComment.getBytes(1, int(self.measurementComment.length())))

        return '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % (
                   encodeString(self.group),
                   encodeString(self.aggregationLevel),
                   encodeString(self.oracleCsi),
                   encodeString(self.oracleProductCategory),
                   encodeString(self.machineId),
                   encodeString(self.vmachineId),
                   encodeString(self.dbEdition),
                   encodeString(self.dbName),
                   encodeString(self.version),
                   encodeString(self.optionsInstalled),
                   encodeString(self.optionsInUse),
                   encodeString(self.packsGranted),
                   encodeString(self.packsAgreed),
                   encodeString(self.applicationName),
                   encodeString(self.applicationStatus),
                   encodeString(self.userCountDbaUsers),
                   encodeString(self.userCountApplication),
                   encodeString(self.serverManufacturer),
                   encodeString(self.serverModel),
                   encodeString(self.operatingSystem),
                   encodeString(self.socketsPopulatedPhys),
                   encodeString(self.totalPhysicalCores),
                   encodeString(self.processorIdentifier),
                   encodeString(self.processorSpeed),
                   encodeString(self.socketCapacityPhysical),
                   encodeString(self.totalLogicalCores),
                   encodeString(self.partitioningMethod),
                   encodeString(self.dbRole),
                   encodeString(self.serverNameInTheCluster),
                   encodeString(self.topConcurrencyTimestamp),
                   encodeString(self.sessions),
                   encodeString(self.instanceSessionHighwater),
                   encodeString(self.installDate),
                   encodeString(measurementCommentStr))

class LMSDetailObject:
    def __init__(self, rlScriptVersion = None, timestamp = None, machineId = None, vmachineId = None, banner = None,
                 dbName = None, userCount = None, serverManufacturer = None, serverModel = None, operatingSystem = None,
                 socketsPopulatedPhys = None, totalPhysicalCores = None, processorIdentifier = None, processorSpeed = None,
                 totalLogicalCores = None, partitioningMethod = None, dbRole = None, installDate = None, discoveryId = None):
        self.rlScriptVersion = rlScriptVersion
        self.timestamp = timestamp
        self.machineId = machineId
        self.vmachineId = vmachineId
        self.banner = banner
        self.dbName = dbName
        self.userCount = userCount
        self.serverManufacturer = serverManufacturer
        self.serverModel = serverModel
        self.operatingSystem = operatingSystem
        self.socketsPopulatedPhys = socketsPopulatedPhys
        self.totalPhysicalCores = totalPhysicalCores
        self.processorIdentifier = processorIdentifier
        self.processorSpeed = processorSpeed
        self.totalLogicalCores = totalLogicalCores
        self.partitioningMethod = partitioningMethod
        self.dbRole = dbRole
        self.installDate = installDate
        self.discoveryId = discoveryId

    def __repr__(self):
        return '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % (
            encodeString(self.rlScriptVersion),
            sdf.format(self.timestamp),
            encodeString(self.machineId),
            encodeString(self.vmachineId),
            encodeString(self.banner),
            encodeString(self.dbName),
            encodeString(self.userCount),
            encodeString(self.serverManufacturer),
            encodeString(self.serverModel),
            encodeString(self.operatingSystem),
            encodeString(self.socketsPopulatedPhys),
            encodeString(self.totalPhysicalCores),
            encodeString(self.processorIdentifier),
            encodeString(self.processorSpeed),
            encodeString(self.totalLogicalCores),
            encodeString(self.partitioningMethod),
            encodeString(self.dbRole),
            encodeString(self.installDate))

class LMSDbaUsersObject:
    def __init__(self, username = None, userId = None, defaultTablespace = None, temporaryTablespace = None, created = None,
                 profile = None, expiryDate = None,  machineId = None, dbName = None, timestamp = None, discoveryId = None):
        self.username = username
        self.userId = userId
        self.defaultTablespace = defaultTablespace
        self.temporaryTablespace = temporaryTablespace
        self.created = created
        self.profile = profile
        self.expiryDate = expiryDate
        self.machineId = machineId
        self.dbName = dbName
        self.timestamp = timestamp
        self.discoveryId = discoveryId

    def __repr__(self):
        return '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % (
            encodeString(self.username),
            encodeString(self.userId),
            encodeString(self.defaultTablespace),
            encodeString(self.temporaryTablespace),
            encodeString(self.created),
            encodeString(self.profile),
            encodeString(self.expiryDate),
            encodeString(self.machineId),
            encodeString(self.dbName),
            sdf.format(self.timestamp))

class LMSOptionsObject:
    def __init__(self, machineId = None, dbName = None, timestamp = None, hostName = None, instanceName = None,
                 optionName = None, optionQuery = None, sqlErrCode = None, sqlErrMessage = None, col010 = None, col020 = None,
                 col030 = None, col040 = None, col050 = None, col060 = None, col070 = None, col080 = None, col090 = None,
                 col100 = None, col110 = None, col120 = None, col130 = None, col140 = None, col150 = None, col160 = None,
                 discoveryId = None):
        self.machineId = machineId
        self.dbName = dbName
        self.timestamp = timestamp
        self.hostName = hostName
        self.instanceName = instanceName
        self.optionName = optionName
        self.optionQuery = optionQuery
        self.sqlErrCode = sqlErrCode
        self.sqlErrMessage = sqlErrMessage
        self.col010 = col010
        self.col020 = col020
        self.col030 = col030
        self.col040 = col040
        self.col050 = col050
        self.col060 = col060
        self.col070 = col070
        self.col080 = col080
        self.col090 = col090
        self.col100 = col100
        self.col110 = col110
        self.col120 = col120
        self.col130 = col130
        self.col140 = col140
        self.col150 = col150
        self.col160 = col160
        self.discoveryId = discoveryId

    def __repr__(self):
        return '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % (
            encodeString(self.machineId),
            encodeString(self.dbName),
            sdf.format(self.timestamp),
            encodeString(self.hostName),
            encodeString(self.instanceName),
            encodeString(self.optionName),
            encodeString(self.optionQuery),
            encodeString(self.sqlErrCode),
            encodeString(self.sqlErrMessage),
            encodeString(self.col010),
            encodeString(self.col020),
            encodeString(self.col030),
            encodeString(self.col040),
            encodeString(self.col050),
            encodeString(self.col060),
            encodeString(self.col070),
            encodeString(self.col080),
            encodeString(self.col090),
            encodeString(self.col100),
            encodeString(self.col110),
            encodeString(self.col120),
            encodeString(self.col130),
            encodeString(self.col140),
            encodeString(self.col150),
            encodeString(self.col160))

class LMSVLicenseObject:
    def __init__(self, sessionsMax = None, sessionsWarning = None, sessionsCurrent = None, sessionsHighwater = None,
                 cpuCountCurrent = None, cpuCountHighwater = None, usersMax = None,  machineId = None, dbName = None,
                 timestamp = None, discoveryId = None):
        self.sessionsMax = sessionsMax
        self.sessionsWarning = sessionsWarning
        self.sessionsCurrent = sessionsCurrent
        self.sessionsHighwater = sessionsHighwater
        self.cpuCountCurrent = cpuCountCurrent
        self.cpuCountHighwater = cpuCountHighwater
        self.usersMax = usersMax
        self.machineId = machineId
        self.dbName = dbName
        self.timestamp = timestamp
        self.discoveryId = discoveryId

    def __repr__(self):
        return '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % (
            encodeString(self.sessionsMax),
            encodeString(self.sessionsWarning),
            encodeString(self.sessionsCurrent),
            encodeString(self.sessionsHighwater),
            encodeString(self.cpuCountCurrent),
            encodeString(self.cpuCountHighwater),
            encodeString(self.usersMax),
            encodeString(self.machineId),
            encodeString(self.dbName),
            sdf.format(self.timestamp))

class LMSVSessionObject:
    def __init__(self, saddr = None, sid = None, paddr = None, userNo = None, userName = None, command = None,
                 status = None, server = None, schemaName = None, osUser = None, process = None, machine = None, terminal = None,
                 program = None, type = None, lastCallEt = None, logonTime = None, machineId = None, dbName = None,
                 timestamp = None, discoveryId = None):
        self.saddr = saddr
        self.sid = sid
        self.paddr = paddr
        self.userNo = userNo
        self.userName = userName
        self.command = command
        self.status = status
        self.server = server
        self.schemaName = schemaName
        self.osUser = osUser
        self.process = process
        self.machine = machine
        self.terminal = terminal
        self.program = program
        self.type = type
        self.lastCallEt = lastCallEt
        self.logonTime = logonTime
        self.machineId = machineId
        self.dbName = dbName
        self.timestamp = timestamp
        self.discoveryId = discoveryId

    def __repr__(self):
        return '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % (
            encodeString(self.saddr),
            encodeString(self.sid),
            encodeString(self.paddr),
            encodeString(self.userNo),
            encodeString(self.userName),
            encodeString(self.command),
            encodeString(self.status),
            encodeString(self.server),
            encodeString(self.schemaName),
            encodeString(self.osUser),
            encodeString(self.process),
            encodeString(self.machine),
            encodeString(self.terminal),
            encodeString(self.program),
            encodeString(self.type),
            encodeString(self.lastCallEt),
            sdf.format(self.logonTime),
            encodeString(self.machineId),
            encodeString(self.dbName),
            sdf.format(self.timestamp))

def encodeString(content):
    if(content != None):
        return content
    else:
        return ''