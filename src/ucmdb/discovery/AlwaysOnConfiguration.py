#coding=utf-8
from appilog.common.system.types.vectors import ObjectStateHolderVector
from appilog.common.system.types import ObjectStateHolder
from java.util import HashMap
from java.util import ArrayList
from java.lang import String
import Util
import netutils
import Queries
import modeling
import logger

class AlwaysOnConfiguration:
    def __init__(self, connection, discoveryOptions):
        self.connection = connection
        self.discoveryOptions = discoveryOptions

    def collectData(self,sqlServerId):
        oshv = ObjectStateHolderVector()
        try:
            self.getAlwaysonConfig(oshv,sqlServerId)
        except:
            logger.warn("Couldn't get Always On configuration for server: ", sqlServerId.toString())
        return oshv

    def getAlwaysonConfig(self,oshv, sqlServerId):
        rs = self.connection.getTable("SELECT group_id,name FROM sys.availability_groups_cluster")
        while rs.next():
            group_id = rs.getString("group_id")
            group_name = rs.getString("name")
            clusterGroupOsh = ObjectStateHolder('node')
            clusterGroupOsh.setAttribute('name', group_name)
            oshv.add(clusterGroupOsh)
            self.getListenerIP(group_id, clusterGroupOsh, oshv)
            resourceGroupOsh = ObjectStateHolder('mscsgroup')
            resourceGroupOsh.setAttribute("name", group_name)
            resourceGroupOsh.setContainer(clusterGroupOsh)
            oshv.add(resourceGroupOsh)
            self.getAvailabilityDatabase(oshv, sqlServerId, group_id, resourceGroupOsh)
        rs.close()
        return oshv

    def getAvailabilityDatabase(self, oshv, sqlServerId, group_id, resourceGroupOsh):
        query = Util.replace(Queries.GET_DATABASE_OF_AVAILABILITYGROUP, group_id)
        rs = self.connection.getTable(query)
        while rs.next():
            dbName = rs.getString('database_name')
            dbOsh = ObjectStateHolder('sqldatabase')
            dbOsh.setAttribute(Queries.DATA_NAME,dbName)
            dbOsh.setContainer(sqlServerId)
            oshv.add(dbOsh)
            memberOsh = modeling.createLinkOSH('membership', resourceGroupOsh, dbOsh)
            oshv.add(memberOsh)
        rs.close()
        return oshv

    def getListenerIP(self, group_id, clusterGroupOsh, oshv):
        query = Util.replace(Queries.GET_LISTENER_IP_OF_AVAILABILITYGROUP, group_id)
        rs = self.connection.getTable(query)
        if rs.next():
            ip_name = rs.getString('ip_address')
            ipOsh = ObjectStateHolder('ip_address')
            ipOsh.setAttribute('name', ip_name)
            oshv.add(ipOsh)
            conOsh = modeling.createLinkOSH('containment', clusterGroupOsh, ipOsh)
            oshv.add(conOsh)
        rs.close()
        return oshv










