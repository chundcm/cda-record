#coding=utf-8
import entity
import modeling
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector


class HasId:
    def __init__(self, id):
        if id is None:
            raise ValueError("Id is empty")
        self.__id = id

    def getId(self):
        return self.__id


class Project(HasId, entity.HasOsh):
    def __init__(self, id_):
        HasId.__init__(self, id_)
        entity.HasOsh.__init__(self)

    def acceptVisitor(self, visitor):
        return visitor.visitProject(self)


class Region(entity.HasName, entity.HasOsh):
    def __init__(self, name):
        entity.HasName.__init__(self)
        entity.HasOsh.__init__(self)
        self.setName(name)
        self.__zones = []

    def addZone(self, zone):
        if not zone:
            raise ValueError("Zone is not specified")
        self.__zones.append(zone)

    def getZones(self):
        return self.__zones[:]

    def acceptVisitor(self, visitor):
        return visitor.visitRegion(self)

    def __repr__(self):
        return 'Region("%s")' % self.getName()


class Zone(entity.HasName, entity.HasOsh):
    def __init__(self, name, regionName, state=None):
        entity.HasName.__init__(self)
        entity.HasOsh.__init__(self)
        self.setName(name)
        if not regionName:
            raise ValueError("Region name is empty")
        self.__regionName = regionName

    def getRegionName(self):
        return str(self.__regionName)

    def acceptVisitor(self, visitor):
        return visitor.visitZone(self)

    def __repr__(self):
        return 'Zone("%s", "%s")' % (self.getName(), self.__regionName)


class Builder:

    def __buildLocationOsh(self, name, locationType, typeStr):
        osh = ObjectStateHolder('location')
        osh.setAttribute('name', name)
        osh.setAttribute('data_note', typeStr)
        osh.setAttribute('location_type', locationType)
        return osh

    def buildLocationOsh(self, name, typeStr):
        return self.__buildLocationOsh(name, 'undefined', typeStr)

    def visitProject(self, project):
        osh = ObjectStateHolder('googlecloudproject')
        osh.setAttribute('name', project.getId())
        return osh

    def visitZone(self, availabilityZone):
        return self.buildLocationOsh(availabilityZone.getName(), 'Zone')

    def visitRegion(self, region):
        return self.buildLocationOsh(region.getName(), 'Region')


class Reporter:
    def __init__(self, locationBuilder):
        self.__builder = locationBuilder

    def _createOshVector(self):
        return ObjectStateHolderVector()

    def reportProject(self, account):
        if not account:
            raise ValueError("Project is not specified")
        return account.build(self.__builder)

    def reportRegion(self, region):
        if not region:
            raise ValueError("Region is not specified")
        vector = ObjectStateHolderVector()
        vector.add(region.build(self.__builder))
        return vector

    def reportZoneInRegion(self, region, zone):
        if not (region and region.getOsh()):
            raise ValueError("Region is not specified or not built")
        if not zone:
            raise ValueError("Zone is not specified")
        vector = self._createOshVector()
        regionOsh = region.getOsh()
        vector.add(regionOsh)
        vector.add(zone.build(self.__builder))
        vector.add(modeling.createLinkOSH('containment', regionOsh, zone.getOsh()))
        return vector
