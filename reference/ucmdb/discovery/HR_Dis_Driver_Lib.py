#coding=utf-8
import re
import logger
import wmiutils
from appilog.common.system.types import ObjectStateHolder

def discoverDriverByWmi(shell, OSHVResult, hostOSH):
    wmiProvider = wmiutils.getWmiProvider(shell)
    queryBuilder = wmiProvider.getBuilder('Win32_PnPSignedDriver')
    queryBuilder.useSplitListOutput(False)
    queryProperties = { 'CompatID':'compat_id',
                        'Description':'description',
                        'DeviceClass':'device_class',
                        'DeviceID':'device_id',
                        'DeviceName':'device_name',
                        'DevLoader':'dev_loader',
                        'DriverDate':'driver_date',
                        'DriverName':'driver_name',
                        'DriverProviderName':'driver_provider_name',
                        'DriverVersion':'driver_version',
                        'FriendlyName':'friendly_name',
                        'HardWareID':'hardware_id',
                        'InfName':'inf_name',
                        'InstallDate':'install_date',
                        'IsSigned':'is_signed',
                        'Location':'location',
                        'Manufacturer':'manufacturer',
                        'Name':'name',
                        'PDO':'pdo',
                        'Signer':'signer'}
    for property in queryProperties.keys():
        queryBuilder.addWmiObjectProperties(property)
    wmiAgent = wmiProvider.getAgent()

    driverItems = []
    try:
        driverItems = wmiAgent.getWmiData(queryBuilder, 60000)
    except:
        logger.debugException('Failed to get driver information via wmi')
        return False

    for driverItem in driverItems:
        driverOsh = ObjectStateHolder('windows_device_driver')
        for property in queryProperties.keys():
            if hasattr(driverItem, property):
                propertyValue = getattr(driverItem, property)
                if isinstance(propertyValue, basestring):
                    propertyValue = propertyValue and propertyValue.strip() or None
                    if propertyValue:
                        if re.search('&amp;', propertyValue):
                            propertyValue = propertyValue.replace('&amp;', '&')
                    driverOsh.setAttribute(queryProperties[property], propertyValue)
        driverOsh.setContainer(hostOSH)

        deviceID = driverItem.DeviceID and driverItem.DeviceID.strip() or None
        deviceName = driverItem.DeviceName and driverItem.DeviceName.strip() or None
        logger.debug('Found Device Driver: %s, DeviceID= %s' % (deviceName, deviceID))

        OSHVResult.add(driverOsh)
    return True