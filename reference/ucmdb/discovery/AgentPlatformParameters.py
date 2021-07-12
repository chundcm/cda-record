#coding=utf-8
import InventoryUtils
import shellutils
import file_system

from java.lang import String

PLUGIN_CONFIG_PATH = {
    "aix": "/usr/lpp/microfocus/Discovery/Plugins/usage/",
    "macosx": "/Library/LaunchDaemons/DiscoveryAgent/Plugins/usage/",
    "windows-x86": "%ProgramFiles%\\Micro Focus\\Discovery Agent\\Plugins\\usage\\",
    "windows": "%ProgramFiles(x86)%\\Micro Focus\\Discovery Agent\\Plugins\\usage\\",
    "default-windows": "%ProgramFiles%\\Micro Focus\\Discovery Agent\\Plugins\\usage\\",
    "default": "/opt/microfocus/Discovery/Plugins/usage/",
    }

PLUGIN_CONFIG_PATH_HP = {
    "aix": "/usr/lpp/HP/Discovery/Plugins/usage/",
    "macosx": "/Library/LaunchDaemons/HPDiscoveryAgent/Plugins/usage/",
    "windows-x86": "%ProgramFiles%\\Hewlett-Packard\\Discovery Agent\\Plugins\\usage\\",
    "windows": "%ProgramFiles(x86)%\\Hewlett-Packard\\Discovery Agent\\Plugins\\usage\\",
    "default-windows": "%ProgramFiles%\\Hewlett-Packard\\Discovery Agent\\Plugins\\usage\\",
    "default": "/opt/HP/Discovery/Plugins/usage/",
    }

RENAME_CMD = {
    "windows": "del \"{PATH}{NEW_FILENAME}\" & rename \"{PATH}{OLD_FILENAME}\" \"{NEW_FILENAME}\"",
    "default": "mv -f \"{PATH}{OLD_FILENAME}\" \"{PATH}{NEW_FILENAME}\""
    }

DISCUSGE_INI_CONTENT = "[Utilization]\r\nPERIOD="

PLUGIN_INI_CONTENT = "[PLUGIN]\r\nEXE=discusge\r\nARGS=-d \"%DATADIR%\"\r\nSTARTUP="

def getAgentConfigurationPath(Framework):
    platform = Framework.getProperty(InventoryUtils.STATE_PROPERTY_PLATFORM)
    architecture = Framework.getProperty(InventoryUtils.STATE_PROPERTY_ARCHITECTURE)

    client = Framework.getConnectedClient()
    shell = shellutils.ShellUtils(client)
    if platform == "windows":
        if not shell.is64BitMachine():
            architecture = "x86"
        else:
            architecture = "x64"
    agentconfPath = getAgentConfigurationPathByPlatform(platform, architecture)

    fs = file_system.createFileSystem(shell)
    if fs.exists(agentconfPath):
        return agentconfPath
    else:
        return getHPAgentConfigurationPathByPlatform(platform, architecture)

def getHPAgentConfigurationPathByPlatform(platform, architecture):
    platformArch = platform
    if architecture:
        platformArch = platformArch + "-" + architecture
    if PLUGIN_CONFIG_PATH_HP.get(platformArch):
        return PLUGIN_CONFIG_PATH_HP.get(platformArch)
    else:
        if PLUGIN_CONFIG_PATH_HP.get(platform):
            return PLUGIN_CONFIG_PATH_HP.get(platform)
        else:
            if platform == "windows":
                return PLUGIN_CONFIG_PATH_HP.get('default-windows')
            else:
                return PLUGIN_CONFIG_PATH_HP.get('default')

def getAgentConfigurationPathByPlatform(platform, architecture):
    platformArch = platform
    if architecture:
        platformArch = platformArch + "-" + architecture
    if PLUGIN_CONFIG_PATH.get(platformArch):
        return PLUGIN_CONFIG_PATH.get(platformArch)
    else:
        if PLUGIN_CONFIG_PATH.get(platform):
            return PLUGIN_CONFIG_PATH.get(platform)
        else:
            if platform == "windows":
                return PLUGIN_CONFIG_PATH.get('default-windows')
            else:
                return PLUGIN_CONFIG_PATH.get('default')

def getRenameCMD(Framework, path, oldName, newName):
    platform = Framework.getProperty(InventoryUtils.STATE_PROPERTY_PLATFORM)
    architecture = Framework.getProperty(InventoryUtils.STATE_PROPERTY_ARCHITECTURE)

    platformArch = platform
    if architecture:
        platformArch = platformArch + "-" + architecture
    cmd = ""
    if RENAME_CMD.get(platformArch):
        cmd = RENAME_CMD.get(platformArch)
    else:
        if RENAME_CMD.get(platform):
            cmd = RENAME_CMD.get(platform)
        else:
            cmd = RENAME_CMD.get('default')
    cmd = String(cmd).replace(String("{PATH}"), String(path))
    cmd = String(cmd).replace(String("{OLD_FILENAME}"), String(oldName))
    cmd = String(cmd).replace(String("{NEW_FILENAME}"), String(newName))
    return cmd
