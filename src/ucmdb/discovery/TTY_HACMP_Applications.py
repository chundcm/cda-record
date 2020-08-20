#coding=utf-8
##############################################################################
##  TTY_HACMP_Applications.py : version 1.4
##
##  Description:
##    Uses TTY to discover IBM HACMP cluster details on servers
##
##  Version info:
##    1.0 : Re written by CORD to adhear to the UCMDB Cluster Model (Pat Odom), Jan 2010
##    1.1 : Revised to fix problems in the modeling of the cluster (Pat Odom) May 2010
##    1.2 : Corrected problems found in release cycle for CP7. (Pat Odom)
##    1.3 : Corrected problems after code review (Pat Odom)
##    1.4 : Fixed problem during QA cycle (Pat Odom)
##    1.5 : Fixed problem with discovery stopping if their was no secondary node (QCIM1H37510: 	HACMP Application Discovery hangs up)
##
##############################################################################

import re
import modeling
import logger
import netutils
import errorcodes
import errorobject
import errormessages

## from Java
from java.lang import Exception as JavaException

## from HP
from shellutils import ShellUtils
from appilog.common.system.types.vectors import ObjectStateHolderVector
from appilog.common.system.types import ObjectStateHolder

##############################################
##  Concatenate strings w/ any object type  ##
##############################################
def concatenate(*args):
    return ''.join(map(str,args))

#####################################
##  Pat Odom Temporary to fake out the commands
#####################################
def  simulatecmd(file):
# Open the file for read
    buffer = ""
    input = open(file,'r')
    lines = input.readlines()
    for line in lines:
        buffer = concatenate(buffer,line)
    input.close()
    return buffer

##############################################
##  Get Host OSH based on the IP ##
##############################################
def gethostOSH(hostIP, hostName):
    hostOSH = modeling.createHostOSH(hostIP, 'node', None, hostName)
    return hostOSH


##############################################
##  Get Cluster OSH based on the name ##
##############################################
def getclusterOSH(cluster):
    clusterOSH = ObjectStateHolder('hacmpcluster')
    clusterOSH.setAttribute('data_name', cluster)
    return clusterOSH

#################################
##  Create Cluster Service
##  These can be thought of as virtual instances of servers in the cluster that are running applications at
## any specific point in time. They are shown in the model to represent where the applications are currently
## allocated but since HACMP cluster applications are dynamic this can change. So the discovery is a point in time
## discovery.
#################################
def addclusterserverOSH( nodeEntry, cluster):
    clusterserverOSH = ObjectStateHolder('clusteredservice')
    clusterserverOSH.setAttribute('data_name', nodeEntry)
    clusterserverOSH.setAttribute('host_key', '%s:%s' % (cluster, nodeEntry))
    clusterserverOSH.setBoolAttribute('host_iscomplete', 1)
    return clusterserverOSH

########################################
# Create the Volume Group OSHs
########################################
def addvolumegroupOSH(HostOSH, myVec, volumegroup, resourcegroupOSH):
    volumegroupOSH = ObjectStateHolder('hacmpresource')
    volumegroupOSH.setAttribute('data_name', volumegroup)
    volumegroupOSH.setAttribute('resource_type', 'shared volume group')
    volumegroupOSH.setContainer(resourcegroupOSH)
    myVec.add(volumegroupOSH)
    hostvgOSH = ObjectStateHolder('volumegroup')
    hostvgOSH.setAttribute('data_name', volumegroup)
    hostvgOSH.setContainer(HostOSH)
    myVec.add(hostvgOSH)
    dependOSH = modeling.createLinkOSH('depend', volumegroupOSH, hostvgOSH)
    myVec.add(dependOSH)
    return myVec

########################################
# Create the Software and Node OSHs
########################################
def addsoftwaretoclusterOSH(ClusterOSH, myVec, primarynode_ip, primarynode_name, secondarynode_ip, secondarynode_name, serviceOSH):
    if primarynode_ip and primarynode_name:
        PrimaryhostOSH = gethostOSH(primarynode_ip, primarynode_name)
        priclusterSoftwareOSH = ObjectStateHolder('failoverclustersoftware')
        priclusterSoftwareOSH.setAttribute('data_name', 'HACMP Cluster Software')
        priclusterSoftwareOSH.setContainer(PrimaryhostOSH)
        myVec.add(PrimaryhostOSH)
        myVec.add(priclusterSoftwareOSH)
        memberOSH = modeling.createLinkOSH('member', ClusterOSH, priclusterSoftwareOSH)
        myVec.add(memberOSH)
        runOSH = modeling.createLinkOSH('run', priclusterSoftwareOSH, serviceOSH)
        myVec.add(runOSH)
        potrunOSH = modeling.createLinkOSH('potentially_run', priclusterSoftwareOSH, serviceOSH)
        myVec.add(potrunOSH)
    if secondarynode_ip and secondarynode_name:
        SecondaryhostOSH = gethostOSH(secondarynode_ip, secondarynode_name)
        secclusterSoftwareOSH = ObjectStateHolder('failoverclustersoftware')
        secclusterSoftwareOSH.setAttribute('data_name', 'HACMP Cluster Software')
        secclusterSoftwareOSH.setContainer(SecondaryhostOSH)
        myVec.add(SecondaryhostOSH)
        myVec.add(secclusterSoftwareOSH)
        memberOSH = modeling.createLinkOSH('member', ClusterOSH, secclusterSoftwareOSH)
        myVec.add(memberOSH)
        potrunOSH = modeling.createLinkOSH('potentially_run', secclusterSoftwareOSH, serviceOSH)
        myVec.add(potrunOSH)
    myVec.add(ClusterOSH)
    containedOSH = modeling.createLinkOSH('contained', ClusterOSH, serviceOSH)
    myVec.add(containedOSH)
    return myVec

########################################
# Create the Service IP OSHs
########################################
def addserviceIPOSH(myVec, service_ip, serviceOSH):
    serviceIPOSH = modeling.createIpOSH(service_ip)
    containedOSH = modeling.createLinkOSH('contained', serviceOSH, serviceIPOSH)
    myVec.add(containedOSH)
    return myVec

########################################
# Create the Resource Group OSH
########################################
def addresourcegroupOSH(myVec, resourceGroup, AttributeDictionary, serviceOSH):
    resourcegroupOSH = ObjectStateHolder('hacmpgroup')
    resourcegroupOSH.setAttribute('data_name', resourceGroup)
    if AttributeDictionary.has_key('hacmpgroup_fallbackpolicy'):
        resourcegroupOSH.setAttribute('hacmpgroup_fallbackpolicy', AttributeDictionary['hacmpgroup_fallbackpolicy'])
    if AttributeDictionary.has_key('hacmpgroup_falloverpolicy'):
        resourcegroupOSH.setAttribute('hacmpgroup_falloverpolicy', AttributeDictionary['hacmpgroup_falloverpolicy'])
    if AttributeDictionary.has_key('hacmpgroup_startpolicy'):
        resourcegroupOSH.setAttribute('hacmpgroup_startuppolicy', AttributeDictionary['hacmpgroup_startpolicy'])
    resourcegroupOSH.setContainer(serviceOSH)
    return resourcegroupOSH

########################################
# Create the Application Resource OSH
########################################
def addapplicationresourceOSH(myVec, application, AttributeDictionary, resourcegroupOSH):
    resourceOSH = ObjectStateHolder('hacmpappresource')
    resourceOSH.setAttribute('data_name', application)
    resourceOSH.setAttribute('resource_type', 'application')
    if AttributeDictionary.has_key('hacmpresource_start'):
        resourceOSH.setAttribute('hacmpresource_start', AttributeDictionary['hacmpresource_start'])
    if AttributeDictionary.has_key('hacmpresource_stop'):
        resourceOSH.setAttribute('hacmpresource_stop', AttributeDictionary['hacmpresource_stop'])
    resourceOSH.setContainer(resourcegroupOSH)
    myVec.add(resourceOSH)
    return myVec

########################################
# Create the Service Resources OSHs
########################################
def addserviceresourceOSH(shell, resourceDictionary, HostOSH, myVec, service, resourcegroupOSH):
    SvcDictionary = service.service_details_dict
    for svcif_name in SvcDictionary.keys():
        (svcif_name, svcif_ip, svcif_device, svcif_interface, svcif_node, svcif_network, svcif_status, service_label) = SvcDictionary[svcif_name]
        if resourceDictionary.has_key(svcif_name):
            (name, type, network, nettype, attr, node, ipaddr, haddr, interfacename, globalname, netmask, hb_addr, site_name) = resourceDictionary[svcif_name]
            if (type == 'boot'):
                svcifOSH = ObjectStateHolder('hacmpresource')
                svcifOSH.setAttribute('data_name', svcif_name)
                svcifOSH.setAttribute('resource_type', 'interface')
                svcifOSH.setAttribute('resource_subtype', type)
                svcifOSH.setContainer(resourcegroupOSH)
                myVec.add(svcifOSH)
                mac_address = None
                mac_address = getMAC(shell, interfacename)
                if (mac_address != None):
                    hostifOSH = modeling.createInterfaceOSH(mac_address, name=interfacename)
                    hostifOSH.setContainer(HostOSH)
                    myVec.add(hostifOSH)
                    dependOSH = modeling.createLinkOSH('depend', svcifOSH, hostifOSH)
                    myVec.add(dependOSH)
            if (type == 'service') and (nettype == 'diskhb'):
                svcifOSH = ObjectStateHolder('hacmpresource')
                svcifOSH.setAttribute('data_name', svcif_name)
                svcifOSH.setAttribute('resource_type', 'network')
                svcifOSH.setAttribute('resource_subtype', type)
                svcifOSH.setContainer(resourcegroupOSH)
                myVec.add(svcifOSH)
                phydiskhb = ipaddr.split('/')
                lenphydisk =len(phydiskhb)
                phydiskOSH =   ObjectStateHolder('physicalvolume')
                phydiskOSH.setAttribute('data_name', phydiskhb[lenphydisk-1] )
                phydiskOSH.setContainer(HostOSH)
                myVec.add(phydiskOSH)
                dependOSH = modeling.createLinkOSH('depend', svcifOSH, phydiskOSH)
                myVec.add(dependOSH)
    svclblOSH = ObjectStateHolder('hacmpresource')
    svclblOSH.setAttribute('data_name', service.service_label)
    svclblOSH.setAttribute('resource_type', 'service label')
    svclblOSH.setContainer(resourcegroupOSH)
    myVec.add(svclblOSH)
    return myVec

#########################################################################
##  Perform Hostname lookup on clustered node via the /etc/hosts file
##  We use the /etc/hosts file becuase this is the recommended resolve
##  method for hostnames in an HACMP cluster
#########################################################################
def hostnamelookup(shell, namesToResolve,  Framework):
    filename = '/etc/hosts'
    cmdResult = None
    nodeDictionary = {}
    try:
        #rawCmdResult = simulatecmd('c:/etchosts.txt')
        rawCmdResult = shell.safecat(filename)
        cmdResult = rawCmdResult.strip()
    except:
        msg = "Failed reading /etc/host file."
        errobj = errorobject.createError(errorcodes.COMMAND_OUTPUT_VERIFICATION_FAILED, None, msg)
        logger.reportWarningObject(errobj)
        logger.debug(msg)
        return nodeDictionary
    keywords = ['Permission\s*Denied', 'Cannot Open']
    for keyword in keywords:
        if re.search(keyword,cmdResult,re.I):
            msg = "Permission failure."
            errobj = errorobject.createError(errorcodes.PERMISSION_DENIED, None, msg)
            logger.reportErrorObject(errobj)
            logger.debug(msg)
            return nodeDictionary
    if (re.search('\r\n', cmdResult)):
        cmdResult = cmdResult.split('\r\n')
    elif (re.search('\n', cmdResult)):
        cmdResult = cmdResult.split('\n')

    ## Only parse the node names at first, for resolution before
    ## trying to map the actual interface content
    for line in cmdResult:
        try:
            line = line.strip()
            ## Parse out headers and blank lines
            if not line or re.match('#', line):
                continue
            Address = None
            ## Remove trailing comments
            if (re.search('#', line)):
                tmp = line.split('#', 1)
                line = tmp[0]
            tmp = line.split()

            # IP Address will be the first entry
            # Names will follow corresponding to the  IP.
            # We will validate the IP then search for the name to match
            # If we find it then we will add it into our node dictionary
            # Alphanumeric representation of the host:
            #    hostname, FQDN, alias....
            # Most objects will only have two names
            # The order (of FQDN, short name, aliases) is not
            #necessarily standard across platforms/versions
            if len(tmp) > 1:
                Address = tmp[0]
                tmp = tmp[1:]
            if not Address or not netutils.isValidIp(Address) or netutils.isLocalIp(Address):
                continue
            for entry in tmp:
                if entry.lower() in namesToResolve:
                    logger.debug ('Name to resolve  ',entry,' Address = ', Address)
                    if nodeDictionary.has_key(entry):
                        logger.debug(concatenate('   From /etc/host output:  Node ', entry.lower(), ' already has an address; ignoring ' , Address))
                    else:
                        nodeDictionary[entry.lower()] = Address
                        logger.debug(concatenate('   From /etc/host output:  Adding ', entry.lower(), ' to the list with address: ', Address))
        except:
            msg = "Failed to parse etc/host file."
            errobj = errorobject.createError(errorcodes.COMMAND_OUTPUT_VERIFICATION_FAILED, None, msg)
            logger.reportWarningObject(errobj)
            logger.debug(msg)

    return nodeDictionary

#######################################################
##  Get the resource info for each host in the cluster
#######################################################
def getresourceinfo(shell,  cllsif_command):
    resourceDictionary = {}
    cmdResult = None
    rawCmdResult = None
    try:
        cmdForInterfaces = cllsif_command
        logger.debug(concatenate(' Executing command: ', cmdForInterfaces))
        rawCmdResult = shell.execCmd(cmdForInterfaces)
        cmdResult = rawCmdResult.strip()
    except:
        msg = "Command Failure - Unable to get cluster resource information "
        errobj = errorobject.createError(errorcodes.FAILED_GETTING_INFORMATION, None, msg)
        logger.reportErrorObject(errobj)
        logger.debug(msg)
        return resourceDictionary
    if not cmdResult:
        msg = "CLLSIF output was empty, unable to get cluster resources."
        errobj = errorobject.createError(errorcodes.COMMAND_OUTPUT_VERIFICATION_FAILED, None, msg)
        logger.reportErrorObject(errobj)
        logger.debug(msg)
        return resourceDictionary
    keywords = ['not found']
    for keyword in keywords:
        if re.search(keyword,cmdResult,re.I):
            msg = "cllsif command not in path, check cldisp command parameter and sudo path"
            errobj = errorobject.createError(errorcodes.COMMAND_OUTPUT_VERIFICATION_FAILED, None, msg)
            logger.reportErrorObject(errobj)
            logger.debug(msg)
            return resourceDictionary
    if (re.search('\r\n', cmdResult)):
        cmdResult = cmdResult.split('\r\n')
    else:
        cmdResult = cmdResult.split('\n')

    ## Build a resource Dictionary from the cllsif data
    for line in cmdResult:
        line = line.strip()
        ## Parse out headers and blank lines
        if not line or re.match('#', line):
            continue
        name = type = network = nettype = attr = node = ipaddr = haddr = interfacename = globalname = netmask = hb_addr = site_name = None
        data = line.split(':')

        if (len(data) == 12):
            [name, type, network, nettype, attr, node, ipaddr, haddr, interfacename, globalname, netmask, hb_addr] = data
        elif (len(data) == 13):
            [name, type, network, nettype, attr, node, ipaddr, haddr, interfacename, globalname, netmask, hb_addr, site_name] = data

        resourceDictionary[name] = (name, type, network, nettype, attr, node, ipaddr, haddr, interfacename, globalname, netmask, hb_addr, site_name)
    return resourceDictionary

################################################
## Get MAC Address for Interface              ##
################################################
def getMAC(shell, int_name):
    cmdResult = None
    rawCmdResult = None
    mac = None
    entstat = None
    try:
        entstat_command = concatenate('entstat ', int_name)

        logger.debug(concatenate(' Executing command: ', entstat_command))
        entstat = shell.execCmd(entstat_command)

        if entstat != None:
            m = re.search('Device Type: (.+)', entstat)
            description = None
            if(m):
                description = m.group(1).strip()
            m = re.search('Hardware Address: ([0-9a-f:]{17})', entstat)
            rawMac = None
            if(m):
                rawMac = m.group(1)
                mac = netutils.parseMac(rawMac)
    except:
        msg = " Failed getting MAC address for interface '%s'" % int_name
        errobj = errorobject.createError(errorcodes.FAILED_GETTING_INFORMATION, None, msg)
        logger.reportWarningObject(errobj)
        logger.debug(msg)
        return None

    return mac

###################################################################################
# Create the OSHs for the Applications and Services and attach them to the Topology
###################################################################################
def createserviceapplicationOSH (shell, appDictionary, resourceDictionary, services_dict, HostOSH, ClusterOSH,  Framework):

    myVec = ObjectStateHolderVector()
    namesToResolve = []
    nodeDictionary = {}
    
    # Loop over all the applications in the dictionary and build the appropriate OSHs
    for application in appDictionary.values():
        #Cluster, resourceGroup, primarynode, secondarynode , service_name, service_ip, volumeGrouplist, AttributeDictionary, SvcDictionary = appDictionary[application]

        # Get all the necessary OSHs that are parents to build the services and application OSHs
        if not application.primary_node or not application.secondary_node:
            continue
        primarynode_ip = None
        secondarynode_ip = None
        namesToResolve.append(application.primary_node.lower())
        namesToResolve.append(application.secondary_node.lower())
        nodeDictionary = hostnamelookup(shell, namesToResolve, Framework)
        if nodeDictionary.has_key(application.primary_node.lower()):
            primarynode_ip = nodeDictionary[application.primary_node.lower()]
        else:
            msg = concatenate(" Cannot resolve primary node for cluster from the /etc/hosts file, discovery aborted.", application.primary_node)
            errobj = errorobject.createError(errorcodes.FAILED_GETTING_INFORMATION, None, msg)
            logger.reportErrorObject(errobj)
            logger.debug(msg)
            return None
        if  nodeDictionary.has_key(application.secondary_node.lower()):
            secondarynode_ip = nodeDictionary[application.secondary_node.lower()]
        else:
            msg = concatenate(" Cannot resolve secondary node for cluster from the /etc/hosts file", application.secondary_node)
            #errobj = errorobject.createError(errorcodes.FAILED_GETTING_INFORMATION, None, msg)
            #logger.reportErrorObject(errobj)
            logger.debug(msg)
            #return None

        if not primarynode_ip :
            msg = concatenate(" Error getting ip address for node in cluster, discovery aborted", application.secondary_node)
            errobj = errorobject.createError(errorcodes.FAILED_GETTING_INFORMATION, None, msg)
            logger.reportErrorObject(errobj)
            logger.debug(msg)
            return None

        # Recreate the Host and the Software OSHs so that we can connect
        # the appropriate services to them
        
        for service in application.services:
            serviceOSH = addclusterserverOSH(service.service_name, application.cluster_name)
            myVec.add(serviceOSH)
            myVec = addsoftwaretoclusterOSH(ClusterOSH, myVec, primarynode_ip, application.primary_node, secondarynode_ip, application.secondary_node, serviceOSH)

            # Create the IP OSH for the services and link them to the service
            myVec = addserviceIPOSH(myVec, service.service_ip, serviceOSH)

            # Create the resource group for the Application and link it to the service
            resourcegroupOSH = addresourcegroupOSH(myVec, application.resource_group_name, application.generic_attributes_dict, serviceOSH)
            myVec.add(resourcegroupOSH)

            # Create the application resource for the Application and link to the Group
            myVec = addapplicationresourceOSH(myVec, application.application_name, application.generic_attributes_dict, resourcegroupOSH)

            # Create the service interfaces and attach them to the resource group. Also create depend links to physical host resources.
            myVec = addserviceresourceOSH(shell, resourceDictionary, HostOSH, myVec, service, resourcegroupOSH)

            # ReCreate the Host Volume groups so we can link them to the HACMP Resource group VG
            for volumegroup in application.volume_groups:
                myVec = addvolumegroupOSH(HostOSH, myVec,  volumegroup, resourcegroupOSH)

    return myVec

######################################################
##  Discover the HACMP Applications and Services    ##
######################################################
class HacmpService:
    def __init__(self, service_name = None, service_ip = None, service_label = None, service_details_dict = {}):
        self.service_name = service_name
        self.service_ip = service_ip
        self.service_label = service_label
        self.service_details_dict = service_details_dict #iface name to properties list
        
    def __str__(self):
        return "HacmpService( service_name = '%s', service_ip = '%s', service_label = '%s', service_details_dict = %s" % \
                (self.service_name, self.service_ip, self.service_label, self.service_details_dict)

    def __repr__(self):
        return self.__str__()
        
class HacmpApplication:
    def __init__(self, application_name = None, cluster_name = None, generic_attributes_dict = None, resource_group_name = None, \
                 primary_node = None, secondary_node = None, services = None, volume_groups = None):
        self.application_name = application_name
        self.cluster_name = cluster_name
        self.generic_attributes_dict = generic_attributes_dict or {}# attr name to value map
        self.resource_group_name = resource_group_name
        self.primary_node = primary_node
        self.secondary_node = secondary_node
        self.services = services or []#list of HacmpService instances
        self.volume_groups = volume_groups or [] #list of volume group names
        
    def __str__(self):
        return "HacmpApplication( application_name = '%s', cluster_name = '%s', generic_attributes_dict = %s, resource_group_name = '%s', primary_node = '%s', secondary_node = '%s', services = '%s', volume_groups = '%s'" % \
            (self.application_name, self.cluster_name, self.generic_attributes_dict, self.resource_group_name, self.primary_node, self.secondary_node, self.services, self.volume_groups)
        
    def __repr__(self):
        return self.__str__()
    
def get_application_info_output(shell, cldisp_command, Framework):
    msg = None
    errobj = None
    cmdResult = None
    try:
        logger.debug(concatenate(' Executing command: ', cldisp_command))
        rawCmdResult = shell.execCmd(cldisp_command)
        cmdResult = rawCmdResult.strip()
    except:
        msg = "Command Failure - Unable to get cluster topology information "
        errobj = errorobject.createError(errorcodes.FAILED_GETTING_INFORMATION, None, msg)
    if re.search('cannot execute', cmdResult):
        msg = "cldisp commamd failed. Verify permissions and sudo access"
        errobj = errorobject.createError(errorcodes.COMMAND_OUTPUT_VERIFICATION_FAILED, None, msg)
    if not cmdResult:
        msg = "cldisp command output was empty, unable to get cluster topology."
        errobj = errorobject.createError(errorcodes.COMMAND_OUTPUT_VERIFICATION_FAILED, None, msg)
    if re.search('not found', cmdResult):
        msg = "cldisp commamd is not found."
        errobj = errorobject.createError(errorcodes.COMMAND_OUTPUT_VERIFICATION_FAILED, None, msg)
    if re.search('Cluster\s+services:\s+inactive', cmdResult, re.I):
        msg = "Cluster services are inactive"
        errobj = errorobject.createError(errorcodes.COMMAND_OUTPUT_VERIFICATION_FAILED, None, msg)
    
    if errobj and msg:
        logger.reportErrorObject(errobj)
        logger.debug(msg)
        return None
    return cmdResult

def parse_cluster_section(output):
    applications = {}
    if not output:
        return applications
    
    m = re.search(r'Cluster\s+(\w+)\s+provides the following applications:(.*)', output)
    cluster_name = m.group(1).strip()
    #application_names = m.group(2).split()
    
    app_blocks = re.split("\r\n\r\n|\n\n", output)
    for app_block in app_blocks:
        if app_block and app_block.find("Application:") != -1:
            application = parse_application_section(app_block)
            if application:
                application.cluster_name = cluster_name
                applications[application.application_name] = application
    return applications

def parse_application_generic_attributes(output):
    attributeDictionary = {}
    if not output:
        return attributeDictionary
    
    #Generic application parameters
    m = re.search('is started by\s+(.*)', output)
    if m:
        attributeDictionary['hacmpresource_start'] = m.group(1)
        
    m = re.search('is stopped by\s+(.*)', output)
    if m:
        attributeDictionary['hacmpresource_stop'] = m.group(1)

    m = re.search(r'Startup:\s*(.+)', output, re.I)
    if (m):
        startup_policy = m.group(1).strip()
        attributeDictionary['hacmpgroup_startpolicy'] = startup_policy

    m = re.search(r'Fallover:\s*(.+)', output, re.I)
    if (m):
        fallover_policy = m.group(1).strip()
        attributeDictionary['hacmpgroup_falloverpolicy'] = fallover_policy

    m = re.search(r'Fallback:\s*(.+)', output, re.I)
    if (m):
        fallback_policy = m.group(1).strip()
        attributeDictionary['hacmpgroup_fallbackpolicy'] = fallback_policy

    m = re.search(r'State of.+:\s*(.+)', output, re.I)
    if (m):
        application_state = m.group(1).strip()
        attributeDictionary['hacmpresource_state'] = application_state

    return attributeDictionary

def parse_service_info(output, service_label):
    svcDictionary = {}
    if not output:
        return svcDictionary
    for line in [ x and x.strip() for x in re.split('[\r\n]+', output)]:
        m = re.match('(\S+)\s*\{(.*)\}', line)
        if  (m):
            svcif_name = ''
            svcif_status = ''
            svcif_ip = ''
            svcif_device = ''
            svcif_interface = ''
            svcif_node = ''
            svcif_network = ''
            svcif_name = m.group(1).strip()
            svcif_status = m.group(2)

            if svcif_name != '':
                svcDictionary[svcif_name] = (svcif_name, svcif_ip, svcif_device, svcif_interface, svcif_node, svcif_network,svcif_status, service_label)
            continue

        m = re.match(r'with IP address:\s*(\S+)', line,re.I)
        if  (m):
            svcif_ip = m.group(1)
            if svcif_name != '':
                svcDictionary[svcif_name] = (svcif_name, svcif_ip, svcif_device, svcif_interface, svcif_node, svcif_network,svcif_status, service_label)
            continue

        m = re.match(r'device:\s*(\S+)', line,re.I)
        if  (m):
            svcif_device = m.group(1)
            if svcif_name != '':
                svcDictionary[svcif_name] = (svcif_name, svcif_ip, svcif_device, svcif_interface, svcif_node, svcif_network,svcif_status, service_label)
            continue

        m = re.match(r'on interface:\s*(\S+)', line,re.I)
        if  (m):
            svcif_interface = m.group(1)
            if svcif_name != '':
                svcDictionary[svcif_name] = (svcif_name, svcif_ip, svcif_device, svcif_interface, svcif_node, svcif_network,svcif_status, service_label)
            continue

        m = re.match(r'on node:\s*(\S+)', line,re.I)
        if  (m):
            svcif_node = m.group(1)
            if svcif_name != '':
                svcDictionary[svcif_name] = (svcif_name, svcif_ip, svcif_device, svcif_interface, svcif_node, svcif_network,svcif_status, service_label)
            continue

        m = re.match(r'on network:\s*(\S+)', line,re.I)
        if  (m):
            svcif_network = m.group(1)
            if svcif_name != '':
                svcDictionary[svcif_name] = (svcif_name, svcif_ip, svcif_device, svcif_interface, svcif_node, svcif_network,svcif_status, service_label)
            continue
    return svcDictionary

def parse_service_label(label_dinition, output):
    if not (label_dinition and output):
        return None
    m = re.search(r'(.*)\((.*)\).*$', label_dinition.strip())
    if (m):
        hacmp_service = HacmpService()
        hacmp_service.service_name = m.group(1).strip()
        hacmp_service.service_ip = m.group(2)
        hacmp_service.service_label = label_dinition.replace ('{online}','').strip()
        logger.debug('Application service ', hacmp_service.service_name,'   ', hacmp_service.service_ip)
        hacmp_service.service_details_dict = parse_service_info(output, hacmp_service.service_label)
        return hacmp_service
    
def parse_application_section(output):
    if not output:
        return
    hacmp_application = HacmpApplication()
    
    m = re.search(r'Application:\s*(\S+)', output)
    if not m:
        logger.warn("Skipping application block since no application name can be found")
        return None
    
    hacmp_application.application_name = m.group(1)
        
    if re.search(r'No nodes configured to provide', output, re.I):
        logger.warn("Skipping application %s as no nodes are configured to it." % hacmp_application.application_name)
        return None
    
    hacmp_application.generic_attributes_dict = parse_application_generic_attributes(output)
    
    ## Resource Group policy section
    m = re.search(r'This application is part of resource group\s*\'(\S+)\'', output, re.I)
    if (m):
        hacmp_application.resource_group_name = m.group(1)

    m = re.search(r'The node that will provide\s+(\S+)\s+if\s+(\S+).*is:\s+(\S+)', output, re.I)
    if (m):
        logger.debug(concatenate('Current node for Application ', m.group(1), ' is ',  m.group(2), ' failover node is ', m.group(3)))
        hacmp_application.primary_node = m.group(2)
        hacmp_application.secondary_node = m.group(3)
    
    #logger.debug(output)
    service_labels_match = re.search("Service Labels(.+)Shared Volume Groups", output, re.DOTALL) or re.search("Service Labels(.+)", output, re.DOTALL) #extract Service Labels part
    if service_labels_match:
        service_label_blocks = re.split(r'(.+\(.+\)\s+\{\w+\}\s+)', service_labels_match.group(1)) #split by service label definition string
        if service_label_blocks and len(service_label_blocks) > 2:
            for i in range(1, len(service_label_blocks), 2):
                logger.debug("For ServiceLabel will parse: ")
                logger.debug(service_label_blocks[i] )
                logger.debug(service_label_blocks[i+1])
                service_label = parse_service_label(service_label_blocks[i] , service_label_blocks[i+1]) # restore and parse each service label
                logger.debug("Parsed out Service Label %s" % service_label)
                if service_label:
                    hacmp_application.services.append(service_label)
        
    volume_groups_match = re.search("Shared Volume Groups:(.+)", output, re.DOTALL)
    if volume_groups_match:
        hacmp_application.volume_groups = [x.strip() for x in volume_groups_match.group(1).split() if x and x.strip()]
    
    return hacmp_application

def parse_application_info_output(output):
    if not output:
        return
    
    app_match = re.search("APPLICATIONS[\r\n]*#+(.+?)####.+", output, re.DOTALL) #get Applications Part
    topo_match = re.search("(TOPOLOGY)[\r\n]*#+(.+)",  output, re.DOTALL) #get Topology part
    applications_dict = {}
    if app_match:
        applications_string = app_match.group(1)
        elems = re.split('(Cluster .+? provides the following applications:)', applications_string) #split output per each defined cluster, not sure if there might be more than one cluster
        if elems and len(elems) > 2:
            applications_dict = parse_cluster_section(elems[1]+elems[2])
    if topo_match:
        services_dict = parse_service_info(topo_match.group(1), None)
    return applications_dict, services_dict
    
def get_application_info(shell, cldisp_command, Framework):
    result = {}
    output = get_application_info_output(shell, cldisp_command, Framework)
    if output:
        result = parse_application_info_output(output)
    return result

##############################
##  Discovery  MAIN  block  ##
##############################
def DiscoveryMain(Framework):
    OSHVResult = ObjectStateHolderVector()
    logger.info('Starting HACMP Applications')
    hostIP = Framework.getDestinationAttribute('ip_address')
    logger.debug ('Host IP: ',hostIP)
    cluster =  Framework.getDestinationAttribute('cluster')
    hostOS = Framework.getDestinationAttribute('host_os')
    hostOS = hostOS or 'NA'
    protocolName = Framework.getDestinationAttribute('Protocol')
    hostId = Framework.getDestinationAttribute('hostId')
    ##  Get Parameter Section
    cldisp_command = Framework.getParameter('cldisp_command') or 'cldisp'
    cllsif_command = Framework.getParameter('cllsif_command') or 'cllsif'

    try:
        client = Framework.createClient()
        shell = ShellUtils(client)
        #   If we get  good client connection , run the client commands to get the Application information for the cluster
        HostOSH = modeling.createOshByCmdbIdString('host', hostId)
        ClusterOSH = getclusterOSH(cluster)
        appDictionary, services_dict = get_application_info(shell,  cldisp_command,  Framework)
        logger.debug( appDictionary )
        resourceDictionary = getresourceinfo(shell, cllsif_command)
        OSHVResult.addAll(createserviceapplicationOSH (shell, appDictionary, resourceDictionary, services_dict, HostOSH, ClusterOSH, Framework))
        client.close()
    except JavaException, ex:
        strException = ex.getMessage()
        logger.debugException('')
        errormessages.resolveAndReport(strException, protocolName, Framework)

    logger.info('Finished HACMP Applications')
    return OSHVResult