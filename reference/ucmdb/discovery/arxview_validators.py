import re

def mandatory(value):
    return value is not None


def notEmpty(value):
    return bool(value)


def excludeIPDUCore(value):
    return value != 'HPIpduCore'

def validateIpAddress(value):
    if re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", value):
        return 1
    else:
        return 0    
    
def validatePortSpeed(value):
    if value:
        if value == "auto":
            value = 0
        else:
            value = re.sub('[^0-9\.]','', value)
    else:
        value = 0
    return value        

def parsePortSpeed(ci):
    value = ci.getValue("port_speed_gbit_sec")
    if value:
        if value == "auto":
            value = 0
        else:
            value = re.sub('[^0-9\.]','', value)
    else:
        value = 0
    return value        

def parseCurrentSpeed(ci):
        value = ci.getValue("current_speed")
        if value:
            if value == "auto":
                value = 0
            else:
                value = re.sub('[^0-9\.]','', value)
        else:
            value = 0 
        return value        

def parseConfiguredSpeed(ci):
            value = ci.getValue("configured_speed")
            if value:
                if value == "auto":
                    value = 0
                else:
                    value = re.sub('[^0-9\.]','', value)
            else:
                value = 0
            return value        



def validateHostName(value):
    return value

    
def parseHostName(ci):
    value = None
    if ci:         
        data = ci.getValue("host_name")                  
        value = data
        if data:
            stringIndex = data.find(".")
            if stringIndex != -1:
                value = data[:stringIndex]    
        else:
            value = data      
    return value 
    
def parseDomainName(ci):
    value = None
    if ci:
        data = ci.getValue("host_name")
        if data:
            stringIndex = data.find(".")
            if stringIndex != -1:
                value = data[stringIndex+1:]
    return value 
   