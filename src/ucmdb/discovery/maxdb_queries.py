"""
Created on 2015-01-15

@author: Moritz Rupp <moritz.rupp@hp.com>

This module contains the used queries for collecting information about 
the MaxDB topology.
"""
# TODO
# Find out with the "node" query is required
# Find out which query should be used for the databse configuration
DATABASE_VERSION = "select MAJORVERSION||'.'||MINORVERSION||'.'||CORRECTIONLEVEL||'.'||BUILD as VERSION from SYSINFO.VERSION"
DATABASE_STARTUP_TIME = "select ONLINESTATEDATE from SYSINFO.INSTANCE"
DATABASE_PATH = "select left(VALUE, INDEX(VALUE, 'wrk')-2) as PATH from (select VALUE from SYSINFO.ACTIVECONFIGURATION where PARAMETERNAME='RUNDIRECTORY')"
DB_USERS = "select USERNAME, CREATEDATE from DOMAIN.USERS"
DB_SCHEMAS = "select SCHEMANAME, OWNER from DOMAIN.SCHEMAS"
DB_TRACE_FILES = "select VALUE from SYSINFO.ACTIVECONFIGURATION where PARAMETERNAME='RunDirectoryPath' or PARAMETERNAME='KernelTraceFile' or PARAMETERNAME='KernelDumpFileName' or PARAMETERNAME='EventFileName' or PARAMETERNAME='RTEDumpFileName'"
DB_LOG_FILES = "select PATH, CONFIGUREDSIZE from SYSINFO.LOGVOLUMES"
DB_DATA_FILES = "select PATH, CONFIGUREDSIZE, USABLESIZE, ID from SYSINFO.DATAVOLUMES"
DATABASE_CONFIGURATION = "select PARAMETERNAME,VALUE from SYSINFO.ACTIVECONFIGURATION"