"""
Created on 2015-01-08

@author: Moritz Rupp <moritz.rupp@hp.com>

This module contains the used queries for collecting information about 
the Hana database topology.
"""

# Instance related
VIRTUAL_HOSTS = "select host from sys.m_host_information where key='net_hostnames' and value like '%??%'"
REAL_HOSTS = "select value from sys.m_host_information where key='net_realhostname' and host='??'"
INSTANCE_SID = "select value from sys.m_host_information where key='sid' and host='??'"
INSTANCE_NUMBER = "select value from sys.m_host_information where key='sapsystem' and host='??'"
INSTANCE_PORT = "select sql_port from sys.m_services where SERVICE_NAME='indexserver' and host='??'"
INSTANCE_PUBLIC_NAME = "select value from sys.m_host_information where key='net_publicname' and host='??'"
INSTANCE_IPADDRESS = "select value from sys.m_host_information where key='net_ip_addresses' and host='??'"
DB_TRACE_FILES = "select FILE_NAME, FILE_SIZE from sys.m_tracefiles where host='??'"
DB_LOG_FILES = "select FILE_NAME, TOTAL_SIZE from sys.m_volume_files where FILE_TYPE='LOG' and host='??'"
DB_DATA_FILES = "select FILE_NAME, USED_SIZE, TOTAL_SIZE, VOLUME_ID from sys.m_volume_files where FILE_TYPE='DATA' and host='??'"
INSTANCE_CONFIG = "select SECTION, KEY, VALUE, FILE_NAME, TENANT_NAME from sys.M_INIFILE_CONTENTS where layer_name<>'DEFAULT' and host='??'"
DATABASE_SERVICE = "select * from sys.m_services where host='??'"
# Database related
DATABASE_NAME = "select database_name from sys.m_database"
DATABASE_VERSION = "select version from sys.m_database"
DATABASE_STARTUP_TIME = "select start_time from sys.m_database"
CONNECTED_SYSTEMS = "select * from sys.M_CONNECTIONS"
DATABASE_REPLICATION = "select HOST,SECONDARY_HOST,REPLICATION_MODE from sys.M_SERVICE_REPLICATION group by HOST,SECONDARY_HOST,REPLICATION_MODE order by HOST"
DATABASE_INSTANCES = "select host from SYS.M_LANDSCAPE_HOST_CONFIGURATION"
DB_USERS_ALL = "select USER_NAME, CREATE_TIME from sys.users"
DB_USERS = "select USER_NAME, CREATE_TIME from sys.users where USER_MODE<>'EXTERNAL'"
DB_SCHEMAS_ALL = "select SCHEMA_NAME, SCHEMA_OWNER from sys.schemas"
DB_SCHEMAS = "SELECT s.SCHEMA_NAME,s.SCHEMA_OWNER FROM sys.schemas AS s INNER JOIN sys.users AS u ON s.SCHEMA_OWNER=u.USER_NAME where u.USER_MODE<>'EXTERNAL'"
DATABASE_CONFIG = "select SECTION, KEY, VALUE, FILE_NAME, TENANT_NAME from sys.M_INIFILE_CONTENTS where layer_name<>'DEFAULT' and host=''"
DATABASE_LICENSE = "select HARDWARE_KEY, INSTALL_NO, SYSTEM_NO, PRODUCT_NAME, PRODUCT_LIMIT, PRODUCT_USAGE, START_DATE, EXPIRATION_DATE, ENFORCED from sys.M_LICENSE"
# General queries
DATABASE_DOMAIN_NAMES = "select distinct VALUE from sys.M_HOST_INFORMATION where key = 'net_domain'"