"""
Created on 2015-01-15

@author: Moritz Rupp <moritz.rupp@hp.com>
"""
import maxdb_queries
import modeling

from appilog.common.system.types import ObjectStateHolder


class MaxDB(object):
    """Containing all information and relations of a logical MaxDB CIT.

    This class is a wrapper for the MaxDBs to be created as ObjectStateHolders.
    It provides functions for retrieving the information related to a MaxDB
    like Log files, Trace files, Configurations, Users, etc.

    Attributes:
        connection: Object of type DriverMaxDBConnection providing the
            JDBC interface
        credential_id: The CMDB ID of the used credentials as string
        name: The name of the MaxDB
        port: The listening port of the MaxDB
        path: The application installed path of the MaxDB
        startup_time: The last startup time of the MaxDB
    """

    def __init__(self, client, maxdb_osh):
        """Inits the MaxDB with the information directly
        retrieved from the database using SQL queries.

        The ObjectStateHolders for the MaxDB and the host are
        created.
        """
        self.client = client
        self._maxdb_osh = maxdb_osh


    def get_maxdb(self):
        self.path = self.get_db_path()
        self.startup_time = self.get_db_startup_time()
        self._maxdb_osh.setStringAttribute('application_path', self.path)
        self._maxdb_osh.setDateAttribute('startup_time', self.startup_time)
        self._maxdb_osh.setStringAttribute('discovered_product_name', 'SAP MaxDB')
        return self._maxdb_osh

    def get_db_path(self):
        path = None
        resultSet = self.client.executeQuery(maxdb_queries.DATABASE_CONFIGURATION)
        if resultSet.next():
            path = resultSet.getString(1)
        resultSet.close()
        return path

    def get_db_startup_time(self):
        startup_time = None
        resultSet = self.client.executeQuery(maxdb_queries.DATABASE_STARTUP_TIME)
        if resultSet.next():
            startup_time = resultSet.getDate(1)
        resultSet.close()
        return startup_time


    def get_db_trace_files(self):
        """Retrieves the DB trace files as ObjectStateHolder objects.

        Returns:
            A list containing the ObjectStateHolder objects.

        Raises:
            SQLException: An error occurred during the SQL connection
        """
        files = []
        rs = self.client.executeQuery(maxdb_queries.DB_TRACE_FILES)
        while rs.next():
            db_tracefile_osh = ObjectStateHolder('db_trace_file')
            db_tracefile_osh.setContainer(self._maxdb_osh)
            db_tracefile_osh.setStringAttribute('name', rs.getString('VALUE'))

            files.append(db_tracefile_osh)
        rs.close()
        return files

    def get_db_log_files(self):
        """Retrieves the DB log files as ObjectStateHolder objects.

        Returns:
            A list containing the ObjectStateHolder objects.

        Raises:
            SQLException: An error occurred during the SQL connection
        """
        files = []

        rs = self.client.executeQuery(maxdb_queries.DB_LOG_FILES)

        while rs.next():
            db_logfile_osh = ObjectStateHolder('db_log_file')
            db_logfile_osh.setContainer(self._maxdb_osh)
            db_logfile_osh.setStringAttribute('name', rs.getString('PATH'))
            db_logfile_osh.setLongAttribute('size', rs.getString('CONFIGUREDSIZE'))
            files.append(db_logfile_osh)
        rs.close()

        return files

    def get_db_data_files(self):
        """Retrieves the DB data files as ObjectStateHolder objects.

        Returns:
            A list containing the ObjectStateHolder objects.

        Raises:
            SQLException: An error occurred during the SQL connection
        """
        files = []

        rs = self.client.executeQuery(maxdb_queries.DB_DATA_FILES)

        while rs.next():
            db_datafile_osh = ObjectStateHolder('dbdatafile')

            db_datafile_osh.setContainer(self._maxdb_osh)
            db_datafile_osh.setStringAttribute('name', rs.getString('PATH'))
            db_datafile_osh.setStringAttribute('dbdatafile_byte', rs.getString('CONFIGUREDSIZE'))
            db_datafile_osh.setStringAttribute('dbdatafile_maxbytes', rs.getString('USABLESIZE'))
            db_datafile_osh.setIntegerAttribute('dbdatafile_fileid', rs.getString('ID'))

            files.append(db_datafile_osh)
        rs.close()

        return files

    def get_db_configuration(self):
        """Retrieves the MaxDB configurations as ObjectStateHolder objects.

        Returns:
            A list containing the ObjectStateHolder objects.

        Raises:
            SQLException: An error occurred during the SQL connection
        """
        db_config_osh = ObjectStateHolder('db_configuration')
        db_config_osh.setContainer(self._maxdb_osh)

        rs = self.client.executeQuery(maxdb_queries.DATABASE_CONFIGURATION)
        while rs.next():
            parameter = rs.getString('PARAMETERNAME') + "=" + rs.getString('VALUE')
            db_config_osh.addAttributeToList("parameters", parameter)
        rs.close()

        return db_config_osh

    def get_db_users(self):
        """Retrieves the DB users as ObjectStateHolder objects.

        Returns:
            A list containing the ObjectStateHolder objects.

        Raises:
            SQLException: An error occurred during the SQL connection
        """
        users = []

        rs = self.client.executeQuery(maxdb_queries.DB_USERS)

        while rs.next():
            db_user_osh = ObjectStateHolder('dbuser')

            db_user_osh.setContainer(self._maxdb_osh)
            db_user_osh.setStringAttribute('name', rs.getString('USERNAME'))
            db_user_osh.setDateAttribute('dbuser_created', rs.getDate('CREATEDATE'))

            users.append(db_user_osh)
        rs.close()

        return users

    def get_db_schema(self, dbusers):
        """Retrieves the DB schemas and the relations to there owners
        as ObjectStateHolder objects.

        Returns:
            Two list containing the ObjectStateHolder objects for the schemas
            and secondly for the links between the user and the schema.

        Raises:
            SQLException: An error occurred during the SQL connection
        """
        schemas = []

        user_dict = dict((u.getAttribute('name').getStringValue(), u) for u in dbusers)

        rs = self.client.executeQuery(maxdb_queries.DB_SCHEMAS)

        while rs.next():
            db_schema_osh = ObjectStateHolder('database_instance')

            db_schema_osh.setContainer(self._maxdb_osh)
            db_schema_osh.setStringAttribute('name', rs.getString('SCHEMANAME'))

            # Get the user which is Owner of the schema and create the ownership
            schema_owner = rs.getString('OWNER')
            schemas.append(db_schema_osh)
            if schema_owner in user_dict:
                link = modeling.createLinkOSH('ownership', user_dict[schema_owner], db_schema_osh)
                schemas.append(link)
        rs.close()
        return schemas
