import json
import logging
import os
import sys
import urllib.parse
from datetime import datetime
from typing import Optional

import psycopg
from azure.identity import DefaultAzureCredential
from azure.mgmt.postgresqlflexibleservers import PostgreSQLManagementClient
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.resources import FunctionResource

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('azure_postgresql_mcp.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Create logger for this module
logger = logging.getLogger(__name__)

# Set Azure SDK logging to ERROR to reduce noise
azure_logger = logging.getLogger("azure")
azure_logger.setLevel(logging.ERROR)


class AzurePostgreSQLMCP:
    def __init__(self):
        logger.info("Initializing AzurePostgreSQLMCP instance")
        self.aad_in_use: Optional[str] = None
        self.dbhost: str = ""
        self.dbuser: str = ""
        self.password: str = ""
        self.subscription_id: Optional[str] = None
        self.resource_group_name: Optional[str] = None
        self.server_name: Optional[str] = None
        self.credential: Optional[DefaultAzureCredential] = None
        self.postgresql_client: Optional[PostgreSQLManagementClient] = None

    def init(self):
        """Initialize the MCP server with configuration."""
        logger.info("Starting initialization process")

        try:
            self.aad_in_use = os.environ.get("AZURE_USE_AAD")
            logger.info(f"Azure AD usage: {self.aad_in_use}")

            self.dbhost = self.get_environ_variable("PGHOST")
            logger.info(f"Database host: {self.dbhost}")

            self.dbuser = urllib.parse.quote(self.get_environ_variable("PGUSER"))
            logger.info(f"Database user: {self.dbuser}")

            if self.aad_in_use == "True":
                logger.info("Configuring Azure AD authentication")
                self.subscription_id = self.get_environ_variable("AZURE_SUBSCRIPTION_ID")
                self.resource_group_name = self.get_environ_variable("AZURE_RESOURCE_GROUP")
                self.server_name = (
                    self.dbhost.split(".", 1)[0] if "." in self.dbhost else self.dbhost
                )
                logger.info(f"Azure configuration - Subscription: {self.subscription_id[:8]}..., "
                           f"Resource Group: {self.resource_group_name}, Server: {self.server_name}")

                self.credential = DefaultAzureCredential()
                self.postgresql_client = PostgreSQLManagementClient(
                    self.credential, self.subscription_id
                )
                logger.info("Azure AD authentication configured successfully")
            else:
                logger.info("Using password-based authentication")

            # Password initialization should be done after checking if AAD is in use
            # because then we need to get the token using the credential
            # which is only available after the above block.
            self.password = self.get_password()
            logger.info("Password/token obtained successfully")

            logger.info("Initialization completed successfully")

        except Exception as e:
            logger.error(f"Failed to initialize AzurePostgreSQLMCP: {str(e)}")
            raise

    @staticmethod
    def get_environ_variable(name: str):
        """Helper function to get environment variable or raise an error."""
        logger.debug(f"Getting environment variable: {name}")
        value = os.environ.get(name)
        if value is None:
            logger.error(f"Environment variable {name} not found")
            raise EnvironmentError(f"Environment variable {name} not found.")
        logger.debug(f"Environment variable {name} found")
        return value

    def get_password(self) -> str:
        """Get password based on the auth mode set"""
        logger.debug("Getting password/token")
        try:
            if self.aad_in_use == "True":
                logger.debug("Requesting Azure AD token")
                token = self.credential.get_token(
                    "https://ossrdbms-aad.database.windows.net/.default"
                ).token
                logger.info("Azure AD token obtained successfully")
                return token
            else:
                logger.debug("Using password from environment variable")
                password = self.get_environ_variable("PGPASSWORD")
                logger.info("Password retrieved from environment")
                return password
        except Exception as e:
            logger.error(f"Failed to get password/token: {str(e)}")
            raise

    def get_dbs_resource_uri(self):
        """Gets the resource URI exposed as MCP resource for getting list of dbs."""
        dbhost_normalized = (
            self.dbhost.split(".", 1)[0] if "." in self.dbhost else self.dbhost
        )
        uri = f"flexpg://{dbhost_normalized}/databases"
        logger.debug(f"Generated resource URI: {uri}")
        return uri

    def get_databases_internal(self) -> str:
        """Internal function which gets the list of all databases in a server instance."""
        logger.info("Fetching list of databases")
        start_time = datetime.now()

        try:
            connection_string = f"host={self.dbhost} user={self.dbuser} dbname='postgres' password={self.password}"
            logger.debug(f"Connecting to database with host: {self.dbhost}, user: {self.dbuser}")

            with psycopg.connect(connection_string) as conn:
                logger.debug("Database connection established")
                with conn.cursor() as cur:
                    query = "SELECT datname FROM pg_database WHERE datistemplate = false;"
                    logger.debug(f"Executing query: {query}")

                    cur.execute(query)
                    colnames = [desc[0] for desc in cur.description]
                    dbs = cur.fetchall()

                    result = json.dumps({
                        "columns": str(colnames),
                        "rows": "".join(str(row) for row in dbs),
                    })

                    elapsed_time = (datetime.now() - start_time).total_seconds()
                    logger.info(f"Successfully retrieved {len(dbs)} databases in {elapsed_time:.2f}s")
                    logger.debug(f"Database names: {[db[0] for db in dbs]}")

                    return result

        except Exception as e:
            elapsed_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Error fetching databases after {elapsed_time:.2f}s: {str(e)}")
            return ""

    def get_databases_resource(self):
        """Gets list of databases as a resource"""
        logger.info("Resource request: getting databases")
        return self.get_databases_internal()

    def get_databases(self):
        """Gets the list of all the databases in a server instance."""
        logger.info("Tool request: getting databases")
        return self.get_databases_internal()

    def get_connection_uri(self, dbname: str) -> str:
        """Construct URI for connection."""
        uri = f"host={self.dbhost} dbname={dbname} user={self.dbuser} password={self.password}"
        logger.debug(f"Generated connection URI for database: {dbname}")
        return uri

    def get_schemas(self, database: str):
        """Gets schemas of all the tables."""
        logger.info(f"Fetching schemas for database: {database}")
        start_time = datetime.now()

        try:
            logger.debug(f"Connecting to database: {database}")
            with psycopg.connect(self.get_connection_uri(database)) as conn:
                logger.debug("Database connection established")
                with conn.cursor() as cur:
                    query = ("SELECT table_name, column_name, data_type FROM information_schema.columns "
                            "WHERE table_schema = 'public' ORDER BY table_name, ordinal_position;")
                    logger.debug(f"Executing schema query: {query}")

                    cur.execute(query)
                    colnames = [desc[0] for desc in cur.description]
                    tables = cur.fetchall()

                    result = json.dumps({
                        "columns": str(colnames),
                        "rows": "".join(str(row) for row in tables),
                    })

                    elapsed_time = (datetime.now() - start_time).total_seconds()
                    unique_tables = set(row[0] for row in tables)
                    logger.info(f"Successfully retrieved schemas for {len(unique_tables)} tables "
                             f"with {len(tables)} columns in {elapsed_time:.2f}s")
                    logger.debug(f"Tables found: {list(unique_tables)}")

                    return result

        except Exception as e:
            elapsed_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Error fetching schemas for database '{database}' after {elapsed_time:.2f}s: {str(e)}")
            return ""

    def query_data(self, dbname: str, s: str) -> str:
        """Runs read queries on a database."""
        logger.info(f"Executing query on database: {dbname}")
        logger.debug(f"Query: {s}")
        start_time = datetime.now()

        try:
            with psycopg.connect(self.get_connection_uri(dbname)) as conn:
                logger.debug("Database connection established")
                with conn.cursor() as cur:
                    cur.execute(s)
                    rows = cur.fetchall()
                    colnames = [desc[0] for desc in cur.description]

                    result = json.dumps({
                        "columns": str(colnames),
                        "rows": ",".join(str(row) for row in rows),
                    })

                    elapsed_time = (datetime.now() - start_time).total_seconds()
                    logger.info(f"Query executed successfully. Returned {len(rows)} rows "
                             f"with {len(colnames)} columns in {elapsed_time:.2f}s")

                    return result

        except Exception as e:
            elapsed_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Error executing query on database '{dbname}' after {elapsed_time:.2f}s: {str(e)}")
            logger.error(f"Failed query: {s}")
            return ""

    def exec_and_commit(self, dbname: str, s: str) -> None:
        """Internal function to execute and commit transaction."""
        logger.info(f"Executing and committing transaction on database: {dbname}")
        logger.debug(f"SQL: {s}")
        start_time = datetime.now()

        try:
            with psycopg.connect(self.get_connection_uri(dbname)) as conn:
                logger.debug("Database connection established")
                with conn.cursor() as cur:
                    cur.execute(s)
                    conn.commit()

                    elapsed_time = (datetime.now() - start_time).total_seconds()
                    logger.info(f"Transaction executed and committed successfully in {elapsed_time:.2f}s")

        except Exception as e:
            elapsed_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Error executing transaction on database '{dbname}' after {elapsed_time:.2f}s: {str(e)}")
            logger.error(f"Failed SQL: {s}")
            raise

    def update_values(self, dbname: str, s: str):
        """Updates or inserts values into a table."""
        logger.info(f"Updating values in database: {dbname}")
        self.exec_and_commit(dbname, s)

    def create_table(self, dbname: str, s: str):
        """Creates a table in a database."""
        logger.info(f"Creating table in database: {dbname}")
        self.exec_and_commit(dbname, s)

    def drop_table(self, dbname: str, s: str):
        """Drops a table in a database."""
        logger.info(f"Dropping table in database: {dbname}")
        self.exec_and_commit(dbname, s)

    def get_server_config(self) -> str:
        """Gets the configuration of a server instance. [Available with Microsoft EntraID]"""
        logger.info("Fetching server configuration")

        if self.aad_in_use:
            start_time = datetime.now()
            try:
                logger.debug(f"Getting server configuration for: {self.server_name}")
                server = self.postgresql_client.servers.get(
                    self.resource_group_name, self.server_name
                )

                result = json.dumps({
                    "server": {
                        "name": server.name,
                        "location": server.location,
                        "version": server.version,
                        "sku": server.sku.name,
                        "storage_profile": {
                            "storage_size_gb": server.storage.storage_size_gb,
                            "backup_retention_days": server.backup.backup_retention_days,
                            "geo_redundant_backup": server.backup.geo_redundant_backup,
                        },
                    },
                })

                elapsed_time = (datetime.now() - start_time).total_seconds()
                logger.info(f"Server configuration retrieved successfully in {elapsed_time:.2f}s")
                logger.debug(f"Server details - Name: {server.name}, Version: {server.version}, "
                           f"Location: {server.location}")

                return result

            except Exception as e:
                elapsed_time = (datetime.now() - start_time).total_seconds()
                logger.error(f"Failed to get PostgreSQL server configuration after {elapsed_time:.2f}s: {e}")
                raise e
        else:
            logger.warning("get_server_config called but Azure AD is not enabled")
            raise NotImplementedError(
                "This tool is available only with Microsoft EntraID"
            )

    def get_server_parameter(self, parameter_name: str) -> str:
        """Gets the value of a server parameter. [Available with Microsoft EntraID]"""
        logger.info(f"Fetching server parameter: {parameter_name}")

        if self.aad_in_use:
            start_time = datetime.now()
            try:
                logger.debug(f"Getting parameter '{parameter_name}' for server: {self.server_name}")
                configuration = self.postgresql_client.configurations.get(
                    self.resource_group_name, self.server_name, parameter_name
                )

                result = json.dumps({
                    "param": configuration.name,
                    "value": configuration.value
                })

                elapsed_time = (datetime.now() - start_time).total_seconds()
                logger.info(f"Server parameter '{parameter_name}' retrieved successfully in {elapsed_time:.2f}s")
                logger.debug(f"Parameter value: {configuration.value}")

                return result

            except Exception as e:
                elapsed_time = (datetime.now() - start_time).total_seconds()
                logger.error(f"Failed to get PostgreSQL server parameter '{parameter_name}' "
                           f"after {elapsed_time:.2f}s: {e}")
                raise e
        else:
            logger.warning(f"get_server_parameter called for '{parameter_name}' but Azure AD is not enabled")
            raise NotImplementedError(
                "This tool is available only with Microsoft EntraID"
            )


if __name__ == "__main__":
    logger.info("Starting Azure PostgreSQL MCP Server")

    try:
        mcp = FastMCP("Flex PG Explorer")
        logger.info("FastMCP server instance created")

        azure_pg_mcp = AzurePostgreSQLMCP()
        azure_pg_mcp.init()
        logger.info("AzurePostgreSQLMCP initialized successfully")

        # Add tools
        logger.info("Adding tools to MCP server")
        mcp.add_tool(azure_pg_mcp.get_databases)
        mcp.add_tool(azure_pg_mcp.get_schemas)
        mcp.add_tool(azure_pg_mcp.query_data)
        mcp.add_tool(azure_pg_mcp.update_values)
        mcp.add_tool(azure_pg_mcp.create_table)
        mcp.add_tool(azure_pg_mcp.drop_table)
        mcp.add_tool(azure_pg_mcp.get_server_config)
        mcp.add_tool(azure_pg_mcp.get_server_parameter)
        logger.info("All tools added successfully")

        # Add resource
        logger.info("Adding databases resource to MCP server")
        databases_resource = FunctionResource(
            name=azure_pg_mcp.get_dbs_resource_uri(),
            uri=azure_pg_mcp.get_dbs_resource_uri(),
            description="List of databases in the server",
            mime_type="application/json",
            fn=azure_pg_mcp.get_databases_resource,
        )
        mcp.add_resource(databases_resource)
        logger.info("Databases resource added successfully")

        logger.info("Starting MCP server...")
        mcp.run()

    except Exception as e:
        logger.critical(f"Failed to start MCP server: {str(e)}")
        sys.exit(1)