"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT License.

MCP server for Azure Database for PostgreSQL - Flexible Server (Read-Only Version).
"""

import json
import logging
import os
import urllib.parse
import traceback

import psycopg
from azure.identity import DefaultAzureCredential
from azure.mgmt.postgresqlflexibleservers import PostgreSQLManagementClient

# Set up more detailed logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("azure_postgresql_mcp")


class AzurePostgreSQLMCP:
    def __init__(self):
        print("🔧 Initializing AzurePostgresMCP...")

        try:
            self.aad_in_use = os.environ.get("AZURE_USE_AAD", "False")
            print(f"   AAD mode: {self.aad_in_use}")

            self.dbhost = self.get_environ_variable("PGHOST")
            print(f"   Database host: {self.dbhost}")

            user_raw = self.get_environ_variable("PGUSER")
            self.dbuser = urllib.parse.quote(user_raw)
            print(f"   Database user: {user_raw} (encoded: {self.dbuser})")

            if self.aad_in_use == "True":
                print("   Setting up Azure AD authentication...")
                self.subscription_id = self.get_environ_variable("AZURE_SUBSCRIPTION_ID")
                self.resource_group_name = self.get_environ_variable("AZURE_RESOURCE_GROUP")
                self.server_name = (
                    self.dbhost.split(".", 1)[0] if "." in self.dbhost else self.dbhost
                )
                print(f"   Azure subscription: {self.subscription_id}")
                print(f"   Resource group: {self.resource_group_name}")
                print(f"   Server name: {self.server_name}")

                self.credential = DefaultAzureCredential()
                self.postgresql_client = PostgreSQLManagementClient(
                    self.credential, self.subscription_id
                )
                print("   ✅ Azure AD authentication setup complete")
            else:
                print("   Using password authentication")

            # Get password after AAD setup
            self.password = self.get_password()
            password_info = "SET" if self.password else "NOT SET"
            print(f"   Password: {password_info}")

            print("✅ AzurePostgreSQLMCP initialization complete")

        except Exception as e:
            print(f"❌ Failed to initialize AzurePostgreSQLMCP: {e}")
            print(f"📋 Traceback: {traceback.format_exc()}")
            raise

    @staticmethod
    def get_environ_variable(name: str):
        """Helper function to get environment variable or raise an error."""
        value = os.environ.get(name)
        if value is None:
            raise EnvironmentError(f"Environment variable {name} not found.")
        return value

    def get_password(self) -> str:
        """Get password based on the auth mode set"""
        try:
            if self.aad_in_use == "True":
                print("   Getting Azure AD token...")
                token = self.credential.get_token(
                    "https://ossrdbms-aad.database.windows.net/.default"
                ).token
                print("   ✅ Azure AD token obtained")
                return token
            else:
                password = self.get_environ_variable("PGPASSWORD")
                if not password:
                    raise ValueError("PGPASSWORD is empty")
                print("   ✅ Password obtained from environment")
                return password
        except Exception as e:
            print(f"   ❌ Failed to get password: {e}")
            raise

    def get_dbs_resource_uri(self):
        """Gets the resource URI exposed as MCP resource for getting list of dbs."""
        dbhost_normalized = (
            self.dbhost.split(".", 1)[0] if "." in self.dbhost else self.dbhost
        )
        return f"flexpg://{dbhost_normalized}/databases"

    def get_databases_internal(self) -> str:
        """Internal function which gets the list of all databases in a server instance."""
        connection_string = f"host={self.dbhost} user={self.dbuser} dbname='postgres' password={self.password}"

        try:
            print(f"🔍 Connecting to database...")
            safe_conn_str = connection_string.replace(self.password, '*' * min(len(self.password), 8))
            print(f"   Connection: {safe_conn_str}")

            with psycopg.connect(connection_string, connect_timeout=15) as conn:
                print("   ✅ Connected successfully")
                with conn.cursor() as cur:
                    print("   🔍 Executing query: SELECT datname FROM pg_database...")
                    cur.execute(
                        "SELECT datname FROM pg_database WHERE datistemplate = false;"
                    )
                    colnames = [desc[0] for desc in cur.description]
                    dbs = cur.fetchall()

                    result = json.dumps(
                        {
                            "columns": str(colnames),
                            "rows": "".join(str(row) for row in dbs),
                        }
                    )
                    print(f"   ✅ Query successful, found {len(dbs)} databases")
                    return result

        except psycopg.OperationalError as e:
            error_msg = f"Database connection failed: {e}"
            print(f"   ❌ {error_msg}")
            logger.error(error_msg)
            return json.dumps({"error": error_msg, "type": "connection_error"})
        except Exception as e:
            error_msg = f"Unexpected database error: {e}"
            print(f"   ❌ {error_msg}")
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return json.dumps({"error": error_msg, "type": "unexpected_error"})

    def get_databases_resource(self):
        """Gets list of databases as a resource"""
        return self.get_databases_internal()

    def get_databases(self):
        """Gets the list of all the databases in a server instance."""
        return self.get_databases_internal()

    def get_connection_uri(self, dbname: str) -> str:
        """Construct URI for connection."""
        return f"host={self.dbhost} dbname={dbname} user={self.dbuser} password={self.password}"

    def get_schemas(self, database: str):
        """Gets schemas of all the tables."""
        try:
            print(f"🔍 Getting schemas for database: {database}")
            with psycopg.connect(self.get_connection_uri(database), connect_timeout=15) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT table_name, column_name, data_type FROM information_schema.columns "
                        "WHERE table_schema = 'public' ORDER BY table_name, ordinal_position;"
                    )
                    colnames = [desc[0] for desc in cur.description]
                    tables = cur.fetchall()

                    result = json.dumps(
                        {
                            "columns": str(colnames),
                            "rows": "".join(str(row) for row in tables),
                        }
                    )
                    print(f"   ✅ Found {len(tables)} table columns")
                    return result

        except Exception as e:
            error_msg = f"Failed to get schemas for {database}: {e}"
            print(f"   ❌ {error_msg}")
            logger.error(error_msg)
            return json.dumps({"error": error_msg})

    def query_data(self, dbname: str, s: str) -> str:
        """Runs read queries on a database."""
        try:
            print(f"🔍 Executing query on {dbname}: {s[:100]}...")
            with psycopg.connect(self.get_connection_uri(dbname), connect_timeout=15) as conn:
                with conn.cursor() as cur:
                    cur.execute(s)
                    rows = cur.fetchall()
                    colnames = [desc[0] for desc in cur.description]

                    result = json.dumps(
                        {
                            "columns": str(colnames),
                            "rows": ",".join(str(row) for row in rows),
                        }
                    )
                    print(f"   ✅ Query returned {len(rows)} rows")
                    return result

        except Exception as e:
            error_msg = f"Query failed on {dbname}: {e}"
            print(f"   ❌ {error_msg}")
            logger.error(error_msg)
            return json.dumps({"error": error_msg})

    def get_server_config(self) -> str:
        """Gets the configuration of a server instance. [Available with Microsoft EntraID]"""
        if self.aad_in_use != "True":
            raise NotImplementedError(
                "This tool is available only with Microsoft EntraID"
            )

        try:
            print("🔍 Getting server configuration...")
            server = self.postgresql_client.servers.get(
                self.resource_group_name, self.server_name
            )

            result = json.dumps(
                {
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
                }
            )
            print("   ✅ Server configuration retrieved")
            return result

        except Exception as e:
            error_msg = f"Failed to get PostgreSQL server configuration: {e}"
            print(f"   ❌ {error_msg}")
            logger.error(error_msg)
            raise e

    def get_server_parameter(self, parameter_name: str) -> str:
        """Gets the value of a server parameter. [Available with Microsoft EntraID]"""
        if self.aad_in_use != "True":
            raise NotImplementedError(
                "This tool is available only with Microsoft EntraID"
            )

        try:
            print(f"🔍 Getting server parameter: {parameter_name}")
            configuration = self.postgresql_client.configurations.get(
                self.resource_group_name, self.server_name, parameter_name
            )

            result = json.dumps(
                {"param": configuration.name, "value": configuration.value}
            )
            print(f"   ✅ Parameter {parameter_name} = {configuration.value}")
            return result

        except Exception as e:
            error_msg = f"Failed to get PostgreSQL server parameter '{parameter_name}': {e}"
            print(f"   ❌ {error_msg}")
            logger.error(error_msg)
            raise e