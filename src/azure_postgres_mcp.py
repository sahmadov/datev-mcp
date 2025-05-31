import json
import logging
import os
import urllib.parse

import psycopg
from azure.identity import DefaultAzureCredential
from azure.mgmt.postgresqlflexibleservers import PostgreSQLManagementClient

logger = logging.getLogger("datev_mcp.azure_postgres")


class AzurePostgresMCP:
    def __init__(self):

        self.dbhost = self._get_env_var("PGHOST")
        self.dbuser = urllib.parse.quote(self._get_env_var("PGUSER"))
        self.use_aad = os.environ.get("AZURE_USE_AAD", "False").lower() == "true"

        if self.use_aad:
            self._setup_azure_auth()

    def _get_env_var(self, name: str) -> str:
        """Get required environment variable or raise error."""
        value = os.environ.get(name)
        if not value:
            raise EnvironmentError(f"Environment variable {name} not found.")
        return value

    def _setup_azure_auth(self):
        """Setup Azure AD authentication."""
        self.subscription_id = self._get_env_var("AZURE_SUBSCRIPTION_ID")
        self.resource_group_name = self._get_env_var("AZURE_RESOURCE_GROUP")
        self.server_name = self.dbhost.split(".", 1)[0]

        self.credential = DefaultAzureCredential()
        self.postgresql_client = PostgreSQLManagementClient(
            self.credential, self.subscription_id
        )

    def _get_password(self) -> str:
        """Get password based on authentication mode."""
        if self.use_aad:
            token = self.credential.get_token(
                "https://ossrdbms-aad.database.windows.net/.default"
            ).token
            return token
        else:
            return self._get_env_var("PGPASSWORD")

    def _get_connection_string(self, dbname: str = "postgres") -> str:
        """Get database connection string."""
        password = self._get_password()
        return f"host={self.dbhost} user={self.dbuser} dbname={dbname} password={password}"

    def _execute_query(self, query: str, dbname: str = "postgres") -> dict:
        """Execute a database query and return results."""
        try:
            conn_str = self._get_connection_string(dbname)

            with psycopg.connect(conn_str, connect_timeout=15) as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    rows = cur.fetchall()
                    columns = [desc[0] for desc in cur.description]

                    return {
                        "columns": columns,
                        "rows": [list(row) for row in rows],
                        "row_count": len(rows)
                    }

        except Exception as e:
            logger.error(f"Query failed: {e}")
            return {"error": str(e), "type": "query_error"}

    def get_databases(self) -> str:
        """Get list of all databases in the server instance."""
        logger.info("Getting database list...")

        query = "SELECT datname FROM pg_database WHERE datistemplate = false;"
        result = self._execute_query(query)

        return json.dumps(result)

    def get_schemas(self, database: str) -> str:
        """Get schemas of all tables in a database."""
        logger.info(f"Getting schemas for database: {database}")

        query = """
        SELECT table_name, column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'public' 
        ORDER BY table_name, ordinal_position;
        """

        result = self._execute_query(query, database)
        return json.dumps(result)

    def query_data(self, dbname: str, query: str) -> str:
        """Execute a read-only query on a database."""
        logger.info(f"Executing query on {dbname}: {query[:50]}...")

        # Basic read-only validation
        query_lower = query.lower().strip()
        if not query_lower.startswith('select'):
            return json.dumps({
                "error": "Only SELECT queries are allowed",
                "type": "validation_error"
            })

        result = self._execute_query(query, dbname)
        return json.dumps(result)

    def get_server_config(self) -> str:
        """Get server configuration (requires Azure AD)."""
        if not self.use_aad:
            raise NotImplementedError("This tool requires Microsoft EntraID authentication")

        logger.info("Getting server configuration...")

        try:
            server = self.postgresql_client.servers.get(
                self.resource_group_name, self.server_name
            )

            config = {
                "server": {
                    "name": server.name,
                    "location": server.location,
                    "version": server.version,
                    "sku": server.sku.name if server.sku else None,
                }
            }

            # Add storage info if available
            if hasattr(server, 'storage') and server.storage:
                config["server"]["storage_size_gb"] = server.storage.storage_size_gb

            # Add backup info if available
            if hasattr(server, 'backup') and server.backup:
                config["server"]["backup_retention_days"] = server.backup.backup_retention_days
                config["server"]["geo_redundant_backup"] = server.backup.geo_redundant_backup

            return json.dumps(config)

        except Exception as e:
            logger.error(f"Failed to get server configuration: {e}")
            raise

    def get_server_parameter(self, parameter_name: str) -> str:
        """Get a server parameter value (requires Azure AD)."""
        if not self.use_aad:
            raise NotImplementedError("This tool requires Microsoft EntraID authentication")

        logger.info(f"Getting server parameter: {parameter_name}")

        try:
            configuration = self.postgresql_client.configurations.get(
                self.resource_group_name, self.server_name, parameter_name
            )

            result = {
                "parameter": configuration.name,
                "value": configuration.value
            }

            return json.dumps(result)

        except Exception as e:
            logger.error(f"Failed to get parameter '{parameter_name}': {e}")
            raise