import sys
import os

print("🚀 Starting MCP Server...")
print(f"Python path: {sys.path}")
print(f"Working directory: {os.getcwd()}")
print(f"Environment variables: PGHOST={os.environ.get('PGHOST', 'NOT SET')}")

from mcp.server import FastMCP
from mcp.server.fastmcp.resources import FunctionResource

# Fixed import - use relative import
from .AzurePostgreSQLMCP import AzurePostgreSQLMCP

if __name__ == "__main__":
    print("📦 Creating FastMCP instance...")
    mcp = FastMCP("Flex PG Explorer - Read Only")

    print("🔧 Initializing Azure PostgreSQL MCP...")
    azure_pg_mcp = AzurePostgreSQLMCP()

    try:
        azure_pg_mcp.init()
        print("✅ Azure PostgreSQL MCP initialized successfully")
    except Exception as e:
        print(f"❌ Failed to initialize Azure PostgreSQL MCP: {e}")
        sys.exit(1)

    print("🛠️ Adding tools...")
    mcp.add_tool(azure_pg_mcp.get_databases)
    mcp.add_tool(azure_pg_mcp.get_schemas)
    mcp.add_tool(azure_pg_mcp.query_data)
    mcp.add_tool(azure_pg_mcp.get_server_config)
    mcp.add_tool(azure_pg_mcp.get_server_parameter)

    print("📚 Creating database resource...")
    databases_resource = FunctionResource(
        name=azure_pg_mcp.get_dbs_resource_uri(),
        uri=azure_pg_mcp.get_dbs_resource_uri(),
        description="List of databases in the server",
        mime_type="application/json",
        fn=azure_pg_mcp.get_databases_resource,
    )

    # Add the resource to the MCP server
    mcp.add_resource(databases_resource)

    print("🌐 Starting MCP server...")
    print("Server should be running now...")
    mcp.run()