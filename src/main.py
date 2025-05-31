from mcp.server import FastMCP
from mcp.server.fastmcp.resources import FunctionResource

from src.AzurePostgreSQLMCP import AzurePostgreSQLMCP

if __name__ == "__main__":
    mcp = FastMCP("Flex PG Explorer - Read Only")
    azure_pg_mcp = AzurePostgreSQLMCP()
    azure_pg_mcp.init()

    mcp.add_tool(azure_pg_mcp.get_databases)
    mcp.add_tool(azure_pg_mcp.get_schemas)
    mcp.add_tool(azure_pg_mcp.query_data)
    mcp.add_tool(azure_pg_mcp.get_server_config)
    mcp.add_tool(azure_pg_mcp.get_server_parameter)

    databases_resource = FunctionResource(
        name=azure_pg_mcp.get_dbs_resource_uri(),
        uri=azure_pg_mcp.get_dbs_resource_uri(),
        description="List of databases in the server",
        mime_type="application/json",
        fn=azure_pg_mcp.get_databases_resource,
    )

    # Add the resource to the MCP server
    mcp.add_resource(databases_resource)
    mcp.run()