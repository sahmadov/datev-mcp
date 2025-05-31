import sys
import logging
import traceback
from mcp.server import FastMCP
from .azure_postgres_mcp import AzurePostgresMCP
from .db_utils import test_database_connection

# Configure single logger for the entire application
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("datev_mcp")

print("🚀 Starting MCP Server...")


def main():
    """Main application"""
    try:
        logger.info("Starting DATEV MCP Server initialization...")

        # Test database connection first
        if not test_database_connection():
            logger.error("Database connection failed!")
            sys.exit(1)

        logger.info("Database connection successful!")

        mcp = FastMCP("Azure PostgreSQL MCP Server")
        azure_pg_mcp = AzurePostgresMCP()

        # Add tools
        logger.info("Adding MCP tools...")
        mcp.add_tool(azure_pg_mcp.get_databases)
        mcp.add_tool(azure_pg_mcp.get_schemas)
        mcp.add_tool(azure_pg_mcp.query_data)
        mcp.add_tool(azure_pg_mcp.get_server_config)
        mcp.add_tool(azure_pg_mcp.get_server_parameter)

        # Start the server
        logger.info("MCP Server is now running!")
        mcp.run()

    except KeyboardInterrupt:
        logger.info("Server stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()