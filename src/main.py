import sys
import os
import logging
import traceback
import urllib.parse
import psycopg

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


def test_database_connection():
    """Test database connection with detailed error reporting"""
    logger.info("Testing Database Connection...")
    print("=" * 50)

    # Show environment variables (safely)
    env_vars = {
        'PGHOST': os.environ.get('PGHOST', 'NOT SET'),
        'PGUSER': os.environ.get('PGUSER', 'NOT SET'),
        'PGPASSWORD': 'SET' if os.environ.get('PGPASSWORD') else 'NOT SET',
        'AZURE_USE_AAD': os.environ.get('AZURE_USE_AAD', 'NOT SET')
    }

    logger.info("Environment Variables:")
    for key, value in env_vars.items():
        if key == 'PGPASSWORD' and value == 'SET':
            actual_password = os.environ.get('PGPASSWORD', '')
            logger.info(f"  {key}: {'*' * len(actual_password)} (length: {len(actual_password)})")
        else:
            logger.info(f"  {key}: {value}")

    # Check required variables
    host = os.environ.get('PGHOST')
    user_raw = os.environ.get('PGUSER')
    password = os.environ.get('PGPASSWORD')

    missing_vars = []
    if not host:
        missing_vars.append('PGHOST')
    if not user_raw:
        missing_vars.append('PGUSER')
    if not password:
        missing_vars.append('PGPASSWORD')

    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return False

    # URL encode the username
    user = urllib.parse.quote(user_raw)

    # Test connection with different SSL modes
    connection_attempts = [
        ("SSL Required", f"host={host} user={user} dbname=postgres password={password} sslmode=require"),
        ("SSL Preferred", f"host={host} user={user} dbname=postgres password={password} sslmode=prefer"),
        ("Basic Connection", f"host={host} user={user} dbname=postgres password={password}")
    ]

    for attempt_name, conn_str in connection_attempts:
        logger.info(f"Trying: {attempt_name}")
        safe_conn_str = conn_str.replace(password, '*' * len(password))
        logger.debug(f"Connection string: {safe_conn_str}")

        try:
            with psycopg.connect(conn_str, connect_timeout=10) as conn:
                logger.info("Database connection successful!")

                # Test query
                with conn.cursor() as cur:
                    cur.execute("SELECT current_database(), version();")
                    db_name, version = cur.fetchone()
                    logger.info(f"Connected to database: {db_name}")
                    logger.info(f"Server version: {version[:60]}...")

                return True

        except psycopg.OperationalError as e:
            error_msg = str(e)
            logger.error(f"Connection failed: {error_msg}")

            # Provide helpful suggestions
            if "password authentication failed" in error_msg.lower():
                server_name = host.split('.')[0] if '.' in host else host
                logger.warning(f"Try username with server suffix: {user_raw}@{server_name}")
            elif "ssl" in error_msg.lower():
                logger.warning("SSL/TLS issue - Azure requires SSL connections")
            elif "timeout" in error_msg.lower() or "network" in error_msg.lower():
                logger.warning("Network issue - check firewall and server status")

        except Exception as e:
            logger.error(f"Unexpected connection error: {e}")

    return False


def main():
    """Main application"""
    try:
        logger.info("Starting DATEV MCP Server initialization...")

        # Test database connection first
        if not test_database_connection():
            logger.error("Database connection failed!")
            logger.warning("Please fix the database connection and try again.")
            logger.info("Container will stay alive for 2 minutes for debugging...")

            import time
            for i in range(120):  # 2 minutes
                if i % 30 == 0:
                    logger.info(f"Still alive... {i + 1}/120 seconds")
                time.sleep(1)

            sys.exit(1)

        logger.info("Database connection successful!")

        # Import MCP components
        try:
            from mcp.server import FastMCP
            logger.info("FastMCP imported successfully")
        except ImportError as e:
            logger.error(f"Failed to import FastMCP: {e}")
            sys.exit(1)

        # Import our PostgreSQL class
        try:
            from .AzurePostgresMCP import AzurePostgreSQLMCP
            logger.info("AzurePostgreSQLMCP imported successfully")
        except ImportError as e:
            logger.error(f"Failed to import AzurePostgreSQLMCP: {e}")
            sys.exit(1)

        # Create MCP server
        logger.info("Creating FastMCP server...")
        mcp = FastMCP("Azure PostgreSQL MCP Server")

        # Initialize PostgreSQL MCP
        logger.info("Initializing PostgreSQL MCP...")
        try:
            azure_pg_mcp = AzurePostgreSQLMCP()
            logger.info("PostgreSQL MCP initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL MCP: {e}")
            logger.debug(f"Error details: {traceback.format_exc()}")
            sys.exit(1)

        # Add tools
        logger.info("Adding MCP tools...")
        tools_added = 0

        try:
            mcp.add_tool(azure_pg_mcp.get_databases)
            tools_added += 1
            logger.debug("get_databases tool added")

            mcp.add_tool(azure_pg_mcp.get_schemas)
            tools_added += 1
            logger.debug("get_schemas tool added")

            mcp.add_tool(azure_pg_mcp.query_data)
            tools_added += 1
            logger.debug("query_data tool added")

            mcp.add_tool(azure_pg_mcp.get_server_config)
            tools_added += 1
            logger.debug("get_server_config tool added")

            mcp.add_tool(azure_pg_mcp.get_server_parameter)
            tools_added += 1
            logger.debug("get_server_parameter tool added")

            logger.info(f"Added {tools_added} tools successfully")

        except Exception as e:
            logger.error(f"Failed to add tools: {e}")
            sys.exit(1)

        # Start the server
        logger.info("Starting MCP server...")
        logger.info("MCP Server is now running!")
        logger.info("Ready to accept MCP protocol connections")

        # This is the correct way to run FastMCP
        mcp.run()

    except KeyboardInterrupt:
        logger.info("Server stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.debug(f"Full traceback: {traceback.format_exc()}")

        # Keep container alive for debugging
        logger.warning("Keeping container alive for debugging...")
        import time
        time.sleep(300)
        sys.exit(1)


if __name__ == "__main__":
    main()