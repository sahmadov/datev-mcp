# src/db_utils.py
import os
import logging
import urllib.parse
import psycopg

logger = logging.getLogger("datev_mcp.db_utils")


def test_database_connection() -> bool:
    """Test database connection with detailed error reporting"""
    logger.info("Testing Database Connection...")

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
