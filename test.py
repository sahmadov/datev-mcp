# src/test_db_connection.py
import os
import sys
import urllib.parse
import psycopg
import time
from datetime import datetime


def test_environment_variables():
    """Test if all required environment variables are set"""
    print("\n🔍 Testing Environment Variables:")
    print("=" * 50)

    required_vars = ['PGHOST', 'PGUSER', 'PGPASSWORD']
    env_status = {}

    for var in required_vars:
        value = os.environ.get(var)
        if value:
            if var == 'PGPASSWORD':
                print(f"✅ {var}: {'*' * len(value)} (length: {len(value)})")
            else:
                print(f"✅ {var}: {value}")
            env_status[var] = True
        else:
            print(f"❌ {var}: NOT SET")
            env_status[var] = False

    return all(env_status.values())


def test_network_connectivity():
    """Test basic network connectivity to the database host"""
    print("\n🌐 Testing Network Connectivity:")
    print("=" * 50)

    host = os.environ.get('PGHOST')
    if not host:
        print("❌ PGHOST not set, cannot test connectivity")
        return False

    try:
        import socket

        # Test DNS resolution
        print(f"🔍 Resolving DNS for {host}...")
        ip = socket.gethostbyname(host)
        print(f"✅ DNS resolved: {host} -> {ip}")

        # Test port connectivity (PostgreSQL default port 5432)
        print(f"🔍 Testing port 5432 connectivity...")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)  # 10 second timeout
        result = sock.connect_ex((host, 5432))
        sock.close()

        if result == 0:
            print(f"✅ Port 5432 is reachable on {host}")
            return True
        else:
            print(f"❌ Port 5432 is NOT reachable on {host} (error code: {result})")
            return False

    except socket.gaierror as e:
        print(f"❌ DNS resolution failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Network test failed: {e}")
        return False


def test_database_connection():
    """Test actual database connection with detailed error reporting"""
    print("\n🗄️  Testing Database Connection:")
    print("=" * 50)

    host = os.environ.get('PGHOST')
    user = urllib.parse.quote(os.environ.get('PGUSER', ''))
    password = os.environ.get('PGPASSWORD')

    if not all([host, user, password]):
        print("❌ Missing required connection parameters")
        return False

    # Test different connection methods
    connection_strings = [
        f"host={host} user={user} dbname='postgres' password={password}",
        f"host={host} user={user} dbname='postgres' password={password} sslmode=require",
        f"host={host} user={user} dbname='postgres' password={password} sslmode=prefer",
        f"postgresql://{user}:{password}@{host}/postgres",
        f"postgresql://{user}:{password}@{host}/postgres?sslmode=require"
    ]

    for i, conn_str in enumerate(connection_strings, 1):
        print(f"\n🔍 Test {i}: Trying connection method {i}...")
        # Hide password in logs
        safe_conn_str = conn_str.replace(password, '*' * len(password))
        print(f"   Connection string: {safe_conn_str}")

        try:
            print("   Attempting connection...")
            with psycopg.connect(conn_str, connect_timeout=10) as conn:
                print("   ✅ Connection successful!")

                # Test a simple query
                with conn.cursor() as cur:
                    cur.execute("SELECT version();")
                    version = cur.fetchone()[0]
                    print(f"   ✅ Query successful: {version[:50]}...")

                return True

        except psycopg.OperationalError as e:
            print(f"   ❌ Operational Error: {e}")
        except psycopg.DatabaseError as e:
            print(f"   ❌ Database Error: {e}")
        except Exception as e:
            print(f"   ❌ Unexpected Error: {e}")

    return False


def test_azure_specific_issues():
    """Test for common Azure PostgreSQL specific issues"""
    print("\n☁️  Testing Azure-Specific Issues:")
    print("=" * 50)

    host = os.environ.get('PGHOST', '')
    user = os.environ.get('PGUSER', '')

    # Check if it's an Azure PostgreSQL host
    if 'postgres.database.azure.com' not in host:
        print("ℹ️  Host doesn't appear to be Azure PostgreSQL, skipping Azure-specific tests")
        return

    print(f"🔍 Detected Azure PostgreSQL host: {host}")

    # Check username format for Azure
    if '@' not in user:
        print(f"⚠️  Username might need server suffix: {user}@{host.split('.')[0]}")
        print("   Try setting PGUSER to: username@servername")
    else:
        print(f"✅ Username appears to have correct Azure format: {user}")

    # Check for common Azure connection requirements
    print("ℹ️  Azure PostgreSQL requirements:")
    print("   - SSL connection required (sslmode=require)")
    print("   - Username format: username@servername")
    print("   - Firewall rules must allow your IP")
    print("   - Server must be running and accessible")


def main():
    """Run comprehensive database connection tests"""
    print("🚀 Database Connection Diagnostic Tool")
    print("=" * 60)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"Python version: {sys.version}")
    print(f"Working directory: {os.getcwd()}")

    # Run all tests
    tests = [
        ("Environment Variables", test_environment_variables),
        ("Network Connectivity", test_network_connectivity),
        ("Database Connection", test_database_connection),
        ("Azure Specific Issues", test_azure_specific_issues)
    ]

    results = {}
    for test_name, test_func in tests:
        try:
            if test_name == "Azure Specific Issues":
                test_func()  # This test doesn't return a boolean
                results[test_name] = "completed"
            else:
                results[test_name] = test_func()
        except Exception as e:
            print(f"\n❌ Test '{test_name}' crashed: {e}")
            results[test_name] = False

    # Summary
    print("\n📊 Test Summary:")
    print("=" * 30)
    for test_name, result in results.items():
        if result is True:
            print(f"✅ {test_name}: PASSED")
        elif result == "completed":
            print(f"ℹ️  {test_name}: COMPLETED")
        else:
            print(f"❌ {test_name}: FAILED")

    # Keep container alive for manual inspection
    print(f"\n⏳ Keeping container alive for 300 seconds for manual inspection...")
    print("   You can docker exec into the container to run additional tests")

    for i in range(300):
        if i % 30 == 0:  # Print every 30 seconds
            print(f"   Still alive... {i + 1}/300 seconds")
        time.sleep(1)


if __name__ == "__main__":
    main()