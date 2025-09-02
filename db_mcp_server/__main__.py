#!/usr/bin/env python3
"""Main entry point for the database MCP server."""

import os
import sys
import argparse
import logging
from dotenv import load_dotenv

try:
    from fastmcp import FastMCP
except ImportError:
    print("Error: FastMCP not found. Please install it with:")
    print("pip install fastmcp")
    sys.exit(1)

from .server import DatabaseMcpServer


def main():
    """Run the database MCP server."""
    # Load environment variables from .env file
    load_dotenv()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Database MCP Server")
    parser.add_argument(
        "--log-level", 
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level"
    )
    parser.add_argument(
        "--config", 
        help="Path to a configuration file"
    )
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger('db-mcp-server')
    
    # Set DB_CONFIG_PATH environment variable if --config is provided
    if args.config:
        os.environ['DB_CONFIG_PATH'] = args.config
    
    # Create and initialize the MCP server
    try:
        # Create FastMCP server
        mcp = FastMCP("db-mcp-server")
        
        # Create the database MCP server
        db_server = DatabaseMcpServer()
        
        # Register tools using decorators
        @mcp.tool()
        def add_connection(connection_id: str, type: str, config_path: str = None) -> dict:
            """Add a new database connection."""
            try:
                result = db_server.add_connection(connection_id, type, config_path)
                return {"success": True, "message": result}
            except Exception as e:
                return {"success": False, "error": str(e)}
        
        @mcp.tool()
        def test_connection(connection_id: str) -> dict:
            """Test a database connection."""
            try:
                result = db_server.test_connection(connection_id)
                return {"success": True, "result": result}
            except Exception as e:
                return {"success": False, "error": str(e)}
        
        @mcp.tool()
        def list_connections() -> dict:
            """List all database connections."""
            try:
                result = db_server.list_connections()
                return {"success": True, "connections": result}
            except Exception as e:
                return {"success": False, "error": str(e)}
        
        @mcp.tool()
        def remove_connection(connection_id: str, config_path: str = None) -> dict:
            """Remove a database connection."""
            try:
                result = db_server.remove_connection(connection_id, config_path)
                return {"success": True, "message": result}
            except Exception as e:
                return {"success": False, "error": str(e)}
        
        @mcp.tool()
        def execute_query(connection_id: str, query: str, params: list = None) -> dict:
            """Execute a SQL query."""
            try:
                result = db_server.execute_query(connection_id, query, params)
                return {"success": True, "result": result}
            except Exception as e:
                return {"success": False, "error": str(e)}
        
        @mcp.tool()
        def get_records(connection_id: str, table: str, columns: list = None, where: dict = None, order_by: list = None, limit: int = None, offset: int = None) -> dict:
            """Get records from a table."""
            try:
                result = db_server.get_records(connection_id, table, columns, where, order_by, limit, offset)
                return {"success": True, "records": result}
            except Exception as e:
                return {"success": False, "error": str(e)}
        
        @mcp.tool()
        def insert_record(connection_id: str, table: str, data: dict) -> dict:
            """Insert a record into a table."""
            try:
                result = db_server.insert_record(connection_id, table, data)
                return {"success": True, "result": result}
            except Exception as e:
                return {"success": False, "error": str(e)}
        
        @mcp.tool()
        def update_record(connection_id: str, table: str, data: dict, where: dict) -> dict:
            """Update records in a table."""
            try:
                result = db_server.update_record(connection_id, table, data, where)
                return {"success": True, "result": result}
            except Exception as e:
                return {"success": False, "error": str(e)}
        
        @mcp.tool()
        def delete_record(connection_id: str, table: str, where: dict) -> dict:
            """Delete records from a table."""
            try:
                result = db_server.delete_record(connection_id, table, where)
                return {"success": True, "result": result}
            except Exception as e:
                return {"success": False, "error": str(e)}
        
        @mcp.tool()
        def list_tables(connection_id: str) -> dict:
            """List all tables in a database."""
            try:
                result = db_server.list_tables(connection_id)
                return {"success": True, "tables": result}
            except Exception as e:
                return {"success": False, "error": str(e)}
        
        @mcp.tool()
        def get_table_schema(connection_id: str, table: str) -> dict:
            """Get the schema for a table."""
            try:
                result = db_server.get_table_schema(connection_id, table)
                return {"success": True, "schema": result}
            except Exception as e:
                return {"success": False, "error": str(e)}
        
        @mcp.tool()
        def create_table(connection_id: str, table: str, columns: list) -> dict:
            """Create a new table."""
            try:
                result = db_server.create_table(connection_id, table, columns)
                return {"success": True, "result": result}
            except Exception as e:
                return {"success": False, "error": str(e)}
        
        @mcp.tool()
        def drop_table(connection_id: str, table: str) -> dict:
            """Drop a table."""
            try:
                result = db_server.drop_table(connection_id, table)
                return {"success": True, "result": result}
            except Exception as e:
                return {"success": False, "error": str(e)}
        
        @mcp.tool()
        def create_index(connection_id: str, table: str, index_name: str, columns: list, unique: bool = False) -> dict:
            """Create an index on a table."""
            try:
                result = db_server.create_index(connection_id, table, index_name, columns, unique)
                return {"success": True, "result": result}
            except Exception as e:
                return {"success": False, "error": str(e)}
        
        @mcp.tool()
        def drop_index(connection_id: str, index_name: str) -> dict:
            """Drop an index."""
            try:
                result = db_server.drop_index(connection_id, index_name)
                return {"success": True, "result": result}
            except Exception as e:
                return {"success": False, "error": str(e)}
        
        @mcp.tool()
        def alter_table(connection_id: str, table: str, operations: list) -> dict:
            """Alter a table structure."""
            try:
                result = db_server.alter_table(connection_id, table, operations)
                return {"success": True, "result": result}
            except Exception as e:
                return {"success": False, "error": str(e)}
        
        @mcp.tool()
        def begin_transaction(connection_id: str) -> dict:
            """Begin a transaction."""
            try:
                result = db_server.begin_transaction(connection_id)
                return {"success": True, "result": result}
            except Exception as e:
                return {"success": False, "error": str(e)}
        
        @mcp.tool()
        def commit_transaction(connection_id: str) -> dict:
            """Commit a transaction."""
            try:
                result = db_server.commit_transaction(connection_id)
                return {"success": True, "result": result}
            except Exception as e:
                return {"success": False, "error": str(e)}
        
        @mcp.tool()
        def rollback_transaction(connection_id: str) -> dict:
            """Rollback a transaction."""
            try:
                result = db_server.rollback_transaction(connection_id)
                return {"success": True, "result": result}
            except Exception as e:
                return {"success": False, "error": str(e)}
        
        # Run the server using FastMCP
        logger.info("Starting database MCP server...")
        
        try:
            # Run the FastMCP server
            mcp.run()
            logger.info("Database MCP server stopped")
        except KeyboardInterrupt:
            logger.info("Server interrupted, shutting down...")
        finally:
            # Clean up connections
            db_server.close_all_connections()
    
    except Exception as e:
        logger.error(f"Error running database MCP server: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
