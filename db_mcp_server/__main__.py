#!/usr/bin/env python3
"""Main entry point for the database MCP server."""

import os
import sys
import argparse
import logging
from dotenv import load_dotenv

try:
    from mcp_sdk import Server
    from mcp_sdk.server.stdio import StdioServerTransport
except ImportError:
    print("Error: MCP SDK not found. Please install it with:")
    print("pip install modelcontextprotocol")
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
        # Initialize the MCP SDK Server
        sdk_server = Server(
            {
                "name": "db-mcp-server",
                "version": "0.1.0"
            },
            {
                "capabilities": {
                    "tools": {
                        # Connection Management
                        "add_connection": {
                            "description": "Add a new database connection",
                            "input_schema": {
                                "type": "object",
                                "properties": {
                                    "connection_id": {
                                        "type": "string",
                                        "description": "Identifier for the connection"
                                    },
                                    "type": {
                                        "type": "string",
                                        "description": "Database type (sqlite, postgres, mysql, mssql)"
                                    },
                                    "config_path": {
                                        "type": "string",
                                        "description": "Path to save connection configuration (optional)"
                                    }
                                },
                                "required": ["connection_id", "type"]
                            }
                        },
                        "test_connection": {
                            "description": "Test a database connection",
                            "input_schema": {
                                "type": "object",
                                "properties": {
                                    "connection_id": {
                                        "type": "string",
                                        "description": "Identifier for the connection"
                                    }
                                },
                                "required": ["connection_id"]
                            }
                        },
                        "list_connections": {
                            "description": "List all database connections",
                            "input_schema": {
                                "type": "object",
                                "properties": {}
                            }
                        },
                        "remove_connection": {
                            "description": "Remove a database connection",
                            "input_schema": {
                                "type": "object",
                                "properties": {
                                    "connection_id": {
                                        "type": "string",
                                        "description": "Identifier for the connection"
                                    },
                                    "config_path": {
                                        "type": "string",
                                        "description": "Path to save connection configuration (optional)"
                                    }
                                },
                                "required": ["connection_id"]
                            }
                        },
                        
                        # Query Execution
                        "execute_query": {
                            "description": "Execute a SQL query",
                            "input_schema": {
                                "type": "object",
                                "properties": {
                                    "connection_id": {
                                        "type": "string",
                                        "description": "Identifier for the connection"
                                    },
                                    "query": {
                                        "type": "string",
                                        "description": "SQL query to execute"
                                    },
                                    "params": {
                                        "type": "array",
                                        "description": "Query parameters (optional)"
                                    }
                                },
                                "required": ["connection_id", "query"]
                            }
                        },
                        "get_records": {
                            "description": "Get records from a table",
                            "input_schema": {
                                "type": "object",
                                "properties": {
                                    "connection_id": {
                                        "type": "string",
                                        "description": "Identifier for the connection"
                                    },
                                    "table": {
                                        "type": "string",
                                        "description": "Table name"
                                    },
                                    "columns": {
                                        "type": "array",
                                        "description": "Columns to retrieve (optional)"
                                    },
                                    "where": {
                                        "type": "object",
                                        "description": "Where conditions (optional)"
                                    },
                                    "order_by": {
                                        "type": "array",
                                        "description": "Order by columns (optional)"
                                    },
                                    "limit": {
                                        "type": "integer",
                                        "description": "Maximum number of records (optional)"
                                    },
                                    "offset": {
                                        "type": "integer",
                                        "description": "Number of records to skip (optional)"
                                    }
                                },
                                "required": ["connection_id", "table"]
                            }
                        },
                        "insert_record": {
                            "description": "Insert a record into a table",
                            "input_schema": {
                                "type": "object",
                                "properties": {
                                    "connection_id": {
                                        "type": "string",
                                        "description": "Identifier for the connection"
                                    },
                                    "table": {
                                        "type": "string",
                                        "description": "Table name"
                                    },
                                    "data": {
                                        "type": "object",
                                        "description": "Record data to insert"
                                    }
                                },
                                "required": ["connection_id", "table", "data"]
                            }
                        },
                        "update_record": {
                            "description": "Update records in a table",
                            "input_schema": {
                                "type": "object",
                                "properties": {
                                    "connection_id": {
                                        "type": "string",
                                        "description": "Identifier for the connection"
                                    },
                                    "table": {
                                        "type": "string",
                                        "description": "Table name"
                                    },
                                    "data": {
                                        "type": "object",
                                        "description": "Data to update"
                                    },
                                    "where": {
                                        "type": "object",
                                        "description": "Where conditions"
                                    }
                                },
                                "required": ["connection_id", "table", "data", "where"]
                            }
                        },
                        "delete_record": {
                            "description": "Delete records from a table",
                            "input_schema": {
                                "type": "object",
                                "properties": {
                                    "connection_id": {
                                        "type": "string",
                                        "description": "Identifier for the connection"
                                    },
                                    "table": {
                                        "type": "string",
                                        "description": "Table name"
                                    },
                                    "where": {
                                        "type": "object",
                                        "description": "Where conditions"
                                    }
                                },
                                "required": ["connection_id", "table", "where"]
                            }
                        },
                        
                        # Schema Management
                        "list_tables": {
                            "description": "List all tables in a database",
                            "input_schema": {
                                "type": "object",
                                "properties": {
                                    "connection_id": {
                                        "type": "string",
                                        "description": "Identifier for the connection"
                                    }
                                },
                                "required": ["connection_id"]
                            }
                        },
                        "get_table_schema": {
                            "description": "Get the schema for a table",
                            "input_schema": {
                                "type": "object",
                                "properties": {
                                    "connection_id": {
                                        "type": "string",
                                        "description": "Identifier for the connection"
                                    },
                                    "table": {
                                        "type": "string",
                                        "description": "Table name"
                                    }
                                },
                                "required": ["connection_id", "table"]
                            }
                        },
                        "create_table": {
                            "description": "Create a new table",
                            "input_schema": {
                                "type": "object",
                                "properties": {
                                    "connection_id": {
                                        "type": "string",
                                        "description": "Identifier for the connection"
                                    },
                                    "table": {
                                        "type": "string",
                                        "description": "Table name"
                                    },
                                    "columns": {
                                        "type": "array",
                                        "description": "Column definitions"
                                    }
                                },
                                "required": ["connection_id", "table", "columns"]
                            }
                        },
                        "drop_table": {
                            "description": "Drop a table",
                            "input_schema": {
                                "type": "object",
                                "properties": {
                                    "connection_id": {
                                        "type": "string",
                                        "description": "Identifier for the connection"
                                    },
                                    "table": {
                                        "type": "string",
                                        "description": "Table name"
                                    }
                                },
                                "required": ["connection_id", "table"]
                            }
                        },
                        "create_index": {
                            "description": "Create an index on a table",
                            "input_schema": {
                                "type": "object",
                                "properties": {
                                    "connection_id": {
                                        "type": "string",
                                        "description": "Identifier for the connection"
                                    },
                                    "table": {
                                        "type": "string",
                                        "description": "Table name"
                                    },
                                    "index_name": {
                                        "type": "string",
                                        "description": "Index name"
                                    },
                                    "columns": {
                                        "type": "array",
                                        "description": "Columns to include in the index"
                                    },
                                    "unique": {
                                        "type": "boolean",
                                        "description": "Whether the index should enforce uniqueness"
                                    }
                                },
                                "required": ["connection_id", "table", "index_name", "columns"]
                            }
                        },
                        "drop_index": {
                            "description": "Drop an index",
                            "input_schema": {
                                "type": "object",
                                "properties": {
                                    "connection_id": {
                                        "type": "string",
                                        "description": "Identifier for the connection"
                                    },
                                    "index_name": {
                                        "type": "string",
                                        "description": "Index name"
                                    }
                                },
                                "required": ["connection_id", "index_name"]
                            }
                        },
                        "alter_table": {
                            "description": "Alter a table structure",
                            "input_schema": {
                                "type": "object",
                                "properties": {
                                    "connection_id": {
                                        "type": "string",
                                        "description": "Identifier for the connection"
                                    },
                                    "table": {
                                        "type": "string",
                                        "description": "Table name"
                                    },
                                    "operations": {
                                        "type": "array",
                                        "description": "Alteration operations"
                                    }
                                },
                                "required": ["connection_id", "table", "operations"]
                            }
                        },
                        
                        # Transaction Management
                        "begin_transaction": {
                            "description": "Begin a transaction",
                            "input_schema": {
                                "type": "object",
                                "properties": {
                                    "connection_id": {
                                        "type": "string",
                                        "description": "Identifier for the connection"
                                    }
                                },
                                "required": ["connection_id"]
                            }
                        },
                        "commit_transaction": {
                            "description": "Commit a transaction",
                            "input_schema": {
                                "type": "object",
                                "properties": {
                                    "connection_id": {
                                        "type": "string",
                                        "description": "Identifier for the connection"
                                    }
                                },
                                "required": ["connection_id"]
                            }
                        },
                        "rollback_transaction": {
                            "description": "Rollback a transaction",
                            "input_schema": {
                                "type": "object",
                                "properties": {
                                    "connection_id": {
                                        "type": "string",
                                        "description": "Identifier for the connection"
                                    }
                                },
                                "required": ["connection_id"]
                            }
                        }
                    }
                }
            }
        )

        # Create the database MCP server
        db_server = DatabaseMcpServer(sdk_server)
        
        # Set up stdio transport for the MCP server
        transport = StdioServerTransport()
        
        # Connect the server to the transport and run
        logger.info("Starting database MCP server...")
        sdk_server.connect(transport)
        logger.info("Database MCP server running")
        
        # Run until interrupted
        try:
            # This will block until the connection is closed
            sdk_server.wait_for_disconnect()
        except KeyboardInterrupt:
            logger.info("Server interrupted, shutting down...")
        finally:
            # Clean up connections
            db_server.close_all_connections()
            sdk_server.close()
    
    except Exception as e:
        logger.error(f"Error running database MCP server: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
