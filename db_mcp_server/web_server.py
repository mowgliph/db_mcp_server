"""Web server implementation for the database MCP server."""

import json
import logging
import argparse
from typing import Dict, Any, Optional
from http.server import HTTPServer, BaseHTTPRequestHandler

from .server import DatabaseMcpServer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('db-mcp-web-server')


class MCPHttpHandler(BaseHTTPRequestHandler):
    """HTTP request handler for MCP requests."""
    
    db_server = None  # Will be set by the main function
    
    def do_GET(self):
        """Handle GET requests."""
        try:
            if self.path == '/':
                # Return basic server info
                info = {
                    'name': 'Database MCP Server',
                    'version': '1.0.0',
                    'description': 'A Model Context Protocol server for database operations',
                    'endpoints': {
                        '/': 'Server information (GET)',
                        '/list_tools': 'List available tools (GET/POST)',
                        '/call_tool': 'Execute a tool (POST)'
                    }
                }
                self._send_response(200, info)
            
            elif self.path == '/list_tools':
                # Return available tools (same as POST)
                tools = {
                    'add_connection': {'description': 'Add a new database connection'},
                    'test_connection': {'description': 'Test a database connection'},
                    'list_connections': {'description': 'List all database connections'},
                    'remove_connection': {'description': 'Remove a database connection'},
                    'execute_query': {'description': 'Execute a SQL query'},
                    'get_records': {'description': 'Get records from a table'},
                    'insert_record': {'description': 'Insert a record into a table'},
                    'update_record': {'description': 'Update records in a table'},
                    'delete_record': {'description': 'Delete records from a table'},
                    'list_tables': {'description': 'List all tables in a database'},
                    'get_table_schema': {'description': 'Get the schema for a table'},
                    'create_table': {'description': 'Create a new table'},
                    'drop_table': {'description': 'Drop a table'},
                    'create_index': {'description': 'Create an index on a table'},
                    'drop_index': {'description': 'Drop an index'},
                    'alter_table': {'description': 'Alter a table structure'},
                    'begin_transaction': {'description': 'Begin a transaction'},
                    'commit_transaction': {'description': 'Commit a transaction'},
                    'rollback_transaction': {'description': 'Rollback a transaction'}
                }
                self._send_response(200, {'tools': tools})
            
            else:
                self._send_error(404, f"Endpoint '{self.path}' not found")
        
        except Exception as e:
            logger.exception("Error processing GET request")
            self._send_error(500, f"Internal server error: {str(e)}")
    
    def do_POST(self):
        """Handle POST requests."""
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length).decode('utf-8')
        
        try:
            request = json.loads(post_data)
            
            # Extract MCP request components
            if self.path == '/call_tool':
                tool_name = request.get('name')
                tool_args = request.get('arguments', {})
                
                if not tool_name:
                    self._send_error(400, "Missing 'name' field in request")
                    return
                
                # Create a mock MCP request object
                class MockRequest:
                    def __init__(self, name, params):
                        self.params = params
                        self.method = name
                
                mock_request = MockRequest(tool_name, tool_args)
                
                # Call the appropriate handler based on the tool name
                handler_method = f"handle_{tool_name}"
                if hasattr(self.db_server, handler_method):
                    handler = getattr(self.db_server, handler_method)
                    result = handler(mock_request)
                    self._send_response(200, result)
                else:
                    self._send_error(404, f"Tool '{tool_name}' not found")
            
            elif self.path == '/list_tools':
                # Return available tools
                tools = {
                    'add_connection': {'description': 'Add a new database connection'},
                    'test_connection': {'description': 'Test a database connection'},
                    'list_connections': {'description': 'List all database connections'},
                    'remove_connection': {'description': 'Remove a database connection'},
                    'execute_query': {'description': 'Execute a SQL query'},
                    'get_records': {'description': 'Get records from a table'},
                    'insert_record': {'description': 'Insert a record into a table'},
                    'update_record': {'description': 'Update records in a table'},
                    'delete_record': {'description': 'Delete records from a table'},
                    'list_tables': {'description': 'List all tables in a database'},
                    'get_table_schema': {'description': 'Get the schema for a table'},
                    'create_table': {'description': 'Create a new table'},
                    'drop_table': {'description': 'Drop a table'},
                    'create_index': {'description': 'Create an index on a table'},
                    'drop_index': {'description': 'Drop an index'},
                    'alter_table': {'description': 'Alter a table structure'},
                    'begin_transaction': {'description': 'Begin a transaction'},
                    'commit_transaction': {'description': 'Commit a transaction'},
                    'rollback_transaction': {'description': 'Rollback a transaction'}
                }
                
                self._send_response(200, {'tools': tools})
            
            else:
                self._send_error(404, f"Endpoint '{self.path}' not found")
        
        except json.JSONDecodeError:
            self._send_error(400, "Invalid JSON in request body")
        except Exception as e:
            logger.exception("Error processing request")
            self._send_error(500, f"Internal server error: {str(e)}")
    
    def _send_response(self, status_code: int, data: Dict[str, Any]):
        """Send a JSON response."""
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))
    
    def _send_error(self, status_code: int, message: str):
        """Send an error response."""
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({'error': message}).encode('utf-8'))


def run_web_server(host: str = 'localhost', port: int = 8000, 
                  config_path: Optional[str] = None, log_level: str = 'INFO'):
    """Run the MCP server as a web server.
    
    Args:
        host: The hostname to bind to.
        port: The port to bind to.
        config_path: Path to a configuration file.
        log_level: Logging level.
    """
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create database server
    db_server = DatabaseMcpServer()
    MCPHttpHandler.db_server = db_server
    
    # Create HTTP server
    server_address = (host, port)
    httpd = HTTPServer(server_address, MCPHttpHandler)
    
    logger.info(f"Starting database MCP web server on {host}:{port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Server interrupted, shutting down...")
    finally:
        # Clean up connections
        db_server.close_all_connections()
        httpd.server_close()
        logger.info("Server stopped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Database MCP Web Server")
    parser.add_argument(
        "--host", default="localhost",
        help="Hostname to bind to (default: localhost)"
    )
    parser.add_argument(
        "--port", type=int, default=8000,
        help="Port to bind to (default: 8000)"
    )
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
    run_web_server(args.host, args.port, args.config, args.log_level)
