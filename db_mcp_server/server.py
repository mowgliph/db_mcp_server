#!/usr/bin/env python3
"""MCP Server implementation for database operations."""

import os
import json
import logging
from typing import Dict, List, Any, Optional

from .connectors import get_connector
from .query import QueryExecutor
from .schema import SchemaManager
from .utils.config import load_config, get_connection_config, save_connection_config
from .utils.errors import DatabaseError, ConnectionError, QueryError, SchemaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('db-mcp-server')


class DatabaseMcpServer:
    """MCP Server for database operations."""
    
    def __init__(self, sdk_server):
        """Initialize the Database MCP Server.
        
        Args:
            sdk_server: The MCP SDK Server instance.
        """
        self.server = sdk_server
        self.connections = {}  # Connection cache
        self.config = load_config()
        
        # Register request handlers
        self._register_handlers()
    
    def _register_handlers(self):
        """Register all MCP request handlers."""
        # Connection Management
        self.server.set_tool_handler("add_connection", self.handle_add_connection)
        self.server.set_tool_handler("test_connection", self.handle_test_connection)
        self.server.set_tool_handler("list_connections", self.handle_list_connections)
        self.server.set_tool_handler("remove_connection", self.handle_remove_connection)
        
        # Query Execution
        self.server.set_tool_handler("execute_query", self.handle_execute_query)
        self.server.set_tool_handler("get_records", self.handle_get_records)
        self.server.set_tool_handler("insert_record", self.handle_insert_record)
        self.server.set_tool_handler("update_record", self.handle_update_record)
        self.server.set_tool_handler("delete_record", self.handle_delete_record)
        
        # Schema Management
        self.server.set_tool_handler("list_tables", self.handle_list_tables)
        self.server.set_tool_handler("get_table_schema", self.handle_get_table_schema)
        self.server.set_tool_handler("create_table", self.handle_create_table)
        self.server.set_tool_handler("drop_table", self.handle_drop_table)
        self.server.set_tool_handler("create_index", self.handle_create_index)
        self.server.set_tool_handler("drop_index", self.handle_drop_index)
        self.server.set_tool_handler("alter_table", self.handle_alter_table)
        
        # Transaction Management
        self.server.set_tool_handler("begin_transaction", self.handle_begin_transaction)
        self.server.set_tool_handler("commit_transaction", self.handle_commit_transaction)
        self.server.set_tool_handler("rollback_transaction", self.handle_rollback_transaction)
    
    def _get_connection(self, connection_id: str):
        """Get a database connection by ID.
        
        Args:
            connection_id: The connection identifier.
            
        Returns:
            A database connector instance.
            
        Raises:
            ConnectionError: If the connection cannot be established.
        """
        # Check if connection is already open
        if connection_id in self.connections:
            return self.connections[connection_id]
        
        # Get connection configuration
        try:
            conn_config = get_connection_config(connection_id, self.config)
        except KeyError:
            raise ConnectionError(f"Connection '{connection_id}' not found")
        
        # Create and cache the connector
        try:
            db_type = conn_config.get('type')
            if not db_type:
                raise ConnectionError(f"Database type not specified for connection '{connection_id}'")
            
            connector = get_connector(db_type, **conn_config)
            connector.connect()
            
            self.connections[connection_id] = connector
            return connector
        
        except Exception as e:
            raise ConnectionError(f"Failed to establish connection '{connection_id}'", e)
    
    def _format_error_response(self, error):
        """Format an error response.
        
        Args:
            error: The exception object.
            
        Returns:
            A formatted error response.
        """
        error_type = type(error).__name__
        error_message = str(error)
        
        if isinstance(error, DatabaseError) and error.original_error:
            original_error = f" (Caused by: {str(error.original_error)})"
        else:
            original_error = ""
        
        return {
            "error": True,
            "error_type": error_type,
            "message": f"{error_message}{original_error}"
        }
    
    # Connection Management Handlers
    
    def handle_add_connection(self, request):
        """Handle add_connection tool requests.
        
        Args:
            request: MCP request object with connection details.
            
        Returns:
            Response with connection status.
        """
        try:
            params = request.params
            connection_id = params.get('connection_id')
            
            if not connection_id:
                return {"success": False, "message": "Connection ID is required"}
            
            # Extract connection parameters
            conn_params = {k: v for k, v in params.items() if k != 'connection_id'}
            
            # Save connection configuration
            if 'config_path' in params:
                save_connection_config(connection_id, conn_params, params['config_path'])
            else:
                # Just update in-memory config
                if 'connections' not in self.config:
                    self.config['connections'] = {}
                self.config['connections'][connection_id] = conn_params
            
            # Test the connection
            try:
                # Create connector instance
                db_type = conn_params.get('type')
                if not db_type:
                    return {"success": False, "message": "Database type is required"}
                
                connector = get_connector(db_type, **conn_params)
                connector.connect()
                
                # Cache the connector
                self.connections[connection_id] = connector
                
                return {
                    "success": True,
                    "message": f"Connection '{connection_id}' added and tested successfully"
                }
            
            except Exception as e:
                return {
                    "success": False,
                    "message": f"Connection parameters saved, but test failed: {str(e)}"
                }
        
        except Exception as e:
            logger.exception("Error in add_connection")
            return self._format_error_response(e)
    
    def handle_test_connection(self, request):
        """Handle test_connection tool requests.
        
        Args:
            request: MCP request object with connection ID.
            
        Returns:
            Response with connection test status.
        """
        try:
            connection_id = request.params.get('connection_id')
            
            if not connection_id:
                return {"success": False, "message": "Connection ID is required"}
            
            try:
                connector = self._get_connection(connection_id)
                
                # Test by listing tables
                tables = connector.list_tables()
                
                return {
                    "success": True,
                    "message": f"Connection '{connection_id}' is working",
                    "tables_count": len(tables)
                }
            
            except Exception as e:
                return {
                    "success": False,
                    "message": f"Connection test failed: {str(e)}"
                }
        
        except Exception as e:
            logger.exception("Error in test_connection")
            return self._format_error_response(e)
    
    def handle_list_connections(self, request):
        """Handle list_connections tool requests.
        
        Args:
            request: MCP request object.
            
        Returns:
            Response with list of configured connections.
        """
        try:
            connections = self.config.get('connections', {})
            result = []
            
            for conn_id, config in connections.items():
                # Mask sensitive information
                masked_config = config.copy()
                for key in ['password', 'secret', 'token', 'key']:
                    if key in masked_config:
                        masked_config[key] = '********'
                
                conn_info = {
                    "id": conn_id,
                    "type": config.get('type', 'unknown'),
                    "config": masked_config,
                    "connected": conn_id in self.connections
                }
                result.append(conn_info)
            
            return {"connections": result}
        
        except Exception as e:
            logger.exception("Error in list_connections")
            return self._format_error_response(e)
    
    def handle_remove_connection(self, request):
        """Handle remove_connection tool requests.
        
        Args:
            request: MCP request object with connection ID.
            
        Returns:
            Response with removal status.
        """
        try:
            connection_id = request.params.get('connection_id')
            
            if not connection_id:
                return {"success": False, "message": "Connection ID is required"}
            
            # Close and remove from cache if connected
            if connection_id in self.connections:
                try:
                    self.connections[connection_id].close()
                except:
                    pass
                del self.connections[connection_id]
            
            # Remove from configuration
            if 'connections' in self.config and connection_id in self.config['connections']:
                del self.config['connections'][connection_id]
                
                # If config path is specified, save to file
                if 'config_path' in request.params:
                    config_path = request.params['config_path']
                    with open(config_path, 'w') as f:
                        json.dump(self.config, f, indent=2)
            
            return {
                "success": True,
                "message": f"Connection '{connection_id}' removed"
            }
        
        except Exception as e:
            logger.exception("Error in remove_connection")
            return self._format_error_response(e)
    
    # Query Execution Handlers
    
    def handle_execute_query(self, request):
        """Handle execute_query tool requests.
        
        Args:
            request: MCP request object with query details.
            
        Returns:
            Response with query results.
        """
        try:
            params = request.params
            connection_id = params.get('connection_id')
            query = params.get('query')
            query_params = params.get('params')
            
            if not connection_id:
                return {"success": False, "message": "Connection ID is required"}
            
            if not query:
                return {"success": False, "message": "Query is required"}
            
            connector = self._get_connection(connection_id)
            executor = QueryExecutor(connector)
            
            result = executor.execute_raw(query, query_params)
            return result
        
        except Exception as e:
            logger.exception("Error in execute_query")
            return self._format_error_response(e)
    
    def handle_get_records(self, request):
        """Handle get_records tool requests.
        
        Args:
            request: MCP request object with query details.
            
        Returns:
            Response with retrieved records.
        """
        try:
            params = request.params
            connection_id = params.get('connection_id')
            table_name = params.get('table')
            columns = params.get('columns')
            where = params.get('where')
            order_by = params.get('order_by')
            limit = params.get('limit')
            offset = params.get('offset')
            
            if not connection_id:
                return {"success": False, "message": "Connection ID is required"}
            
            if not table_name:
                return {"success": False, "message": "Table name is required"}
            
            connector = self._get_connection(connection_id)
            executor = QueryExecutor(connector)
            
            records = executor.select(
                table_name, columns, where, order_by, limit, offset
            )
            
            return {"records": records, "count": len(records)}
        
        except Exception as e:
            logger.exception("Error in get_records")
            return self._format_error_response(e)
    
    def handle_insert_record(self, request):
        """Handle insert_record tool requests.
        
        Args:
            request: MCP request object with record details.
            
        Returns:
            Response with insertion status.
        """
        try:
            params = request.params
            connection_id = params.get('connection_id')
            table_name = params.get('table')
            data = params.get('data')
            
            if not connection_id:
                return {"success": False, "message": "Connection ID is required"}
            
            if not table_name:
                return {"success": False, "message": "Table name is required"}
            
            if not data:
                return {"success": False, "message": "Record data is required"}
            
            connector = self._get_connection(connection_id)
            executor = QueryExecutor(connector)
            
            result = executor.insert(table_name, data)
            return result
        
        except Exception as e:
            logger.exception("Error in insert_record")
            return self._format_error_response(e)
    
    def handle_update_record(self, request):
        """Handle update_record tool requests.
        
        Args:
            request: MCP request object with update details.
            
        Returns:
            Response with update status.
        """
        try:
            params = request.params
            connection_id = params.get('connection_id')
            table_name = params.get('table')
            data = params.get('data')
            where = params.get('where')
            
            if not connection_id:
                return {"success": False, "message": "Connection ID is required"}
            
            if not table_name:
                return {"success": False, "message": "Table name is required"}
            
            if not data:
                return {"success": False, "message": "Update data is required"}
            
            if not where:
                return {"success": False, "message": "Where conditions are required"}
            
            connector = self._get_connection(connection_id)
            executor = QueryExecutor(connector)
            
            result = executor.update(table_name, data, where)
            return result
        
        except Exception as e:
            logger.exception("Error in update_record")
            return self._format_error_response(e)
    
    def handle_delete_record(self, request):
        """Handle delete_record tool requests.
        
        Args:
            request: MCP request object with delete details.
            
        Returns:
            Response with delete status.
        """
        try:
            params = request.params
            connection_id = params.get('connection_id')
            table_name = params.get('table')
            where = params.get('where')
            
            if not connection_id:
                return {"success": False, "message": "Connection ID is required"}
            
            if not table_name:
                return {"success": False, "message": "Table name is required"}
            
            if not where:
                return {"success": False, "message": "Where conditions are required"}
            
            connector = self._get_connection(connection_id)
            executor = QueryExecutor(connector)
            
            result = executor.delete(table_name, where)
            return result
        
        except Exception as e:
            logger.exception("Error in delete_record")
            return self._format_error_response(e)
    
    # Schema Management Handlers
    
    def handle_list_tables(self, request):
        """Handle list_tables tool requests.
        
        Args:
            request: MCP request object with connection ID.
            
        Returns:
            Response with list of tables.
        """
        try:
            connection_id = request.params.get('connection_id')
            
            if not connection_id:
                return {"success": False, "message": "Connection ID is required"}
            
            connector = self._get_connection(connection_id)
            schema_manager = SchemaManager(connector)
            
            tables = schema_manager.list_tables()
            return {"tables": tables}
        
        except Exception as e:
            logger.exception("Error in list_tables")
            return self._format_error_response(e)
    
    def handle_get_table_schema(self, request):
        """Handle get_table_schema tool requests.
        
        Args:
            request: MCP request object with table details.
            
        Returns:
            Response with table schema.
        """
        try:
            params = request.params
            connection_id = params.get('connection_id')
            table_name = params.get('table')
            
            if not connection_id:
                return {"success": False, "message": "Connection ID is required"}
            
            if not table_name:
                return {"success": False, "message": "Table name is required"}
            
            connector = self._get_connection(connection_id)
            schema_manager = SchemaManager(connector)
            
            schema = schema_manager.get_table_schema(table_name)
            return {"schema": schema}
        
        except Exception as e:
            logger.exception("Error in get_table_schema")
            return self._format_error_response(e)
    
    def handle_create_table(self, request):
        """Handle create_table tool requests.
        
        Args:
            request: MCP request object with table definition.
            
        Returns:
            Response with creation status.
        """
        try:
            params = request.params
            connection_id = params.get('connection_id')
            table_name = params.get('table')
            columns = params.get('columns')
            
            if not connection_id:
                return {"success": False, "message": "Connection ID is required"}
            
            if not table_name:
                return {"success": False, "message": "Table name is required"}
            
            if not columns:
                return {"success": False, "message": "Columns definition is required"}
            
            connector = self._get_connection(connection_id)
            schema_manager = SchemaManager(connector)
            
            result = schema_manager.create_table(table_name, columns)
            return result
        
        except Exception as e:
            logger.exception("Error in create_table")
            return self._format_error_response(e)
    
    def handle_drop_table(self, request):
        """Handle drop_table tool requests.
        
        Args:
            request: MCP request object with table name.
            
        Returns:
            Response with drop status.
        """
        try:
            params = request.params
            connection_id = params.get('connection_id')
            table_name = params.get('table')
            
            if not connection_id:
                return {"success": False, "message": "Connection ID is required"}
            
            if not table_name:
                return {"success": False, "message": "Table name is required"}
            
            connector = self._get_connection(connection_id)
            schema_manager = SchemaManager(connector)
            
            result = schema_manager.drop_table(table_name)
            return result
        
        except Exception as e:
            logger.exception("Error in drop_table")
            return self._format_error_response(e)
    
    def handle_create_index(self, request):
        """Handle create_index tool requests.
        
        Args:
            request: MCP request object with index definition.
            
        Returns:
            Response with creation status.
        """
        try:
            params = request.params
            connection_id = params.get('connection_id')
            table_name = params.get('table')
            index_name = params.get('index_name')
            columns = params.get('columns')
            unique = params.get('unique', False)
            
            if not connection_id:
                return {"success": False, "message": "Connection ID is required"}
            
            if not table_name:
                return {"success": False, "message": "Table name is required"}
            
            if not index_name:
                return {"success": False, "message": "Index name is required"}
            
            if not columns:
                return {"success": False, "message": "Columns list is required"}
            
            connector = self._get_connection(connection_id)
            schema_manager = SchemaManager(connector)
            
            result = schema_manager.create_index(table_name, index_name, columns, unique)
            return result
        
        except Exception as e:
            logger.exception("Error in create_index")
            return self._format_error_response(e)
    
    def handle_drop_index(self, request):
        """Handle drop_index tool requests.
        
        Args:
            request: MCP request object with index name.
            
        Returns:
            Response with drop status.
        """
        try:
            params = request.params
            connection_id = params.get('connection_id')
            index_name = params.get('index_name')
            
            if not connection_id:
                return {"success": False, "message": "Connection ID is required"}
            
            if not index_name:
                return {"success": False, "message": "Index name is required"}
            
            connector = self._get_connection(connection_id)
            schema_manager = SchemaManager(connector)
            
            result = schema_manager.drop_index(index_name)
            return result
        
        except Exception as e:
            logger.exception("Error in drop_index")
            return self._format_error_response(e)
    
    def handle_alter_table(self, request):
        """Handle alter_table tool requests.
        
        Args:
            request: MCP request object with alteration details.
            
        Returns:
            Response with alteration status.
        """
        try:
            params = request.params
            connection_id = params.get('connection_id')
            table_name = params.get('table')
            operations = params.get('operations')
            
            if not connection_id:
                return {"success": False, "message": "Connection ID is required"}
            
            if not table_name:
                return {"success": False, "message": "Table name is required"}
            
            if not operations:
                return {"success": False, "message": "Alteration operations are required"}
            
            connector = self._get_connection(connection_id)
            schema_manager = SchemaManager(connector)
            
            result = schema_manager.alter_table(table_name, operations)
            return result
        
        except Exception as e:
            logger.exception("Error in alter_table")
            return self._format_error_response(e)
    
    # Transaction Management Handlers
    
    def handle_begin_transaction(self, request):
        """Handle begin_transaction tool requests.
        
        Args:
            request: MCP request object with connection ID.
            
        Returns:
            Response with transaction status.
        """
        try:
            connection_id = request.params.get('connection_id')
            
            if not connection_id:
                return {"success": False, "message": "Connection ID is required"}
            
            connector = self._get_connection(connection_id)
            executor = QueryExecutor(connector)
            
            executor.begin_transaction()
            return {"success": True, "message": "Transaction started"}
        
        except Exception as e:
            logger.exception("Error in begin_transaction")
            return self._format_error_response(e)
    
    def handle_commit_transaction(self, request):
        """Handle commit_transaction tool requests.
        
        Args:
            request: MCP request object with connection ID.
            
        Returns:
            Response with commit status.
        """
        try:
            connection_id = request.params.get('connection_id')
            
            if not connection_id:
                return {"success": False, "message": "Connection ID is required"}
            
            connector = self._get_connection(connection_id)
            executor = QueryExecutor(connector)
            
            executor.commit_transaction()
            return {"success": True, "message": "Transaction committed"}
        
        except Exception as e:
            logger.exception("Error in commit_transaction")
            return self._format_error_response(e)
    
    def handle_rollback_transaction(self, request):
        """Handle rollback_transaction tool requests.
        
        Args:
            request: MCP request object with connection ID.
            
        Returns:
            Response with rollback status.
        """
        try:
            connection_id = request.params.get('connection_id')
            
            if not connection_id:
                return {"success": False, "message": "Connection ID is required"}
            
            connector = self._get_connection(connection_id)
            executor = QueryExecutor(connector)
            
            executor.rollback_transaction()
            return {"success": True, "message": "Transaction rolled back"}
        
        except Exception as e:
            logger.exception("Error in rollback_transaction")
            return self._format_error_response(e)
    
    def close_all_connections(self):
        """Close all open database connections."""
        for conn_id, connector in list(self.connections.items()):
            try:
                connector.close()
                logger.info(f"Closed connection: {conn_id}")
            except Exception as e:
                logger.error(f"Error closing connection {conn_id}: {str(e)}")
        
        self.connections.clear()
