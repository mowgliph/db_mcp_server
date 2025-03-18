# Database MCP Server

A Model Context Protocol (MCP) server that provides tools for connecting to and interacting with various database systems.

## Features

- **Multi-Database Support**: Connect to SQLite, PostgreSQL, MySQL/MariaDB, and SQL Server databases
- **Unified Interface**: Common tools for database operations across all supported database types
- **Database-Specific Extensions**: Where needed, specific tools for database-specific features
- **Schema Management**: Create, alter, and drop tables and indexes
- **Query Execution**: Execute raw SQL queries or use structured query tools
- **Transaction Support**: Begin, commit, and rollback transactions

## Installation

### Prerequisites

- Python 3.8 or higher
- Required Python packages (installed automatically with pip):
  - SQLAlchemy
  - Various database drivers, depending on which databases you want to use:
    - SQLite (included with Python)
    - PostgreSQL: `psycopg2-binary`
    - MySQL/MariaDB: `mysql-connector-python`
    - SQL Server: `pyodbc`

### Installing from Source

```bash
# Clone the repository
git clone <repository-url>
cd db-mcp-server

# Install the package
pip install -e .
```

Or install directly from PyPI:

```bash
pip install db-mcp-server
```

## Configuration

The server can be configured using environment variables, a configuration file, or by providing connection details at runtime.

### Environment Variables

- `DB_CONFIG_PATH`: Path to a JSON configuration file
- `DB_CONNECTIONS`: A comma-separated list of connection IDs or a JSON string with connection details

### Configuration File Format

```json
{
  "connections": {
    "sqlite_conn": {
      "type": "sqlite",
      "db_path": "/path/to/database.db"
    },
    "postgres_conn": {
      "type": "postgres",
      "host": "localhost",
      "port": 5432,
      "database": "mydatabase",
      "user": "myuser",
      "password": "mypassword"
    }
  }
}
```

## Usage

### Running the Server

#### As an MCP Server for Claude

```bash
# Run with default settings
python -m db_mcp_server

# Specify a configuration file
python -m db_mcp_server --config /path/to/config.json

# Set logging level
python -m db_mcp_server --log-level DEBUG
```

#### As a Standalone Web Server (for any LLM)

```bash
# Run as a web server
python -m db_mcp_server.web_server

# Specify host and port
python -m db_mcp_server.web_server --host 0.0.0.0 --port 8000

# Specify configuration file and logging level
python -m db_mcp_server.web_server --config /path/to/config.json --log-level DEBUG
```

### Available MCP Tools

#### Connection Management

- `add_connection`: Add a new database connection
- `test_connection`: Test a database connection
- `list_connections`: List all database connections
- `remove_connection`: Remove a database connection

#### Query Execution

- `execute_query`: Execute a SQL query
- `get_records`: Get records from a table
- `insert_record`: Insert a record into a table
- `update_record`: Update records in a table
- `delete_record`: Delete records from a table

#### Schema Management

- `list_tables`: List all tables in a database
- `get_table_schema`: Get the schema for a table
- `create_table`: Create a new table
- `drop_table`: Drop a table
- `create_index`: Create an index on a table
- `drop_index`: Drop an index
- `alter_table`: Alter a table structure

#### Transaction Management

- `begin_transaction`: Begin a transaction
- `commit_transaction`: Commit a transaction
- `rollback_transaction`: Rollback a transaction

## Examples

### Add a Connection

```json
{
  "connection_id": "my_sqlite_db",
  "type": "sqlite",
  "db_path": "/path/to/database.db"
}
```

### Execute a Query

```json
{
  "connection_id": "my_sqlite_db",
  "query": "SELECT * FROM users WHERE age > ?",
  "params": [21]
}
```

### Create a Table

```json
{
  "connection_id": "my_sqlite_db",
  "table": "users",
  "columns": [
    {
      "name": "id",
      "type": "INTEGER",
      "primary_key": true,
      "nullable": false
    },
    {
      "name": "name",
      "type": "TEXT",
      "nullable": false
    },
    {
      "name": "email",
      "type": "TEXT",
      "nullable": true
    }
  ]
}
```

### Insert Records

```json
{
  "connection_id": "my_sqlite_db",
  "table": "users",
  "data": {
    "name": "John Doe",
    "email": "john@example.com"
  }
}
```

## Development

### Running Tests

```bash
# Run all tests
python -m unittest discover

# Run specific test file
python -m unittest tests.test_sqlite
```

## Connecting from Other LLMs

When running as a standalone web server, other LLMs (like Llama 3) can connect to the database MCP server via HTTP. The server exposes the following endpoints:

### Endpoints

- `/list_tools` - GET or POST: Returns a list of all available tools with their descriptions and input schemas
- `/call_tool` - POST: Execute a specific database tool

### Example: Calling from Another LLM

To use this server with another LLM, have the LLM generate HTTP requests to the server. Here's an example of how you could structure the prompt for an LLM like Llama 3:

```
You can interact with a database by making HTTP requests to a database service at http://localhost:8000. 
The service provides the following endpoints:

1. To get a list of available tools:
   Make a POST request to: http://localhost:8000/list_tools
   
2. To execute a database tool:
   Make a POST request to: http://localhost:8000/call_tool
   with a JSON body like:
   {
     "name": "tool_name",
     "arguments": {
       "param1": "value1",
       "param2": "value2"
     }
   }

For example, to execute a SQL query, you would make a request like:
POST http://localhost:8000/call_tool
Content-Type: application/json

{
  "name": "execute_query",
  "arguments": {
    "connection_id": "my_db",
    "query": "SELECT * FROM users"
  }
}
```

### Sample Python Code for Client Integration

```python
import requests
import json

# Base URL of the database MCP server
BASE_URL = "http://localhost:8000"

# List available tools
def list_tools():
    response = requests.post(f"{BASE_URL}/list_tools")
    return response.json()

# Execute a database tool
def call_tool(tool_name, arguments):
    payload = {
        "name": tool_name,
        "arguments": arguments
    }
    response = requests.post(f"{BASE_URL}/call_tool", json=payload)
    return response.json()

# Example: List tables in a database
def list_tables(connection_id):
    return call_tool("list_tables", {"connection_id": connection_id})

# Example: Execute a SQL query
def execute_query(connection_id, query, params=None):
    return call_tool("execute_query", {
        "connection_id": connection_id,
        "query": query,
        "params": params
    })

# Example: Add a new connection
def add_connection(connection_id, db_type, **kwargs):
    args = {"connection_id": connection_id, "type": db_type}
    args.update(kwargs)
    return call_tool("add_connection", args)
```

## License

[MIT License](LICENSE)
