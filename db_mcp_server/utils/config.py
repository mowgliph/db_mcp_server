"""Configuration utilities for database connections."""

import os
import json
from typing import Dict, Any, Optional
from pathlib import Path
from dotenv import load_dotenv


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load database configuration from a file or environment variables.
    
    Args:
        config_path: Path to a JSON configuration file. If None, 
                    looks for environment variable DB_CONFIG_PATH.
    
    Returns:
        Dictionary containing configuration settings.
    
    Raises:
        FileNotFoundError: If the configuration file cannot be found.
        json.JSONDecodeError: If the configuration file contains invalid JSON.
    """
    # Load environment variables from .env file if it exists
    load_dotenv()
    
    # If no config path provided, check environment variable
    if not config_path:
        config_path = os.environ.get('DB_CONFIG_PATH')
        
    # If we have a config file path, load from there
    if config_path and Path(config_path).exists():
        with open(config_path, 'r') as f:
            return json.load(f)
    
    # Otherwise, construct config from environment variables
    connections = {}
    
    # Parse DB_CONNECTIONS if it exists
    connections_str = os.environ.get('DB_CONNECTIONS')
    if connections_str:
        try:
            connections = json.loads(connections_str)
        except json.JSONDecodeError:
            # If it's not valid JSON, assume it's a comma-separated list of connection IDs
            connection_ids = [conn_id.strip() for conn_id in connections_str.split(',')]
            
            for conn_id in connection_ids:
                conn_type = os.environ.get(f'{conn_id}_TYPE')
                if conn_type:
                    connections[conn_id] = {'type': conn_type}
                    
                    # Add all environment variables with this prefix as connection parameters
                    prefix = f'{conn_id}_'
                    for key, value in os.environ.items():
                        if key.startswith(prefix) and key != f'{conn_id}_TYPE':
                            param_name = key[len(prefix):].lower()
                            connections[conn_id][param_name] = value
    
    return {
        'connections': connections
    }


def get_connection_config(connection_id: str, config: Optional[Dict] = None) -> Dict[str, Any]:
    """Get configuration for a specific database connection.
    
    Args:
        connection_id: Identifier for the database connection.
        config: Configuration dictionary. If None, loads from environment.
    
    Returns:
        Dictionary containing connection settings.
    
    Raises:
        KeyError: If the connection ID is not found in the configuration.
    """
    if config is None:
        config = load_config()
    
    connections = config.get('connections', {})
    if connection_id not in connections:
        raise KeyError(f"Connection '{connection_id}' not found in configuration")
    
    return connections[connection_id]


def save_connection_config(connection_id: str, connection_config: Dict, 
                          config_path: Optional[str] = None) -> None:
    """Save a connection configuration.
    
    Args:
        connection_id: Identifier for the database connection.
        connection_config: Configuration dictionary for the connection.
        config_path: Path to the configuration file. If None, uses DB_CONFIG_PATH env variable.
        
    Raises:
        FileNotFoundError: If the configuration file cannot be found or created.
    """
    if not config_path:
        config_path = os.environ.get('DB_CONFIG_PATH')
        if not config_path:
            raise ValueError("No configuration file path provided")
    
    # Load existing config if file exists
    config = {}
    if Path(config_path).exists():
        with open(config_path, 'r') as f:
            try:
                config = json.load(f)
            except json.JSONDecodeError:
                config = {}
    
    # Ensure connections key exists
    if 'connections' not in config:
        config['connections'] = {}
    
    # Update or add the connection
    config['connections'][connection_id] = connection_config
    
    # Write back to file
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)
