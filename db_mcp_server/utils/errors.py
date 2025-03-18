"""Custom error classes for database operations."""

class DatabaseError(Exception):
    """Base class for all database errors."""
    
    def __init__(self, message: str, original_error: Exception = None):
        self.message = message
        self.original_error = original_error
        super().__init__(self.message)
    
    def __str__(self):
        if self.original_error:
            return f"{self.message} (Original error: {str(self.original_error)})"
        return self.message


class ConnectionError(DatabaseError):
    """Error raised when there is a problem with database connections."""
    pass


class QueryError(DatabaseError):
    """Error raised when there is a problem executing a query."""
    pass


class SchemaError(DatabaseError):
    """Error raised when there is a problem with schema operations."""
    pass
