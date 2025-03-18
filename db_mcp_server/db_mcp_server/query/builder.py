"""SQL query builder implementation."""

from typing import Any, Dict, List, Optional, Union, Tuple


class QueryBuilder:
    """Builds SQL queries in a database-agnostic way."""
    
    def __init__(self, db_type: str = 'unknown'):
        """Initialize query builder.
        
        Args:
            db_type: The type of database (sqlite, postgres, mysql, mssql)
        """
        self.db_type = db_type.lower()
    
    def select(self, table_name: str, columns: Optional[List[str]] = None,
               where: Optional[Dict[str, Any]] = None, order_by: Optional[List[str]] = None,
               limit: Optional[int] = None, offset: Optional[int] = None) -> Tuple[str, List]:
        """Build a SELECT query.
        
        Args:
            table_name: The name of the table to query.
            columns: List of columns to retrieve. If None, retrieves all columns.
            where: Dictionary of column-value pairs for filtering records.
            order_by: List of columns to order by (prefix with '-' for descending order).
            limit: Maximum number of records to return.
            offset: Number of records to skip.
            
        Returns:
            A tuple containing (SQL query string, parameters list)
        """
        # Handle column selection
        column_clause = "*"
        if columns:
            column_clause = ", ".join(columns)
        
        # Build the base query
        query = f"SELECT {column_clause} FROM {table_name}"
        params = []
        
        # Add WHERE clause if specified
        if where:
            where_clause, where_params = self._build_where_clause(where)
            query += f" WHERE {where_clause}"
            params.extend(where_params)
        
        # Add ORDER BY clause if specified
        if order_by:
            order_clause = []
            for col in order_by:
                if col.startswith('-'):
                    order_clause.append(f"{col[1:]} DESC")
                else:
                    order_clause.append(f"{col} ASC")
            query += f" ORDER BY {', '.join(order_clause)}"
        
        # Add LIMIT clause if specified
        if limit is not None:
            if self.db_type == 'mssql':
                # SQL Server uses TOP instead of LIMIT
                # But this should be placed after SELECT, so we'll handle it differently
                if offset is None:
                    # Simple TOP case
                    query = query.replace("SELECT", f"SELECT TOP {limit}")
                else:
                    # For OFFSET/LIMIT in SQL Server, we need ORDER BY
                    if not order_by:
                        # Add a default ORDER BY clause if none exists
                        query += " ORDER BY (SELECT NULL)"
                    # Add OFFSET/FETCH clause
                    query += f" OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
            else:
                # Most databases use LIMIT
                query += f" LIMIT {limit}"
                
                # Add OFFSET if specified
                if offset is not None:
                    query += f" OFFSET {offset}"
        elif offset is not None:
            # Handle OFFSET without LIMIT
            if self.db_type == 'mssql':
                if not order_by:
                    # Add a default ORDER BY clause if none exists
                    query += " ORDER BY (SELECT NULL)"
                query += f" OFFSET {offset} ROWS"
            else:
                # Most databases can use OFFSET with a very large LIMIT
                query += f" LIMIT 18446744073709551615 OFFSET {offset}"
        
        return query, params
    
    def insert(self, table_name: str, data: Dict[str, Any]) -> Tuple[str, List]:
        """Build an INSERT query.
        
        Args:
            table_name: The name of the table.
            data: Dictionary of column-value pairs to insert.
            
        Returns:
            A tuple containing (SQL query string, parameters list)
        """
        if not data:
            raise ValueError("No data provided for insertion")
        
        columns = list(data.keys())
        values = list(data.values())
        
        # Build placeholders based on database type
        if self.db_type == 'postgres':
            # PostgreSQL uses $1, $2, etc.
            placeholders = [f"${i+1}" for i in range(len(columns))]
        else:
            # Most databases use ?
            placeholders = ["?"] * len(columns)
        
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        
        return query, values
    
    def update(self, table_name: str, data: Dict[str, Any], where: Dict[str, Any]) -> Tuple[str, List]:
        """Build an UPDATE query.
        
        Args:
            table_name: The name of the table.
            data: Dictionary of column-value pairs to update.
            where: Dictionary of column-value pairs for filtering records to update.
            
        Returns:
            A tuple containing (SQL query string, parameters list)
        """
        if not data:
            raise ValueError("No data provided for update")
        
        if not where:
            raise ValueError("No conditions provided for update")
        
        # Build SET clause
        set_clause = []
        params = []
        
        for column, value in data.items():
            if self.db_type == 'postgres':
                set_clause.append(f"{column} = ${len(params) + 1}")
            else:
                set_clause.append(f"{column} = ?")
            params.append(value)
        
        # Build WHERE clause
        where_clause, where_params = self._build_where_clause(where, offset=len(params))
        params.extend(where_params)
        
        query = f"UPDATE {table_name} SET {', '.join(set_clause)} WHERE {where_clause}"
        
        return query, params
    
    def delete(self, table_name: str, where: Dict[str, Any]) -> Tuple[str, List]:
        """Build a DELETE query.
        
        Args:
            table_name: The name of the table.
            where: Dictionary of column-value pairs for filtering records to delete.
            
        Returns:
            A tuple containing (SQL query string, parameters list)
        """
        if not where:
            raise ValueError("No conditions provided for delete operation")
        
        # Build WHERE clause
        where_clause, params = self._build_where_clause(where)
        
        query = f"DELETE FROM {table_name} WHERE {where_clause}"
        
        return query, params
    
    def _build_where_clause(self, conditions: Dict[str, Any], offset: int = 0) -> Tuple[str, List]:
        """Build a WHERE clause from a dictionary of conditions.
        
        Args:
            conditions: Dictionary of column-value pairs for filtering.
            offset: Offset for parameter numbering in PostgreSQL.
            
        Returns:
            A tuple containing (WHERE clause string, parameters list)
        """
        where_conditions = []
        params = []
        
        for i, (column, value) in enumerate(conditions.items()):
            # Handle special operators
            if isinstance(value, dict) and len(value) == 1:
                operator, operand = next(iter(value.items()))
                
                if operator == 'in':
                    # Handle IN operator
                    if not operand:
                        # Empty IN clause - will always be false
                        where_conditions.append("1 = 0")
                        continue
                    
                    if self.db_type == 'postgres':
                        placeholders = [f"${offset + len(params) + j + 1}" for j in range(len(operand))]
                    else:
                        placeholders = ["?"] * len(operand)
                    
                    where_conditions.append(f"{column} IN ({', '.join(placeholders)})")
                    params.extend(operand)
                    
                elif operator == 'not_in':
                    # Handle NOT IN operator
                    if not operand:
                        # Empty NOT IN clause - will always be true
                        where_conditions.append("1 = 1")
                        continue
                    
                    if self.db_type == 'postgres':
                        placeholders = [f"${offset + len(params) + j + 1}" for j in range(len(operand))]
                    else:
                        placeholders = ["?"] * len(operand)
                    
                    where_conditions.append(f"{column} NOT IN ({', '.join(placeholders)})")
                    params.extend(operand)
                    
                elif operator == 'between':
                    # Handle BETWEEN operator
                    if self.db_type == 'postgres':
                        where_conditions.append(f"{column} BETWEEN ${offset + len(params) + 1} AND ${offset + len(params) + 2}")
                    else:
                        where_conditions.append(f"{column} BETWEEN ? AND ?")
                    
                    params.extend(operand)
                    
                elif operator in ('>', '<', '>=', '<=', '<>', '!=', 'like', 'not_like'):
                    # Handle comparison operators
                    op_map = {
                        '>': '>',
                        '<': '<',
                        '>=': '>=',
                        '<=': '<=',
                        '<>': '<>',
                        '!=': '!=',
                        'like': 'LIKE',
                        'not_like': 'NOT LIKE'
                    }
                    
                    if self.db_type == 'postgres':
                        where_conditions.append(f"{column} {op_map[operator]} ${offset + len(params) + 1}")
                    else:
                        where_conditions.append(f"{column} {op_map[operator]} ?")
                    
                    params.append(operand)
                    
                elif operator == 'is_null':
                    # Handle IS NULL operator
                    where_conditions.append(f"{column} IS NULL" if operand else f"{column} IS NOT NULL")
                    
                else:
                    # Unknown operator, treat as equality
                    if self.db_type == 'postgres':
                        where_conditions.append(f"{column} = ${offset + len(params) + 1}")
                    else:
                        where_conditions.append(f"{column} = ?")
                    
                    params.append(value)
            
            elif value is None:
                # Handle NULL value
                where_conditions.append(f"{column} IS NULL")
                
            else:
                # Handle simple equality
                if self.db_type == 'postgres':
                    where_conditions.append(f"{column} = ${offset + len(params) + 1}")
                else:
                    where_conditions.append(f"{column} = ?")
                
                params.append(value)
        
        return " AND ".join(where_conditions), params
