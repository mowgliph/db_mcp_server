from setuptools import setup, find_packages

setup(
    name="db_mcp_server",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "sqlalchemy>=2.0.0",
        "psycopg2-binary>=2.9.3",
        "mysql-connector-python>=8.0.28",
        "pyodbc>=4.0.34",
        "python-dotenv>=1.0.0",
    ],
    author="Your Name",
    author_email="your.email@example.com",
    description="MCP Server for database operations",
    keywords="database, MCP, SQLite, PostgreSQL, MySQL, SQL Server",
    python_requires=">=3.8",
)
