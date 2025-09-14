import psycopg2
from psycopg2 import sql
from typing import Optional, Dict, Any, Union
import pydantic
import pandas as pd
from pydantic import BaseModel


class GetUrlParams(BaseModel):
    db_url: str


class GetDbParams(BaseModel):
    host: str
    port: Union[str, int]
    dbname: str
    user: str
    password: str


ConnectionParams = Union[GetUrlParams, GetDbParams]


def create_connection(params: ConnectionParams):
    """
    Establish a raw psycopg2 connection using either a full db_url or individual params.
    """
    try:
        if isinstance(params, GetUrlParams):
            return psycopg2.connect(params.db_url)

        elif isinstance(params, GetDbParams):
            return psycopg2.connect(**params.dict())

        else:
            raise ValueError("❗ Invalid connection parameters provided.")

    except Exception as e:
        raise ConnectionError(f"❌ Failed to connect to PostgreSQL: {e}")
    
def list_tables(connection) -> list[str]:
    """
    Return a list of all table names in the connected PostgreSQL database.
    Only includes user-defined base tables in the 'public' schema.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE';
            """)
            tables = cursor.fetchall()
            return [table[0] for table in tables]

    except Exception as e:
        raise RuntimeError(f"❌ Failed to list tables: {e}")

def load_table(connection, table_name: str) -> list[dict]:
    """
    Load all rows from the given table name.
    
    Returns:
        A list of rows, where each row is a dictionary with column names as keys.
    """
    try:
        with connection.cursor() as cursor:
            # Use psycopg2.sql to safely interpolate table name
            query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]

    except Exception as e:
        raise RuntimeError(f"❌ Failed to load table '{table_name}': {e}")

def list_columns(connection, table_name: str, schema: str = 'public') -> list[str]:
    """
    List all column names of a specified table in a given schema.
    
    Args:
        connection: psycopg2 connection object.
        table_name: Name of the table.
        schema: Schema name (default 'public').
        
    Returns:
        List of column names.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position;
            """, (schema, table_name))
            columns = cursor.fetchall()
            return [col[0] for col in columns]

    except Exception as e:
        raise RuntimeError(f"❌ Failed to list columns for table '{table_name}': {e}")

def converting_tables_to_df(connection):
    """
    Convert all tables in the database to pandas DataFrames.
    
    Returns:
        A dictionary mapping table names to their corresponding DataFrames.
    """
    try:
        tables = list_tables(connection)
        dataframes = {}
        
        for table in tables:
            query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table))
            df = pd.read_sql_query(query, connection)
            dataframes[table] = df
            
        return dataframes

    except Exception as e:
        raise RuntimeError(f"❌ Failed to convert tables to DataFrames: {e}")
    
def table_to_df(connection, table_name: str) -> pd.DataFrame:
    """
    Load a specific table from the database into a Pandas DataFrame.
    
    Args:
        connection: psycopg2 connection object.
        table_name: Name of the table to load.
    
    Returns:
        A Pandas DataFrame containing the table data.
    """
    try:
        query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
        df = pd.read_sql_query(query, connection)
        return df

    except Exception as e:
        raise RuntimeError(f"❌ Failed to load table '{table_name}' into DataFrame: {e}")

def get_table_info(connection, table_name):
    with connection.cursor() as cursor:
        # Get columns
        cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = 'public';
        """, (table_name,))
        columns = cursor.fetchall()

        # Get primary keys
        cursor.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
              ON tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY' 
              AND tc.table_name = %s AND tc.table_schema = 'public';
        """, (table_name,))
        primary_keys = [row[0] for row in cursor.fetchall()]

        # Get foreign keys
        cursor.execute("""
            SELECT kcu.column_name, ccu.table_name, ccu.column_name
            FROM information_schema.table_constraints tc 
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_name = %s AND tc.table_schema = 'public';
        """, (table_name,))
        foreign_keys = cursor.fetchall()

    return {
        "columns": columns,
        "primary_keys": primary_keys,
        "foreign_keys": foreign_keys
    }
