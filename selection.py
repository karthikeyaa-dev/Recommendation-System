from typing import Union
import logging
from connection import create_connection, list_tables, list_columns, load_table, converting_tables_to_df, table_to_df
from exceptions import NoTablesFoundError

logger = logging.getLogger(__name__)

def prompt_table_choice(tables, prompt_text):
    print("\nAvailable tables:")
    print(", ".join(tables))
    table = input(prompt_text + ": ")
    while table not in tables:
        print(f"Table '{table}' not found. Please try again.")
        table = input(prompt_text + ": ")
    return table

def prompt_column_choice(columns, prompt_text):
    print("\nAvailable columns:")
    print(", ".join(columns))
    col = input(prompt_text + ": ")
    while col not in columns:
        print(f"Column '{col}' not found. Please try again.")
        col = input(prompt_text + ": ")
    return col

def select_table(connection, prompt_message, return_key):
    """
    Interactive function to select a table from the database.
    
    Args:
        connection: psycopg2 connection object.
        prompt_message: str, prompt shown to the user.
        return_key: str, key name for the returned dictionary.
    
    Returns:
        dict: {return_key: selected_table_name}
    """
    try:
        tables = list_tables(connection)
        if not tables:
            raise NoTablesFoundError("No tables found in the database.")
    except NoTablesFoundError as e:
        logger.error(str(e))
        raise
    except Exception:
        logger.exception("An unexpected error occurred while listing tables.")
        raise

    selected_table = prompt_table_choice(tables, prompt_message)
    return {return_key: selected_table}

def select_column(connection, table_name, prompt_message, return_key):
    """
    Interactive function to select a column from a specified table.
    
    Args:
        connection: psycopg2 connection object.
        table_name: str, name of the table to get columns from.
        prompt_message: str, prompt shown to the user.
        return_key: str, key name for the returned dictionary.

    Returns:
        dict: {return_key: selected_column_name}
    """
    try:
        columns = list_columns(connection, table_name)
        if not columns:
            raise NoTablesFoundError(f"No columns found in table '{table_name}'.")
    except NoTablesFoundError as e:
        logger.error(str(e))
        raise
    except Exception:
        logger.exception(f"An unexpected error occurred while listing columns for table '{table_name}'.")
        raise

    selected_column = prompt_column_choice(columns, prompt_message)
    return {return_key: selected_column}

def validate_referential_integrity(connection, parent_table_info, child_table_info):
    """
    Validate referential integrity between parent and child tables based on foreign keys.
    
    Parameters:
    - connection: DB connection object
    - parent_table_info: dict with 'columns', 'primary_keys', 'foreign_keys' (from get_table_info)
    - child_table_info: dict with same structure
    
    Returns:
    - True if referential integrity holds for all foreign keys from child to parent
    - False if any violation is found
    """
    parent_table = parent_table_info.get('table_name')
    child_table = child_table_info.get('table_name')

    # For each foreign key in child table
    for (fk_column, ref_table, ref_column) in child_table_info['foreign_keys']:
        # We only validate foreign keys referencing the given parent table
        if ref_table != parent_table:
            continue

        query = f"""
        SELECT COUNT(*)
        FROM {child_table} child
        LEFT JOIN {parent_table} parent ON child.{fk_column} = parent.{ref_column}
        WHERE child.{fk_column} IS NOT NULL AND parent.{ref_column} IS NULL
        """

        with connection.cursor() as cursor:
            cursor.execute(query)
            orphan_count = cursor.fetchone()[0]

        if orphan_count > 0:
            # Referential integrity violation found
            return False

    # No violations found
    return True
