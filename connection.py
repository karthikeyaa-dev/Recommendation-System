import psycopg2
from typing import Optional, Dict, Any


def get_connection_params(
    db_url: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    dbname: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None
) -> Dict[str, Optional[Any]]:
    """
    Return connection parameters as a dictionary.
    """
    return {
        "db_url": db_url,
        "host": host,
        "port": port,
        "dbname": dbname,
        "user": user,
        "password": password
    }


def create_connection(params: Dict[str, Optional[Any]]):
    """
    Establish a raw psycopg2 connection either using a full db_url or individual params.
    """
    try:
        db_url = params.get("db_url")
        host = params.get("host")
        port = params.get("port")
        dbname = params.get("dbname")
        user = params.get("user")
        password = params.get("password")

        if db_url:
            conn = psycopg2.connect(db_url)
        elif all([host, port, dbname, user, password]):
            conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=user,
                password=password
            )
        else:
            raise ValueError("❗ Provide either db_url or all of: host, port, dbname, user, password")

        return conn

    except Exception as e:
        raise ConnectionError(f"❌ Failed to connect to PostgreSQL: {e}")
