import os
from dotenv import load_dotenv

load_dotenv()

SQLITE_PATH = os.environ.get("SQLITE_PATH", "")
PSQL_DB_NAME = os.environ.get("PSQL_DB_NAME", "")
PSQL_USER = os.environ.get("PSQL_USER", "")
PSQL_PASSWORD = os.environ.get("PSQL_PASSWORD", "")
PSQL_HOST = os.environ.get("PSQL_HOST", "")
PSQL_PORT = os.environ.get("PSQL_PORT", "")


SQLITE_CHUNK_SIZE = 200
