import io
import sqlite3

import psycopg
from typing import Generator
from psycopg.rows import dict_row
from dataclasses import astuple

from psycopg.errors import UniqueViolation, BadCopyFileFormat, OperationalError

from contextlib import contextmanager

from loguru import logger

from src.data_description import Filmwork
from src.db_connection import SQLiteLoader, PostgresSaver
from src.constants import (
    SQLITE_PATH,
    PSQL_DB_NAME,
    PSQL_USER,
    PSQL_PASSWORD,
    PSQL_HOST,
    PSQL_PORT,
)


if len(SQLITE_PATH) == 0:
    raise Exception("Missing sqlite connection")


@contextmanager
def conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def load_from_sqlite(
    connection: sqlite3.Connection, pg_conn: psycopg.Connection
):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(connection)

    for data in sqlite_loader.load_movies():
        postgres_saver.upload_movies(data)
    # postgres_saver.check_connection()

    # data = sqlite_loader.load_movies()
    # postgres_saver.save_all_data(data)


if __name__ == "__main__":
    dsl = {
        "dbname": PSQL_DB_NAME,
        "user": PSQL_USER,
        "password": PSQL_PASSWORD,
        "host": PSQL_HOST,
        "port": PSQL_PORT,
    }
    try:
        with conn_context(SQLITE_PATH) as sqlite_conn, psycopg.connect(
            **dsl, row_factory=dict_row, cursor_factory=psycopg.ClientCursor
        ) as pg_conn:  # type: ignore
            load_from_sqlite(sqlite_conn, pg_conn)
    except UniqueViolation as e:
        logger.error(
            f"Failed to upload data, unique field already exists: {e}"
        )
