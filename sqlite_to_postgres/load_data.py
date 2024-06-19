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
from src.constants import (
    SQLITE_PATH,
    PSQL_DB_NAME,
    PSQL_USER,
    PSQL_PASSWORD,
    PSQL_HOST,
    PSQL_PORT,
    SQLITE_CHUNK_SIZE,
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


class SQLiteLoader:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection

    def load_data_in_chunks(self, query: str):
        with self.connection:
            curs = self.connection.cursor()
            curs.execute(query)
            while True:
                result = curs.fetchmany(SQLITE_CHUNK_SIZE)
                if not result:
                    return None
                yield result

    def load_movies(self):
        movie_data = self.load_data_in_chunks("SELECT * FROM film_work;")
        for chunk in movie_data:
            movies = []
            movies_csv = io.StringIO()
            for movie in chunk:
                x = dict(movie)
                x["created"] = x.pop("created_at")
                x["modified"] = x.pop("updated_at")
                x = Filmwork(**x)
                movies.append(x)
                movies_csv.write(x.to_csv())
            movies_csv.seek(0)
            yield movies_csv


class PostgresSaver:
    def __init__(self, connection: psycopg.Connection):
        self.connection = connection

    def check_connection(self):
        curr = pg_conn.cursor()
        curr.execute("SELECT COUNT(*) FROM content.film_work")
        print(curr.fetchall())

    def upload_movies(self, movies_csv: io.StringIO):
        cursor = self.connection.cursor()
        with cursor.copy(
            """COPY content.film_work FROM STDIN (FORMAT 'csv', HEADER false, DELIMITER E'\t')"""
        ) as copy:
            copy.write(movies_csv.read())


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
