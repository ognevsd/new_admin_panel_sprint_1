import sqlite3
import psycopg

from psycopg.rows import dict_row
from loguru import logger

from src.db_connection import SQLiteLoader, PostgresSaver, conn_context

from src.data_description import (
    Filmwork,
    Genre,
    Person,
    GenreFilmWork,
    PersonFilmWork,
)
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


def load_from_sqlite(
    connection: sqlite3.Connection, pg_conn: psycopg.Connection
):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(connection)

    timestamped_tables = {
        "film_work": Filmwork,
        "genre": Genre,
        "person": Person,
    }

    relation_tables = {
        "genre_film_work": GenreFilmWork,
        "person_film_work": PersonFilmWork,
    }

    try:
        for table, data_class in timestamped_tables.items():
            for data in sqlite_loader.load_timestamped(table, data_class):
                logger.info(f"Uploading {table} batch...")
                postgres_saver.upload_data(data, table_name=table)

        for table, data_class in relation_tables.items():
            for data in sqlite_loader.load_relational(table, data_class):
                logger.info(f"Uploading {table} batch...")
                postgres_saver.upload_data(data, table_name=table)
    except Exception as e:
        logger.error(f"Unknown exception: {e}")


if __name__ == "__main__":
    dsl = {
        "dbname": PSQL_DB_NAME,
        "user": PSQL_USER,
        "password": PSQL_PASSWORD,
        "host": PSQL_HOST,
        "port": PSQL_PORT,
    }
    with conn_context(SQLITE_PATH) as sqlite_conn, psycopg.connect(
        **dsl, row_factory=dict_row, cursor_factory=psycopg.ClientCursor
    ) as pg_conn:  # type: ignore
        load_from_sqlite(sqlite_conn, pg_conn)
