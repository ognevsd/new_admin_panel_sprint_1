import psycopg
from psycopg.rows import dict_row

from src.db_connection import SQLiteLoader, PostgresSaver, conn_context
from src.constants import (
    SQLITE_PATH,
    PSQL_DB_NAME,
    PSQL_USER,
    PSQL_PASSWORD,
    PSQL_HOST,
    PSQL_PORT,
)


def test_film_work_items_integrity():
    table_name = "film_work"
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
        sqlite_db = SQLiteLoader(sqlite_conn)
        pg_db = PostgresSaver(pg_conn)

        assert sqlite_db.count_items_in_table(
            table_name
        ) == pg_db.count_items_in_table(table_name)


def test_genre_items_integrity():
    table_name = "genre"
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
        sqlite_db = SQLiteLoader(sqlite_conn)
        pg_db = PostgresSaver(pg_conn)

        assert sqlite_db.count_items_in_table(
            table_name
        ) == pg_db.count_items_in_table(table_name)


def test_person_items_integrity():
    table_name = "person"
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
        sqlite_db = SQLiteLoader(sqlite_conn)
        pg_db = PostgresSaver(pg_conn)

        assert sqlite_db.count_items_in_table(
            table_name
        ) == pg_db.count_items_in_table(table_name)


def test_genre_film_work_items_integrity():
    table_name = "genre_film_work"
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
        sqlite_db = SQLiteLoader(sqlite_conn)
        pg_db = PostgresSaver(pg_conn)

        assert sqlite_db.count_items_in_table(
            table_name
        ) == pg_db.count_items_in_table(table_name)


def test_person_film_work_items_integrity():
    table_name = "person_film_work"
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
        sqlite_db = SQLiteLoader(sqlite_conn)
        pg_db = PostgresSaver(pg_conn)

        assert sqlite_db.count_items_in_table(
            table_name
        ) == pg_db.count_items_in_table(table_name)
