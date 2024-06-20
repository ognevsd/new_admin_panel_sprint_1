import sqlite3
import psycopg
from psycopg.rows import dict_row
import datetime as dt
import uuid

from src.db_connection import SQLiteLoader, PostgresSaver, conn_context
from src.constants import (
    SQLITE_PATH,
    PSQL_DB_NAME,
    PSQL_USER,
    PSQL_PASSWORD,
    PSQL_HOST,
    PSQL_PORT,
    SQLITE_CHUNK_SIZE,
)
from src.data_description import (
    Genre,
    Person,
    PersonFilmWork,
    GenreFilmWork,
    Filmwork,
)


def test_genre_content():
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
        table_name = "genre"
        sqlite_db = SQLiteLoader(sqlite_conn)
        pg_db = PostgresSaver(pg_conn)

        offset = 0
        while True:
            sqlite_records = sqlite_db.execute_query_with_limit_and_offset(
                f"SELECT * FROM {table_name} ORDER BY id LIMIT ? OFFSET ?",
                SQLITE_CHUNK_SIZE,
                offset,
            )

            pg_records = pg_db.execute_query_with_limit_and_offset(
                f"SELECT * FROM content.{table_name} ORDER BY id LIMIT %s "
                "OFFSET %s",
                SQLITE_CHUNK_SIZE,
                offset,
            )

            if not pg_records and not sqlite_records:
                break

            sqlite_new = []
            for item in sqlite_records:
                item = dict(item)
                item["id"] = uuid.UUID(item["id"])
                item["created"] = dt.datetime.fromisoformat(
                    item.pop("created_at")
                )
                item["modified"] = dt.datetime.fromisoformat(
                    item.pop("updated_at")
                )
                sqlite_new.append(Genre(**item))

            pg_new = []
            for item in pg_records:
                pg_new.append(Genre(**item))

            offset += SQLITE_CHUNK_SIZE

            assert sqlite_new == pg_new


def test_person_content():
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
        table_name = "person"
        sqlite_db = SQLiteLoader(sqlite_conn)
        pg_db = PostgresSaver(pg_conn)

        offset = 0
        while True:
            sqlite_records = sqlite_db.execute_query_with_limit_and_offset(
                f"SELECT * FROM {table_name} ORDER BY id LIMIT ? OFFSET ?",
                SQLITE_CHUNK_SIZE,
                offset,
            )

            pg_records = pg_db.execute_query_with_limit_and_offset(
                f"SELECT * FROM content.{table_name} ORDER BY id LIMIT %s "
                "OFFSET %s",
                SQLITE_CHUNK_SIZE,
                offset,
            )

            if not pg_records and not sqlite_records:
                break

            sqlite_new = []
            for item in sqlite_records:
                item = dict(item)
                item["id"] = uuid.UUID(item["id"])
                item["created"] = dt.datetime.fromisoformat(
                    item.pop("created_at")
                )
                item["modified"] = dt.datetime.fromisoformat(
                    item.pop("updated_at")
                )
                sqlite_new.append(Person(**item))

            pg_new = []
            for item in pg_records:
                pg_new.append(Person(**item))

            offset += SQLITE_CHUNK_SIZE

            assert sqlite_new == pg_new


def test_filmwork_content():
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
        table_name = "film_work"
        sqlite_db = SQLiteLoader(sqlite_conn)
        pg_db = PostgresSaver(pg_conn)

        offset = 0
        while True:
            sqlite_records = sqlite_db.execute_query_with_limit_and_offset(
                f"SELECT * FROM {table_name} ORDER BY id LIMIT ? OFFSET ?",
                SQLITE_CHUNK_SIZE,
                offset,
            )

            pg_records = pg_db.execute_query_with_limit_and_offset(
                f"SELECT * FROM content.{table_name} ORDER BY id LIMIT %s "
                "OFFSET %s",
                SQLITE_CHUNK_SIZE,
                offset,
            )

            if not pg_records and not sqlite_records:
                break

            sqlite_new = []
            for item in sqlite_records:
                item = dict(item)
                item["id"] = uuid.UUID(item["id"])
                item["created"] = dt.datetime.fromisoformat(
                    item.pop("created_at")
                )
                item["modified"] = dt.datetime.fromisoformat(
                    item.pop("updated_at")
                )
                sqlite_new.append(Filmwork(**item))

            pg_new = []
            for item in pg_records:
                pg_new.append(Filmwork(**item))

            offset += SQLITE_CHUNK_SIZE

            assert sqlite_new == pg_new


def test_genre_film_work_content():
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
        table_name = "genre_film_work"
        sqlite_db = SQLiteLoader(sqlite_conn)
        pg_db = PostgresSaver(pg_conn)

        offset = 0
        while True:
            sqlite_records = sqlite_db.execute_query_with_limit_and_offset(
                f"SELECT * FROM {table_name} ORDER BY id LIMIT ? OFFSET ?",
                SQLITE_CHUNK_SIZE,
                offset,
            )

            pg_records = pg_db.execute_query_with_limit_and_offset(
                f"SELECT * FROM content.{table_name} ORDER BY id LIMIT %s "
                "OFFSET %s",
                SQLITE_CHUNK_SIZE,
                offset,
            )

            if not pg_records and not sqlite_records:
                break

            sqlite_new = []
            for item in sqlite_records:
                item = dict(item)
                item["id"] = uuid.UUID(item["id"])
                item["genre_id"] = uuid.UUID(item["genre_id"])
                item["film_work_id"] = uuid.UUID(item["film_work_id"])
                item["created"] = dt.datetime.fromisoformat(
                    item.pop("created_at")
                )
                sqlite_new.append(GenreFilmWork(**item))

            pg_new = []
            for item in pg_records:
                pg_new.append(GenreFilmWork(**item))

            offset += SQLITE_CHUNK_SIZE

            assert sqlite_new == pg_new


def test_person_film_work_content():
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
        table_name = "person_film_work"
        sqlite_db = SQLiteLoader(sqlite_conn)
        pg_db = PostgresSaver(pg_conn)

        offset = 0
        while True:
            sqlite_records = sqlite_db.execute_query_with_limit_and_offset(
                f"SELECT * FROM {table_name} ORDER BY id LIMIT ? OFFSET ?",
                SQLITE_CHUNK_SIZE,
                offset,
            )

            pg_records = pg_db.execute_query_with_limit_and_offset(
                f"SELECT * FROM content.{table_name} ORDER BY id LIMIT %s "
                "OFFSET %s",
                SQLITE_CHUNK_SIZE,
                offset,
            )

            if not pg_records and not sqlite_records:
                break

            sqlite_new = []
            for item in sqlite_records:
                item = dict(item)
                item["id"] = uuid.UUID(item["id"])
                item["person_id"] = uuid.UUID(item["person_id"])
                item["film_work_id"] = uuid.UUID(item["film_work_id"])
                item["created"] = dt.datetime.fromisoformat(
                    item.pop("created_at")
                )
                sqlite_new.append(PersonFilmWork(**item))

            pg_new = []
            for item in pg_records:
                pg_new.append(PersonFilmWork(**item))

            offset += SQLITE_CHUNK_SIZE

            assert sqlite_new == pg_new
