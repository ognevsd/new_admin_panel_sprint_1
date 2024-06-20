import sqlite3
import psycopg

from dataclasses import astuple

from .data_description import DatabaseItem
from .constants import SQLITE_CHUNK_SIZE


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

    def load_timestamped(self, table_name: str, _dataclass):
        data = self.load_data_in_chunks(f"SELECT * FROM {table_name};")
        for chunk in data:
            final = []
            for item in chunk:
                item = dict(item)
                item["created"] = item.pop("created_at")
                item["modified"] = item.pop("updated_at")
                final.append(_dataclass(**item))
            yield final

    def load_relational(self, table_name: str, _dataclass):
        data = self.load_data_in_chunks(f"SELECT * FROM {table_name};")
        for chunk in data:
            final = []
            for item in chunk:
                item = dict(item)
                item["created"] = item.pop("created_at")
                final.append(_dataclass(**item))
            yield final


class PostgresSaver:
    def __init__(self, connection: psycopg.Connection):
        self.connection = connection

    def upload_data(
        self,
        data: list[DatabaseItem],
        table_name: str,
        schema_name: str = "content",
    ) -> None:
        if len(data) == 0:
            return

        cursor = self.connection.cursor()
        col_count = data[0].generate_col_count()
        column_names_str = data[0].generate_col_names_str()

        bind_values = ",".join(
            cursor.mogrify(f"({col_count})", astuple(item)) for item in data
        )

        query = (
            f"INSERT INTO {schema_name}.{table_name} ({column_names_str}) "
            f"VALUES {bind_values} "
            f"ON CONFLICT (id) DO NOTHING"
        )

        cursor.execute(query)
