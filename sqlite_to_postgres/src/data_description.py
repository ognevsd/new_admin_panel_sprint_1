import uuid
import datetime
from dataclasses import dataclass, fields


@dataclass(frozen=True)
class DatabaseItem:
    id: uuid.UUID

    def generate_col_count(self) -> str:
        column_names = [field.name for field in fields(self)]
        col_count = ", ".join(["%s"] * len(column_names))
        return col_count

    def generate_col_names_str(self) -> str:
        column_names = [field.name for field in fields(self)]  # [id, name]
        column_names_str = ",".join(column_names)
        return column_names_str


@dataclass(frozen=True)
class Timestamped:
    created: datetime.datetime
    modified: datetime.datetime


@dataclass(frozen=True)
class Filmwork(DatabaseItem, Timestamped):
    title: str
    description: str
    creation_date: datetime.datetime
    rating: float
    type: str
    file_path: str


@dataclass(frozen=True)
class Genre(DatabaseItem, Timestamped):
    name: str
    description: str


@dataclass(frozen=True)
class Person(DatabaseItem, Timestamped):
    full_name: str


@dataclass(frozen=True)
class GenreFilmWork(DatabaseItem):
    film_work_id: uuid.UUID
    genre_id: uuid.UUID
    created: datetime.datetime


@dataclass(frozen=True)
class PersonFilmWork(DatabaseItem):
    film_work_id: uuid.UUID
    person_id: uuid.UUID
    role: str
    created: datetime.datetime
