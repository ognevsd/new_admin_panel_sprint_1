import uuid
import datetime
from dataclasses import dataclass, fields


@dataclass(frozen=True)
class DatabaseItem:
    id: uuid.UUID

    def to_csv(self):
        values = []
        for field in fields(self):
            item = getattr(self, field.name)
            if item is None:
                values.append("")
            else:
                values.append(str(item))
        return "\t".join(values) + "\n"


@dataclass(frozen=True)
class Filmwork(DatabaseItem):
    title: str
    description: str
    creation_date: datetime.datetime
    rating: float
    type: str
    file_path: str
    created: datetime.datetime
    modified: datetime.datetime
