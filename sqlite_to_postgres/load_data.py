import os
import datetime
import sqlite3
from contextlib import contextmanager, closing
import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor, execute_values
import uuid
from dataclasses import dataclass, field, astuple
from movies_admin.config import settings

db_path = 'db.sqlite'
SCHEMA = 'content'


@dataclass
class FilmWork:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    title: str = field(default='')
    description: str = field(default='')
    creation_date: datetime.date = field(default_factory=None)
    file_path: str = field(default=None)
    rating: float = field(default=0.0)
    type: str = field(default='movie')
    created: datetime.datetime = field(default_factory=datetime.datetime.now)
    modified: datetime.datetime = field(default_factory=datetime.datetime.now)

    def __post_init__(self):
        if self.description is None:
            self.description = ''
        if self.rating is None:
            self.rating = 0.0


@dataclass
class Genre:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    name: str = field(default=None)
    description: str = field(default=None)
    created: datetime.datetime.strptime = field(default_factory=datetime.datetime.now)
    modified: datetime.datetime = field(default_factory=datetime.datetime.now)


@dataclass
class Person:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    full_name: str = field(default='')
    created: datetime.datetime = field(default_factory=datetime.datetime.now)
    modified: datetime.datetime = field(default_factory=datetime.datetime.now)


@dataclass
class PersonFilmWork:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)
    person_id: uuid.UUID = field(default_factory=uuid.uuid4)
    role: str = field(default='actor')
    created: datetime.datetime = field(default_factory=datetime.datetime.now)


@dataclass
class GenreFilmWork:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)
    genre_id: uuid.UUID = field(default_factory=uuid.uuid4)
    created: datetime.datetime = field(default_factory=datetime.datetime.now)


TABLES = [Genre, Person, FilmWork, PersonFilmWork, GenreFilmWork]


def dataclasses_to_tables(dataclass_name: str):
    table_name = ''
    for i in dataclass_name:
        if i.isupper():
            table_name = f'{table_name}_{i}'
        else:
            table_name = f'{table_name}{i}'
    return table_name.lower()[1:]


class PostgresSaver:
    def __init__(self, pg_conn):
        self.pg_conn = pg_conn
        self.cursor = self.pg_conn.cursor()

    def save_to_postgres(self, data, table):
        args = [astuple(i) for i in data]
        sql_insert = (
            f"""INSERT INTO {SCHEMA}.{dataclasses_to_tables(table.__name__)} 
            ({', '.join(list(table.__annotations__.keys()))}) VALUES %s ON CONFLICT (id) DO NOTHING; """)
        execute_values(self.cursor, sql_insert, args)

    def save_all_data(self, data, table):
        self.save_to_postgres(data, table)
        self.pg_conn.commit()


class SQLiteExtractor:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection
        self.rows_amount = 100

    @contextmanager
    def conn_context(self, _db_path: str):
        conn = sqlite3.connect(_db_path)
        conn.row_factory = sqlite3.Row
        yield conn
        conn.close()

    def extract_movies(self, table, postgres_saver):
        with self.conn_context(db_path) as conn:
            curs = conn.cursor()
            curs.execute(f"SELECT * FROM '{dataclasses_to_tables(table.__name__)}';")
            while True:
                rows = curs.fetchmany(self.rows_amount)
                if rows:
                    postgres_saver.save_all_data([table(*i) for i in rows], table)
                else:
                    break


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    sqlite_extractor = SQLiteExtractor(connection)
    postgres_saver = PostgresSaver(pg_conn)

    for table in TABLES:
        sqlite_extractor.extract_movies(table, postgres_saver)


if __name__ == '__main__':
    dsl = {'dbname': os.environ.get('DB_NAME'), 'user': os.environ.get('DB_USER'),
           'password': os.environ.get('DB_PASSWORD'), 'host': os.environ.get('DB_HOST'),
           'port': os.environ.get('DB_PORT')}
    with sqlite3.connect(db_path) as sqlite_conn, closing(
            psycopg2.connect(**dsl, cursor_factory=DictCursor)) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
    pg_conn.close()
