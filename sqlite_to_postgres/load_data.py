import datetime
import sqlite3
from contextlib import contextmanager
import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
import uuid
from dataclasses import dataclass, field

import os
from split_settings.tools import include
from dotenv import load_dotenv
from movies_admin.config.components import database
from movies_admin.config import settings

load_dotenv()

include(
    '../movies_admin/config/components/database.py',
)

db_path = 'db.sqlite'


@dataclass
class FilmWork:
    title: str
    description: str
    creation_date: datetime.date
    type: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    rating: float = field(default=0.0)

    def __post_init__(self):
        if self.description is None:
            self.description = ""
        if self.rating is None:
            self.rating = 0.0

    def save_to_postgres(self, cursor: psycopg2.extensions.connection.cursor):
        cursor.execute(
            f""" INSERT INTO content.film_work (id, title, type, description, rating, created, modified)
            VALUES ('{self.id}', '{self.title.replace("'", "''")}', '{self.type}',
            '{self.description.replace("'", "''")}',
            '{self.rating}', NOW(), NOW())
            ON CONFLICT (id) DO UPDATE SET title=EXCLUDED.title, description=EXCLUDED.description,
            rating=EXCLUDED.rating, type=EXCLUDED.type, created=EXCLUDED.created, modified=EXCLUDED.modified; """)


@dataclass
class Genre:
    name: str
    description: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)

    def __post_init__(self):
        if self.description is None:
            self.description = ""

    def save_to_postgres(self, cursor: psycopg2.extensions.connection.cursor):
        cursor.execute(f"""
                    INSERT INTO content.genre (id, name, description, created, modified)
                    VALUES ('{self.id}', '{self.name}', '{self.description.replace("'", "''")}', NOW(), NOW())
                    ON CONFLICT (id) DO
                    UPDATE SET name=EXCLUDED.name, description=EXCLUDED.description, created=EXCLUDED.created,
                    modified=EXCLUDED.modified; """)


@dataclass
class Person:
    full_name: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)

    def save_to_postgres(self, cursor: psycopg2.extensions.connection.cursor):
        cursor.execute(f"""
                    INSERT INTO content.person (id, full_name, created, modified)
                    VALUES ('{self.id}', '{self.full_name.replace("'", "''")}', NOW(), NOW())
                    ON CONFLICT (id) DO
                    UPDATE SET full_name=EXCLUDED.full_name, created=EXCLUDED.created,
                    modified=EXCLUDED.modified; """)


@dataclass
class PersonFilmWork:
    role: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    person_id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)

    def save_to_postgres(self, cursor: psycopg2.extensions.connection.cursor):
        cursor.execute(f"""
                       INSERT INTO content.person_film_work (id, role, person_id, film_work_id, created)
                       VALUES ('{self.id}', '{self.role}', '{self.person_id}', '{self.film_work_id}', NOW())
                       ON CONFLICT (id) DO
                       UPDATE SET role=EXCLUDED.role, person_id=EXCLUDED.person_id, film_work_id=EXCLUDED.film_work_id,
                       created=EXCLUDED.created; """)


@dataclass
class GenreFilmWork:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    genre_id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)

    def save_to_postgres(self, cursor: psycopg2.extensions.connection.cursor):
        cursor.execute(f"""
                       INSERT INTO content.genre_film_work (id, genre_id, film_work_id, created)
                       VALUES ('{self.id}', '{self.genre_id}', '{self.film_work_id}', NOW())
                       ON CONFLICT (id) DO
                       UPDATE SET genre_id=EXCLUDED.genre_id, film_work_id=EXCLUDED.film_work_id,
                       created=EXCLUDED.created; """)


class PostgresSaver:
    def __init__(self, pg_conn):
        self.pg_conn = pg_conn
        self.cursor = self.pg_conn.cursor()

    def save_all_data(self, data):
        for i in data:
            i.save_to_postgres(self.cursor)


class SQLiteExtractor:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection

    @contextmanager
    def conn_context(self, _db_path: str):
        conn = sqlite3.connect(_db_path)
        conn.row_factory = sqlite3.Row
        yield conn
        conn.close()

    def extract_movies(self):
        with self.conn_context(db_path) as conn:
            curs = conn.cursor()
            curs.execute("SELECT * FROM genre;")
            genre_data = curs.fetchall()
            curs.execute("SELECT * FROM person;")
            person_data = curs.fetchall()
            curs.execute("SELECT * FROM film_work;")
            film_work_data = curs.fetchall()
            curs.execute("SELECT * FROM person_film_work;")
            person_film_work_data = curs.fetchall()
            curs.execute("SELECT * FROM genre_film_work;")
            genre_film_work_data = curs.fetchall()
            data_ = []

            for i in range(0, len(genre_data), 1):
                genre_ = Genre(id=dict(genre_data[i])['id'],
                               name=dict(genre_data[i])['name'],
                               description=dict(genre_data[i])['description'])
                data_.append(genre_)

            for i in range(0, len(person_data), 1):
                person_ = Person(id=dict(person_data[i])['id'],
                                 full_name=dict(person_data[i])['full_name'])
                data_.append(person_)

            for i in range(0, len(film_work_data), 1):
                filmwork_ = FilmWork(id=dict(film_work_data[i])['id'],
                                     title=dict(film_work_data[i])['title'],
                                     description=dict(film_work_data[i])['description'],
                                     creation_date=dict(film_work_data[i])['creation_date'],
                                     type=dict(film_work_data[i])['type'],
                                     rating=dict(film_work_data[i])['rating'])
                data_.append(filmwork_)

            for i in range(0, len(person_film_work_data), 1):
                person_filmwork_ = PersonFilmWork(id=dict(person_film_work_data[i])['id'],
                                                  role=dict(person_film_work_data[i])['role'],
                                                  person_id=dict(person_film_work_data[i])['person_id'],
                                                  film_work_id=dict(person_film_work_data[i])['film_work_id'])
                data_.append(person_filmwork_)

            for i in range(0, len(genre_film_work_data), 1):
                genre_filmwork_ = GenreFilmWork(id=dict(genre_film_work_data[i])['id'],
                                                genre_id=dict(genre_film_work_data[i])['genre_id'],
                                                film_work_id=dict(genre_film_work_data[i])['film_work_id'])
                data_.append(genre_filmwork_)

            return data_


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)

    data = sqlite_extractor.extract_movies()
    postgres_saver.save_all_data(data)


if __name__ == '__main__':
    dsl = {'dbname': 'movies_database', 'user': 'app', 'password': '123qwe', 'host': '127.0.0.1', 'port': 5432}
    with sqlite3.connect(db_path) as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
