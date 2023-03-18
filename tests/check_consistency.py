import pytest
import psycopg2
import sqlite3
from contextlib import contextmanager
from psycopg2.extras import DictCursor

db_path = 'db.sqlite'
dsl = {'dbname': 'movies_database', 'user': 'app', 'password': '123qwe', 'host': '127.0.0.1', 'port': 5432}
pg_conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
pg_cursor = pg_conn.cursor()
sqlite_conn = sqlite3.connect(db_path)


@contextmanager
def conn_context(_db_path: str):
    conn = sqlite3.connect(_db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


def current_len(table_name: str):
    """Возвращает количество записей в базе данных movies_database в таблице table_name"""
    pg_cursor.execute(f"""SELECT COUNT (*) FROM content.{table_name}; """)
    data_ = pg_cursor.fetchall()
    return data_[0][0]


def expected_len(table_name: str):
    """Возвращает количество записей в базе данных sqlite в таблице table_name"""
    with conn_context(db_path) as conn:
        sqlite_cursor = conn.cursor()
        sqlite_cursor.execute(f"""SELECT COUNT (*) as cnt FROM {table_name}; """)
        data_ = sqlite_cursor.fetchall()
        return dict(data_[0])['cnt']


def current_data(table_name: str, table_columns: str):
    """Возвращает значения полей table_columns из таблицы table_name в базе данных movies_database.
    Выбираются те поля, которые были перенесены в postgresql из sqlite"""
    pg_cursor.execute(f"""SELECT {table_columns} FROM content.{table_name} ORDER BY id; """)
    data_ = [x[0:] for x in pg_cursor.fetchall()]
    return data_


def expected_data(table_name: str, table_columns: str):
    """Возвращает значения полей table_columns из таблицы table_name в базе данных sqlite.
    Выбираются те поля, которые были перенесены в postgresql из sqlite"""
    with conn_context(db_path) as conn:
        sqlite_cursor = conn.cursor()
        sqlite_cursor.execute(f"""SELECT {table_columns} FROM {table_name} ORDER BY id; """)
        data_ = [list(x[0:]) for x in sqlite_cursor.fetchall()]
        return data_


@pytest.mark.parametrize(
    'table_name_len,expected_len',
    [('genre', expected_len('genre')), ('person', expected_len('person')), ('film_work', expected_len('film_work')),
     ('person_film_work', expected_len('person_film_work')), ('genre_film_work', expected_len('genre_film_work'))],
)
def test_length(table_name_len: str, expected_len: int):
    """Количество записей в таблицах postgres и sqlite совпадает."""
    assert current_len(table_name_len) == expected_len


@pytest.mark.parametrize(
    'table_name_data, table_columns, expected_data',
    [('genre', 'id, name', expected_data('genre', 'id, name')),
     ('person', 'id, full_name', expected_data('person', 'id, full_name')), (
             'film_work', 'id, title, description, rating, type',
             expected_data('film_work', 'id, title, coalesce(description, ""), coalesce(rating, 0.0), type')),
     ('person_film_work', 'id, person_id, film_work_id, role',
      expected_data('person_film_work', 'id, person_id, film_work_id, role')),
     ('genre_film_work', 'id, genre_id, film_work_id', expected_data('genre_film_work', 'id, genre_id, film_work_id'))],
)
def test_data(table_name_data: str, table_columns: str, expected_data: list):
    """Записи в таблицах postgres и sqlite совпадают."""
    assert current_data(table_name_data, table_columns) == expected_data
