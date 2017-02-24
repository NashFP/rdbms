import json
import os
import os.path

import pytest

from ..constants import (
    COLUMNS,
    FILE_EXTENSION,
    ROWS,
)
from ..exceptions import (
    DatabaseDoesNotExist,
    DatabaseExists,
    TableExists,
    MissingColumns,
)
from ..main import (
    create_database,
    create_table,
    drop_database,
    get_db_file_name,
    insert,
    select,
)

DB_NAME = 'app'
TABLE_NAME = 'thing'


def test_get_db_file_name():
    assert get_db_file_name(DB_NAME) == 'app.{}'.format(FILE_EXTENSION)


def test_create_database():
    create_database(DB_NAME)
    db_file_name = get_db_file_name(DB_NAME)
    assert os.path.isfile(db_file_name)
    os.remove(db_file_name)


def test_create_database_database_exists():
    create_database(DB_NAME)
    try:
        with pytest.raises(DatabaseExists):
            create_database(DB_NAME)
    finally:
        os.remove(get_db_file_name(DB_NAME))


def test_drop_database():
    create_database(DB_NAME)
    drop_database(DB_NAME)
    assert not os.path.isfile(get_db_file_name(DB_NAME))


def test_create_table():
    create_database(DB_NAME)
    columns = ['id', 'name', 'other']
    create_table(DB_NAME, TABLE_NAME, columns)
    with open(get_db_file_name(DB_NAME), 'r') as f:
        db = json.load(f)
        assert db == {TABLE_NAME: {COLUMNS: columns, ROWS: []}}
    drop_database(DB_NAME)


def test_create_table_db_does_not_exist():
    with pytest.raises(DatabaseDoesNotExist):
        create_table(DB_NAME, TABLE_NAME, [])


def test_create_table_table_exists():
    create_database(DB_NAME)
    create_table(DB_NAME, TABLE_NAME, [])
    try:
        with pytest.raises(TableExists):
            create_table(DB_NAME, TABLE_NAME, [])
    finally:
        drop_database(DB_NAME)


def test_insert():
    create_database(DB_NAME)
    columns = ['id', 'created', 'name']
    create_table(DB_NAME, TABLE_NAME, columns)
    row = {'id': 1, 'created': '2017-02-22', 'name': 'High Quality Gifs'}
    insert(DB_NAME, TABLE_NAME, row)
    try:
        with open(get_db_file_name(DB_NAME), 'r') as f:
            db = json.load(f)
            assert db == {TABLE_NAME: {COLUMNS: columns, ROWS: [[row['id'], row['created'], row['name']]]}}
    finally:
        drop_database(DB_NAME)


def test_insert_missing_columns():
    create_database(DB_NAME)
    columns = ['id', 'created', 'name']
    create_table(DB_NAME, TABLE_NAME, columns)
    row = {'id': 1, 'name': 'High Quality Gifs'}
    try:
        with pytest.raises(MissingColumns):
            insert(DB_NAME, TABLE_NAME, row)
    finally:
        drop_database(DB_NAME)


def test_select():
    create_database(DB_NAME)
    columns = ['id', 'created', 'name']
    create_table(DB_NAME, TABLE_NAME, columns)
    row = {'id': 1, 'created': '2017-02-22', 'name': 'High Quality Gifs'}
    insert(DB_NAME, TABLE_NAME, row)
    try:
        assert select(DB_NAME, TABLE_NAME, ['id', 'name']) == [{'id': row['id'], 'name': row['name']}]
    finally:
        drop_database(DB_NAME)
