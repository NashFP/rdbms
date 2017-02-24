import json
import os
import os.path

from .constants import (
    COLUMNS,
    FILE_EXTENSION,
    ROWS,
)
from .exceptions import (
    DatabaseDoesNotExist,
    DatabaseExists,
    MissingColumns,
    TableDoesNotExist,
    TableExists,
    UnknownColumnNames,
)


def get_db_file_name(db_name):
    return '{}.{}'.format(db_name, FILE_EXTENSION)


def create_database(db_name):
    db_file_name = get_db_file_name(db_name)
    if os.path.isfile(db_file_name):
        raise DatabaseExists()

    with open(db_file_name, 'w+') as f:
        json.dump({}, f)


def drop_database(db_name):
    db_file_name = get_db_file_name(db_name)
    if not os.path.isfile(db_file_name):
        raise DatabaseDoesNotExist()

    os.remove(db_file_name)


def create_table(db_name, table_name, columns):
    db_file_name = get_db_file_name(db_name)
    if not os.path.isfile(db_file_name):
        raise DatabaseDoesNotExist()

    with open(db_file_name, 'r+') as f:
        db = json.load(f)
        if table_name in db:
            raise TableExists()

        db[table_name] = {
            COLUMNS: columns,
            ROWS: [],
        }
        f.seek(0)
        json.dump(db, f)
        f.truncate()


def insert(db_name, table_name, row):
    db_file_name = get_db_file_name(db_name)
    if not os.path.isfile(db_file_name):
        raise DatabaseDoesNotExist()

    with open(db_file_name, 'r+') as f:
        db = json.load(f)
        if table_name not in db:
            raise TableDoesNotExist()

        table_columns = db[table_name][COLUMNS]

        # Make sure it is a known column name
        unknown_column_names = set(row) - set(table_columns)
        if unknown_column_names:
            raise UnknownColumnNames(column_names=unknown_column_names)

        missing_columns = set(table_columns) - set(row)
        if missing_columns:
            raise MissingColumns(missing_columns=missing_columns)

        ordered_keys = sorted(row.keys(), key=lambda k: table_columns.index(k))
        db[table_name][ROWS].append([row[key] for key in ordered_keys])

        f.seek(0)
        json.dump(db, f)
        f.truncate()


def select(db_name, table_name, columns):
    db_file_name = get_db_file_name(db_name)
    if not os.path.isfile(db_file_name):
        raise DatabaseDoesNotExist()

    with open(db_file_name, 'r') as f:
        db = json.load(f)
        if table_name not in db:
            raise TableDoesNotExist()

        table_columns = db[table_name][COLUMNS]

        # Make sure it is a known column name
        unknown_column_names = set(columns) - set(table_columns)

        if unknown_column_names:
            raise UnknownColumnNames(column_names=unknown_column_names)

        return [
            {column_name: row[table_columns.index(column_name)] for column_name in columns}
            for row in db[table_name][ROWS]
        ]
