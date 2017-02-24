class DatabaseDoesNotExist(Exception):
    pass


class DatabaseExists(Exception):
    pass


class MissingColumns(Exception):
    def __init__(self, missing_columns, *args, **kwargs):
        self.missing_columns = missing_columns
        super(MissingColumns, self).__init__(*args, **kwargs)


class TableDoesNotExist(Exception):
    pass


class TableExists(Exception):
    pass


class UnknownColumnNames(Exception):
    def __init__(self, column_names, *args, **kwargs):
        self.column_names = column_names
        super(UnknownColumnNames, self).__init__(*args, **kwargs)
