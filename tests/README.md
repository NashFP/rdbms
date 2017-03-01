This directory contains a sample database and some test queries you can
run against it.

The database is [the Chinook database](https://chinookdatabase.codeplex.com/).

*   **select** contains tests.
    Each file contains a single SQL query against the sample database
    and the expected results in comma-separated-values format.

*   **tables** contains the database as a set of comma-separated-values files.
    Each file is one table.

    There's no schema information in here, but the first line of each file
    gives the column names.
    Null values are represented as empty strings.

*   **db/ChinookData.xml** is the same data, all in one XML file.

*   **db/chinook.sqlite** is the same data in a SQLite database.
    This is pretty awesome: if you have SQLite installed, you can just
    do `sqlite3 db/chinook.sqlite` and it lets you issue SQL queries and commands
    from your Terminal—a great way to test that your SQL syntax is right.


