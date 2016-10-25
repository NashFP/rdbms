# NashFP's "build your own relational database" playground
Let's write a database! 

More specifically, let's write a relational database management system (RDBMS)-- an oldschool, SQL-blooded, table thumping, Boyce–Codd saluting database. You'll know you're done when you have that warm, vintage feeling that only comes from tables with static columns, primary keys, indexes (clustered and non-clustered), and foreign keys constraints. 

## High-level Parts:

Some of the pieces to tackle:
* *Data structure*: writing and reading database schema for database catalogs, tables, columns, etc.
* *Data manipulation*: writing to tables, and reading from tables.
* *Data integrity*: type checking, preventing duplicate keys, preventing foreign-key violations, etc.
* *SQL Parser*: Turning the standard SQL text into the function calls your engine supports

## Resources
* http://ecomputernotes.com/database-system/rdbms/rdbms-components
* https://en.wikipedia.org/wiki/SQL
* https://dev.mysql.com/doc/employee/en/sakila-structure.html
* http://www.w3schools.com/sql/sql_syntax.asp

## How to get started

This excercise will work best if test driven. A good place to start is with an API for creating database structures. Here are some rough ideas on a path forward...

### Creating databases
Your database engine needs to support creating database catalogs:
```
create_database :: string -> status
create_database db_name
...
create_database "my_db"
```
A simple implementation might be to simply create a 

### Creating tables
A good database needs good tables with good columns. 

```
create_table :: string -> string -> [column_spec] -> status
create_table db_name table_name column_specs
...
create_table "my_db" "person" ["Id", "LastName", "FirstName", "ZipCode"]
```

A simple way to get started is to say your database engine only supports string data (`VARCHAR(MAX)`). In this case our `column_specs` can simply be a list of column names.

### Inserting rows into tables

```
insert :: string -> string -> [string] -> status
insert db_name table_name values
...
insert "my_db" "person" ["100", "Marx", "Groucho", "10001"]
```

### Selecting rows from tables
```
select :: string -> string -> [string] -> [[string]]
select db_name from_table_name column_names
...
select "my_db" "person" ["Id", "LastName"]
```

### Using SQL
```
query :: string -> string ->  [[string]]
query db_name sql
...
query "my_db", "SELECT Id, LastName FROM Person"
```

### Next steps
From here you have a launchpad to make things more real. Examples:
* Support `WHERE` 
* Support `ORDER BY`
* Support `INNER JOIN`
* Enforce primary keys
* Enforce foreign key constraints
* Leverage indexes for queries
* Support clustered and non-clustered indexes

## Write your own

Join the fun! Add a directory to this repo containing your code.

If you don't already have the right permissions to push to this repo, file an issue! We'll hook you up.

By convention, we use directory names that tell who wrote the code and what language it's in, separated by a `+`. For example: `bryan_hunter+elixir`.
