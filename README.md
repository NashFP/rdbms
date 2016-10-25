# NashFP's DIY relational database engine playground
Let's write a relational database! 

We're talking that warm, vintage feel you can only get from tables with static columns, primary keys, indexes (clustered and non-clustered), and foreign keys constraints.

High-level Parts:
* *Data structure*: writing and reading database schema for database catalogs, tables, columns, etc.
* *Data manipulation*: writing to tables, and reading from tables.
* *Data integrity*: type checking, preventing duplicate keys, preventing foreign-key violations, etc.
* *SQL Parser*: Turning the standard SQL text into the function calls your engine supports

## Resources
* http://ecomputernotes.com/database-system/rdbms/rdbms-components
* https://en.wikipedia.org/wiki/SQL
* https://dev.mysql.com/doc/employee/en/sakila-structure.html


## Write your own

Join the fun! Add a directory to this repo containing your code.

If you don't already have the right permissions to push to this repo, file an issue! We'll hook you up.

By convention, we use directory names that tell who wrote the code and what language it's in, separated by a `+`. For example: `bryan_hunter+elixir`.
