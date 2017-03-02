# Scheme RDBMS Query

This is an implementation in Chicken Scheme of a query engine
for the Chinook database. It can join efficiently across multiple
tables and has support for order-by.

The trick that this engine uses for making the queries efficient
is something of a "poison pill", although in this case it is more
of a happy pill. For example, consider the following query (in SQL,
not the s-expr used by the engine):
```sql
SELECT album.Title, track.Name FROM album, artist, track 
    WHERE artist.Name="AC/DC" AND artist.ArtistId=album.ArtistId AND
          track.AlbumId=album.AlbumID
```

If we try to create a cartesian product of all three tables, we would
have more than 300 million rows to process. But, what if we first filter
the artist table for artist.Name="AC/DC" and then do a product with
album, filtering that down to the AC/DC albums, then doing a product
with track.

But, how do we filter a subset of the tables using the WHERE clause?

When we first filter the album table, what if we replace any column
comparisons with true if the table hasn't been processed yet? That is,
when we filter album, the WHERE clause effectively becomes:
```sql
WHERE artist.Name="AC/DC" AND true AND true
```

Then, once we join with artist, the WHERE clause becomes:
```sql
WHERE artist.Name="AC/DC" AND artist.ArtistId=album.ArtistId AND true
```

And finally after bring in track, the full WHERE clause is evaluated
against the remaining rows.

This engine represents the WHERE clause as an s-expr, so the above WHERE
clause is represented as:
```scheme
(db-and (equal? '("artist" . "Name") "AC/DC") 
        (equal? '("artist" . "ArtistId") '("album" . "ArtistId"))
        (equal? '("track" . "AlbumId") '("album" . "AlbumId")))
```

When this expression is evaluated, any column references are replaced
either with the value of the column, or a special "unavailable" symbol.
Whenever a function is to be executed, except for db-and and db-or, the
arguments are examined and if any are unavailable, the function result
is automatically unavailable, so (equal? unavailable "something") has
a result of "unavailable".

This is dependent on the ordering of the tables being joined, so that
if track and album are joined first, we have to filter a fill cartesian
product of these two tables because there is no way to create a subset.
But, if artist comes first, we reduce that table to one row - the one
containing "AC/DC".

An optimization that could be added would be to look for comparisons
between a column and constant, and move the tables with those columns
to the beginning of the list.

## Query Parser

The query parser handles some basic SQL syntax, but is missing some
important features. Among the things missing that are present in some
of the test files are:
* Table aliases  (FROM Album A, Track T)
* COUNT
* Numeric values

The parser is implemented using a parser combinator library called
Prcc. The parser is contained in the select-parser.scm file.

The sql.scm file has several ways to invoke

## Installation Instructions

This program uses some extension libraries that you may have to install
in Chicken Scheme. You may have to do:

```bash
sudo chicken-install lookup-table
sudo chicken-install data-structures
sudo chicken-install csv
sudo chicken-install prcc
```

To compile the program, run:
csc -o sql sql.scm

## Running

To get an interactive prompt:
sql

If you have rlwrap on your system to add readline functionality, then
rlwrap sql

To execute a .sql file:
sql somefile.sql

To try one of the .md test files:
sql ../tests/select/sometestfile.md

## To Do

* Add table aliasing
* Add COUNT
* Add DISTINCT
* Add a query planner that can optimize the order that tables are joined
