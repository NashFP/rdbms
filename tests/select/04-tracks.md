Select a row by a string rather than by the table's primary key.

## Query

    SELECT TrackId, AlbumId, GenreId, Milliseconds, UnitPrice
    FROM Track
    WHERE Name = 'Rehab'

## Answer

    3455,321,14,213240,0.99
