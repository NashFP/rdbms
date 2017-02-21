Table names are optional when column names alone are unambiguous (compare 16-whole-lotta-love.md)

## Query

    SELECT Title, Milliseconds
    FROM Album, Track
    WHERE Track.AlbumId = Album.AlbumId AND Name = 'Whole Lotta Love'
    ORDER BY Title

## Answer

    BBC Sessions [Disc 1] [Live],373394
    Led Zeppelin II,334471
    The Song Remains The Same (Disc 2),863895
