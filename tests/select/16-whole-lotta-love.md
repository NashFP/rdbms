Perform a simple join to show tracks with a particular name.

## Query

    SELECT Album.Title
    FROM Album, Track
    WHERE Track.AlbumId = Album.AlbumId AND Track.Name = 'Whole Lotta Love'
    ORDER BY Album.Title

## Answer

    BBC Sessions [Disc 1] [Live],373394
    Led Zeppelin II,334471
    The Song Remains The Same (Disc 2),863895
