Perform a simple join to show tracks with a particular name. (Compare 16-whole-lotta-love.md.)

## Query

    SELECT A.Title, T.Milliseconds
    FROM Album A, Track T
    WHERE T.AlbumId = A.AlbumId AND T.Name = 'Whole Lotta Love'
    ORDER BY A.Title

## Answer

    BBC Sessions [Disc 1] [Live],373394
    Led Zeppelin II,334471
    The Song Remains The Same (Disc 2),863895
