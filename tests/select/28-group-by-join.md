Group tracks by genre.

## Query

    SELECT G.Name, COUNT(*)
    FROM Genre G, Track T
    WHERE T.GenreId = G.GenreId
    GROUP BY G.GenreId
    ORDER BY G.Name

## Answer

    Alternative,40
    Alternative & Punk,332
    Blues,81
    Bossa Nova,15
    Classical,74
    Comedy,17
    Drama,64
    Easy Listening,24
    Electronica/Dance,30
    Heavy Metal,28
    Hip Hop/Rap,35
    Jazz,130
    Latin,579
    Metal,374
    Opera,1
    Pop,48
    R&B/Soul,61
    Reggae,58
    Rock,1297
    Rock And Roll,12
    Sci Fi & Fantasy,26
    Science Fiction,13
    Soundtrack,43
    TV Shows,93
    World,28
