Join three tables to show who has recorded a track titled "Believe".

## Query

    SELECT Artist.Name, Album.Title, Track.Name, Track.Milliseconds
    FROM Artist, Album, Track
    WHERE Artist.ArtistId = Album.ArtistId AND Album.AlbumId = Track.AlbumId AND Track.Name = 'Believe'
    ORDER BY Artist.Name

## Answer

    Lenny Kravitz,Greatest Hits,Believe,295131
    Smashing Pumpkins,Judas 0: B-Sides and Rarities,Believe,192940
    Spyro Gyra,Heart of the Night,Believe,310778
