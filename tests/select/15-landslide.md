Use the `AND` operator to show the 'Landslide' that costs 99 cents.

## Query

    SELECT TrackId, Name, AlbumId, Composer
    FROM Track
    WHERE Name = 'Landslide' AND UnitPrice = 0.99

## Answer

    2494,Landslide,202,Stevie Nicks
