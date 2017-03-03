Join four tables to produce a playlist.

## Query

    SELECT T.TrackId, A.Title, T.Name, T.Milliseconds
    FROM Playlist PL, PlaylistTrack PLT, Track T, Album A
    WHERE PL.PlaylistId = PLT.PlaylistId AND PLT.TrackId = T.TrackId AND T.AlbumId = A.AlbumId AND PL.Name = 'Classical 101 - The Basics'
    ORDER BY T.TrackId

## Answer

    3403,Adorate Deum: Gregorian Chant from the Proper of the Mass,Intoitus: Adorate Deum,245317
    3404,Allegri: Miserere,"Miserere mei, Deus",501503
    3405,Pachelbel: Canon & Gigue,Canon and Gigue in D Major: I. Canon,271788
    3406,Vivaldi: The Four Seasons,"Concerto No. 1 in E Major, RV 269 ""Spring"": I. Allegro",199086
    3407,Bach: Violin Concertos,"Concerto for 2 Violins in D Minor, BWV 1043: I. Vivace",193722
    3408,Bach: Goldberg Variations,"Aria Mit 30 Veränderungen, BWV 988 ""Goldberg Variations"": Aria",120463
    3409,Bach: The Cello Suites,"Suite for Solo Cello No. 1 in G Major, BWV 1007: I. Prélude",143288
    3410,Handel: The Messiah (Highlights),"The Messiah: Behold, I Tell You a Mystery... The Trumpet Shall Sound",582029
    3411,The World of Classical Favourites,Solomon HWV 67: The Arrival of the Queen of Sheba,197135
    3412,Sir Neville Marriner: A Celebration,"""Eine Kleine Nachtmusik"" Serenade In G, K. 525: I. Allegro",348971
    3413,Mozart: Wind Concertos,"Concerto for Clarinet in A Major, K. 622: II. Adagio",394482
    3414,Haydn: Symphonies 99 - 104,"Symphony No. 104 in D Major ""London"": IV. Finale: Spiritoso",306687
    3415,Beethoven: Symhonies Nos. 5 & 6,Symphony No.5 in C Minor: I. Allegro con brio,392462
    3416,A Soprano Inspired,Ave Maria,338243
    3417,Great Opera Choruses,"Nabucco: Chorus, ""Va, Pensiero, Sull'ali Dorate""",274504
    3418,Wagner: Favourite Overtures,Die Walküre: The Ride of the Valkyries,189008
    3419,"Fauré: Requiem, Ravel: Pavane & Others","Requiem, Op.48: 4. Pie Jesu",258924
    3420,Tchaikovsky: The Nutcracker,"The Nutcracker, Op. 71a, Act II: Scene 14: Pas de deux: Dance of the Prince & the Sugar-Plum Fairy",304226
    3421,The Last Night of the Proms,"Nimrod (Adagio) from Variations On an Original Theme, Op. 36 ""Enigma""",250031
    3422,Puccini: Madama Butterfly - Highlights,Madama Butterfly: Un Bel Dì Vedremo,277639
    3423,"Holst: The Planets, Op. 32 & Vaughan Williams: Fantasies","Jupiter, the Bringer of Jollity",522099
    3424,Pavarotti's Opera Made Easy,"Turandot, Act III, Nessun dorma!",176911
    3425,Great Performances - Barber's Adagio and Other Romantic Favorites for Strings,"Adagio for Strings from the String Quartet, Op. 11",596519
    3426,Carmina Burana,Carmina Burana: O Fortuna,156710
    3427,"A Copland Celebration, Vol. I",Fanfare for the Common Man,198064
