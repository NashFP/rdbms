// sample_queries.js - Selected queries, hand-translated from SQL to JS.

"use strict";

let fs = require("fs");
let parse = require("csv-parse/lib/sync");
let combinations = require("cartesian-product");

function load(tableName) {
    let filename = __dirname + "/../tests/tables/" + tableName + ".csv";
    return parse(fs.readFileSync(filename), {columns: true});
}

let genre = load("genre");
let track = load("track");
let artist = load("artist");
let album = load("album");
let playlist = load("playlist");


// ----


// 01-genre-name.md
//     SELECT Name
//     FROM Genre
//     WHERE GenreId = 6


// 05-album-tracks.md
//     SELECT Name
//     FROM Track
//     WHERE AlbumId = 187
//     ORDER BY TrackId


// 10-count-u2-albums.md
//     SELECT COUNT(*)
//     FROM Album
//     WHERE ArtistId = 150
Array.prototype.groupAll = function groupAll() { return [this]; };


// 12-all-playlists.md
//     SELECT PlaylistId, Name
//     FROM Playlist
//     ORDER BY PlaylistId


// 16-whole-lotta-love.md
//     SELECT Album.Title
//     FROM Album, Track
//     WHERE Track.AlbumId = Album.AlbumId AND Track.Name = 'Whole Lotta Love'
//     ORDER BY Album.Title


// 17-believe.md
//     SELECT Artist.Name, Album.Title, Track.Name, Track.Milliseconds
//     FROM Artist, Album, Track
//     WHERE Artist.ArtistId = Album.ArtistId AND Album.AlbumId = Track.AlbumId AND Track.Name = 'Believe'
//     ORDER BY Artist.Name
