# TinyRdbms

This is a little SQL query engine. If you have Elixir installed, you can do this:

<pre class="console">
$ <strong>mix deps.get</strong>
$ <strong>iex -S mix</strong>
Erlang/OTP 19 [erts-8.2] [source] [64-bit] [smp:8:8] [async-threads:10] [hipe] [kernel-poll:false] [dtrace]

Compiling 1 file (.ex)
Interactive Elixir (1.4.0) - press Ctrl+C to exit (type h() ENTER for help)
iex(1)&gt; <strong>TinyRdbms.repl("../tests/tables")</strong>
sql&gt; <strong>SELECT TrackId, Name, Milliseconds FROM Track WHERE AlbumId = 237</strong>
{"TrackId", "Name", "Milliseconds"}
----
{2987, "Helter Skelter", "187350"}
{2988, "Van Diemen's Land", "186044"}
{2989, "Desire", "179226"}
{2990, "Hawkmoon 269", "382458"}
{2991, "All Along The Watchtower", "264568"}
{2992, "I Still Haven't Found What I'm Looking for", "353567"}
{2993, "Freedom For My People", "38164"}
{2994, "Silver And Gold", "349831"}
{2995, "Pride (In The Name Of Love)", "267807"}
{2996, "Angel Of Harlem", "229276"}
{2997, "Love Rescue Me", "384522"}
{2998, "When Love Comes To Town", "255869"}
{2999, "Heartland", "303360"}
{3000, "God Part II", "195604"}
{3001, "The Star Spangled Banner", "43232"}
{3002, "Bullet The Blue Sky", "337005"}
{3003, "All I Want Is You", "390243"}
sql&gt; <strong>SELECT Title FROM Album WHERE AlbumId = 237</strong>
{"Title"}
----
{"Rattle And Hum"}
</pre>

At the moment, it handles only the most basic `SELECT` syntax.
