# Query 1: Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

create_listen_history_by_session = """
        CREATE TABLE IF NOT EXISTS listen_history_by_session
            (
                itemInSession int, 
                sessionId int,
                artist text, 
                song text,
                length float,
                PRIMARY KEY ((sessionId), itemInSession)
            )
    """

insert_listen_history_by_session = (
    "INSERT INTO listen_history_by_session (itemInSession, sessionId, artist, song, length) VALUES (%s,%s,%s,%s,%s)",
    [(3, int), (8, int), (0, str), (9, str), (5, float)],
)

select_listen_history_by_session = "SELECT artist, song, length FROM listen_history_by_session WHERE sessionId = 338 and itemInSession = 4"

drop_listen_history_by_session = "DROP TABLE IF EXISTS listen_history_by_session"


# Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
create_song_playlist_session = """
        CREATE TABLE IF NOT EXISTS song_playlist_session
            (
                userId int, 
                sessionId int,
                itemInSession int,
                artist text, 
                song text,
                firstName text,
                lastName text,
                PRIMARY KEY ((userId), sessionId, itemInSession)
            )
            WITH CLUSTERING ORDER BY (sessionId DESC, iteminSession DESC)
    """

insert_song_playlist_session = (
    "INSERT INTO song_playlist_session (userId, sessionId, itemInSession, artist, song, firstName, lastName) VALUES (%s,%s,%s,%s,%s,%s,%s)",
    [(10, int), (8, int), (3, int), (0, str), (9, str), (1, str), (4, str)],
)

select_song_playlist_session = "SELECT artist, song, firstName, lastName FROM song_playlist_session WHERE userId = 10 AND sessionId = 182"

drop_song_playlist_session = "DROP TABLE IF EXISTS song_playlist_session"


# Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

create_username_by_listen_history = """
        CREATE TABLE IF NOT EXISTS username_by_listen_history
            (
                song text,
                userId int,
                firstName text,
                lastName text,
                PRIMARY KEY ((song), userId)
            )
    """

insert_username_by_listen_history = (
    "INSERT INTO username_by_listen_history (song, userId, firstName, lastName) VALUES (%s,%s,%s,%s)",
    [(9, str), (10, int), (1, str), (4, str)],
)

select_username_by_listen_history = (
    "SELECT firstName, lastName FROM username_by_listen_history WHERE song = 'All Hands Against His Own'"
)

drop_username_by_listen_history = "DROP TABLE IF EXISTS username_by_listen_history"

# Export to other files

create_stmts = [create_listen_history_by_session, create_song_playlist_session, create_username_by_listen_history]
insert_stmts = [insert_listen_history_by_session, insert_song_playlist_session, insert_username_by_listen_history]
select_stmts = [select_listen_history_by_session, select_song_playlist_session, select_username_by_listen_history]
drop_stmts = [drop_listen_history_by_session, drop_song_playlist_session, drop_username_by_listen_history]
