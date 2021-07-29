
import sqlite3

database = 'sqlite:///spotify_project_DW'


def create_database():
    # establishes connection. If it doesn't exist is creates it.
    conn = sqlite3.connect('spotify_project_DW.sqlite')
    cursor = conn.cursor()

    # Tables are created in order of dependencies to avoid errors: artists >> albums >> tracks >> facts + time table.

    # SQL queries to create the tables in SQLite DB.
    create_artist_query = ''' 
    CREATE TABLE IF NOT EXISTS artist_DIM(
        artist_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        artist_natural_id TEXT NOT NULL,
        artist_name TEXT NOT NULL,
        num_followers INTEGER NOT NULL,
        artist_img_url TEXT NOT NULL,
        popularity_rating INTEGER NOT NULL,
        UNIQUE(artist_id, artist_natural_id)        
    ); '''

    create_album_query = '''   
    CREATE TABLE IF NOT EXISTS album_DIM(
        album_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        album_natural_id TEXT NOT NULL,
        album_art_url TEXT NOT NULL,
        album_name TEXT NOT NULL,
        popularity_rating INTEGER NOT NULL,
        release_date TEXT NOT NULL,
        artist_id INTEGER NOT NULL,
        FOREIGN KEY (artist_id) REFERENCES artist_DIM(artist_id)      
    );
    '''

    create_track_query = '''
    CREATE TABLE IF NOT EXISTS tracks_DIM(
        track_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
        track_natural_id TEXT NOT NULL,
        track_name TEXT NOT NULL,
        duration_secs INTEGER NOT NULL,
        formatted_duration TEXT NOT NULL,
        explicit_rating BIT NOT NULL,
        popularity_rating INTEGER NOT NULL,
        track_num INTEGER NOT NULL,
        album_id INTEGER NOT NULL,
        FOREIGN KEY (album_id) REFERENCES album_DIM(album_id)        
    );
    '''

    create_time_query = '''
    CREATE TABLE IF NOT EXISTS times_DIM(
        time_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        time TEXT NOT NULL  
    ); 
    '''

    create_fact_table = '''
    CREATE TABLE IF NOT EXISTS recently_played_fact(
        track_id INTEGER NOT NULL,
        time_id TEXT NOT NULL,
        FOREIGN KEY (track_id) REFERENCES tracks_DIM(track_id),
        FOREIGN KEY (time_id) REFERENCES times_DIM(time_id)
        PRIMARY KEY (track_id, time_id)
    );          
    '''

    # Ability to drop tables for testing/debugging.

    # cursor.execute('DROP TABLE IF EXISTS recently_played_fact;')
    # cursor.execute('DROP TABLE IF EXISTS times_DIM;')
    # cursor.execute('DROP TABLE IF EXISTS tracks_DIM;')
    # cursor.execute('DROP TABLE IF EXISTS album_DIM;')
    # cursor.execute('DROP TABLE IF EXISTS artist_DIM;')

    # Executes all of the queries.
    cursor.execute(create_artist_query)
    cursor.execute(create_album_query)
    cursor.execute(create_track_query)
    cursor.execute(create_time_query)
    cursor.execute(create_fact_table)

    # commits changes and closes DB connection (best practice)
    conn.commit()
    conn.close()


def load_data(artist_df, album_df, track_df, fact_table_df):

    # establishes DB connection and cursor.
    conn = sqlite3.connect('spotify_project_DW.sqlite')
    cursor = conn.cursor()

    # INSERT LOGIC:
    # It iterates over the values ('index') for the range of the length of each DF.
    # Using df.iloc, the row corresponding to the index for a given iteration is selected, and converted to an array.
    # Each DF has a query, with parameter values. These parameters are filled in with appropriate values from the array.

    # For foreign keys, a nested select query is used. E.g. for the album table it will need a reference for the key
    # for the right artist. The array has the spotify id, which acts as a natural key, so we can select the correct,
    # surrogate key that matches the natural key.

    # WHERE
    # artist_natural_id
    # NOT in (SELECT artist_natural_id FROM artist_DIM

    for index in range(len(artist_df)):
        row = artist_df.iloc[index, :].values

        cursor.execute('''
        INSERT INTO artist_DIM (artist_natural_id, artist_name, num_followers, artist_img_url, popularity_rating)
        VALUES (?,?,?,?,?);
        ''', (row[0], row[1], int(row[2]), row[3], int(row[4])))

    for index in range(len(album_df)):
        row = album_df.iloc[index, :].values

        cursor.execute('''
        INSERT INTO album_DIM (album_natural_id, album_art_url, album_name, popularity_rating, release_date, artist_id)
        VALUES (?,?,?,?,?,  (SELECT artist_id FROM artist_DIM WHERE artist_natural_id = ?))    
        ''', (row[0], row[1], row[2], int(row[3]), str(row[4]), row[5]))

    for index in range(len(track_df)):
        row = track_df.iloc[index, :].values

        cursor.execute('''
        INSERT INTO tracks_DIM (track_natural_id, track_name, duration_secs, explicit_rating,
        popularity_rating, track_num, album_id, formatted_duration)
        VALUES (?,?,?,?,?,?, (SELECT album_id FROM album_DIM WHERE album_natural_id = ?), ?)
        ''', (row[0], row[1], int(row[2]), int(row[3]), int(row[4]), int(row[5]), row[6], row[7]))

    for index in range(len(fact_table_df)):
        row = fact_table_df.iloc[index, :].values

        cursor.execute('''
        INSERT INTO times_DIM (time) 
        VALUES (?)
        ''', (str(row[0]),))

    for index in range(len(fact_table_df)):
        row = fact_table_df.iloc[index, :].values
        cursor.execute('''
        INSERT OR REPLACE INTO recently_played_fact (time_id, track_id) 
        VALUES ( 
        (SELECT time_id FROM times_DIM WHERE time = ?),
        (SELECT track_id FROM tracks_DIM WHERE track_natural_id = ?)    
        )
        ''', (str(row[0]), row[1]))

    conn.commit()
    conn.close()
