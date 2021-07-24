
import sqlite3

database = 'sqlite:///spotify_project_DW'


def create_database():
    conn = sqlite3.connect('spotify_project_DW.sqlite')
    cursor = conn.cursor()

    create_artist_query = ''' 
    create table if not exists artist_DIM(
        artist_id integer primary key autoincrement not null,
        artist_natural_id text not null,
        artist_name text not null,
        num_followers integer not null,
        artist_img_url text not null,
        popularity_rating integer not null        
    ); '''

    create_album_query = '''   
    create table if not exists album_DIM(
        album_id integer primary key autoincrement not null,
        album_natural_id text not null,
        album_art_url text not null,
        album_name text not null,
        popularity_rating integer not null,
        release_date text not null,
        artist_id integer not null,
        foreign key (artist_id) references artist_DIM(artist_id)      
    );
    '''

    create_track_query = '''
    create table if not exists tracks_DIM(
        track_id integer primary key autoincrement not null, 
        track_natural_id text not null,
        track_name text not null,
        duration_secs integer not null,
        formatted_duration text not null,
        explicit_rating bit not null,
        popularity_rating integer not null,
        track_num integer not null,
        album_id integer not null,
        foreign key (album_id) references album_DIM(album_id)        
    );
    '''

    create_time_query = '''
    create table if not exists times_DIM(
        time_id integer primary key autoincrement not null,
        time text not null    
    ); 
    '''

    create_fact_table = '''
    create table if not exists recently_played_fact(
        track_id integer not null,
        time_id text not null,
        foreign key (track_id) references tracks_DIM(track_id),
        foreign key (time_id) references times_DIM(time_id)
        primary key (track_id, time_id)
    );          
    '''

    # cursor.execute('drop table if exists recently_played_fact;')
    # cursor.execute('drop table if exists times_DIM;')
    # cursor.execute('drop table if exists tracks_DIM;')
    # cursor.execute('drop table if exists album_DIM;')
    # cursor.execute('drop table if exists artist_DIM;')

    cursor.execute(create_artist_query)
    cursor.execute(create_album_query)
    cursor.execute(create_track_query)
    cursor.execute(create_time_query)
    cursor.execute(create_fact_table)

    conn.commit()
    conn.close()


def load_data(artist_df, album_df, track_df, fact_table_df):
    conn = sqlite3.connect('spotify_project_DW.sqlite')
    cursor = conn.cursor()

    for index in range(len(artist_df)):
        row = artist_df.iloc[index, :].values

        cursor.execute('''
        insert into artist_DIM (artist_natural_id, artist_name, num_followers, artist_img_url, popularity_rating)
        values (?,?,?,?,?);
        ''', (row[0], row[1], int(row[2]), row[3], int(row[4])))

    for index in range(len(album_df)):
        row = album_df.iloc[index, :].values

        cursor.execute('''
        insert into album_DIM (album_natural_id, album_art_url, album_name, popularity_rating, release_date, artist_id)
        values (?,?,?,?,?,  (select artist_id from artist_DIM where artist_natural_id = ?))    
        ''', (row[0], row[1], row[2], int(row[3]), str(row[4]), row[5]))

    for index in range(len(track_df)):
        row = track_df.iloc[index, :].values

        cursor.execute('''
        insert into tracks_DIM (track_natural_id, track_name, duration_secs, explicit_rating,
        popularity_rating, track_num, album_id, formatted_duration)
        values (?,?,?,?,?,?, (select album_id from album_DIM where album_natural_id = ?), ?)
        ''', (row[0], row[1], int(row[2]), int(row[3]), int(row[4]), int(row[5]), row[6], row[7]))

    for index in range(len(fact_table_df)):
        row = fact_table_df.iloc[index, :].values

        cursor.execute('''
        insert into times_DIM (time) 
        values (?)
        ''', (str(row[0]),))

    for index in range(len(fact_table_df)):
        row = fact_table_df.iloc[index, :].values
        # todo dunno if replace breaks it or not
        cursor.execute('''
        insert or replace into recently_played_fact (time_id, track_id) 
        values ( 
        (select time_id from times_DIM where time = ?),
        (select track_id from tracks_DIM where track_natural_id = ?)    
        )
        ''', (str(row[0]), row[1]))

    conn.commit()
    conn.close()
