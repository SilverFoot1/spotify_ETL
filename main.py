
from Extract import extract_recently_played, extract_track_data, extract_album_data, extract_artist_data
from Transform import basic_validation, data_transformations
from Load import create_database, load_data

# The token is needed for authorization from the spotify web api, it can be gotten from the following url
# URL is https://developer.spotify.com/console/get-recently-played/


if __name__ == '__main__':
    # todo some sort of visualisation for results.

    # Basic validation will raise exception if the DF is empty, if there are any null values, or if the ID's are not
    # unique e.g. timestamps can't have duplicates (cannot listen to 2 songs at once, and will act as a natural key)

    # Returns dataframe (DF) with facts i.e. the song id and the timestamp for when it was played.
    fact_table_df = extract_recently_played()
    basic_validation(fact_table_df, 'Fact Table')

    # gets track data in a DF, and a list of album ids associated with those tracks. Track ids must be unique.
    track_data, album_ids = extract_track_data(fact_table_df)
    basic_validation(track_data, 'Tracks Data')

    # gets album data in a DF, and a list of the MAIN artists associated with the album. Album ids must be unique.
    album_data, artist_ids = extract_album_data(album_ids)
    basic_validation(album_data, 'Album Data')

    # gets artist data in a DF. Artist ids must be unique.
    artist_data = extract_artist_data(artist_ids)
    basic_validation(artist_data, 'Artist Data')

    # Sends the DFs to have data transformations done (Transform.py). We know there is no missing data by this point
    track_data, album_data, artist_data = data_transformations(track_data, album_data, artist_data)

    # Creates a SQLite DB.
    create_database()

    # takes all the DFs to be loaded into the DB
    load_data(artist_data, album_data, track_data, fact_table_df)

    print('EXAMPLE: TRACKS \n', track_data.to_string(), '\n\n This is a print out of the tracks DF')
