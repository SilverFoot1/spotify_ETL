
from Extract import extract_recently_played, extract_track_data, extract_album_data, extract_artist_data
from Transform import basic_validation, data_transformations
from Load import create_database, load_data

if __name__ == '__main__':
    # todo make a web part for it in flask? maybe incorporate sonic features for some sort of analysis?
    # todo remove recently played json?
    # todo add comments, add DB diagram, clean code
    # todo token input stage

    fact_table_df = extract_recently_played()
    basic_validation(fact_table_df, 'Fact Table')

    track_data, album_ids = extract_track_data(fact_table_df)
    basic_validation(track_data, 'Tracks Data')

    album_data, artist_ids = extract_album_data(album_ids)
    basic_validation(album_data, 'Album Data')

    artist_data = extract_artist_data(artist_ids)
    basic_validation(artist_data, 'Artist Data')

    track_data, album_data, artist_data = data_transformations(track_data, album_data, artist_data)

    create_database()
    load_data(artist_data, album_data, track_data, fact_table_df)
