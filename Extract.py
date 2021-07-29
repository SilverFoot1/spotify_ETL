import pandas as pd
import requests
from datetime import datetime
import datetime


def get_token():
    print('Spotify Recently Played ETL')
    print('URL to get Token: https://developer.spotify.com/console/get-recently-played/')
    return input('Enter Spotify Token: ')


TOKEN = get_token()
USER_ID = '22ifudskuwf2uos2x32eeb6by'  # This is your username on spotify -- use on web api.
# Headers are used for fetch requests for the spotify API, including the token generated.
HEADERS = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {TOKEN}'
}


def extract_recently_played():
    # Get unix timestamp in ms (as required by spotify web-api), for a week ago.
    today = datetime.datetime.now()
    past = today - datetime.timedelta(days=7)
    unix_timestamp = int(past.timestamp()) * 1000

    # Makes request for recently played songs, from up to 7 days ago. Will return data for up to 20 songs.
    r = requests.get(f'https://api.spotify.com/v1/me/player/recently-played?after={unix_timestamp}', headers=HEADERS)

    # converts response to json.
    recent_played_data = r.json()

    # When the token expires, the response object will contain error. This lets user know to generate a new token.
    if 'error' in recent_played_data.keys():
        print('\n Data Not Retrieved: Try A New Spotify Token')

    # extracts timestamps and track id for the song played, and adds them to lists.
    track_ids = []
    times_played = []
    for track in recent_played_data['items']:
        track_ids.append(track['track']['id'])
        times_played.append(track['played_at'])

    # creates fact table DF and returns.
    fact_df = pd.DataFrame({'Time Played At': times_played, 'Track IDs': track_ids})

    return fact_df


def extract_track_data(facts_df):
    # Gets track ids as an array, removes duplicates and then joins them in the format required by the web-api.
    track_id_list = facts_df['Track IDs'].values
    # makes a dict from the list values, and then converts back to list, removing duplicates.
    track_id_list = list(dict.fromkeys(track_id_list))
    id_string = ','.join(track_id_list)

    r = requests.get(f'https://api.spotify.com/v1/tracks?ids={id_string}', headers=HEADERS)

    track_json = r.json()

    tracks = []
    album_ids = []
    index_count = 0

    for track in track_json['tracks']:
        song_dict = {
            'track_identifier': track_id_list[index_count],
            'name': track['name'],
            'duration': track['duration_ms'],
            'explicit_rating': track['explicit'],
            'popularity_rating': track['popularity'],
            'track_num': track['track_number'],
            'album_reference': track['album']['id']
        }
        album_ids.append(track['album']['id'])
        tracks.append(song_dict)
        index_count += 1

    # track data extracted, put into a dictionary, converted to a DF, and associated album ids put into a list.
    # DF and album ids are returned.
    tracks_df = pd.DataFrame(tracks)

    return [tracks_df, album_ids]


def extract_album_data(album_ids):
    # Removes duplicates from album ids, and joins them into string for web-api.
    album_ids = list(dict.fromkeys(album_ids))
    album_string = ','.join(album_ids)

    r = requests.get(f'https://api.spotify.com/v1/albums?ids={album_string}', headers=HEADERS)
    album_json = r.json()

    artist_ids = []
    album_data = []

    for album in album_json['albums']:
        album_dict = {
            'album_identifier': album['id'],
            'album_art_url': album['images'][0]['url'],
            'album_name': album['name'],
            'popularity': album['popularity'],
            'release_date': album['release_date'],
            'artist_reference': album['artists'][0]['id']
        }

        artist_ids.append(album['artists'][0]['id'])
        album_data.append(album_dict)

    # album data extracted, put into a dictionary, converted to a DF, and artist ids collected in a list.
    album_df = pd.DataFrame(album_data)

    return [album_df, artist_ids]


def extract_artist_data(artist_ids):
    artist_ids = list(dict.fromkeys(artist_ids))
    artist_ids_string = ','.join(artist_ids)

    r = requests.get(f'https://api.spotify.com/v1/artists?ids={artist_ids_string}', headers=HEADERS)
    artists_json = r.json()

    artist_data = []

    for artist in artists_json['artists']:
        artist_dict = {
            'artist_identifier': artist['id'],
            'artist_name': artist['name'],
            'num_followers': artist['followers']['total'],
            'image_url': artist['images'][0]['url'],
            'popularity': artist['popularity']
        }
        artist_data.append(artist_dict)

    # Same logic as before. Duplicates removed, string made, fetch made, data extracted and converted to DF.
    # DF is returned but no IDs, as this will be the terminal table in the database.
    artist_df = pd.DataFrame(artist_data)

    return artist_df
