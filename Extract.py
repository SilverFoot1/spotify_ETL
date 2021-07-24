
import pandas as pd
import requests
from datetime import datetime
import datetime


USER_ID = '22ifudskuwf2uos2x32eeb6by'  # This is your username on spotify
TOKEN = 'BQCUCQ9qnyV7HGYrAFTHQgLHk-JCO1nIFvZO-VZV6SBEuNT4k3cOto83qfh1BxwF-JjxNpA3taB2aKCV_AsY5j9-IBeb96wTlRseAC4FiVUDXW3jRo_JVo4pJ2f8QJCujd3BSh3inDeQrAT9YFw-51gMeZD8oj0RIzhyD0SC'
HEADERS = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {TOKEN}'
}


def extract_recently_played():
    today = datetime.datetime.now()
    past = today - datetime.timedelta(days=7)
    unix_timestamp = int(past.timestamp()) * 1000

    r = requests.get(f'https://api.spotify.com/v1/me/player/recently-played?after={unix_timestamp}', headers=HEADERS)

    recent_played_data = r.json()

    if 'error' in recent_played_data.keys():
        print('\n Data Not Retrieved: Try A New Spotify Token')

    # todo time played for fact table
    # todo validation check here?
    track_ids = []
    times_played = []
    for track in recent_played_data['items']:
        track_ids.append(track['track']['id'])
        times_played.append(track['played_at'])

    fact_df = pd.DataFrame({'Time Played At': times_played, 'Track IDs': track_ids})

    return fact_df


def extract_track_data(facts_df):

    track_id_list = facts_df['Track IDs'].values

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

    tracks_df = pd.DataFrame(tracks)

    return [tracks_df, album_ids]


def extract_album_data(album_ids):
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

    artist_df = pd.DataFrame(artist_data)

    return artist_df

