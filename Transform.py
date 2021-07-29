
from Exceptions import DataInvalid
from dateutil.parser import parse


def basic_validation(df, data_source):
    if df.empty:
        raise DataInvalid('DataFrame is Empty: Data Frame = ' + data_source)

    for col in df.columns:
        if df[col].isnull().values.any():
            raise DataInvalid('Data Has Missing Values: Data Frame = ' + data_source + ' : column = ' + col)

    if not df.iloc[:, 0].is_unique:
        raise DataInvalid('IDs Were Not Unique: Data Frame = ' + data_source)


def data_transformations(track_data, album_data, artist_data):

    # Converts the duration from ms to seconds.
    track_data['duration'] = track_data['duration'].apply(lambda x: int(x / 1000))

    # converts boolean values for the explicit rating to a binary value (1 for True, 0 for False)
    track_data['explicit_rating'] = track_data['explicit_rating'].apply(lambda x: 1 if x == True else 0)

    new_durations = []

    # iterates over the duration values that were converted to seconds.
    for duration in track_data['duration'].values:
        # extracts the minutes and left over seconds from the duration value, and formats it as a string.
        x = '' + str(int((duration / 60))) + ':' + str(int((duration % 60)))
        # If length is 3 e.g. 3:9, this inserts a 0 to make it 3:09, which is the standard time format for songs.
        if len(x) == 3:
            x = x[0:2] + '0' + x[2:]
        new_durations.append(x)

    # Creates new column in the data frame with newly formatted durations
    track_data['formatted_duration'] = new_durations

    # Converts the release date to a datetime object using 'parse'
    album_data['release_date'] = album_data['release_date'].apply(lambda x: parse(x))

    return track_data, album_data, artist_data
