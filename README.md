# spotify_ETL
This project retrieves data from the Spotify web-API relevant to a users most recently played songs. This is then stored within a database following a snowflake schema. 

An OAuth token needs to be retrieved from https://developer.spotify.com/console/get-recently-played/?limit=&after=&before= in order to function.

Database:

![Untitled Diagram](https://user-images.githubusercontent.com/80291504/127564216-aa34d013-4db4-4760-b830-ef0ffd055dd6.jpg)

Due to limitations set by the api, only up to 20 songs can be retrieved. Track, album and artist duplicates aren't stored.  