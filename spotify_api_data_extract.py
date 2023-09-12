import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime
import boto3

def lambda_handler(event, context):
    print('hello world')
    
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    # Initialize Spotify client
    credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=credentials_manager)
    
    # Define the Spotify playlist link
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbLiRSasKsNU9"

    # Extract the playlist URI from the link
    playlist_uri = playlist_link.split("/")[-1]
    
    # Fetch playlist tracks using the Spotify API
    data = sp.playlist_tracks(playlist_uri)
    
    client = boto3.client('s3')
    
    filename = "spotify_raw_" + str(datetime.now()) + '.json'
    
    client.put_object(
            Bucket="spotify-etl-project-wahidul",
            Key='raw_data/to_process/' + filename,
            Body=json.dumps(data)
        )