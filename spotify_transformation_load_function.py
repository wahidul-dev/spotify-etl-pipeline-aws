import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd


def album(data):
    # Initialize an empty list to store album information
    album_list = []

    # Iterate over the 'items' in the 'data' object
    for item in data['items']:
        # Extract album information from the nested structure within 'item'
        id = item['track']['album']['id']
        name = item['track']['album']['name']
        release_date = item['track']['album']['release_date']
        total_tracks = item['track']['album']['total_tracks']
        external_urls = item['track']['album']['external_urls']['spotify']

        # Create a dictionary 'album_element' to store the extracted information
        album_element = {
            'id': id,
            'name': name,
            'release_date': release_date,
            'total_tracks': total_tracks,
            'external_urls': external_urls
        }

        # Append 'album_element' to the 'album_list'
        album_list.append(album_element)

    # Return the 'album_list' containing the extracted album information
    return album_list

def artist(data):
    # Initialize an empty list for artist information
    artist_list = []
    
    # Extract artist data from the 'data' object
    for item in data['items']:
        for key, value in item.items():
            if key == 'track':
                for artist in value['artists']:
                    # Create a dictionary with artist details
                    artist_dict = {
                        'artist_id': artist['id'],
                        'artist_name': artist['name'],
                        'external_url': artist['href']
                    }
                    artist_list.append(artist_dict)
    
    # 'artist_list' now contains artist information
    
    return artist_list


def song(data):
    # Initialize an empty list to store song information
    song_list = []
    
    # Loop through each item in the 'data' dictionary's 'items' list
    for row in data['items']:
        # Extract various attributes of the song from the 'row' dictionary
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        
        # Create a dictionary representing a song element with extracted attributes
        song_element = {
            'song_id': song_id,
            'song_name': song_name,
            'song_duration': song_duration,
            'song_url': song_url,
            'song_popularity': song_popularity,
            'song_added': song_added,
            'album_id': album_id,
            'artist_id': artist_id
        }
        
        # Append the song element to the 'song_list'
        song_list.append(song_element)
        
        return song_list
        
def save_dataframe_to_s3(data_df, bucket, key_prefix):
    s3=boto3.client('s3')
    # Generate the key for the S3 object
    key = f"{key_prefix}_{str(datetime.now())}.csv"
    
    # Create a buffer for the DataFrame
    buffer = StringIO()
    
    # Convert the DataFrame to CSV and store it in the buffer
    data_df.to_csv(buffer, index=False)
    
    # Get the buffer content as a string
    content = buffer.getvalue()
    
    # Put the content in the S3 bucket
    s3.put_object(Bucket=bucket, Key=key, Body=content)



def lambda_handler(event, context):
    
    s3=boto3.client('s3')
    
    Bucket = "spotify-etl-project-wahidul"
    Key = "raw_data/to_process/"
    
    spotify_data = []
    spotify_key = []
    for item in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = item['Key']
        if file_key.split('.')[-1] == 'json':
            response = s3.get_object(Bucket=Bucket, Key = file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_key.append(file_key)
    
    
    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        song_list = song(data)
        
        # Create DataFrames from dictionaries and drop duplicates in a single line
        album_df = pd.DataFrame.from_dict(album_list).drop_duplicates(subset=['id'])
        song_df = pd.DataFrame.from_dict(song_list).drop_duplicates(subset=['song_id'])
        artist_df = pd.DataFrame.from_dict(artist_list).drop_duplicates(subset=['artist_id'])
        
        # Convert date columns to datetime objects
        album_df['release_date'] = pd.to_datetime(album_df['release_date'])
        song_df['song_added'] = pd.to_datetime(song_df['song_added'])
    
        key_prefix_album = "transformed_data/album_data/album_transformed"
        key_prefix_artist = "transformed_data/artist_data/artist_transformed"
        key_prefix_song = "transformed_data/song_data/song_transformed"
        
        # Save DataFrames to S3
        save_dataframe_to_s3(album_df, Bucket, key_prefix_album)
        save_dataframe_to_s3(artist_df, Bucket, key_prefix_artist)
        save_dataframe_to_s3(song_df, Bucket, key_prefix_song)
    
    s3_resource =boto3.resource('s3')
    for key in spotify_key:
        copy_source = {
            'Bucket' : Bucket,
            'Key' : key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split('/')[-1])
        s3_resource.Object(Bucket, key).delete()
    