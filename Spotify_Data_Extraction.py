import json
import os
import spotipy 
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime


def lambda_handler(event, context):
    
    client_id=os.environ.get('client_ida')
    client_secret=os.environ.get('client_secret')
    
    ccm=SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)
    spot= spotipy.Spotify(client_credentials_manager=ccm)
    playlist_link="https://open.spotify.com/playlist/2YRe7HRKNRvXdJBp9nXFza"
    pl=playlist_link.split("/")[-1]
    
    sp_data=spot.playlist_tracks(pl)
    
    filename="Transformed_data/Songs_data/song_transformed"+str(datetime.now())+'.json'
    
    client=boto3.client('s3')
    client.put_object(
        
        Bucket="spotify-bucket-ankur",
        Key="Raw_data/to_processed/"+filename,
        Body=json.dumps(sp_data)
        
        )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
