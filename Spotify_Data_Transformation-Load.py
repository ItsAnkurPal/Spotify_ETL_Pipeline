import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def album(data):
    album_list=[]
    for row in data['items']:
        album_id=row['track']['album']['id']
        name=row['track']['album']['name']
        release_date=row['track']['album']['release_date']
        tracks=row['track']['album']['total_tracks']
        album_url=row['track']['external_urls']['spotify']
        album_ele={'album_id':album_id,'album_name':name,'release_date':release_date,'Total_tracks':tracks,'url':album_url}
        album_list.append(album_ele)
        
    return album_list
    
    
def artist(data):
    artist_list=[]
    for row in data['items']:
        for key,value in row.items():
            if key=='track':
                for artist in value['artists']:
                    artist_id=artist['id']
                    name=artist['name']
                    artist_url=artist['uri']
                    artist_ele={'artist_id':artist_id,'artist_name':name,'url':artist_url}
                    artist_list.append(artist_ele)
                    
    return artist_list
    
    
def songs(data):
    songs_list=[]
    for row in data['items']:
        song_id=row['track']['id']
        name=row['track']['name']
        popularity=row['track']['popularity']
        track_no=row['track']['track_number']
        duration=row['track']['duration_ms']
        song_ele={'song_id':song_id,'song_name':name,'Popularity':popularity,'Track_number':track_no,'Duration':duration}
        songs_list.append(song_ele) 
        
    return songs_list


def lambda_handler(event, context):
    
    s3=boto3.client('s3')
    Bucket="spotify-bucket-ankur"
    key="Raw_data/to_processed/"
    
    spotify_data=[]
    spotify_keys=[]
    for file in s3.list_objects(Bucket=Bucket,Prefix=key)['Contents']:
        file_key=file['Key']
        if (file_key.split('.')[-1])=='json':
            response=s3.get_object(Bucket=Bucket,Key=file_key)
            content=response['Body']  
            jsonObject=json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
        
          
    for data in spotify_data:
        album_data=album(data)
        artist_data=artist(data)
        songs_data=songs(data)
        
        album_df=pd.DataFrame.from_dict(album_data)
        album_df=album_df.drop_duplicates(subset=['album_id'])
        album_df['release_date']=pd.to_datetime(album_df['release_date'],errors='coerce')
        
        artist_df=pd.DataFrame.from_dict(artist_data)
        artist_df=artist_df.drop_duplicates(subset=['artist_id'])
        
        songs_df=pd.DataFrame.from_dict(songs_data)
        songs_df=songs_df.drop_duplicates(subset=['song_id'])
        
        songs_key="Transformed_data/Songs_data/song_transformed_"+str(datetime.now())+'.csv'
        songs_buffer=StringIO()
        songs_df.to_csv(songs_buffer , index=False)
        songs_content=songs_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=songs_key,Body=songs_content)
        
        album_key="Transformed_data/Album_data/album_transformed_"+str(datetime.now())+'.csv'
        album_buffer=StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content=album_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=album_key,Body=album_content)
        
        artist_key="Transformed_data/Artist_data/artist_transformed_"+str(datetime.now())+'.csv'
        artist_buffer=StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content=artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=artist_key,Body=artist_content)
        
    s3_resource=boto3.resource('s3')
    for key in spotify_keys:
        copy_source={
            'Bucket':Bucket,
            'Key':key
        }
        s3_resource.meta.client.copy(copy_source,Bucket,'Raw_data/processed/'+key.split('/')[-1])
        s3_resource.Object(Bucket,key).delete()
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
