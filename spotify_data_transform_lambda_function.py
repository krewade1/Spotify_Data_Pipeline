import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd

def album(data):
    album_list=[]
    for i in data['items']:
        album_id=i["track"]["album"]['id']
        album_name=i["track"]["album"]['name']
        album_date=i["track"]["album"]['release_date']
        album_total_track=i["track"]["album"]['total_tracks']
        album_url=i["track"]['album']['external_urls']['spotify']
        album_elements={'album_id':album_id,'name':album_name,'release_date':album_date,'total_tracks':album_total_track,
                   'URL':album_url}
        album_list.append(album_elements)
    return album_list
    
def artist(data):
    artist_list=[]
    for i in data['items']:
        for key,value in i.items():
            if key=="track":
                for artists in value['artists']:
                    data_artists={'artists_id':artists['id'],'artist_name':artists['name'],'artists_URL':artists['href']}
                    artist_list.append(data_artists)
    return artist_list
def songs(data):
    
    song_list=[]
    for i in data['items']:
        song_id=i['track']['id']
        song_name=i['track']['name']
        song_duration=i['track']['duration_ms']
        song_url=i['track']['external_urls']['spotify']
        song_popularity=i['track']['popularity']
        song_added=i['added_at']
        album_id=i['track']['album']['id']
        artist_id=i['track']['album']['artists'][0]['id']
        song_elements={'song_id':song_id,'song_name':song_name,'song_duration':song_duration,'song_url':song_url,'song_popularity':song_popularity,'song_added':song_added,'album_id':album_id,'artist_id':artist_id}
        song_list.append(song_elements)
    return  song_list
    
def lambda_handler(event, context):
    s3=boto3.client('s3')
    Bucket='spotify-etl-project-kunalrewade'
    Key='raw_data/to_processed/'
    spotify_data= []
    spotify_keys= []
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] =="json":
            response = s3.get_object(Bucket=Bucket, Key=file_key)
            Content= response['Body']       
            jsonObject = json.loads(Content.read())
            print(jsonObject)
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
            
    for data in spotify_data:
        album_list=album(data)
        artist_list=artist(data)
        song_list=songs(data)
        
        album_df=pd.DataFrame.from_dict(album_list)

        album_df = album_df.drop_duplicates(subset=['album_id'])
        album_df['release_date'] = pd.to_datetime(album_df['release_date'],errors='coerce')
        artist_df = pd.DataFrame.from_dict(artist_list)
        
      
        song_df=pd.DataFrame.from_dict(song_list)
        
        
    
        album_df.dropna()
        song_df['song_added']=pd.to_datetime(song_df['song_added'])
        
        song_key="transformed_data/songs_data/songs_transformed_"+ str(datetime.now())+".csv"
        song_buffer=StringIO()
        song_df.to_csv(song_buffer)
        song_content=song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=song_key, Body=song_content)
        
        artist_key="transformed_data/artist_data/artists_transformed_"+ str(datetime.now())+".csv"
        artist_buffer=StringIO()
        artist_df.to_csv(artist_buffer)
        artist_content=artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)
        
        album_key = "transformed_data/album_data/album_transformed_" + str(datetime.now()) + ".csv"
        album_buffer=StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_content)
        
    
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source,Bucket,'raw_data/processed/'+ key.split("/")[-1])
        s3_resource.Object(Bucket,key).delete()
