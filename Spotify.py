#!/usr/bin/env python
# coding: utf-8

# In[2]:


pip install spotipy
import pandas as pd


# In[3]:


import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd


# In[4]:


client_credential_manager= SpotifyClientCredentials(client_id="504d3bd3ddbf4bb08dc1b61517463856",client_secret="2984768e9eb047afa5301a64a57ef54e")


# In[5]:


sp = spotipy.Spotify(client_credentials_manager=client_credential_manager)


# In[6]:


playlist_link="https://open.spotify.com/playlist/4hOKQuZbraPDIfaGbM3lKI"


# In[7]:


playlist_uri=playlist_link.split("/")[-1]


# In[8]:


data=sp.playlist_tracks(playlist_uri)


# In[9]:


data["items"][0]["track"]["album"]['id']


# In[10]:


data["items"][0]["track"]["album"]['name']


# In[11]:


data["items"][0]["track"]["album"]['release_date']


# In[12]:


data["items"][0]["track"]["album"]['total_tracks']


# In[13]:


data["items"][0]["track"]['album']['external_urls']['spotify']


# In[14]:


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



    


# In[15]:


#artist_list=[]
#for i in data('items'):
 #   data["items"][0]['track']["artists"]['name']
    
data['items'][0]['track']['artists']


# In[16]:


data['items'][1]['track']


# In[17]:


artist_list=[]
for i in data['items']:
    for key,value in i.items():
        if key=="track":
            for artists in value['artists']:
                data_artists={'artists_id':artists['id'],'artist_name':artists['name'],'artists_URL':artists['href']}
                artist_list.append(data_artists)


# In[18]:


print(artist_list)


# In[19]:


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


# In[20]:


print(song_list)


# In[21]:


album_df=pd.DataFrame.from_dict(album_list)


# In[22]:


song_df=pd.DataFrame.from_dict(song_list)


# In[23]:


artist_df=pd.DataFrame.from_dict(artist_list)


# In[24]:


album_df['release_date'] = pd.to_datetime(album_df['release_date'],errors='coerce')


# In[25]:


album_df=album_df.dropna()


# In[26]:


album_df.head(100)


# In[27]:


song_df.head()


# In[28]:


song_df['song_added']=pd.to_datetime(song_df['song_added'])


# In[29]:


song_df.info()


# In[30]:


artist_df


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




