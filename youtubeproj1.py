import googleapiclient.discovery
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st

#get youtube api key
def youtube_api_connection():
    api_key="AIzaSyBozI00sEZ1wtXcjhncVVFnc07oCbpasFM"
    api_service_name = "youtube"
    api_version = "v3"
    
    youtube = googleapiclient.discovery.build(
              api_service_name, api_version, developerKey="AIzaSyBozI00sEZ1wtXcjhncVVFnc07oCbpasFM")
    return youtube
youtube=youtube_api_connection()

#get channel information   

def get_channel_info(channel_id):
    
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
    )
    response=request.execute()
    
    for i in response['items']:
          
          data=dict(channel_Id=i["id"],
                  channel_name=i["snippet"]["title"],
                  subscribers=i["statistics"]["subscriberCount"],
                  total_videos=i["statistics"]["videoCount"],
                  views=i["statistics"]["viewCount"],
                  channel_description=i['snippet']['description'],
                  playlist_id=i["contentDetails"]["relatedPlaylists"]
                 )
    return(data)

#get video ids
def get_video_ids(channel_id):
    video_ids=[]
    
    response=youtube.channels().list(
             part="snippet,contentDetails,statistics",id=channel_id).execute()
    playlists=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    page_token=None
    while True:
        playlist_response=youtube.playlistItems().list(
            part="snippet",
                playlistId=playlists,maxResults=50,pageToken=page_token).execute()
        for i in range ((len(playlist_response['items']))):
            video_ids.append(playlist_response['items'][i]['snippet']['resourceId']['videoId'])
        page_token=playlist_response.get('nextPageToken')
        if page_token is None:
           break
    videos_id=[]
    for i in range(0,100):
        videos_id.append(video_ids[i])
    return videos_id

#get video information

def get_video_info(video_details):
    video_list=[]
    for video_id in video_details:
        #print(video_id)
        request=youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id)
        response=request.execute()
        for i in response['items']:
            data=dict(channel_name=i['snippet']['channelTitle'],
                     channel_id=i['snippet']['channelId'],
                     video_id=i['id'],
                     video_title=i['snippet']['title'],
                     thumnail=i['snippet']['thumbnails']['default']['url'],
                     published_date=i['snippet']['publishedAt'],
                     duration=i['contentDetails']['duration'],
                     commentcount=i['statistics'].get('commentCount'),
                     views=i["statistics"]["viewCount"],
                     likes=i["statistics"]["likeCount"],
                     definition=i["contentDetails"]["definition"],
                     caption=i["contentDetails"]["caption"])
            video_list.append(data)
    return video_list

#get comment information


def get_comment_info(video_details):
    comment_list=[]        
    try:
        for video_id in video_details:
           request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_id,
                maxResults=50
            )
           response = request.execute()
           for i in response['items']:
               data=dict(comment_id=i['snippet']['topLevelComment']['id'],
                         video_id=i['snippet']['topLevelComment']['snippet']['videoId'],
                         comment_text=i['snippet']['topLevelComment']['snippet'][ 'textDisplay'],
                         comment_author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                         published_date=i['snippet']['topLevelComment']['snippet']['publishedAt'])
               comment_list.append(data)
    except:
        pass
    return comment_list

# pymongo connection

from pymongo import MongoClient
connection=MongoClient("mongodb+srv://subhasinikrish:12345@cluster0.thtg8ir.mongodb.net/")
db=connection["youtubeproject"]
col=db["channel_details"]

#get all details of a channel to insert into mongodb

def channel_details(channel_id):
    ch_info=get_channel_info(channel_id)
    vd_ids=get_video_ids(channel_id)
    vd_info=get_video_info(vd_ids)
    com_info=get_comment_info(vd_ids)
    col=db["channel_details"]
    col.insert_one({"channel_details":ch_info,"video_ids":vd_ids,"video_info":vd_info,"comment_details":com_info})
    return "true"

#get channel table 

def channel_table():

    connection=mysql.connector.connect(host="localhost",user="root",password="12345",database="youtubeproject")
    mycursor=connection.cursor()
    drop_query="""drop table if exists channeltable"""
    mycursor.execute(drop_query)
    connection.commit()


    query="""create table if not exists channeltable(channel_Id varchar(100) primary key,channel_name varchar(50),subscribers bigint,total_videos bigint,views bigint,channel_description text,
             playlist_id varchar(80))"""
    mycursor.execute(query)
    connection.commit()
   
    connection=MongoClient("mongodb+srv://subhasinikrish:12345@cluster0.thtg8ir.mongodb.net/")
    
    db=connection["youtubeproject"]
    col=db["channel_details"]
        

    channel_list=[]
    db=connection["youtubeproject"]
    col=db["channel_details"]
    for i in col.find({},{'_id':0,'channel_details':1}):
        channel_list.append (i['channel_details'])
    df=pd.DataFrame(channel_list)

    rows=[]
    for index in df.index:
        row=tuple(df.loc[index].values)
        row=tuple([str(d) for d in row])
        rows.append(row)

    connection=mysql.connector.connect(host="localhost",user="root",password="12345",database="youtubeproject")
    mycursor=connection.cursor()

    insert_query="insert into channeltable values(%s,%s,%s,%s,%s,%s,%s)"
    mycursor.executemany(insert_query,rows)
    connection.commit()
    return

#get video table
def video_table():

    connection=mysql.connector.connect(host="localhost",user="root",password="12345",database="youtubeproject")
    mycursor=connection.cursor()

    drop_query="""drop table if exists videotable"""
    mycursor.execute(drop_query)
    connection.commit()

    query="""create table if not exists videotable(channel_name varchar(100),
                     channel_id varchar(100),
                     video_id varchar(100) primary key,
                     video_title varchar(100),
                     thumnail varchar(500),
                     published_date varchar(50),
                     duration  varchar(50) ,
                     commentcount varchar(50),
                     views bigint,
                     likes bigint,
                     definition varchar(10),
                     caption varchar(100))"""
    mycursor.execute(query)
    connection.commit()
   
    connection=MongoClient("mongodb+srv://subhasinikrish:12345@cluster0.thtg8ir.mongodb.net/")

    db=connection["youtubeproject"]
    col=db["channel_details"]
        


    vd_details=[]
    
    for i in col.find({},{'_id':0,'video_info':1}):
        for j in range (len(i['video_info'])):
            vd_details.append (i['video_info'][j])
    df1=pd.DataFrame(vd_details)


    rows=[]
    for index in df1.index:
        row=tuple(df1.loc[index].values)
        row=tuple([str(d) for d in row])
        rows.append(row)
  
    connection=mysql.connector.connect(host="localhost",user="root",password="12345",database="youtubeproject")
    mycursor=connection.cursor()


    query="insert into videotable values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    mycursor.executemany(query,rows)
    connection.commit()
    return

#get comment table

def comment_table():
    connection=mysql.connector.connect(host="localhost",user="root",password="12345",database="youtubeproject")
    mycursor=connection.cursor()

    drop_query="""drop table if exists commenttable"""
    mycursor.execute(drop_query)
    connection.commit()

    query="""create table if not exists commenttable(comment_id varchar(100),
                        video_id varchar(100),
                        comment_text text,
                        comment_author varchar(100),
                        published_date varchar(100)
                        )"""
    mycursor.execute(query)
    connection.commit()
    
    connection=MongoClient("mongodb+srv://subhasinikrish:12345@cluster0.thtg8ir.mongodb.net/")

    db=connection["youtubeproject"]
    col=db["channel_details"]


    comment_list=[]
    db=connection["youtubeproject"]
    col=db["channel_details"]
    for i in col.find({},{'_id':0,'comment_details':1}):
        for j in range (len(i['comment_details'])):
            comment_list.append (i['comment_details'][j])
    df2=pd.DataFrame(comment_list)
    


    rows=[]
    for index in df2.index:
        row=tuple(df2.loc[index].values)
        row=tuple([str(d) for d in row])
        rows.append(row)
    rows
    connection=mysql.connector.connect(host="localhost",user="root",password="12345",database="youtubeproject")
    mycursor=connection.cursor()


    insert_query="insert into commenttable values(%s,%s,%s,%s,%s)"
    mycursor.executemany(insert_query,rows)
    connection.commit()
    return

 #store datas to mysql through this table fn   
def table():
    channel_table()
    video_table()        
    comment_table()
    return
  
#view channeltable in streamlit
def view_ch_table():
    channel_list=[]
    db=connection["youtubeproject"]
    col=db["channel_details"]
    for i in col.find({},{'_id':0,'channel_details':1}):
        channel_list.append(i['channel_details'])
    df=st.dataframe(channel_list)
    return df

#view video table in streamlit

def view_vd_table():
    vd_details=[]
    channel_list=[]
    db=connection["youtubeproject"]
    col=db["channel_details"]
    for i in col.find({},{'_id':0,'video_info':1}):
        for j in range (len(i['video_info'])):
            vd_details.append (i['video_info'][j])
    df1=st.dataframe(vd_details)
    return df1

#view commenttable in streamlit

def view_comment_table():   

    comment_list=[]
    db=connection["youtubeproject"]
    col=db["channel_details"]

    for i in col.find({},{'_id':0,'comment_details':1}):
        for j in range (len(i['comment_details'])):
            comment_list.append (i['comment_details'][j])
    df2=st.dataframe(comment_list)
    return df2

#streamlit code

st.title("YOUTUBE DATA HARVESTING AND WAREHOUSING USING SQL,MONGO DB AND STREAMLIT")
with st.sidebar:
    st.title(":red[PROJECT DETAILS AND SKILLS DEVELOPED]")
    st.header(":blue[STEPS FOLLOWED]")
    st.caption("API INTEGRATION")
    st.caption("CHANNEL DETAILS EXTRACTION")
    st.caption("FETCHING VIDEO IDS")
    st.caption("FETCHING VIDEO DETAILS")
    st.caption("COMMENT DETAILS EXTRACTION")
    st.caption("INSERT CHANNEL DETAILS INTO MONGO DB")
    st.caption("TRANSFERING DATAS INTO MYSQL")
    st.header(":blue[SKILLS DEVELOPED]")
    st.caption("PYTHON SCRIPTING")
    st.caption("DATA COLLECTION")
    st.caption("DATA MANAGEMENT USING MONGODB ATLAS AND SQL")
    st.caption ("MONGODB,SQL AND STREAMLIT")
    st.header(":blue[DOMAIN]")
    st.caption("SOCIAL MEDIA")


channelid=st.text_input("ENTER THE CHANNEL ID") 
st.button("enter")
    
if st.button("CHECK AND INSERT CHANNEL DATAS INTO MONGODB"):
    chan_id=[]
    db=connection["youtubeproject"]
    col=db["channel_details"]
    for i in col.find({},{'_id':0,'channel_details':1}):
        chan_id.append(i['channel_details']['channel_Id'])
    if channelid in chan_id:
        st.success("The given channel id details already existed")
    else:
        insert=channel_details(channelid)
        st.success(insert)
if st.button("TRANSFER DATA FROM MONGODB TO MYSQL"):
    result=table()
    st.success(result)

view_table=st.radio("VIEW THE SELECTED TABLE",("CHANNELTABLE","VIDEOTABLE","COMMENTTABLE"))

if view_table=="CHANNELTABLE":
    view_ch_table()

elif view_table=="VIDEOTABLE":
    view_vd_table()

elif view_table=="COMMENTTABLE":
    view_comment_table()

#mysql query questions

connection=mysql.connector.connect(host="localhost",user="root",password="12345",database="youtubeproject")
mycursor=connection.cursor()

questions=st.selectbox("choose your question",("1.What are the names of all the videos and their corresponding channels?",
                       "2. Which channels have the most number of videos, and how many videos do they have?",
                       "3.What are the top 10 most viewed videos and their respective channels?",
                       "4. How many comments were made on each video, and what are their corresponding video names?",
                       "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                       "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                       "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                       "8.What are the names of all the channels that have published videos in the year 2022?",
                       "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                       "10.Which videos have the highest number of comments, and what are their corresponding channel names?")) 

if questions=="1.What are the names of all the videos and their corresponding channels?":
    connection=mysql.connector.connect(host="localhost",user="root",password="12345",database="youtubeproject")
    mycursor=connection.cursor()
    query1='''select video_title ,channel_name  from videotable'''
    mycursor.execute(query1)
    q1=mycursor.fetchall()
    df4=pd.DataFrame(q1,columns=["videotitle","channelname"])
    st.write(df4)

elif questions=="2. Which channels have the most number of videos, and how many videos do they have?":
    connection=mysql.connector.connect(host="localhost",user="root",password="12345",database="youtubeproject")
    mycursor=connection.cursor()
    query2='''select channel_name,total_videos from channeltable order by total_videos desc '''
    mycursor.execute(query2)
    q2=mycursor.fetchall()
    df4=pd.DataFrame(q2,columns=["channel_name","total_videos"])
    st.write(df4)


elif questions=="3.What are the top 10 most viewed videos and their respective channels?":
    connection=mysql.connector.connect(host="localhost",user="root",password="12345",database="youtubeproject")
    mycursor=connection.cursor()
    query3='''select channel_name,video_title,views from videotable order by views desc limit 10'''
    mycursor.execute(query3)
    q3=mycursor.fetchall()
    df4=pd.DataFrame(q3,columns=["channel_name","video_title","views"])
    st.write(df4)

elif questions=="4. How many comments were made on each video, and what are their corresponding video names?":
    connection=mysql.connector.connect(host="localhost",user="root",password="12345",database="youtubeproject")
    mycursor=connection.cursor()
    query4='''select video_title,commentcount from videotable '''
    mycursor.execute(query4)
    q4=mycursor.fetchall()
    df4=pd.DataFrame(q4,columns=["video_title","commentcount"])
    st.write(df4)

elif questions=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    connection=mysql.connector.connect(host="localhost",user="root",password="12345",database="youtubeproject")
    mycursor=connection.cursor()
    query='''select channel_name,video_id,likes from videotable order by likes desc '''
    mycursor.execute(query)
    q5=mycursor.fetchall()
    df5=pd.DataFrame(q5,columns=["channel_name","video_id","likes"])
    st.write(df5)

elif questions=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    connection=mysql.connector.connect(host="localhost",user="root",password="12345",database="youtubeproject")
    mycursor=connection.cursor()
    query='''select sum(likes),video_title from videotable group by video_id order by sum(likes) desc '''
    mycursor.execute(query)
    q6=mycursor.fetchall()
    df6=pd.DataFrame(q6,columns=["sum(likes)","video_title"])
    st.write(df6)

elif questions=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
    connection=mysql.connector.connect(host="localhost",user="root",password="12345",database="youtubeproject")
    mycursor=connection.cursor()
    query='''select channel_name,sum(views) from videotable group by channel_name'''
    mycursor.execute(query)
    q7=mycursor.fetchall()
    df7=pd.DataFrame(q7,columns=["channel_name","sum(views)"])
    st.write(df7)

elif questions=="8.What are the names of all the channels that have published videos in the year 2022?":
    connection=mysql.connector.connect(host="localhost",user="root",password="12345",database="youtubeproject")
    mycursor=connection.cursor()
    query='''select channel_name,published_date from videotable where published_date=2022'''
    mycursor.execute(query)
    q8=mycursor.fetchall()
    df8=pd.DataFrame(q8,columns=["channel_name","published_date"])
    st.write(df8)

elif questions=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    connection=mysql.connector.connect(host="localhost",user="root",password="12345",database="youtubeproject")
    mycursor=connection.cursor()
    query='''select channel_name,avg(duration) from videotable group by channel_name'''
    mycursor.execute(query)
    q9=mycursor.fetchall()
    df9=pd.DataFrame(q9,columns=["channel_name","avg(duration)"])
    st.write(df9)

elif questions=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    connection=mysql.connector.connect(host="localhost",user="root",password="12345",database="youtubeproject")
    mycursor=connection.cursor()
    query='''select commentcount,channel_name,video_title from videotable order by commentcount desc'''
    mycursor.execute(query)
    q10=mycursor.fetchall()
    df10=pd.DataFrame(q10,columns=["commentcount","channel_name","video_title"])
    st.write(df10)





