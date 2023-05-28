import streamlit as st
import googleapiclient.discovery
from googleapiclient.errors import HttpError
import pandas as pd
from pymongo import MongoClient
import pandas as pd
import mysql.connector
from datetime import datetime

# Set up YouTube Data API client
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey="AIzaSyCwy4jiru9cMyQaBl84lY2FRkYiJ49wrcg")

client = MongoClient("mongodb://localhost:27017")
mydb = client["demo1"]
mycol = mydb["channel_data6"]

# Connect to MySQL
mydb_sql = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Mouli@1719",
    database="fli"
)
cursor = mydb_sql.cursor()

def table_exists(table_name):
    cursor.execute("SHOW TABLES LIKE %s", (table_name,))
    return cursor.fetchone() is not None

# Create table if it doesn't exist
if not table_exists('sql_channel_data3'):
    cursor.execute("CREATE TABLE sql_channel_data3 (channel_name VARCHAR(255), channel_id VARCHAR(255), started_date DATETIME, subscribers INT, views INT, total_video INT)")

if not table_exists('sql_video_data3'):
     cursor.execute("create table sql_video_data3(channel_name VARCHAR(255),channel_id VARCHAR(255),title VARCHAR(255),published_date DATETIME,views INT,likes INT,comments INT)")

if not table_exists('sql_playlist3'):
     cursor.execute("create table sql_playlist3(channel_name VARCHAR(255),channel_id VARCHAR(255),playlist_id VARCHAR(255))")


if not table_exists('sql_comments_data3'):
     cursor.execute("create table sql_comments_data3(channel_name VARCHAR(255),channel_id VARCHAR(255),video_id VARCHAR(255),comment VARCHAR(10000),author VARCHAR(255),publishedAt DATETIME)")



def convert_to_mysql_datetime(date_str):
    datetime_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
    return datetime_obj.strftime("%Y-%m-%d %H:%M:%S")


# Retrieve channel data using YouTube Data API
def get_channel_data(channel_id):
    request = youtube.channels().list(part="snippet,statistics,contentDetails", id=channel_id)
    response = request.execute()

    channel_data = {
        "channel_id":response['items'][0]['id'],
        "channel_name": response['items'][0]['snippet']['title'],
        "started_date": convert_to_mysql_datetime(response['items'][0]['snippet']['publishedAt']),
        "subscribers": response['items'][0]['statistics']['subscriberCount'],
        "views": response['items'][0]['statistics']['viewCount'],
        "total_video": response['items'][0]['statistics']['videoCount'],
        "playlist_id": response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    }
    return channel_data


# get channel video_ids
def get_video_ids(playlist_id):
    request = youtube.playlistItems().list(part='contentDetails', playlistId=playlist_id, maxResults=50)
    response = request.execute()

    video_ids = []
    for item in response['items']:
        video_ids.append(item['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')
    more_pages = True

    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(part='contentDetails', playlistId=playlist_id, maxResults=50,
                                                   pageToken=next_page_token)
            response = request.execute()
            for item in response['items']:
                video_ids.append(item['contentDetails']['videoId'])
            next_page_token = response.get('nextPageToken')
    return video_ids


# Get video details
def get_video_details(video_ids):
    all_video_stats = []

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part='snippet,statistics',
            id=','.join(video_ids[i:i + 50]))
        response = request.execute()

        for video in response['items']:
            video_stats = {
                "title": video['snippet']['title'],
                "published_date": convert_to_mysql_datetime(video['snippet']['publishedAt']),
                "views": video['statistics']['viewCount'],
                "likes": video['statistics'].get('likeCount', 0),
                "comments": video['statistics'].get('commentCount', 0)
            }
            all_video_stats.append(video_stats)

    return all_video_stats

#get comments Data
def get_comments(video_ids):
    all_comments = []

    for video in video_ids:
        request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video)
        response = request.execute()
        for comment in response['items']:
            comment_stats = {'video_id': comment['snippet']['videoId'],
                             'comment': comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                             'author': comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                             'publishedAt': convert_to_mysql_datetime(comment['snippet']['topLevelComment']['snippet']['publishedAt'])}
            all_comments.append(comment_stats)
    return all_comments





# Display channel and video details
def display_channel_data(channel_data, video_data,playlist_id,comments_data):
    st.subheader("Channel Details")
    st.write('Channel Id:', channel_data['channel_id'])
    st.write('Channel Name:', channel_data['channel_name'])
    st.write('DOS:', channel_data['started_date'])
    st.write('Subscribers:', channel_data['subscribers'])
    st.write('Views:', channel_data['views'])
    st.write('Total Videos:', channel_data['total_video'])
    st.write('Playlist ID:', channel_data['playlist_id'])

    st.subheader("Video Details")
    st.write(pd.DataFrame(video_data))

    st.subheader("Comments Details")
    st.write(pd.DataFrame(comments_data))


    # Store data in MongoDB

    data = {
        "_id":channel_data['channel_name'],
        "channel_data": channel_data,
        "video_stats": video_data,
        "comments_data":comments_data

    }
    mycol.insert_one(data)


def store_in_sql(selected_document):
    # Store document data in MySQL table
    channel_query = "INSERT INTO sql_channel_data3 (channel_name,channel_id,started_date, subscribers, views, total_video) VALUES ( %s, %s, %s, %s, %s, %s)"
    channel_values = (
        selected_document['channel_data']['channel_name'],
        selected_document['channel_data']['channel_id'],
        selected_document['channel_data']['started_date'],
        selected_document['channel_data']['subscribers'],
        selected_document['channel_data']['views'],
        selected_document['channel_data']['total_video'],

    )
    cursor.execute(channel_query, channel_values)

    # Store video data
    video_query = "INSERT INTO sql_video_data3 (channel_name,channel_id, title, published_date, views, likes, comments) VALUES (%s, %s, %s, %s, %s, %s ,%s)"
    video_values = [(selected_document['channel_data']['channel_name'], selected_document['channel_data']['channel_id'], video['title'], video['published_date'],video['views'], video['likes'], video['comments']) for video in selected_document['video_stats']]
    cursor.executemany(video_query, video_values)

    # Store playlist Details
    playlist_query = "INSERT INTO sql_playlist3(channel_name,channel_id,playlist_id) VALUES (%s ,%s ,%s)"
    playlist_values = (selected_document['channel_data']['channel_name'], selected_document['channel_data']['channel_id'], selected_document['channel_data']['playlist_id'])
    cursor.execute(playlist_query, playlist_values)

    # store comment Details
    comments_query = "INSERT INTO sql_comments_data3(channel_name,channel_id,video_id,comment,author,publishedAt) VALUES (%s ,%s ,%s ,%s ,%s ,%s)"
    comments_values = [(selected_document['channel_data']['channel_name'], selected_document['channel_data']['channel_id'], comment['video_id'], comment['comment'], comment['author'], comment['publishedAt']) for comment in selected_document['comments_data']]
    cursor.executemany(comments_query, comments_values)

    # Commit the changes to MySQL
    mydb_sql.commit()

    # Remove the uploaded document ID from the dropdown

    st.success("Document uploaded to MySQL!")





#main program
st.title("YouTube Data Scraping")

channel_ids = st.text_input("Enter YouTube Channel ID")
count = 0
if st.button("Get Channel Details"):
    for channel_id in channel_ids.strip().split(','):
        try:
            count += 1
            st.title(count)
            channel_data = get_channel_data(channel_id)
            playlist_id = channel_data['playlist_id']
            video_ids = get_video_ids(playlist_id)
            video_data = get_video_details(video_ids)
            comments_data=get_comments(video_ids)
            display_channel_data(channel_data, video_data,playlist_id,comments_data)
            st.write(
                "/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////"
                "////////////////////////////////////////////////////////////"
                "/////////////////////////////////////////////////////////////"
                "//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////")
        except HttpError as e:
            st.error(f"An error occurred: {e}")





# Get document IDs from MongoDB collection
document_ids = [str(doc["_id"]) for doc in mycol.find()]

# Display dropdowns to select MongoDB document and MySQL table
selected_document_id = st.selectbox("Select MongoDB Document", document_ids)
st.write((document_ids))

# Retrieve selected document from MongoDB
selected_document = mycol.find_one({"_id": selected_document_id})


# Store selected document in MySQL
if st.button("Upload Document"):
    store_in_sql(selected_document)

    #join operation
    st.subheader("Join Operations")
    join_query = """
    SELECT cd.channel_name, cd.channel_id,cd.started_date,cd.subscribers, cd.views, cd.total_video,vd.title, vd.published_date, vd.views, vd.likes, vd.comments,pl.playlist_id
    FROM sql_channel_data3 AS cd
    JOIN sql_video_data3 AS vd ON cd.channel_id = vd.channel_id
    JOIN sql_playlist3 AS pl ON cd.channel_id = pl.channel_id
    """
    cursor.execute(join_query)
    result = cursor.fetchall()

    st.write(pd.DataFrame(result,columns=['name', 'id', 'Stared_date', 'subscribers', 'total_views', 'total_video', 'title', 'video_date','views', 'likes', 'comments_count', 'playlist_id']))

    #query1 -  What are the names of all the videos and their corresponding channels?
    st.subheader("#query1 -  What are the names of all the videos and their corresponding channels?")
    cursor.execute("select sql_channel_data3.channel_name,sql_video_data3.title from sql_channel_data3 join sql_video_data3 ON sql_channel_data3.channel_id=sql_video_data3.channel_id")
    result1 = cursor.fetchall()
    st.write(pd.DataFrame(result1, columns=['Video Title', 'Channel Name']))

    #query2 - Which channels have the most number of videos, and how many videos do they have?
    st.subheader("query2 - Which channels have the most number of videos, and how many videos do they have?")
    query2 = "SELECT cd.channel_name, COUNT(vd.title) AS video_count FROM sql_channel_data3 AS cd JOIN sql_video_data3 AS vd ON cd.channel_id = vd.channel_id  GROUP BY cd.channel_name ORDER BY video_count DESC"
    cursor.execute(query2)
    result2 = cursor.fetchall()
    st.write(pd.DataFrame(result2, columns=['Channel Name', 'Video Count']))

    #query3 - What are the top 10 most viewed videos and their respective channels?
    st.subheader("query3 - What are the top 10 most viewed videos and their respective channels?")
    query3 = "SELECT vd.channel_name,vd.views from sql_video_data3 AS vd ORDER BY vd.views DESC LIMIT 10"
    cursor.execute(query3)
    result3 = cursor.fetchall()
    st.write(pd.DataFrame(result3, columns=['Channel_name', 'views']))

    #query4 - How many comments were made on each video, and what are their corresponding video names?
    st.subheader("query4 - How many comments were made on each video, and what are their corresponding video names?")
    query4 = "SELECT vd.channel_name,vd.title,vd.comments from sql_video_data3 AS vd"
    cursor.execute(query4)
    result4 = cursor.fetchall()
    st.write(pd.DataFrame(result4, columns=['channel_name', 'video_title', 'comments']))

    #query5 - Which videos have the highest number of likes, and what are their corresponding channel names?
    st.subheader("query5 - Which videos have the highest number of likes, and what are their corresponding channel names?")
    query5 = "SELECT vd.channel_name,vd.title,vd.likes from sql_video_data3 AS vd ORDER BY vd.likes DESC LIMIT 1"
    cursor.execute(query5)
    result5 = cursor.fetchall()
    st.write(pd.DataFrame(result5, columns=['channel_name', 'video_title', 'likes']))

    #query6 - What is the total number of likes and dislikes for each video, and what are their corresponding video names?
    st.subheader("query6 - What is the total number of likes and comments for each video, and what are their corresponding video names?")
    query6 = "SELECT vd.channel_name,vd.title,vd.likes,vd.comments from sql_video_data3 AS vd"
    cursor.execute(query6)
    result6 = cursor.fetchall()
    st.write(pd.DataFrame(result6, columns=['channel_name', 'video_title', 'likes', 'comments']))

    #query7 - What is the total number of views for each channel, and what are their corresponding channel names?
    st.subheader("query7 - What is the total number of views for each channel, and what are their corresponding channel names?")
    query7 = "SELECT cd.channel_name,cd.views from sql_channel_data3 AS cd"
    cursor.execute(query7)
    result7 = cursor.fetchall()
    st.write(pd.DataFrame(result7, columns=['channel_name', 'views']))

    #query8 - What are the names of all the channels that have published videos in the year 2022?
    st.subheader("query8 - What are the names of all the channels that have published videos in the year 2022?")
    year = 2022
    query8 = "SELECT vd.channel_name, vd.title, vd.published_date FROM sql_video_data3 AS vd WHERE YEAR(vd.published_date) = %s"
    cursor.execute(query8, (year,))
    result8 = cursor.fetchall()
    st.write(pd.DataFrame(result8, columns=['channel_name', 'video_title', 'published_date']))

    #query9 - What are the top 10 videos got likes and their respective channels?
    st.subheader("query9 - What are the top 10 videos got likes and their respective channels?")
    query9 = "SELECT vd.channel_name,vd.title,vd.likes from sql_video_data3 AS vd ORDER BY vd.likes DESC "
    cursor.execute(query9)
    result9 = cursor.fetchall()
    st.write(pd.DataFrame(result9, columns=['channel_name', 'video_title', 'likes']))

    #query10 - Which videos have the highest number of comments, and what are their corresponding channel names?
    st.subheader("query10 - Which videos have the highest number of comments, and what are their corresponding channel names?")
    query10 = "SELECT vd.channel_name,vd.title,vd.comments from sql_video_data3 AS vd ORDER BY vd.likes DESC LIMIT 1"
    cursor.execute(query10)
    result10 = cursor.fetchall()
    st.write(pd.DataFrame(result10, columns=['channel_name', 'video_title', 'comments_count']))












