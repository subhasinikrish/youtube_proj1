# youtube_proj1
This repository is to store Youtube dataharvesting and warehousing project codes and documents
What is this Project all about?
    This project is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels.
Requirements of this project:
     Ability to input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using Google API.
     Option to store the data in a MongoDB database as a data lake.
     Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button.
     Option to select a channel name and migrate its data from the data lake to a SQL database as tables.
     Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.
Steps followed:
     1.Created an api key to do an api integration to access youtube channel details
     2.With the reference of youtube developer data got to know how to fetch channel id,video id,video inforamtion of all channels and comments information of each video ids.
     3.First import googleapiclient discovery in visual studio code notebook and wrote the codes for api integration and fetching channel id,video id details with the help of channel id
     4.With the help of video id details got the information of comments details using python codes
     5.Then import mongodb and create a connection link and then use that in visual studio notebook and did a program to fetch all the channel,video and comment information and insert them into mongodb atlas in my database
     6.Then create codes to migrate datas from mongodb to mysql using mysql connector to create separate tables for channel,video and comment details
     7.Then create an python file in virtual environment to store all the codes safely and inserted 10 channel id details into the mongodb and mysql
     8.Create streatmlit codes to save all the details in a webpage for easily access and analyse all the informations
     9.Finally added 10 queries in mysql and got the result and wrote codes regarding that in python and stored it in the webpage.
Skills developed:
     1. Python scripting
     2. Api integration
     3. Usage of mongodba and mysql
     4. Creating webpage in streamlit
     5. Using Github Repository
     This project will enable users to search for and analyse details of multiple youtube channels with the user friendly interface using python codes,mongodb,mysql and streamlit.
     
     


