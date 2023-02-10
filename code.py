import snscrape.modules.twitter as st
import pandas as pd
import datetime 
from time import sleep
from pymongo import MongoClient
import streamlit as sm
import json 

#connection to mongodb
client=MongoClient("mongodb://localhost:27017")

#streamlit webpage code
sm.title("TWITTER SCRAPPER \n WELCOME")

User = sm.text_input("Enter Username or Hashtag to search")
date_since = sm.text_input("Enter date since (YYYY-MM-DD)")
date_until = sm.text_input("Enter date until (YYYY-MM-DD)")
limit = sm.number_input("Enter limit of tweets to scrape")

#code to scrape data on streamlit
if sm.button("Scrape Data and Display"):
    tweets_data=[]
    for i, tweets in enumerate(st.TwitterSearchScraper('{}'.format(User)).get_items()):
        if i>limit:
            break
        tweets_data.append([tweets.date, tweets.content, tweets.user.username, tweets.url, tweets.replyCount, tweets.retweetCount, tweets.source, tweets.lang,tweets.likeCount,tweets.id])
        df=pd.DataFrame(tweets_data, columns=['Date', 'Content', 'Username', 'URL', 'Replycount', 'Retweetcount', 'Source', 'Language', 'Likecount', 'Id'])
    sm.write("DATA SCRAPED SUCCESSFULLY")
    sm.dataframe(df)

#code to download the scraped data in the json format
if sm.button("Download Data in Json"):
    tweets_data=[]
    for i, tweets in enumerate(st.TwitterSearchScraper('{}'.format(User)).get_items()):
        if i>limit:
            break
        tweets_data.append([tweets.date, tweets.content, tweets.user.username, tweets.url, tweets.replyCount, tweets.retweetCount, tweets.source, tweets.lang,tweets.likeCount,tweets.id])
    df=pd.DataFrame(tweets_data, columns=['Date', 'Content', 'Username', 'URL', 'Replycount', 'Retweetcount', 'Source', 'Language', 'Likecount', 'Id'])
    with open(f"{User}.json", "w") as f:
        f.write(df.to_json(orient="records"))
    sm.write("DATA DOWNLOADED IN JSON")

#code to download the scraped data in the csv format
if sm.button("Download Data in CSV"):
    tweets_data=[]
    for i, tweets in enumerate(st.TwitterSearchScraper('{}'.format(User)).get_items()):
        if i>limit:
            break
        tweets_data.append([tweets.date, tweets.content, tweets.user.username, tweets.url, tweets.replyCount, tweets.retweetCount, tweets.source, tweets.lang,tweets.likeCount,tweets.id])
    df=pd.DataFrame(tweets_data, columns=['Date', 'Content', 'Username', 'URL', 'Replycount', 'Retweetcount', 'Source', 'Language', 'Likecount', 'Id'])
    df.to_csv(f'{User}.csv', index=False, encoding='utf-8')
    sm.write("DATA DOWNLOADED IN CSV")

#code to upload the scraped data in the mongodb
if sm.button("Upload data in Mongodb"):
    tweets_data=[]
    for i, tweets in enumerate(st.TwitterSearchScraper('{}'.format(User)).get_items()):
        if i>limit:
            break
        tweets_data.append([tweets.date, tweets.content, tweets.user.username, tweets.url, tweets.replyCount, tweets.retweetCount, tweets.source, tweets.lang,tweets.likeCount,tweets.id])
    df=pd.DataFrame(tweets_data, columns=['Date', 'Content', 'Username', 'URL', 'Replycount', 'Retweetcount', 'Source', 'Language', 'Likecount', 'Id'])
    data=df.to_dict(orient="records")
    db=client["TwitterDatabase"]
    db.tweets_collection.insert_many(data)
    sm.write("DATA UPLOADED IN MONGODB")
