


import tweepy
import pandas as pd
import time
import datetime
import sqlite3


access_token='Your access token here'
access_token_secret='Your access secret here'
consumer_key='Your Consumer Key here'
consumer_secret='Your consumer secret here'
auth = tweepy.OAuthHandler(consumer_key=consumer_key, consumer_secret=consumer_secret)
api = tweepy.API(auth)
auth.set_access_token(access_token,access_token_secret)
api = tweepy.API(auth)
conn = sqlite3.connect('third.db')
strdate=datetime.datetime.now().strftime("%Y-%m-%d")

#Run this commented section first separately to create the table
#import sqlite3
#conn = sqlite3.connect('third.db')
#c = conn.cursor()
#c.execute('CREATE TABLE IF NOT EXISTS TWEETS (QRY TEXT, TWEET_ID INTEGER, TWEET_TXT TEXT, TWEET_USR TEXT, URLLINK TEXT, TWEET_TIME TEXT,USR_LOC TEXT, RETWEET_CNT INTEGER)')




def process_results(results):
    
    #Creates list with tweet id as content of the list 
    id_list = [tweet.id for tweet in results]
    #Creates data frame based on the list with column id
    data_set = pd.DataFrame(id_list, columns=["id"])
    # Processing Tweet Data. Creates data_set dataframe Columns from the list object(results) passed as input to the function
    data_set["text"] = [tweet.text for tweet in results]
    data_set["created_at"] = [tweet.created_at for tweet in results]
    data_set["retweet_count"] = [tweet.retweet_count for tweet in results]
    data_set["favorite_count"] = [tweet.favorite_count for tweet in results]
    data_set["source"] = [tweet.source for tweet in results]
    # Processing User Data
    data_set["user_id"] = [tweet.author.id for tweet in results]
    data_set["user_screen_name"] = [tweet.author.screen_name for tweet in results]
    data_set["user_name"] = [tweet.author.name for tweet in results]
    data_set["user_created_at"] = [tweet.author.created_at for tweet in results]
    data_set["user_description"] = [tweet.author.description for tweet in results]
    data_set["user_followers_count"] = [tweet.author.followers_count for tweet in results]
    data_set["user_friends_count"] = [tweet.author.friends_count for tweet in results]
    data_set["user_location"] = [tweet.author.location for tweet in results]
    data_set["url"] = [tweet.entities['urls'] for tweet in results]
    return data_set

#query=input("Input the twitter search string : " )   
#numresults=int(input("Input number of seach results : " )  )
query="Your search string here"
numresults=50

while strdate == datetime.datetime.now().strftime("%Y-%m-%d") :
    c = conn.cursor()
    c.execute("select max(TWEET_ID) from TWEETS where (QRY=:px )",{"px": query})

    if (c.rowcount>0):
        for row in c.fetchall():
            sinceid=int(row[0]) 
    else:
        sinceid=771341418761588737
    results = []
    #for tweet insinceid= tweepy.Cursor(api.search, q="BigData",since_id=771341418761588737).items(2):
    #Creates a list object with tweets
    for tweet in tweepy.Cursor(api.search, q=query,since_id=sinceid).items(numresults):    
        results.append(tweet)
        
    data_set = process_results(results)    
    
    #print(type(data_set))
    #print(data_set)

    for index , row in data_set.iterrows():
        #print('tweet id is ' + str(data_set.iloc[index]['id']))
        #print (type(data_set.iloc[index]['id']))
        tweetid=int(data_set.iloc[index]['id'])
        #print('tweet text is ' + data_set.iloc[index]['text']) 
        tweettxt=str(data_set.iloc[index]['text'])
        #print('user name is ' + data_set.iloc[index]['user_screen_name'])
        uname=str(data_set.iloc[index]['user_screen_name'])
        #print('no of url preset'+ str(len(data_set.iloc[index]['url'])))
        urllink=''
        if len(data_set.iloc[index]['url'])>0:
            #print( 'url in the tweet is '+ data_set.iloc[index]['url'][0]['expanded_url'] )
            #print(type(data_set.iloc[index]['url'][0]['expanded_url']))
            urllink=str(data_set.iloc[index]['url'][0]['expanded_url'] )
        #print ('tweet was created at ' + str(data_set.iloc[index]['created_at']))    
        tweettime=str(data_set.iloc[index]['created_at'])
        #print('user location is ' + data_set.iloc[index]['user_location'])
        userloc=str(data_set.iloc[index]['user_location'])
        #print('retweet count is ' + str(data_set.iloc[index]['retweet_count']))
        retweetcount=int(data_set.iloc[index]['retweet_count'])
        #print('search results is for query string '+query)
        c.execute("INSERT INTO TWEETS (QRY,TWEET_ID,TWEET_TXT,TWEET_USR,URLLINK,TWEET_TIME,USR_LOC,RETWEET_CNT) VALUES (?,?,?,?,?,?,?,?) ",
             (query,tweetid,tweettxt,uname,urllink,tweettime,userloc,retweetcount))
        conn.commit()
    time.sleep(300)
    if strdate == datetime.datetime.now().strftime("%Y-%m-%d") :
        pass
    else:
        strdate=datetime.datetime.now().strftime("%Y-%m-%d")

c.close()
conn.close()
    
    
    
