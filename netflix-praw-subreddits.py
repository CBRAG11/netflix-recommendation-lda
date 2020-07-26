#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 10 08:48:15 2020

@author: brag
"""

import praw
import datetime
import pymysql
import pymysql.cursors
reddit = praw.Reddit(user_agent='asu_project)',
                     client_id='4LK3N_k_aPbHiw', client_secret="-7GIGvyrGs8C2D-ydZKn0cIiLHU",
                     username='', password='')                
subredditList = ['netflix', 'bestofnetflix', 'amazonprime', 'BestOfAmazonPrime', 'hulu', 'bestofhulu', 'DisneyPlus'] 
subredditTable = 'subreddit' 
count = 1

for subredditName in subredditList:
    try:
        for comment in reddit.subreddit(subredditName).comments():
            commentID = str(comment.id).encode('utf8')
            author = str(comment.author).encode('utf8')
            timestamp = str(datetime.datetime.fromtimestamp(comment.created)).encode('utf8')
            replyTo = ""
            if not comment.is_root:
                replyTo = str(comment.parent().id).encode('utf8')
            else:
                 replyTo = "-"
            threadID = str(comment.submission.id).encode('utf8')
            threadTitle = str(comment.submission.title).encode('utf8')
            msgBody = str(comment.body).encode('utf8')
            permalink = str(comment.permalink).encode('utf8')
            print("-------------------------------------------------------")
            print("Comment ID: " + str(comment.id))
            print("Comment Author: "+ str(comment.author))
            print("Timestamp: "+str(datetime.datetime.fromtimestamp(comment.created)))
            if not comment.is_root:
                print("Comment is a reply to: " + str(comment.parent().id))
            else:
                print("Comment is a reply to: -")
                print("Comment Thread ID: " + str(comment.submission.id))
                print("Comment Thread Title: " + str(comment.submission.title))
        #        print("Comment Body: " + str(comment.body))
                print("Comment Permalink: " + str(comment.permalink))
            db = pymysql.connect(host="localhost", user="", passwd="", 
            db="cis591P", charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
            cur = db.cursor()
            sqlStatement = "INSERT INTO " + subredditTable + " (MsgID, SubredditName, Timestamp, Author, ThreadID, ThreadTitle, MsgBody, ReplyTo, Permalink) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            inputData = (commentID, subredditName, timestamp, author, threadID, threadTitle, msgBody, replyTo, permalink)
            cur.execute(sqlStatement, inputData )
            db.commit()
            db.close()
            print("Total messages collected from /r/"+subredditName+": " + str(count))
            count += 1
    except:
        pass
        