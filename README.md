# PaytmLabs
To Run code
```sbt clean package
   spark-submit --master spark:/<host>:7077 target/scala-2.11/paytmlabs_2.11-0.1.jar 
```

## 1. Sessionize
        
        I have used window function lag() to construct sessions.
        
        Idea here is to group all events of user and order them by timestamp, then use lag() function to find out time
        difference between consecutive events. If this time difference is greater than some threshold, 
        it is marked as session boundary (first event of next session)
        
        Ref : https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html 
    
  ##  2. Handle Unique Users 
         
        Ignore client port info when creating user session. 
        Connection from same IP with different ports are interleaving in time, which could be result of multiple browser
        tabs during same session.
         
        IP addresses do not guarantee distinct users
        
        One way to deal with this is to concatenate 'user-agent' with ip and use this as key to uniquely determine user.
        I used sparks default md5 function to generate unique id for every ip, user_agent combination.
        This approach still doesn't guarantee uniqueness of users.
        
        I have analyzed dataset based on both ip and id. 
        
   ## 3. URL
        
        Extract url from HTTP request. 
    
   ## 4. Invalid records
        
        Filter records which are not processed by backend (where backend = '-' and *_processing_time = -1) 
        We could also filter events with 4XX and 5XX error codes if we wish to not consider those request as part of session. 
