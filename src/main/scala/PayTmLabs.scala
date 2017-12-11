/*
  PaytmLabs WeblogChallenge for Data Engineer position
  This code can be run as interactive Apache Zeppelin notebook on

 http://138.68.27.169:8080/#/notebook/2D3M35VQG

   username : admin
   password: password1
   click on 'paytmlabs' in notebook section

  For any questions email at: neelt@sfu.ca

*/

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window

object PayTmLabs {

  def saveAsCSV(df: DataFrame, name: String) : Unit = {
      df.write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .save("results/" + name)
  }

  def main(args: Array[String]) {

      val spark = SparkSession
                  .builder
                  .appName("PayTmLabs")
                  .getOrCreate()

      import spark.implicits._

      /*
          Schema Ref : http://docs.aws.amazon.com/elasticloadbalancing/latest/classic/
                        access-log-collection.html#access-log-entry-format

          timestamp elb client:port backend:port request_processing_time backend_processing_time
          response_processing_time elb_status_code backend_status_code received_bytes sent_bytes
          "request" "user_agent" ssl_cipher ssl_protocol
      */

      val schema = new StructType(
                          Array(
                                StructField("request_timestamp", TimestampType, false),
                                StructField("elb", StringType, false),
                                StructField("client", StringType, false),
                                StructField("backend", StringType, true),
                                StructField("request_processing_time", DoubleType, true),
                                StructField("backend_processing_time", DoubleType, true),
                                StructField("response_processing_time", DoubleType, true),
                                StructField("elb_status_code", IntegerType, true),
                                StructField("backend_status_code", IntegerType, true),
                                StructField("received_bytes", LongType, true),
                                StructField("sent_bytes", LongType, true),
                                StructField("request", StringType, true),
                                StructField("user_agent", StringType, true),
                                StructField("ssl_cipher", StringType, true),
                                StructField("ssl_protocol", StringType, true)
                              )
                          )

      val rawLogs = spark.sqlContext.read.format("com.databricks.spark.csv")
                    .option("header", "false")
                    .option("delimiter", " ")
                    .schema(schema)
                    .load("/home/neel/WeblogChallenge/data")

      rawLogs.createOrReplaceTempView("rawLogs")

      /* Basic Data Analysis

        ELB = marketpalce-shop
        From : 2015-07-22 02:40:16.121
        To:    2015-07-22 21:27:03.632
        numOfRecords = 1158500
        invalidRecords = 162
        Distinct IP/Users = 90539

      */

      spark.sqlContext.sql("SELECT DISTINCT(elb) FROM rawLogs").show(1,false)

      spark.sqlContext.sql("SELECT MIN(request_timestamp),MAX(request_timestamp) FROM rawLogs").show(1, false)

      spark.sqlContext.sql("SELECT COUNT(*) FROM rawLogs " +
                           "WHERE backend = '-' " +
                           "OR request_processing_time = -1 " +
                           "OR backend_processing_time = -1 " +
                           "OR response_processing_time = -1 ").show(200, false)

      spark.sqlContext.sql("SELECT elb_status_code FROM rawLogs WHERE backend_status_code = 404 ").show(false)
      spark.sqlContext.sql("SELECT * FROM rawLogs WHERE request = '---'" ).show(false)

      /*
         1. Sessionize

            I have used window function lag() to construct sessions.

            Idea here is to group all events of user and order them by timestamp, then use lag() function to find out
            time difference between consecutive events. If this time difference is greater than some threshold,
            it is marked as session boundary (first event of next session)

            Ref : https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html

        2. Handle Unique Users

            Ignore client port info when creating user session.
            Connection from same IP with different ports are interleaving in time, which could be result of multiple
            browser tabs during same session.

            IP addresses do not guarantee distinct users

            One way to deal with this is to concatenate 'user-agent' with ip and use this as key to uniquely
            determine user.
            I used sparks default md5 function to generate unique id for every ip, user_agent combination.

            This approach still doesn't guarantee uniqueness of users.

            I have analyzed dataset based on both ip and id.

        3. URL

            Extract url from HTTP request.

        4. Invalid records

            Filter records which are not processed by backend (where backend = '-' and *_processing_time = -1)

            We could also filter events with 4XX and 5XX error codes if we wish to not consider those
            request as part of session.
       */

      val sessionThreshold = 900;
      val partitionByIp = Window.partitionBy($"ip").orderBy(unix_timestamp($"request_timestamp"))

      val logs = rawLogs.filter($"backend"!== "-")
                .withColumn("ip", split($"client", ":")(0))
                .withColumn("id", md5(concat($"ip", $"user_agent")).cast(StringType))
                .filter($"id".isNotNull)
                .withColumn("url", split($"request", " ")(1))
                .withColumn("processing_time", $"request_processing_time" + $"backend_processing_time" + $"response_processing_time")
                .withColumn("bytes_exchanged", $"received_bytes" + $"sent_bytes")
                .withColumn("time_difference", unix_timestamp($"request_timestamp") - lag(unix_timestamp($"request_timestamp"), 1)
                .over(partitionByIp))
                .withColumn("new_session", when($"time_difference".isNull ||$"time_difference" > sessionThreshold, 1).otherwise(0))
                .withColumn("session_id", sum("new_session") over partitionByIp )

      logs.createOrReplaceTempView("logs")

      spark.sqlContext.cacheTable("logs")

      spark.sqlContext.sql("SELECT COUNT(DISTINCT(ip)) FROM logs").show(false)
      spark.sqlContext.sql("SELECT COUNT(DISTINCT(id)) FROM logs").show(false)
      spark.sqlContext.sql("SELECT ip, id, request_timestamp, session_id FROM logs").show(500, false)


      // Goal 1 sessionize log

      val sessonizedLogs = spark.sqlContext.sql("""SELECT
                                                      ip, session_id , COUNT(url) as total_requests,
                                                      MIN(request_timestamp) as start_time, MAX(request_timestamp) as end_time, unix_timestamp(MAX(request_timestamp)) - unix_timestamp(MIN(request_timestamp)) as duration,
                                                      SUM(processing_time) as processing_time,
                                                      SUM(bytes_exchanged) as bytes_exchanged
                                                FROM logs
                                                GROUP BY ip, session_id
                                                ORDER BY ip, session_id
                                              """)

      sessonizedLogs.createOrReplaceTempView("sessonizedLogs")
      spark.sqlContext.cacheTable("sessonizedLogs") // cache both logs and sessonizedLogs table for better performance
      saveAsCSV(sessonizedLogs.coalesce(1), "sessonizedLogs")

      /*
          Goal 2 Average session time
            1. Overall Avg : 574.5s
            2. Average session time per user
      */

      spark.sqlContext.sql("SELECT AVG(duration) FROM sessonizedLogs").show()

      val avgSessionTimePerIp = spark.sqlContext.sql("SELECT ip, AVG(durataion) FROM sessonizedLogs GROUP BY ip")
      saveAsCSV(avgSessionTimePerIp.coalesce(1), "avgSessionTimePerIp")

      // Goal 3 Unique URL visits per session
      val uniqueURLPerSession = spark.sqlContext.sql("SELECT ip, session_id, url FROM logs GROUP BY ip, session_id, url")
      saveAsCSV(uniqueURLPerSession.coalesce(1), "uniqueURLPerSession")

      /*
        Goal 4 Most engaged user
        1. Longest session times
        2. longest average session duration
      */

      val topEngagedUsersByDuration = spark.sqlContext.sql("SELECT ip, MAX(duration) as session_duration " +
                                                            "FROM sessonizedLogs " +
                                                            "GROUP BY ip ORDER BY MAx(duration) DESC limit 100")
      saveAsCSV(topEngagedUsersByDuration.coalesce(1), "topEngagedUsersByDuration")


      val topEngagedUsersByAvgDuration = spark.sqlContext.sql("SELECT ip, AVG(duration) FROM sessonizedLogs " +
                                                              "GROUP BY ip ORDER BY AVG(duration) DESC limit 100")
      saveAsCSV(topEngagedUsersByAvgDuration.coalesce(1), "topEngagedUsersByAvgDuration")


      /*
        Analysis using id
      */

      val partitionById = Window.partitionBy($"id").orderBy(unix_timestamp($"request_timestamp"))

      val logsById = rawLogs.filter($"backend"!== "-")
                    .withColumn("ip", split($"client", ":")(0))
                    .withColumn("id", md5(concat($"ip", $"user_agent")).cast(StringType))
                    .filter($"id".isNotNull)
                    .withColumn("url", split($"request", " ")(1))
                    .withColumn("processing_time", $"request_processing_time" + $"backend_processing_time" + $"response_processing_time")
                    .withColumn("bytes_exchanged", $"received_bytes" + $"sent_bytes")
                    .withColumn("time_difference", unix_timestamp($"request_timestamp") - lag(unix_timestamp($"request_timestamp"), 1)
                    .over(partitionById))
                    .withColumn("new_session", when($"time_difference".isNull ||$"time_difference" > sessionThreshold, 1).otherwise(0))
                    .withColumn("session_id", sum("new_session") over partitionById )

      //Drop columns which aren't used

      logsById.createOrReplaceTempView("logsById")
      spark.sqlContext.cacheTable("logsById")

      /*
          Goal 1 sessionize log
      */

      val sessonizedLogsById = spark.sqlContext.sql("""SELECT
                                                          id, session_id , COUNT(url) as total_requests,
                                                          MIN(request_timestamp) as start_time, MAX(request_timestamp) as end_time, unix_timestamp(MAX(request_timestamp)) - unix_timestamp(MIN(request_timestamp)) as duration,
                                                          SUM(processing_time) as processing_time,
                                                          SUM(bytes_exchanged) as bytes_exchanged
                                                        FROM logsById
                                                        GROUP BY id, session_id
                                                        ORDER BY id, session_id
                                                    """)

      sessonizedLogsById.createOrReplaceTempView("sessonizedLogsById")

      spark.sqlContext.cacheTable("sessonizedLogsById") // cache both logs and sessonizedLogs table for better performance

      saveAsCSV(sessonizedLogsById.coalesce(1), "sessonizedLogsById")

      /*
        Goal 2 Average session time
          1. Overall Avg : 532.40s
          2. Average session time per user
      */

      spark.sqlContext.sql("SELECT AVG(duration) FROM sessonizedLogsById").show(false)
      spark.sqlContext.sql("SELECT id, AVG(duration) FROM sessonizedLogsById GROUP BY id").show(false)

      // Goal 3 Unique URL visits per session
      val uniqueURLPerSessionById = spark.sqlContext.sql("SELECT id, session_id, url " +
                                                        "FROM logsById GROUP BY id, session_id, url " +
                                                        "ORDER BY id, session_id ")

      saveAsCSV(uniqueURLPerSessionById.coalesce(1), "uniqueURLPerSessionById")

      /*
        Goal 4 Most engaged user
        1. Longest session times
        2. longest average session duration
      */

      val topEngagedUsersByDurationById = spark.sqlContext.sql("SELECT id, max(duration) as session_duration" +
                                                            "FROM sessonizedLogsById" +
                                                            "GROUP BY id ORDER BY MAX(duration) DESC")

      saveAsCSV(topEngagedUsersByDurationById.coalesce(1), "topEngagedUsersByDurationById")

      val topEngagedUsersByAvgDurationById = spark.sqlContext.sql("SELECT id, AVG(duration) " +
                                                                "FROM sessonizedLogsById " +
                                                                "GROUP BY id " +
                                                                "ORDER BY AVG(duration) DESC ")

      saveAsCSV(topEngagedUsersByAvgDurationById.coalesce(1), "topEngagedUsersByAvgDurationById")

      spark.stop()
  }

}
