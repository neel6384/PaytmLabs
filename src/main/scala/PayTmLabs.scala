

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object PayTmLabs {

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

      val schema = new StructType(Array(
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

      val elb = spark.sqlContext.sql("SELECT DISTINCT(elb) FROM rawLogs")

      val timeRange = spark.sqlContext.sql("SELECT MIN(request_timestamp),MAX(request_timestamp) FROM rawLogs")

      val invalidRecords = spark.sqlContext.sql("SELECT COUNT(*) FROM rawLogs " +
                                               "WHERE backend = '-' " +
                                               "OR request_processing_time = -1 " +
                                               "OR backend_processing_time = -1 " +
                                               "OR response_processing_time = -1 ")

        /*
          I have used window function lag() to construct sessions.

          Idea here is to group all events of user and order them by timestamp,
          then use lag() function to find out time difference between consecutive events.
          If this time difference is greater than some threshold,
          it is marked as session boundary (first event of next session)

          Ref : https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html

          IP : Ignore client port info when creating user session.
               Connection from same IP with different ports are interleaving in time,
               which could be result of multiple browser tabs during same session.

          URL : Extract url from HTTP request.

          Filter records which are not processed by backend (where backend = '-' and *_processing_time = -1)

          We could also filter events with 4XX and 5XX error codes if we wish to not consider those request
          as part of session.
        */

      val sessionLength = 900;
      val partitionByIP = Window.partitionBy("ip").orderBy(unix_timestamp($"request_timestamp"))

      val logs = rawLogs.filter($"backend"!== "-")
                .withColumn("ip", split($"client", ":")(0))
                .withColumn("url", split($"request", " ")(1))
                .withColumn("processing_time", $"request_processing_time" + $"backend_processing_time" + $"response_processing_time")
                .withColumn("bytes_exchanged", $"received_bytes" + $"sent_bytes")
                .withColumn("time_difference", unix_timestamp($"request_timestamp") - lag(unix_timestamp($"request_timestamp"), 1)
                .over(partitionByIP))
                .withColumn("new_session", when($"time_difference".isNull ||$"time_difference" > sessionLength, 1).otherwise(0))
                .withColumn("session_id", sum("new_session") over partitionByIP )

      logs.createOrReplaceTempView("logs")

      spark.sqlContext.cacheTable("logs")

      val users = spark.sqlContext.sql("SELECT count(DISTINCT(IP)) FROM logs ")


      // Goal 1 sessionize log

      val sessonizedLogs = spark.sqlContext.sql("""SELECT
                                              ip, session_id , COUNT(url) as total_requests,
                                              MIN(request_timestamp) as start_time, MAX(request_timestamp) as end_time, unix_timestamp(MAX(request_timestamp)) - unix_timestamp(MIN(request_timestamp)) as duration,
                                              SUM(processing_time) as processing_time,
                                              SUM(bytes_exchanged) as bytes_exchanged
                                        FROM logs
                                        GROUP BY ip, session_id
                                      """)

      sessonizedLogs.createOrReplaceTempView("sessonizedLogs")

      spark.sqlContext.cacheTable("sessonizedLogs") // cache both logs and sessonizedLogs table for better performance

      sessonizedLogs.write
                    .format("com.databricks.spark.csv")
                    .option("header", "true")
                    .save("/home/neel/sessionizedlogs.csv")

        /*
        Goal 2 Average session time
          1. Overall Avg : 574.5s
          2. Average session time per user
        */

      val avgSessionTime = spark.sqlContext.sql("SELECT AVG(duration) from sessonizedLogs")
      val avgSessionTimePerIp = spark.sqlContext.sql("SELECT ip, AVG(duration) from sessonizedLogs group by ip")

      // Goal 3 Unique URL visits per session
      val uniqueURLPerSession = spark.sqlContext.sql("SELECT ip, session_id, url, count(*) FROM logs GROUP BY ip, session_id, url")

    /*
      Goal 4 Most engaged user
      1. Longest session times
      2. longest average session duration
    */

      val topEngagedUsersByDuration = spark.sqlContext.sql("SELECT ip, max(duration) as session_duration " +
                                                            "FROM sessonizedLogs " +
                                                            "GROUP BY ip order by max(duration) desc limit 100")


      val topEngagedUsersByAvgDuration = spark.sqlContext.sql("SELECT ip, AVG(duration) FROM sessonizedLogs " +
                                                              "GROUP BY ip ORDER BY AVG(duration) DESC limit 100")


      spark.stop()
  }

}
