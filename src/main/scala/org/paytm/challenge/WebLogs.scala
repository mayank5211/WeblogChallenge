package org.paytm.challenge

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql._

object WebLogs {
  val log = LogManager.getRootLogger()
  log.setLevel(Level.INFO)

  def run(spark: SparkSession ,options: scala.collection.mutable.Map[String,String]):Unit = {
    val hiveDatabase=options.getOrElse("hiveDB","")
    val inputPath=options.getOrElse("inputPath","")

    if(hiveDatabase=="" || inputPath==""){
      log.error("Please provide necessary inpit parameters : hiveDatabase and inputPath")
      throw new IllegalArgumentException("Please provide necessary inpit parameters : hiveDatabase and inputPath")
    }

    //set hive database
    spark.sql(s"use $hiveDatabase").show

    //Create input table
    val inputTable="elb_logs"
    createInputTable(spark,inputTable,inputPath)

    //Generate epoch_time from request_timestamp i.e convert from ISO 8601 to epoch
    //we wil use this temp view/table inputDF elsewhere
    val inputDF = spark.sql(s"""select a.*, unix_timestamp(regexp_replace(request_timestamp, 'T',' ')) as epoch_time  from $inputTable a""")
    val inputDFView = "inputDFView"
    inputDF.createOrReplaceTempView(inputDFView)

    /**
      * Calculate session timeout based on total time difference of max numbers of users.
      * Where total time difference(totalDiff) = (last request time - first request time) per user
      * We create a historgram to find this out
      */
    log.info("Calculating sessionTimeout using historgram")
    val queryHistorgram =
      s"""
        |select CAST(e1.x AS BIGINT) from (
        | select histogram_numeric(maxDiff,10) as hist
        | from
        | (
        |   select
        |   max(epoch_time)-min(epoch_time) as maxDiff
        |   from
        |   $inputDFView
        |   group by request_ip
        | ) a
        | ) b
        | LATERAL VIEW explode(hist) e as e1
        | order by e1.y desc
        | limit 1
      """.stripMargin

    //val sessionTimeout = 15*60 // 15 min
    val sessionTimeout = spark.sql(queryHistorgram).head.getLong(0)
    log.info(s"Computed sessionTimeout : $sessionTimeout")
    // result is 129 sec i.e 2.15 min
    //scala> println(sessionTimeout)
    //129

    /**
      * 1. Query to sessionize the users by ip and sessionTimeout calculated above
      * Logic : For a particulat uset -> for each request we calculate the diff between current request time and last request time called "time_diff"
      *    If this difference(time_diff) > sessionTimeout , a new session is created. We create session_boundary_flag as 0 to 1 to denote session boundary
      *    To calculate incremental seesionId, we use this session_boundary_flag and keep summing to find new session id and concatenate it with request_ip
      */
    val sessionTable="elb_logs_sessionize"
    log.info(s"Calculating session info and results will be stored in table $sessionTable")
    val querySessionize =
      s"""
        |select
        |	b.*,
        |   --concatenate request_ip and calculated session id (based on session_boundary_flag)
        |   CONCAT_WS('-','user', request_ip,'session', CAST(sum(session_boundary_flag) OVER( PARTITION BY request_ip ORDER BY epoch_time)  AS STRING)) as session_id
        | FROM
        |    (select
        |      a.*,
        |      -- create session_boundary_flag where 1 represents boundary of new session
        |      CASE WHEN time_diff >= $sessionTimeout THEN 1 ELSE 0 END AS session_boundary_flag
        |    FROM
        |       (select
        |      	i.*,
        |          --get time diff between current request_time(epoch_time) and last request_time(epoch_time)
        |          epoch_time - LAG(epoch_time,1) OVER( PARTITION BY request_ip ORDER BY epoch_time) as time_diff
        |        from $inputDFView i
        |        )  a
        |   ) b
      """.stripMargin

    //execute above query and store result in table
    spark.sql(querySessionize).write.mode(SaveMode.Overwrite).saveAsTable(s"$sessionTable")
    //Sample query below to show sessions for a particular user/ip
    spark.sql(s"select request_ip,request_timestamp,epoch_time,time_diff,session_boundary_flag,session_id  from $sessionTable where request_ip = '54.169.191.85' order by epoch_time").show(200,false)

    /*+-------------+---------------------------+----------+---------+---------------------+----------------------------+
      |request_ip   |request_timestamp          |epoch_time|time_diff|session_boundary_flag|session_id                  |
      +-------------+---------------------------+----------+---------+---------------------+----------------------------+
      |54.169.191.85|2015-07-22T02:40:09.571632Z|1437558009|0        |0                    |user-54.169.191.85-session-0|
      |54.169.191.85|2015-07-22T02:40:09.559439Z|1437558009|null     |0                    |user-54.169.191.85-session-0|
      |54.169.191.85|2015-07-22T02:40:14.582310Z|1437558014|5        |0                    |user-54.169.191.85-session-0|
      |54.169.191.85|2015-07-22T02:45:06.338079Z|1437558306|0        |0                    |user-54.169.191.85-session-0|
      |54.169.191.85|2015-07-22T02:45:06.326796Z|1437558306|5        |0                    |user-54.169.191.85-session-0|
      |54.169.191.85|2015-07-22T09:00:31.548428Z|1437580831|22525    |1                    |user-54.169.191.85-session-1|
      |54.169.191.85|2015-07-22T09:00:31.561310Z|1437580831|0        |0                    |user-54.169.191.85-session-1|
      |54.169.191.85|2015-07-22T09:00:36.628626Z|1437580836|5        |0                    |user-54.169.191.85-session-1|
      |54.169.191.85|2015-07-22T09:04:42.982514Z|1437581082|5        |0                    |user-54.169.191.85-session-1|
      |54.169.191.85|2015-07-22T10:30:32.171992Z|1437586232|5150     |1                    |user-54.169.191.85-session-2|
    */

    /**
      * 2. Calculate Avg Session Time
      * Logic : For each session denoted by session_id , we calculte sessionTime as
      * difference between last and first request  max(epoch_time) - min(epoch_time)
      * and take take average
      */

    val queryAvgSessionLength =
      s"""
        |  select ROUND(avg(sessionTime),2) avg_session_time_sec
        |  from
        |  (
        |    select max(epoch_time) - min(epoch_time) as sessionTime
        |    from $sessionTable
        |    group by session_id
        |  ) a
      """.stripMargin
    log.info("Computing Avg Session Time")
    spark.sql(queryAvgSessionLength).show
   /* +--------------------+
     |avg_session_time_sec|
     +--------------------+
     |               45.14|
     +--------------------+
   */

    /**
      * 3. Unique urls per session
      * Logic : We group on session_id and take count of distinct urls withing each session
      */
    val uniqueURLsTable="elb_logs_unique_urls"
    val queryUniqueURL =
      s"""
        |  select session_id, count(distinct url) unique_url_count
        |  from $sessionTable
        |  group by session_id
      """.stripMargin

    //Execute and save results to a table
    log.info(s"Computing Unique urls per session and results saved to table $uniqueURLsTable")
    spark.sql(queryUniqueURL).write.mode(SaveMode.Overwrite).saveAsTable(s"$uniqueURLsTable")
    spark.sql(s"select * from $uniqueURLsTable").show(false)
    /*+------------------------------+----------------+
      |session_id                    |unique_url_count|
      +------------------------------+----------------+
      |user-121.242.63.60-session-1  |87              |
      |user-107.167.108.82-session-1 |89              |
      |user-52.74.219.71-session-11  |2907            |
      |user-70.39.187.66-session-0   |5               |
      |user-220.226.206.7-session-14 |91              |
    */

    /**
      * 4. Most engaged users
      * Logic : Most engaged users are those who have sessions with max length.
      *
      */

    val queryMostEngagedUsers =
      s"""
        |select
        |   request_ip,
        |   sessionLength as maxSessionLength
        | from
        |  (select
        |      request_ip,
        |      sessionLength,
        |      ROW_NUMBER() OVER(PARTITION BY request_ip ORDER BY sessionLength DESC) row_num
        |   from
        |     (select
        |         request_ip,
        |         session_id,
        |         max(epoch_time)-min(epoch_time) as sessionLength
        |      from $sessionTable
        |      group by
        |         request_ip,
        |         session_id
        |     ) a
        | ) b where row_num = 1 ORDER by maxSessionLength DESC
      """.stripMargin
    log.info("Computing Most engaged users")
    spark.sql(queryMostEngagedUsers).show(10)

   /*+--------------+----------------+
    |    request_ip|maxSessionLength|
    +--------------+----------------+
    |  52.74.219.71|             559|
    | 106.186.23.95|             559|
    |103.29.159.186|             558|
    | 119.81.61.166|             558|
    |188.40.135.194|             557|
    | 122.15.156.64|             557|
    |  125.19.44.66|             557|
    | 54.251.151.39|             557|
    |103.29.159.213|             556|
    |203.189.176.14|             556|
    +--------------+----------------+*/

    log.info("Spark Application Completed")
  }

  def createInputTable(spark: SparkSession, inputTable: String, inputPath: String):Unit = {
    //Drop table is exists already
    spark.sql(s"DROP TABLE IF EXISTS $inputTable")

    val queryCreateInputTable=
     s"""
        |CREATE EXTERNAL TABLE IF NOT EXISTS $inputTable
        | (
        | request_timestamp string,
        | elb_name string,
        | request_ip string,
        | request_port int,
        | backend_ip string,
        | backend_port int,
        | request_processing_time double,
        | backend_processing_time double,
        | client_response_time double,
        | elb_response_code string,
        | backend_response_code string,
        | received_bytes bigint,
        | sent_bytes bigint,
        | request_verb string,
        | url string,
        | protocol string,
        | user_agent string,
        | ssl_cipher string,
        | ssl_protocol string
        | )
        | ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
        | WITH SERDEPROPERTIES (
        |  'serialization.format' = '1',
        | 'input.regex' = '([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*)[:\\-]([0-9]*) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) ([-0-9]*) ([-0-9]*) \\\\\\"([^ ]*) ([^ ]*) (- |[^ ]*)\\\\\\" (\\"[^\\"]*\\") ([A-Z0-9-]+) ([A-Za-z0-9.-]*)$$' )
        | LOCATION '$inputPath'
     """.stripMargin
    spark.sql(queryCreateInputTable).count

  }

  /**
    * Main method
    * @param args : Expects hiveDB and inputPath
    */
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("WebLogs-Analyzer")
      .enableHiveSupport()
      .getOrCreate()

    try{
      //Parse input params and covert to key/value pair as options
      val options = scala.collection.mutable.Map[String,String]()
      args.foreach( arg => {
        val k :: v :: _ = arg.split("=").toList
        options(k) = v
      })

      run(spark,options)
    } finally {
      spark.stop()
    }
  }
}
