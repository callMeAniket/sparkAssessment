package Task4

import org.apache.spark.sql.SparkSession


object Task4Optional1 extends App {
  // Step 1: Initialize Spark session
  val spark = SparkSession.builder
    .appName("Log Analysis")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  // Step 2: Load the log data into an RDD
  val logFilePath = "/Users/aniketsharma/Documents/access_log.txt"
  val logData = sc.textFile(logFilePath)

  // Step 3: Parse the log data
  val logPattern = """(\S+) - - \[.*\] "(?:GET|POST) (\S+) \S+" (\d{3}) (\d+|-).*""".r

  val parsedLogs = logData.flatMap {
    case logPattern(ip, url, status, bytes) => Some((ip, url, status.toInt, if (bytes == "-") 0 else bytes.toInt))
    case _ => None
  }

  // 1. List of Unique IPs that made the requests
  val uniqueIps = parsedLogs.map(_._1).distinct().collect()
  println("Unique IPs:")
  uniqueIps.foreach(println)

  // 2. Group by URLs and number of 200 status
  val urlStatus200 = parsedLogs.filter(_._3 == 200)
    .map { case (_, url, _, _) => (url, 1) }
    .reduceByKey(_ + _)
    .collect()

  println("URLs with number of 200 status:")
  urlStatus200.foreach { case (url, count) => println(s"$url: $count") }

  // 3. Number of 4xx responses
  val responses4xx = parsedLogs.filter { case (_, _, status, _) => status >= 400 && status < 500 }.count()
  println(s"Number of 4xx responses: $responses4xx")

  // 4. Number of requests that sent more than 5000 bytes as response
  val responsesMoreThan5000Bytes = parsedLogs.filter { case (_, _, _, bytes) => bytes > 5000 }.count()
  println(s"Number of requests that sent more than 5000 bytes: $responsesMoreThan5000Bytes")

  // 5. URL that has the most number of requests
  val mostRequestedUrl = parsedLogs.map { case (_, url, _, _) => (url, 1) }
    .reduceByKey(_ + _)
    .sortBy(_._2, ascending = false)
    .take(1)

  println("URL with the most number of requests:")
  mostRequestedUrl.foreach { case (url, count) => println(s"$url: $count") }

  // 6. URL that has the most number of 404 responses
  val most404ResponsesUrl = parsedLogs.filter(_._3 == 404)
    .map { case (_, url, _, _) => (url, 1) }
    .reduceByKey(_ + _)
    .sortBy(_._2, ascending = false)
    .take(1)

  println("URL with the most number of 404 responses:")
  most404ResponsesUrl.foreach { case (url, count) => println(s"$url: $count")}


    spark.stop()
}