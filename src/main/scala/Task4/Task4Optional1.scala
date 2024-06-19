package Task4

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Task4Optional1 extends App {

  val spark = initializeSparkSession()
  val sc = spark.sparkContext

  val logFilePath = "/Users/aniketsharma/Documents/access_log.txt"
  val logData = loadLogData(sc, logFilePath)
  val parsedLogs = parseLogData(logData)

  val uniqueIps = getUniqueIPs(parsedLogs)
  println("Unique IPs:")
  uniqueIps.foreach(println)

  val urlStatus200 = getURLStatus200(parsedLogs)
  println("URLs with number of 200 status:")
  urlStatus200.foreach { case (url, count) => println(s"$url: $count") }

  val responses4xx = getResponses4xx(parsedLogs)
  println(s"Number of 4xx responses: $responses4xx")

  val responsesMoreThan5000Bytes = getResponsesMoreThan5000Bytes(parsedLogs)
  println(s"Number of requests that sent more than 5000 bytes: $responsesMoreThan5000Bytes")

  val mostRequestedUrl = getMostRequestedUrl(parsedLogs)
  println("URL with the most number of requests:")
  mostRequestedUrl.foreach { case (url, count) => println(s"$url: $count") }

  val most404ResponsesUrl = getMost404ResponsesUrl(parsedLogs)
  println("URL with the most number of 404 responses:")
  most404ResponsesUrl.foreach { case (url, count) => println(s"$url: $count") }

  spark.stop()

  def initializeSparkSession(): SparkSession = {
    SparkSession.builder
      .appName("Log Analysis")
      .master("local[*]")
      .getOrCreate()
  }

  def loadLogData(sc: SparkContext, logFilePath: String) = {
    sc.textFile(logFilePath)
  }

  def parseLogData(logData: org.apache.spark.rdd.RDD[String]) = {
    val logPattern = """(\S+) - - \[.*\] "(?:GET|POST) (\S+) \S+" (\d{3}) (\d+|-).*""".r
    logData.flatMap {
      case logPattern(ip, url, status, bytes) => Some((ip, url, status.toInt, if (bytes == "-") 0 else bytes.toInt))
      case _ => None
    }
  }

  def getUniqueIPs(parsedLogs: org.apache.spark.rdd.RDD[(String, String, Int, Int)]): Array[String] = {
    parsedLogs.map(_._1).distinct().collect()
  }

  def getURLStatus200(parsedLogs: org.apache.spark.rdd.RDD[(String, String, Int, Int)]): Array[(String, Int)] = {
    parsedLogs.filter(_._3 == 200)
      .map { case (_, url, _, _) => (url, 1) }
      .reduceByKey(_ + _)
      .collect()
  }

  def getResponses4xx(parsedLogs: org.apache.spark.rdd.RDD[(String, String, Int, Int)]): Long = {
    parsedLogs.filter { case (_, _, status, _) => status >= 400 && status < 500 }.count()
  }

  def getResponsesMoreThan5000Bytes(parsedLogs: org.apache.spark.rdd.RDD[(String, String, Int, Int)]): Long = {
    parsedLogs.filter { case (_, _, _, bytes) => bytes > 5000 }.count()
  }

  def getMostRequestedUrl(parsedLogs: org.apache.spark.rdd.RDD[(String, String, Int, Int)]): Array[(String, Int)] = {
    parsedLogs.map { case (_, url, _, _) => (url, 1) }
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(1)
  }

  def getMost404ResponsesUrl(parsedLogs: org.apache.spark.rdd.RDD[(String, String, Int, Int)]): Array[(String, Int)] = {
    parsedLogs.filter(_._3 == 404)
      .map { case (_, url, _, _) => (url, 1) }
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(1)
  }
}
