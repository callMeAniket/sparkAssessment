package Task1

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object taskt1 {
  val config = ConfigFactory.load()
  private val awsAccessKeyId = config.getString("aws.access-key-id")
  private val awsSecretAccessKey = config.getString("aws.secret-access-key")
  private val awsS3Endpoint = config.getString("aws.s3.endpoint")

  def main(args: Array[String]): Unit = {
    val tableName = "air_quality_data_new"
    val keySpaceName = "my_keyspace"
    val df = readFromS3(getSpark("readS3"), "s3a://mytestbucket-payoda/zaragoza_data.csv")
    writeToKeySpace(df, tableName, keySpaceName)
    readFromKeySpace(getSpark("KeyspaceToParquet"), tableName, keySpaceName)
    writeToS3(df, "s3a://mytestbucket-payoda/air-quality-parquet/")
    aggregationOnParquet(getSpark("aggregation"), "s3a://mytestbucket-payoda/air-quality-parquet/")
  }

  private def getSpark(name: String): SparkSession = {
    SparkSession.builder
      .appName(name)
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .config("spark.master", "local")
      .config("spark.cassandra.connection.host", "cassandra.ap-south-1.amazonaws.com")
      .config("spark.cassandra.connection.port", "9142")
      .config("spark.cassandra.connection.ssl.enabled", "true")
      .config("spark.cassandra.auth.username", "aniketProgram-at-533267282416")
      .config("spark.cassandra.auth.password", "dqXefs1niwS9cT4HWSSWyjwpQKiDifh74zTbwYlPPWmvJyUKe4paAlvOHDE=")
      .config("spark.cassandra.input.consistency.level", "LOCAL_QUORUM")
      .config("spark.cassandra.connection.ssl.trustStore.path", "/Users/aniketsharma/cassandra_truststore.jks")
      .config("spark.cassandra.connection.ssl.trustStore.password", "pbdmbm6*")
      .getOrCreate()
  }

  private def readFromS3(spark: SparkSession, csvFilePath: String): DataFrame = {

    println("Aniket :: trying reading from S3")

    val hadoopConfig = spark.sparkContext.hadoopConfiguration
    hadoopConfig.set("fs.s3a.access.key", awsAccessKeyId)
    hadoopConfig.set("fs.s3a.secret.key", awsSecretAccessKey)
    hadoopConfig.set("fs.s3a.endpoint", awsS3Endpoint)
    hadoopConfig.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    val df = spark.read.option("header", "true").csv(csvFilePath)
    println("Aniket :: read from S3 " + df.show())
    df
  }

  private def writeToKeySpace(df: DataFrame, tableName: String, keySpaceName: String): Unit = {
    println("Aniket :: writing to keySpace")
    df.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> tableName, "keyspace" -> keySpaceName))
      .mode("append")
      .save()
    println("Aniket :: written to KeySpace")
  }

  private def readFromKeySpace(sparkSession: SparkSession, tableName: String, keySpaceName: String): DataFrame = {
    println("Aniket :: trying reading from keyspace")
    val df = sparkSession.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> tableName, "keyspace" -> keySpaceName))
      .load()
    println("Aniket :: fetched from keyspace")
    df
  }

  private def writeToS3(df: DataFrame, parquetOutputPath: String): Unit = {
    println("Aniket :: reached here to write in s3")
    df.write.parquet(parquetOutputPath)
  }

  private def aggregationOnParquet(spark: SparkSession, parquetInputPath: String) = {
    println("Aniket :: reached here")
    val parquetDF = spark.read.parquet(parquetInputPath)

    // Example 1: Average pollutant value by city
    val avgPollutantByCity = parquetDF.groupBy("city").agg(avg("value").alias("average_pollutant"))
    avgPollutantByCity.show()

    // Example 2: Maximum pollutant value by country
    val maxPollutantByCountry = parquetDF.groupBy("country").agg(functions.max("value").alias("max_pollutant"))
    maxPollutantByCountry.show()

    // Example 3: Count of records per pollutant
    val countByPollutant = parquetDF.groupBy("pollutant").count()
    countByPollutant.show()
  }
}
