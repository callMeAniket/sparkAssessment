package Task5

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BookSalesETL {
  val config = ConfigFactory.load()
  private val dbUrl = config.getString("db.url")
  private val dbUser = config.getString("db.user")
  private val dbPassword = config.getString("db.password")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("BookSalesETL")
      .getOrCreate()

    // Read the input data
    val bookSalesDF = spark
      .read
      .format("jdbc")
      .option("url", dbUrl)
      .option("dbtable", "BookSales")
      .option("user", dbUser)
      .option("password", dbPassword)
      .load()

    // Create the normalized Books table
    val booksDF = bookSalesDF.select("book_id", "title", "author_id", "genre", "publish_date").distinct()

    // Create the normalized Authors table
    val authorsDF = bookSalesDF.select("author_id", "author_name").distinct()

    // Create the normalized Sales table
    val salesDF = bookSalesDF.select("sale_id", "book_id", "sale_date", "quantity", "price")

    // Create the aggregated Sales by Book table
    val salesByBookDF = salesDF.groupBy("book_id").agg(
      sum("quantity").alias("total_quantity"),
      sum(col("quantity") * col("price")).alias("total_sales")
    )

    // Create the aggregated Sales by Title table
    val salesByTitleDF = bookSalesDF.groupBy("title").agg(
      sum("quantity").alias("total_quantity"),
      sum(col("quantity") * col("price")).alias("total_sales")
    )

    // Create the aggregated Sales by Sales Month table
    val salesByMonthDF = salesDF.withColumn("year", year(col("sale_date")))
      .withColumn("month", month(col("sale_date")))
      .groupBy("year", "month").agg(
        sum("quantity").alias("total_quantity"),
        sum(col("quantity") * col("price")).alias("total_sales")
      )

    // JDBC connection properties
    val jdbcUrl = dbUrl
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("user", dbUser)
    connectionProperties.setProperty("password", dbPassword)

    // Write DataFrames to RDBMS tables
    booksDF.write
      .mode("overwrite")
      .jdbc(jdbcUrl, "Book1s", connectionProperties)

    authorsDF.write
      .mode("overwrite")
      .jdbc(jdbcUrl, "Authors1", connectionProperties)

    salesDF.write
      .mode("overwrite")
      .jdbc(jdbcUrl, "Sales1", connectionProperties)

    salesByBookDF.write
      .mode("overwrite")
      .jdbc(jdbcUrl, "SalesByBook1", connectionProperties)

    salesByTitleDF.write
      .mode("overwrite")
      .jdbc(jdbcUrl, "SalesByTitle1", connectionProperties)

    salesByMonthDF.write
      .mode("overwrite")
      .jdbc(jdbcUrl, "SalesByMonth1", connectionProperties)

    spark.stop()
  }
}

