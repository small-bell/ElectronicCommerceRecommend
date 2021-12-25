package com.smallbell.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)

object StatisticsRecommender {

  val MONGODB_RATING_COLLECTION = "Rating"
  //统计的表的名称
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  val AVERAGE_PRODUCTS = "AverageProducts"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://root:123456@121.40.178.96:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf = new SparkConf().setAppName("StatisticsRecommender")
      .setMaster(config("spark.cores"))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 隐式转换
    import spark.implicits._

    // 数据加载
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    ratingDF.createOrReplaceTempView("ratings")

    // 统计所有历史数据中每个商品的评分数
    val rateMoreProductsDF = spark.sql("select productId, count(productId) as count from ratings group by productId ")
    rateMoreProductsDF.write
        .option("uri", mongoConfig.uri)
        .option("collection", RATE_MORE_PRODUCTS)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()

    val simpleDateFormat = new SimpleDateFormat("yyyMM")
    spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(
      new Date(x * 100L)
    ).toInt)

    // 将原来的Rating数据集中的时间转换成年月的格式
    val ratingOfYearMonth = spark.sql("select productId, score, " +
      "changeDate(timestamp) as yearmonth from ratings")

    // 将新的数据集注册成为一张表
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")
    val rateMoreRecentlyProducts = spark.sql("select productId, count(productId)" +
      " as count, yearmonth from ratingOfMonth group by yearmonth, productId" +
      " order by yearmonth desc, count desc")

    rateMoreRecentlyProducts
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",RATE_MORE_RECENTLY_PRODUCTS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //统计每个商品的平均评分
    val averageProductsDF = spark.sql("select productId, avg(score) as avg from ratings group by productId ")
    averageProductsDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",AVERAGE_PRODUCTS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()


  }

}
