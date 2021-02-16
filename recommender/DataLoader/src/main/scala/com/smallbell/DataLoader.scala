package com.smallbell

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Product(productId: Int, name: String, imageUrl: String, categories: String,
                   tags: String)

case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)

object DataLoader {

  val PRODUCT_DATA_PATH = "E:\\java_idea\\ECommerceRecommendSystem" +
    "\\recommender\\DataLoader\\src\\main\\resources\\products.csv"
  val RATING_DATA_PATH = "E:\\java_idea\\ECommerceRecommendSystem" +
    "\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"

  val MONGODB_PRODUCT_COLLECTION = "Product"
  val MONGODB_RATING_COLLECTION = "Rating"

  def main(args: Array[String]): Unit = {

    def storeDataInMongoDB(productDF: DataFrame, ratingDF: DataFrame)
                          (implicit mongoConfig: MongoConfig): Unit = {
      val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

      val productCollection = mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION)
      val ratingCollection = mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)

      productCollection.dropCollection()
      ratingCollection.dropCollection()

      productDF.write
        .option("uri", mongoConfig.uri)
        .option("collection", MONGODB_PRODUCT_COLLECTION)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()
      ratingDF.write
        .option("uri",mongoConfig.uri)
        .option("collection",MONGODB_RATING_COLLECTION)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()

      //对数据表建索引
      productCollection.createIndex(MongoDBObject("productId" -> 1))
      ratingCollection.createIndex(MongoDBObject("userId" -> 1))
      ratingCollection.createIndex(MongoDBObject("productId" -> 1))

      //关闭MongoDB的连接
      mongoClient.close()
    }

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://root:123456@121.40.178.96:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf = new SparkConf().setAppName("DataLoader")
      .setMaster(config("spark.cores"))
    val spark = SparkSession.builder()
      .config(sparkConf).getOrCreate();
    import spark.implicits._
    val productRDD = spark.sparkContext.textFile(PRODUCT_DATA_PATH)
    val productDF = productRDD.map(item => {
      val attr = item.split("\\^");
      Product(attr(0).toInt, attr(1).trim, attr(4).trim,
        attr(5).trim, attr(6).trim)
    }).toDF()
    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
    }).toDF()

    implicit val mongoConfig = MongoConfig(config.get("mongo.uri").get,
      config.get("mongo.db").get)

    storeDataInMongoDB(productDF, ratingDF)

    spark.stop()
  }


}
