package com.smallbell.offline

import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// 模型评估和参数选择
object ALSTrainer {

  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]) = {
    val userProducts = data.map(item => (item.user,item.product))
    val predictRating = model.predict(userProducts)
    val real = data.map(item => ((item.user,item.product),item.rating))
    val predict = predictRating.map(item => ((item.user,item.product),item.rating))
    sqrt(
      real.join(predict).map{
        case ((userId,productId),(real,pre))=>
          val err = real - pre
          err * err
      }.mean()
    )
  }

  def adjustALSParams(trainData: RDD[Rating], testData: RDD[Rating]) = {
    val result = for (rank <- Array(100, 200, 250); lambda <- Array(1, 0.1, 0.01, 0.001))
      yield {
        val model = ALS.train(trainData, rank, 5, lambda)
        val rmse = getRMSE(model, testData)
        (rank, lambda, rmse)
      }
  }

  def main(args: Array[String]): Unit = {
    // 定义配置
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://root:123456@121.40.178.96:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建spark session
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    import spark.implicits._

    //加载评分数据
    val ratingRDD = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",OfflineRecommender.MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(rating => Rating(rating.userId,rating.productId,rating.score)).cache()

    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))

    val trainingRDD = splits(0)
    val testingRDD = splits(1)


    adjustALSParams(trainingRDD, testingRDD)

    //关闭Spark
    spark.close()
  }
}
