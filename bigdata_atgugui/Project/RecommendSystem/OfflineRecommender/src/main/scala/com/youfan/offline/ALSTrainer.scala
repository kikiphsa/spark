package com.youfan.offline

import breeze.numerics.sqrt
import com.youfan.offline.ALSTrainer.adjustALSParams
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD


/**
  * Create by chenqinping on 2019/4/22 23:07
  */
object ALSTrainer {


  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://linux:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val conf = new SparkConf().setAppName("ALSTrainer").setMaster(config("spark.cores"))

    val spark = SparkSession.builder().config(conf).getOrCreate()


    val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    import spark.implicits._

    //加载评分数据
    val ratingRDD = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", offlineRecommender.MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => Rating(rating.uid, rating.mid, rating.score)).cache()

    ratingRDD.take(3).foreach(println)


    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))


    val trainingRDD = splits(0)
    val testingRDD = splits(1)

    //输出最优参数
    adjustALSParams(trainingRDD, testingRDD);

  }


  def adjustALSParams(trainingRDD: RDD[Rating], testingRDD: RDD[Rating]): Unit = {

    val result = for (rank <- Array(30, 40, 50, 60, 70); lambda <- Array(1, 0.1, 0.0001))
      yield {

        val model = ALS.train(testingRDD, rank, 5, lambda)
        val rmse = getRmse(model, testingRDD)
        (rank, lambda, rmse)

      }

    println(result.sortBy(_._3).head)

  }

  def getRmse(model: MatrixFactorizationModel, trainData: RDD[Rating]): Double = {

    val userMovies = trainData.map(item => (item.user, item.product))

    val prediceRating = model.predict(userMovies)

    val real = trainData.map(item => ((item.user, item.product), item.rating))

    real.take(2).foreach(println)

    val predict = prediceRating.map(item => ((item.user, item.product), item.rating))

    predict.take(2).foreach(println)

    println(11111)
    sqrt(
      real.join(predict).map {
        case ((uid, mid), (real, pre)) =>
          val err = real - pre
          err * err
      }.mean()

    )

  }


}
