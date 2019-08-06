package com.youfan.dataloader

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

/**
  * Create by chenqinping on 2019/4/12 18:50
  */

/**
  * Movie数据集，数据集字段通过分割
  *
  * 151^                          电影的ID
  * Rob Roy (1995)^               电影的名称
  * In the highlands ....^        电影的描述
  * 139 minutes^                  电影的时长
  * August 26, 1997^              电影的发行日期
  * 1995^                         电影的拍摄日期
  * English ^                     电影的语言
  * Action|Drama|Romance|War ^    电影的类型
  * Liam Neeson|Jessica Lange...  电影的演员
  * Michael Caton-Jones           电影的导演
  *
  * tag1|tag2|tag3|....           电影的Tag
  **/

case class Movie(val mid: Int, val name: String, val descri: String, val timelog: String, val issue: String, val shoot: String,
                 val language: String, val genres: String, val actors: String, val directors: String)


/**
  * Rating数据集，用户对于电影的评分数据集，用，分割
  *
  * 1,           用户的ID
  * 31,          电影的ID
  * 2.5,         用户对于电影的评分
  * 1260759144   用户对于电影评分的时间
  */

case class Rating(val uid: Int, val mid: Int, val score: Double, val timestamp: Int)


/**
  * Tag数据集，用户对于电影的标签数据集，用，分割
  *
  * 15,          用户的ID
  * 1955,        电影的ID
  * dentist,     标签的具体内容
  * 1193435061   用户对于电影打标签的时间
  */
case class Tag(val uid: Int, val mid: Int, val tag: String, val timestamp: Int)


/**
  * MongoDB连接配置
  *
  */
case class MongoConfig(val uri: String, val db: String)

/**
  * ElasticSearch的连接配置
  *
  * @param httpHosts      Http的主机列表，以，分割
  * @param transportHosts Transport主机列表， 以，分割
  * @param index          需要操作的索引
  * @param clustername    ES集群的名称，
  */
case class ESConfig(val httpHosts: String, val transportHosts: String, val index: String, val clustername: String)

object Dataloader {

  val MOVIE_DATA_PATH = "C:\\D\\JAVA\\spark\\workbase\\spark\\RecommendSystem\\recommender\\dataloader\\src\\main\\resources\\olddata\\movies.csv"
  val RATING_DATA_PATH = "C:\\D\\JAVA\\spark\\workbase\\spark\\RecommendSystem\\recommender\\dataloader\\src\\main\\resources\\olddata\\ratings.csv"
  val TAG_DATA_PATH = "C:\\D\\JAVA\\spark\\workbase\\spark\\RecommendSystem\\recommender\\dataloader\\src\\main\\resources\\olddata\\tags.csv"

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"

  val ES_MOVIE_INDEX = "Movie"

  //程序的入口
  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://linux:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "linux:9200",
      "es.transportHosts" -> "linux:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "es-cluster"
    )

    //需要创建一个sparkConf配置

    val sparkConf = new SparkConf().setAppName("Dataloader").setMaster(config.get("spark.cores").get)

    //需要创建一个sparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // 将Movie、Rating、Tag数据集加载进来
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)

    val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)


    //将movie转化为DataFrame
    val movieDF = movieRDD.map(item => {
      val attr = item.split("\\^")
      Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim, attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
    }).toDF()

    movieDF.show(3)

    //将rating转化为DataFrame
    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    ratingDF.show(3);

    //将tag转化DataFrame
    var tagDF = tagRDD.map(item => {
      val attr = item.split(",")
      Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
    }).toDF()

    tagDF.show(3)

    implicit val mongoConfig = MongoConfig(config.get("mongo.uri").get, config.get("mongo.db").get)


    //需要将数据保存到mongodb中
    storeDataInMongoDB(movieDF, ratingDF, tagDF)


    // 首先需要将Tag数据集进行处理，  处理后的形式为  MID ， tag1|tag2|tag3     tag1   tag2  tag3
    import org.apache.spark.sql.functions._

    /**
      * MID , Tags
      * 1     tag1|tag2|tag3|tag4....
      */

    val newTag = tagDF.groupBy($"mid").agg(concat_ws("|", collect_set($"tag")).as("tags")).select("mid", "tags")
    // 需要将处理后的Tag数据，和Moive数据融合，产生新的Movie数据，
    val movieWithTagsDF = movieDF.join(newTag, Seq("mid", "mid"), "left")

    // 声明了一个ES配置的隐式参数
    //case class ESConfig(val httpHosts: String, val transportHosts: String, val index: String, val clustername: String)
    implicit val esConfig = ESConfig(config.get("es.httpHosts").get, config.get("es.transportHosts").get, config.get("es.index").get, config.get("es.cluster.name").get)

    // 需要将新的Movie数据保存到ES中
    storeDataInES(movieWithTagsDF)

    spark.stop()
  }

  // 将数据保存到MongoDB中的方法
  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {

    //新建一个到MongoDB的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    //如果MongoDB中有对应的数据库，那么应该删除
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()

    //将当前数据写入到MongoDB
    movieDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //对数据表建立索引
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))

    //关闭mongodb连接
    mongoClient.close()

  }


  // 将数据保存到ES中的方法
  def storeDataInES(movieDF: DataFrame)(implicit eSConfig: ESConfig): Unit = {

    //新建一个配置
    val settings: Settings = Settings.builder().put("cluster.name", eSConfig.clustername).build()

    //新建一个ES的客户端
    val esClient = new PreBuiltTransportClient(settings)

    //需要将TransportHosts添加到esClient中
    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    eSConfig.transportHosts.split(",").foreach {
      case REGEX_HOST_PORT(host: String, port: String) => {
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
      }
    }

    //需要清除掉ES中遗留的数据
    if (esClient.admin().indices().exists(new IndicesExistsRequest(eSConfig.index)).actionGet().isExists) {
      esClient.admin().indices().delete(new DeleteIndexRequest(eSConfig.index))
    }
    //创建es
    esClient.admin().indices().create(new CreateIndexRequest(eSConfig.index))

    //将数据写入到ES中

    movieDF
      .write
      .option("es.nodes", eSConfig.httpHosts)
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(eSConfig.index + "/" + ES_MOVIE_INDEX)

  }

}
