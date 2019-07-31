package com.youfan.bigdata.copy

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkSubmit
import org.apache.spark.deploy.yarn.ApplicationMaster
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

/**
  * Create by chenqinping on 2019/5/14 8:49
  */
object Scala_UDAF_Class_Copy {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Scala_UDAF_Class_Copy").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    SparkSubmit
    ApplicationMaster

    val copy = new MyCopy

    val avgAge: TypedColumn[User, Double] = copy.toColumn.name("avgAge")

    val userDF: DataFrame = spark.read.json("C:\\D\\JAVA\\spark\\workbase\\spark\\hadoop\\Spark\\in\\user.json")

    import  spark.implicits._

    val uDS: Dataset[User] = userDF.as[User]

    uDS.select(avgAge).show()

    spark.stop()

  }
}

case class User(name: String, age: BigInt)

case class ACount(var sum: BigInt, var count: Int)

class MyCopy extends Aggregator[User, ACount, Double] {
  override def zero: ACount = ACount(0, 0)

  override def reduce(b: ACount, a: User): ACount = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  override def merge(b1: ACount, b2: ACount): ACount = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  override def finish(reduction: ACount): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[ACount] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
