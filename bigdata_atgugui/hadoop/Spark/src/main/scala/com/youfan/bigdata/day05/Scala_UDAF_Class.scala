package com.youfan.bigdata.day05

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{Encoder, _}
import org.apache.spark.sql.catalyst.expressions.aggregate.Average

/**
  * Create by chenqinping on 2019/5/13 14:39
  */
object Scala_UDAF_Class {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Scala_UDAF_Class").setMaster("local[*]")

    //配置对象

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    val udaf: MyUserRDD = new MyUserRDD


    val avgAge: TypedColumn[UserBean, Double] = udaf.toColumn.name("avgAge")

    val userDF: DataFrame = spark.read.json("C:\\D\\JAVA\\spark\\workbase\\spark\\hadoop\\Spark\\in\\user.json")

    val userBean: Dataset[UserBean] = userDF.as[UserBean]

    userBean.select(avgAge).show()

    spark.stop()
  }

}


case class UserBean(name: String, age: BigInt)

case class Average(var sum: BigInt, var count: Int)

class MyUserRDD extends Aggregator[UserBean, Average, Double] {

  override def zero: Average = {
    Average(0, 0)
  }

  override def reduce(b: Average, a: UserBean): Average = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  //聚合不同的结果
  override def merge(b1: Average, b2: Average): Average = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  override def finish(reduction: Average): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[Average] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}