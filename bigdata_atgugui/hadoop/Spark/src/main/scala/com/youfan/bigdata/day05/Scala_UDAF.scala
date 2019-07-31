package com.youfan.bigdata.day05

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Create by chenqinping on 2019/5/13 14:39
  */
object Scala_UDAF {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Scala_UDAF").setMaster("local[*]")

    //配置对象

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    val udaf: myUSer = new myUSer

    spark.udf.register("avgAge", udaf)


    val userDF: DataFrame = spark.read.json("C:\\D\\JAVA\\spark\\workbase\\spark\\hadoop\\Spark\\in\\user.json")

    userDF.createOrReplaceTempView("user")

    spark.sql("select avgAge(age) from user").show()

    spark.stop()
  }

}

class myUSer extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  //计算时的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", LongType)
  }

  //返回类型
  override def dataType: DataType = DoubleType

  //是否稳定性
  override def deterministic: Boolean = true

  //计算之前缓冲区的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //sum
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    //count
    buffer(1) = buffer.getLong(1) + 1
  }

  //合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)

  }

  //计算
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}