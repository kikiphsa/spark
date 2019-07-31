/*
package com.atguigu.bigdata.portrayal


import org.apache.spark.mllib.linalg
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.ml.classification.LogisticRegression

import org.apache.spark.sql.Row
/**
  * Create by chenqinping on 2019/5/15 18:56
  */
object SparkML_example1 {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkML_example1").setMaster("local[*]")

    val sqlContext: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


    val training = sqlContext.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0,1.1,0.1)),
      (0.0, Vectors.dense(2.0,1.0,-1.0)),
      (0.0, Vectors.dense(2.0,1.3,1.0)),
      (1.0, Vectors.dense(0.0,1.2,-0.5))
    )).toDF("label","features")

  }
}
*/
