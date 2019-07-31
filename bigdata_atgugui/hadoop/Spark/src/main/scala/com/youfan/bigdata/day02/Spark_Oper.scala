package com.youfan.bigdata.day02

import java.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by chenqinping on 2019/5/8 14:24
  */
object Spark_Oper {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Spark_Oper").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)
    /*
        val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

        val listRDD2: RDD[Int] = listRDD.map(_*2)

        listRDD2.collect().foreach(println)*/

    //        val listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(2, 3)))


    /* val partitionsRDD: RDD[Int] = listRDD.mapPartitions(datas => {

       datas.map( data=> data * 2)

     })
     partitionsRDD.collect().foreach(println)*/

    /*  val tupleMap: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
        case (num, datas) => {
          datas.map((_, " 分区号 " + num))
        }
      }*/


    //    val tupleMap: RDD[Int] = listRDD.flatMap(x => x)
    //
    //    tupleMap.collect().foreach(println)

    /* val partitions: RDD[Int] = sc.makeRDD(1 to 16, 5)
     val glom: RDD[Array[Int]] = partitions.glom()
     glom.collect().foreach(array=>{
       println(array.min)
     })*/


    //    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
    //
    //    /*val groupByKey: RDD[(Int, Iterable[Int])] = listRDD.groupBy(_ % 3)*/
    //
    ////    val flliterRDD: RDD[Int] = listRDD.filter(_%2==1)
    //
    //
    //    val sampleRDD: RDD[Int] = listRDD.sample(true,1,1)
    //
    //    sampleRDD.collect().foreach(println)
   /* val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 3, 3, 5, 2, 4, 5, 6, 7))
    val distinctRDD: RDD[Int] = listRDD.distinct()

    distinctRDD.collect().foreach(println)*/


    val listRDD: RDD[Int] = sc.parallelize(1 to 16,4)

    println(listRDD.partitions.size)

    val coRDD: RDD[Int] = listRDD.coalesce(3)


    println(coRDD.partitions.size)

    coRDD.saveAsObjectFile("C:\\D\\JAVA\\spark\\workbase\\spark\\hadoop\\Spark\\output")

  }

}
