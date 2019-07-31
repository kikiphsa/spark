package com.youfan.bigdata.day04

import java.sql.Connection

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf

/**
  * Create by chenqinping on 2019/5/11 14:22
  */

object Scala_mysql {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Scala_hbase").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val conf: Configuration = HBaseConfiguration.create()

    /*
    查询hbase信息
    conf.set(TableInputFormat.INPUT_TABLE, "student")

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    hbaseRDD.foreach {
      case (rowkey, result) => {
        val cells: Array[Cell] = result.rawCells()
        for (elem <- cells) {
          println(Bytes.toString(CellUtil.cloneValue(elem)))
        }
      }
    }*/


    //保存到hbase中
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("1004", 10), ("1005", 11), ("1006", 12)))

    val putRDD: RDD[(ImmutableBytesWritable, Put)] = dataRDD.map {
      case (rowkey, age) => {

        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("into"), Bytes.toBytes("age"), Bytes.toBytes(age))
        (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
      }
    }

    val jobConf = new JobConf(conf)

    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "student")

    putRDD.saveAsHadoopDataset(jobConf)

    sc.stop()

  }
}
