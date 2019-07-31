package com.atguigu.bigdata.zhaocheng.sql

import com.atguigu.bigdata.zhaocheng.bean.{ActionEvent, AlertEvent, AlertType}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.cep.nfa.aftermatch.{AfterMatchSkipStrategy, SkipPastLastStrategy}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Create by chenqinping on 2019/6/10 15:40
  */
object FlickCepAPp {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[String] = env.socketTextStream("hadoop102", 4444)

    dataStream.print("第一次=")
    val actionEvDStream: DataStream[ActionEvent] = dataStream.map { str =>
      val arr: Array[String] = str.split(" ")
      ActionEvent(arr(0), arr(1), arr(2), arr(3).toLong)
    }

    val eventTsStream: DataStream[ActionEvent] = actionEvDStream.assignAscendingTimestamps(_.ts).setParallelism(1)

    //    /1、同一个设备keyby
    //    //2、5分钟内window
    //    //3连续三次
    //    //4登录->没有浏览->领取
    //    A//5不同账号

    //1.同一个设备keyby
    val midKeyByDStream: KeyedStream[ActionEvent, Tuple] = eventTsStream.keyBy("mid")

    val strategy: SkipPastLastStrategy = AfterMatchSkipStrategy.skipPastLastEvent()


    //    2.登录->没有浏览->领取
    val pattern = Pattern.begin[ActionEvent]("start").where(_.action.equals("login"))
      .notFollowedBy("without").where(_.action == "item")
      .followedBy("follow").where(_.action == "coupon")



    //出发三次

    val groupPattern: Pattern[ActionEvent, ActionEvent] = Pattern.begin(pattern, strategy).times(2)

    //设置时间特性
    val groupPatternWithTime: Pattern[ActionEvent, ActionEvent] = groupPattern.within(Time.seconds(20))

    val patternStream: PatternStream[ActionEvent] = CEP.pattern(midKeyByDStream, groupPatternWithTime)

   /* val alertDstream: DataStream[String] = patternStream.select { pMap =>
      pMap.mkString(",")

    }*/
   val alterDstream: DataStream[AlertEvent] = patternStream.flatSelect { (patternMap, collector: Collector[AlertEvent]) =>
     println(patternMap)
     val startEvents: Iterable[ActionEvent] = patternMap.getOrElse("start", null)
     if(startEvents!=null && startEvents.size>0){
       val startList: List[ActionEvent] = startEvents.toList
       val firstEvent: ActionEvent = startList(0)
       val uidSet: Set[String] = startList.map(_.uid).toSet
       if (uidSet.size == startList.size) {
         collector.collect(AlertEvent(firstEvent.mid, AlertType.CouponFraud, uidSet.mkString(","), firstEvent.ts))
       }
     }
   }


    val alertDstream: DataStream[String] = patternStream.select { pMap =>
      pMap.mkString(",")

    }


    alertDstream.print("alert:::")


    env.execute("job")

  }

}
