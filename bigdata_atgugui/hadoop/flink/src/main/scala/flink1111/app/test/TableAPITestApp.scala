package flink1111.app.test

import com.alibaba.fastjson.JSON
import com.atguigu.flink1111.bean.StartupLog
import com.atguigu.flink1111.util.MyKafkaUtil
import org.apache.flink.streaming.api.scala.{AllWindowedStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.api.scala._

object TableAPITestApp {

  //每10秒中渠道为appstore的个数
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val myKafkaConsumer: FlinkKafkaConsumer011[String] = MyKafkaUtil.getConsumer("GMALL_STARTUP")
    val dstream: DataStream[String] = env.addSource(myKafkaConsumer)

    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

    val startupLogDstream: DataStream[StartupLog] = dstream.map{ jsonString =>JSON.parseObject(jsonString,classOf[StartupLog]) }



    val startupLogWithEtDstream: DataStream[StartupLog] = startupLogDstream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[StartupLog](Time.milliseconds(0L)) {
      override def extractTimestamp(element: StartupLog): Long = element.ts
    })

    //每10秒中渠道为appstore的个数
    val startupLogTable: Table = tableEnv.fromDataStream(startupLogWithEtDstream,'mid,'uid,'appid,'area,'os,'ch,'logType,'vs,'logDate,'logHour,'logHourMinute,'ts.rowtime)

  //  val table: Table = startupLogTable.filter("ch ='appstore'").window(Tumble over 10000.millis on 'ts as 'tt).groupBy('ch ,'tt).select("ch,ch.count ")

    val table2: Table = tableEnv.sqlQuery("select ch, count(ch) ct from "+startupLogTable+" where ch='appstore'  group by TUMBLE(ts, INTERVAL '10' SECOND), ch   ")

    val rDstream: DataStream[(Boolean, (String, Long))] = table2.toRetractStream[(String,Long)]

    rDstream.filter(_._1).print()


    env.execute()
  }

}
