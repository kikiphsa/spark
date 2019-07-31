import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Create by chenqinping on 2019/6/12 9:45
  */
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object TrafficAnalysis {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(8)

    var stream = env.readTextFile(
      "C:\\D\\JAVA\\spark\\workbase\\spark\\Project\\UserBehaviorAnalysis\\NetWork\\src\\main\\resources\\apachetest.log"
    ).map(line => {
      val arr: Array[String] = line.split(" ")
      //定义时间传化
      val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val time: Long = simpleDateFormat.parse(arr(3)).getTime
      ApacheLogEvent(arr(0), arr(1), time, arr(5), arr(6))
    }) //水位线
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(10)) {
      override def extractTimestamp(element: ApacheLogEvent): Long = {
        element.eventTime
      }
    }).filter(_.method == "GET")
      .keyBy(_.url)
      .timeWindow(Time.minutes(1), Time.seconds(5))
      .aggregate(new CountAgg, new WindowResultFunction())
      .keyBy(_.windowEnd)
      .process(new TopNotUrl3(3))
      .print()

    env.execute("TrafficAnalysis")
  }

  class CountAgg extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1L

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  class WindowResultFunction extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      val url = key
      val count: Long = input.iterator.next()
      out.collect(UrlViewCount(url, window.getEnd, count))
    }
  }

  class TopNotUrl3(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {

    //直接定义状态
    lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(
      new ListStateDescriptor[UrlViewCount]("urlState", classOf[UrlViewCount]))


    override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
      urlState.add(value)

      ctx.timerService().registerEventTimeTimer(value.windowEnd * 1000 + 10 * 1000)
    }

    override def onTimer(timestamp: Long, context: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

      val allUrls: ListBuffer[UrlViewCount] = ListBuffer()

      import scala.collection.JavaConversions._

      for (url <- urlState.get()) {
        allUrls += url
      }

      //清除
      urlState.clear()


      val sortedUrls: ListBuffer[UrlViewCount] = allUrls.sortBy(_.count)(Ordering.Long.reverse).take(topSize)


      // 将排名数据格式化，便于打印输出
      val result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间：").append(new Timestamp(timestamp - 10 * 1000)).append("\n")


      for (i <- sortedUrls.indices) {
        val urlViewCount = sortedUrls(i)
        // 输出打印的格式 e.g.  No1：  商品ID=12224  浏览量=2413
        result.append("No").append(i + 1).append(":")
          .append("  Url=").append(urlViewCount.url)
          .append("  浏览量=").append(urlViewCount.count).append("\n")
      }
      result.append("====================================\n\n")
      // 控制输出频率
      Thread.sleep(1000)
      out.collect(result.toString)

    }
  }

}
