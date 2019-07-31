package copy

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.util.{Date, Properties}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

/**
  * Create by chenqinping on 2019/6/12 8:55
  */
object TwoHotItems {
  //      "C:\\D\\JAVA\\spark\\workbase\\spark\\Project\\UserBehaviorAnalysis\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv"
  def main(args: Array[String]): Unit = {
    //配置env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")


    //设置EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置并发
    env.setParallelism(1)


    val stream =
      env.readTextFile(
        "C:\\D\\JAVA\\spark\\workbase\\spark\\Project\\UserBehaviorAnalysis\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv"
      )
        /*  env
            .addSource(new FlinkKafkaConsumer011[String]("hotitems", new SimpleStringSchema(), properties))*/
        .map(line => {
        val arr: Array[String] = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      }).assignAscendingTimestamps(_.timestamp * 1000)
        .filter(_.behavior == "pv")
        .keyBy("itemId")
        .timeWindow(Time.hours(1), Time.minutes(5))
        .aggregate(new CountAgg(), new WindowResultFunction())
        .keyBy("windowEnd")
        .process(new TopN3(3))
        .print()

    env.execute("job")


  }

  class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1L

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  //  自定义实现Window Function，输出ItemViewCount格式
  class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0

      val count: Long = input.iterator.next()

      out.collect(ItemViewCount(itemId, window.getEnd, count))
    }
  }

  class TopN3(pageSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
    // 定义状态ListState
    private var itemState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      // 命名状态变量的名字和类型
      val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemState", classOf[ItemViewCount])
      itemState = getRuntimeContext.getListState(itemStateDesc)
    }

    override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
      itemState.add(i)
      // 注册定时器，触发时间定为 windowEnd + 1，触发时说明window已经收集完成所有数据
      context.timerService.registerEventTimeTimer(i.windowEnd + 1)
    }

    // 定时器触发操作，从state里取出所有数据，排序取TopN，输出
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 获取所有的商品点击信息
      val allItems: ListBuffer[ItemViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for (item <- itemState.get) {
        allItems += item
      }
      // 清除状态中的数据，释放空间
      itemState.clear()

      // 按照点击量从大到小排序，选取TopN
      val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(pageSize)

      // 将排名数据格式化，便于打印输出
      val result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      for (i <- sortedItems.indices) {
        val currentItem: ItemViewCount = sortedItems(i)
        // 输出打印的格式 e.g.  No1：  商品ID=12224  浏览量=2413
        result.append("No").append(i + 1).append(":")
          .append("  商品ID=").append(currentItem.itemId)
          .append("  浏览量=").append(currentItem.count).append("\n")

      }
      val listBuffer = new ListBuffer[ItemViewCount]

      for (i <- sortedItems.indices) {
        val currentItem: ItemViewCount = sortedItems(i)


        val format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date = new Date()
        val str: String = format.format(date)
        val key = str


        val buffer = new StringBuffer()
        buffer.append(currentItem.itemId.toString).append(",")
        buffer.append(currentItem.windowEnd.toString).append(",")
        buffer.append(currentItem.count.toString)

        listBuffer += currentItem

        jedis.sadd(key, buffer.toString)
      }

      MyEsUtil.executeIndexBulk("two_hoe",listBuffer.toList)

      result.append("====================================\n\n")



      // 控制输出频率
      Thread.sleep(1000)
      out.collect(listBuffer.toList.toString())
    }
  }

}
