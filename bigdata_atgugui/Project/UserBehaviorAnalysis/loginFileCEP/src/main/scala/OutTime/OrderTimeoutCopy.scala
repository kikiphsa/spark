package OutTime

import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.cep.scala.{CEP, PatternStream}

/**
  * Create by chenqinping on 2019/6/14 10:46
  */
object OrderTimeoutCopy {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)


    //数据
    val orderEventStream = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(2, "pay", 1558430844)
    )).assignAscendingTimestamps(_.eventTime)
      .keyBy(_.orderId)


    val orderEventPattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin").where(_.eventType.equals("create"))
      .followedBy("follow").where(_.eventType.equals("pay")).within(Time.minutes(15))

    val outPutResult: OutputTag[OrderResult] = OutputTag[OrderResult]("outTimeResult")


    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream, orderEventPattern)

    import scala.collection.Map


    val patternDStream: DataStream[OrderResult] = patternStream.select(outPutResult)(
      (pattern: Map[String, Iterable[OrderEvent]], timout: Long) => {
        val outTimeOrderId: Long = pattern.getOrElse("begin", null).iterator.next().orderId

        OrderResult(outTimeOrderId, "TimeOut")
      }
    )(
      (pattern: Map[String, Iterable[OrderEvent]]) => {
        val payOrderId: Long = pattern.getOrElse("follow", null).iterator.next().orderId

        OrderResult(payOrderId, "success")
      }
    )


    patternDStream.print()

    patternDStream.getSideOutput(outPutResult).print()


    env.execute("OrderTimeoutCopy")
  }

}
