package OutTime

import org.apache.flink.streaming.api.windowing.time.Time

import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.cep.scala.{CEP, PatternStream}

/**
  * Create by chenqinping on 2019/6/14 10:06
  */
case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)

case class OrderResult(orderId: Long, eventType: String)

object OrderTimeout {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val orderEventStream = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(2, "pay", 1558430844)
    )).assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.orderId)

    val orderEventpattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin").where(_.eventType.equals("create"))
      .followedBy("follow").where(_.eventType.equals("pay")).within(Time.minutes(15))


    //定义输出
    val orderTimeOutPut: OutputTag[OrderResult] = OutputTag[OrderResult]("orderTimeOut")

    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream, orderEventpattern)

    import scala.collection.Map
    //从patternStream获得输出流
    val completeResullt = patternStream.select(orderTimeOutPut)(
      //对于超时的调用 pattern timeout function
      (pattern: Map[String, Iterable[OrderEvent]], timestamp: Long) => {
        val timeoutOrderId: Long = pattern.getOrElse("begin", null).iterator.next().orderId

        OrderResult(timeoutOrderId, "timeout")
      }
    )(

      //匹配出来的  pattern select function
      (pattern: Map[String, Iterable[OrderEvent]]) => {
        val payOrderId: Long = pattern.getOrElse("follow", null).iterator.next().orderId

        OrderResult(payOrderId, "success")
      }
    )


    //获取匹配成功的
    completeResullt.print()


    //获取匹配超时的
    val timoutDStream: DataStream[OrderResult] = completeResullt.getSideOutput(orderTimeOutPut)

    timoutDStream.print()

    env.execute("OrderTimeout")

  }
}
