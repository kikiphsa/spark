import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Create by chenqinping on 2019/6/12 10:53
  */


case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

case class WaringInfo(userId:Long)
object LoginFail {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(8)

    val loginEventStream = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    )).assignAscendingTimestamps(_.eventTime * 1000)
      .filter(_.eventType == "fail")
      .keyBy(_.userId)
      .process(new MathFunction())
      .print()
    env.execute("LoginFail")
  }

  class MathFunction extends KeyedProcessFunction[Long, LoginEvent, LoginEvent] {
    //直接定义状态
    lazy val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(
      new ListStateDescriptor[LoginEvent]("urlState", classOf[LoginEvent]))


    override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#Context, out: Collector[LoginEvent]): Unit = {
      loginState.add(value)

      ctx.timerService().registerEventTimeTimer(value.eventTime + 2 * 1000)
    }


    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#OnTimerContext, out: Collector[LoginEvent]): Unit = {
      val arrLogin: ListBuffer[LoginEvent] = ListBuffer()

      import scala.collection.JavaConversions._

      for (login <- loginState.get()) {
        arrLogin += login
      }

      loginState.clear()

      if (arrLogin.length > 1) {
        out.collect(arrLogin.head)
      }
    }
  }

}
