import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.cep.scala.{CEP, PatternStream}

/**
  * Create by chenqinping on 2019/6/14 9:12
  */

object LogInFallCEP {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val loginEventStream = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.userId)









    //定义Patter

    val loginEventPattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail").within(Time.seconds(2))

    //在KeyBy之后的流中,匹配定义好的事件流
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream, loginEventPattern)


    import scala.collection.Map

    //从patternStream后去想要的数据
    val loginFailDataStream = patternStream.select(
      (pattern: Map[String, Iterable[LoginEvent]]) => {
        val next: LoginEvent = pattern.getOrElse("next", null).iterator.next()
        (next.userId, next.ip, next.eventType)
      }
    ).print()

    env.execute("LogInFallCEP")
  }

}
