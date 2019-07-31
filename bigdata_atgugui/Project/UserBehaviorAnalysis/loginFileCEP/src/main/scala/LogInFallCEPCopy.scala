import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.cep.scala.{CEP, PatternStream}

/**
  * Create by chenqinping on 2019/6/14 9:42
  */
object LogInFallCEPCopy {

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
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.userId)

    //pattern
    val pattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin").where(_.eventType.equals("fail"))
      .next("next").where(_.eventType.equals("fail")).within(Time.seconds(2))

    val patternSteram: PatternStream[LoginEvent] = CEP.pattern(loginEventStream, pattern)

    import scala.collection.Map
    patternSteram.select(
      (patter: Map[String, Iterable[LoginEvent]]) => {
        val next: LoginEvent = patter.getOrElse("next",null).iterator.next()
        (next.userId, next.ip, next.eventType)
      }
    ).print()

    env.execute("LogInFallCEPCopy")
  }

}
