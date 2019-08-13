package xuwei.tech.streaming

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import xuwei.tech.custormSource.{MyNoParalleSource, MyParallelSourceScala}

object StreamConnect {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    import org.apache.flink.api.scala._

    val text1 = env.addSource(new MyParallelSourceScala)
    val text2 = env.addSource(new MyParallelSourceScala)

    val text_str = text2.map("str" + _)

    val connect = text1.connect(text_str)

    val sum = connect.map(line1=>{line1},line2=>{line2})

    sum.print().setParallelism(1)

    env.execute("ss")
  }

}
