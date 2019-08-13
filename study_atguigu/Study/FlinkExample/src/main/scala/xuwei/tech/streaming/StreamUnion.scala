package xuwei.tech.streaming

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import xuwei.tech.custormSource.{MyNoParalleSource, MyParallelSourceScala}
import org.apache.flink.streaming.api.windowing.time.Time

object StreamUnion {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    import org.apache.flink.api.scala._

    val text1 = env.addSource(new MyNoParalleSource)

    val text2 = env.addSource(new MyNoParalleSource)

    val union = text1.union(text2)

    val unionAll = union.map(line => {
      println("数据" + line)
      line
    }).timeWindowAll(Time.seconds(2)).sum(0)

    unionAll.print().setParallelism(1)

    env.execute("jsjsj")

  }

}
