package xuwei.tech.streaming

import java.{lang, util}

import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import xuwei.tech.custormSource.MyParallelSourceScala

object StreamSplit {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val source = env.addSource(new MyParallelSourceScala)

    val splitList = source.split(new OutputSelector[Long] {
      override def select(value: Long): lang.Iterable[String] = {
        val list = new util.ArrayList[String]()

        if (value % 2 == 0) {
          list.add("even")
        } else {
          list.add("end")
        }
        list
      }
    })
    val select = splitList.select("even")

    select.print().setParallelism(1)

    env.execute("ss")
  }

}
