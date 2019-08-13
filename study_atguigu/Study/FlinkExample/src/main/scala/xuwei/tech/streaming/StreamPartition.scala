package xuwei.tech.streaming

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import xuwei.tech.custormSource.MyNoParalleSource

object StreamPartition {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    import org.apache.flink.api.scala._

    val sopurce = env.addSource(new MyNoParalleSource)

    val tuple = sopurce.map(line => {
      Tuple1(line)
    })

    val partition = tuple.partitionCustom(new PartitionSource(),0)

    val lines = partition.map(line => {
      println("id: " + Thread.currentThread().getId, "value: " + line)
      line._1
    })
    lines.print().setParallelism(1)
    env.execute("ss")
  }
}