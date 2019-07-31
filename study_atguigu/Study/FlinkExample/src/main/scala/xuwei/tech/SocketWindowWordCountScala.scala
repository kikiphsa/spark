package xuwei.tech

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Create by chenqinping on 2019/7/27 18:44
  */
object SocketWindowWordCountScala {

  def main(args: Array[String]): Unit = {
    //获取socket端口号
    val port:Int=try{
      ParameterTool.fromArgs(args).getInt("port")
    }catch{
      case e:Exception=>{
      System.err.println("ni mei de")
      }
        9200
    }

    //获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //链接socket获取输入数据
    val work: DataStream[String] = env.socketTextStream("hadoop102",port,'\n')

    //解析数据(把数据打平)，分组，窗口计算，并且聚合求sum
    //注意：必须要添加这一行隐式转行，否则下面的flatmap方法执行会报错

    val wordCount: DataStream[WordCount] = work.flatMap(work => work.split(","))
      .map(x => WordCount(x, 1))
      .keyBy(x => x.word)
      .timeWindow(Time.seconds(2), Time.seconds(1))
      .sum("count")

    //打印到控制台
    wordCount.print().setParallelism(1)

    //执行任务
    env.execute("sjs ")
  }


  case class WordCount(word:String,count:Long)

}
