package xuwei.tech.sink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisClusterConfig, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


/**
  * Create by chenqinping on 2019/8/15 20:58
  */
object StreamingDataToRedisScala {

  def main(args: Array[String]): Unit = {
    //获取socket端口号
    val port = 9000

    //获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //链接socket获取输入数据
    val text: DataStream[String] = env.socketTextStream("hadoop102",port,'\n')

    //注意：必须要添加这一行隐式转行，否则下面的flatmap方法执行会报错
    import org.apache.flink.api._

    val word: DataStream[(String, String)] = text.map(line=>("1_word",line))

    //连接redis
    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build()


    new  RedisSink[Tuple2[String,String]](config,new MyRedisMapper)

  }


  class MyRedisMapper extends RedisMapper[Tuple2[String,String]] {
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.LPUSH)
    }

    override def getKeyFromData(data: (String, String)): String = {
      data._1
    }

    override def getValueFromData(data: (String, String)): String = {
      data._2
    }
  }
}
