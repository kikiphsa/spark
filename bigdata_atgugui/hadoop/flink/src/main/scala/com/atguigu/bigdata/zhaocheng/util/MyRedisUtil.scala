package com.atguigu.bigdata.zhaocheng.util

import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper._

/**
  * Create by chenqinping on 2019/6/6 15:34
  */
object MyRedisUtil {

  val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build()

  def getRedisSink(): RedisSink[(String, String)] = {

    new RedisSink[(String, String)](conf, new MyRedisMapper)
  }

  class MyRedisMapper extends RedisMapper[(String, String)] {
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET, "channel_count")
    }

    override def getKeyFromData(t: (String, String)): String = t._2

    override def getValueFromData(t: (String, String)): String = t._1

  }


}
