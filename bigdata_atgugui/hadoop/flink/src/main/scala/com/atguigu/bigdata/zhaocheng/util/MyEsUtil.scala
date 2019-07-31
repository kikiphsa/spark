package com.atguigu.bigdata.zhaocheng.util


import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
/**
  * Create by chenqinping on 2019/6/6 16:12
  */
object MyEsUtil {

  val httpHosts = new util.ArrayList[HttpHost]
  httpHosts.add(new HttpHost("hadoop102", 9200, "http"))
  httpHosts.add(new HttpHost("hadoop103", 9200, "http"))
  httpHosts.add(new HttpHost("hadoop104", 9200, "http"))


  def getEsSink(indexName: String): ElasticsearchSink[String] = {


    val esSinkFunc = new ElasticsearchSinkFunction[String] {
      override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
       /* val jSONObject = org.json4s.native.Serialization.write(element)
        val indexRequest: IndexRequest = Requests.indexRequest().index(indexName).`type`("_doc").source(jSONObject)
        indexer.add(indexRequest)*/
      }
    }

    val esSinkBuilder = new ElasticsearchSink.Builder[String](httpHosts, esSinkFunc)

    esSinkBuilder.setBulkFlushMaxActions(10)

    val esSink: ElasticsearchSink[String] = esSinkBuilder.build()

    esSink
  }

}
