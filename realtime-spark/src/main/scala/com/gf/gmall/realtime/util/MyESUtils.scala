package com.gf.gmall.realtime.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializeConfig
import com.gf.gmall.realtime.bean.DauInfo
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.collection.Searching.SearchResult
import scala.collection.mutable.ListBuffer

/**
 * ES工具类
 */
object MyESUtils {

  def main(args: Array[String]): Unit = {
    val midList: List[String] = searchField("gmall_dau_info_2022-02-21", "mid")
    println(midList.size)
    close()
  }

  /**
   *  从es中查找当前的所有，存在返回索引List
   * @param indexName
   * @param field
   * @return
   */
  def searchField(indexName: String, field: String): List[String] = {
    val getIndexRequest = new GetIndexRequest(indexName)
    val bool: Boolean =
      esClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT)
    if (!bool) {
      return null
    }
    //查询指定字段所有数据
    val searchRequest = new SearchRequest(indexName)
    val sourceBuilder = new SearchSourceBuilder()
    sourceBuilder.fetchSource(field,null).size(1000)
    searchRequest.source(sourceBuilder)
    val response: SearchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT)

    //处理结果
    val midList: ListBuffer[String] = ListBuffer[String]()
    val hits: Array[SearchHit] = response.getHits.getHits
    for (hit <- hits) {
      val sourceJson: String = hit.getSourceAsString
      val sourceJsonObj: JSONObject = JSON.parseObject(sourceJson)
      val mid: String = sourceJsonObj.getString("mid")
      midList.append(mid)
    }
    midList.toList
  }

  /**
   * 批量写 (幂等)
   * @param docList
   * @param indexName
   * @return
   */
  def save(docList: List[(String, AnyRef)], indexName: String) = {

    val bulkRequest = new BulkRequest()
    for (doc <- docList) {
      val indexRequest = new IndexRequest()
      indexRequest.index(indexName)
      indexRequest.id(doc._1)
      indexRequest.source(JSON.toJSONString(doc._2,new SerializeConfig(true)),XContentType.JSON)
      bulkRequest.add(indexRequest)
    }
    esClient.bulk(bulkRequest,RequestOptions.DEFAULT)

  }
  /**
   *  关闭客户端
   */
  def close()={
    if (esClient!=null) {
      esClient.close()
    }
  }

  /**
   * 创建ES客户端
   *
   * @return
   */
  def build(): RestHighLevelClient = {

    val httpHost =
      new HttpHost(PropertiesUtils(ConfigUtil.ES_HOST), PropertiesUtils(ConfigUtil.ES_PORT).toInt)
    val restClientBuilder: RestClientBuilder = RestClient.builder(httpHost)
    val client = new RestHighLevelClient(restClientBuilder)
    client

  }

  /**
   * ES客户端对象
   */
  var esClient: RestHighLevelClient = build()
}
