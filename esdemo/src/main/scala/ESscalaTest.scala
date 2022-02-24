import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.{GetRequest, GetResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.text.Text
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, QueryBuilders, RangeQueryBuilder}
import org.elasticsearch.index.reindex.UpdateByQueryRequest
import org.elasticsearch.script.{Script, ScriptType}
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.aggregations.bucket.terms.{ParsedStringTerms, ParsedTerms, Terms, TermsAggregationBuilder}
import org.elasticsearch.search.aggregations.metrics.{AvgAggregationBuilder, ParsedAvg}
import org.elasticsearch.search.aggregations.{AggregationBuilders, Aggregations, BucketOrder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.{HighlightBuilder, HighlightField}
import org.elasticsearch.search.sort.SortOrder
import org.json4s.NoTypeHints

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ESscalaTest {

  def main(args: Array[String]): Unit = {
    searchAgg()
    close()
  }

  /**
   * 聚合查询
       查询每位演员参演的电影的平均分，倒叙排序
   */
  def searchAgg()={
    val searchRequest = new SearchRequest("movie_index")
    val sourceBuilder = new SearchSourceBuilder()
    //分组
    val termsAggregationBuilder: TermsAggregationBuilder = AggregationBuilders
      .terms("groupByActorName")
      .size(10)
      .field("actorList.name.keyword")
      .order(BucketOrder.aggregation("avg_score", false))
    //avg
    val avgAggregationBuilder: AvgAggregationBuilder = AggregationBuilders.avg("avg_score").field("doubanScore")
    termsAggregationBuilder.subAggregation(avgAggregationBuilder)
    sourceBuilder.aggregation(termsAggregationBuilder)
    searchRequest.source(sourceBuilder)

    val searchResponse: SearchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT)
    val aggregations: Aggregations = searchResponse.getAggregations
    val parsedStringTerms: ParsedTerms = aggregations.get[ParsedTerms]("groupByActorName")
    val buckets: util.List[_ <: Terms.Bucket] = parsedStringTerms.getBuckets
    import  scala.collection.JavaConverters._
    for (bucket <- buckets.asScala) {
      val actorName: AnyRef = bucket.getKey
      val movieCount: Long = bucket.getDocCount
      val aggregations: Aggregations = bucket.getAggregations
      val avg_score: ParsedAvg = aggregations.get[ParsedAvg]("avg_score")
      val movieScore: Double = avg_score.getValue
      println(s"$actorName 共参演了 $movieCount 部电影, 平均评分为 $movieScore")
    }
  }


  /**
   * 条件查询
   *
   *  查询doubanScore>=5.0 关键词搜索red sea
   *  关键词高亮显示
   *  显示第一页，每页20条
   *  按doubanScore从大到小排序
   */
  def search()={
    val searchRequest = new SearchRequest("movie_index")
    val sourceBuilder = new SearchSourceBuilder()
    val boolQueryBuilder: BoolQueryBuilder = QueryBuilders.boolQuery()

    //filter
    val rangeQueryBuilder: RangeQueryBuilder = QueryBuilders.rangeQuery("doubanScore").gte(5.0)
    boolQueryBuilder.filter(rangeQueryBuilder)

    //must
    val matchQueryBuilder: MatchQueryBuilder = QueryBuilders.matchQuery("name", "行动")

    boolQueryBuilder.must(matchQueryBuilder)
    sourceBuilder.query(boolQueryBuilder)

    //高亮
    val highlightBuilder: HighlightBuilder =
      new HighlightBuilder().field("name")

    sourceBuilder.highlighter(highlightBuilder)

    //分页
    sourceBuilder.from(0)
    sourceBuilder.size(2)
    //排序
    sourceBuilder.sort("doubanScore",SortOrder.ASC)
    searchRequest.source(sourceBuilder)

    val searchResponse: SearchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT)
    //条数
    val value: Long = searchResponse.getHits.getTotalHits.value
    val hits: Array[SearchHit] = searchResponse.getHits.getHits
    for (hit <- hits) {
      val dataJson: String = hit.getSourceAsString
      println("数据:"+dataJson)
      val fields: util.Map[String, HighlightField] = hit.getHighlightFields
      import scala.collection.JavaConverters._
      for (field <- fields.asScala) {
        val fragments: Array[Text] = field._2.getFragments
        for (elem <- fragments) {
          println("高亮:"+elem.toString)
        }
      }
    }
  }




  /**
   *  查询
   */
  def get()={
    val getRequest = new GetRequest("movie_index", "001")
    val getResponse: GetResponse = esClient.get(getRequest, RequestOptions.DEFAULT)
    val results: String = getResponse.getSourceAsString
    println(results)
  }



  /**
   * 删除
   */
  def delete()={
    val deleteRequest = new DeleteRequest("movie_index", "002")
    esClient.delete(deleteRequest,RequestOptions.DEFAULT)
  }

  /**
   * 条件修改
   */
  def updateByQuery()={
    val updateByQueryRequest = new UpdateByQueryRequest("movie_index")

    //query
    val matchQueryBuilder = new MatchQueryBuilder("name", "懂")
    updateByQueryRequest.setQuery(matchQueryBuilder)

    //script
    val params = new util.HashMap[String, AnyRef]()
    params.put("newName","你好呀")
    val script =
      new Script(ScriptType.INLINE, "painless", "ctx._source['actorList'][0]['name']=params.newName", params)

    updateByQueryRequest.setScript(script)
    esClient.updateByQuery(updateByQueryRequest,RequestOptions.DEFAULT)
  }



  /**
   * 修改
   */
  def update() ={
    val updateRequest = new UpdateRequest("movie_index", "002")
    updateRequest.doc(XContentType.JSON,"name","懂")
    esClient.update(updateRequest,RequestOptions.DEFAULT)

  }



  /**
   * 批量新增(幂等)
   */
  def bulkPut()={

    val bulkRequest = new BulkRequest()

    val actor1: Actor = Actor("01", "啦啦啦")
    val actor2: Actor = Actor("01", "啦")
    val actors = List(actor1,actor2)
    val map: mutable.Map[String, Any] = mutable.Map()
    map+= ("id"->"001")
    map+=("name"->"天天行动")
    map+= ("doubanScore"->6.0D)
    map+= ("actorList"->actors)
    val map1: mutable.Map[String, Any] = mutable.Map()
    map1+= ("id"->"002")
    map1+=("name"->"行动")
    map1+= ("doubanScore"->5.0D)
    map1+= ("actorList"->actors)

    val listBuffer: ListBuffer[mutable.Map[String, Any]] =
      ListBuffer[mutable.Map[String, Any]](map,map1)
    import org.json4s.jackson.Serialization._
    import org.json4s.jackson.Serialization
    implicit val formats = Serialization.formats(NoTypeHints)
    for (map <- listBuffer) {
      val indexRequest = new IndexRequest("movie_index")
      val value: String =  map.getOrElse("id","1").asInstanceOf[String]

      indexRequest.id(value)

      indexRequest.source(write(map),XContentType.JSON)
      bulkRequest.add(indexRequest)
    }

    esClient.bulk(bulkRequest,RequestOptions.DEFAULT)
  }


  /**
   * 新增文档(非幂等)
   */
  def post(): Unit ={
    val indexRequest = new IndexRequest()
    //指定index名称
    indexRequest.index("movie_index")
    //指定数据
    val actor1: Actor = Actor("01", "啦啦啦")
    val actors = List(actor1)
    val map: mutable.Map[String, Any] = mutable.Map()
    map+= ("id"->"001")
    map+=("name"->"天天行动")
    map+= ("doubanScore"->6.0D)
    map+= ("actorList"->actors)

    import org.json4s.jackson.Serialization._
    import org.json4s.jackson.Serialization
    implicit val formats = Serialization.formats(NoTypeHints)
    val str: String = write(map)

    indexRequest.source(str,XContentType.JSON)
    esClient.index(indexRequest,RequestOptions.DEFAULT)
  }
  /**
   *  新建文档(幂等)
   *  todo
   *      比如有一个复杂对象：
   *        Map[Int, Map[Int, Double]]
   *      需要将其转为JSON保存，之后再读取使用，试了几种方法，最后的方案是：
   *      1、定义case class
   *      2、所有的数据类型都转为String（避免不必要的麻烦，至少Map的key都要为String，不然会报错scala.MatchError）
   *      3、Map必须是immutable.Map
   *
   */
  def put()={
    val indexRequest = new IndexRequest()
    //指定index名字
    indexRequest.index("movie_index")
    //指定数据
    val actor1: Actor = Actor("01", "水门桥")
    val actor2: Actor = Actor("02", "天猫")
    val actor3: Actor = Actor("03", "猫猫")
    val actors = List(actor1, actor2,actor3)
    val map: mutable.Map[String, Any] = mutable.Map()
    map+= ("id"->"001")
    map+=("name"->"天天行动")
    map+= ("doubanScore"->6.0D)
    map+= ("actorList"->actors)

    import org.json4s.jackson.Serialization._
    import org.json4s.jackson.Serialization
    implicit val formats = Serialization.formats(NoTypeHints)
    val str: String = write(map)

    indexRequest.source(str,XContentType.JSON)
    //指定id
    indexRequest.id("1001")
    esClient.index(indexRequest,RequestOptions.DEFAULT)
  }

  /**
   *  声明es客户端对象
   */
  def build(): RestHighLevelClient = {
    val restClientBuilder: RestClientBuilder = RestClient
      .builder(new HttpHost("hadoop102", 9200))

    val client = new RestHighLevelClient(restClientBuilder)
    client
  }

  /**
   * 关闭客户端
   */
  def close()={
    if (esClient!=null) {
      esClient.close()
    }
  }
  var esClient:RestHighLevelClient =build()


  case class Movie_Detail(id:String,name:String,doubanScore:Double,actorList:List[Actor]){}

  case class Actor(id:String ,name:String)
}
