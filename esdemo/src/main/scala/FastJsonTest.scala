import java.sql.Timestamp

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.{SerializeConfig, SerializerFeature}
import org.json4s.NoTypeHints

import scala.collection.parallel.immutable

//import org.json4s.NoTypeHints
//
//import scala.collection.mutable
//import scala.collection.parallel.immutable
//
/**
 * todo
 *    1.json4s.JsonDSL 用的是scala 2.13版本，目前版本不适用
 */
object FastJsonTest {


  def json4sJson(): Unit = {
    var testMap = Map[String, Map[String, String]]()
    var subMap = Map[String, String]()
    subMap += ("1" -> "1.1")
    testMap += ("1" -> subMap)

    import org.json4s.jackson.Serialization._
    import org.json4s.jackson.Serialization
    implicit val formats = Serialization.formats(NoTypeHints)
    println(write(testMap))

  }

  def main(args: Array[String]): Unit = {

    //    val testMap = Map[String, String]()
    //    testMap += ("1" -> "2.034")
    //    testMap += ("2" -> "2.0134")
    //    println(scala.util.parsing.json.JSONObject(scala.collection.immutable.Map(testMap.toList: _*)))

    //scala解析json
    //    val text = "{\"name\":\"name1\", \"age\":55}"
    //    val json = JSON.parseObject(text)
    //    println(json.get("name"))
    //    println(json.get("age"))
    json4sJson()
    //    parseJson()
  }

  //  def jsonString(): Unit ={
  //    val testMap = Map[String, String]()
  //    testMap += ("1" -> "2.034")
  //    testMap += ("2" -> "2.0134")
  //    val jj = compact(render(testMap))
  //    println(jj)
  //  }
  //将要解析得数据
  case class NeedEntity(val vin: String,
                        val downoutput: Double,
                        val collectTime: Long,
                        val lon: Double,
                        val lat: Double,
                        val failureList: java.util.List[Integer] = new java.util.ArrayList[Integer]()
                       ) extends Serializable

  //管理状态
  //这是事件管理得 按照每个事件来处理


  class OverLimitEvent(var vin: String,
                       var startTime: Long,
                       var startLon: Double,
                       var startLat: Double,
                       var eventType: String = "overlimit",
                       var endTime: Long = 0,
                       var endLon: Double = 0.0,
                       var endLat: Double = 0.0,
                       var minValue: Double = 0.0,
                       var maxValue: Double = 0.0
                      ) extends Serializable {

    def getInsertMap(): Map[String, Any] = {
      Map(
        "vin" -> vin,
        "startTime" -> new Timestamp(startTime),
        "startLon" -> startLon,
        "startLat" -> startLat
      )
    }

    def getUpdateMap(): Map[String, Any] = {
      Map(
        "vin" -> vin,
        "startTime" -> new Timestamp(startTime),
        "endTime" -> new Timestamp(startTime),
        "endLon" -> startLon,
        "endLat" -> startLat,
        "maxValue" -> maxValue,
        "minValue" -> minValue
      )
    }

    def updateByEntity(entity: NeedEntity) = {
      this.endTime = entity.collectTime
      this.endLat = entity.lat
      this.endLon = entity.lon
      if (this.maxValue != null && this.maxValue < entity.downoutput) {
        this.maxValue = entity.downoutput
      }
      if (this.minValue != null && this.minValue > entity.downoutput) {
        this.minValue = entity.downoutput
      }

    }

    /**
     * 转成JSON串
     *
     * @return
     */
    override def toString(): String = {
      import org.json4s.jackson.Serialization._
      import org.json4s.jackson.Serialization
      implicit val formats = Serialization.formats(NoTypeHints)
      write(this)
    }
  }

  object OverLimitEvent {
    val ID_FIELD = Array("vin", "startTime")

    def apply(
               vin: String,
               startTime: Long,
               startLon: Double,
               startLat: Double,
               endTime: Long,
               endLon: Double,
               endLat: Double,
               minValue: Double,
               maxValue: Double
             ): OverLimitEvent = {
      val event = new OverLimitEvent(vin, startTime, startLon, startLat)
      event.endTime = endTime
      event.endLat = endLat
      event.endLon = endLon
      event.maxValue = maxValue
      event.minValue = minValue
      event
    }

    def buildByEntity(entity: NeedEntity): OverLimitEvent = {
      new OverLimitEvent(entity.vin, entity.collectTime, entity.lon, entity.lat)
    }

    def buildByJson(json: String): OverLimitEvent = {
      com.alibaba.fastjson.JSON.parseObject(json, classOf[OverLimitEvent])
    }

    override def toString(): String = {
      import org.json4s.jackson.Serialization._
      import org.json4s.jackson.Serialization
      implicit val formats = Serialization.formats(NoTypeHints)
      write(this)
    }

  }

  case class ExhaustAlarmStatus(val vin: String, var overLimitEvent: OverLimitEvent = null, var faultEvent: Map[String, OverLimitEvent] = null, var lastTime: Long) {
    override def toString(): String = {
      import org.json4s.jackson.Serialization._
      import org.json4s.jackson.Serialization
      implicit val formats = Serialization.formats(NoTypeHints)
      write(this)
    }
  }

  object ExhaustAlarmStatus {
    def buildByJson(json: String): ExhaustAlarmStatus = {
      if (json != null) {
        com.alibaba.fastjson.JSON.parseObject(json,
          classOf[ExhaustAlarmStatus])
      } else {
        null
      }
    }
  }

  def toJSON(state: ExhaustAlarmStatus): String = com.alibaba.fastjson.JSON.toJSONString(state, SerializerFeature.PrettyFormat)
  def toJSON1(state: ExhaustAlarmStatus): String = com.alibaba.fastjson.JSON.toJSONString(state, new SerializeConfig(true)) //针对对象进行序列化
  def toJSON2(state: ExhaustAlarmStatus): String = com.alibaba.fastjson.JSON.toJSONString(state, true)


  /**
   * 解析普通JSON
   */
  def parJson(): Unit = {
    val json = "{\"vin\":\"222\", \"OverLimitEvent\":{ \"vin\":\"222\",  \"startTime\":123456789, \"startLon\":1.0, \"startLat\":1.0, \"endTime\":123456789, \"endLon\":1.0, \"endLat\":1.0, \"minValue\":1.0, \"maxValue\":1.0 },\"lastTime\":1556441242000}";
    val state = com.alibaba.fastjson.JSON.parseObject(json,
      classOf[ExhaustAlarmStatus])
    println(state.overLimitEvent)
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    import org.json4s.jackson.Serialization._
    import org.json4s.jackson.Serialization
    implicit val formats = Serialization.formats(NoTypeHints)
    val jsonstr = write(state)
    println(jsonstr)
  }

  /**
   * 解析复杂Json串
   */
  def parseJson(): Unit = {
    val str2 = "{\"et\":\"kanqiu_client_join\",\"vtm\":1435898329434,\"body\":{\"client\":\"866963024862254\",\"client_type\":\"android\",\"room\":\"NBA_HOME\",\"gid\":\"\",\"type\":\"\",\"roomid\":\"\"},\"time\":1435898329}"
    val json = JSON.parseObject(str2)
    //获取成员
    val fet = json.get("et")
    //返回字符串成员
    val etString = json.getString("et")
    //返回整形成员
    val vtm = json.getInteger("vtm")
    println(vtm)
    //返回多级成员
    val client = json.getJSONObject("body").get("client")
    println(client)
  }


}




