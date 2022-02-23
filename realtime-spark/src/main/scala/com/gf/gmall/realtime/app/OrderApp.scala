package com.gf.gmall.realtime.app

import java.time.{LocalDate, Period}
import java.util

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.gf.gmall.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.gf.gmall.realtime.util.{DateUtils, MyESUtils, MyOffsetUtils, MyRedisUtils, MykafkaUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * 订单宽表
 * 0. 准备环境
 * 1. 从redis中读取offset  * 2(order_inf,order_detail)
 * 2. 接收Kafka的数据 * 2
 * 3. 提取偏移量结束点 * 2
 *
 * 4. 处理数据
 *    4.1 转换结构
 *    4.2 补充维度
 *    4.3 双流join
 *
 * 5. 写入ES
 * 6. 提交offset
 */
object OrderApp {
  def main(args: Array[String]): Unit = {
    //0.环境准备
    val sparkConf: SparkConf = new SparkConf().setAppName("order_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //1.从redis中读取偏移量
    val dwdOrderInfoTopic = "DWD_ORDER_INFO_I"
    val dwdOrderDetailTopic = "DWD_ORDER_DETAIL_I"
    val dwdOrderInfoGroup = "dwd_order_info_group"
    val dwdOrderDetailGroup = "dwd_order_detail_group"

    val dwdOrderInfoOffsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffsets(dwdOrderInfoTopic, dwdOrderInfoGroup)
    val dwdOrderDetailOffsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffsets(dwdOrderDetailTopic, dwdOrderDetailGroup)

    //2.从kafka接收数据
    var orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (dwdOrderInfoOffsets != null && dwdOrderInfoOffsets.nonEmpty) {
      orderInfoKafkaDStream = MykafkaUtils.getKafkaDStream(ssc, dwdOrderInfoTopic, dwdOrderInfoGroup, dwdOrderInfoOffsets)
    } else {
      orderInfoKafkaDStream = MykafkaUtils.getKafkaDStream(ssc, dwdOrderInfoTopic, dwdOrderInfoGroup)
    }
    var orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (dwdOrderDetailOffsets != null && dwdOrderDetailOffsets.nonEmpty) {
      orderDetailKafkaDStream = MykafkaUtils.getKafkaDStream(ssc, dwdOrderDetailTopic, dwdOrderDetailGroup, dwdOrderDetailOffsets)
    } else {
      orderDetailKafkaDStream = MykafkaUtils.getKafkaDStream(ssc, dwdOrderDetailTopic, dwdOrderDetailGroup)
    }
    //orderInfoKafkaDStream.print(10)
   //orderDetailKafkaDStream.print(10)

    //3.提取偏移量
    var orderInfoOffsetRanges: Array[OffsetRange]=null
    var orderDetailOffsetRanges: Array[OffsetRange]=null

    val orderInfoKafkaOffsetsDStream: DStream[ConsumerRecord[String, String]] = orderInfoKafkaDStream.transform(
      rdd => {
         orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    val orderDetailKafkaOffsetsDStream: DStream[ConsumerRecord[String, String]] = orderDetailKafkaDStream.transform(
      rdd => {
         orderDetailOffsetRanges  = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    //4.处理数据
    //4.1转换数据
    val OrderInfoDStream: DStream[OrderInfo] = orderInfoKafkaOffsetsDStream.map(
      record => JSON.parseObject(record.value(), classOf[OrderInfo])
    )

    val orderDetailDStream: DStream[OrderDetail] = orderDetailKafkaOffsetsDStream.map(

      record => JSON.parseObject(record.value(), classOf[OrderDetail])
    )
//    OrderInfoDStream.print(20)
//    orderDetailDStream.print(20)

    //4.2维度关联
    val dimOrderInfoDStream: DStream[OrderInfo] = OrderInfoDStream.mapPartitions(
      orderInfoIter => {

        val jedis: Jedis = MyRedisUtils.getJedis()
        val orderInfoResultList: ListBuffer[OrderInfo] = ListBuffer[OrderInfo]()
        for (orderInfo <- orderInfoIter) {

          //补充用户维度信息
          val userInfoKey = s"DIM:USER_INFO:${orderInfo.user_id}"
          val userInfoJson: String = jedis.get(userInfoKey)
          val userInfoJsonObj: JSONObject = JSON.parseObject(userInfoJson)
          //获取性别
          orderInfo.user_gender = userInfoJsonObj.getString("gender")

          val birthday: String = userInfoJsonObj.getString("birthday")
          val birthdayDate: LocalDate = LocalDate.parse(birthday)
          val nowdate: LocalDate = LocalDate.now()
          val period: Period = Period.between(birthdayDate, nowdate)
          orderInfo.user_age = period.getYears

          //处理日期
          val create_time: String = orderInfo.create_time
          val dateArr: Array[String] = create_time.split(" ")

          orderInfo.create_date = dateArr(0)
          orderInfo.create_hour = dateArr(1).split(":")(0)
          //地区维度
          val province_id: Long = orderInfo.province_id
          val dimProvinceKey = s"DIM:BASE_PROVINCE:${province_id}"
          val provinceJson: String = jedis.get(dimProvinceKey)
          val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)

          orderInfo.province_name = provinceJsonObj.getString("name")
          orderInfo.province_area_code = provinceJsonObj.getString("area_code")
          orderInfo.province_iso_code = provinceJsonObj.getString("iso_code")
          orderInfo.province_iso_3166_2 = provinceJsonObj.getString("iso_3166_2")

          orderInfoResultList.append(orderInfo)
        }

        orderInfoResultList.iterator
      }
    )
    //    dimOrderInfoDStream.print(100)
    //    orderDetailDStream.print(100)
    //4.3双流join
    // OrderInfo   => (id, OrderInfo)
    val orderInfoKVDStream: DStream[(Long, OrderInfo)] = dimOrderInfoDStream.map(
      orderInfo => (orderInfo.id, orderInfo)
    )
    // OrderDetail => (order_id, OrderDetail)
    val OrderDetailKVDStream: DStream[(Long, OrderDetail)] = orderDetailDStream.map(
      orderDetail => (orderDetail.order_id, orderDetail)
    )
//    orderInfoKVDStream.print(10)
//
//    OrderDetailKVDStream.print(10)
    // 使用哪种join方式?
    // join (innerJoin) :
    // leftOuterJoin:
    // rightOuterJoin:
    // fullOuterJoin :

    //如果数据会在同一个批次中.那么可以用join方式
    //val orderJoinDStream: DStream[(Long, (OrderInfo, OrderDetail))] =
    //    orderInfoKVDStream.join(orderDetailKVDStream)

    //orderJoinDStream.print(100000)
    /*
       todo
        流join中可能出现的问题 ?   因为延迟，导致数据丢失
          实际情况是数据可能在同一批次，也可能不在同一个批次
          同批次的双流的数据处理
          不同批次的双流的数据处理
          如何解决?
            1. 调整批次大小
            2. 窗口
            3. 通过第三方组件维护数据状态  √
            主表在，从表在 -> join成功
            主表在，从表不在
               主表写缓存 (主先到)
               主表查缓存 (主后到)
            主表不在 ， 从表在
              从表写缓存 (从先到)
              从表读缓存 (从后到)
     */
    //实际情况是数据可能在同一批次，也可能不在同一个批次
    val orderJoinDStream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] =
    orderInfoKVDStream.fullOuterJoin( OrderDetailKVDStream)
//    orderJoinDStream.print(20)


    val orderWideDStream: DStream[OrderWide] = orderJoinDStream.flatMap {
      case (key, (orderInfoOP, orderDetailOP)) => {
        val orderWideList: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
        val jedis: Jedis = MyRedisUtils.getJedis()
        //1.主表在
        if (orderInfoOP != None) {
          val orderInfo: OrderInfo = orderInfoOP.get
          //从表在(主从都在)
          if (orderDetailOP != None) {
            val orderDetail: OrderDetail = orderDetailOP.get
            //宽表拼接  orderInfo和orderDetail
            val orderWide = new OrderWide(orderInfo, orderDetail)
            orderWideList.append(orderWide)
          }
          //从表不在
          //存主表
          /*
                todo
                    主表写缓存
                    type:    string
                    key :    ORDERJOIN:ORDER_INFO:[ID]
                    value :  orderInfoJson
                    写入API: set  / setex
                    读取API: get
                    过期时间: 1天
             */
          val orderJoinOrderInfoKey = s"ORDERJOIN:ORDER_INFO:${orderInfo.id}"
          //orderInfo-->orderInfo json字符串
          val orderJoinOrderInfoValue: String =
            JSON.toJSONString(orderInfo, new SerializeConfig(true))

          jedis.setex(orderJoinOrderInfoKey, DateUtils.getSecondsNextEarlyMorning.intValue(), orderJoinOrderInfoValue)

          //读从表
          /**
           * todo
           * 主表读缓存
           * type :    set
           * key  :    ORDERJOIN:ORDER_DETAIL:[order_id]
           * value :   ordeDetailJson ...
           * 写入API:  sadd
           * 读取API:  smembers
           * 过期时间: 1天
           * Join完成后，删除从表的缓存 ,是否可行?
           */
          val orderJoinOrderDetailKey = s"ORDERJOIN:ORDER_TETAIL:${orderInfo.id}"
          val orderDetails: util.Set[String] = jedis.smembers(orderJoinOrderDetailKey)
          import scala.collection.JavaConverters._
          if (orderDetails != null && orderDetails.size() > 0) {
            for (orderDetailJson <- orderDetails.asScala) {
              val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
              val orderWide = new OrderWide(orderInfo, orderDetail)
              orderWideList.append(orderWide)
            }
          }
          //主表不在
        } else {
          //从表在, 从redis读数据
          val orderDetail: OrderDetail = orderDetailOP.get

          val orderJoinOrderInfoKey = s"ORDERJOIN:ORDER_INFO:${orderDetail.order_id}"

          val orderInfoJson: String = jedis.get(orderJoinOrderInfoKey)

          if (orderInfoJson != null && orderInfoJson.size > 0) {
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            //宽表拼接  orderInfo和orderDetail
            val orderWide = new OrderWide(orderInfo, orderDetail)

            orderWideList.append(orderWide)
          }

          //写数据，写从表到redis
          val orderJoinOrderDetailKey = s"ORDERJOIN:ORDER_DETAIL:${orderDetail.order_id}"
          val orderJoinOrderDetailValue: String = JSON.toJSONString(orderDetail, new SerializeConfig(true))
          jedis.sadd(orderJoinOrderDetailKey, orderJoinOrderDetailValue)
          jedis.expire(orderJoinOrderDetailKey, DateUtils.getSecondsNextEarlyMorning.intValue())
        }
        jedis.close()
        orderWideList
      }
    }

    //测试
//  orderWideDStream.print(100)
    //写入es
    orderWideDStream.foreachRDD(
      rdd=>{
        rdd.foreachPartition(
          orderWideIter=>{
            val orderWideList: List[(String, OrderWide)] =
              orderWideIter.map(orderWide => (orderWide.detail_id.toString, orderWide)).toList
            if (orderWideList.size>0) {
              val orderWideT: (String, OrderWide) = orderWideList.head
              val dt: String = orderWideT._2.create_date
              val indexName:String =s"gmall_order_wide_$dt"
              MyESUtils.save(orderWideList,indexName)
            }
          }
        )
        //提交到offset
        MyOffsetUtils.writeOffset(dwdOrderInfoTopic,dwdOrderInfoGroup,orderInfoOffsetRanges)
        MyOffsetUtils.writeOffset(dwdOrderDetailTopic,dwdOrderDetailGroup,orderDetailOffsetRanges)
      }
    )

    ssc.start
    ssc.awaitTermination()
  }
}
