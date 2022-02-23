package com.gf.gmall.realtime.app

import java.lang

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.gf.gmall.realtime.bean.{ActionLog, DisplayLog, PageLog, StartLog}
import com.gf.gmall.realtime.util.{MyOffsetUtils, MykafkaUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
 * todo
 *  日志数据消费分流  ODS -> DWD
 *    0. 创建实时环境
 *    1. 从kafka中读取数据
 *    2. 处理数据
 *        2.1 转换数据结构
 *  通用结构( Map 、 JSONObject )   or 专用结构（Bean）
 *        2.2 分流
 *  分流错误数据   DWD_ERROR_LOG_TOPIC
 *  分流页面数据   DWD_PAGE_LOG_TOPIC
 *  分流事件数据   DWD_PAGE_ACTION_LOG_TOPIC
 *  分流曝光数据   DWD_PAGE_DISPLAY_LOG_TOPIC
 *  分流启动数据   DWD_START_LOG_TOPIC
 *    3. 将数据写入到下一层
 */
object BaseLogApp {
  def main(args: Array[String]): Unit = {
    //0.创建环境
    val sparkConf: SparkConf = new SparkConf().setAppName("base_log_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //1.从kafka读取数据
    //定义要消费的topic
    val ods_topic: String = "ODS_BASE_LOG"
    //定义消费者组
    val ods_groupId: String = "ods_base_log_group"
    // todo 读取redis中的 offset
    val topicPartitionOffsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffsets(ods_topic, ods_groupId)
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null

    if (topicPartitionOffsets != null && topicPartitionOffsets.nonEmpty) {
      kafkaDStream = MykafkaUtils.getKafkaDStream(ssc, ods_topic, ods_groupId, topicPartitionOffsets)
    } else {
      //todo 未指定offset时,根据scc,topic,消费者组 生成kafkaDStream
      kafkaDStream =
        MykafkaUtils.getKafkaDStream(ssc, ods_topic, ods_groupId)
    }

    //todo 提取偏移量,不对数据进行处理
    var offsetRanges: Array[OffsetRange] = null
    /*
      todo
        把周期范围内的kafkaDStream中的rdd都转换为offsetRanges类型
     */
    val kafkaOffsetDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        /*
            todo HasOffsetRanges
                表示一个对象，它有一个offsetrange的集合,可以用来访问直接由Kafka DStream生成的rdd中的偏移范围,即上一次记录的offset位置
                (例如，一个Kafka topic分区的偏移量范围)
                其实将rdd转换成HasOffsetRanges类型的一个DStream的rdd偏移量范围
                offsetRanges包含主题、分区和偏移量
                注意：用此rdd.asInstanceOf[HasOffsetRanges].offsetRanges方法时，必须在DStream逻辑处理之前
         */
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    //todo 基础环境数据测试
    //    kafkaDStream.map((record:ConsumerRecord[String,String])=>{record.value()})
    //    val valueDStream: DStream[String] = kafkaDStream.map(_.value())
    //    valueDStream.print(20)
    //2.处理数据
    //2.1结构转换,把消费到的kafka字符串数据转换成json类型
    val valueDStream: DStream[JSONObject] = kafkaOffsetDStream.map(
      record => JSON.parseObject(record.value()))

    //todo 此时的valueDStream数据分为5中业务类型，分别定义这5种类型的DWD

    //2.2分流 -->根据数据的业务类型对数据分流
    val DWD_ERROR_LOG_TOPIC: String = "DWD_ERROR_LOG_TOPIC"
    val DWD_PAGE_LOG_TOPIC: String = "DWD_PAGE_LOG_TOPIC"
    val DWD_PAGE_ACTION_LOG_TOPIC: String = "DWD_PAGE_ACTION_LOG_TOPIC"
    val DWD_PAGE_DISPLAY_LOG_TOPIC: String = "DWD_PAGE_DISPLAY_LOG_TOPIC"
    val DWD_START_LOG_TOPIC: String = "DWD_START_LOG_TOPIC"

    /**
     * todo foreach和foreachPartition区别：
     *    1.foreach 是action算子 是针对于RDD的每个元素来操作的，
     *      每次foreach得到的一个rdd的kv实例,也就是具体的数据.
     *    2.foreachPartition,是action算子,将函数func应用于此RDD的每个分区
     *      functionPartition中函数处理的是分区迭代器,而非具体的数据
     *      是针对于RDD的每个分区进行操作的
     *      从优化层面讲：foreachPartition用于存储大量结果数据的场景
     *
     * todo 提交偏移量的位置:
     * 代码的执行位置:  driver  executor
     * 代码的执行频率:  一次   一批次一次  一条数据一次
     *    1. foreachRDD的外边   driver    一次
     *    2. foreachRDD里面     driver   一批次一次
     *    3. foreach里面        executor  一条数据一次(算子rdd内都是Executor)
     *    4. foreachPartition   executor  一批次一分区
     * todo 注意：
     * 连接对象不允许序列化，即不适合在Executor中执行，比如Jedis,JdbcConnect等...
     */
    valueDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          jsonObjIter => {
            for (jsonObj <- jsonObjIter) {
              //每个jsonObj 对应的是不同业务类型对数据,如actions,common,err,displays,page,ts
              //这里对错误信息进行分流，错误信息和其他通用信息是取反的
              val errObj: JSONObject = jsonObj.getJSONObject("err")
              if (errObj != null) {
                MykafkaUtils.send(DWD_ERROR_LOG_TOPIC, errObj.toJSONString)
              } else {
                //todo 提取公共字段common
                val commonObj: JSONObject = jsonObj.getJSONObject("common")
                val ar: String = commonObj.getString("ar")
                val ba: String = commonObj.getString("ba")
                val ch: String = commonObj.getString("ch")
                val is_new: String = commonObj.getString("is_new")
                val md: String = commonObj.getString("md")
                val mid: String = commonObj.getString("mid")
                val os: String = commonObj.getString("os")
                val uid: String = commonObj.getString("uid")
                val vc: String = commonObj.getString("vc")
                //todo 提取时间戳
                val ts: Long = jsonObj.getLong("ts")
                //todo 提取页面数据
                val pageObj: JSONObject = jsonObj.getJSONObject("page")
                if (pageObj != null) {
                  //提取页面字段
                  val during_time: Long = pageObj.getLong("during_time")
                  val page_item: String = pageObj.getString("item")
                  val page_item_type: String = pageObj.getString("item_type")
                  val last_page_id: String = pageObj.getString("last_page_id")
                  val page_id: String = pageObj.getString("page_id")
                  val sourceType: String = pageObj.getString("source_type")
                  //封装PageLog
                  val pageLog: PageLog = PageLog(ar, ba, ch, is_new, md, mid, os, uid, vc, during_time, page_item, page_item_type, last_page_id, page_id, sourceType, ts)
                  //发送kafka
                  //todo SerializeConfig：内部是个map容器主要功能是配置并记录每种Java类型对应的序列化类。
                  // 设置为true,则开启只基于字段进行反序列化。就不是根据get和set方法来进行序列化和反序列化
                  MykafkaUtils.send(DWD_PAGE_LOG_TOPIC, JSON.toJSONString(pageLog, new SerializeConfig(true)))

                  //todo 提取事件，
                  val actions: JSONArray = jsonObj.getJSONArray("actions")
                  if (actions != null && actions.size() > 0) {
                    for (i <- 0 until actions.size()) {
                      val actionObj: JSONObject = actions.getJSONObject(i)
                      val action_id: String = actionObj.getString("action_id")
                      val action_item: String = actionObj.getString("item")
                      val action_item_type: String = actionObj.getString("item_type")

                      val action_ts: Long = actionObj.getLong("ts")
                      val actionLog: ActionLog = ActionLog(mid, uid, ar, ch, is_new, md, os, vc, page_id, last_page_id, page_item, page_item_type,
                        during_time, action_id, action_item, action_item_type, action_ts, ts)
                      MykafkaUtils.send(DWD_PAGE_ACTION_LOG_TOPIC, JSON.toJSONString(actionLog, new SerializeConfig(true)))
                      //todo  提取曝光
                      val displays: JSONArray = jsonObj.getJSONArray("displays")
                      if (displays != null && displays.size() > 0) {
                        for (i <- 0 until displays.size()) {
                          val displayObj: JSONObject = displays.getJSONObject(i)
                          val displayType: String = displayObj.getString("display_type")
                          val display_item: String = displayObj.getString("item")
                          val display_item_type: String = displayObj.getString("item_type")
                          val order: String = displayObj.getString("order") // 曝光顺序
                          val pos_id: String = displayObj.getString("pos_id") // 曝光位置
                          val displaylog: DisplayLog = DisplayLog(mid, uid, ar, ch, is_new, md, os, vc, page_id, last_page_id, page_item, page_item_type,
                            during_time, displayType, display_item, display_item_type, order, pos_id, ts)
                          MykafkaUtils.send(DWD_PAGE_DISPLAY_LOG_TOPIC, JSON.toJSONString(displaylog, new SerializeConfig(true)))
                        }
                      }
                    }
                  }
                }
                //todo  提取启动,此处的启动数据和页面数据是取反的
                val startlogObj: JSONObject = jsonObj.getJSONObject("start")
                if (startlogObj != null) {
                  val entry: String = startlogObj.getString("entry") //启动类型
                  val loading_time: Long = startlogObj.getLong("loading_time") //
                  val open_ad_id: String = startlogObj.getString("open_ad_id")
                  val open_ad_ms: Long = startlogObj.getLong("open_ad_ms")
                  val open_ad_skip_ms: Long = startlogObj.getLong("open_ad_skip_ms")
                  val startLog: StartLog = StartLog(mid, uid, ar, ch, is_new, md, os, vc,
                    entry, loading_time, open_ad_id, open_ad_ms, open_ad_skip_ms, ts)
                  MykafkaUtils.send(DWD_START_LOG_TOPIC, JSON.toJSONString(startLog, new SerializeConfig(true)))
                }
              }
            }
            //todo flush ,防止在数据在存缓冲区并且写kafka之前，kafka挂掉引发数据丢失问题，所以需要在每次批量Executor端刷写
            MykafkaUtils.flush()
          }
        )
        //todo 提交offset,对每个分区数据进行写出(默认粘性)
        MyOffsetUtils.writeOffset(ods_topic, ods_groupId, offsetRanges)
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
