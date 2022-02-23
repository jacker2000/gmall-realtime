package com.gf.gmall.realtime.app

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.gf.gmall.realtime.util.{MyOffsetUtils, MyRedisUtils, MykafkaUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * 业务数据采集分流
 * 0. 准备环境
 * 1. 从redis中读取offset
 * 2. 接收Kafka的数据
 * 3. 提取offset结束点
 * 4. 处理数据
 *    4.1 转换结构
 *    4.2 分流
 * 事实数据 -> Kafka
 * 维度数据 -> Redis
 * 5. 刷写Kafka
 * 6. 提交offset
 */
object BaseDbApp {

  def main(args: Array[String]): Unit = {
    //0.环境准备
    val sparkConf: SparkConf = new SparkConf().setAppName("base_db_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //1.从Redis中读取offset
    val ods_db_topic = "ODS_BASE_DB"
    val ods_base_db_group = "ods_base_db_group"
    val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffsets(ods_db_topic, ods_base_db_group)

    //2.接收kafka数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      kafkaDStream = MykafkaUtils.getKafkaDStream(ssc, ods_db_topic, ods_base_db_group, offsets)
    } else {
      kafkaDStream = MykafkaUtils.getKafkaDStream(ssc, ods_db_topic, ods_base_db_group)
    }
    //3. 提取偏移量结束点,对kafkaDStream中的rdd进行转换
    var offsetRanges: Array[OffsetRange] = null
    val kafkaOffsetDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    //4.处理数据
    // 4.1结构转换
    val jsonObjDStream: DStream[JSONObject] = kafkaOffsetDStream.map(
      record =>
        JSON.parseObject(record.value())
    )
    //对maxwell发kafka 再到spark 的数据进行测试
//        jsonObjDStream.print(200)

    /*
      todo 位置选择
        A. foreachRDD外边           driver   任务启动执行一次
        B. foreachRDD里面           driver   每批次一次
        C. foreachPartition里面     executor 每批次每分区一次

      todo
          注意：
            1.连接对象不能在Driver端 ，如Jedis， 应该放在Executor中，foreachPartition里面
            2.Driver端的对象，在Executor要用，需要发几次(Task次，每个Task就是每个分区的数据)
                 广播变量：共享只读变量(Driver--->Executor,每个Task引用这个广播变量)
            3.事实表、维度表动态配置:
              读流程:
               redis:
                  type :  set
                  key :   DIM:TABLES    FACT:TABLES
                  value :
                  写入api: sadd
                  读取api: smembers
                  是否过期: 否
              写流程:
                redis:
                  type :    string
                  key :     DIM:[TABLENAME]:[ID]
                  value :   json
                  写入api:  set
                  读取api:  get
                  是否过期: 否
     */
    //4.2分流
    jsonObjDStream.foreachRDD(
      rdd => {

        //获取jedis连接
        val jedis: Jedis = MyRedisUtils.getJedis()
        val dimTableKey = "DIM:TABLES"
        val factTableKey = "FACT:TABLES"
        /*
            todo 从redis 中 动态 获取维度表和事实表集合
                需要在redis 服务器中动态维护需要的维度表和事实表的表名，
                   相对应的事实表数据存放在kafka中
                   相对应的维度表数据存放在redis中
                  如 SADD MID:TABLES order_info
                  当维度表 order_info 表中数据修改操作后,会写入redis
         */
        val dimTables: util.Set[String] = jedis.smembers(dimTableKey)
        println("dimTables:" + dimTables)
        //todo 广播变量 dimTablesBC
        val dimTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dimTables)
        val factTables: util.Set[String] = jedis.smembers(factTableKey)
        println("factTables:" + factTables)
        //todo 广播变量 factTablesBC
        //sadd FACT:TABLES order_info  sadd DIM:TABLES: user_info  sadd DIM:TABLES  base_province
        /*
          todo
             sadd FACT:TABLES  order_info
             sadd FACT:TABLES  order_detail
             sadd DIM:TABLES  user_info
             sadd DIM:TABLES  base_province
         */
        val factTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(factTables)
        jedis.close()

        rdd.foreachPartition(

          jsonObjIter => {
            val jedis: Jedis = MyRedisUtils.getJedis()

            for (jsonObj <- jsonObjIter) {
              //提取表名
              val tableName: String = jsonObj.getString("table")
              //提取操作类型
              val operType: String = jsonObj.getString("type")
              //判断操作类型(1. 明确是什么操作 2.过滤不需要的操作，首次操作有bootstrap-start、bootstrap-insert、bootstrap-complete)
              val oper: String = operType match {
                case "bootstrap-insert" => "I"
                case "insert" => "I"
                case "update" => "U"
                case "delete" => "D"
                //在模式匹配中, _表示任意值
                case _ => null
              }
              if (oper!=null) {
                //分流事实数据 --->把事实表数据发往kafka
                if (factTablesBC.value.contains(tableName)) {
                  //提取data
                  val dataJsonObj: JSONObject = jsonObj.getJSONObject("data")
                  val dataId: String = dataJsonObj.getString("id")
                  //拼接主题Topic 格式: DWD_ORDER_INFO_I  DWD_ORDER_INFO_U
                  /*
                    DWD_ORDER_INFO_I,DWD_ORDER_DETAIL_I
                   */
                  val topicName =s"DWD_${tableName.toUpperCase()}_${oper}"
                  /*
                     todo
                        这里指定key的目的：
                          1.通过key取模，把数据发送到不同分区中, 相同的key修改，只会对当前分区数据有效，对其他分区的数据没有影响
                          2.可以保证数据的顺序性
                   */
                  MykafkaUtils.send(topicName,dataId,dataJsonObj.toJSONString)
                }

                //分流维度数据 --->把维度数据发往redis

                if (dimTablesBC.value.contains(tableName)) {
                  //  type :    string
                  //  key :     DIM:[TABLENAME]:[ID]
                  //  value :   json
                  //  写入api:  set
                  //  读取api:  get
                  //  是否过期: 否

                  //  提取data
                  val dataJsonObj: JSONObject = jsonObj.getJSONObject("data")
                  val id: String = dataJsonObj.getString("id")

                  val redisKey =s"DIM:${tableName.toUpperCase()}:${id}"
                  //在此处获取redis的连接不好，因为每条数据都要开关连接， 太频繁. 所以把连接放循环外面
                  jedis.set(redisKey,dataJsonObj.toJSONString)
                }
              }
            }
            MykafkaUtils.flush()
            //redis关闭连接
            jedis.close()
          }
        )
        MyOffsetUtils.writeOffset(ods_db_topic,ods_base_db_group,offsetRanges)
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }

}
