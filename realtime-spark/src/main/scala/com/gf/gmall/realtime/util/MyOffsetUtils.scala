package com.gf.gmall.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
 * kafka offset 维护工具类
 */
object MyOffsetUtils {

  /**
   * todo 读取偏移量
   *  Kafka维护偏移量由: 消费者组groupId 、主题topic 、分区partition 三者决定==> offset
   *  偏移量的数据结构定义：
   *  kv结构:
   *  TopicPartition,Long
   *  kafka 消费者定义要从指定的offset位置进行消费,数据类型为 HashMap[TopicPartition, Long]
   */
  def readOffsets(topic: String, groupId: String): Map[TopicPartition, Long] = {
    val jedis: Jedis = MyRedisUtils.getJedis()
    val offsetsKey = s"offset:$topic:$groupId"
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetsKey)
    println("读取到的偏移量:" + offsetMap)
    jedis.close()

    /*
      todo 对读取到的偏移量offsetMap,进行结构转换 java.map --->Scala.map
        在import操作中，_可以表示导入一个包下的所有类，
     */
    import scala.collection.JavaConverters._
    val partitionOffsetMap: Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (partitionId, offset) => (new TopicPartition(topic, partitionId.toInt), offset.toLong)
    }.toMap
    partitionOffsetMap
  }

  /**
   * todo 写入偏移量
   */
  def writeOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    //todo 写入的数据包括 partition,offset,以map方式写入redis
    //todo 由于jedis 只支持java,不支持Scala中的数据类型，所有map定义为util.map
    val offsetMap = new util.HashMap[String, String]()
    for (offsetRange <- offsetRanges) {
      val partitionId: Int = offsetRange.partition
      val endoffset: Long = offsetRange.untilOffset
      offsetMap.put(partitionId.toString, endoffset.toString)
    }
    println("写入的偏移量:" + offsetMap)

    if (offsetMap.size > 0) {
      val offsetKey = s"offset:$topic:$groupId"
      val jedis: Jedis = MyRedisUtils.getJedis()
      jedis.hset(offsetKey, offsetMap)
      jedis.close()
    }

  }

}
