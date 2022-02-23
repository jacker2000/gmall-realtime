package com.gf.gmall.realtime.util

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

/*
  kafka工具类，提供生成、消费数据
 */
object MykafkaUtils {

  /**
   * 生产者对象方法
   */
  def createKafkaProducer() = {
    val pros = new Properties()
    //Kafka集群位置
    pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtils(ConfigUtil.KAFKA_BROKER_LIST))
    //key 和 value序列化器
    pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    /*
        当设置为'true'时，生产者将确保每个消息的一个副本被写入流中。如果'false'，生产者重试由于代理失败等，
        可能会写入重试消息的副本在流中。请注意，启用幂等性要求max.in.flight.requests.per.connection小于或等于5，
        重试大于0,acks必须为'all'。如果用户没有显式地设置这些值，则会选择合适的值。如果设置了不兼容的值，则会抛出一个ConfigException。
     */
    //todo 幂等性设置为开启,默认false
    pros.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    //ack       linger.ms     batch.size
    val producer = new KafkaProducer[String, String](pros)
    producer
  }

  /**
   * 构造生产者对象
   */
  private val producer: KafkaProducer[String, String] = createKafkaProducer()


  /**
   * 根据指定key,生成数据
   */
  def send(topic: String, key: String, value: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, key, value))
  }

  /**
   * 没有指定key,生成数据
   */
  def send(topic: String, value: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, value))
  }

  /**
   * 消费者配置
   */
  val kafkaConsumerParams: mutable.Map[String, Object] = mutable.Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertiesUtils(ConfigUtil.KAFKA_BROKER_LIST),
    //    ConsumerConfig.GROUP_ID_CONFIG -> PropertiesUtils(ConfigUtil.KAFKA_CONSUMER_GROUP_ID),
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    // todo 设置消费者自动提交
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",  //todo 手动维护offset后，和kafka 的自动提交这里就没关系了
    // todo 设置自动提交策略，最新偏移量，默认latest
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
  )

  /**
   * 获取DStream ,不指定Offset
   *
   * todo
   *            1.从kafka获取数据
   *            2.kafka中数据格式为:k-v
   *   Producer分区策略：
   *    通过key要把对应数据放在哪个分区中,有三种情况:
   *       - 情况1 ：指定了partition，则直接将指明的值直接作为partition值；
   *       - 情况2  未指定partition，封装key，则按照key的hashcode %分区数量partition = 得出把数据发往哪个分区；
   *       - 情况3：未指定partition，也未封装key处理方式 :
   *          Kafka采用Sticky Partition（黏性分区器），第一次会随机(生成一个整数)选择一个分区，并尽可能一直使用该分区，
   *            待该分区的batch已满或者linger.ms时间到，Kafka再随机一个分区进行使用（和上一次的分区不同）。
   *
   */
  def getKafkaDStream(scc: StreamingContext, topic: String, groupID: String) = {
    kafkaConsumerParams(ConsumerConfig.GROUP_ID_CONFIG) = groupID
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      scc,
      LocationStrategies.PreferConsistent, // todo 位置策略，分发到所有分区
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaConsumerParams)) //todo 消费策略
    kafkaDStream
  }

  /**
   * 获取DStream ,指定Offset
   */
  def getKafkaDStream(scc: StreamingContext, topic: String, groupID: String, offsets: Map[TopicPartition, Long]) = {
    kafkaConsumerParams(ConsumerConfig.GROUP_ID_CONFIG) = groupID
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      scc,
      LocationStrategies.PreferConsistent, // todo 位置策略，分发到所有分区
      // todo kafka 消费者定义要从指定的offset位置进行消费,数据类型为 HashMap[TopicPartition, Long]
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaConsumerParams, offsets)) //todo 消费策略
    kafkaDStream
  }

  /**
   * 关闭生产者对象
   */
  def close() = {
    if (producer != null) producer.close()
  }

  /**
   * 刷写缓冲区
   * todo 目的：
   * 防止消息存入到缓冲区的时候,数据丢失
   */
  def flush() = {
    if (producer != null) {
      producer.flush()

    }
  }

}
