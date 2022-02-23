package com.gf.gmall.realtime.app

import java.{lang, util}
import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.gf.gmall.realtime.bean.{DauInfo, PageLog}
import com.gf.gmall.realtime.util.{DateUtils, MyBeanUtils, MyESUtils, MyOffsetUtils, MyRedisUtils, MykafkaUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, Pipeline}

import scala.collection.mutable.ListBuffer

/**
 * 日活宽表
 * 0. 准备环境
 * 1. 从redis中读取偏移量
 * 2. 接收kafka的数据
 * 3. 提取偏移量结束点
 * 4. 数据处理
 *    4.1 转换结构
 *    4.2 去重
 *    4.3 维度关联
 *
 * 5. 写入ES
 * 6. 提交offset
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    //状态还原
    reverDauState()
    //0.环境准备
    val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //1.从redis中读取偏移量
    val dwdPageLogTopic = "DWD_PAGE_LOG_TOPIC"
    val dwdDauGroup = "dwd_dau_group"
    val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffsets(dwdPageLogTopic, dwdDauGroup)

    //2.接收kafka的数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      kafkaDStream = MykafkaUtils.getKafkaDStream(ssc, dwdPageLogTopic, dwdDauGroup, offsets)
    } else {
      kafkaDStream = MykafkaUtils.getKafkaDStream(ssc, dwdPageLogTopic, dwdDauGroup)
    }

    //3.提取偏移量结束点
    var offsetRanges: Array[OffsetRange] = null
    val kafkaOffsetDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        //因为recodeDStream底层封装的是KafkaRDD，混入了HasOffsetRanges特质，这个特质中提供了可以获取偏移量范围的方法
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    //4.数据处理
    //4.1 转换
    val pageLogDStream: DStream[PageLog] = kafkaOffsetDStream.map(
      record =>
        JSON.parseObject(record.value(), classOf[PageLog])
    )
    /*
      RDD通过Cache或者Persist方法将前面的计算结果缓存，默认情况下会把数据以缓存在JVM的堆内存中。
      但是并不是这两个方法被调用时立即缓存，而是触发后面的action算子时，该RDD将会被缓存在计算节点的内存中，并供后面重用。
     */
    pageLogDStream.cache() // rdd 缓存
    // count 是行动算子，目的是把pageLogDStream进行缓存
    pageLogDStream.count().foreachRDD( //把数据拉回到Driver端
      rdd => rdd.collect().foreach(
        //审查前数据，随着采集周期采集数据量不同，起伏波动
        result => println("自我审查前:" + result) //数据在Executor端打印 (会根据分区打印4次数据)
      )
    )
    //    pageLogDStream.print(100)

    //4.2 去重
    //一个用户某日的访问数据：
    // 早8:00     首页 ->  列表页 -> 详情页  ->  购物车
    // 晚上9:00   详情页 -> 首页 -> 详情页 -> 购物车

    //自我审查
    /*
      filter为转换算子， 里面为true的保留，fasle的过滤
     */
    val filterDStream: DStream[PageLog] = pageLogDStream.filter(
      pagelog => pagelog.last_page_id == null //last_page_id :上页类型
    )
    filterDStream.cache()
    // count 是行动算子，目的是把filterDStream进行缓存
    filterDStream.count().foreachRDD(
      rdd => rdd.collect().foreach(
        filterResoult => println("自我审查后:" + filterResoult)
      )
    )
    //    filterDStream.print(100)

    // 第三方审查(redis)
    // type :    set
    // key :     DAU:[DATE]
    // value :   uid / mid   -->每个设备今日的每条不重复数据写入ES
    // 写入api:  sadd
    // 读取api:  sadd
    // 是否过期: 过期, 一天(ES出图)
    /*
       todo
          redis去重的目的：
            对当前用户当天访问的数据进行去重
            4个分区，4个并行度(并行执行)
            如果是String,进行if判断，则覆盖写redis,但es会生成4条重复数据
            如果是list,也要进行if判断
            总结：if 判断容易在并行度中穿插
            如果是 sadd :
                  放存在的，返回0
                  放不存在的，返回1
            在不同并行度中，进行sadd, 其中一个返回1，其他都返回0, 即1个1，3个0 (redis的幂等)
       todo
          foreachRdd、MapPartitions、foreachPartition的区别:
            1.foreachRDD 并不是以分区为单位进行处理，本身是行动算子
            2.mapPartitions: 本身是转换算子，对每个分区里面数据进程操作
                当入参只有1个的时候，可以转成{},其他情况是()
            3.foreachPartition,是action算子,将函数func应用于此RDD的每个分区
       todo
          判断到底用转换算子还是行动算子的标准?
            1.如果要想拿到要处理后结果的值(处理后的值用变量接收,在再下一个DSteam接着用)，则用转换
            2.如果只是打印，输出，不想要处理后的值,则用行动算子
     */
    val redisFilterDStream: DStream[PageLog] = filterDStream.mapPartitions(
      //每个批次 每个分区数据进行处理
      //将待处理的数据以分区为单位发送到计算节点进行处理，这里的处理是指可以进行任意的处理，哪怕是过滤数据。
      pageLogIter => {
        // todo 注意：iter 只遍历一次， 所以需要把iter转为List
        val pageLogList: List[PageLog] = pageLogIter.toList
        println("第三方redis失效过期审查前" + pageLogList.size)

        //存储过滤后的结果
        val pageLogResult: ListBuffer[PageLog] = ListBuffer[PageLog]() //定义最后存储的结果变量

        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val jedis: Jedis = MyRedisUtils.getJedis()

        for (pagelog <- pageLogList) {
          val ts: Long = pagelog.ts
          val date: String = sdf.format(new Date(ts))
          val dauKey = s"DAU:${date}"
          val mid: String = pagelog.mid
          //4个并行度 都执行sadd, 有的话取，没有的话舍掉
          val isNew: lang.Long = jedis.sadd(dauKey, mid) //redis并行通过set判断是否存在
          //过期时间  设置为当前时间晚上凌晨12点
          val expiresTime: Long = DateUtils.getSecondsNextEarlyMorning
          jedis.expire(dauKey, expiresTime.intValue())

          if (isNew == 1L) {
            pageLogResult.append(pagelog)
          }
        }
        jedis.close()
        println("第三方审查后:" + pageLogResult.size)
        pageLogResult.iterator
      }
    )
    // redis命令查询set数据类型多少条数据: scard  DAU:2020-06-15
    //        redisFilterDStream.print(100)


    //4.3 维度关联
    val dauInfoDStream: DStream[DauInfo] = redisFilterDStream.mapPartitions(
      pageLogIter => {

        val dauInfoList: ListBuffer[DauInfo] = ListBuffer[DauInfo]()
        val jedis: Jedis = MyRedisUtils.getJedis()
        //        val pageLogList: List[PageLog] = pageLogIter.toList
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
        for (pagelog <- pageLogIter) {

          val dauInfo = new DauInfo()
          //数据组装成dauInfo，//将pageLog信息填充dauInfo中

          MyBeanUtils.copyProperties(pagelog, dauInfo)
          //          val dauInfo: DauInfo = DauInfo(pagelog.province_id, pagelog.brand, pagelog.channel,
          //            pagelog.is_new, pagelog.model, pagelog.mid, pagelog.operate_system, pagelog.user_id, pagelog.version_code,
          //            pagelog.during_time, pagelog.page_item, pagelog.page_item_type, pagelog.page_id,
          //            gender, age, provinceName,
          //            iso_code, iso_3166_2,
          //            area_code, dtHrArr(0), dtHrArr(0), pagelog.ts
          //          )

          //用户信息关联,//补充用户维度 性别 年龄等 (redis)
          val dimUserInfo_Key = s"DIM:USER_INFO:${pagelog.user_id}"
          val userInfoJson: String = jedis.get(dimUserInfo_Key)
          val userInfoJsonObj: JSONObject = JSON.parseObject(userInfoJson)
          //提取生日
          val birthday: String = userInfoJsonObj.getString("birthday")
          //提取性别
          dauInfo.user_gender = userInfoJsonObj.getString("gender")
          //对生日处理
          var age: String = null
          if (birthday != null) {
            val nowDate: LocalDate = LocalDate.now()
            val birthdayDate: LocalDate = LocalDate.parse(birthday)
            val period: Period = Period.between(birthdayDate, nowDate)
            val years: Int = period.getYears
            dauInfo.user_age = years.toString
          }

          //补充地区维度 (redis)
          val dimBaseProvince_Key = s"DIM:BASE_PROVINCE:${pagelog.province_id}"
          val provinceJson: String = jedis.get(dimBaseProvince_Key)
          //          if (provinceJson != null) {
          val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)
          dauInfo.province_name = provinceJsonObj.getString("name")
          dauInfo.province_area_code = provinceJsonObj.getString("area_code")
          dauInfo.province_iso_code = provinceJsonObj.getString("iso_code")
          dauInfo.province_iso_3166_2 = provinceJsonObj.getString("iso_3166_2")
          //          }

          //补充日期字段

          val dtDate = new Date(pagelog.ts)
          val dtHr: String = sdf.format(dtDate)
          val dtHrArr: Array[String] = dtHr.split(" ")
          dauInfo.dt = dtHrArr(0)
          dauInfo.hr = dtHrArr(1)


          dauInfoList.append(dauInfo)
        }
        jedis.close()
        //过滤后的数据存ES(OLAP)
        dauInfoList.iterator
      }
    )
    //    dauInfoDStream.print(100)
    //写入到ES中
    //indexName
    dauInfoDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          dauInfoIter => {
            //转换数据结构  DauInfo => (mid, DauInfo)
            val dauInfoList: List[(String, DauInfo)] =
              dauInfoIter.map(dauInfo => (dauInfo.mid, dauInfo)).toList
            //模板中的index patterns : gmall_dau_info*
            // indexName : gmall_dau_info_2022-02-21
            if (dauInfoList.size > 0) {
              val dauInfo: (String, DauInfo) = dauInfoList.head
              val dt: String = dauInfo._2.dt
              val indexName: String = s"gmall_dau_info_$dt"

              MyESUtils.save(dauInfoList, indexName)
            }
          }
        )
        //提交offset
        MyOffsetUtils.writeOffset(dwdPageLogTopic, dwdDauGroup, offsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * todo
   * 状态还原
   *    1. 如果es有数据，redis没写成功，则需要把es的数据重新写入redis，防止redis没有写入成功，数据不一致的情况
   *    2. 如果es没有数据，redis有数据，则删除redis数据,重新写es和redis, 最后保证redis和es的数据是一致的
   */
  def reverDauState() = {
    //从ES中加载所有mid
    val localDate = LocalDate.now()
    val indexName = s"gmall_dau_info_$localDate"
    val field: String = "mid"
    val midList: List[String] = MyESUtils.searchField(indexName, field)
    val daukey = s"DAU:${localDate}"
    //    val daukey = "DAU:2022-02-01"
    val jedis: Jedis = MyRedisUtils.getJedis()
    //删除redis数据
    jedis.del(daukey)

    if (midList != null) {
      //进行状态还原
      //将es写入redis
      //通过redis做批量写入
      val pipeline: Pipeline = jedis.pipelined()
      for (mid <- midList) {
        pipeline.sadd(daukey, mid)
      }
      pipeline.sync()
      jedis.expire(daukey, DateUtils.getSecondsNextEarlyMorning.intValue())
    }
    jedis.close()
  }

}
