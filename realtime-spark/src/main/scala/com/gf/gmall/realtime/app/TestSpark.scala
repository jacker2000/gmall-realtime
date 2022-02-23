package com.gf.gmall.realtime.app

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object TestSpark {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setMaster("testApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val socketDStream: ReceiverInputDStream[String] =
      ssc.socketTextStream("hadoop102", 9999)


    ssc.start()
    ssc.awaitTermination()
  }
  /*
    todo 自定义分区器
      1.继承Partitioner
      2.重写方法
 */
  class MyPartitioner extends Partitioner{
    //重分区的数量
    override def numPartitions: Int = 3
    //根据数据key值返回所在分区的变化(从0开始)
    override def getPartition(key: Any): Int = {
      key match {
        /*
            todo 总共3个分区，分别为0,1,2，当然顺序可以打乱
                这里不能超过这3个分区的索引值，否则报错
         */
        case "nba" =>2 //分区号0
        case "cba"=>0  //分区号1
        case "wnba"=>1 //分区号2
      }
    }
  }
}
