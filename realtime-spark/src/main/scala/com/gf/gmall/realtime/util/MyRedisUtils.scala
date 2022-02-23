package com.gf.gmall.realtime.util

import java.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * Redis工具类
 */
object MyRedisUtils {
  //为什么不能用val
  //todo 这里用var声明的变量的值是可以变量的， val一般声明不可变的量
  var jedisPool: JedisPool = null

  def getJedis(): Jedis = {

    if (jedisPool == null) {
      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(1000) //最大可连接数
      jedisPoolConfig.setMaxIdle(200) //最大空闲连接数
      jedisPoolConfig.setMinIdle(200) //最小空闲连接数
      jedisPoolConfig.setBlockWhenExhausted(true) // 连接耗尽是否等待
      jedisPoolConfig.setMaxWaitMillis(50000) //等待时间
      jedisPoolConfig.setTestOnBorrow(true) //取连接的时候进行下一个测试 Ping pong
      jedisPool = new JedisPool(jedisPoolConfig,
        PropertiesUtils(ConfigUtil.REDIS_HOST), PropertiesUtils(ConfigUtil.REDIS_PORT).toInt)
    }
    jedisPool.getResource
  }

  def main(args: Array[String]): Unit = {
    val jedis: Jedis = MyRedisUtils.getJedis()
//    val redisKeys: util.Set[String] = jedis.keys("*")
//    println(redisKeys)
    println(jedis.ping())
    jedis.close()
  }
}
