package com.gf.gmall.realtime.util

import java.util.ResourceBundle

/*
    读取kafka配置解析工具
 */
object PropertiesUtils {
  // 绑定配置文件
  // todo ResourceBundle专门用于读取配置文件，所以读取时，不需要增加扩展名
  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  //如果出现实例名称(有参或无参)，就自动调用类中定义的apply方法，输出如下
  //在Scala中，我们把所有类的构造方法以apply方法的形式定义在它的伴生对象当中，这样伴生对象的方法就会自动被调用，调用就会生成类对象。
  def apply(key: String): String = {
    //通过key 返回value
    bundle.getString(key)
  }

  def main(args: Array[String]): Unit = {
    println(PropertiesUtils(ConfigUtil.KAFKA_BROKER_LIST))
  }

}
